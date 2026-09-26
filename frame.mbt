// Pulsar binary protocol framing.
//
// Every frame starts with a 4-byte big-endian total size, followed by a
// 4-byte command size and the protobuf-encoded `BaseCommand`. Message
// frames (SEND / MESSAGE) append, after the command, the message metadata
// and payload, optionally preceded by broker entry metadata protected by
// a CRC32-C checksum:
//
//   [TOTAL_SIZE u32][CMD_SIZE u32][BaseCommand]
//   [MAGIC 0x0e01 u16][CHECKSUM u32][METADATA_SIZE u32]
//   [MessageMetadata][PAYLOAD]
//
// A batched payload consists of `num_messages_in_batch` entries of:
//   [SINGLE_META_SIZE u32][SingleMessageMetadata][payload]

///|
/// Magic number preceding the CRC32-C checksum on message frames.
const MESSAGE_CHECKSUM_MAGIC : Int = 0x0e01

///|
fn write_be32(buf : @buffer.Buffer, v : UInt) -> Unit {
  buf.write_byte((v >> 24).to_byte())
  buf.write_byte((v >> 16).to_byte())
  buf.write_byte((v >> 8).to_byte())
  buf.write_byte(v.to_byte())
}

///|
fn read_be32(data : BytesView, at : Int) -> UInt raise PulsarError {
  if at + 4 > data.length() {
    raise InvalidFrame("unexpected end of frame")
  }
  (data[at].to_uint() << 24) |
  (data[at + 1].to_uint() << 16) |
  (data[at + 2].to_uint() << 8) |
  data[at + 3].to_uint()
}

///|
fn[M : @lib.Write] write_proto(buf : @buffer.Buffer, msg : M) -> Unit raise {
  msg.write(buf)
}

///|
fn[M : @lib.Read] read_proto(data : Bytes) -> M raise {
  let reader = @lib.BytesReader::from_bytes(data)
  M::read_with_limit(@lib.LimitedReader(reader))
}

///|
/// Encode a plain command frame (no payload attached).
pub fn encode_command_frame(cmd : @proto.BaseCommand) -> Bytes raise {
  let body = @buffer.Buffer()
  write_proto(body, cmd)
  let cmd_bytes = body.to_bytes()
  let frame = @buffer.Buffer()
  write_be32(frame, (cmd_bytes.length() + 4).reinterpret_as_uint())
  write_be32(frame, cmd_bytes.length().reinterpret_as_uint())
  frame.write_bytes(cmd_bytes)
  frame.to_bytes()
}

///|
/// Encode a message frame (SEND command with metadata and payload).
/// Client-side frames never carry broker entry metadata.
pub fn encode_send_frame(
  cmd : @proto.BaseCommand,
  metadata : @proto.MessageMetadata,
  payload : Bytes,
) -> Bytes raise {
  let cmd_body = @buffer.Buffer()
  write_proto(cmd_body, cmd)
  let cmd_bytes = cmd_body.to_bytes()
  let meta_body = @buffer.Buffer()
  write_proto(meta_body, metadata)
  let meta_bytes = meta_body.to_bytes()
  let tail = @buffer.Buffer()
  write_be32(tail, meta_bytes.length().reinterpret_as_uint())
  tail.write_bytes(meta_bytes)
  tail.write_bytes(payload)
  let tail_bytes = tail.to_bytes()
  let checksum = crc32c(tail_bytes)
  let total = 4 + cmd_bytes.length() + 6 + tail_bytes.length()
  let frame = @buffer.Buffer()
  write_be32(frame, total.reinterpret_as_uint())
  write_be32(frame, cmd_bytes.length().reinterpret_as_uint())
  frame.write_bytes(cmd_bytes)
  frame.write_byte((MESSAGE_CHECKSUM_MAGIC >> 8).to_byte())
  frame.write_byte(MESSAGE_CHECKSUM_MAGIC.to_byte())
  write_be32(frame, checksum)
  frame.write_bytes(tail_bytes)
  frame.to_bytes()
}

///|
/// Read one frame from an async byte stream: 4-byte big-endian total size,
/// then the frame body.
pub async fn[R : @io.Reader] read_frame(reader : R) -> IncomingFrame {
  let header = reader.read_exactly(4) catch { _ => raise ConnectionClosed }
  let total = ((header[0].to_uint() << 24) |
  (header[1].to_uint() << 16) |
  (header[2].to_uint() << 8) |
  header[3].to_uint()).reinterpret_as_int()
  if total < 4 {
    raise InvalidFrame("invalid frame size")
  }
  let body = reader.read_exactly(total) catch { _ => raise ConnectionClosed }
  decode_frame_body(body)
}

///|
/// Reserve the encoded frame before allocating its body. The caller holds the
/// returned lease through dispatch, then releases it even if dispatch fails.
async fn[R : @io.Reader] read_frame_with_budget(
  reader : R,
  limiter : ClientMemoryLimiter?,
) -> (IncomingFrame, ClientMemoryLease?) {
  let header = reader.read_exactly(4) catch { _ => raise ConnectionClosed }
  let total = ((header[0].to_uint() << 24) |
  (header[1].to_uint() << 16) |
  (header[2].to_uint() << 8) |
  header[3].to_uint()).reinterpret_as_int()
  if total < 4 || total > 2147483643 {
    raise InvalidFrame("invalid frame size")
  }
  let lease = match limiter {
    Some(budget) => Some(ClientMemoryLease::new(budget, total + 4))
    None => None
  }
  errdefer (if lease is Some(reservation) { reservation.release() })
  let body = reader.read_exactly(total) catch { _ => raise ConnectionClosed }
  (decode_frame_body(body), lease)
}

///|
/// A frame received from the broker: the command plus, for MESSAGE frames,
/// the message metadata and payload.
pub(all) struct IncomingFrame {
  cmd : @proto.BaseCommand
  metadata : @proto.MessageMetadata?
  payload : Bytes?
  wire_size : Int
} derive(Debug)

///|
// The generated proto2 reader applies the declared default of 1 even when
// field 11 is absent. Keep the wire presence so a one-message batch differs
// from an ordinary unbatched message.
fn metadata_has_batch_count(data : Bytes) -> Bool raise {
  let reader = @lib.LimitedReader(@lib.BytesReader::from_bytes(data))
  while (reader |> @lib.read_next_message_field()) is Some((field, wire)) {
    if field == 11 {
      return true
    }
    reader |> @lib.skip_message_field_by_number(field, wire)
  }
  false
}

///|
/// Decode a frame body (without the leading total size field).
pub fn decode_frame_body(body : Bytes) -> IncomingFrame raise PulsarError {
  let cmd_size = read_be32(body, 0).reinterpret_as_int()
  if cmd_size <= 0 || cmd_size > body.length() - 4 {
    raise InvalidFrame("command size exceeds frame length")
  }
  let cmd_bytes = body[4:4 + cmd_size].to_owned()
  let cmd : @proto.BaseCommand = read_proto(cmd_bytes) catch {
    _ => raise InvalidFrame("malformed BaseCommand")
  }
  if cmd.new_txn_response is Some(response) {
    restore_transaction_id_presence(cmd_bytes, response) catch {
      _ => raise InvalidFrame("malformed NEW_TXN_RESPONSE")
    }
  }
  let rest = body[4 + cmd_size:]
  if rest.length() == 0 {
    return { cmd, metadata: None, payload: None, wire_size: body.length() + 4, }
  }
  let mut offset = 0
  // Payload frames carry a CRC32-C checksum guarded by a magic number.
  // Broker entry metadata uses a separate header, which we never request
  // with `supports_broker_entry_metadata` in CONNECT.
  if rest.length() >= 2 &&
    rest[0].to_int() == MESSAGE_CHECKSUM_MAGIC >> 8 &&
    rest[1].to_int() == (MESSAGE_CHECKSUM_MAGIC & 0xff) {
    let checksum = read_be32(rest, 2)
    if crc32c(rest[6:]) != checksum {
      raise ChecksumMismatch
    }
    offset = 6
  }
  let metadata_size = read_be32(rest, offset).reinterpret_as_int()
  if metadata_size < 0 || metadata_size > rest.length() - offset - 4 {
    raise InvalidFrame("message metadata exceeds frame length")
  }
  let meta_bytes = rest[offset + 4:offset + 4 + metadata_size].to_owned()
  let metadata : @proto.MessageMetadata = read_proto(meta_bytes) catch {
    _ => raise InvalidFrame("malformed MessageMetadata")
  }
  let has_batch_count = metadata_has_batch_count(meta_bytes) catch {
    _ => raise InvalidFrame("malformed MessageMetadata")
  }
  if !has_batch_count {
    metadata.num_messages_in_batch = None
  } else if metadata.num_messages_in_batch.unwrap_or(0) <= 0 {
    raise InvalidFrame("message batch count must be positive")
  }
  {
    cmd,
    metadata: Some(metadata),
    payload: Some(rest[offset + 4 + metadata_size:].to_owned()),
    wire_size: body.length() + 4,
  }
}

///|
// Generated proto2 decoding defaults absent transaction IDs to Some(0).
// Preserve wire presence so the coordinator can reject incomplete responses.
fn restore_transaction_id_presence(
  data : Bytes,
  response : @proto.CommandNewTxnResponse,
) -> Unit raise {
  let reader = @lib.LimitedReader(@lib.BytesReader::from_bytes(data))
  let mut has_least = false
  let mut has_most = false
  while (reader |> @lib.read_next_message_field()) is Some((field, wire)) {
    if field == 51 && wire == @lib.WIRE_TYPE_LENGTH_DELIMITED {
      let nested = @lib.LimitedReader(
        @lib.BytesReader::from_bytes(@lib.read_bytes(reader)),
      )
      // Match the generated reader's last occurrence semantics.
      has_least = false
      has_most = false
      while (nested |> @lib.read_next_message_field()) is Some((number, kind)) {
        if number == 2 {
          has_least = true
        }
        if number == 3 {
          has_most = true
        }
        nested |> @lib.skip_message_field_by_number(number, kind)
      }
    } else {
      reader |> @lib.skip_message_field_by_number(field, wire)
    }
  }
  if !has_least {
    response.txnid_least_bits = None
  }
  if !has_most {
    response.txnid_most_bits = None
  }
}
