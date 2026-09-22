// Pulsar binary protocol framing.
//
// Every frame starts with a 4-byte big-endian total size, followed by a
// 4-byte command size and the protobuf-encoded `BaseCommand`. Message
// frames (SEND / MESSAGE) append, after the command, the message metadata
// and payload, optionally preceded by broker entry metadata protected by
// a CRC32-C checksum:
//
//   [TOTAL_SIZE u32][CMD_SIZE u32][BaseCommand]
//   [MAGIC 0x0e01 u16][CHECKSUM u32][BROKER_META_SIZE u32][BrokerEntryMetadata]
//   [METADATA_SIZE u32][MessageMetadata][PAYLOAD]
//
// A batched payload consists of `num_messages_in_batch` entries of:
//   [SINGLE_META_SIZE u32][SingleMessageMetadata][SINGLE_PAYLOAD_SIZE u32][payload]

///|
/// Magic number preceding the broker entry metadata checksum.
const BROKER_ENTRY_METADATA_MAGIC : Int = 0x0e01

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
  let total = 4 +
    cmd_bytes.length() +
    4 +
    meta_bytes.length() +
    payload.length()
  let frame = @buffer.Buffer()
  write_be32(frame, total.reinterpret_as_uint())
  write_be32(frame, cmd_bytes.length().reinterpret_as_uint())
  frame.write_bytes(cmd_bytes)
  write_be32(frame, meta_bytes.length().reinterpret_as_uint())
  frame.write_bytes(meta_bytes)
  frame.write_bytes(payload)
  frame.to_bytes()
}

///|
/// Read one frame from an async byte stream: 4-byte big-endian total size,
/// then the frame body.
pub async fn[R : @io.Reader] read_frame(reader : R) -> IncomingFrame raise {
  let header = reader.read_exactly(4) catch { _ => raise ConnectionClosed }
  let total = (((header[0].to_uint() << 24) |
  (header[1].to_uint() << 16) |
  (header[2].to_uint() << 8) |
  header[3].to_uint())).reinterpret_as_int()
  let body = reader.read_exactly(total) catch { _ => raise ConnectionClosed }
  decode_frame_body(body)
}

///|
/// A frame received from the broker: the command plus, for MESSAGE frames,
/// the message metadata and payload.
pub(all) struct IncomingFrame {
  cmd : @proto.BaseCommand
  metadata : @proto.MessageMetadata?
  payload : Bytes?
} derive(Debug)

///|
/// Decode a frame body (without the leading total size field).
pub fn decode_frame_body(body : Bytes) -> IncomingFrame raise PulsarError {
  let cmd_size = read_be32(body, 0).reinterpret_as_int()
  if 4 + cmd_size > body.length() {
    raise InvalidFrame("command size exceeds frame length")
  }
  let cmd : @proto.BaseCommand = read_proto(
    body[4:4 + cmd_size].to_owned(),
  ) catch {
    _ => raise InvalidFrame("malformed BaseCommand")
  }
  let rest = body[4 + cmd_size:]
  if rest.length() == 0 {
    return { cmd, metadata: None, payload: None }
  }
  let mut offset = 0
  // Optional broker entry metadata, protected by a CRC32-C checksum.
  if rest.length() >= 2 &&
    rest[0].to_int() == (BROKER_ENTRY_METADATA_MAGIC >> 8) &&
    rest[1].to_int() == (BROKER_ENTRY_METADATA_MAGIC & 0xff) {
    let checksum = read_be32(rest, 2)
    if crc32c(rest[6:]) != checksum {
      raise ChecksumMismatch
    }
    let broker_meta_size = read_be32(rest, 6).reinterpret_as_int()
    offset = 10 + broker_meta_size
    if offset > rest.length() {
      raise InvalidFrame("broker entry metadata exceeds frame length")
    }
  }
  let metadata_size = read_be32(rest, offset).reinterpret_as_int()
  if offset + 4 + metadata_size > rest.length() {
    raise InvalidFrame("message metadata exceeds frame length")
  }
  let metadata : @proto.MessageMetadata = read_proto(
    rest[offset + 4:offset + 4 + metadata_size].to_owned(),
  ) catch {
    _ => raise InvalidFrame("malformed MessageMetadata")
  }
  {
    cmd,
    metadata: Some(metadata),
    payload: Some(rest[offset + 4 + metadata_size:].to_owned()),
  }
}
