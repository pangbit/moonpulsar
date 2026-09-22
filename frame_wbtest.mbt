// Unit tests for CRC32-C and frame codec.

///|
fn be32(buf : @buffer.Buffer, v : Int) -> Unit {
  buf.write_byte((v >> 24).to_byte())
  buf.write_byte((v >> 16).to_byte())
  buf.write_byte((v >> 8).to_byte())
  buf.write_byte(v.to_byte())
}

///|
test "crc32c standard vectors" {
  assert_eq(crc32c(b""), 0)
  assert_eq(crc32c(b"123456789"), 0xe3069283)
}

///|
test "command frame roundtrip" {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::PING
  cmd.ping = Some(@proto.CommandPing::default())
  let frame = encode_command_frame(cmd)
  // total size field covers everything after itself
  let total = ((frame[0].to_uint() << 24) |
  (frame[1].to_uint() << 16) |
  (frame[2].to_uint() << 8) |
  frame[3].to_uint()).reinterpret_as_int()
  assert_eq(total, frame.length() - 4)
  let decoded = decode_frame_body(frame[4:].to_owned())
  assert_eq(decoded.cmd.type_, @proto.BaseCommand_Type::PING)
  assert_true(decoded.cmd.ping is Some(_))
  assert_true(decoded.metadata is None)
}

///|
test "send frame roundtrip with payload" {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SEND
  let send = @proto.CommandSend::default()
  send.producer_id = 7
  send.sequence_id = 42
  cmd.send = Some(send)
  let metadata = @proto.MessageMetadata::default()
  metadata.producer_name = "test-producer"
  metadata.sequence_id = 42
  metadata.publish_time = 1234567890
  let payload = b"hello"
  let frame = encode_send_frame(cmd, metadata, payload)
  let decoded = decode_frame_body(frame[4:].to_owned())
  assert_eq(decoded.cmd.type_, @proto.BaseCommand_Type::SEND)
  guard decoded.cmd.send is Some(send2) else { fail("missing send command") }
  assert_eq(send2.producer_id, 7)
  assert_eq(send2.sequence_id, 42)
  guard decoded.metadata is Some(meta2) else { fail("missing metadata") }
  assert_eq(meta2.producer_name, "test-producer")
  guard decoded.payload is Some(p) else { fail("missing payload") }
  assert_eq(p, b"hello")
}

///|
test "decode frame with broker entry metadata and checksum" {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::MESSAGE
  let message = @proto.CommandMessage::default()
  message.consumer_id = 1
  cmd.message = Some(message)
  let metadata = @proto.MessageMetadata::default()
  metadata.producer_name = "p"
  metadata.sequence_id = 1
  metadata.publish_time = 1
  let payload = b"data"
  // build tail: broker meta size + broker meta + metadata size + metadata + payload
  let broker_meta = @proto.BrokerEntryMetadata::default()
  broker_meta.broker_timestamp = Some(99)
  let broker_buf = @buffer.Buffer()
  @lib.Write::write(broker_meta, broker_buf)
  let broker_bytes = broker_buf.to_bytes()
  let meta_buf = @buffer.Buffer()
  @lib.Write::write(metadata, meta_buf)
  let meta_bytes = meta_buf.to_bytes()
  let tail = @buffer.Buffer()
  be32(tail, broker_bytes.length())
  tail.write_bytes(broker_bytes)
  be32(tail, meta_bytes.length())
  tail.write_bytes(meta_bytes)
  tail.write_bytes(payload)
  let tail_bytes = tail.to_bytes()
  let checksum = crc32c(tail_bytes)
  let body = @buffer.Buffer()
  let cmd_buf = @buffer.Buffer()
  @lib.Write::write(cmd, cmd_buf)
  let cmd_bytes = cmd_buf.to_bytes()
  be32(body, cmd_bytes.length())
  body.write_bytes(cmd_bytes)
  // magic + checksum
  body.write_byte(0x0e)
  body.write_byte(0x01)
  body.write_byte((checksum >> 24).to_byte())
  body.write_byte((checksum >> 16).to_byte())
  body.write_byte((checksum >> 8).to_byte())
  body.write_byte(checksum.to_byte())
  body.write_bytes(tail_bytes)
  let decoded = decode_frame_body(body.to_bytes())
  assert_eq(decoded.cmd.type_, @proto.BaseCommand_Type::MESSAGE)
  guard decoded.payload is Some(p) else { fail("missing payload") }
  assert_eq(p, b"data")
  guard decoded.metadata is Some(m) else { fail("missing metadata") }
  assert_eq(m.producer_name, "p")
}

///|
test "decode frame rejects bad checksum" {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::MESSAGE
  cmd.message = Some(@proto.CommandMessage::default())
  let cmd_buf = @buffer.Buffer()
  @lib.Write::write(cmd, cmd_buf)
  let cmd_bytes = cmd_buf.to_bytes()
  let body = @buffer.Buffer()
  be32(body, cmd_bytes.length())
  body.write_bytes(cmd_bytes)
  body.write_byte(0x0e)
  body.write_byte(0x01)
  // deliberately wrong checksum
  body.write_byte(0xde)
  body.write_byte(0xad)
  body.write_byte(0xbe)
  body.write_byte(0xef)
  be32(body, 0)
  let result : Result[IncomingFrame, Error] = Ok(
    decode_frame_body(body.to_bytes()),
  ) catch {
    e => Err(e)
  }
  assert_true(result is Err(ChecksumMismatch))
}
