///|
test "batch payload encode/split roundtrip" {
  let msgs = [ProducerMessage::new(b"m0", key="a"), ProducerMessage::new(b"m1")]
  let payload = encode_batch_payload(msgs)
  let singles = split_batch_payload(payload, 2)
  assert_eq(singles.length(), 2)
  assert_eq(singles[0].1, b"m0")
  assert_eq(singles[0].0.partition_key, Some("a"))
  assert_eq(singles[1].1, b"m1")
}

///|
test "batch payload uses single-message metadata size without another length" {
  let payload = encode_batch_payload([ProducerMessage::new(b"m0")])
  let meta_size = read_be32(payload, 0).reinterpret_as_int()
  assert_eq(payload.length(), 4 + meta_size + 2)
  assert_eq(payload[4 + meta_size:], b"m0")
  let singles = split_batch_payload(payload, 1)
  assert_eq(singles[0].1, b"m0")
  let go_singles = split_batch_payload(b"\x00\x00\x00\x04\x18\x02\x40\x00m0", 1)
  assert_eq(go_singles[0].1, b"m0")
  let malformed : Result[Array[(@proto.SingleMessageMetadata, Bytes)], Error] = Ok(
    split_batch_payload(b"\x00\x00\x00\x02\x18\x05hi", 1),
  ) catch {
    e => Err(e)
  }
  assert_true(malformed is Err(InvalidFrame(_)))
}

///|
test "frame rejects an explicit nonpositive batch count" {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::MESSAGE
  cmd.message = Some(@proto.CommandMessage::default())
  let metadata = test_metadata()
  metadata.num_messages_in_batch = Some(0)
  let frame = encode_send_frame(cmd, metadata, b"payload")
  let result : Result[IncomingFrame, Error] = Ok(
    decode_frame_body(frame[4:].to_owned()),
  ) catch {
    e => Err(e)
  }
  assert_true(result is Err(InvalidFrame(_)))
}
