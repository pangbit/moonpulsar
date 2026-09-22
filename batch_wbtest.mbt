///|
test "batch payload encode/split roundtrip" {
  let msgs = [
    ProducerMessage::new(b"m0", key="a"),
    ProducerMessage::new(b"m1"),
  ]
  let payload = encode_batch_payload(msgs)
  let singles = split_batch_payload(payload, 2)
  assert_eq(singles.length(), 2)
  assert_eq(singles[0].1, b"m0")
  assert_eq(singles[0].0.partition_key, Some("a"))
  assert_eq(singles[1].1, b"m1")
}
