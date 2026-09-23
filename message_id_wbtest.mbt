///|
test "earliest and latest message ids are distinct" {
  let earliest = MessageId::earliest()
  let latest = MessageId::latest()
  assert_true(earliest != latest)
  assert_eq(earliest.ledger_id, 18446744073709551615)
  assert_eq(latest.ledger_id, 9223372036854775807)
}
