///|
async test "consumer splits a one-message broker batch" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(1)
      let payload = encode_batch_payload([ProducerMessage::new(b"single")])
      let (cmd, meta, body) = message_frame(1UL, 5UL, 7UL, payload, metadata)
      peer.tcp.write(encode_send_frame(cmd, meta, body))
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/one-batch", "sub",
    )
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"single")
    assert_eq(message.id().batch_index, 0)
    message.ack()
    consumer.close()
    manager.close()
  })
}

///|
async test "reader splits a one-message broker batch" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(1)
      let payload = encode_batch_payload([ProducerMessage::new(b"single")])
      let (cmd, meta, body) = message_frame(1UL, 5UL, 7UL, payload, metadata)
      peer.tcp.write(encode_send_frame(cmd, meta, body))
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/one-batch-reader",
    )
    let received = @async.with_timeout(3000, async fn() { reader.next() })
    guard received is Some(message) else { fail("reader returned end") }
    assert_eq(message.payload(), b"single")
    assert_eq(message.id().batch_index, 0)
    reader.close()
    manager.close()
  })
}
