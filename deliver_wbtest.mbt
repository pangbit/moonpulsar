///|
async test "deliver_after produces deliver_at_time in metadata" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      guard frame.metadata is Some(meta) else { fail("no metadata") }
      let now = @async.now()
      guard meta.deliver_at_time is Some(t) else {
        fail("deliver_at_time missing")
      }
      let delta = t - now
      assert_true(delta > 2000 && delta < 5000)
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group, conn, "persistent://public/default/t",
    )
    ignore(producer.send(ProducerMessage::new(b"x", deliver_after=3000)))
    producer.close()
    manager.close()
  })
}
