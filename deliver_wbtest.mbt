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

///|
async test "batched producer sends delayed messages separately" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let first = peer.next()
      guard first.cmd.send is Some(send1) else { fail("expected first SEND") }
      guard first.metadata is Some(meta1) else {
        fail("first metadata missing")
      }
      assert_true(meta1.deliver_at_time is Some(_))
      assert_eq(meta1.num_messages_in_batch.unwrap_or(1), 1)
      send_receipt(peer, send1.producer_id, send1.sequence_id)
      let second = peer.next()
      guard second.cmd.send is Some(send2) else { fail("expected second SEND") }
      guard second.metadata is Some(meta2) else {
        fail("second metadata missing")
      }
      assert_eq(meta2.deliver_at_time, None)
      assert_eq(meta2.num_messages_in_batch.unwrap_or(1), 1)
      send_receipt(peer, send2.producer_id, send2.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/t",
      batching={ max_messages: 2, max_bytes: 1024, max_delay_ms: 60000, },
    )
    let delayed = producer.send_async(
      ProducerMessage::new(b"later", deliver_after=3000),
    )
    let ordinary = producer.send_async(ProducerMessage::new(b"now"))
    producer.flush()
    ignore(delayed.wait())
    ignore(ordinary.wait())
    producer.close()
    manager.close()
  })
}
