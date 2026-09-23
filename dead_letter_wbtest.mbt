// Dead-letter delivery must publish before acknowledging the source message.

///|
test "dead-letter policy validates delivery limit and topic" {
  let zero : Result[DeadLetterPolicy, Error] = Ok(
    DeadLetterPolicy::new(0U, "persistent://public/default/dlq"),
  ) catch {
    e => Err(e)
  }
  assert_true(zero is Err(_))
  let empty : Result[DeadLetterPolicy, Error] = Ok(
    DeadLetterPolicy::new(2U, ""),
  ) catch {
    e => Err(e)
  }
  assert_true(empty is Err(_))
  let policy = DeadLetterPolicy::new(2U, "persistent://public/default/dlq")
  assert_eq(policy.max_deliveries(), 2U)
}

///|
async test "dead-letter publish succeeds before source ack" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.partition_key = Some("key")
      metadata.ordering_key = Some(b"order")
      metadata.event_time = Some(123UL)
      let kv = @proto.KeyValue::default()
      kv.key = "property"
      kv.value = "value"
      metadata.properties.push(kv)
      let (cmd, meta, payload) = message_frame(3UL, 1, 2, b"failed", metadata)
      peer.send_message(cmd, meta, payload)
      let nacked = peer.next()
      guard nacked.cmd.redeliver_unacknowledged_messages is Some(_) else {
        fail("expected initial NACK")
      }
      let redelivery_meta = test_metadata()
      redelivery_meta.partition_key = Some("key")
      redelivery_meta.ordering_key = Some(b"order")
      redelivery_meta.event_time = Some(123UL)
      let redelivery_kv = @proto.KeyValue::default()
      redelivery_kv.key = "property"
      redelivery_kv.value = "value"
      redelivery_meta.properties.push(redelivery_kv)
      let (again, again_meta, again_payload) = message_frame(
        3UL, 1, 2, b"failed", redelivery_meta,
      )
      peer.send_message(again, again_meta, again_payload)
      let sent = peer.next()
      guard sent.cmd.send is Some(send) &&
        sent.metadata is Some(sent_meta) &&
        sent.payload is Some(sent_payload) else {
        fail("expected dead-letter SEND")
      }
      assert_eq(sent_payload, b"failed")
      assert_eq(sent_meta.partition_key, Some("key"))
      assert_eq(sent_meta.ordering_key, Some(b"order"))
      assert_eq(sent_meta.event_time, Some(123UL))
      assert_eq(sent_meta.properties[0].value, "value")
      send_receipt(peer, send.producer_id, send.sequence_id)
      let acked = peer.next()
      guard acked.cmd.ack is Some(ack) else { fail("expected source ACK") }
      assert_eq(ack.message_id[0].entry_id, 2)
      let (next, next_meta, next_body) = message_frame(
        3UL,
        1,
        3,
        b"good",
        test_metadata(),
      )
      peer.send_message(next, next_meta, next_body)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = Producer::single(
      SingleProducer::create(group, conn, "persistent://public/default/dlq"),
    )
    let consumer = Consumer::single(
      SingleConsumer::create(
        group, conn, "persistent://public/default/source", "sub",
      ),
    )
    consumer.set_dead_letter(
      DeadLetterPolicy::new(1U, "persistent://public/default/dlq"),
      producer,
      group,
    )
    let initial = @async.with_timeout(5000, async fn() { consumer.receive() })
    assert_eq(initial.redelivery_count(), 0U)
    consumer.nack(initial.id())
    let received = @async.with_timeout(5000, async fn() { consumer.receive() })
    assert_eq(received.payload(), b"good")
    consumer.close()
    manager.close()
  })
}

///|
async test "dead-letter send failure nacks the source" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        3UL,
        1,
        2,
        b"failed",
        test_metadata(),
      )
      if cmd.message is Some(m) {
        m.redelivery_count = Some(1U)
      }
      peer.send_message(cmd, meta, payload)
      let sent = peer.next()
      guard sent.cmd.send is Some(send) else { fail("expected SEND") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SEND_ERROR
      let error = @proto.CommandSendError::default()
      error.producer_id = send.producer_id
      error.sequence_id = send.sequence_id
      error.error = @proto.ServerError::PersistenceError
      error.message = "disk full"
      reply.send_error = Some(error)
      peer.send_command(reply)
      let redelivery = peer.next()
      guard redelivery.cmd.redeliver_unacknowledged_messages is Some(req) else {
        fail("expected source REDELIVER")
      }
      assert_eq(req.message_ids[0].entry_id, 2)
      let (next, next_meta, next_body) = message_frame(
        3UL,
        1,
        3,
        b"good",
        test_metadata(),
      )
      peer.send_message(next, next_meta, next_body)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = Producer::single(
      SingleProducer::create(group, conn, "persistent://public/default/dlq"),
    )
    let consumer = Consumer::single(
      SingleConsumer::create(
        group, conn, "persistent://public/default/source", "sub",
      ),
    )
    consumer.set_dead_letter(
      DeadLetterPolicy::new(1U, "persistent://public/default/dlq"),
      producer,
      group,
    )
    let received = @async.with_timeout(5000, async fn() { consumer.receive() })
    assert_eq(received.payload(), b"good")
    consumer.close()
    manager.close()
  })
}

///|
async test "composite try_receive routes a dead-letter message" {
  @async.with_task_group(async fn(group) {
    let routed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        3UL,
        1,
        2,
        b"failed",
        test_metadata(),
      )
      if cmd.message is Some(m) {
        m.redelivery_count = Some(1U)
      }
      peer.send_message(cmd, meta, payload)
      let sent = peer.next()
      guard sent.cmd.send is Some(send) else { fail("expected SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
      let acked = peer.next()
      guard acked.cmd.ack is Some(_) else { fail("expected source ACK") }
      routed.put(true)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = Producer::single(
      SingleProducer::create(group, conn, "persistent://public/default/dlq"),
    )
    let child = Consumer::single(
      SingleConsumer::create(
        group, conn, "persistent://public/default/source", "sub",
      ),
    )
    let consumer = Consumer::partitioned(group, [child])
    consumer.set_dead_letter(
      DeadLetterPolicy::new(1U, "persistent://public/default/dlq"),
      producer,
      group,
    )
    @async.with_timeout(5000, async fn() {
      while routed.try_get() is None {
        assert_true(consumer.try_receive() is None)
        @async.sleep(1)
      }
    })
    consumer.close()
    manager.close()
  })
}
