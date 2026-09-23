// Retry-letter routing through producer receipts and source acknowledgments.

///|
async test "reconsume_later publishes retry then dead letter before ack" {
  @async.with_task_group(async fn(group) {
    let source = "persistent://public/default/source"
    let retry_topic = "persistent://public/default/retry"
    let dead_letter_topic = "persistent://public/default/dlq"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      expect_producer(peer)
      expect_subscribe(peer)
      let (first, first_meta, first_payload) = message_frame(
        5UL,
        1,
        1,
        b"first",
        test_metadata(),
      )
      peer.send_message(first, first_meta, first_payload)
      let retried = peer.next()
      guard retried.cmd.send is Some(retry_send) &&
        retried.metadata is Some(retry_meta) else {
        fail("expected retry SEND")
      }
      guard retry_meta.deliver_at_time is Some(deliver_at) else {
        fail("retry SEND missing delivery time")
      }
      assert_true(deliver_at >= @async.now() + 800L)
      assert_eq(retry_meta.properties.length(), 6)
      send_receipt(peer, retry_send.producer_id, retry_send.sequence_id)
      let first_ack = peer.next()
      guard first_ack.cmd.ack is Some(ack) else { fail("expected first ACK") }
      assert_eq(ack.message_id[0].entry_id, 1)
      let second_meta = test_metadata()
      let count = @proto.KeyValue::default()
      count.key = "RECONSUMETIMES"
      count.value = "1"
      second_meta.properties.push(count)
      let (second, metadata, payload) = message_frame(
        5UL, 1, 2, b"second", second_meta,
      )
      peer.send_message(second, metadata, payload)
      let dead_lettered = peer.next()
      guard dead_lettered.cmd.send is Some(dlq_send) &&
        dead_lettered.metadata is Some(dlq_meta) else {
        fail("expected dead-letter SEND")
      }
      assert_eq(dlq_meta.deliver_at_time, None)
      let mut attempts = ""
      for item in dlq_meta.properties {
        if item.key == "RECONSUMETIMES" {
          attempts = item.value
        }
      }
      assert_eq(attempts, "2")
      send_receipt(peer, dlq_send.producer_id, dlq_send.sequence_id)
      let second_ack = peer.next()
      guard second_ack.cmd.ack is Some(ack) else { fail("expected second ACK") }
      assert_eq(ack.message_id[0].entry_id, 2)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let retry_producer = Producer::single(
      SingleProducer::create(group, conn, retry_topic),
    )
    let dlq_producer = Producer::single(
      SingleProducer::create(group, conn, dead_letter_topic),
    )
    let consumer = Consumer::single(
      SingleConsumer::create(group, conn, source, "sub"),
    )
    consumer.set_dead_letter(
      DeadLetterPolicy::new(1U, dead_letter_topic),
      dlq_producer,
      group,
    )
    consumer.set_retry_letter(retry_topic, retry_producer)
    let first = consumer.receive()
    consumer.reconsume_later(first, delay_ms=1000, custom_properties={
      "custom": "value",
    })
    let second = consumer.receive()
    consumer.reconsume_later(second)
    consumer.close()
    manager.close()
  })
}

///|
async test "reconsume_later send error nacks source" {
  @async.with_task_group(async fn(group) {
    let source = "persistent://public/default/source"
    let retry_topic = "persistent://public/default/retry"
    let dead_letter_topic = "persistent://public/default/dlq"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      expect_producer(peer)
      expect_subscribe(peer)
      let (frame, meta, payload) = message_frame(
        5UL,
        1,
        1,
        b"failed",
        test_metadata(),
      )
      peer.send_message(frame, meta, payload)
      let sent = peer.next()
      guard sent.cmd.send is Some(send) else { fail("expected retry SEND") }
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
        fail("expected source NACK")
      }
      assert_eq(req.message_ids[0].entry_id, 1)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let retry_producer = Producer::single(
      SingleProducer::create(group, conn, retry_topic),
    )
    let dlq_producer = Producer::single(
      SingleProducer::create(group, conn, dead_letter_topic),
    )
    let consumer = Consumer::single(
      SingleConsumer::create(group, conn, source, "sub"),
    )
    consumer.set_dead_letter(
      DeadLetterPolicy::new(1U, dead_letter_topic),
      dlq_producer,
      group,
    )
    consumer.set_retry_letter(retry_topic, retry_producer)
    let message = consumer.receive()
    let result : Result[Unit, Error] = Ok(consumer.reconsume_later(message)) catch {
      e => Err(e)
    }
    assert_true(result is Err(_))
    consumer.close()
    manager.close()
  })
}
