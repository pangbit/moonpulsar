// Consumer tests against the in-process mock broker.

///|
/// Mock broker side: expect SUBSCRIBE, answer SUCCESS, then skip the
/// initial FLOW command.
async fn expect_subscribe(peer : MockPeer) -> Unit {
  let frame = peer.next()
  guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
  let reply = @proto.BaseCommand::default()
  reply.type_ = @proto.BaseCommand_Type::SUCCESS
  let success = @proto.CommandSuccess::default()
  success.request_id = req.request_id
  reply.success = Some(success)
  peer.send_command(reply)
  let flow = peer.next()
  guard flow.cmd.flow is Some(_) else { fail("expected FLOW") }
}

///|
fn message_frame(
  consumer_id : UInt64,
  ledger_id : UInt64,
  entry_id : UInt64,
  payload : Bytes,
  metadata : @proto.MessageMetadata,
) -> (@proto.BaseCommand, @proto.MessageMetadata, Bytes) {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::MESSAGE
  let message = @proto.CommandMessage::default()
  message.consumer_id = consumer_id
  let id = @proto.MessageIdData::default()
  id.ledger_id = ledger_id
  id.entry_id = entry_id
  message.message_id = id
  cmd.message = Some(message)
  (cmd, metadata, payload)
}

///|
fn test_metadata(
  producer_name? : String = "mock-producer",
  sequence_id? : UInt64 = 0,
) -> @proto.MessageMetadata {
  let metadata = @proto.MessageMetadata::default()
  metadata.producer_name = producer_name
  metadata.sequence_id = sequence_id
  metadata.publish_time = 1000
  metadata
}

///|
async test "consumer receives message and acks individually" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let meta = test_metadata()
      meta.partition_key = Some("key1")
      let kv = @proto.KeyValue::default()
      kv.key = "foo"
      kv.value = "bar"
      meta.properties.push(kv)
      let (cmd, metadata, payload) = message_frame(1UL, 5, 6, b"hello", meta)
      peer.send_message(cmd, metadata, payload)
      let ack_frame = peer.next()
      guard ack_frame.cmd.ack is Some(ack) else { fail("expected ACK") }
      assert_eq(ack.ack_type, @proto.CommandAck_AckType::Individual)
      assert_eq(ack.message_id.length(), 1)
      assert_eq(ack.message_id[0].ledger_id, 5)
      assert_eq(ack.message_id[0].entry_id, 6)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/t", "sub-1",
    )
    // consumer id was allocated as second id on the connection
    let message = consumer.receive()
    assert_eq(message.payload(), b"hello")
    assert_eq(message.key(), Some("key1"))
    assert_eq(message.properties().get("foo"), Some("bar"))
    assert_eq(message.id().ledger_id, 5)
    message.ack()
    consumer.close()
    manager.close()
  })
}

///|
async test "try_receive replenishes flow permits" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        1,
        1,
        b"one",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let flow = peer.next()
      guard flow.cmd.flow is Some(req) else { fail("expected refill FLOW") }
      assert_eq(req.message_permits, 1)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/t",
      "sub-1",
      receiver_queue_size=2,
    )
    let received = @async.with_timeout(5000, async fn() {
      while true {
        if consumer.try_receive() is Some(message) {
          return message
        }
        @async.sleep(1)
      }
      fail("unreachable")
    })
    assert_eq(received.payload(), b"one")
    consumer.close()
    manager.close()
  })
}

///|
async test "consumer nack triggers redeliver command" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        7,
        8,
        b"x",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let nack_frame = peer.next()
      guard nack_frame.cmd.redeliver_unacknowledged_messages is Some(redeliver) else {
        fail("expected REDELIVER_UNACKNOWLEDGED_MESSAGES")
      }
      assert_eq(redeliver.message_ids.length(), 1)
      assert_eq(redeliver.message_ids[0].ledger_id, 7)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/t", "sub-1",
    )
    let message = consumer.receive()
    message.nack()
    consumer.close()
    manager.close()
  })
}

///|
async test "consumer subscription options are sent on the wire" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      assert_eq(req.priority_level, Some(2))
      assert_eq(req.durable, Some(false))
      assert_eq(req.read_compacted, Some(true))
      assert_eq(req.metadata.length(), 1)
      assert_eq(req.metadata[0].key, "consumer")
      assert_eq(req.metadata[0].value, "test")
      assert_eq(req.subscription_properties.length(), 1)
      assert_eq(req.subscription_properties[0].key, "subscription")
      assert_eq(req.subscription_properties[0].value, "test")
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/options",
      "options-sub",
      priority_level=2,
      durable=false,
      read_compacted=true,
      properties={ "consumer": "test" },
      subscription_properties={ "subscription": "test" },
    )
    consumer.close()
    manager.close()
  })
}

///|
async test "unsubscribe removes a subscription and closes the consumer" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let frame = peer.next()
      guard frame.cmd.unsubscribe is Some(req) else {
        fail("expected UNSUBSCRIBE")
      }
      assert_eq(req.consumer_id, 1UL)
      assert_eq(req.force, Some(true))
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/unsubscribe", "sub-1",
    )
    consumer.unsubscribe(force=true)
    assert_true(consumer.is_closed())
    manager.close()
  })
}

///|
async test "ack timeout redelivers only unacknowledged messages" {
  @async.with_task_group(async fn(group) {
    let redelivered : @aqueue.Queue[UInt64] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd_a, meta_a, body_a) = message_frame(
        1UL,
        7,
        1,
        b"ack",
        test_metadata(),
      )
      let (cmd_b, meta_b, body_b) = message_frame(
        1UL,
        7,
        2,
        b"timeout",
        test_metadata(),
      )
      peer.send_message(cmd_a, meta_a, body_a)
      peer.send_message(cmd_b, meta_b, body_b)
      assert_true(peer.next().cmd.ack is Some(_))
      let frame = peer.next()
      guard frame.cmd.redeliver_unacknowledged_messages is Some(req) else {
        fail("expected timeout redelivery")
      }
      assert_eq(req.message_ids.length(), 1)
      redelivered.put(req.message_ids[0].entry_id)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/t",
      "sub-1",
      ack_timeout_ms=50,
    )
    let first = consumer.receive()
    let second = consumer.receive()
    assert_eq(first.payload(), b"ack")
    assert_eq(second.payload(), b"timeout")
    first.ack()
    assert_eq(consumer.ack_pending.get(first.id()), None)
    assert_true(consumer.ack_pending.get(second.id()) is Some(_))
    assert_eq(@async.with_timeout(1000, async fn() { redelivered.get() }), 2UL)
    assert_eq(consumer.ack_pending.get(second.id()), None)
    consumer.close()
    manager.close()
  })
}

///|
async test "consumer splits batched messages" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let meta = test_metadata()
      meta.num_messages_in_batch = Some(2)
      let msgs = [
        ProducerMessage::new(b"part0", properties={ "i": "0" }),
        ProducerMessage::new(b"part1", properties={ "i": "1" }),
      ]
      let (cmd, metadata, payload) = message_frame(
        1UL,
        9,
        10,
        encode_batch_payload(msgs),
        meta,
      )
      peer.send_message(cmd, metadata, payload)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/t", "sub-1",
    )
    let m0 = consumer.receive()
    let m1 = consumer.receive()
    assert_eq(m0.payload(), b"part0")
    assert_eq(m1.payload(), b"part1")
    assert_eq(m0.id().batch_index, 0)
    assert_eq(m1.id().batch_index, 1)
    assert_eq(m1.properties().get("i"), Some("1"))
    m0.ack()
    m1.ack()
    consumer.close()
    manager.close()
  })
}

///|
async test "cumulative ack sends cumulative ack type" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        1,
        1,
        b"x",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let ack_frame = peer.next()
      guard ack_frame.cmd.ack is Some(ack) else { fail("expected ACK") }
      assert_eq(ack.ack_type, @proto.CommandAck_AckType::Cumulative)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/t", "sub-1",
    )
    let message = consumer.receive()
    consumer.ack_cumulative(message.id())
    consumer.close()
    manager.close()
  })
}

///|
async test "broker close consumer terminates receive" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let close = @proto.BaseCommand::default()
      close.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
      let c = @proto.CommandCloseConsumer::default()
      c.consumer_id = 1
      c.request_id = 99
      close.close_consumer = Some(c)
      peer.send_command(close)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/t", "sub-1",
    )
    let result : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(result is Err(_))
    assert_true(consumer.is_closed())
    manager.close()
  })
}

///|
async test "consumer seek drops buffered messages and re-subscribes" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (old_cmd, old_meta, old_body) = message_frame(
        1UL,
        1,
        1,
        b"stale",
        test_metadata(),
      )
      peer.send_message(old_cmd, old_meta, old_body)
      let frame = peer.next()
      guard frame.cmd.seek is Some(seek) else { fail("expected SEEK") }
      assert_eq(seek.message_publish_time, Some(12345UL))
      let success = @proto.BaseCommand::default()
      success.type_ = @proto.BaseCommand_Type::SUCCESS
      let response = @proto.CommandSuccess::default()
      response.request_id = seek.request_id
      success.success = Some(response)
      peer.send_command(success)
      let frame = peer.next()
      guard frame.cmd.close_consumer is Some(close) else {
        fail("expected CLOSE_CONSUMER after seek")
      }
      let close_success = @proto.BaseCommand::default()
      close_success.type_ = @proto.BaseCommand_Type::SUCCESS
      let close_response = @proto.CommandSuccess::default()
      close_response.request_id = close.request_id
      close_success.success = Some(close_response)
      peer.send_command(close_success)
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else {
        fail("expected re-SUBSCRIBE after seek")
      }
      assert_true(sub.consumer_id != close.consumer_id)
      let subscribed = @proto.BaseCommand::default()
      subscribed.type_ = @proto.BaseCommand_Type::SUCCESS
      let subscribed_response = @proto.CommandSuccess::default()
      subscribed_response.request_id = sub.request_id
      subscribed.success = Some(subscribed_response)
      peer.send_command(subscribed)
      assert_true(peer.next().cmd.flow is Some(_))
      let (new_cmd, new_meta, new_body) = message_frame(
        sub.consumer_id,
        2,
        1,
        b"fresh",
        test_metadata(),
      )
      peer.send_message(new_cmd, new_meta, new_body)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/seek", "seek-sub",
    )
    consumer.seek(PositionTimestamp(12345UL))
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"fresh")
    consumer.close()
    manager.close()
  })
}

///|
async test "broker CLOSE_CONSUMER re-subscribes on an open connection" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let close = @proto.BaseCommand::default()
      close.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
      let req = @proto.CommandCloseConsumer::default()
      req.consumer_id = 1UL
      req.request_id = 99UL
      close.close_consumer = Some(req)
      peer.send_command(close)
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        3,
        4,
        b"after-close",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/seek",
      "seek-sub",
      reconnect_with=fn() { conn },
    )
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"after-close")
    consumer.close()
    manager.close()
  })
}
