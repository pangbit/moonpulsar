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
