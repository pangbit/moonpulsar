// Reader tests: non-durable subscription, end-of-topic, seek.

///|
async test "reader reads until end of topic" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      assert_eq(
        req.initial_position,
        Some(@proto.CommandSubscribe_InitialPosition::Latest),
      )
      // readers use non-durable subscriptions
      assert_eq(req.durable, Some(false))
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else { fail("expected FLOW") }
      // push two messages, then signal end of topic
      let (cmd0, meta0, p0) = message_frame(1UL, 1, 1, b"r0", test_metadata())
      peer.send_message(cmd0, meta0, p0)
      let (cmd1, meta1, p1) = message_frame(1UL, 1, 2, b"r1", test_metadata())
      peer.send_message(cmd1, meta1, p1)
      let end = @proto.BaseCommand::default()
      end.type_ = @proto.BaseCommand_Type::REACHED_END_OF_TOPIC
      let e = @proto.CommandReachedEndOfTopic::default()
      e.consumer_id = 1
      end.reached_end_of_topic = Some(e)
      peer.send_command(end)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/t",
    )
    assert_eq(reader.next().map(fn(m) { m.payload() }), Some(b"r0"))
    assert_eq(reader.next().map(fn(m) { m.payload() }), Some(b"r1"))
    assert_true(reader.next() is None)
    reader.close()
    manager.close()
  })
}

///|
async test "reader has_message_available via last message id" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let frame = peer.next()
      guard frame.cmd.get_last_message_id is Some(req) else {
        fail("expected GET_LAST_MESSAGE_ID")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
      let resp = @proto.CommandGetLastMessageIdResponse::default()
      resp.request_id = req.request_id
      let id = @proto.MessageIdData::default()
      id.ledger_id = 9
      id.entry_id = 9
      resp.last_message_id = id
      reply.get_last_message_id_response = Some(resp)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/t",
    )
    // last seen = latest position marker; broker reports 9:9 -> available
    assert_true(reader.has_message_available())
    reader.close()
    manager.close()
  })
}

///|
async test "reader seek by message id re-subscribes at the position" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      // push a stale message, then the reader seeks back to 100:0
      let (cmd0, meta0, p0) = message_frame(1UL, 1, 1, b"old", test_metadata())
      peer.send_message(cmd0, meta0, p0)
      // expect CLOSE_CONSUMER, answer it
      let frame = peer.next()
      guard frame.cmd.close_consumer is Some(c) else {
        fail("expected CLOSE_CONSUMER before re-subscribe")
      }
      let close_reply = @proto.BaseCommand::default()
      close_reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let cs = @proto.CommandSuccess::default()
      cs.request_id = c.request_id
      close_reply.success = Some(cs)
      peer.send_command(close_reply)
      // expect a fresh SUBSCRIBE at the seek position
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else {
        fail("expected re-SUBSCRIBE")
      }
      guard req.start_message_id is Some(mid) else {
        fail("re-SUBSCRIBE without start position")
      }
      assert_eq(mid.ledger_id, 100)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else {
        fail("expected FLOW after re-SUBSCRIBE")
      }
      let (cmd1, meta1, p1) = message_frame(
        req.consumer_id,
        100,
        0,
        b"new",
        test_metadata(),
      )
      peer.send_message(cmd1, meta1, p1)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/t",
    )
    let target : MessageId = {
      ledger_id: 100,
      entry_id: 0,
      batch_index: -1,
      partition: -1,
    }
    reader.seek(PositionId(target))
    let message = @async.with_timeout(5000, async fn() { reader.next() })
    assert_eq(message.map(fn(m) { m.payload() }), Some(b"new"))
    reader.close()
    manager.close()
  })
}

///|
async test "reader timestamp seek resets cursor and re-subscribes" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let frame = peer.next()
      guard frame.cmd.seek is Some(req) else { fail("expected SEEK") }
      assert_true(req.message_publish_time is Some(_))
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      // broker disconnects the consumer; the reader closes and re-subscribes
      let frame = peer.next()
      guard frame.cmd.close_consumer is Some(c) else {
        fail("expected CLOSE_CONSUMER after SEEK")
      }
      let close_reply = @proto.BaseCommand::default()
      close_reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let cs = @proto.CommandSuccess::default()
      cs.request_id = c.request_id
      close_reply.success = Some(cs)
      peer.send_command(close_reply)
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else {
        fail("expected re-SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else { fail("expected FLOW") }
      let (cmd1, meta1, p1) = message_frame(
        req.consumer_id,
        9,
        0,
        b"after-seek",
        test_metadata(),
      )
      peer.send_message(cmd1, meta1, p1)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/t",
    )
    reader.seek(PositionTimestamp(12345))
    let message = @async.with_timeout(5000, async fn() { reader.next() })
    assert_eq(message.map(fn(m) { m.payload() }), Some(b"after-seek"))
    reader.close()
    manager.close()
  })
}

///|
async test "partitioned reader resumes after end of topic and seek" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (old_cmd, old_meta, old_payload) = message_frame(
        1UL,
        1,
        1,
        b"old",
        test_metadata(),
      )
      peer.send_message(old_cmd, old_meta, old_payload)
      let end = @proto.BaseCommand::default()
      end.type_ = @proto.BaseCommand_Type::REACHED_END_OF_TOPIC
      let reached = @proto.CommandReachedEndOfTopic::default()
      reached.consumer_id = 1
      end.reached_end_of_topic = Some(reached)
      peer.send_command(end)
      let close_frame = peer.next()
      guard close_frame.cmd.close_consumer is Some(close) else {
        fail("expected CLOSE_CONSUMER")
      }
      let close_reply = @proto.BaseCommand::default()
      close_reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let close_success = @proto.CommandSuccess::default()
      close_success.request_id = close.request_id
      close_reply.success = Some(close_success)
      peer.send_command(close_reply)
      let subscribe_frame = peer.next()
      guard subscribe_frame.cmd.subscribe is Some(req) else {
        fail("expected re-SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let (new_cmd, new_meta, new_payload) = message_frame(
        req.consumer_id,
        2,
        1,
        b"new",
        test_metadata(),
      )
      peer.send_message(new_cmd, new_meta, new_payload)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let single = SingleReader::create(
      group, conn, "persistent://public/default/t",
    )
    let reader = Reader::partitioned(group, [single])
    assert_eq(reader.next().map(fn(m) { m.payload() }), Some(b"old"))
    assert_true(reader.next() is None)
    let target : MessageId = {
      ledger_id: 2,
      entry_id: 0,
      batch_index: -1,
      partition: -1,
    }
    reader.seek(PositionId(target))
    let message = @async.with_timeout(5000, async fn() { reader.next() })
    assert_eq(message.map(fn(m) { m.payload() }), Some(b"new"))
    reader.close()
    manager.close()
  })
}

///|
async test "partitioned reader tracks end of topic per connection" {
  @async.with_task_group(async fn(group) {
    let on_peer = async fn(_addr : String, peer : MockPeer) -> Unit {
      expect_subscribe(peer)
      let end = @proto.BaseCommand::default()
      end.type_ = @proto.BaseCommand_Type::REACHED_END_OF_TOPIC
      let reached = @proto.CommandReachedEndOfTopic::default()
      reached.consumer_id = 1
      end.reached_end_of_topic = Some(reached)
      peer.send_command(end)
    }
    let addr0 = start_mock_broker(group, on_peer)
    let addr1 = start_mock_broker(group, on_peer)
    let manager = ConnectionManager::new(group)
    let first = SingleReader::create(
      group,
      manager.get(addr0),
      "persistent://public/default/t-partition-0",
    )
    let second = SingleReader::create(
      group,
      manager.get(addr1),
      "persistent://public/default/t-partition-1",
    )
    let reader = Reader::partitioned(group, [first, second])
    assert_true(@async.with_timeout(5000, async fn() { reader.next() }) is None)
    reader.close()
    manager.close()
  })
}
