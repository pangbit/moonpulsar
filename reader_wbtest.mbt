// Reader tests: non-durable subscription, end-of-topic, seek.

///|
async test "reader reads until end of topic" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      // readers use non-durable subscriptions and carry a start position
      assert_eq(req.durable, Some(false))
      assert_true(req.start_message_id is Some(_))
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
async test "reader seek drops buffered messages" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      // push a stale message, then answer the SEEK
      let (cmd0, meta0, p0) = message_frame(1UL, 1, 1, b"old", test_metadata())
      peer.send_message(cmd0, meta0, p0)
      let frame = peer.next()
      guard frame.cmd.seek is Some(req) else { fail("expected SEEK") }
      guard req.message_id is Some(mid) else { fail("SEEK without position") }
      assert_eq(mid.ledger_id, 100)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      // after the seek the broker resends flow-controlled data from 100:0
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else { fail("expected FLOW after SEEK") }
      let (cmd1, meta1, p1) = message_frame(
        1UL,
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
    reader.seek(target)
    let message = @async.with_timeout(5000, async fn() { reader.next() })
    assert_eq(message.map(fn(m) { m.payload() }), Some(b"new"))
    reader.close()
    manager.close()
  })
}
