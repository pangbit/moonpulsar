// Reader tests: non-durable subscription, end-of-topic, seek.

///|
async test "reader name and properties are sent in SUBSCRIBE" {
  @async.with_task_group(async fn(group) {
    let checked : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      checked.put(
        req.subscription == "custom-reader" &&
        req.consumer_name == Some("reader-name") &&
        properties_of(req.metadata).get("team") == Some("moonbit"),
      )
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      ignore(peer.next())
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/t",
      name="reader-name",
      properties={ "team": "moonbit" },
      subscription_name="custom-reader",
    )
    assert_true(checked.get())
    reader.close()
    manager.close()
  })
}

///|
async test "reader rejects invalid name and malformed subscribe reply" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
      let result = @proto.CommandPartitionedTopicMetadataResponse::default()
      result.request_id = req.request_id
      reply.partition_metadata_response = Some(result)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let invalid : Result[SingleReader, Error] = Ok(
      SingleReader::create(
        group,
        conn,
        "persistent://public/default/t",
        subscription_name="",
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(invalid is Err(InvalidFrame(_)))
    assert_true(conn.message_handlers.is_empty())
    let malformed : Result[SingleReader, Error] = Ok(
      @async.with_timeout(1000, async fn() {
        SingleReader::create(group, conn, "persistent://public/default/t")
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(malformed is Err(UnexpectedResponse(_)))
    assert_true(conn.message_handlers.is_empty())
    manager.close()
  })
}

///|
async test "inclusive latest reader closes after malformed last-id reply" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let frame = peer.next()
      guard frame.cmd.get_last_message_id is Some(req) else {
        fail("expected GET_LAST_MESSAGE_ID")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let malformed : Result[SingleReader, Error] = Ok(
      @async.with_timeout(1000, async fn() {
        SingleReader::create(
          group,
          conn,
          "persistent://public/default/latest-error",
          start_message_id=MessageId::latest(),
          include_start_message_id=true,
        )
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(malformed is Err(UnexpectedResponse(_)))
    assert_true(conn.message_handlers.is_empty())
    manager.close()
  })
}

///|
async fn check_reader_start_inclusion(
  inclusive : Bool,
  expected_entry : UInt64,
) -> Unit {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      guard req.start_message_id is Some(start) else {
        fail("missing start message id")
      }
      assert_eq(start.entry_id, 1UL)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else { fail("expected FLOW") }
      for entry in 1UL..<=2UL {
        let (cmd, meta, payload) = message_frame(
          req.consumer_id,
          9UL,
          entry,
          @utf8.encode("entry-\{entry}"),
          test_metadata(),
        )
        peer.send_message(cmd, meta, payload)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let start : MessageId = {
      ledger_id: 9UL,
      entry_id: 1UL,
      batch_index: -1,
      partition: -1,
    }
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/start-inclusive",
      start_message_id=start,
      include_start_message_id=inclusive,
    )
    let message = @async.with_timeout(3000, async fn() { reader.next() })
    guard message is Some(found) else { fail("reader reached end") }
    assert_eq(found.id().entry_id, expected_entry)
    reader.close()
    manager.close()
  })
}

///|
async test "reader includes or excludes its starting message id" {
  check_reader_start_inclusion(false, 2UL)
  check_reader_start_inclusion(true, 1UL)
}

///|
async test "inclusive latest reader leaves an empty topic at its end" {
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
      let empty = @proto.MessageIdData::default()
      empty.ledger_id = 18446744073709551615
      empty.entry_id = 18446744073709551615
      resp.last_message_id = empty
      reply.get_last_message_id_response = Some(resp)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/empty-latest",
      start_message_id=MessageId::latest(),
      include_start_message_id=true,
    )
    assert_true(reader.start_bound.val is None)
    reader.close()
    manager.close()
  })
}

///|
async test "inclusive latest reader replays the current last message" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let frame = peer.next()
      guard frame.cmd.get_last_message_id is Some(req) else {
        fail("expected GET_LAST_MESSAGE_ID")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
      let last = @proto.CommandGetLastMessageIdResponse::default()
      last.request_id = req.request_id
      let id = @proto.MessageIdData::default()
      id.ledger_id = 9UL
      id.entry_id = 2UL
      last.last_message_id = id
      reply.get_last_message_id_response = Some(last)
      peer.send_command(reply)
      let close = peer.next()
      guard close.cmd.close_consumer is Some(closing) else {
        fail("expected reader cursor reset")
      }
      let closed = @proto.BaseCommand::default()
      closed.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = closing.request_id
      closed.success = Some(success)
      peer.send_command(closed)
      let subscribe = peer.next()
      guard subscribe.cmd.subscribe is Some(sub) &&
        sub.start_message_id is Some(start) else {
        fail("expected reader re-SUBSCRIBE from last message")
      }
      assert_eq(start.ledger_id, 9UL)
      assert_eq(start.entry_id, 2UL)
      let subscribed = @proto.BaseCommand::default()
      subscribed.type_ = @proto.BaseCommand_Type::SUCCESS
      let ok = @proto.CommandSuccess::default()
      ok.request_id = sub.request_id
      subscribed.success = Some(ok)
      peer.send_command(subscribed)
      let flow = peer.next()
      assert_true(flow.cmd.flow is Some(_))
      let (message, metadata, payload) = message_frame(
        sub.consumer_id,
        9UL,
        2UL,
        b"last message",
        test_metadata(),
      )
      peer.send_message(message, metadata, payload)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/latest-nonempty",
      start_message_id=MessageId::latest(),
      include_start_message_id=true,
    )
    let message = @async.with_timeout(1000, async fn() { reader.next() })
    guard message is Some(found) else { fail("expected last message") }
    assert_eq(found.payload(), b"last message")
    reader.close()
    manager.close()
  })
}

///|
async test "single reader wakes next on broker disconnect" {
  @async.with_task_group(async fn(group) {
    let disconnect : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      ignore(disconnect.get())
      peer.tcp.close()
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/reader-disconnect",
    )
    disconnect.put(true)
    let result : Result[Message?, Error] = Ok(
      @async.with_timeout(3000, async fn() { reader.next() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectionClosed))
    assert_true(reader.closed.val)
    manager.close()
  })
}

///|
async test "partitioned reader wakes next when a source disconnects" {
  @async.with_task_group(async fn(group) {
    let disconnect : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      ignore(disconnect.get())
      peer.tcp.close()
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let source = SingleReader::create(
      group, conn, "persistent://public/default/reader-disconnect-partition-0",
    )
    let reader = Reader::partitioned(group, [source])
    disconnect.put(true)
    let result : Result[Message?, Error] = Ok(
      @async.with_timeout(3000, async fn() { reader.next() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectionClosed))
    reader.close()
    manager.close()
  })
}

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
      for _ in 0..<4 {
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
        id.batch_index = Some(2)
        resp.last_message_id = id
        reply.get_last_message_id_response = Some(resp)
        peer.send_command(reply)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/t",
    )
    assert_eq(Reader::one(reader).last_message_id().entry_id, 9UL)
    // last seen = latest position marker; broker reports 9:9 -> available
    assert_true(reader.has_message_available())
    reader.last_seen.val = Some({
      ledger_id: 9,
      entry_id: 9,
      batch_index: 0,
      partition: -1,
    })
    assert_true(reader.has_message_available())
    reader.last_seen.val = Some({
      ledger_id: 9,
      entry_id: 9,
      batch_index: 2,
      partition: -1,
    })
    assert_true(!reader.has_message_available())
    reader.close()
    manager.close()
  })
}

///|
async test "partitioned reader returns last message id per topic" {
  @async.with_task_group(async fn(group) {
    let first = "persistent://public/default/last-partition-0"
    let second = "persistent://public/default/last-partition-1"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      expect_subscribe(peer)
      for entry in 1..<3 {
        let frame = peer.next()
        guard frame.cmd.get_last_message_id is Some(req) else {
          fail("expected GET_LAST_MESSAGE_ID")
        }
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
        let resp = @proto.CommandGetLastMessageIdResponse::default()
        resp.request_id = req.request_id
        let id = @proto.MessageIdData::default()
        id.ledger_id = 3
        id.entry_id = entry.reinterpret_as_uint().to_uint64()
        resp.last_message_id = id
        reply.get_last_message_id_response = Some(resp)
        peer.send_command(reply)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let r0 = SingleReader::create(group, conn, first)
    let r1 = SingleReader::create(group, conn, second)
    let reader = Reader::partitioned(group, [r0, r1])
    let ids = reader.last_message_ids()
    assert_eq(ids.get(first).map(fn(id) { id.entry_id }), Some(1UL))
    assert_eq(ids.get(second).map(fn(id) { id.entry_id }), Some(2UL))
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
      group,
      conn,
      "persistent://public/default/t",
      include_start_message_id=true,
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
