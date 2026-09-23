// TableView reconstructs state from a compacted reader and follows updates.

///|
async test "table view replays keys and follows updates and tombstones" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/table-view"
    let proceed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else {
        fail("expected reader SUBSCRIBE")
      }
      assert_eq(sub.topic, topic)
      assert_eq(sub.durable, Some(false))
      assert_eq(sub.read_compacted, Some(true))
      assert_eq(
        sub.initial_position,
        Some(@proto.CommandSubscribe_InitialPosition::Earliest),
      )
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let initial = [("a", b"old-a"), ("b", b"old-b")]
      for i, entry in initial {
        let (key, value) = entry
        let metadata = test_metadata()
        metadata.partition_key = Some(key)
        let (cmd, meta, body) = message_frame(
          sub.consumer_id,
          1,
          (i + 1).to_uint64(),
          value,
          metadata,
        )
        peer.send_message(cmd, meta, body)
      }
      let end = @proto.BaseCommand::default()
      end.type_ = @proto.BaseCommand_Type::REACHED_END_OF_TOPIC
      let reached = @proto.CommandReachedEndOfTopic::default()
      reached.consumer_id = sub.consumer_id
      end.reached_end_of_topic = Some(reached)
      peer.send_command(end)
      let mut ready = false
      while !ready {
        if (proceed.try_get() catch { _ => None }) is Some(_) {
          ready = true
        } else {
          match (peer.received.try_get() catch { _ => None }) {
            Some(frame) =>
              if frame.cmd.get_last_message_id is Some(req) {
                let reply = @proto.BaseCommand::default()
                reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
                let last = @proto.CommandGetLastMessageIdResponse::default()
                last.request_id = req.request_id
                let id = @proto.MessageIdData::default()
                id.ledger_id = 1
                id.entry_id = 2
                last.last_message_id = id
                reply.get_last_message_id_response = Some(last)
                peer.send_command(reply)
              }
            None => @async.sleep(1)
          }
        }
      }
      let updates = [("a", b"new-a"), ("b", b"")]
      for i, entry in updates {
        let (key, value) = entry
        let metadata = test_metadata()
        metadata.partition_key = Some(key)
        let (cmd, meta, body) = message_frame(
          sub.consumer_id,
          1,
          (i + 3).to_uint64(),
          value,
          metadata,
        )
        peer.send_message(cmd, meta, body)
      }
      let (cmd, meta, body) = message_frame(
        sub.consumer_id,
        1,
        5,
        b"unkeyed",
        test_metadata(),
      )
      peer.send_message(cmd, meta, body)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let view = @async.with_timeout(3000, async fn() {
      client.create_table_view(topic)
    })
    assert_eq(view.size(), 2)
    assert_eq(view.get("a"), Some(b"old-a"))
    assert_true(view.contains_key("b"))
    let copy = view.entries()
    copy["other"] = b"outside"
    assert_true(!view.contains_key("other"))
    let calls : Ref[Int] = { val: 0, }
    view.for_each_and_listen(fn(_key, _value) { calls.val += 1 })
    assert_eq(calls.val, 2)
    proceed.put(true)
    @async.with_timeout(3000, async fn() {
      while view.get("a") != Some(b"new-a") || view.contains_key("b") {
        @async.sleep(1)
      }
    })
    assert_eq(view.size(), 1)
    assert_eq(view.keys(), ["a"])
    assert_eq(calls.val, 4)
    view.close()
    client.close()
  })
}

///|
async test "typed table view decodes values and reports malformed live updates" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/typed-table"
    let codec = SchemaCodec::int32()
    let checked : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let proceed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else {
        fail("expected typed reader SUBSCRIBE")
      }
      checked.put(
        sub.schema is Some(schema) && schema.type_ == @proto.Schema_Type::Int32,
      )
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let initial = test_metadata()
      initial.partition_key = Some("key")
      let (cmd, meta, body) = message_frame(
        sub.consumer_id,
        1,
        1,
        codec.encode(7),
        initial,
      )
      peer.send_message(cmd, meta, body)
      let end = @proto.BaseCommand::default()
      end.type_ = @proto.BaseCommand_Type::REACHED_END_OF_TOPIC
      let reached = @proto.CommandReachedEndOfTopic::default()
      reached.consumer_id = sub.consumer_id
      end.reached_end_of_topic = Some(reached)
      peer.send_command(end)
      let mut ready = false
      while !ready {
        if (proceed.try_get() catch { _ => None }) is Some(_) {
          ready = true
        } else {
          match (peer.received.try_get() catch { _ => None }) {
            Some(frame) =>
              if frame.cmd.get_last_message_id is Some(req) {
                let reply = @proto.BaseCommand::default()
                reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
                let response = @proto.CommandGetLastMessageIdResponse::default()
                response.request_id = req.request_id
                let id = @proto.MessageIdData::default()
                id.ledger_id = 1
                id.entry_id = 1
                response.last_message_id = id
                reply.get_last_message_id_response = Some(response)
                peer.send_command(reply)
              }
            None => @async.sleep(1)
          }
        }
      }
      for index, value in [codec.encode(8), b"bad", b""] {
        let metadata = test_metadata()
        metadata.partition_key = Some("key")
        let (cmd, meta, body) = message_frame(
          sub.consumer_id,
          1,
          (index + 2).to_uint64(),
          value,
          metadata,
        )
        peer.send_message(cmd, meta, body)
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let view = @async.with_timeout(3000, async fn() {
      client.create_typed_table_view(topic, codec)
    })
    assert_true(checked.get())
    assert_eq(view.get("key"), Some(7))
    assert_eq(view.entries().get("key"), Some(7))
    let updates : @aqueue.Queue[Result[Int?, PulsarError]] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    view.for_each_and_listen(fn(_key, value) {
      ignore(updates.try_put(value) catch { _ => false })
    })
    assert_true(updates.get() is Ok(Some(7)))
    proceed.put(true)
    assert_true(
      @async.with_timeout(3000, async fn() { updates.get() }) is Ok(Some(8)),
    )
    assert_true(
      @async.with_timeout(3000, async fn() { updates.get() })
      is Err(InvalidFrame(_)),
    )
    assert_true(
      @async.with_timeout(3000, async fn() { updates.get() }) is Ok(None),
    )
    assert_true(!view.contains_key("key"))
    view.close()
    client.close()
  })
}

///|
async test "typed table view reports broker schema incompatibility" {
  @async.with_task_group(async fn(group) {
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) && sub.schema is Some(_) else {
        fail("expected typed SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::ERROR
      let error = @proto.CommandError::default()
      error.request_id = sub.request_id
      error.error = @proto.ServerError::IncompatibleSchema
      error.message = "incompatible table schema"
      reply.error = Some(error)
      peer.send_command(reply)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let result : Result[TypedTableView[Int], Error] = Ok(
      client.create_typed_table_view(
        "persistent://public/default/incompatible-table",
        SchemaCodec::int32(),
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(
      result is Err(ServerError(message="incompatible table schema", ..)),
    )
    client.close()
  })
}

///|
async test "typed table view rejects a malformed initial snapshot" {
  @async.with_task_group(async fn(group) {
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else {
        fail("expected typed SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let metadata = test_metadata()
      metadata.partition_key = Some("bad")
      let (cmd, meta, body) = message_frame(
        sub.consumer_id,
        1,
        1,
        b"bad",
        metadata,
      )
      peer.send_message(cmd, meta, body)
      let end = @proto.BaseCommand::default()
      end.type_ = @proto.BaseCommand_Type::REACHED_END_OF_TOPIC
      let reached = @proto.CommandReachedEndOfTopic::default()
      reached.consumer_id = sub.consumer_id
      end.reached_end_of_topic = Some(reached)
      peer.send_command(end)
      let mut closed = false
      while !closed {
        let frame = peer.next()
        if frame.cmd.get_last_message_id is Some(req) {
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
          let response = @proto.CommandGetLastMessageIdResponse::default()
          response.request_id = req.request_id
          let id = @proto.MessageIdData::default()
          id.ledger_id = 1
          id.entry_id = 1
          response.last_message_id = id
          reply.get_last_message_id_response = Some(response)
          peer.send_command(reply)
        } else if frame.cmd.close_consumer is Some(req) {
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = req.request_id
          reply.success = Some(success)
          peer.send_command(reply)
          closed = true
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let result : Result[TypedTableView[Int], Error] = Ok(
      @async.with_timeout(3000, async fn() {
        client.create_typed_table_view(
          "persistent://public/default/malformed-table",
          SchemaCodec::int32(),
        )
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(InvalidFrame(_)))
    client.close()
  })
}

///|
async test "table view of an empty topic is ready without an end event" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/empty-table-view"
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else {
        fail("expected reader SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let frame = peer.next()
      guard frame.cmd.get_last_message_id is Some(req) else {
        fail("expected last-message query")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
      let response = @proto.CommandGetLastMessageIdResponse::default()
      response.request_id = req.request_id
      let no_message = MessageId::earliest().to_proto()
      no_message.partition = Some(0)
      response.last_message_id = no_message
      reply.get_last_message_id_response = Some(response)
      peer.send_command(reply)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let view = @async.with_timeout(3000, async fn() {
      client.create_table_view(topic)
    })
    assert_true(view.is_empty())
    view.close()
    client.close()
  })
}

///|
async test "table view fails when initial last-message query fails" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/table-query-error"
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else {
        fail("expected reader SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let frame = peer.next()
      guard frame.cmd.get_last_message_id is Some(req) else {
        fail("expected last-message query")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::ERROR
      let error = @proto.CommandError::default()
      error.request_id = req.request_id
      error.error = @proto.ServerError::MetadataError
      error.message = "last-message query failed"
      reply.error = Some(error)
      peer.send_command(reply)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let result : Result[TableView, Error] = Ok(
      @async.with_timeout(3000, async fn() { client.create_table_view(topic) }),
    ) catch {
      e => Err(e)
    }
    assert_true(
      result is Err(ServerError(message="last-message query failed", ..)),
    )
    client.close()
  })
}
