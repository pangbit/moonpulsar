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
      response.last_message_id = MessageId::earliest().to_proto()
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
