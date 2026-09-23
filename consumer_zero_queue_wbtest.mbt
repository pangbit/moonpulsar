// A zero-sized receive queue requests one broker entry when needed.

///|
async test "zero receive queue sends FLOW only for demand" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      @async.sleep(20)
      observed.put(peer.received.try_get() is None)
      let flow = peer.next()
      guard flow.cmd.flow is Some(request) else { fail("expected FLOW") }
      observed.put(request.message_permits == 1U)
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        b"one",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      @async.sleep(20)
      observed.put(peer.received.try_get() is None)
      let next_flow = peer.next()
      guard next_flow.cmd.flow is Some(request) else {
        fail("expected second FLOW")
      }
      observed.put(request.message_permits == 1U)
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1UL,
        2UL,
        b"two",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
    })
    let manager = ConnectionManager::new(group)
    let consumer = SingleConsumer::create(
      group,
      manager.get(addr),
      "persistent://public/default/zero",
      "sub",
      receiver_queue_size=0,
    )
    assert_true(observed.get())
    let first = @async.with_timeout(1000, async fn() { consumer.receive() })
    assert_eq(first.payload(), b"one")
    assert_true(observed.get())
    assert_true(observed.get())
    let second = @async.with_timeout(1000, async fn() { consumer.receive() })
    assert_eq(second.payload(), b"two")
    assert_true(observed.get())
    consumer.close()
    manager.close()
  })
}

///|
async test "zero receive queue try_receive starts a single demand" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(request) else { fail("expected FLOW") }
      assert_eq(request.message_permits, 1U)
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        b"one",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
    })
    let manager = ConnectionManager::new(group)
    let consumer = SingleConsumer::create(
      group,
      manager.get(addr),
      "persistent://public/default/zero-try",
      "sub",
      receiver_queue_size=0,
    )
    assert_true(consumer.try_receive() is None)
    let message = @async.with_timeout(1000, async fn() {
      while true {
        if consumer.try_receive() is Some(message) {
          return message
        }
        @async.sleep(5)
      }
      raise ConsumerClosed
    })
    assert_eq(message.payload(), b"one")
    consumer.close()
    manager.close()
  })
}
