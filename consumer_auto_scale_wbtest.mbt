// A full receive queue followed by an empty read doubles broker prefetch.

///|
async test "auto-scaled consumer grows FLOW from one to its configured maximum" {
  @async.with_task_group(async fn(group) {
    let ready : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let subscribe = peer.next()
      guard subscribe.cmd.subscribe is Some(request) else {
        fail("expected SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = request.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let first_flow = peer.next()
      guard first_flow.cmd.flow is Some(flow) else { fail("expected FLOW") }
      assert_eq(flow.message_permits, 1U)
      let (cmd, metadata, payload) = message_frame(
        request.consumer_id,
        1UL,
        1UL,
        b"one",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let refill = peer.next()
      guard refill.cmd.flow is Some(flow) else { fail("expected refill") }
      assert_eq(flow.message_permits, 1U)
      ready.put(())
      release.get()
      let (cmd, metadata, payload) = message_frame(
        request.consumer_id,
        1UL,
        2UL,
        b"two",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let grown = peer.next()
      guard grown.cmd.flow is Some(flow) else { fail("expected grown FLOW") }
      assert_eq(flow.message_permits, 2U)
      let recovered = peer.next()
      guard recovered.cmd.flow is Some(flow) else {
        fail("expected FLOW after reset")
      }
      assert_eq(flow.message_permits, 2U)
    })
    let manager = ConnectionManager::new(group)
    let consumer = SingleConsumer::create(
      group,
      manager.get(addr),
      "persistent://public/default/auto-scale",
      "sub",
      receiver_queue_size=2,
      auto_scaled_receiver_queue=true,
    )
    assert_eq(consumer.receive().payload(), b"one")
    ready.get()
    let second : @aqueue.Queue[Message] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    group.spawn_bg(
      async fn() { second.put(consumer.receive()) },
      no_wait=true,
      allow_failure=true,
    )
    @async.with_timeout(1000, async fn() {
      while consumer.current_receiver_permits != 2 {
        @async.sleep(1)
      }
    })
    release.put(())
    assert_eq(
      @async.with_timeout(1000, async fn() { second.get() }).payload(),
      b"two",
    )
    assert_eq(consumer.current_receiver_permits, 2)
    assert_true(consumer.try_receive() is None)
    assert_eq(consumer.current_receiver_permits, 2)
    consumer.reset_flow()
    consumer.close()
    manager.close()
  })
}

///|
async test "auto-scaled consumer rejects a zero maximum" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, fn(_addr, _peer) { () })
    let manager = ConnectionManager::new(group)
    let invalid : Result[SingleConsumer, Error] = Ok(
      SingleConsumer::create(
        group,
        manager.get(addr),
        "persistent://public/default/auto-scale-zero",
        "sub",
        receiver_queue_size=0,
        auto_scaled_receiver_queue=true,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(invalid is Err(InvalidFrame(_)))
    manager.close()
  })
}
