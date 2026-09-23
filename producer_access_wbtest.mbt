// Producer access modes and broker readiness are part of the PRODUCER handshake.

///|
async test "exclusive producer sends access mode and retains topic epoch" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.producer is Some(req) else { fail("expected PRODUCER") }
      assert_eq(
        req.producer_access_mode,
        Some(@proto.ProducerAccessMode::ExclusiveWithFencing),
      )
      assert_eq(req.topic_epoch, None)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "exclusive"
      success.topic_epoch = Some(42UL)
      reply.producer_success = Some(success)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/exclusive",
      access_mode=ExclusiveWithFencing,
    )
    assert_eq(producer.topic_epoch, Some(42UL))
    let command = producer.producer_command(conn.next_request_id())
    assert_eq(command.producer.unwrap().topic_epoch, Some(42UL))
    producer.close()
    manager.close()
  })
}

///|
async test "wait-for-exclusive producer waits for the ready response" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.producer is Some(req) else { fail("expected PRODUCER") }
      assert_eq(
        req.producer_access_mode,
        Some(@proto.ProducerAccessMode::WaitForExclusive),
      )
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "waiting"
      success.producer_ready = Some(false)
      reply.producer_success = Some(success)
      peer.send_command(reply)
      @async.sleep(70)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "ready"
      success.producer_ready = Some(true)
      reply.producer_success = Some(success)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let started = @async.now()
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/waiting",
      access_mode=WaitForExclusive,
    )
    assert_true(@async.now() - started >= 60L)
    assert_eq(producer.name(), "ready")
    producer.close()
    manager.close()
  })
}

///|
async test "canceling a wait-for-exclusive request releases its pending slot" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.producer is Some(req) else { fail("expected PRODUCER") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "waiting"
      success.producer_ready = Some(false)
      reply.producer_success = Some(success)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let timed_out : Result[SingleProducer, Error] = Ok(
      @async.with_timeout(50, async fn() {
        SingleProducer::create(
          group,
          conn,
          "persistent://public/default/canceled-wait",
          access_mode=WaitForExclusive,
        )
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(timed_out is Err(@async.TimeoutError))
    assert_true(conn.pending.is_empty())
    manager.close()
  })
}
