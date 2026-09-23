// Negative-acknowledgment backoff follows delivery count and consumer lifetime.

///|
test "negative ack delay doubles and stops at its cap" {
  assert_eq(NackBackoff::default().delay_ms(0U), 1000)
  assert_eq(NackBackoff::default().delay_ms(20U), 600000)
  let policy = NackBackoff::new(20, 100)
  assert_eq(policy.delay_ms(0U), 20)
  assert_eq(policy.delay_ms(1U), 40)
  assert_eq(policy.delay_ms(2U), 80)
  assert_eq(policy.delay_ms(3U), 100)
  assert_eq(policy.delay_ms(4294967295U), 100)
  let invalid : Result[NackBackoff, Error] = Ok(NackBackoff::new(0, 100)) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
}

///|
async test "message nack schedules redelivery after the configured delay" {
  @async.with_task_group(async fn(group) {
    let redelivered : @aqueue.Queue[Int64] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        1,
        2,
        b"retry",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let frame = peer.next()
      assert_true(frame.cmd.redeliver_unacknowledged_messages is Some(_))
      redelivered.put(@async.now())
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/backoff",
      "backoff-sub",
      nack_backoff=NackBackoff::new(40, 100),
    )
    let message = consumer.receive()
    let started = @async.now()
    message.nack()
    @async.sleep(10)
    assert_true((redelivered.try_get() catch { _ => None }) is None)
    let finished = @async.with_timeout(2000, async fn() { redelivered.get() })
    assert_true(finished - started >= 35L)
    consumer.close()
    manager.close()
  })
}

///|
async test "closing a consumer cancels a delayed nack" {
  @async.with_task_group(async fn(group) {
    let checked : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        1,
        2,
        b"retry",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let frame = peer.next()
      guard frame.cmd.close_consumer is Some(close) else {
        fail("expected CLOSE_CONSUMER before delayed redelivery")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = close.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      @async.sleep(80)
      checked.put((peer.received.try_get() catch { _ => None }) is None)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/backoff",
      "backoff-sub",
      nack_backoff=NackBackoff::new(40, 100),
    )
    let message = consumer.receive()
    message.nack()
    consumer.close()
    assert_true(@async.with_timeout(2000, async fn() { checked.get() }))
    manager.close()
  })
}
