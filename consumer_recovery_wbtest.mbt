///|
async test "reconnect reserves FLOW capacity for retained messages" {
  @async.with_timeout(5000, async fn() {
    @async.with_task_group(async fn(group) {
      let peer_queue : @aqueue.Queue[MockPeer] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      let addr = start_mock_broker(group, async fn(_addr, peer) {
        expect_subscribe(peer)
        peer_queue.put(peer)
        // Subsequent SUBSCRIBE requests are real reconnect attempts.
        for _ in 0..<3 {
          let frame = peer.next()
          guard frame.cmd.subscribe is Some(req) else {
            fail("unexpected FLOW while buffer is full")
          }
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = req.request_id
          reply.success = Some(success)
          peer.send_command(reply)
        }
        let flow = peer.next()
        guard flow.cmd.flow is Some(req) else { fail("expected replenishment") }
        assert_eq(req.message_permits, 2U)
      })
      let manager = ConnectionManager::new(group)
      let conn = manager.get(addr)
      let budget = ClientMemoryLimiter::new(1048576)
      let consumer = SingleConsumer::create_with_memory(
        group,
        conn,
        "persistent://public/default/recovery-buffer",
        "sub",
        receiver_queue_size=4,
        memory_limiter=budget,
        reconnect_with=fn() { conn },
      )
      let peer = peer_queue.get()
      for entry in 0..<4 {
        let (cmd, meta, payload) = message_frame(
          consumer.consumer_id,
          1UL,
          entry.to_uint64(),
          Bytes::make(65536, entry.to_byte()),
          test_metadata(),
        )
        peer.send_message(cmd, meta, payload)
      }
      while consumer.buffered_messages != 4 {
        @async.sleep(1)
      }
      let retained = budget.used
      assert_true(retained >= 4 * 65536)
      for _ in 0..<3 {
        consumer.reconnect()
        assert_eq(consumer.buffered_messages, 4)
        assert_eq(consumer.available_permits, 4)
        assert_eq(budget.used, retained)
      }
      for entry in 0..<4 {
        let message = consumer.receive()
        assert_eq(message.payload(), Bytes::make(65536, entry.to_byte()))
      }
      assert_eq(budget.used, 0)
      consumer.close()
      assert_eq(consumer.buffered_messages, 0)
      manager.close()
    })
  })
}

///|
async test "close wakes a blocked receiver before broker close response" {
  @async.with_timeout(5000, async fn() {
    @async.with_task_group(async fn(group) {
      let closing : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      let addr = start_mock_broker(group, async fn(_addr, peer) {
        expect_subscribe(peer)
        let frame = peer.next()
        guard frame.cmd.close_consumer is Some(_) else {
          fail("expected CLOSE_CONSUMER")
        }
        closing.put(())
        release.get()
        peer.unread(frame)
      })
      let manager = ConnectionManager::new(group)
      let consumer = SingleConsumer::create(
        group,
        manager.get(addr),
        "persistent://public/default/close-waiter",
        "sub",
      )
      let received : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      let finished : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      group.spawn_bg(async fn() {
        let result : Result[Message, Error] = Ok(consumer.receive()) catch {
          e => Err(e)
        }
        received.put(result is Err(ConsumerClosed))
      })
      @async.sleep(10)
      group.spawn_bg(async fn() {
        consumer.close()
        finished.put(())
      })
      closing.get()
      // Release the broker even if the assertion fails, so the regression
      // reports the actual wakeup failure rather than hanging the test group.
      let woke = @async.with_timeout(100, async fn() { received.get() }) catch {
        _ => false
      }
      release.put(())
      finished.get()
      assert_true(woke)
      manager.close()
    })
  })
}
