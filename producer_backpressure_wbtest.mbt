// Producer queue and timeout behavior with broker receipts under test control.

///|
async test "producer rejects a full pending queue and releases it on receipt" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let first = peer.next()
      guard first.cmd.send is Some(send) else { fail("expected first SEND") }
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
      let second = peer.next()
      guard second.cmd.send is Some(next) else { fail("expected second SEND") }
      send_receipt(peer, next.producer_id, next.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/bounded",
      max_pending_messages=1,
    )
    let first = producer.send_async(ProducerMessage::new(b"one"))
    let full : Result[ReceiptFuture, Error] = Ok(
      producer.send_async(ProducerMessage::new(b"rejected")),
    ) catch {
      e => Err(e)
    }
    assert_true(full is Err(ProducerQueueFull))
    release.put(())
    ignore(first.wait())
    let second = producer.send_async(ProducerMessage::new(b"two"))
    ignore(second.wait())
    producer.close()
    manager.close()
  })
}

///|
async test "blocking producer queue waits for a receipt" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let first = peer.next()
      guard first.cmd.send is Some(send) else { fail("expected first SEND") }
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
      let second = peer.next()
      guard second.cmd.send is Some(next) else { fail("expected second SEND") }
      send_receipt(peer, next.producer_id, next.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/blocking",
      max_pending_messages=1,
      block_if_queue_full=true,
    )
    let first = producer.send_async(ProducerMessage::new(b"one"))
    let ready : @aqueue.Queue[ReceiptFuture] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    group.spawn_bg(
      async fn() {
        ready.put(producer.send_async(ProducerMessage::new(b"two")))
      },
      no_wait=true,
      allow_failure=true,
    )
    @async.sleep(20)
    assert_true(ready.try_get() is None)
    release.put(())
    ignore(first.wait())
    ignore(@async.with_timeout(1000, async fn() { ready.get().wait() }))
    producer.close()
    manager.close()
  })
}

///|
async test "send timeout releases queue capacity and ignores a late receipt" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let first = peer.next()
      guard first.cmd.send is Some(send) else { fail("expected first SEND") }
      let second = peer.next()
      guard second.cmd.send is Some(next) else { fail("expected second SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
      send_receipt(peer, next.producer_id, next.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/timeout",
      max_pending_messages=1,
      send_timeout_ms=30,
    )
    let timed_out : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"one")),
    ) catch {
      e => Err(e)
    }
    assert_true(timed_out is Err(RequestTimeout))
    let second = producer.send(ProducerMessage::new(b"two"))
    assert_eq(second.sequence_id, 1UL)
    producer.close()
    manager.close()
  })
}

///|
async test "broker send error releases bounded producer capacity" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let first = peer.next()
      guard first.cmd.send is Some(send) else { fail("expected first SEND") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SEND_ERROR
      let error = @proto.CommandSendError::default()
      error.producer_id = send.producer_id
      error.sequence_id = send.sequence_id
      error.error = @proto.ServerError::PersistenceError
      error.message = "write failed"
      reply.send_error = Some(error)
      peer.send_command(reply)
      let second = peer.next()
      guard second.cmd.send is Some(next) else { fail("expected second SEND") }
      send_receipt(peer, next.producer_id, next.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/error-release",
      max_pending_messages=1,
    )
    let first : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"one")),
    ) catch {
      e => Err(e)
    }
    assert_true(first is Err(ServerError(message="write failed", ..)))
    ignore(producer.send(ProducerMessage::new(b"two")))
    producer.close()
    manager.close()
  })
}

///|
async test "bounded producer counts buffered batch messages" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected batched SEND") }
      assert_eq(send.num_messages, Some(2))
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/batch-limit",
      batching={ max_messages: 2, max_bytes: 1000, max_delay_ms: 60000, },
      max_pending_messages=2,
    )
    let first = producer.send_async(ProducerMessage::new(b"one"))
    let second = producer.send_async(ProducerMessage::new(b"two"))
    let full : Result[ReceiptFuture, Error] = Ok(
      producer.send_async(ProducerMessage::new(b"three")),
    ) catch {
      e => Err(e)
    }
    assert_true(full is Err(ProducerQueueFull))
    ignore(first.wait())
    ignore(second.wait())
    producer.close()
    manager.close()
  })
}

///|
async test "closing a producer wakes callers blocked on its queue" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(_) else { fail("expected SEND") }
      observed.put(())
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/close-queue",
      max_pending_messages=1,
      block_if_queue_full=true,
    )
    let first = producer.send_async(ProducerMessage::new(b"one"))
    observed.get()
    let blocked : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    group.spawn_bg(
      async fn() {
        let result : Result[ReceiptFuture, Error] = Ok(
          producer.send_async(ProducerMessage::new(b"two")),
        ) catch {
          e => Err(e)
        }
        blocked.put(result is Err(ProducerClosed))
      },
      no_wait=true,
      allow_failure=true,
    )
    @async.sleep(10)
    assert_true(blocked.try_get() is None)
    producer.close()
    assert_true(@async.with_timeout(1000, async fn() { blocked.get() }))
    let first_result : Result[SendReceipt, Error] = Ok(first.wait()) catch {
      e => Err(e)
    }
    assert_true(first_result is Err(ProducerClosed))
    manager.close()
  })
}

///|
async test "producer rejects invalid queue and timeout settings" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
    })
    let manager = ConnectionManager::new(group)
    let invalid : Result[SingleProducer, Error] = Ok(
      SingleProducer::create(
        group,
        manager.get(addr),
        "persistent://public/default/invalid",
        max_pending_messages=0,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(invalid is Err(InvalidFrame(_)))
    let invalid_timeout : Result[SingleProducer, Error] = Ok(
      SingleProducer::create(
        group,
        manager.get(addr),
        "persistent://public/default/invalid",
        send_timeout_ms=0,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(invalid_timeout is Err(InvalidFrame(_)))
    manager.close()
  })
}

///|
async test "batch message timeout removes it before flush and releases capacity" {
  @async.with_task_group(async fn(group) {
    let sent : @aqueue.Queue[Int?] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      sent.put(send.num_messages)
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/batch-queue-timeout",
      batching={ max_messages: 10, max_bytes: 1000, max_delay_ms: 60000, },
      max_pending_messages=1,
      send_timeout_ms=30,
    )
    let first = producer.send_async(ProducerMessage::new(b"expired"))
    let expired : Result[SendReceipt, Error] = Ok(
      @async.with_timeout(1000, async fn() { first.wait() }),
    ) catch {
      e => Err(e)
    }
    let second = producer.send_async(ProducerMessage::new(b"survived"))
    producer.flush()
    let receipt = second.wait()
    assert_true(expired is Err(RequestTimeout))
    assert_eq(sent.get(), Some(1))
    assert_eq(receipt.sequence_id, 0UL)
    producer.close()
    manager.close()
  })
}

///|
async test "batched send timeout includes time spent waiting for flush" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/batch-inflight-timeout",
      batching={ max_messages: 10, max_bytes: 1000, max_delay_ms: 60000, },
      send_timeout_ms=2000,
    )
    let future = producer.send_async(ProducerMessage::new(b"late"))
    let original_deadline = producer.deadlines.heap[0].1
    @async.sleep(50)
    producer.flush()
    // Assert the deadline itself: scheduler latency must not decide whether
    // flush preserved the admission time or restarted the timeout.
    assert_eq(producer.deadlines.heap[0].1, original_deadline)
    let result : Result[SendReceipt, Error] = Ok(
      @async.with_timeout(5000, async fn() { future.wait() }),
    ) catch {
      e => Err(e)
    }
    release.put(())
    assert_true(result is Err(RequestTimeout))
    producer.close()
    manager.close()
  })
}

///|
async test "batch receipt remains readable after its timeout task wakes" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/batch-receipt-before-timeout",
      batching={ max_messages: 10, max_bytes: 1000, max_delay_ms: 60000, },
      send_timeout_ms=30,
    )
    let future = producer.send_async(ProducerMessage::new(b"early"))
    producer.flush()
    @async.sleep(50)
    assert_eq(future.wait().sequence_id, 0UL)
    producer.close()
    manager.close()
  })
}
