// A client-wide byte budget is shared by all of its producers.

///|
test "client memory limit must be positive" {
  let invalid : Result[ClientOptions, Error] = Ok(
    ClientOptions::new("pulsar://localhost:6650", max_memory_bytes=0),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
}

///|
async test "two producers share one pending payload-byte limit" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let first_addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected first SEND") }
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let second_addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected second SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(5)
    let first = SingleProducer::create_with_memory(
      group,
      manager.get(first_addr),
      "persistent://public/default/memory-first",
      memory_limiter=limiter,
    )
    let second = SingleProducer::create_with_memory(
      group,
      manager.get(second_addr),
      "persistent://public/default/memory-second",
      memory_limiter=limiter,
    )
    let pending = first.send_async(ProducerMessage::new(b"12345"))
    let full : Result[ReceiptFuture, Error] = Ok(
      second.send_async(ProducerMessage::new(b"x")),
    ) catch {
      e => Err(e)
    }
    assert_true(full is Err(ClientMemoryFull))
    let oversized : Result[ReceiptFuture, Error] = Ok(
      second.send_async(ProducerMessage::new(b"123456")),
    ) catch {
      e => Err(e)
    }
    assert_true(oversized is Err(ClientMemoryFull))
    release.put(())
    ignore(pending.wait())
    ignore(second.send(ProducerMessage::new(b"x")))
    assert_eq(limiter.used, 0)
    first.close()
    second.close()
    manager.close()
  })
}

///|
async test "blocked producer resumes when another producer releases memory" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let first_addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected first SEND") }
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let second_addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected second SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(4)
    let first = SingleProducer::create_with_memory(
      group,
      manager.get(first_addr),
      "persistent://public/default/memory-block-first",
      memory_limiter=limiter,
    )
    let second = SingleProducer::create_with_memory(
      group,
      manager.get(second_addr),
      "persistent://public/default/memory-block-second",
      memory_limiter=limiter,
      block_if_queue_full=true,
    )
    let pending = first.send_async(ProducerMessage::new(b"1234"))
    let ready : @aqueue.Queue[ReceiptFuture] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    group.spawn_bg(
      async fn() { ready.put(second.send_async(ProducerMessage::new(b"x"))) },
      no_wait=true,
      allow_failure=true,
    )
    @async.sleep(20)
    assert_true(ready.try_get() is None)
    release.put(())
    ignore(pending.wait())
    ignore(@async.with_timeout(1000, async fn() { ready.get().wait() }))
    assert_eq(limiter.used, 0)
    first.close()
    second.close()
    manager.close()
  })
}

///|
async test "send timeout releases shared memory for another producer" {
  @async.with_task_group(async fn(group) {
    let first_addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      guard peer.next().cmd.send is Some(_) else { fail("expected SEND") }
    })
    let second_addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(3)
    let first = SingleProducer::create_with_memory(
      group,
      manager.get(first_addr),
      "persistent://public/default/memory-timeout-first",
      memory_limiter=limiter,
      send_timeout_ms=30,
    )
    let second = SingleProducer::create_with_memory(
      group,
      manager.get(second_addr),
      "persistent://public/default/memory-timeout-second",
      memory_limiter=limiter,
    )
    let expired : Result[SendReceipt, Error] = Ok(
      first.send(ProducerMessage::new(b"123")),
    ) catch {
      e => Err(e)
    }
    assert_true(expired is Err(RequestTimeout))
    ignore(second.send(ProducerMessage::new(b"x")))
    assert_eq(limiter.used, 0)
    first.close()
    second.close()
    manager.close()
  })
}

///|
async test "closing a memory budget wakes blocked senders" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(1)
    limiter.acquire(1, false)
    let result : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    group.spawn_bg(
      async fn() {
        let blocked : Result[Unit, Error] = Ok(limiter.acquire(1, true)) catch {
          e => Err(e)
        }
        result.put(blocked is Err(ProducerClosed))
      },
      no_wait=true,
      allow_failure=true,
    )
    @async.sleep(10)
    assert_true(result.try_get() is None)
    limiter.close()
    assert_true(@async.with_timeout(1000, async fn() { result.get() }))
    limiter.release(1)
    assert_eq(limiter.used, 0)
  })
}
