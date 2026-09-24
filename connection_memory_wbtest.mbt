// Encoded broker frames reserve the shared budget before the body is read.

///|
async test "broker frame bytes are charged through dispatch then released" {
  @async.with_task_group(async fn(group) {
    let send : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let charged : @aqueue.Queue[(Int, Int)] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      send.get()
      let (cmd, meta, body) = message_frame(
        1UL,
        1,
        1UL,
        b"frame-data",
        test_metadata(),
      )
      peer.send_message(cmd, meta, body)
    })
    let limiter = ClientMemoryLimiter::new(1024)
    let conn = Connection::connect_with_memory(
      group,
      addr,
      memory_limiter=limiter,
    )
    conn.message_handlers[1UL] = fn(frame) {
      ignore(
        charged.try_put((limiter.used, frame.wire_size)) catch {
          _ => false
        },
      )
    }
    send.put(())
    let (used, wire_size) = @async.with_timeout(1000, async fn() {
      charged.get()
    })
    assert_true(used > 10)
    assert_eq(used, wire_size)
    assert_eq(limiter.used, 0)
    conn.close()
  })
}

///|
async test "broker frame admission shares held bytes and fails before reading body" {
  @async.with_task_group(async fn(group) {
    let send : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let closed : @aqueue.Queue[Error?] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      send.get()
      let (cmd, meta, body) = message_frame(
        1UL,
        1,
        1UL,
        Bytes::make(140, b'x'),
        test_metadata(),
      )
      peer.send_message(cmd, meta, body)
    })
    let limiter = ClientMemoryLimiter::new(256)
    let conn = Connection::connect_with_memory(
      group,
      addr,
      memory_limiter=limiter,
    )
    conn.on_close(fn(error) {
      ignore(closed.try_put(error) catch { _ => false })
    })
    limiter.reserve_now(100)
    send.put(())
    let error = @async.with_timeout(1000, async fn() { closed.get() })
    assert_true(error is Some(ClientMemoryFull))
    assert_eq(limiter.used, 100)
    limiter.release_now(100)
    assert_eq(limiter.used, 0)
    assert_true(conn.is_closed())
  })
}

///|
async test "oversized inbound frame reports memory error to consumer without retry" {
  @async.with_task_group(async fn(group) {
    let send : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      send.get()
      let (cmd, meta, body) = message_frame(
        1UL,
        2,
        1UL,
        Bytes::make(400, b'x'),
        test_metadata(),
      )
      peer.send_message(cmd, meta, body)
    })
    let limiter = ClientMemoryLimiter::new(256)
    let conn = Connection::connect_with_memory(
      group,
      addr,
      memory_limiter=limiter,
    )
    let retries : Ref[Int] = { val: 0, }
    let consumer = SingleConsumer::create_with_memory(
      group,
      conn,
      "persistent://public/default/oversized-consumer-frame",
      "oversized-consumer-sub",
      reconnect_with=async fn() {
        @async.sleep(1)
        retries.val += 1
        raise ConnectionClosed
      },
      reconnect_policy={
        max_retries: 0,
        initial_backoff_ms: 1,
        max_backoff_ms: 1,
      },
      memory_limiter=limiter,
    )
    send.put(())
    let result : Result[Message, Error] = Ok(
      @async.with_timeout(1000, async fn() { consumer.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ClientMemoryFull))
    assert_eq(retries.val, 0)
    assert_eq(limiter.used, 0)
    consumer.close()
  })
}

///|
async test "oversized inbound frame reports memory error to reader" {
  @async.with_task_group(async fn(group) {
    let send : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      send.get()
      let (cmd, meta, body) = message_frame(
        1UL,
        2,
        1UL,
        Bytes::make(400, b'x'),
        test_metadata(),
      )
      peer.send_message(cmd, meta, body)
    })
    let limiter = ClientMemoryLimiter::new(256)
    let conn = Connection::connect_with_memory(
      group,
      addr,
      memory_limiter=limiter,
    )
    let reader = SingleReader::create_with_memory(
      group,
      conn,
      "persistent://public/default/oversized-reader-frame",
      start_message_id=MessageId::earliest(),
      memory_limiter=limiter,
    )
    send.put(())
    let result : Result[Message?, Error] = Ok(
      @async.with_timeout(1000, async fn() { reader.next() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ClientMemoryFull))
    assert_eq(limiter.used, 0)
    reader.close()
  })
}

///|
async test "malformed broker frame releases its encoded reservation" {
  @async.with_task_group(async fn(group) {
    let send : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let closed : @aqueue.Queue[Error?] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      send.get()
      peer.tcp.write(b"\x00\x00\x00\x04\xff\xff\xff\xff")
    })
    let limiter = ClientMemoryLimiter::new(64)
    let conn = Connection::connect_with_memory(
      group,
      addr,
      memory_limiter=limiter,
    )
    conn.on_close(fn(error) {
      ignore(closed.try_put(error) catch { _ => false })
    })
    send.put(())
    let error = @async.with_timeout(1000, async fn() { closed.get() })
    assert_true(error is Some(InvalidFrame(_)))
    assert_eq(limiter.used, 0)
    assert_true(conn.is_closed())
  })
}
