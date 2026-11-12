///|
async test "failed construction cleanup survives cancellation and preserves errors" {
  let cleaned = Ref(false)
  try
    @async.with_timeout(20, () => {
      errdefer cleanup_after_failure(() => {
        @async.sleep(10)
        cleaned.val = true
        raise InvalidFrame("cleanup failure")
      })
      @async.sleep(5000)
    })
  catch {
    @async.TimeoutError => ()
    error => raise error
  } noraise {
    _ => fail("expected timeout")
  }
  assert_true(cleaned.val)
  let body : () -> Unit raise = () => raise RequestTimeout
  try {
    errdefer cleanup_after_failure(() => raise InvalidFrame("cleanup failure"))
    body()
  } catch {
    RequestTimeout => ()
    error => raise error
  } noraise {
    _ => fail("expected original construction error")
  }
}

///|
async test "cancelled memory admission removes both waiter registrations" {
  let limiter = ClientMemoryLimiter::new(1)
  let owner : ProducerMemoryAdmission = { closed: false, waiters: Map([]), }
  limiter.acquire(1, false)
  try @async.with_timeout(20, () => limiter.acquire(1, true, owner~)) catch {
    @async.TimeoutError => ()
    error => raise error
  } noraise {
    _ => fail("expected blocked admission timeout")
  }
  assert_eq(limiter.waiters.length(), 0)
  assert_eq(owner.waiters.length(), 0)
  assert_eq(limiter.used, 1)
  limiter.release_now(1)
  limiter.acquire(1, false, owner~)
  limiter.release_now(1)
  assert_eq(limiter.used, 0)
}

///|
async test "failed partitioned producer cleanup bounds stalled CLOSE and visits every leaf" {
  @async.with_task_group(group => {
    let saw_close = Ref(false)
    let first_addr = start_mock_broker(group, (_, peer) => {
      expect_producer(peer)
      guard peer.next().cmd.close_producer is Some(_) else {
        fail("expected CLOSE_PRODUCER")
      }
      saw_close.val = true
      @async.sleep(5000)
    })
    let second_addr = start_mock_broker(group, (_, peer) => {
      expect_producer(peer)
    })
    let manager = ConnectionManager::new(group)
    defer manager.close()
    let first_conn = manager.get(first_addr)
    let second_conn = manager.get(second_addr)
    let first = SingleProducer::create(
      group, first_conn, "persistent://public/default/cleanup-0",
    )
    let second = SingleProducer::create(
      group, second_conn, "persistent://public/default/cleanup-1",
    )
    let producer = Producer::partitioned([
      Producer::single(first),
      Producer::single(second),
    ])
    let start = @env.now()
    try
      @async.with_timeout(20, () => {
        errdefer producer.cleanup_after_failure()
        @async.sleep(5000)
      })
    catch {
      @async.TimeoutError => ()
      error => raise error
    } noraise {
      _ => fail("expected construction cancellation")
    }
    assert_true(saw_close.val)
    assert_true(@env.now() - start < 4000UL)
    assert_true(producer.is_closed())
    assert_true(first.closed)
    assert_true(second.closed)
    assert_eq(first_conn.producer_handlers.length(), 0)
    assert_eq(second_conn.producer_handlers.length(), 0)
    assert_eq(first_conn.pending.length(), 0)
  })
}
