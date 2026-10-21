// Public client configuration and pool replacement on an owned random loopback
// port. The synthetic peer completes handshakes and deliberately ignores PING.

///|
async test "keepalive client options close a silent pooled connection and allow replacement" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let accepted : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let addr = start_pool_broker(group, accepted)
      let client = Client::connect_with_options(
        group,
        ClientOptions::new("pulsar://\{addr}", keepalive_interval_ms=100),
      )
      defer client.close()
      accepted.get()
      let first = client.manager.get(addr)
      let closed : @aqueue.Queue[Error?] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      first.on_close(error => ignore(closed.try_put(error) catch { _ => false }))
      let error = closed.get()
      assert_true(error is Some(ConnectionClosed))
      assert_true(first.is_closed() && first.transport_closed)
      assert_true(accepted.try_get() is None)
      let second = client.manager.get(addr)
      accepted.get()
      assert_false(second.is_closed())
      client.close()
      assert_true(second.is_closed() && second.transport_closed)
      assert_true(closed.try_get() is None)
    })
  })
}
