// Connection pool behavior with a broker that accepts multiple handshakes.

///|
async fn start_pool_broker(
  group : @async.TaskGroup[Unit],
  accepted : @aqueue.Queue[Unit],
) -> String {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let addr = server.addr.to_string()
  group.spawn_bg(
    async fn() {
      defer server.close()
      while true {
        let (tcp, _) = server.accept()
        group.spawn_bg(
          async fn() {
            defer tcp.close()
            let hello = read_frame(tcp)
            guard hello.cmd.connect is Some(_) else { fail("expected CONNECT") }
            let connected = @proto.BaseCommand::default()
            connected.type_ = @proto.BaseCommand_Type::CONNECTED
            let info = @proto.CommandConnected::default()
            info.server_version = "pool-mock"
            info.protocol_version = Some(21)
            connected.connected = Some(info)
            tcp.write(encode_command_frame(connected))
            accepted.put(())
            try {
              while true {
                ignore(read_frame(tcp))
              }
            } catch {
              _ => ()
            }
          },
          no_wait=true,
          allow_failure=true,
        )
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  addr
}

///|
async test "connection pool bounds broker connections and replaces closed ones" {
  @async.with_task_group(async fn(group) {
    let accepted : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_pool_broker(group, accepted)
    let manager = ConnectionManager::new(group, max_connections_per_broker=2)
    let first = manager.get(addr)
    accepted.get()
    ignore(manager.get(addr))
    accepted.get()
    for _ in 0..<5 {
      ignore(manager.get(addr))
    }
    assert_true(accepted.try_get() is None)
    first.close()
    ignore(manager.get(addr))
    @async.with_timeout(1000, async fn() { accepted.get() })
    manager.close()
  })
}

///|
async test "idle pool evicts only connections without active handlers" {
  @async.with_task_group(async fn(group) {
    let accepted : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_pool_broker(group, accepted)
    let manager = ConnectionManager::new(group, connection_idle_timeout_ms=20)
    let first = manager.get(addr)
    accepted.get()
    first.producer_handlers[1UL] = fn(_) {  }
    @async.sleep(30)
    ignore(manager.get(addr))
    assert_true(accepted.try_get() is None)
    first.producer_handlers.remove(1UL)
    @async.with_timeout(1000, async fn() {
      while !first.is_closed() {
        @async.sleep(5)
      }
    })
    ignore(manager.get(addr))
    @async.with_timeout(1000, async fn() { accepted.get() })
    assert_true(first.is_closed())
    manager.close()
  })
}

///|
test "client options reject invalid connection pool settings" {
  let invalid : Result[ClientOptions, Error] = Ok(
    ClientOptions::new("pulsar://localhost:6650", max_connections_per_broker=0),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
  let invalid_idle : Result[ClientOptions, Error] = Ok(
    ClientOptions::new("pulsar://localhost:6650", connection_idle_timeout_ms=0),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid_idle is Err(InvalidFrame(_)))
}
