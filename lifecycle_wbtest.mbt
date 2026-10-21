// These fixtures use async's in-memory pipes only: no sockets, DNS or Broker.
// They exercise real request/dispatch and handle constructors on one connection.

///|
fn lifecycle_memory_connection(
  group : @async.TaskGroup[Unit],
  limiter : ClientMemoryLimiter,
  reject? : Bool = false,
  blocked? : (String, @aqueue.Queue[Unit]),
  close_calls? : Ref[Int],
  wrap_writer? : (&@io.Writer) -> &@io.Writer,
  observe? : (IncomingFrame) -> Unit,
) -> (Connection, @io.PipeWrite) {
  let (reader, peer_writer) = @io.pipe()
  let (peer_reader, writer) = @io.pipe()
  let conn : Connection = {
    reader,
    writer: match wrap_writer {
      Some(wrap) => wrap(writer)
      None => writer
    },
    auth: None,
    auth_provider: None,
    async_auth_provider: None,
    close_transport: fn() {
      if close_calls is Some(calls) {
        calls.val += 1
      }
      // Closing a PipeRead before a pending/future read violates its contract.
      // Close write ends so both background readers finish through EOF.
      writer.close()
      peer_writer.close()
    },
    transport_closed: false,
    next_id: 0UL,
    pending: {},
    message_handlers: {},
    producer_handlers: {},
    coordinator_leases: 0,
    close_handlers: {},
    next_close_handler_id: 0UL,
    closed: false,
    stop: @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1)),
    write_mu: @async.Mutex::Mutex(),
    remote_version: "in-memory-lifecycle",
    remote_protocol_version: 21,
    max_message_size: None,
    request_timeout_ms: 1000,
    memory_limiter: Some(limiter),
  }
  group.spawn_bg(
    async fn() { conn.reader_loop() },
    no_wait=true,
    allow_failure=true,
  )
  group.spawn_bg(
    async fn() {
      while true {
        let frame = read_frame(peer_reader) catch { _ => return }
        if observe is Some(callback) {
          callback(frame)
        }
        let cmd = frame.cmd
        if blocked is Some((phase, ready)) &&
          (
            (
              phase == "create" &&
              (cmd.producer is Some(_) || cmd.subscribe is Some(_))
            ) ||
            (phase == "send" && cmd.send is Some(_)) ||
            (
              phase == "close" &&
              (cmd.close_producer is Some(_) || cmd.close_consumer is Some(_))
            )
          ) {
          ready.put(())
          continue
        }
        let reply = @proto.BaseCommand::default()
        if cmd.producer is Some(req) && !reject {
          reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
          let success = @proto.CommandProducerSuccess::default()
          success.request_id = req.request_id
          success.producer_name = "memory-producer"
          reply.producer_success = Some(success)
        } else if cmd.send is Some(req) {
          reply.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
          let receipt = @proto.CommandSendReceipt::default()
          receipt.producer_id = req.producer_id
          receipt.sequence_id = req.sequence_id
          let id = @proto.MessageIdData::default()
          id.ledger_id = 1UL
          id.entry_id = req.sequence_id
          receipt.message_id = Some(id)
          reply.send_receipt = Some(receipt)
        } else if request_id_of(cmd) is Some(id) {
          if reject && (cmd.producer is Some(_) || cmd.subscribe is Some(_)) {
            reply.type_ = @proto.BaseCommand_Type::ERROR
            let error = @proto.CommandError::default()
            error.request_id = id
            error.error = @proto.ServerError::AuthorizationError
            error.message = "synthetic constructor rejection"
            reply.error = Some(error)
          } else {
            reply.type_ = @proto.BaseCommand_Type::SUCCESS
            let success = @proto.CommandSuccess::default()
            success.request_id = id
            reply.success = Some(success)
          }
        } else {
          continue
        }
        peer_writer.write(encode_command_frame(reply)) catch {
          _ => return
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  (conn, peer_writer)
}

///|
fn lifecycle_assert_connection_empty(
  conn : Connection,
  limiter : ClientMemoryLimiter,
) -> Unit raise {
  assert_eq(conn.close_handlers.length(), 0)
  assert_eq(conn.producer_handlers.length(), 0)
  assert_eq(conn.message_handlers.length(), 0)
  assert_eq(conn.pending.length(), 0)
  assert_eq(limiter.used, 0)
  assert_eq(limiter.waiters.length(), 0)
}

///|
async test "lifecycle producer 200 create-close cycles release callbacks and send state" {
  @async.with_timeout(10000, async fn() {
    @async.with_task_group(async fn(group) {
      let limiter = ClientMemoryLimiter::new(65536)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      for cycle in 0..<200 {
        let producer = SingleProducer::create_with_memory(
          group,
          conn,
          "persistent://public/default/lifecycle",
          send_timeout_ms=60000,
          max_pending_messages=2,
          memory_limiter=limiter,
          batching?=if cycle % 2 == 0 {
            Some({ max_messages: 2, max_bytes: 4096, max_delay_ms: 60000, })
          } else {
            None
          },
          chunk_size?=if cycle % 2 == 1 { Some(3) } else { None },
        )
        if cycle % 2 == 0 {
          let first = producer.send_async(ProducerMessage::new(b"first"))
          assert_true(limiter.used > 0)
          assert_true(producer.batch_timer is Some(_))
          let second = producer.send_async(ProducerMessage::new(b"second"))
          ignore(first.wait())
          ignore(second.wait())
        } else {
          ignore(producer.send(ProducerMessage::new(b"charged")))
        }
        producer.close()
        producer.close()
        assert_eq(producer.pending.length(), 0)
        assert_eq(producer.sends.length(), 0)
        assert_eq(producer.payloads.length(), 0)
        assert_eq(producer.deadlines.heap.length(), 0)
        assert_true(producer.deadline_worker is None)
        assert_true(producer.batch_timer is None)
        lifecycle_assert_connection_empty(conn, limiter)
        assert_true(!conn.is_closed())
      }
    })
  })
}

///|
async test "lifecycle consumer 200 create-close cycles release callbacks and handlers" {
  @async.with_timeout(10000, async fn() {
    @async.with_task_group(async fn(group) {
      let limiter = ClientMemoryLimiter::new(65536)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      for _ in 0..<200 {
        let consumer = SingleConsumer::create_with_memory(
          group,
          conn,
          "persistent://public/default/lifecycle",
          "subscription",
          memory_limiter=limiter,
        )
        consumer.on_frame(lifecycle_delivery(consumer.consumer_id))
        consumer.on_frame(
          lifecycle_delivery(consumer.consumer_id, incomplete=true),
        )
        assert_true(limiter.used > 0)
        assert_eq(consumer.buffered_messages, 1)
        consumer.close()
        consumer.close()
        assert_eq(consumer.ack_pending.length(), 0)
        lifecycle_assert_connection_empty(conn, limiter)
        assert_true(!conn.is_closed())
      }
    })
  })
}

///|
async test "lifecycle reader 200 create-close cycles release callbacks and handlers" {
  @async.with_timeout(10000, async fn() {
    @async.with_task_group(async fn(group) {
      let limiter = ClientMemoryLimiter::new(65536)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      for _ in 0..<200 {
        let reader = SingleReader::create_with_memory(
          group,
          conn,
          "persistent://public/default/lifecycle",
          memory_limiter=limiter,
        )
        reader.on_frame(lifecycle_delivery(reader.consumer_id))
        reader.on_frame(lifecycle_delivery(reader.consumer_id, incomplete=true))
        assert_true(limiter.used > 0)
        reader.close()
        reader.close()
        lifecycle_assert_connection_empty(conn, limiter)
        assert_true(!conn.is_closed())
      }
    })
  })
}

///|
async test "lifecycle failed constructor leaves no close callback or ghost reconnect" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(65536)
    let (conn, _) = lifecycle_memory_connection(group, limiter, reject=true)
    defer conn.close()
    let reconnects : Ref[Int] = { val: 0, }
    let provider = async fn() -> Connection {
      @async.pause()
      reconnects.val += 1
      conn
    }
    let producer : Result[SingleProducer, Error] = Ok(
      SingleProducer::create(
        group,
        conn,
        "persistent://public/default/lifecycle",
        reconnect_with=provider,
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(producer is Err(ServerError(code=4, ..)))
    let consumer : Result[SingleConsumer, Error] = Ok(
      SingleConsumer::create(
        group,
        conn,
        "persistent://public/default/lifecycle",
        "subscription",
        reconnect_with=provider,
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(consumer is Err(ServerError(code=4, ..)))
    lifecycle_assert_connection_empty(conn, limiter)
    conn.close()
    @async.pause()
    assert_eq(reconnects.val, 0)
  })
}

///|
async test "lifecycle initial auth failure rolls back manager while task group stays alive" {
  @async.with_task_group(async fn(group) {
    let manager = ConnectionManager::new(
      group,
      connection_idle_timeout_ms=60000,
      async_auth_provider=async fn() -> AuthData {
        @async.pause()
        raise AuthenticationFailed("synthetic pre-transport failure")
      },
    )
    defer manager.close()
    let worker = manager.idle_worker.unwrap()
    let options = ClientOptions::new("pulsar://never-connected.invalid:6650")
    let result : Result[Client, Error] = Ok(
      Client::connect_manager(manager, options, "never-connected.invalid:6650"),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(AuthenticationFailed(_)))
    assert_true(manager.is_closed())
    assert_eq(manager.connections.length(), 0)
    assert_true(manager.idle_worker is None)
    let ended : Result[Unit, Error] = Ok(
      @async.with_timeout(1000, async fn() { worker.wait() }),
    ) catch {
      e => Err(e)
    }
    assert_true(ended is Err(@async.TaskCancelled))
    @async.pause()
    assert_true(manager.is_closed())
  })
}

///|
fn lifecycle_delivery(
  consumer_id : UInt64,
  incomplete? : Bool = false,
) -> IncomingFrame raise {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::MESSAGE
  let message = @proto.CommandMessage::default()
  message.consumer_id = consumer_id
  let id = @proto.MessageIdData::default()
  id.ledger_id = 1UL
  id.entry_id = if incomplete { 2UL } else { 1UL }
  message.message_id = id
  cmd.message = Some(message)
  let metadata = @proto.MessageMetadata::default()
  metadata.producer_name = "in-memory-lifecycle"
  metadata.sequence_id = 1UL
  metadata.publish_time = 1UL
  metadata.num_messages_in_batch = None
  if incomplete {
    metadata.uuid = Some("lifecycle-incomplete")
    metadata.num_chunks_from_msg = Some(2)
    metadata.chunk_id = Some(0)
    metadata.total_chunk_msg_size = Some(16)
  }
  let bytes = encode_send_frame(cmd, metadata, b"retained")
  decode_frame_body(bytes[4:].to_owned())
}

///|
async test "lifecycle close notification supports ordered removal reentry and closed subscriptions" {
  @async.with_task_group(async fn(group) {
    let close_calls : Ref[Int] = { val: 0, }
    let (conn, _) = lifecycle_memory_connection(
      group,
      ClientMemoryLimiter::new(65536),
      close_calls~,
    )
    let calls : Array[Int] = []
    let removed : Ref[UInt64?] = { val: None, }
    ignore(
      conn.add_close_handler(fn(_) {
        calls.push(1)
        if removed.val is Some(id) {
          conn.close_handlers.remove(id)
        }
        conn.on_close(fn(_) { calls.push(4) })
        conn.close()
      }),
    )
    removed.val = conn.add_close_handler(fn(_) { calls.push(2) })
    conn.on_close(fn(_) { calls.push(3) })
    conn.close()
    conn.close()
    @async.pause()
    assert_eq(calls, [1, 3])
    assert_eq(close_calls.val, 1)
    assert_eq(conn.close_handlers.length(), 0)
    assert_true(conn.add_close_handler(fn(_) { calls.push(5) }) is None)
    assert_eq(calls, [1, 3])
  })
}

///|
async fn[T] lifecycle_wait_cancelled(task : @async.Task[T]) -> Unit {
  task.cancel()
  let result : Result[T, Error] = Ok(
    @async.with_timeout(1000, async fn() { task.wait() }),
  ) catch {
    e => Err(e)
  }
  assert_true(result is Err(@async.TaskCancelled))
}

///|
async test "lifecycle cancellation during each constructor releases pending and registration" {
  @async.with_task_group(async fn(group) {
    for kind in 0..<3 {
      let limiter = ClientMemoryLimiter::new(65536)
      let ready : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Blocking(1),
      )
      let (conn, _) = lifecycle_memory_connection(
        group,
        limiter,
        blocked=("create", ready),
      )
      let task = group.spawn(
        async fn() {
          if kind == 0 {
            ignore(
              SingleProducer::create(
                group, conn, "persistent://public/default/lifecycle",
              ),
            )
          } else if kind == 1 {
            ignore(
              SingleConsumer::create(
                group, conn, "persistent://public/default/lifecycle", "subscription",
              ),
            )
          } else {
            ignore(
              SingleReader::create(
                group, conn, "persistent://public/default/lifecycle",
              ),
            )
          }
        },
        no_wait=true,
        allow_failure=true,
      )
      @async.with_timeout(1000, async fn() { ready.get() })
      assert_eq(conn.pending.length(), 1)
      lifecycle_wait_cancelled(task)
      lifecycle_assert_connection_empty(conn, limiter)
      conn.close()
    }
  })
}

///|
async test "lifecycle cancellation during each close retires local resources" {
  @async.with_task_group(async fn(group) {
    for kind in 0..<3 {
      let limiter = ClientMemoryLimiter::new(65536)
      let ready : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Blocking(1),
      )
      let (conn, _) = lifecycle_memory_connection(
        group,
        limiter,
        blocked=("close", ready),
      )
      let close : async () -> Unit = if kind == 0 {
        let producer = SingleProducer::create_with_memory(
          group,
          conn,
          "persistent://public/default/lifecycle",
          send_timeout_ms=60000,
          max_pending_messages=2,
          memory_limiter=limiter,
        )
        async fn() { producer.close() }
      } else if kind == 1 {
        let consumer = SingleConsumer::create_with_memory(
          group,
          conn,
          "persistent://public/default/lifecycle",
          "subscription",
          memory_limiter=limiter,
        )
        consumer.on_frame(lifecycle_delivery(consumer.consumer_id))
        assert_true(limiter.used > 0)
        async fn() { consumer.close() }
      } else {
        let reader = SingleReader::create_with_memory(
          group,
          conn,
          "persistent://public/default/lifecycle",
          memory_limiter=limiter,
        )
        reader.on_frame(lifecycle_delivery(reader.consumer_id))
        assert_true(limiter.used > 0)
        async fn() { reader.close() }
      }
      let task = group.spawn(close, no_wait=true, allow_failure=true)
      @async.with_timeout(1000, async fn() { ready.get() })
      lifecycle_wait_cancelled(task)
      lifecycle_assert_connection_empty(conn, limiter)
      close()
      lifecycle_assert_connection_empty(conn, limiter)
      conn.close()
    }
  })
}

///|
async test "lifecycle initial cancellation closes manager and terminates idle worker" {
  @async.with_task_group(async fn(group) {
    let entered : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let hold : @aqueue.Queue[AuthData] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let manager = ConnectionManager::new(
      group,
      connection_idle_timeout_ms=60000,
      async_auth_provider=async fn() -> AuthData {
        entered.put(())
        hold.get()
      },
    )
    defer manager.close()
    let worker = manager.idle_worker.unwrap()
    let options = ClientOptions::new("pulsar://never-connected.invalid:6650")
    let task = group.spawn(
      async fn() {
        ignore(
          Client::connect_manager(
            manager, options, "never-connected.invalid:6650",
          ),
        )
      },
      no_wait=true,
      allow_failure=true,
    )
    @async.with_timeout(1000, async fn() { entered.get() })
    lifecycle_wait_cancelled(task)
    assert_true(manager.is_closed())
    assert_true(manager.idle_worker is None)
    assert_eq(manager.connections.length(), 0)
    let ended : Result[Unit, Error] = Ok(
      @async.with_timeout(1000, async fn() { worker.wait() }),
    ) catch {
      e => Err(e)
    }
    assert_true(ended is Err(@async.TaskCancelled))
    manager.close()
    @async.pause()
  })
}

///|
async test "lifecycle closed connection rejects new owners without retaining callbacks" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(65536)
    let (conn, _) = lifecycle_memory_connection(group, limiter)
    conn.close()
    let producer : Result[SingleProducer, Error] = Ok(
      SingleProducer::create(
        group, conn, "persistent://public/default/lifecycle",
      ),
    ) catch {
      e => Err(e)
    }
    let consumer : Result[SingleConsumer, Error] = Ok(
      SingleConsumer::create(
        group, conn, "persistent://public/default/lifecycle", "subscription",
      ),
    ) catch {
      e => Err(e)
    }
    let reader : Result[SingleReader, Error] = Ok(
      SingleReader::create(group, conn, "persistent://public/default/lifecycle"),
    ) catch {
      e => Err(e)
    }
    assert_true(producer is Err(ConnectionClosed))
    assert_true(consumer is Err(ConnectionClosed))
    assert_true(reader is Err(ConnectionClosed))
    lifecycle_assert_connection_empty(conn, limiter)
  })
}

///|
async test "lifecycle owner retirement preserves other owners and public close observers" {
  @async.with_task_group(async fn(group) {
    let (conn, _) = lifecycle_memory_connection(
      group,
      ClientMemoryLimiter::new(65536),
    )
    defer conn.close()
    let calls : Ref[Int] = { val: 0, }
    conn.on_close(fn(_) { calls.val += 1 })
    let producer = SingleProducer::create(
      group, conn, "persistent://public/default/lifecycle",
    )
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/lifecycle", "subscription",
    )
    let reader = SingleReader::create(
      group, conn, "persistent://public/default/lifecycle",
    )
    assert_eq(conn.close_handlers.length(), 4)
    producer.close()
    assert_eq(conn.close_handlers.length(), 3)
    assert_eq(conn.message_handlers.length(), 2)
    consumer.close()
    assert_eq(conn.close_handlers.length(), 2)
    assert_eq(conn.message_handlers.length(), 1)
    reader.close()
    assert_eq(conn.close_handlers.length(), 1)
    assert_eq(calls.val, 0)
    conn.close()
    assert_eq(calls.val, 1)
    assert_eq(conn.close_handlers.length(), 0)
  })
}

///|
async test "lifecycle reconnect transfers only live owner registrations" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(65536)
    let (first, _) = lifecycle_memory_connection(group, limiter)
    let (second, _) = lifecycle_memory_connection(group, limiter)
    defer first.close()
    defer second.close()
    let provider = async fn() -> Connection {
      @async.pause()
      second
    }
    let producer = SingleProducer::create_with_memory(
      group,
      first,
      "persistent://public/default/lifecycle",
      reconnect_with=provider,
      memory_limiter=limiter,
    )
    let consumer = SingleConsumer::create_with_memory(
      group,
      first,
      "persistent://public/default/lifecycle",
      "subscription",
      reconnect_with=provider,
      memory_limiter=limiter,
    )
    consumer.on_frame(lifecycle_delivery(consumer.consumer_id, incomplete=true))
    assert_true(limiter.used > 0)
    first.close()
    @async.with_timeout(1000, async fn() {
      while producer.reconnecting.val || consumer.reconnecting.val {
        @async.pause()
      }
    })
    assert_eq(first.close_handlers.length(), 0)
    assert_eq(second.close_handlers.length(), 2)
    assert_eq(second.producer_handlers.length(), 1)
    assert_eq(second.message_handlers.length(), 1)
    assert_eq(limiter.used, 0)
    ignore(producer.send(ProducerMessage::new(b"replacement")))
    producer.close()
    assert_eq(second.close_handlers.length(), 1)
    consumer.close()
    lifecycle_assert_connection_empty(second, limiter)
  })
}

///|
async test "lifecycle failed reconnect releases registrations and incomplete chunk charges" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(65536)
    let (first, _) = lifecycle_memory_connection(group, limiter)
    let (second, _) = lifecycle_memory_connection(group, limiter, reject=true)
    defer first.close()
    defer second.close()
    let provider = async fn() -> Connection {
      @async.pause()
      second
    }
    let policy = { ..RetryPolicy::default(), max_retries: 0, }
    let producer = SingleProducer::create_with_memory(
      group,
      first,
      "persistent://public/default/lifecycle",
      reconnect_with=provider,
      reconnect_policy=policy,
      memory_limiter=limiter,
    )
    let consumer = SingleConsumer::create_with_memory(
      group,
      first,
      "persistent://public/default/lifecycle",
      "subscription",
      reconnect_with=provider,
      reconnect_policy=policy,
      memory_limiter=limiter,
    )
    consumer.on_frame(lifecycle_delivery(consumer.consumer_id, incomplete=true))
    assert_true(limiter.used > 0)
    first.close()
    @async.with_timeout(1000, async fn() {
      while producer.reconnecting.val || consumer.reconnecting.val {
        @async.pause()
      }
    })
    assert_true(producer.closed && consumer.closed.val)
    lifecycle_assert_connection_empty(second, limiter)
    producer.close()
    consumer.close()
    lifecycle_assert_connection_empty(second, limiter)
  })
}

///|
async test "lifecycle terminal disconnect releases a non-reconnecting consumer chunk charge" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(65536)
    let (conn, _) = lifecycle_memory_connection(group, limiter)
    let consumer = SingleConsumer::create_with_memory(
      group,
      conn,
      "persistent://public/default/lifecycle",
      "subscription",
      memory_limiter=limiter,
    )
    consumer.on_frame(lifecycle_delivery(consumer.consumer_id, incomplete=true))
    assert_eq(limiter.used, 8)
    conn.close()
    assert_true(consumer.closed.val)
    lifecycle_assert_connection_empty(conn, limiter)
    consumer.close()
    lifecycle_assert_connection_empty(conn, limiter)
  })
}

///|
async test "lifecycle rejected producer does not attempt ghost reconnect" {
  @async.with_task_group(async fn(group) {
    let limiter = ClientMemoryLimiter::new(65536)
    let (conn, _) = lifecycle_memory_connection(group, limiter, reject=true)
    let reconnects : Ref[Int] = { val: 0, }
    let result : Result[SingleProducer, Error] = Ok(
      SingleProducer::create(
        group,
        conn,
        "persistent://public/default/lifecycle",
        reconnect_with=async fn() -> Connection {
          reconnects.val += 1
          @async.pause()
          raise ConnectFailed("unexpected ghost reconnect")
        },
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ServerError(code=4, ..)))
    conn.close()
    for _ in 0..<3 {
      @async.pause()
    }
    assert_eq(reconnects.val, 0)
  })
}
