// Private two-admission checkpoint over real loopback TCP. Not live Broker
// acceptance: the peers below implement only the required protocol exchange.
// No environment guard, credentials, DNS hostnames, or product API changes.

///|
suberror ReplayTcpFault {
  InjectedAfterPrefix
  ReplacementNotPrepared
} derive(Debug)

///|
pub extend ReplayTcpFault with @debug.Debug::{to_repr}

///|
struct ReplayTcpWriter {
  tcp : @socket.Tcp
  held_frame : Bytes?
  started : @aqueue.Queue[Unit]
  release : @aqueue.Queue[Bool]
  mut entered : Bool
  mut active : Int32
  mut prefix_bytes : Int32
  mut injected : Int32
}

///|
impl @io.Writer for ReplayTcpWriter with fn write_once(
  self,
  bytes,
  offset~,
  len~,
) {
  if !self.entered &&
    offset == 0 &&
    self.held_frame is Some(expected) &&
    decode_frame_body(bytes[4:].to_owned()).cmd.send is Some(_) {
    // Gate the real awaited replay write, not a log event or a later receipt.
    assert_eq(bytes, expected)
    self.entered = true
    self.active += 1
    defer {
      self.active -= 1
    }
    let count = self.tcp.write_once(bytes, offset~, len=1)
    assert_eq(count, 1)
    self.prefix_bytes += count
    self.started.put(())
    if !self.release.get() {
      self.injected += 1
      raise ReplayTcpFault::InjectedAfterPrefix
    }
    count
  } else {
    self.tcp.write_once(bytes, offset~, len~)
  }
}

///|
fn replay_tcp_join(a : Bytes, b : Bytes) -> Bytes {
  Bytes::makei(a.length() + b.length(), i => {
    if i < a.length() {
      a[i]
    } else {
      b[i - a.length()]
    }
  })
}

///|
async fn replay_tcp_read(tcp : @socket.Tcp, prefix? : Bytes) -> Bytes {
  let header = match prefix {
    Some(first) => {
      let tail = tcp.read_exactly(3) catch { _ => raise ConnectionClosed }
      replay_tcp_join(first, tail)
    }
    None => tcp.read_exactly(4) catch { _ => raise ConnectionClosed }
  }
  let size = ((header[0].to_uint() << 24) |
  (header[1].to_uint() << 16) |
  (header[2].to_uint() << 8) |
  header[3].to_uint()).reinterpret_as_int()
  guard size >= 4 && size <= 65536 else {
    raise InvalidFrame("TCP replay fixture frame bound")
  }
  replay_tcp_join(
    header,
    tcp.read_exactly(size) catch {
      _ => raise ConnectionClosed
    },
  )
}

///|
struct ReplayTcpPeer {
  address : String
  tcp : Ref[@socket.Tcp?]
  sends : Array[Bytes]
  epochs : Array[UInt64]
  received : @aqueue.Queue[Unit]
  prefix_seen : @aqueue.Queue[Unit]
  mut finished : Bool
}

///|
async fn replay_tcp_peer(
  group : @async.TaskGroup[Unit],
  observe_prefix : Bool,
) -> ReplayTcpPeer {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let state : ReplayTcpPeer = {
    address: server.addr.to_string(),
    tcp: { val: None, },
    sends: [],
    epochs: [],
    received: @aqueue.Queue::Queue(kind=Blocking(4)),
    prefix_seen: @aqueue.Queue::Queue(kind=Blocking(1)),
    finished: false,
  }
  group.spawn_bg(
    async fn() {
      let listener_open : Ref[Bool] = { val: true, }
      defer (if listener_open.val { server.close() })
      let (tcp, _) = server.accept()
      // No more accepts; the endpoint is retired while this one TCP pair lives.
      server.close()
      listener_open.val = false
      state.tcp.val = Some(tcp)
      defer {
        tcp.close()
        state.finished = true
      }
      let mut prefix_next = false
      while true {
        let raw = try {
          if prefix_next {
            prefix_next = false
            let byte = tcp.read_exactly(1) catch { _ => raise ConnectionClosed }
            state.prefix_seen.put(())
            replay_tcp_read(tcp, prefix=byte)
          } else {
            replay_tcp_read(tcp)
          }
        } catch {
          ConnectionClosed => return
          error => raise error
        }
        let command = decode_frame_body(raw[4:].to_owned()).cmd
        let reply = @proto.BaseCommand::default()
        if command.connect is Some(_) {
          reply.type_ = @proto.BaseCommand_Type::CONNECTED
          let connected = @proto.CommandConnected::default()
          connected.server_version = "two-send-tcp-mock"
          connected.protocol_version = Some(21)
          reply.connected = Some(connected)
        } else if command.producer is Some(request) {
          state.epochs.push(request.epoch.unwrap())
          reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
          let success = @proto.CommandProducerSuccess::default()
          success.request_id = request.request_id
          success.producer_name = "two-send-producer"
          reply.producer_success = Some(success)
          prefix_next = observe_prefix
        } else if command.send is Some(_) {
          assert_true(state.sends.length() < 2)
          state.sends.push(raw)
          state.received.put(())
          // Both receipts are explicitly controlled by the test, including A.
          continue
        } else if command.close_producer is Some(request) {
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = request.request_id
          reply.success = Some(success)
        } else {
          fail("unexpected two-send peer command")
        }
        tcp.write(encode_command_frame(reply))
      }
    },
    no_wait=true,
  )
  state
}

///|
// Test-only transport injection. Uses the product Connection reader/dispatch,
// write lock and producer reconnect implementation over an actual Tcp Writer.
// The endpoint must be a canonical literal loopback address; no TLS/auth here.
async fn replay_tcp_connect(
  group : @async.TaskGroup[Unit],
  address : String,
  limiter : ClientMemoryLimiter,
  held_frame? : Bytes,
) -> (Connection, ReplayTcpWriter) {
  let (host, port) = split_host_port(address)
  assert_eq(host, "127.0.0.1")
  assert_true(port > 0 && port <= 65535)
  assert_eq(address, "127.0.0.1:\{port}")
  let tcp = @socket.Tcp::connect_to_host(host, port~)
  errdefer tcp.close()
  let writer : ReplayTcpWriter = {
    tcp,
    held_frame,
    started: @aqueue.Queue::Queue(kind=Blocking(1)),
    release: @aqueue.Queue::Queue(kind=Blocking(1)),
    entered: false,
    active: 0,
    prefix_bytes: 0,
    injected: 0,
  }
  let conn : Connection = {
    reader: tcp,
    writer,
    auth: None,
    auth_provider: None,
    async_auth_provider: None,
    close_transport: fn() { tcp.close() },
    transport_closed: false,
    next_id: 0UL,
    pending: {},
    message_handlers: {},
    producer_handlers: {},
    coordinator_leases: 0,
    close_handlers: {},
    next_close_handler_id: 0UL,
    closed: false,
    waiting_for_ping_response: false,
    stop: @aqueue.Queue::Queue(kind=Blocking(1)),
    write_mu: @async.Mutex::Mutex(),
    remote_version: "",
    remote_protocol_version: 0,
    max_message_size: None,
    request_timeout_ms: 2000,
    memory_limiter: Some(limiter),
  }
  errdefer conn.close()
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::CONNECT
  let hello = @proto.CommandConnect::default()
  hello.client_version = CLIENT_VERSION
  hello.protocol_version = Some(PROTOCOL_VERSION)
  command.connect = Some(hello)
  conn.send_bytes(encode_command_frame(command))
  let connected = read_frame(tcp).cmd.connected.unwrap()
  conn.remote_version = connected.server_version
  conn.remote_protocol_version = connected.protocol_version.unwrap_or(0)
  conn.max_message_size = connected.max_message_size
  assert_eq(conn.remote_protocol_version, 21)
  group.spawn_bg(() => conn.reader_loop(), no_wait=true, allow_failure=true)
  group.spawn_bg(
    () => conn.keepalive_loop(60000),
    no_wait=true,
    allow_failure=true,
  )
  (conn, writer)
}

///|
async fn replay_tcp_receipt(peer : ReplayTcpPeer, index : Int32) -> Unit {
  let frame = decode_frame_body(peer.sends[index][4:].to_owned())
  let send = frame.cmd.send.unwrap()
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
  let receipt = @proto.CommandSendReceipt::default()
  receipt.producer_id = send.producer_id
  receipt.sequence_id = send.sequence_id
  let id = @proto.MessageIdData::default()
  id.ledger_id = 1UL
  id.entry_id = send.sequence_id
  receipt.message_id = Some(id)
  command.send_receipt = Some(receipt)
  peer.tcp.val.unwrap().write(encode_command_frame(command))
}

///|
fn replay_tcp_wait_once(
  group : @async.TaskGroup[Unit],
  original : ReceiptFuture,
  entered : @aqueue.Queue[Unit],
) -> @async.Task[Result[SendReceipt, Error]] {
  group.spawn(
    () => {
      entered.put(())
      let result : Result[SendReceipt, Error] = Ok(original.wait()) catch {
        error => Err(error)
      }
      result
    },
    no_wait=true,
  )
}

///|
async fn replay_tcp_two_send(retry : Bool, closing : Bool) -> Unit {
  assert_false(retry && closing)
  @async.with_timeout(8000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let first_peer = replay_tcp_peer(group, false)
      let (first, _) = replay_tcp_connect(group, first_peer.address, limiter)
      defer first.close()
      let second_ref : Ref[Connection?] = { val: None, }
      let third_ref : Ref[Connection?] = { val: None, }
      let attempts : Ref[Int32] = { val: 0, }
      let completed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/two-send-tcp",
        name="two-send-producer",
        max_pending_messages=2,
        block_if_queue_full=false,
        send_timeout_ms=60000,
        memory_limiter=limiter,
        reconnect_with=() => {
          attempts.val += 1
          if attempts.val == 1 {
            guard second_ref.val is Some(second) else {
              raise ReplayTcpFault::ReplacementNotPrepared
            }
            second
          } else {
            assert_eq(attempts.val, 2)
            assert_true(retry)
            guard third_ref.val is Some(third) else {
              raise ReplayTcpFault::ReplacementNotPrepared
            }
            third
          }
        },
        reconnect_policy={
          max_retries: if retry {
            1
          } else {
            0
          },
          initial_backoff_ms: 1,
          max_backoff_ms: 1,
        },
        on_event=fn(event) {
          if event is ProducerReconnected(_, result) {
            replay_notify_event(completed, result is Ok(_))
          }
        },
      )
      // Retire the owner before transport defers can notify it on an error.
      // This synchronous cleanup neither retries nor replaces the primary error.
      defer {
        producer.fail_all(ProducerClosed)
        if third_ref.val is Some(third) {
          third.close()
        }
        if second_ref.val is Some(second) {
          second.close()
        }
      }
      let a = producer.send_async(ProducerMessage::new(b"replay-A"))
      first_peer.received.get()
      let original_a = first_peer.sends[0]
      let a_command = decode_frame_body(original_a[4:].to_owned()).cmd.send.unwrap()
      let a_seq = a_command.sequence_id
      let a_entry = producer.pending.get(a_seq).unwrap().entries[0].0
      let a_deadline = a_entry.deadline
      let entered : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Blocking(1))
      let a_wait = replay_tcp_wait_once(group, a, entered)
      entered.get()
      let second_peer = replay_tcp_peer(group, true)
      let (second, gate) = replay_tcp_connect(
        group,
        second_peer.address,
        limiter,
        held_frame=original_a,
      )
      second_ref.val = Some(second)
      // Allocate a third owned TCP pair only for the registration-retry case.
      let third_peer : ReplayTcpPeer? = if retry {
        let peer = replay_tcp_peer(group, false)
        let (third, _) = replay_tcp_connect(group, peer.address, limiter)
        third_ref.val = Some(third)
        Some(peer)
      } else {
        None
      }
      first.close()
      gate.started.get()
      second_peer.prefix_seen.get()
      assert_eq(gate.active, 1)
      assert_eq(gate.prefix_bytes, 1)
      assert_true(producer.reconnecting.val)
      assert_eq(producer.epoch, 1UL)
      assert_eq(second_peer.sends.length(), 0)
      assert_true(a_wait.try_wait() is None)
      assert_true(
        producer.pending.get(a_seq).unwrap().write_task.val is Some(_),
      )
      assert_eq(producer.payloads.get(a_seq).unwrap(), original_a)
      // B is admitted only after BOTH the actual Writer and peer prefix gates.
      let b = producer.send_async(ProducerMessage::new(b"during-replay-B"))
      let b_seq = a_seq + 1UL
      let b_frame = producer.pending.get(b_seq).unwrap()
      let b_entry = b_frame.entries[0].0
      let b_deadline = b_entry.deadline
      let original_b = producer.payloads.get(b_seq).unwrap()
      let b_wait = replay_tcp_wait_once(group, b, entered)
      entered.get()
      assert_eq(gate.active, 1)
      assert_true(producer.reconnecting.val)
      assert_true(b_frame.write_task.val is None)
      assert_true(a_wait.try_wait() is None && b_wait.try_wait() is None)
      assert_eq(a_entry.deadline, a_deadline)
      assert_eq(producer.pending.length(), 2)
      assert_eq(producer.sends.length(), 2)
      // This third call must not produce another admitted message/future.
      let full : Result[ReceiptFuture, Error] = Ok(
        producer.send_async(ProducerMessage::new(b"must-not-be-admitted")),
      ) catch {
        error => Err(error)
      }
      assert_true(full is Err(ProducerQueueFull))
      assert_eq(producer.pending.length(), 2)
      assert_eq(producer.sends.length(), 2)
      assert_eq(second_peer.sends.length(), 0)
      if closing {
        let close_task = group.spawn(() => producer.close(), no_wait=true)
        while !producer.closed {
          @async.pause()
        }
        gate.release.put(true)
        assert_false(completed.get())
        close_task.wait()
        assert_true(a_wait.wait() is Err(ProducerClosed))
        assert_true(b_wait.wait() is Err(ProducerClosed))
        assert_eq(second_peer.sends.length(), 1)
        assert_eq(second_peer.sends[0], original_a)
        assert_true(producer.closed)
      } else {
        gate.release.put(!retry)
        assert_true(completed.get())
        let final_peer = if retry { third_peer.unwrap() } else { second_peer }
        // Missing B is an internal, bounded failure, not an outer watchdog RED.
        @async.with_timeout(2000, () => {
          final_peer.received.get()
          final_peer.received.get()
        })
        assert_eq(final_peer.sends.length(), 2)
        assert_eq(final_peer.sends[0], original_a)
        assert_eq(final_peer.sends[1], original_b)
        let final_b = decode_frame_body(final_peer.sends[1][4:].to_owned())
        assert_eq(final_b.payload, Some(b"during-replay-B"))
        assert_eq(final_b.cmd.send.unwrap().producer_id, a_command.producer_id)
        assert_eq(final_b.cmd.send.unwrap().sequence_id, b_seq)
        assert_eq(a_entry.deadline, a_deadline)
        assert_eq(b_entry.deadline, b_deadline)
        assert_false(producer.reconnecting.val)
        assert_true(a_wait.try_wait() is None && b_wait.try_wait() is None)
        // Complete B first: sequence correlation must not settle A's future.
        replay_tcp_receipt(final_peer, 1)
        let b_result = b_wait.wait()
        assert_true(b_result is Ok(_))
        assert_eq(b_result.unwrap().sequence_id, b_seq)
        assert_true(a_wait.try_wait() is None)
        assert_eq(producer.pending.length(), 1)
        replay_tcp_receipt(final_peer, 0)
        let a_result = a_wait.wait()
        assert_true(a_result is Ok(_))
        assert_eq(a_result.unwrap().sequence_id, a_seq)
        // A late duplicate from this valid epoch cannot create a third send.
        replay_tcp_receipt(final_peer, 0)
        producer.close()
        assert_eq(final_peer.sends.length(), 2)
      }
      assert_eq(first_peer.epochs, [0UL])
      assert_eq(second_peer.epochs, [1UL])
      assert_eq(first_peer.sends.length(), 1)
      assert_eq(attempts.val, if retry { 2 } else { 1 })
      assert_eq(gate.injected, if retry { 1 } else { 0 })
      assert_eq(gate.active, 0)
      assert_eq(gate.prefix_bytes, 1)
      assert_false(producer.reconnecting.val)
      assert_producer_drained(producer)
      first.close()
      second.close()
      if third_ref.val is Some(third) {
        third.close()
      }
      while !first_peer.finished || !second_peer.finished {
        @async.pause()
      }
      if third_peer is Some(peer) {
        while !peer.finished {
          @async.pause()
        }
        assert_eq(peer.epochs, [2UL])
        assert_eq(second_peer.sends.length(), 0)
      }
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
      if third_ref.val is Some(third) {
        lifecycle_assert_connection_empty(third, limiter)
      }
      assert_true(first.is_closed() && second.is_closed())
      if third_ref.val is Some(third) {
        assert_true(third.is_closed())
      }
    })
  })
}

///|
async test "TCP replay checkpoint admits B and resolves both original futures" {
  replay_tcp_two_send(false, false)
}

///|
async test "TCP partial replay failure retries A then B in the next epoch" {
  replay_tcp_two_send(true, false)
}

///|
async test "TCP close during replay rejects both original futures without sending B" {
  replay_tcp_two_send(false, true)
}
