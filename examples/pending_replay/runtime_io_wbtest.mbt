// One mandatory async test, with two serialized scopes. No real Broker.

///|
suberror IoMockError {
  PeerClosed
} derive(Debug)

///|
struct IoOwned {
  directory : String
  run_id : String
  servers : Array[@socket.TcpServer]
  sockets : Array[@socket.Tcp]
  addresses : Array[String]
}

///|
async fn io_wait(condition : () -> Bool) -> Unit {
  @async.with_timeout(3000, async fn() {
    while !condition() {
      @async.sleep(10)
    }
  })
}

///|
async fn io_listen(owned : IoOwned) -> @socket.TcpServer {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  owned.servers.push(server)
  owned.addresses.push(server.addr.to_string())
  server
}

///|
async fn io_pair(
  owned : IoOwned,
  group : @async.TaskGroup[Unit],
) -> (@socket.Tcp, @socket.Tcp) {
  let server = io_listen(owned)
  let connecting = group.spawn(async fn() { @socket.Tcp::connect(server.addr) })
  let (accepted, _) = server.accept()
  owned.sockets.push(accepted)
  let client = connecting.wait()
  owned.sockets.push(client)
  server.close()
  (client, accepted)
}

///|
async fn io_scope(
  phase : String,
  body : async (IoOwned, @async.TaskGroup[Unit]) -> Unit,
) -> Unit {
  guard phase == "persist" || phase == "client" else {
    raise FixtureError::Rejected("unknown I/O scope")
  }
  let controlled_root = @env.get_env_var("MP_PENDING_IO_ROOT")
  let (directory, run_id) = match controlled_root {
    Some(parent) => {
      guard parent.strip_prefix("/tmp/moonpulsar-pending-io-") is Some(suffix) &&
        valid_nonce(suffix.to_owned()) else {
        raise FixtureError::Rejected("unsafe I/O harness root")
      }
      guard @fs.kind(parent, follow_symlink=false) == @fs.FileKind::Directory &&
        @fs.kind(parent + "/owner-token", follow_symlink=false) ==
        @fs.FileKind::Regular &&
        @fs.file_size(parent + "/owner-token") == 32L else {
        raise FixtureError::Rejected("I/O root ownership marker missing")
      }
      let run_id = suffix.to_owned()
      assert_eq(@fs.read_file(parent + "/owner-token").text(), run_id)
      let directory = parent + "/" + phase
      @fs.mkdir(directory, permission=0o700)
      (directory, run_id)
    }
    None =>
      (
        @fs.tmpdir(prefix="moonpulsar-io-" + phase + "-"),
        "0123456789abcdef0123456789abcdef",
      )
  }
  println("IO_SCOPE_BEGIN \{phase} \{directory}")
  let owned : IoOwned = {
    directory,
    run_id,
    servers: [],
    sockets: [],
    addresses: [],
  }
  let result : Result[Unit, Error] = Ok(
    @async.with_timeout(10000, async fn() {
      @async.with_task_group(async fn(group) { body(owned, group) })
    }),
  ) catch {
    error => Err(error)
  }
  if result is Err(error) {
    println("IO_SCOPE_FAILURE \{phase}: \{error.to_string()}")
    // These are exclusively this scope's synthetic journal records. Preserve
    // the failing stage before removing the owned directory.
    for ordinal in 0..<512 {
      let path = directory + "/event-" + ordinal.to_string() + ".json"
      if @fs.exists(path) {
        println("IO_JOURNAL \{phase} \{@fs.read_file(path).text()}")
      }
    }
  }
  // Only handles and the fresh directory created by this scope are touched.
  for server in owned.servers {
    server.close()
  }
  for socket in owned.sockets {
    socket.close()
  }
  @fs.rmdir(directory, recursive=true)
  assert_false(@fs.exists(directory))
  for address in owned.addresses {
    let probe : Result[@socket.Tcp, Error] = Ok(
      @async.with_timeout(500, async fn() {
        @socket.Tcp::connect(@socket.Addr::parse(address))
      }),
    ) catch {
      error => Err(error)
    }
    match probe {
      Ok(socket) => {
        socket.close()
        fail("owned listener still accepts after cleanup: \{address}")
      }
      Err(@async.TimeoutError) =>
        fail("owned-port cleanup probe did not settle")
      Err(_) => ()
    }
  }
  if controlled_root is Some(parent) {
    let receipt : Json = {
      "phase": phase,
      "run_id": run_id,
      "ports_checked": owned.addresses.length(),
      "directory_absent": true,
      "case_success": result is Ok(_),
    }
    @fs.write_file(
      parent + "/" + phase + ".clean",
      receipt,
      create_mode=CreateNew,
      permission=0o600,
      sync=Full,
    )
  }
  println("IO_SCOPE_CLEAN \{phase} ports=\{owned.addresses.length()}")
  match result {
    Ok(_) => ()
    Err(error) => raise error
  }
}

///|
fn io_journal(owned : IoOwned) -> Journal {
  {
    directory: owned.directory,
    run_id: owned.run_id,
    mutex: @async.Mutex::Mutex(),
    next: 0,
  }
}

///|
async fn io_persistence(
  owned : IoOwned,
  group : @async.TaskGroup[Unit],
) -> Unit {
  let journal = io_journal(owned)
  let gate = new_gate()
  let id = gate.open()
  ignore(gate.client_frame(id, send_record()))
  let state = pending_attempt()
  let ledger = CommandLedger::new()
  let (source, reader) = io_pair(owned, group)
  let (writer, destination) = io_pair(owned, group)
  let closed = Ref(false)
  let locked = Ref(true)
  journal.mutex.acquire()
  defer (if locked.val { journal.mutex.release() })
  let pumping = group.spawn(async fn() {
    // A one-byte read bound forces actual fragmented reads independent of TCP
    // packet coalescing. Two receipts plus PONG share one source write.
    relay_pump(
      reader,
      writer,
      false,
      id,
      gate,
      state,
      journal,
      closed,
      read_chunk_limit=1,
    )
  })
  let receipt = receipt_record()
  let marker = @proto.BaseCommand::default()
  marker.type_ = @proto.BaseCommand_Type::PONG
  marker.pong = Some(@proto.CommandPong::default())
  let pong = @pulsar.encode_command_frame(marker)
  source.write(join_bytes(join_bytes(receipt, receipt), pong))
  io_wait(() => gate.held_count == 1)
  assert_eq(gate.held_persisted, 0)
  assert_false(@fs.exists(owned.directory + "/old-receipt-1.bin"))
  expect_rejected(() => {
    ignore(
      ledger.apply(
        1, "arm_fault", "11111111111111111111111111111111", gate, state,
      ),
    )
  })
  locked.val = false
  journal.mutex.release()
  // Any leaked receipt would be the next frame instead of the positive PONG.
  let observed = @pulsar.read_frame(destination)
  assert_true(observed.cmd.pong is Some(_))
  assert_true(observed.cmd.send_receipt is None)
  assert_eq(gate.held_persisted, 2)
  assert_eq(
    @fs.read_file(owned.directory + "/old-receipt-1.bin").binary(),
    receipt,
  )
  assert_eq(
    @fs.read_file(owned.directory + "/old-receipt-2.bin").binary(),
    receipt,
  )
  assert_false(@fs.exists(owned.directory + "/event-0.json.tmp"))
  assert_true(@fs.exists(owned.directory + "/event-0.json"))
  assert_true(
    ledger.apply(
      1, "arm_fault", "11111111111111111111111111111111", gate, state,
    ),
  )
  source.close()
  pumping.wait()
  assert_eq(gate.live.length(), 0)
}

///|
struct IoPeer {
  tcp : @socket.Tcp
  mutex : @async.Mutex
}

///|
async fn IoPeer::write(
  self : IoPeer,
  raw : Bytes,
  fragment? : Bool = false,
) -> Unit {
  self.mutex.acquire()
  defer self.mutex.release()
  if fragment {
    self.tcp.write(raw[:1])
    @async.pause()
    self.tcp.write(raw[1:3])
    @async.pause()
    self.tcp.write(raw[3:])
  } else {
    self.tcp.write(raw)
  }
}

///|
async fn io_read_exact(tcp : @socket.Tcp, length : Int) -> Bytes {
  let mut raw = b""
  while raw.length() < length {
    guard tcp.read_some(max_len=length - raw.length()) is Some(chunk) else {
      raise IoMockError::PeerClosed
    }
    guard chunk.length() > 0 else {
      raise FixtureError::Rejected("empty non-EOF mock read")
    }
    raw = join_bytes(raw, chunk)
  }
  raw
}

///|
async fn io_read_raw(tcp : @socket.Tcp) -> Bytes {
  // Only actual EOF becomes PeerClosed. I/O errors and cancellation propagate.
  let prefix = io_read_exact(tcp, 4)
  let body = io_read_exact(tcp, total_size(prefix))
  let raw = join_bytes(prefix, body)
  ignore(decode_raw(raw))
  raw
}

///|
struct IoMock {
  producer_name : String
  payload : Bytes
  mut old_peer : IoPeer?
  mut old_session : @async.Task[Unit]?
  mut old_session_done : Bool
  mut old_receipt : Bytes?
  mut original_send : Bytes?
  mut consumer_id : UInt64?
  mut cut_requested : Bool
  mut accepted : Int
  mut sends : Int
  mut lookups : Int
  mut confirmed_acks : Int
}

///|
fn io_message_id(entry : UInt64) -> @proto.MessageIdData {
  let id = @proto.MessageIdData::default()
  id.ledger_id = 9UL
  id.entry_id = entry
  id
}

///|
fn io_send_receipt(send : @proto.CommandSend, entry : UInt64) -> Bytes raise {
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
  let receipt = @proto.CommandSendReceipt::default()
  receipt.producer_id = send.producer_id
  receipt.sequence_id = send.sequence_id
  receipt.message_id = Some(io_message_id(entry))
  command.send_receipt = Some(receipt)
  @pulsar.encode_command_frame(command)
}

///|
async fn io_mock_session(
  peer : IoPeer,
  epoch : Int,
  mock : IoMock,
  decoy : String,
) -> Unit {
  let hello = decode_raw(io_read_raw(peer.tcp))
  assert_true(hello.cmd.connect is Some(_))
  println("IO_MOCK_HANDSHAKE epoch=\{epoch}")
  let connected = @proto.BaseCommand::default()
  connected.type_ = @proto.BaseCommand_Type::CONNECTED
  let info = @proto.CommandConnected::default()
  info.server_version = "owned-local-io-mock"
  info.protocol_version = Some(21)
  connected.connected = Some(info)
  peer.write(@pulsar.encode_command_frame(connected), fragment=true)
  let mut closes = 0
  while true {
    let next : Result[Bytes, Error] = Ok(io_read_raw(peer.tcp)) catch {
      error => Err(error)
    }
    let raw = match next {
      Ok(raw) => raw
      Err(IoMockError::PeerClosed) => {
        guard (epoch == 1 && mock.cut_requested) || (epoch == 2 && closes == 2) else {
          raise FixtureError::Rejected("unplanned mock connection close")
        }
        break
      }
      Err(error) => raise error
    }
    let frame = decode_raw(raw)
    println("IO_MOCK_FRAME epoch=\{epoch} type=\{Repr(frame.cmd.type_)}")
    let reply = @proto.BaseCommand::default()
    if frame.cmd.partition_metadata is Some(request) {
      reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
      let response = @proto.CommandPartitionedTopicMetadataResponse::default()
      response.request_id = request.request_id
      response.partitions = Some(0U)
      reply.partition_metadata_response = Some(response)
    } else if frame.cmd.lookup_topic is Some(request) {
      mock.lookups += 1
      reply.type_ = @proto.BaseCommand_Type::LOOKUP_RESPONSE
      let response = @proto.CommandLookupTopicResponse::default()
      response.request_id = request.request_id
      response.response = Some(
        @proto.CommandLookupTopicResponse_LookupType::Connect,
      )
      response.broker_service_url = Some("pulsar://" + decoy)
      response.proxy_through_service_url = Some(false)
      reply.lookup_topic_response = Some(response)
    } else if frame.cmd.subscribe is Some(request) {
      mock.consumer_id = Some(request.consumer_id)
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = request.request_id
      reply.success = Some(success)
    } else if frame.cmd.producer is Some(request) {
      assert_eq(request.producer_name, Some(mock.producer_name))
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = request.request_id
      success.producer_name = mock.producer_name
      success.last_sequence_id = Some(-1L)
      reply.producer_success = Some(success)
    } else if frame.cmd.send is Some(send) {
      assert_eq(mock.sends, epoch - 1)
      assert_eq(frame.payload, Some(mock.payload))
      mock.sends += 1
      let receipt = io_send_receipt(send, epoch.to_uint64())
      if epoch == 1 {
        mock.original_send = Some(raw)
        mock.old_receipt = Some(receipt)
        let message = @proto.BaseCommand::default()
        message.type_ = @proto.BaseCommand_Type::MESSAGE
        let delivery = @proto.CommandMessage::default()
        delivery.consumer_id = mock.consumer_id.unwrap()
        delivery.message_id = io_message_id(1UL)
        message.message = Some(delivery)
        let metadata = frame.metadata.unwrap()
        metadata.num_messages_in_batch = None
        let payload = @pulsar.encode_send_frame(message, metadata, mock.payload)
        peer.write(join_bytes(receipt, payload), fragment=true)
      } else {
        assert_eq(Some(raw), mock.original_send)
        peer.write(receipt, fragment=true)
      }
      continue
    } else if frame.cmd.ack is Some(ack) {
      assert_eq(epoch, 2)
      let response = @proto.CommandAckResponse::default()
      response.consumer_id = ack.consumer_id
      response.request_id = ack.request_id
      assert_true(response.request_id is Some(_))
      reply.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
      reply.ack_response = Some(response)
      mock.confirmed_acks += 1
    } else if frame.cmd.close_consumer is Some(request) {
      closes += 1
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = request.request_id
      reply.success = Some(success)
    } else if frame.cmd.close_producer is Some(request) {
      closes += 1
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = request.request_id
      reply.success = Some(success)
    } else if frame.cmd.ping is Some(_) {
      reply.type_ = @proto.BaseCommand_Type::PONG
      reply.pong = Some(@proto.CommandPong::default())
    } else if frame.cmd.flow is Some(_) {
      continue
    } else {
      fail("unexpected local mock command")
    }
    peer.write(@pulsar.encode_command_frame(reply))
  }
}

///|
async fn io_command(
  journal : Journal,
  index : Int,
  command : String,
  challenge : String,
) -> Unit {
  let path = journal.directory + "/command-" + index.to_string() + ".txt"
  let text = command +
    "\n" +
    journal.run_id +
    "\n" +
    index.to_string() +
    "\n" +
    challenge +
    "\n"
  @fs.write_file(path + ".tmp", text, create_mode=CreateNew, sync=Full)
  @fs.rename(path + ".tmp", path, replace=false)
}

///|
fn io_field(value : Json, key : String) -> Json? {
  match value {
    Object(fields) => fields.get(key)
    _ => None
  }
}

///|
async fn io_acknowledged(journal : Journal, index : Int) -> Unit {
  println("IO_COMMAND_WAIT index=\{index}")
  @async.with_timeout(3000, async fn() {
    while true {
      for ordinal in 0..<journal.next {
        let path = journal.directory + "/event-" + ordinal.to_string() + ".json"
        let record = @fs.read_file(path).json()
        if io_field(record, "event") is Some(String("command_ack")) &&
          io_field(record, "data") is Some(data) &&
          io_field(data, "index") is Some(Number(value)) &&
          value == index.to_double() {
          println("IO_COMMAND_ACK index=\{index}")
          return
        }
      }
      @async.sleep(10)
    }
  })
}

///|
async fn io_client(owned : IoOwned, group : @async.TaskGroup[Unit]) -> Unit {
  let journal = io_journal(owned)
  let backend = io_listen(owned)
  let decoy = io_listen(owned)
  let bypassed = Ref(false)
  group.spawn_bg(
    async fn() {
      let (socket, _) = decoy.accept()
      owned.sockets.push(socket)
      bypassed.val = true
      socket.close()
    },
    no_wait=true,
  )
  let runtime = start_fixture_runtime(group, backend.addr.to_string(), journal)
  owned.servers.push(runtime.server)
  owned.addresses.push(runtime.gate.frontend)
  let mock : IoMock = {
    producer_name: runtime.producer_name,
    payload: runtime.payload,
    old_peer: None,
    old_session: None,
    old_session_done: false,
    old_receipt: None,
    original_send: None,
    consumer_id: None,
    cut_requested: false,
    accepted: 0,
    sends: 0,
    lookups: 0,
    confirmed_acks: 0,
  }
  let serving = group.spawn(async fn() {
    for epoch in [1, 2] {
      let (tcp, _) = backend.accept()
      owned.sockets.push(tcp)
      let peer : IoPeer = { tcp, mutex: @async.Mutex::Mutex(), }
      println("IO_MOCK_ACCEPT epoch=\{epoch}")
      mock.accepted += 1
      if epoch == 1 {
        mock.old_peer = Some(peer)
      }
      let session = group.spawn(
        async fn() {
          defer tcp.close()
          io_mock_session(peer, epoch, mock, decoy.addr.to_string())
        },
        no_wait=true,
      )
      if epoch == 1 {
        mock.old_session = Some(session)
      }
      session.wait() catch {
        @async.TaskCancelled if epoch == 1 && mock.cut_requested => ()
        error => raise error
      }
      if epoch == 1 {
        mock.old_session_done = true
      }
    }
  })
  let running = group.spawn(async fn() { runtime.run_client(group) })
  println("IO_CLIENT_WAIT first_persisted_receipt")
  io_wait(() => runtime.gate.held_persisted == 1 && runtime.state.waits == 1)
  println("IO_CLIENT_HELD first_persisted_receipt")
  assert_true(runtime.state.pending())
  assert_eq(
    @fs.read_file(owned.directory + "/old-receipt-1.bin").binary(),
    mock.old_receipt.unwrap(),
  )
  io_command(journal, 1, "arm_fault", "11111111111111111111111111111111")
  io_acknowledged(journal, 1)
  let old = mock.old_peer.unwrap()
  old.write(mock.old_receipt.unwrap(), fragment=true)
  io_wait(() => runtime.gate.held_persisted == 2)
  println("IO_CLIENT_HELD duplicate_old_receipt")
  assert_true(runtime.state.pending())
  mock.cut_requested = true
  // Tcp.close detaches the descriptor; it is not cancellation/join of the
  // mock's own suspended reader. Cancel that owned session, whose defer closes
  // the socket, and require its completion before the replacement generation.
  mock.old_session.unwrap().cancel()
  io_wait(() => mock.old_session_done && runtime.gate.old_live == 0)
  println("IO_CLIENT_RETIRED old_generation")
  io_command(journal, 2, "old_gone", "22222222222222222222222222222222")
  io_acknowledged(journal, 2)
  assert_true(runtime.state.pending())
  io_command(
    journal, 3, "grant_replacement", "33333333333333333333333333333333",
  )
  io_acknowledged(journal, 3)
  io_wait(() => {
    runtime.state.completed &&
    runtime.state.acks > 0 &&
    runtime.state.producer_reconnects > 0 &&
    runtime.state.consumer_reconnects > 0
  })
  println("IO_CLIENT_COMPLETED original_future_and_ack")
  assert_true(runtime.state.success)
  assert_eq(mock.sends, 2)
  assert_eq(mock.accepted, 2)
  assert_true(mock.lookups > 0)
  assert_false(bypassed.val)
  assert_eq(runtime.state.attempts, 1)
  assert_eq(runtime.state.waits, 1)
  assert_eq(runtime.gate.held_persisted, 2)
  assert_eq(
    @fs.read_file(owned.directory + "/original-send.bin").binary(),
    @fs.read_file(owned.directory + "/replayed-send.bin").binary(),
  )
  let final_frame = decode_raw(
    @fs.read_file(owned.directory + "/new-receipt.bin").binary(),
  )
  assert_eq(
    final_frame.cmd.send_receipt.unwrap().message_id.unwrap().entry_id,
    2UL,
  )
  io_command(journal, 4, "finish", "44444444444444444444444444444444")
  io_acknowledged(journal, 4)
  let receipt = running.wait()
  assert_eq(receipt.message_id.entry_id, 2UL)
  assert_true(mock.confirmed_acks > 0)
  serving.wait()
  io_wait(() => runtime.gate.live.length() == 0)
}

///|
async test "runtime IO serial persistence routing generation and cleanup" {
  @async.with_timeout(30000, async fn() {
    io_scope("persist", io_persistence)
    io_scope("client", io_client)
  })
}
