// Connection layer tests against the in-process mock broker.

///|
async test "handshake completes and reports server version" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, _peer) {  })
    let conn = Connection::connect(group, addr)
    assert_eq(conn.server_version(), "mock-broker 1.0")
    assert_eq(conn.remote_protocol_version(), 21)
    assert_false(conn.is_closed())
    conn.close()
  })
}

///|
async test "handshake retains broker maximum message size" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(
      group,
      async fn(_addr, _peer) {  },
      max_message_size=1048576,
    )
    let conn = Connection::connect(group, addr)
    assert_eq(conn.max_message_size(), Some(1048576))
    conn.close()
  })
}

///|
test "basic authentication uses Pulsar command credentials" {
  let basic = BasicAuth::new("alice", "secret")
  assert_eq(basic.method_name(), "basic")
  assert_eq(basic.auth_data(), b"alice:secret")
  let missing : Result[BasicAuth, Error] = Ok(BasicAuth::new("", "secret")) catch {
    e => Err(e)
  }
  assert_true(missing is Err(InvalidFrame(_)))
}

///|
async test "authentication challenge during handshake gets a response" {
  @async.with_task_group(async fn(group) {
    let server = @socket.TcpServer::TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
    )
    let addr = server.addr.to_string()
    group.spawn_bg(
      async fn() {
        defer server.close()
        let (tcp, _) = server.accept()
        defer tcp.close()
        let hello = read_frame(tcp)
        guard hello.cmd.connect is Some(connect) else {
          fail("expected CONNECT")
        }
        assert_eq(connect.auth_data, Some(b"token-1"))
        let challenge = @proto.BaseCommand::default()
        challenge.type_ = @proto.BaseCommand_Type::AUTH_CHALLENGE
        challenge.auth_challenge = Some(@proto.CommandAuthChallenge::default())
        tcp.write(encode_command_frame(challenge))
        let answer = read_frame(tcp)
        guard answer.cmd.auth_response is Some(response) &&
          response.response is Some(data) else {
          fail("expected AUTH_RESPONSE")
        }
        assert_eq(data.auth_method_name, Some("token"))
        assert_eq(data.auth_data, Some(b"token-2"))
        let connected = @proto.BaseCommand::default()
        connected.type_ = @proto.BaseCommand_Type::CONNECTED
        let info = @proto.CommandConnected::default()
        info.server_version = "mock-broker"
        info.protocol_version = Some(21)
        connected.connected = Some(info)
        tcp.write(encode_command_frame(connected))
      },
      no_wait=true,
      allow_failure=true,
    )
    let calls : Ref[Int] = { val: 0, }
    let conn = Connection::connect(group, addr, auth_provider=fn() {
      calls.val += 1
      { method_name: "token", data: @utf8.encode("token-\{calls.val}"), }
    })
    assert_eq(conn.server_version(), "mock-broker")
    assert_eq(calls.val, 2)
    conn.close()
  })
}

///|
async test "asynchronous authentication provider refreshes on challenge" {
  @async.with_task_group(async fn(group) {
    let seen : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let challenge = @proto.BaseCommand::default()
      challenge.type_ = @proto.BaseCommand_Type::AUTH_CHALLENGE
      challenge.auth_challenge = Some(@proto.CommandAuthChallenge::default())
      peer.send_command(challenge)
      let answer = peer.next()
      guard answer.cmd.auth_response is Some(response) &&
        response.response is Some(data) else {
        fail("expected AUTH_RESPONSE")
      }
      seen.put(data.auth_data == Some(b"token-2"))
    })
    let calls : Ref[Int] = { val: 0, }
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{addr}", auth_provider=async fn() {
        calls.val += 1
        @async.sleep(1)
        { method_name: "token", data: @utf8.encode("token-\{calls.val}"), }
      }),
    )
    assert_true(@async.with_timeout(1000, async fn() { seen.get() }))
    assert_eq(calls.val, 2)
    client.close()
  })
}

///|
async test "authentication challenge after handshake gets a response" {
  @async.with_task_group(async fn(group) {
    let seen : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let challenge = @proto.BaseCommand::default()
      challenge.type_ = @proto.BaseCommand_Type::AUTH_CHALLENGE
      challenge.auth_challenge = Some(@proto.CommandAuthChallenge::default())
      peer.send_command(challenge)
      let frame = peer.next()
      guard frame.cmd.auth_response is Some(response) &&
        response.response is Some(data) else {
        fail("expected AUTH_RESPONSE")
      }
      seen.put(data.auth_data == Some(b"test-token"))
    })
    let conn = Connection::connect(group, addr, auth={
      method_name: "token",
      data: b"test-token",
    })
    assert_true(@async.with_timeout(1000, async fn() { seen.get() }))
    conn.close()
  })
}

///|
async test "request/response correlation by request id" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.type_ is @proto.BaseCommand_Type::PARTITIONED_METADATA else {
        fail("expected PARTITIONED_METADATA")
      }
      guard frame.cmd.partition_metadata is Some(req) else {
        fail("missing partition metadata command")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
      let resp = @proto.CommandPartitionedTopicMetadataResponse::default()
      resp.request_id = req.request_id
      resp.partitions = Some(3)
      reply.partition_metadata_response = Some(resp)
      peer.send_command(reply)
    })
    let conn = Connection::connect(group, addr)
    let req = @proto.CommandPartitionedTopicMetadata::default()
    req.topic = "persistent://public/default/my-topic"
    req.request_id = conn.next_request_id()
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA
    cmd.partition_metadata = Some(req)
    let reply = conn.request(cmd)
    guard reply.partition_metadata_response is Some(resp) else {
      fail("missing response")
    }
    assert_eq(resp.partitions, Some(3))
    conn.close()
  })
}

///|
async test "broker error surfaces as ServerError" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.partition_metadata is Some(req) else {
        fail("missing request")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::ERROR
      let err = @proto.CommandError::default()
      err.request_id = req.request_id
      err.error = @proto.ServerError::MetadataError
      err.message = "topic not found"
      reply.error = Some(err)
      peer.send_command(reply)
    })
    let conn = Connection::connect(group, addr)
    let req = @proto.CommandPartitionedTopicMetadata::default()
    req.topic = "persistent://public/default/missing"
    req.request_id = conn.next_request_id()
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA
    cmd.partition_metadata = Some(req)
    let result : Result[@proto.BaseCommand, Error] = Ok(conn.request(cmd)) catch {
      e => Err(e)
    }
    assert_true(result is Err(ServerError(message="topic not found", ..)))
    conn.close()
  })
}

///|
async test "client answers broker PING with PONG" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let ping = @proto.BaseCommand::default()
      ping.type_ = @proto.BaseCommand_Type::PING
      ping.ping = Some(@proto.CommandPing::default())
      peer.send_command(ping)
      let reply = peer.next()
      assert_eq(reply.cmd.type_, @proto.BaseCommand_Type::PONG)
    })
    let conn = Connection::connect(group, addr)
    // give the reader task a chance to answer
    @async.sleep(100)
    conn.close()
  })
}

///|
async test "pending request fails when connection breaks" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      // read one request then close abruptly
      let _ = peer.next()
      peer.tcp.close()
    })
    let conn = Connection::connect(group, addr)
    let req = @proto.CommandPartitionedTopicMetadata::default()
    req.topic = "persistent://public/default/t"
    req.request_id = conn.next_request_id()
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA
    cmd.partition_metadata = Some(req)
    let result : Result[@proto.BaseCommand, Error] = Ok(conn.request(cmd)) catch {
      e => Err(e)
    }
    assert_true(result is Err(_))
    @async.sleep(50)
    assert_true(conn.is_closed())
  })
}

///|
async test "malformed broker response closes connection and releases request" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let _ = peer.next()
      peer.tcp.write(b"\x00\x00\x00\x04\xff\xff\xff\xff")
    })
    let conn = Connection::connect(group, addr)
    let req = @proto.CommandPartitionedTopicMetadata::default()
    req.topic = "persistent://public/default/malformed"
    req.request_id = conn.next_request_id()
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA
    cmd.partition_metadata = Some(req)
    let result : Result[@proto.BaseCommand, Error] = Ok(
      @async.with_timeout(1000, async fn() { conn.request(cmd) }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(InvalidFrame(_)))
    assert_true(conn.is_closed())
    assert_true(conn.pending.is_empty())
    conn.close()
  })
}

///|
async test "handshake rejects CONNECTED without broker information" {
  @async.with_task_group(async fn(group) {
    let server = @socket.TcpServer::TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
    )
    let addr = server.addr.to_string()
    group.spawn_bg(
      async fn() {
        defer server.close()
        let (tcp, _) = server.accept()
        defer tcp.close()
        let _ = read_frame(tcp)
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::CONNECTED
        tcp.write(encode_command_frame(reply))
      },
      no_wait=true,
      allow_failure=true,
    )
    let result : Result[Connection, Error] = Ok(
      @async.with_timeout(1000, async fn() { Connection::connect(group, addr) }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectFailed(_)))
  })
}

///|
async test "handshake rejects frame shorter than command header" {
  @async.with_task_group(async fn(group) {
    let server = @socket.TcpServer::TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
    )
    let addr = server.addr.to_string()
    group.spawn_bg(
      async fn() {
        defer server.close()
        let (tcp, _) = server.accept()
        defer tcp.close()
        let _ = read_frame(tcp)
        tcp.write(b"\x00\x00\x00\x00")
      },
      no_wait=true,
      allow_failure=true,
    )
    let result : Result[Connection, Error] = Ok(
      @async.with_timeout(1000, async fn() { Connection::connect(group, addr) }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(InvalidFrame(_)))
  })
}

///|
async test "connection refusal returns ConnectFailed" {
  @async.with_task_group(async fn(group) {
    let result : Result[Connection, Error] = Ok(
      @async.with_timeout(1000, async fn() {
        Connection::connect(group, "127.0.0.1:0", connection_timeout_ms=500)
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectFailed(_)))
  })
}
