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
