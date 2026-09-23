///|
test "client options reject invalid timeouts and competing authentication" {
  let invalid : Result[ClientOptions, Error] = Ok(
    ClientOptions::new("pulsar://localhost:6650", operation_timeout_ms=0),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
  let token = TokenAuth::new("one")
  let competing : Result[ClientOptions, Error] = Ok(
    ClientOptions::new("pulsar://localhost:6650", auth=token, auth_provider=async fn() {
      { method_name: "token", data: b"two", }
    }),
  ) catch {
    e => Err(e)
  }
  assert_true(competing is Err(InvalidFrame(_)))
}

///|
async test "token file provider reloads the token" {
  let dir = @fs.tmpdir(prefix="moonpulsar-token-")
  let path = dir + "/token"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.rmdir(dir) catch {
      _ => ()
    }
  }
  @fs.write_file(path, "first\n")
  let provider = token_file_provider(path)
  assert_eq(provider().data, b"first")
  @fs.write_file(path, "second\n")
  assert_eq(provider().data, b"second")
}

///|
async test "client options propagate listener and lookup properties" {
  @async.with_task_group(async fn(group) {
    let checked : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(self_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.lookup_topic is Some(req) else { fail("expected LOOKUP") }
      checked.put(
        req.advertised_listener_name == Some("internal") &&
        properties_of(req.properties).get("region") == Some("test"),
      )
      peer.send_command(
        lookup_reply(
          req.request_id,
          @proto.CommandLookupTopicResponse_LookupType::Connect,
          broker_url="pulsar://\{self_addr}",
        ),
      )
    })
    let options = ClientOptions::new(
      "pulsar://\{addr}",
      listener_name="internal",
      lookup_properties={ "region": "test" },
    )
    let client = Client::connect_with_options(group, options)
    ignore(lookup_topic(client.manager, addr, "persistent://public/default/t"))
    assert_true(checked.get())
    client.close()
  })
}

///|
async test "client operation timeout applies to broker requests" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      ignore(peer.next())
      @async.sleep(100)
    })
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{addr}", operation_timeout_ms=20),
    )
    let conn = client.manager.get(addr)
    let req = @proto.CommandPartitionedTopicMetadata::default()
    req.topic = "persistent://public/default/t"
    req.request_id = conn.next_request_id()
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA
    cmd.partition_metadata = Some(req)
    let result : Result[@proto.BaseCommand, Error] = Ok(conn.request(cmd)) catch {
      e => Err(e)
    }
    assert_true(result is Err(RequestTimeout))
    client.close()
  })
}
