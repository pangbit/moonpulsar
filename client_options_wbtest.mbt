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
  let invalid_backoff : Result[ClientOptions, Error] = Ok(
    ClientOptions::new("pulsar://localhost:6650", reconnect_policy={
      max_retries: -2,
      initial_backoff_ms: 100,
      max_backoff_ms: 50,
    }),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid_backoff is Err(InvalidFrame(_)))
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
async test "Athenz role token supplier refreshes on broker challenge" {
  @async.with_task_group(async fn(group) {
    let seen : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let challenge = @proto.BaseCommand::default()
      challenge.type_ = @proto.BaseCommand_Type::AUTH_CHALLENGE
      challenge.auth_challenge = Some(@proto.CommandAuthChallenge::default())
      peer.send_command(challenge)
      let reply = peer.next()
      guard reply.cmd.auth_response is Some(response) &&
        response.response is Some(data) else {
        fail("expected Athenz AUTH_RESPONSE")
      }
      seen.put(
        data.auth_method_name == Some("athenz") &&
        data.auth_data == Some(b"role-2"),
      )
    })
    let calls : Ref[Int] = { val: 0, }
    let provider = athenz_role_token_provider(async fn() {
      calls.val += 1
      "role-\{calls.val}"
    })
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{addr}", auth_provider=provider),
    )
    assert_true(@async.with_timeout(1000, async fn() { seen.get() }))
    assert_eq(calls.val, 2)
    client.close()
  })
}

///|
async test "Athenz role token file rotates and empty tokens fail" {
  let dir = @fs.tmpdir(prefix="moonpulsar-athenz-")
  let path = dir + "/role-token"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.rmdir(dir) catch {
      _ => ()
    }
  }
  @fs.write_file(path, "first\n")
  let provider = athenz_role_token_file_provider(path)
  assert_eq(provider().method_name, "athenz")
  assert_eq(provider().data, b"first")
  @fs.write_file(path, "second\n")
  assert_eq(provider().data, b"second")
  @fs.write_file(path, "  \n")
  let empty : Result[AuthData, Error] = Ok(provider()) catch { e => Err(e) }
  assert_true(empty is Err(AuthenticationFailed(_)))
}

///|
async test "OAuth2 client credentials encode form and refresh tokens" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      async fn(request, payload) {
        assert_eq(request.meth, @http.RequestMethod::Post)
        assert_eq(request.path, "/token")
        assert_eq(
          @utf8.decode(payload),
          "grant_type=client_credentials&client_id=client%20id&client_secret=sec%26ret&audience=urn%3Atest&scope=read%20write",
        )
        calls.val += 1
        (200, "{\"access_token\":\"token-\{calls.val}\",\"expires_in\":60}")
      },
      async fn(url) {
        let provider = oauth2_client_credentials_provider(
          url + "/token",
          "client id",
          "sec&ret",
          audience="urn:test",
          scope="read write",
        )
        assert_eq(provider().data, b"token-1")
        assert_eq(provider().data, b"token-2")
        assert_eq(calls.val, 2)
      },
    )
  })
}

///|
async test "OAuth2 token endpoint failures reject authentication" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        match request.path {
          "/denied" => (401, "private error")
          "/malformed" => (200, "not JSON")
          _ => (200, "{\"token_type\":\"Bearer\"}")
        }
      },
      async fn(url) {
        for path in ["denied", "malformed", "missing"] {
          let provider = oauth2_client_credentials_provider(
            url + "/" + path,
            "client",
            "secret",
          )
          let result : Result[AuthData, Error] = Ok(provider()) catch {
            e => Err(e)
          }
          match path {
            "denied" => assert_true(result is Err(ServerError(code=401, ..)))
            _ => assert_true(result is Err(InvalidFrame(_)))
          }
        }
      },
    )
  })
  let invalid : Result[async () -> AuthData, Error] = Ok(
    oauth2_client_credentials_provider("", "client", "secret"),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
}

///|
async test "OAuth2 provider refreshes for a broker authentication challenge" {
  @async.with_task_group(async fn(group) {
    let issued : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      async fn(_request, _payload) {
        issued.val += 1
        (200, "{\"access_token\":\"token-\{issued.val}\"}")
      },
      async fn(url) {
        let seen : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
          kind=@aqueue.Kind::Unbounded,
        )
        let addr = start_mock_broker(group, async fn(_addr, peer) {
          let challenge = @proto.BaseCommand::default()
          challenge.type_ = @proto.BaseCommand_Type::AUTH_CHALLENGE
          challenge.auth_challenge = Some(
            @proto.CommandAuthChallenge::default(),
          )
          peer.send_command(challenge)
          let reply = peer.next()
          guard reply.cmd.auth_response is Some(response) &&
            response.response is Some(data) else {
            fail("expected OAuth2 AUTH_RESPONSE")
          }
          seen.put(data.auth_data == Some(b"token-2"))
        })
        let provider = oauth2_client_credentials_provider(
          url + "/token",
          "client",
          "secret",
        )
        let client = Client::connect_with_options(
          group,
          ClientOptions::new("pulsar://\{addr}", auth_provider=provider),
        )
        assert_true(@async.with_timeout(1000, async fn() { seen.get() }))
        assert_eq(issued.val, 2)
        client.close()
      },
    )
  })
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

///|
async test "client reconnect policy reaches producers and consumers" {
  @async.with_task_group(async fn(group) {
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      expect_producer(peer)
      expect_subscribe(peer)
    })
    let policy : RetryPolicy = {
      max_retries: 0,
      initial_backoff_ms: 5,
      max_backoff_ms: 10,
    }
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{addr}", reconnect_policy=policy),
    )
    let producer = client.create_producer(
      "persistent://public/default/reconnect-options",
    )
    guard producer.inner is ProducerImpl::Single(single_producer) else {
      fail("expected single producer")
    }
    assert_eq(single_producer.reconnect_policy.max_retries, 0)
    let consumer = client.create_consumer(
      "persistent://public/default/reconnect-options", "sub",
    )
    guard consumer.inner is ConsumerImpl::Single(single_consumer) else {
      fail("expected single consumer")
    }
    assert_eq(single_consumer.reconnect_policy.initial_backoff_ms, 5)
    consumer.close()
    producer.close()
    client.close()
  })
}
