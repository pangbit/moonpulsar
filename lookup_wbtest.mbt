// Topic lookup tests against the in-process mock broker.

///|
fn lookup_reply(
  request_id : UInt64,
  response : @proto.CommandLookupTopicResponse_LookupType,
  broker_url? : String,
) -> @proto.BaseCommand {
  let reply = @proto.BaseCommand::default()
  reply.type_ = @proto.BaseCommand_Type::LOOKUP_RESPONSE
  let resp = @proto.CommandLookupTopicResponse::default()
  resp.request_id = request_id
  resp.response = Some(response)
  if broker_url is Some(url) {
    resp.broker_service_url = Some(url)
  }
  reply.lookup_topic_response = Some(resp)
  reply
}

///|
async test "lookup connect returns connection to owning broker" {
  @async.with_task_group(async fn(group) {
    let peers : @aqueue.Queue[String] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(self_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.lookup_topic is Some(req) else { fail("expected LOOKUP") }
      peers.put("lookup on \{self_addr}")
      // pretend this broker owns the topic
      peer.send_command(
        lookup_reply(
          req.request_id,
          @proto.CommandLookupTopicResponse_LookupType::Connect,
          broker_url="pulsar://\{self_addr}",
        ),
      )
    })
    let manager = ConnectionManager::new(group)
    let conn = lookup_topic(manager, addr, "persistent://public/default/t")
    assert_false(conn.is_closed())
    manager.close()
  })
}

///|
async test "lookup follows redirect to second broker" {
  @async.with_task_group(async fn(group) {
    let connected : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    // second broker: owns the topic
    let addr2 = start_mock_broker(group, async fn(self_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.lookup_topic is Some(req) else { fail("expected LOOKUP") }
      peer.send_command(
        lookup_reply(
          req.request_id,
          @proto.CommandLookupTopicResponse_LookupType::Connect,
          broker_url="pulsar://\{self_addr}",
        ),
      )
      ignore(connected.try_put(true))
    })
    // first broker: redirects to the second
    let addr1 = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.lookup_topic is Some(req) else { fail("expected LOOKUP") }
      peer.send_command(
        lookup_reply(
          req.request_id,
          @proto.CommandLookupTopicResponse_LookupType::Redirect,
          broker_url="pulsar://\{addr2}",
        ),
      )
    })
    let manager = ConnectionManager::new(group)
    let conn = lookup_topic(manager, addr1, "persistent://public/default/t")
    assert_false(conn.is_closed())
    assert_true(connected.get())
    manager.close()
  })
}

///|
async test "lookup failure raises LookupFailed" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.lookup_topic is Some(req) else { fail("expected LOOKUP") }
      let reply = lookup_reply(
        req.request_id,
        @proto.CommandLookupTopicResponse_LookupType::Failed,
      )
      guard reply.lookup_topic_response is Some(resp) else { fail("") }
      resp.message = Some("topic not found")
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let result : Result[Connection, Error] = Ok(
      lookup_topic(manager, addr, "persistent://public/default/missing"),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(LookupFailed(_)))
  })
}

///|
test "broker_addr_of parses schemes" {
  assert_eq(broker_addr_of("pulsar://host:6650"), ("host:6650", false))
  assert_eq(broker_addr_of("pulsar+ssl://host:6651"), ("host:6651", true))
  let result : Result[(String, Bool), Error] = Ok(
    broker_addr_of("http://host:8080"),
  ) catch {
    e => Err(e)
  }
  assert_true(result is Err(LookupFailed(_)))
}
