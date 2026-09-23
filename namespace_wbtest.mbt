// Namespace discovery for pattern subscriptions.

///|
async test "namespace lookup sends pattern and topic mode" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.get_topics_of_namespace is Some(req) else {
        fail("expected GET_TOPICS_OF_NAMESPACE")
      }
      assert_eq(req.namespace_, "public/default")
      assert_eq(req.topics_pattern, None)
      assert_eq(
        req.mode,
        Some(@proto.CommandGetTopicsOfNamespace_Mode::PERSISTENT),
      )
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
      let resp = @proto.CommandGetTopicsOfNamespaceResponse::default()
      resp.request_id = req.request_id
      resp.topics = [
        "persistent://public/default/abc", "persistent://public/default/other",
      ]
      reply.get_topics_of_namespace_response = Some(resp)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let topics = get_topics_of_namespace(
      manager,
      addr,
      "public/default",
      pattern="persistent://public/default/a.*",
    )
    assert_eq(topics, ["persistent://public/default/abc"])
    manager.close()
  })
}

///|
test "partition suffix normalization only strips numeric partitions" {
  assert_eq(
    base_topic_name("persistent://public/default/name-partition-12"),
    "persistent://public/default/name",
  )
  assert_eq(
    base_topic_name("persistent://public/default/name-partition-later"),
    "persistent://public/default/name-partition-later",
  )
}

///|
async test "pattern consumer discovers a topic after subscription" {
  @async.with_task_group(async fn(group) {
    let topic_a = "persistent://public/default/pattern-a"
    let topic_b = "persistent://public/default/pattern-b"
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      for round in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.get_topics_of_namespace is Some(req) else {
          fail("expected namespace discovery")
        }
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
        let resp = @proto.CommandGetTopicsOfNamespaceResponse::default()
        resp.request_id = req.request_id
        resp.topics = if round == 0 {
          [topic_a]
        } else {
          [topic_a, topic_b, "persistent://public/default/other"]
        }
        reply.get_topics_of_namespace_response = Some(resp)
        peer.send_command(reply)
        let frame = peer.next()
        guard frame.cmd.subscribe is Some(sub) else {
          fail("expected topic subscription")
        }
        assert_eq(sub.topic, if round == 0 { topic_a } else { topic_b })
        let ack = @proto.BaseCommand::default()
        ack.type_ = @proto.BaseCommand_Type::SUCCESS
        let success = @proto.CommandSuccess::default()
        success.request_id = sub.request_id
        ack.success = Some(success)
        peer.send_command(ack)
        assert_true(peer.next().cmd.flow is Some(_))
        if round == 1 {
          let (cmd, meta, body) = message_frame(
            sub.consumer_id,
            1,
            1,
            b"discovered",
            test_metadata(),
          )
          peer.send_message(cmd, meta, body)
          assert_true(peer.next().cmd.ack is Some(_))
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_pattern_consumer(
      "public/default",
      "^persistent://public/default/pattern-.*$",
      "pattern-sub",
      refresh_interval_ms=20,
    )
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_true(consumer.topics().contains(topic_a))
    assert_true(consumer.topics().contains(topic_b))
    assert_eq(message.topic(), topic_b)
    assert_eq(message.payload(), b"discovered")
    message.ack()
    consumer.close()
    client.close()
  })
}

///|
async test "pattern consumer closes removed topics and discards stale queued messages" {
  @async.with_task_group(async fn(group) {
    let topic_a = "persistent://public/default/removal-a"
    let topic_b = "persistent://public/default/removal-b"
    let ready : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let readd : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.get_topics_of_namespace is Some(first) else {
        fail("expected initial discovery")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
      let response = @proto.CommandGetTopicsOfNamespaceResponse::default()
      response.request_id = first.request_id
      response.topics = [topic_a, topic_b]
      reply.get_topics_of_namespace_response = Some(response)
      peer.send_command(reply)
      let mut first_id = 0UL
      for i in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.subscribe is Some(sub) else {
          fail("expected topic subscription")
        }
        assert_eq(sub.topic, if i == 0 { topic_a } else { topic_b })
        if i == 0 {
          first_id = sub.consumer_id
        }
        let success = @proto.BaseCommand::default()
        success.type_ = @proto.BaseCommand_Type::SUCCESS
        let response = @proto.CommandSuccess::default()
        response.request_id = sub.request_id
        success.success = Some(response)
        peer.send_command(success)
        assert_true(peer.next().cmd.flow is Some(_))
      }
      let (cmd, metadata, payload) = message_frame(
        first_id,
        1,
        1,
        b"stale",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      ignore(ready.get())
      let frame = peer.next()
      guard frame.cmd.get_topics_of_namespace is Some(next) else {
        fail("expected refreshed discovery")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
      let response = @proto.CommandGetTopicsOfNamespaceResponse::default()
      response.request_id = next.request_id
      response.topics = [topic_b]
      reply.get_topics_of_namespace_response = Some(response)
      peer.send_command(reply)
      let frame = peer.next()
      guard frame.cmd.close_consumer is Some(closed) else {
        fail("expected removed source to close")
      }
      assert_eq(closed.consumer_id, first_id)
      let success = @proto.BaseCommand::default()
      success.type_ = @proto.BaseCommand_Type::SUCCESS
      let response = @proto.CommandSuccess::default()
      response.request_id = closed.request_id
      success.success = Some(response)
      peer.send_command(success)
      let frame = peer.next()
      guard frame.cmd.get_topics_of_namespace is Some(third) else {
        fail("expected discovery after removal")
      }
      ignore(readd.get())
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
      let response = @proto.CommandGetTopicsOfNamespaceResponse::default()
      response.request_id = third.request_id
      response.topics = [topic_a, topic_b]
      reply.get_topics_of_namespace_response = Some(response)
      peer.send_command(reply)
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(again) else {
        fail("expected re-added source to subscribe")
      }
      assert_eq(again.topic, topic_a)
      let success = @proto.BaseCommand::default()
      success.type_ = @proto.BaseCommand_Type::SUCCESS
      let response = @proto.CommandSuccess::default()
      response.request_id = again.request_id
      success.success = Some(response)
      peer.send_command(success)
      assert_true(peer.next().cmd.flow is Some(_))
      let (cmd, metadata, payload) = message_frame(
        again.consumer_id,
        2,
        1,
        b"fresh",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_pattern_consumer(
      "public/default",
      "^persistent://public/default/removal-.*$",
      "removal-sub",
      refresh_interval_ms=20,
    )
    match consumer.inner {
      ConsumerImpl::Single(_) => fail("expected pattern composite")
      ConsumerImpl::Partitioned(state) =>
        @async.with_timeout(3000, async fn() {
          while true {
            match state.queue.try_get() {
              Some(message) => {
                state.queue.put(message)
                break
              }
              None => @async.sleep(1)
            }
          }
        })
    }
    ready.put(true)
    @async.with_timeout(3000, async fn() {
      while consumer.topics().contains(topic_a) {
        @async.sleep(1)
      }
    })
    assert_eq(consumer.topics(), [topic_b])
    assert_true(consumer.try_receive() is None)
    readd.put(true)
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.topic(), topic_a)
    assert_eq(message.payload(), b"fresh")
    consumer.close()
    client.close()
  })
}
