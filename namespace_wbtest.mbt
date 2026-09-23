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
