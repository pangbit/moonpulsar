// Reconnection tests: producer replays unconfirmed messages and the
// consumer re-subscribes after the broker connection breaks.

///|
/// Mock lookup broker: answers the first LOOKUP with `first_addr` and the
/// second with `second_addr`, then drains (answering CLOSE_*).
async fn two_stage_lookup(
  peer : MockPeer,
  first_addr : String,
  second_addr : String,
) -> Unit {
  // partition metadata is only asked once, at creation time
  let meta_frame = peer.next()
  guard meta_frame.cmd.partition_metadata is Some(meta_req) else {
    fail("expected PARTITIONED_METADATA")
  }
  let meta_reply = @proto.BaseCommand::default()
  meta_reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
  let meta_resp = @proto.CommandPartitionedTopicMetadataResponse::default()
  meta_resp.request_id = meta_req.request_id
  meta_resp.partitions = Some(0)
  meta_reply.partition_metadata_response = Some(meta_resp)
  peer.send_command(meta_reply)
  for stage in [first_addr, second_addr] {
    let frame = peer.next()
    guard frame.cmd.lookup_topic is Some(req) else { fail("expected LOOKUP") }
    peer.send_command(
      lookup_reply(
        req.request_id,
        @proto.CommandLookupTopicResponse_LookupType::Connect,
        broker_url="pulsar://\{stage}",
      ),
    )
  }
}

///|
async test "producer reconnects and replays unconfirmed message" {
  @async.with_task_group(async fn(group) {
    // B2: receives the re-registered producer and the replayed SEND
    let b2 = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.producer is Some(req) else {
        fail("expected re-registered PRODUCER")
      }
      assert_eq(req.producer_name, Some("mock-producer"))
      assert_eq(req.user_provided_producer_name, Some(false))
      assert_eq(req.epoch, Some(1UL))
      assert_eq(req.topic_epoch, Some(42UL))
      assert_eq(
        req.producer_access_mode,
        Some(@proto.ProducerAccessMode::Exclusive),
      )
      assert_eq(req.metadata.length(), 1)
      assert_eq(req.metadata[0].key, "owner")
      assert_eq(req.metadata[0].value, "test")
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "mock-producer"
      success.topic_epoch = Some(43UL)
      reply.producer_success = Some(success)
      peer.send_command(reply)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected replayed SEND") }
      guard frame.payload is Some(payload) else { fail("no payload") }
      assert_eq(payload, b"important")
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    // B1: accepts the producer, receives the SEND, then drops the connection
    let b1 = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.producer is Some(req) else {
        fail("expected initial PRODUCER")
      }
      assert_eq(req.epoch, Some(0UL))
      assert_eq(req.topic_epoch, None)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "mock-producer"
      success.topic_epoch = Some(42UL)
      reply.producer_success = Some(success)
      peer.send_command(reply)
      let frame = peer.next()
      guard frame.cmd.send is Some(_) else { fail("expected SEND") }
      // never answer the receipt; kill the connection instead
      peer.tcp.close()
    })
    // L: lookup broker redirecting to B1 then B2
    let lookup_addr = start_mock_broker(group, async fn(_addr, peer) {
      two_stage_lookup(peer, b1, b2)
    })
    let metrics = ClientMetrics::new()
    let observed : Array[String] = []
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{lookup_addr}", on_event=fn(event) {
        metrics.record(event)
        match event {
          ProducerReconnecting(_) => observed.push("reconnecting")
          ProducerReconnected(_, Ok(_)) => observed.push("reconnected")
          SendCompleted(_, _, Ok(_)) => observed.push("sent")
          _ => ()
        }
      }),
    )
    let producer = client.create_producer(
      "persistent://public/default/t",
      properties={ "owner": "test" },
      access_mode=Exclusive,
    )
    let future = producer.send_async(ProducerMessage::new(b"important"))
    let receipt = @async.with_timeout(10000, async fn() { future.wait() })
    assert_eq(receipt.message_id.ledger_id, 10)
    assert_true(observed.contains("reconnecting"))
    assert_true(observed.contains("reconnected"))
    assert_true(observed.contains("sent"))
    assert_eq(metrics.snapshot().sends, 1L)
    assert_eq(metrics.snapshot().producer_reconnects, 1L)
    producer.close()
    client.close()
  })
}

///|
async test "consumer re-subscribes after connection break" {
  @async.with_task_group(async fn(group) {
    // B2: receives the re-subscription and pushes a message
    let b2 = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, metadata, payload) = message_frame(
        1UL,
        42,
        43,
        b"after",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
    })
    // B1: accepts the subscription then drops the connection
    let b1 = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      peer.tcp.close()
    })
    let lookup_addr = start_mock_broker(group, async fn(_addr, peer) {
      two_stage_lookup(peer, b1, b2)
    })
    let metrics = ClientMetrics::new()
    let observed : Array[String] = []
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{lookup_addr}", on_event=fn(event) {
        metrics.record(event)
        match event {
          ConsumerReconnecting(_) => observed.push("reconnecting")
          ConsumerReconnected(_, Ok(_)) => observed.push("reconnected")
          MessageReceived(_) => observed.push("received")
          Acknowledged(_, _, Ok(_)) => observed.push("acked")
          _ => ()
        }
      }),
    )
    let consumer = client.create_consumer(
      "persistent://public/default/t", "sub-1",
    )
    let message = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"after")
    message.ack()
    assert_true(observed.contains("reconnecting"))
    assert_true(observed.contains("reconnected"))
    assert_true(observed.contains("received"))
    assert_true(observed.contains("acked"))
    assert_eq(metrics.snapshot().received, 1L)
    assert_eq(metrics.snapshot().acknowledgments, 1L)
    assert_eq(metrics.snapshot().consumer_reconnects, 1L)
    consumer.close()
    client.close()
  })
}

///|
async test "finite producer reconnect policy fails pending sends and closes" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(_) else { fail("expected SEND") }
      peer.tcp.close()
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let attempts : Ref[Int] = { val: 0, }
    let policy : RetryPolicy = {
      max_retries: 0,
      initial_backoff_ms: 1,
      max_backoff_ms: 1,
    }
    let metrics = ClientMetrics::new()
    let producer = SingleProducer::create_with_memory(
      group,
      conn,
      "persistent://public/default/finite-producer",
      reconnect_with=() => {
        attempts.val += 1
        raise ConnectFailed("forced reconnect failure")
      },
      reconnect_policy=policy,
      on_event=fn(event) { metrics.record(event) },
    )
    let future = producer.send_async(ProducerMessage::new(b"pending"))
    let result : Result[SendReceipt, Error] = Ok(
      @async.with_timeout(3000, async fn() { future.wait() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectionClosed))
    assert_eq(attempts.val, 1)
    assert_eq(metrics.snapshot().producer_reconnects, 1L)
    assert_eq(metrics.snapshot().reconnect_failures, 1L)
    let second : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"after-failure")),
    ) catch {
      e => Err(e)
    }
    assert_true(second is Err(ProducerClosed))
    manager.close()
  })
}

///|
async test "finite consumer reconnect policy closes after exhausted attempt" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      peer.tcp.close()
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let attempts : Ref[Int] = { val: 0, }
    let policy : RetryPolicy = {
      max_retries: 0,
      initial_backoff_ms: 1,
      max_backoff_ms: 1,
    }
    let metrics = ClientMetrics::new()
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/finite-consumer",
      "sub",
      reconnect_with=() => {
        attempts.val += 1
        raise ConnectFailed("forced reconnect failure")
      },
      reconnect_policy=policy,
      on_event=fn(event) { metrics.record(event) },
    )
    let result : Result[Message, Error] = Ok(
      @async.with_timeout(3000, async fn() { consumer.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectionClosed))
    assert_eq(attempts.val, 1)
    assert_eq(metrics.snapshot().consumer_reconnects, 1L)
    assert_eq(metrics.snapshot().reconnect_failures, 1L)
    assert_true(consumer.closed.val)
    manager.close()
  })
}
