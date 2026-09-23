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
    let client = Client::connect(group, "pulsar://\{lookup_addr}")
    let producer = client.create_producer(
      "persistent://public/default/t",
      properties={ "owner": "test" },
      access_mode=Exclusive,
    )
    let future = producer.send_async(ProducerMessage::new(b"important"))
    let receipt = @async.with_timeout(10000, async fn() { future.wait() })
    assert_eq(receipt.message_id.ledger_id, 10)
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
    let client = Client::connect(group, "pulsar://\{lookup_addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/t", "sub-1",
    )
    let message = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"after")
    message.ack()
    consumer.close()
    client.close()
  })
}
