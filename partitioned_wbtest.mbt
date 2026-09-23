// Partitioned topic tests: client-side routing and aggregation over
// per-partition producers/consumers.

///|
/// Mock broker answering partition metadata with `partitions` and all
/// lookups with Connect to itself, then runs `on_peer`.
async fn start_partitioned_broker(
  group : @async.TaskGroup[Unit],
  partitions : Int,
  on_peer : async (String, MockPeer) -> Unit,
  partitions_ref? : Ref[Int],
) -> String {
  // metadata/lookup answers must keep working while the scenario runs,
  // because partition creation interleaves lookups and PRODUCER/SUBSCRIBE
  let data_q : @aqueue.Queue[IncomingFrame] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Unbounded,
  )
  let addr = start_mock_broker(group, async fn(addr, peer) {
    let data_peer : MockPeer = {
      tcp: peer.tcp,
      received: data_q,
      stash: { val: None, },
    }
    group.spawn_bg(
      async fn() {
        while true {
          let frame = peer.next()
          println("DEBUG route: frame type \{frame.cmd.type_.to_enum().0}")
          if frame.cmd.partition_metadata is Some(req) {
            let reply = @proto.BaseCommand::default()
            reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
            let resp = @proto.CommandPartitionedTopicMetadataResponse::default()
            resp.request_id = req.request_id
            let count = match partitions_ref {
              Some(current) => current.val
              None => partitions
            }
            resp.partitions = Some(count.reinterpret_as_uint())
            reply.partition_metadata_response = Some(resp)
            peer.send_command(reply)
          } else if frame.cmd.lookup_topic is Some(req) {
            peer.send_command(
              lookup_reply(
                req.request_id,
                @proto.CommandLookupTopicResponse_LookupType::Connect,
                broker_url="pulsar://\{addr}",
              ),
            )
          } else {
            data_q.put(frame)
          }
        }
      },
      no_wait=true,
      allow_failure=true,
    )
    on_peer(addr, data_peer)
    drain_closes(data_peer)
  })
  addr
}

///|
async test "partitioned producer routes keyed messages consistently" {
  @async.with_task_group(async fn(group) {
    let got : @aqueue.Queue[(String, Bytes)] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_partitioned_broker(group, 2, async fn(_addr, peer) {
      let topics : Map[UInt64, String] = {}
      // 2 PRODUCER + 4 SEND
      for _ in 0..<6 {
        let frame = peer.next()
        if frame.cmd.producer is Some(req) {
          assert_true(req.schema is Some(_))
          topics[req.producer_id] = req.topic
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
          let success = @proto.CommandProducerSuccess::default()
          success.request_id = req.request_id
          success.producer_name = "p"
          reply.producer_success = Some(success)
          peer.send_command(reply)
        } else if frame.cmd.send is Some(send) {
          guard frame.payload is Some(payload) else { fail("no payload") }
          got.put((topics.get(send.producer_id).unwrap_or("?"), payload))
          send_receipt(peer, send.producer_id, send.sequence_id)
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    println("DEBUG: connected")
    let producer = client.create_producer(
      "persistent://public/default/pt",
      schema=SchemaInfo::string(),
    )
    println("DEBUG: producer created")
    ignore(producer.send(ProducerMessage::new(b"a1", key="alpha")))
    ignore(producer.send(ProducerMessage::new(b"b1", key="beta")))
    ignore(producer.send(ProducerMessage::new(b"a2", key="alpha")))
    ignore(producer.send(ProducerMessage::new(b"b2", key="beta")))
    let by_payload : Map[String, String] = {}
    for _ in 0..<4 {
      let (topic, payload) = got.get()
      by_payload[@utf8.decode(payload)] = topic
    }
    // same key must always land on the same partition topic
    assert_true(by_payload.get("a1") is Some(_))
    assert_eq(by_payload.get("a1"), by_payload.get("a2"))
    assert_eq(by_payload.get("b1"), by_payload.get("b2"))
    producer.close()
    client.close()
  })
}

///|
async test "partitioned consumer merges all partitions" {
  @async.with_task_group(async fn(group) {
    let addr = start_partitioned_broker(group, 2, async fn(_addr, peer) {
      // two SUBSCRIBE + two FLOW on the shared connection
      for _ in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.subscribe is Some(req) else {
          fail("expected SUBSCRIBE")
        }
        assert_true(req.schema is Some(_))
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::SUCCESS
        let success = @proto.CommandSuccess::default()
        success.request_id = req.request_id
        reply.success = Some(success)
        peer.send_command(reply)
        let flow = peer.next()
        assert_true(flow.cmd.flow is Some(_))
      }
      // consumer ids on this connection: 3 and 6
      let (cmd0, meta0, p0) = message_frame(
        3UL,
        1,
        1,
        b"from-p0",
        test_metadata(),
      )
      peer.send_message(cmd0, meta0, p0)
      let (cmd1, meta1, p1) = message_frame(
        6UL,
        2,
        2,
        b"from-p1",
        test_metadata(),
      )
      peer.send_message(cmd1, meta1, p1)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/pt",
      "sub-1",
      schema=SchemaInfo::string(),
    )
    let received = []
    for _ in 0..<2 {
      let message = @async.with_timeout(5000, async fn() { consumer.receive() })
      received.push(message.payload())
      message.ack()
    }
    received.sort()
    assert_eq(received, [b"from-p0", b"from-p1"])
    consumer.close()
    client.close()
  })
}
