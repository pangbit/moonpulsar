// A partitioned topic can grow while its producer and consumer stay open.

///|
async test "producer discovers an added partition and routes to it" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/growing-producer"
    let count : Ref[Int] = { val: 1, }
    let routed : @aqueue.Queue[String] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_partitioned_broker(
      group,
      1,
      async fn(_addr, peer) {
        let topics : Map[UInt64, String] = Map([])
        for i in 0..<2 {
          let frame = peer.next()
          guard frame.cmd.producer is Some(req) else {
            fail("expected partition producer")
          }
          assert_eq(req.topic, "\{topic}-partition-\{i}")
          topics[req.producer_id] = req.topic
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
          let success = @proto.CommandProducerSuccess::default()
          success.request_id = req.request_id
          success.producer_name = "growing-producer-\{i}"
          reply.producer_success = Some(success)
          peer.send_command(reply)
        }
        let frame = peer.next()
        guard frame.cmd.send is Some(send) else { fail("expected SEND") }
        routed.put(topics.get(send.producer_id).unwrap_or("missing"))
        send_receipt(peer, send.producer_id, send.sequence_id)
      },
      partitions_ref=count,
    )
    let client = Client::connect(group, "pulsar://\{addr}")
    let producer = client.create_producer(topic, partition_refresh_ms=20)
    assert_eq(producer.partition_count(), 1)
    count.val = 2
    @async.with_timeout(3000, async fn() {
      while producer.partition_count() != 2 {
        @async.sleep(5)
      }
    })
    let mut key = ""
    for i in 0..<100 {
      let candidate = "key-\{i}"
      let hash = xxhash32(@utf8.encode(candidate)).reinterpret_as_int() &
        0x7fffffff
      if hash % 2 == 1 {
        key = candidate
        break
      }
    }
    assert_true(key != "")
    ignore(producer.send(ProducerMessage::new(b"new", key~)))
    assert_eq(routed.get(), "\{topic}-partition-1")
    producer.close()
    client.close()
  })
}

///|
async test "consumer discovers an added partition without recreating it" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/growing-consumer"
    let count : Ref[Int] = { val: 1, }
    let addr = start_partitioned_broker(
      group,
      1,
      async fn(_addr, peer) {
        for i in 0..<2 {
          let frame = peer.next()
          guard frame.cmd.subscribe is Some(sub) else {
            fail("expected partition subscription")
          }
          assert_eq(sub.topic, "\{topic}-partition-\{i}")
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = sub.request_id
          reply.success = Some(success)
          peer.send_command(reply)
          assert_true(peer.next().cmd.flow is Some(_))
          if i == 1 {
            let (cmd, metadata, payload) = message_frame(
              sub.consumer_id,
              3,
              4,
              b"added",
              test_metadata(),
            )
            peer.send_message(cmd, metadata, payload)
          }
        }
      },
      partitions_ref=count,
    )
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_consumer(
      topic,
      "growing-sub",
      partition_refresh_ms=20,
    )
    count.val = 2
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.topic(), "\{topic}-partition-1")
    assert_eq(message.payload(), b"added")
    consumer.close()
    client.close()
  })
}

///|
async test "reader discovers an added partition without recreating it" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/growing-reader"
    let count : Ref[Int] = { val: 1, }
    let addr = start_partitioned_broker(
      group,
      1,
      async fn(_addr, peer) {
        for i in 0..<2 {
          let frame = peer.next()
          guard frame.cmd.subscribe is Some(sub) else {
            fail("expected reader subscription")
          }
          assert_eq(sub.topic, "\{topic}-partition-\{i}")
          assert_eq(sub.durable, Some(false))
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = sub.request_id
          reply.success = Some(success)
          peer.send_command(reply)
          assert_true(peer.next().cmd.flow is Some(_))
          if i == 1 {
            let (cmd, metadata, payload) = message_frame(
              sub.consumer_id,
              5,
              6,
              b"new-partition",
              test_metadata(),
            )
            peer.send_message(cmd, metadata, payload)
          }
        }
      },
      partitions_ref=count,
    )
    let client = Client::connect(group, "pulsar://\{addr}")
    let reader = client.create_reader(
      topic,
      start_message_id=MessageId::earliest(),
      partition_refresh_ms=20,
    )
    count.val = 2
    let message = @async.with_timeout(3000, async fn() { reader.next() })
    guard message is Some(found) else {
      fail("reader ended before new partition")
    }
    assert_eq(found.topic(), "\{topic}-partition-1")
    assert_eq(found.payload(), b"new-partition")
    reader.close()
    client.close()
  })
}
