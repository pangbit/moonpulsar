// Multi-topic subscriptions merge independently looked-up topics.

///|
async test "multi-topic consumer receives and acknowledges each source topic" {
  @async.with_task_group(async fn(group) {
    let topics = [
      "persistent://public/default/multi-a", "persistent://public/default/multi-b",
    ]
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let ids : Map[String, UInt64] = {}
      for _ in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.subscribe is Some(req) else {
          fail("expected SUBSCRIBE")
        }
        ids[req.topic] = req.consumer_id
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::SUCCESS
        let success = @proto.CommandSuccess::default()
        success.request_id = req.request_id
        reply.success = Some(success)
        peer.send_command(reply)
        assert_true(peer.next().cmd.flow is Some(_))
      }
      let id_a = ids.get(topics[0]).unwrap_or(0UL)
      let id_b = ids.get(topics[1]).unwrap_or(0UL)
      assert_true(id_a != 0UL && id_b != 0UL)
      let (cmd_a, meta_a, body_a) = message_frame(
        id_a,
        1,
        1,
        b"from-a",
        test_metadata(),
      )
      let (cmd_b, meta_b, body_b) = message_frame(
        id_b,
        1,
        2,
        b"from-b",
        test_metadata(),
      )
      peer.send_message(cmd_a, meta_a, body_a)
      peer.send_message(cmd_b, meta_b, body_b)
      for _ in 0..<2 {
        assert_true(peer.next().cmd.ack is Some(_))
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_multi_topic_consumer(
      [topics[0], topics[1], topics[0]],
      "multi-sub",
    )
    let received : Map[String, Bytes] = {}
    for _ in 0..<2 {
      let message = @async.with_timeout(5000, async fn() { consumer.receive() })
      received[message.topic()] = message.payload()
      message.ack()
    }
    assert_eq(received.get(topics[0]), Some(b"from-a"))
    assert_eq(received.get(topics[1]), Some(b"from-b"))
    consumer.close()
    client.close()
  })
}

///|
async test "client rejects invalid consumer and chunking options before lookup" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, _peer) {  })
    let client = Client::connect(group, "pulsar://\{addr}")
    let topic = "persistent://public/default/options"
    let no_topics : Result[Consumer, Error] = Ok(
      client.create_multi_topic_consumer([], "sub"),
    ) catch {
      e => Err(e)
    }
    assert_true(no_topics is Err(InvalidFrame(_)))
    let no_queue : Result[Consumer, Error] = Ok(
      client.create_consumer(topic, "sub", receiver_queue_size=0),
    ) catch {
      e => Err(e)
    }
    assert_true(no_queue is Err(InvalidFrame(_)))
    let negative_timeout : Result[Consumer, Error] = Ok(
      client.create_consumer(topic, "sub", ack_timeout_ms=-1),
    ) catch {
      e => Err(e)
    }
    assert_true(negative_timeout is Err(InvalidFrame(_)))
    let no_refresh : Result[Consumer, Error] = Ok(
      client.create_pattern_consumer(
        "public/default",
        "a.*",
        "sub",
        refresh_interval_ms=0,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(no_refresh is Err(InvalidFrame(_)))
    let no_chunk : Result[Producer, Error] = Ok(
      client.create_producer(topic, chunk_size=0),
    ) catch {
      e => Err(e)
    }
    assert_true(no_chunk is Err(InvalidFrame(_)))
    let batch_chunk : Result[Producer, Error] = Ok(
      client.create_producer(
        topic,
        batching=BatchingConfig::default(),
        chunk_size=3,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(batch_chunk is Err(InvalidFrame(_)))
    let bad_producer_refresh : Result[Producer, Error] = Ok(
      client.create_producer(topic, partition_refresh_ms=0),
    ) catch {
      e => Err(e)
    }
    assert_true(bad_producer_refresh is Err(InvalidFrame(_)))
    let bad_consumer_refresh : Result[Consumer, Error] = Ok(
      client.create_consumer(topic, "sub", partition_refresh_ms=0),
    ) catch {
      e => Err(e)
    }
    assert_true(bad_consumer_refresh is Err(InvalidFrame(_)))
    let bad_reader_refresh : Result[Reader, Error] = Ok(
      client.create_reader(topic, partition_refresh_ms=0),
    ) catch {
      e => Err(e)
    }
    assert_true(bad_reader_refresh is Err(InvalidFrame(_)))
    client.close()
  })
}

///|
async test "multi-topic ack timeout starts when the application receives" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/ack-on-delivery"
    let redelivery : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1,
        1,
        b"queued",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let request = peer.next()
      redelivery.put(request.cmd.redeliver_unacknowledged_messages is Some(_))
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_multi_topic_consumer(
      [topic],
      "ack-delivery-sub",
      ack_timeout_ms=50,
    )
    match consumer.inner {
      ConsumerImpl::Partitioned(state) =>
        @async.with_timeout(2000, async fn() {
          while true {
            match state.queue.try_get() {
              Some(queued) => {
                state.queue.put(queued)
                break
              }
              None => @async.sleep(1)
            }
          }
        })
      ConsumerImpl::Single(_) => fail("expected a composite consumer")
    }
    @async.sleep(150)
    assert_true((redelivery.try_get() catch { _ => None }) is None)
    let message = consumer.receive()
    assert_eq(message.payload(), b"queued")
    assert_true(@async.with_timeout(2000, async fn() { redelivery.get() }))
    consumer.close()
    client.close()
  })
}
