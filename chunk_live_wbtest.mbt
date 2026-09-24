// Optional real-broker regression. Run with PULSAR_LIVE_CHUNK_URL,
// PULSAR_LIVE_CHUNK_ADMIN_URL, and a fresh PULSAR_LIVE_CHUNK_TOPIC.

///|
fn live_chunk_acknowledged(stats : Json, subscription : String) -> Bool raise {
  guard stats is Object(root) &&
    root.get("msgOutCounter") is Some(Number(sent, ..)) &&
    root.get("subscriptions") is Some(Object(subscriptions)) &&
    subscriptions.get(subscription) is Some(Object(state)) &&
    state.get("msgBacklog") is Some(Number(backlog, ..)) &&
    state.get("lastMarkDeleteAdvancedTimestamp") is Some(Number(acked_at, ..)) else {
    raise InvalidFrame("broker topic stats omit chunk ACK evidence")
  }
  sent >= 1.0 && backlog == 0.0 && acked_at > 0.0
}

///|
async test "live broker ACKs an expired incomplete chunk" {
  guard @env.get_env_var("PULSAR_LIVE_CHUNK_URL") is Some(url) &&
    @env.get_env_var("PULSAR_LIVE_CHUNK_ADMIN_URL") is Some(admin_url) &&
    @env.get_env_var("PULSAR_LIVE_CHUNK_TOPIC") is Some(topic) else {
    return
  }
  @async.with_task_group(async fn(group) {
    let client = Client::connect(group, url)
    let subscription = "chunk-expiry-live"
    let consumer = client.create_consumer(
      topic,
      subscription,
      initial_position=Earliest,
      chunk_policy=ChunkAssemblyPolicy::new(
        expiry_ms=5000,
        auto_ack_incomplete=true,
      ),
    )
    guard consumer.inner is ConsumerImpl::Single(single_consumer) else {
      fail("expected a single-topic consumer")
    }
    let producer = client.create_producer(topic)
    guard producer.inner is ProducerImpl::Single(single_producer) else {
      fail("expected a single-topic producer")
    }
    let metadata = single_producer.build_metadata(
      ProducerMessage::new(b"first"),
      0UL,
    )
    metadata.uuid = Some("incomplete-live")
    metadata.num_chunks_from_msg = Some(2)
    metadata.total_chunk_msg_size = Some(10)
    metadata.chunk_id = Some(0)
    metadata.txnid_least_bits = None
    metadata.txnid_most_bits = None
    let command = @proto.BaseCommand::default()
    command.type_ = @proto.BaseCommand_Type::SEND
    let send = @proto.CommandSend::default()
    send.producer_id = single_producer.producer_id
    send.sequence_id = 0UL
    send.is_chunk = Some(true)
    send.txnid_least_bits = None
    send.txnid_most_bits = None
    command.send = Some(send)
    single_producer.conn.send_payload_frame(command, metadata, b"first")
    single_producer.sequence_id = 1UL
    @async.with_timeout(10000, async fn() {
      while single_consumer.chunks.buffered_bytes == 0 {
        @async.sleep(10)
      }
    })
    assert_eq(single_consumer.chunks.buffered_bytes, 5)
    @async.with_timeout(10000, async fn() {
      while single_consumer.chunks.buffered_bytes != 0 {
        @async.sleep(25)
      }
    })
    let admin = Admin::new(admin_url)
    @async.with_timeout(10000, async fn() {
      while !live_chunk_acknowledged(admin.topic_stats(topic), subscription) {
        @async.sleep(25)
      }
    })
    ignore(producer.send(ProducerMessage::new(b"after expiry")))
    let message = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"after expiry")
    message.ack(await_response=true)
    consumer.close()
    producer.close()
    client.close()
  })
}

///|
async test "live broker releases an incomplete chunk after shared budget failure" {
  guard @env.get_env_var("PULSAR_LIVE_CHUNK_URL") is Some(url) &&
    @env.get_env_var("PULSAR_LIVE_CHUNK_TOPIC") is Some(topic_prefix) else {
    return
  }
  @async.with_task_group(async fn(group) {
    let topic = topic_prefix + "-budget"
    let source = Client::connect(group, url)
    let limited = Client::connect_with_options(
      group,
      ClientOptions::new(url, max_memory_bytes=100000),
    )
    let consumer = limited.create_consumer(
      topic,
      "chunk-budget-live",
      initial_position=Earliest,
      receiver_queue_size=1,
    )
    let producer = source.create_producer(topic)
    guard producer.inner is ProducerImpl::Single(single_producer) else {
      fail("expected a single-topic producer")
    }
    let command = @proto.BaseCommand::default()
    command.type_ = @proto.BaseCommand_Type::SEND
    let send = @proto.CommandSend::default()
    send.producer_id = single_producer.producer_id
    send.sequence_id = 0UL
    send.is_chunk = Some(true)
    send.txnid_least_bits = None
    send.txnid_most_bits = None
    command.send = Some(send)
    for index in 0..<2 {
      let metadata = single_producer.build_metadata(
        ProducerMessage::new(b"unused"),
        0UL,
      )
      metadata.uuid = Some("over-budget-live")
      metadata.num_chunks_from_msg = Some(2)
      metadata.total_chunk_msg_size = Some(80000)
      metadata.chunk_id = Some(index)
      metadata.txnid_least_bits = None
      metadata.txnid_most_bits = None
      single_producer.conn.send_payload_frame(
        command,
        metadata,
        Bytes::make(40000, b'x'),
      )
      if index == 0 {
        guard consumer.inner is ConsumerImpl::Single(single) else {
          fail("expected a single-topic consumer")
        }
        @async.with_timeout(10000, async fn() {
          while single.chunks.buffered_bytes == 0 {
            @async.sleep(10)
          }
        })
        assert_eq(single.chunks.buffered_bytes, 40000)
      }
    }
    let failed : Result[Message, Error] = Ok(
      @async.with_timeout(10000, async fn() { consumer.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(failed is Err(ClientMemoryFull))
    assert_eq(limited.memory_limiter.unwrap().used, 0)
    consumer.close()
    limited.close()
    producer.close()
    source.close()
  })
}
