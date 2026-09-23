// Repeatable real-broker check for multi-topic consumption and coordinator lookup.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let prefix = @env.get_env_var("PULSAR_TEST_PREFIX").unwrap_or(
    "moonpulsar-live-capabilities",
  )
  @async.with_task_group(async fn(group) {
    let client = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.Client::connect(group, url, auth=@pulsar.TokenAuth::new(token))
      None => @pulsar.Client::connect(group, url)
    }
    let topic_a = "persistent://public/default/\{prefix}-a"
    let topic_b = "persistent://public/default/\{prefix}-b"
    let producer_a = client.create_producer(topic_a)
    let producer_b = client.create_producer(topic_b)
    let discovered = client.topics_of_namespace(
      "public/default",
      pattern="persistent://public/default/\{prefix}-.*",
    )
    assert_true(discovered.contains(topic_a))
    assert_true(discovered.contains(topic_b))
    let consumer = client.create_multi_topic_consumer(
      [topic_a, topic_b],
      "\{prefix}-sub",
      initial_position=Earliest,
    )
    ignore(producer_a.send(@pulsar.ProducerMessage::new(b"a")))
    ignore(producer_b.send(@pulsar.ProducerMessage::new(b"b")))
    let received : Map[String, String] = {}
    for _ in 0..<2 {
      let message = @async.with_timeout(10000, async fn() { consumer.receive() })
      received[message.topic()] = @utf8.decode(message.payload())
      message.ack()
    }
    assert_eq(received.get(topic_a), Some("a"))
    assert_eq(received.get(topic_b), Some("b"))
    let timeout_consumer = client.create_consumer(
      topic_a,
      "\{prefix}-timeout-sub",
      initial_position=Earliest,
      ack_timeout_ms=200,
    )
    let first = @async.with_timeout(10000, async fn() {
      timeout_consumer.receive()
    })
    assert_eq(first.payload(), b"a")
    let retry = @async.with_timeout(10000, async fn() {
      timeout_consumer.receive()
    })
    assert_eq(retry.payload(), b"a")
    assert_eq(retry.id(), first.id())
    retry.ack()
    timeout_consumer.seek(
      @pulsar.SeekPosition::PositionId(@pulsar.MessageId::earliest()),
    )
    let replayed_consumer_message = @async.with_timeout(10000, async fn() {
      timeout_consumer.receive()
    })
    assert_eq(replayed_consumer_message.payload(), b"a")
    replayed_consumer_message.ack()
    let pattern_consumer = client.create_pattern_consumer(
      "public/default",
      "^persistent://public/default/\{prefix}-.*$",
      "\{prefix}-pattern-sub",
      refresh_interval_ms=200,
    )
    let topic_c = "persistent://public/default/\{prefix}-c"
    let producer_c = client.create_producer(topic_c)
    @async.with_timeout(10000, async fn() {
      while !pattern_consumer.topics().contains(topic_c) {
        @async.sleep(20)
      }
    })
    ignore(producer_c.send(@pulsar.ProducerMessage::new(b"c")))
    let discovered_message = @async.with_timeout(10000, async fn() {
      pattern_consumer.receive()
    })
    assert_eq(discovered_message.topic(), topic_c)
    assert_eq(discovered_message.payload(), b"c")
    discovered_message.ack()
    let topic_d = "persistent://public/default/\{prefix}-chunked"
    let chunk_producer = client.create_producer(topic_d, chunk_size=3)
    let chunk_consumer = client.create_consumer(
      topic_d,
      "\{prefix}-chunk-sub",
      initial_position=Earliest,
    )
    ignore(chunk_producer.send(@pulsar.ProducerMessage::new(b"abcdefg")))
    let chunked = @async.with_timeout(10000, async fn() {
      chunk_consumer.receive()
    })
    assert_eq(chunked.payload(), b"abcdefg")
    chunked.ack()
    let chunk_reader = client.create_reader(
      topic_d,
      start_message_id=@pulsar.MessageId::earliest(),
    )
    let replay = @async.with_timeout(10000, async fn() { chunk_reader.next() })
    guard replay is Some(replayed) else {
      fail("missing chunked reader replay")
    }
    assert_eq(replayed.payload(), b"abcdefg")
    let topic_e = "persistent://public/default/\{prefix}-compressed-chunked"
    let compressed_producer = client.create_producer(
      topic_e,
      chunk_size=5,
      compression=@pulsar.CompressionCodec::Zstd,
    )
    let compressed_consumer = client.create_consumer(
      topic_e,
      "\{prefix}-compressed-sub",
      initial_position=Earliest,
    )
    let original = b"compressed chunked payload: abcdefghijklmnopqrstuvwxyz"
    ignore(compressed_producer.send(@pulsar.ProducerMessage::new(original)))
    let compressed_message = @async.with_timeout(10000, async fn() {
      compressed_consumer.receive()
    })
    assert_eq(compressed_message.payload(), original)
    compressed_message.ack()
    let coord = client.transaction_coordinator(0UL)
    let txn = coord.new_transaction()
    txn.abort()
    coord.close()
    if @env.get_env_var("PULSAR_ADMIN_URL") is Some(admin_url) {
      let admin = @pulsar.Admin::new(
        admin_url,
        token?=@env.get_env_var("PULSAR_TOKEN"),
      )
      let admin_topic = "persistent://public/default/\{prefix}-admin"
      let partitioned_topic = "persistent://public/default/\{prefix}-admin-partitioned"
      let volatile_topic = "non-persistent://public/default/\{prefix}-admin-volatile"
      admin.create_topic(admin_topic)
      admin.create_topic(volatile_topic)
      assert_true(
        admin
        .list_topics("public", "default", domain="non-persistent")
        .contains(volatile_topic),
      )
      admin.create_partitioned_topic(partitioned_topic, 2)
      assert_eq(admin.partition_count(partitioned_topic), 2)
      admin.update_partitioned_topic(partitioned_topic, 3)
      assert_eq(admin.partition_count(partitioned_topic), 3)
      let admin_consumer = client.create_consumer(
        admin_topic,
        "\{prefix}-admin-sub",
      )
      admin_consumer.close()
      assert_true(
        admin.list_subscriptions(admin_topic).contains("\{prefix}-admin-sub"),
      )
      admin.delete_subscription(admin_topic, "\{prefix}-admin-sub", force=true)
      assert_true(
        !admin.list_subscriptions(admin_topic).contains("\{prefix}-admin-sub"),
      )
      admin.delete_topic(admin_topic, force=true)
      admin.delete_topic(volatile_topic, force=true)
      admin.delete_partitioned_topic(partitioned_topic, force=true)
    }
    compressed_consumer.close()
    chunk_reader.close()
    chunk_consumer.close()
    pattern_consumer.close()
    timeout_consumer.close()
    consumer.close()
    producer_a.close()
    producer_b.close()
    producer_c.close()
    chunk_producer.close()
    compressed_producer.close()
    if @env.get_env_var("PULSAR_ADMIN_URL") is Some(admin_url) {
      let admin = @pulsar.Admin::new(
        admin_url,
        token?=@env.get_env_var("PULSAR_TOKEN"),
      )
      for topic in [topic_a, topic_b, topic_c, topic_d, topic_e] {
        admin.delete_topic(topic, force=true)
      }
    }
    client.close()
    println(
      "live multi-topic, pattern discovery, ack timeout, chunking, and coordinator lookup OK",
    )
  })
}
