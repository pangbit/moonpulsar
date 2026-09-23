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
    let backoff_consumer = client.create_consumer(
      topic_a,
      "\{prefix}-backoff-sub",
      initial_position=Earliest,
      nack_backoff=@pulsar.NackBackoff::new(200, 1000),
    )
    let backoff_first = @async.with_timeout(10000, async fn() {
      backoff_consumer.receive()
    })
    let nack_started = @async.now()
    backoff_first.nack()
    let backoff_retry = @async.with_timeout(10000, async fn() {
      backoff_consumer.receive()
    })
    assert_eq(backoff_retry.id(), backoff_first.id())
    assert_true(@async.now() - nack_started >= 180L)
    backoff_retry.ack()
    backoff_consumer.close()
    let key_shared_consumer = client.create_consumer(
      topic_a,
      "\{prefix}-key-shared-sub",
      sub_type=KeyShared,
      key_shared_policy=@pulsar.KeySharedPolicy::sticky([(0, 65535)]),
    )
    ignore(
      producer_a.send(@pulsar.ProducerMessage::new(b"key-shared", key="ks")),
    )
    let key_shared_message = @async.with_timeout(10000, async fn() {
      key_shared_consumer.receive()
    })
    assert_eq(key_shared_message.payload(), b"key-shared")
    key_shared_message.ack()
    key_shared_consumer.close()
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
      let table_topic = "persistent://public/default/\{prefix}-table-view"
      let dead_letter_source = "persistent://public/default/\{prefix}-dlq-source"
      let dead_letter_topic = "persistent://public/default/\{prefix}-dlq"
      let retry_source_topic = "persistent://public/default/\{prefix}-retry-source"
      let retry_topic = "persistent://public/default/\{prefix}-retry"
      let retry_dead_letter_topic = "persistent://public/default/\{prefix}-retry-dlq"
      let partitioned_topic = "persistent://public/default/\{prefix}-admin-partitioned"
      let volatile_topic = "non-persistent://public/default/\{prefix}-admin-volatile"
      admin.create_topic(admin_topic)
      admin.create_topic(table_topic)
      admin.create_topic(dead_letter_source)
      admin.create_topic(dead_letter_topic)
      admin.create_topic(retry_source_topic)
      admin.create_topic(retry_topic)
      admin.create_topic(retry_dead_letter_topic)
      let dead_letter_producer = client.create_producer(dead_letter_source)
      let dead_letter_consumer = client.create_consumer(
        dead_letter_topic,
        "\{prefix}-dlq-sub",
      )
      let source_consumer = client.create_consumer(
        dead_letter_source,
        "\{prefix}-source-sub",
        dead_letter_policy=@pulsar.DeadLetterPolicy::new(1U, dead_letter_topic),
      )
      let dlq_properties : Map[String, String] = { "reason": "test" }
      ignore(
        dead_letter_producer.send(
          @pulsar.ProducerMessage::new(
            b"dead-letter-payload",
            key="dead-letter-key",
            properties=dlq_properties,
            event_time=123UL,
          ),
        ),
      )
      let initial = @async.with_timeout(10000, async fn() {
        source_consumer.receive()
      })
      assert_eq(initial.redelivery_count(), 0U)
      initial.nack()
      let next_source : @aqueue.Queue[@pulsar.Message] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      group.spawn_bg(
        async fn() { next_source.put(source_consumer.receive()) },
        no_wait=true,
        allow_failure=true,
      )
      let dead_lettered = @async.with_timeout(10000, async fn() {
        dead_letter_consumer.receive()
      })
      assert_eq(dead_lettered.payload(), b"dead-letter-payload")
      assert_eq(dead_lettered.key(), Some("dead-letter-key"))
      assert_eq(dead_lettered.properties().get("reason"), Some("test"))
      assert_eq(dead_lettered.event_time(), Some(123UL))
      dead_lettered.ack()
      ignore(dead_letter_producer.send(@pulsar.ProducerMessage::new(b"next")))
      let next = @async.with_timeout(10000, async fn() { next_source.get() })
      assert_eq(next.payload(), b"next")
      next.ack()
      source_consumer.close()
      dead_letter_consumer.close()
      dead_letter_producer.close()
      let retry_producer = client.create_producer(retry_source_topic)
      let retry_consumer = client.create_retry_consumer(
        retry_source_topic,
        "\{prefix}-retry-sub",
        retry_topic,
        @pulsar.DeadLetterPolicy::new(1U, retry_dead_letter_topic),
      )
      let retry_dlq_consumer = client.create_consumer(
        retry_dead_letter_topic,
        "\{prefix}-retry-dlq-sub",
      )
      ignore(
        retry_producer.send(
          @pulsar.ProducerMessage::new(b"retry-payload", key="retry-key"),
        ),
      )
      let retry_initial = @async.with_timeout(10000, async fn() {
        retry_consumer.receive()
      })
      let retry_started = @async.now()
      retry_consumer.reconsume_later(retry_initial, delay_ms=5000, custom_properties={
        "attempt": "one",
      })
      let retry_delivered = @async.with_timeout(10000, async fn() {
        retry_consumer.receive()
      })
      assert_eq(retry_delivered.topic(), retry_topic)
      assert_eq(retry_delivered.properties().get("RECONSUMETIMES"), Some("1"))
      assert_eq(retry_delivered.properties().get("attempt"), Some("one"))
      assert_true(@async.now() - retry_started >= 4000L)
      retry_consumer.reconsume_later(retry_delivered)
      let retry_dead_lettered = @async.with_timeout(10000, async fn() {
        retry_dlq_consumer.receive()
      })
      assert_eq(retry_dead_lettered.payload(), b"retry-payload")
      assert_eq(retry_dead_lettered.key(), Some("retry-key"))
      assert_eq(
        retry_dead_lettered.properties().get("RECONSUMETIMES"),
        Some("2"),
      )
      retry_dead_lettered.ack()
      retry_consumer.close()
      retry_dlq_consumer.close()
      retry_producer.close()
      let exclusive_producer = client.create_producer(
        admin_topic,
        access_mode=Exclusive,
      )
      let conflict : Result[@pulsar.Producer, Error] = Ok(
        client.create_producer(admin_topic),
      ) catch {
        e => Err(e)
      }
      assert_true(conflict is Err(_))
      let waiting : @aqueue.Queue[@pulsar.Producer] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Unbounded,
      )
      group.spawn_bg(
        async fn() {
          let producer = client.create_producer(
            admin_topic,
            access_mode=WaitForExclusive,
          ) catch {
            e => {
              waiting.close(error=e, clear=true)
              return
            }
          }
          waiting.put(producer)
        },
        no_wait=true,
        allow_failure=true,
      )
      @async.sleep(100)
      assert_true((waiting.try_get() catch { _ => None }) is None)
      exclusive_producer.close()
      let waiting_producer = @async.with_timeout(10000, async fn() {
        waiting.get()
      })
      ignore(waiting_producer.send(@pulsar.ProducerMessage::new(b"ready")))
      waiting_producer.close()
      admin.create_topic(volatile_topic)
      assert_true(
        admin
        .list_topics("public", "default", domain="non-persistent")
        .contains(volatile_topic),
      )
      admin.create_partitioned_topic(partitioned_topic, 2)
      assert_true(
        admin
        .list_partitioned_topics("public", "default")
        .contains(partitioned_topic),
      )
      let empty_table = @async.with_timeout(10000, async fn() {
        client.create_table_view(table_topic)
      })
      assert_true(empty_table.is_empty())
      empty_table.close()
      let table_producer = client.create_producer(table_topic)
      ignore(
        table_producer.send(@pulsar.ProducerMessage::new(b"first", key="a")),
      )
      ignore(
        table_producer.send(@pulsar.ProducerMessage::new(b"second", key="b")),
      )
      let table = @async.with_timeout(10000, async fn() {
        client.create_table_view(table_topic)
      })
      assert_eq(table.get("a"), Some(b"first"))
      assert_eq(table.get("b"), Some(b"second"))
      ignore(
        table_producer.send(@pulsar.ProducerMessage::new(b"updated", key="a")),
      )
      ignore(table_producer.send(@pulsar.ProducerMessage::new(b"", key="b")))
      @async.with_timeout(10000, async fn() {
        while table.get("a") != Some(b"updated") || table.contains_key("b") {
          @async.sleep(20)
        }
      })
      table.close()
      table_producer.close()
      assert_eq(admin.partition_count(partitioned_topic), 2)
      let scaling_producer = client.create_producer(
        partitioned_topic,
        partition_refresh_ms=100,
      )
      let scaling_consumer = client.create_consumer(
        partitioned_topic,
        "\{prefix}-scaling-sub",
        initial_position=Earliest,
        partition_refresh_ms=100,
      )
      let scaling_reader = client.create_reader(
        partitioned_topic,
        start_message_id=@pulsar.MessageId::earliest(),
        partition_refresh_ms=100,
      )
      let scaling_table = @async.with_timeout(10000, async fn() {
        client.create_table_view(partitioned_topic, partition_refresh_ms=100)
      })
      assert_true(scaling_table.is_empty())
      assert_eq(scaling_producer.partition_count(), 2)
      admin.update_partitioned_topic(partitioned_topic, 3)
      assert_eq(admin.partition_count(partitioned_topic), 3)
      @async.with_timeout(10000, async fn() {
        while scaling_producer.partition_count() != 3 {
          @async.sleep(20)
        }
      })
      let mut partition_key = ""
      for i in 0..<100 {
        let candidate = "partition-key-\{i}"
        let hash = @pulsar.xxhash32(@utf8.encode(candidate)).reinterpret_as_int() &
          0x7fffffff
        if hash % 3 == 2 {
          partition_key = candidate
          break
        }
      }
      assert_true(partition_key != "")
      ignore(
        scaling_producer.send(
          @pulsar.ProducerMessage::new(b"expanded", key=partition_key),
        ),
      )
      let expanded = @async.with_timeout(10000, async fn() {
        scaling_consumer.receive()
      })
      assert_eq(expanded.topic(), "\{partitioned_topic}-partition-2")
      assert_eq(expanded.payload(), b"expanded")
      expanded.ack()
      @async.with_timeout(10000, async fn() {
        while scaling_table.get(partition_key) != Some(b"expanded") {
          @async.sleep(20)
        }
      })
      let read_expanded = @async.with_timeout(10000, async fn() {
        let mut result : @pulsar.Message? = None
        while result is None {
          result = scaling_reader.next()
          if result is None {
            @async.sleep(20)
          }
        }
        result.unwrap()
      })
      assert_eq(read_expanded.topic(), "\{partitioned_topic}-partition-2")
      assert_eq(read_expanded.payload(), b"expanded")
      assert_true(
        admin.partitioned_topic_stats(partitioned_topic, per_partition=true)
        is Object(_),
      )
      scaling_table.close()
      scaling_reader.close()
      scaling_consumer.close()
      scaling_producer.close()
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
      admin.delete_topic(table_topic, force=true)
      admin.delete_topic(dead_letter_source, force=true)
      admin.delete_topic(dead_letter_topic, force=true)
      admin.delete_topic(retry_source_topic, force=true)
      admin.delete_topic(retry_topic, force=true)
      admin.delete_topic(retry_dead_letter_topic, force=true)
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
      "live multi-topic, pattern discovery, dead-letter/retry, ack timeout, chunking, partition expansion, and coordinator lookup OK",
    )
  })
}
