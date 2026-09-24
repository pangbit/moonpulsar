// Repeatable real-broker check for multi-topic consumption and coordinator lookup.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let prefix = @env.get_env_var("PULSAR_TEST_PREFIX").unwrap_or(
    "moonpulsar-live-capabilities",
  )
  @async.with_task_group(async fn(group) {
    let pool_size = if @env.get_env_var("PULSAR_TEST_POOL") is Some(_) {
      2
    } else {
      1
    }
    let metrics = @pulsar.ClientMetrics::new()
    let observe = fn(event : @pulsar.ClientEvent) { metrics.record(event) }
    let options = match
      (@env.get_env_var("PULSAR_TOKEN_FILE"), @env.get_env_var("PULSAR_TOKEN")) {
      (Some(path), _) =>
        @pulsar.ClientOptions::new(
          url,
          auth_provider=@pulsar.token_file_provider(path),
          max_connections_per_broker=pool_size,
          on_event=observe,
        )
      (None, Some(token)) =>
        @pulsar.ClientOptions::new(
          url,
          auth=@pulsar.TokenAuth::new(token),
          max_connections_per_broker=pool_size,
          on_event=observe,
        )
      (None, None) =>
        @pulsar.ClientOptions::new(
          url,
          max_connections_per_broker=pool_size,
          on_event=observe,
        )
    }
    let client = @pulsar.Client::connect_with_options(group, options)
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
    let received : Map[String, String] = Map([])
    for _ in 0..<2 {
      let message = @async.with_timeout(10000, async fn() { consumer.receive() })
      received[message.topic()] = @utf8.decode(message.payload())
      message.ack()
    }
    assert_eq(received.get(topic_a), Some("a"))
    assert_eq(received.get(topic_b), Some("b"))
    let source_ids = consumer.last_message_ids()
    assert_true(source_ids.contains(topic_a) && source_ids.contains(topic_b))
    let list_topic = "persistent://public/default/\{prefix}-ack-list"
    let list_producer = client.create_producer(list_topic)
    let list_consumer = client.create_consumer(
      list_topic,
      "\{prefix}-ack-list-sub",
      initial_position=Earliest,
    )
    ignore(list_producer.send(@pulsar.ProducerMessage::new(b"list-a")))
    ignore(list_producer.send(@pulsar.ProducerMessage::new(b"list-b")))
    let list_first = @async.with_timeout(10000, async fn() {
      list_consumer.receive()
    })
    let list_second = @async.with_timeout(10000, async fn() {
      list_consumer.receive()
    })
    list_consumer.ack_ids(
      [list_first.id(), list_second.id()],
      await_response=true,
    )
    list_consumer.close()
    list_producer.close()
    println("live batch ACK: begin")
    let batch_topic = "persistent://public/default/\{prefix}-batch-ack"
    let batch_producer = client.create_producer(batch_topic, batching=@pulsar.BatchingConfig::{
      max_messages: 2,
      max_bytes: 4096,
      max_delay_ms: 60000,
    })
    let batch_consumer = client.create_consumer(
      batch_topic,
      "\{prefix}-batch-ack-sub",
      initial_position=Earliest,
    )
    let index_consumer = client.create_consumer(
      batch_topic,
      "\{prefix}-batch-index-sub",
      initial_position=Earliest,
      enable_batch_index_ack=true,
    )
    let batch_zero = batch_producer.send_async(
      @pulsar.ProducerMessage::new(b"batch-zero"),
    )
    let batch_one = batch_producer.send_async(
      @pulsar.ProducerMessage::new(b"batch-one"),
    )
    batch_producer.flush()
    assert_eq(batch_zero.wait().message_id.batch_index, 0)
    assert_eq(batch_one.wait().message_id.batch_index, 1)
    let full_zero = @async.with_timeout(10000, async fn() {
      batch_consumer.receive()
    })
    let full_one = @async.with_timeout(10000, async fn() {
      batch_consumer.receive()
    })
    assert_eq(full_zero.payload(), b"batch-zero")
    assert_eq(full_one.payload(), b"batch-one")
    full_zero.ack()
    full_one.ack(await_response=true)
    batch_consumer.close()
    let index_zero = @async.with_timeout(10000, async fn() {
      index_consumer.receive()
    })
    let index_one = @async.with_timeout(10000, async fn() {
      index_consumer.receive()
    })
    assert_eq(index_zero.id().batch_index, 0)
    assert_eq(index_one.id().batch_index, 1)
    index_zero.ack(await_response=true)
    index_consumer.close()
    println("live batch ACK: index acknowledged, reopening")
    @async.sleep(200)
    let reopened_index_consumer = client.create_consumer(
      batch_topic,
      "\{prefix}-batch-index-sub",
      enable_batch_index_ack=true,
    )
    let remaining = @async.with_timeout(10000, async fn() {
      reopened_index_consumer.receive()
    })
    assert_eq(remaining.payload(), b"batch-one")
    remaining.ack(await_response=true)
    reopened_index_consumer.close()
    batch_producer.close()
    println("live batch ACK: completed")
    let grouped_topic = "persistent://public/default/\{prefix}-grouped-ack"
    let grouped_consumer = client.create_consumer(
      grouped_topic,
      "\{prefix}-grouped-ack-sub",
      initial_position=Earliest,
      ack_grouping=@pulsar.AckGroupingOptions::new(max_size=2, max_time_ms=0),
    )
    let grouped_producer = client.create_producer(grouped_topic)
    for index in 0..<3 {
      ignore(
        grouped_producer.send(
          @pulsar.ProducerMessage::new(@utf8.encode("grouped-\{index}")),
        ),
      )
    }
    for _ in 0..<3 {
      let message = @async.with_timeout(10000, async fn() {
        grouped_consumer.receive()
      })
      message.ack()
    }
    grouped_consumer.close()
    let reopened_grouped_consumer = client.create_consumer(
      grouped_topic,
      "\{prefix}-grouped-ack-sub",
    )
    @async.sleep(300)
    assert_true(reopened_grouped_consumer.try_receive() is None)
    reopened_grouped_consumer.close()
    grouped_producer.close()
    let batch_timeout_topic = "persistent://public/default/\{prefix}-batch-timeout"
    let batch_timeout_consumer = client.create_consumer(
      batch_timeout_topic,
      "\{prefix}-batch-timeout-sub",
      initial_position=Earliest,
    )
    let batch_timeout_producer = client.create_producer(
      batch_timeout_topic,
      batching=@pulsar.BatchingConfig::{
        max_messages: 10,
        max_bytes: 4096,
        max_delay_ms: 60000,
      },
      send_timeout_ms=100,
    )
    let expired_future = batch_timeout_producer.send_async(
      @pulsar.ProducerMessage::new(b"expired-before-flush"),
    )
    let expired_result : Result[@pulsar.SendReceipt, Error] = Ok(
      @async.with_timeout(1000, async fn() { expired_future.wait() }),
    ) catch {
      e => Err(e)
    }
    assert_true(expired_result is Err(@pulsar.PulsarError::RequestTimeout))
    let surviving_future = batch_timeout_producer.send_async(
      @pulsar.ProducerMessage::new(b"sent-after-timeout"),
    )
    batch_timeout_producer.flush()
    ignore(surviving_future.wait())
    let surviving_message = @async.with_timeout(10000, async fn() {
      batch_timeout_consumer.receive()
    })
    assert_eq(surviving_message.payload(), b"sent-after-timeout")
    surviving_message.ack()
    batch_timeout_consumer.close()
    batch_timeout_producer.close()
    let memory_options = match
      (@env.get_env_var("PULSAR_TOKEN_FILE"), @env.get_env_var("PULSAR_TOKEN")) {
      (Some(path), _) =>
        @pulsar.ClientOptions::new(
          url,
          auth_provider=@pulsar.token_file_provider(path),
          max_memory_bytes=5,
        )
      (None, Some(token)) =>
        @pulsar.ClientOptions::new(
          url,
          auth=@pulsar.TokenAuth::new(token),
          max_memory_bytes=5,
        )
      (None, None) => @pulsar.ClientOptions::new(url, max_memory_bytes=5)
    }
    let memory_client = @pulsar.Client::connect_with_options(
      group, memory_options,
    )
    let memory_buffered = memory_client.create_producer(
      "persistent://public/default/\{prefix}-memory-buffered",
      batching=@pulsar.BatchingConfig::{
        max_messages: 10,
        max_bytes: 4096,
        max_delay_ms: 60000,
      },
    )
    let memory_other = memory_client.create_producer(
      "persistent://public/default/\{prefix}-memory-other",
    )
    let held = memory_buffered.send_async(
      @pulsar.ProducerMessage::new(b"12345"),
    )
    let full : Result[@pulsar.ReceiptFuture, Error] = Ok(
      memory_other.send_async(@pulsar.ProducerMessage::new(b"x")),
    ) catch {
      e => Err(e)
    }
    assert_true(full is Err(@pulsar.PulsarError::ClientMemoryFull))
    memory_buffered.flush()
    ignore(held.wait())
    ignore(memory_other.send(@pulsar.ProducerMessage::new(b"x")))
    memory_buffered.close()
    memory_other.close()
    memory_client.close()
    let auto_topic = "persistent://public/default/\{prefix}-auto-scale"
    let auto_producer = client.create_producer(auto_topic)
    let auto_consumer = client.create_consumer(
      auto_topic,
      "\{prefix}-auto-scale-sub",
      initial_position=Earliest,
      receiver_queue_size=2,
      auto_scaled_receiver_queue=true,
    )
    ignore(auto_producer.send(@pulsar.ProducerMessage::new(b"first")))
    ignore(auto_producer.send(@pulsar.ProducerMessage::new(b"second")))
    let auto_first = @async.with_timeout(10000, async fn() {
      auto_consumer.receive()
    })
    let auto_second = @async.with_timeout(10000, async fn() {
      auto_consumer.receive()
    })
    assert_eq(auto_first.payload(), b"first")
    assert_eq(auto_second.payload(), b"second")
    auto_first.ack()
    auto_second.ack()
    auto_consumer.close()
    auto_producer.close()
    let interceptor_topic = "persistent://public/default/\{prefix}-interceptor"
    let send_observed : Ref[Bool] = { val: false, }
    let interceptor_producer = client.create_producer(interceptor_topic, interceptors=[
      @pulsar.ProducerInterceptor::new(
        before_send=fn(_message) {
          @pulsar.ProducerMessage::new(b"intercepted")
        },
        on_send_result=fn(_message, result) {
          send_observed.val = result is Ok(_)
        },
      ),
    ])
    let consume_observed : Ref[Bool] = { val: false, }
    let ack_observed : Ref[Bool] = { val: false, }
    let interceptor_consumer = client.create_consumer(
      interceptor_topic,
      "\{prefix}-interceptor-sub",
      initial_position=Earliest,
      interceptors=[
        @pulsar.ConsumerInterceptor::new(
          before_consume=fn(message) {
            consume_observed.val = message.payload() == b"intercepted"
          },
          on_ack=fn(_id, result) { ack_observed.val = result is Ok(_) },
        ),
      ],
    )
    ignore(interceptor_producer.send(@pulsar.ProducerMessage::new(b"original")))
    assert_true(send_observed.val)
    let intercepted = @async.with_timeout(10000, async fn() {
      interceptor_consumer.receive()
    })
    assert_eq(intercepted.payload(), b"intercepted")
    intercepted.ack()
    assert_true(consume_observed.val && ack_observed.val)
    interceptor_consumer.close()
    interceptor_producer.close()
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
    let auto_chunk_topic = "persistent://public/default/\{prefix}-auto-chunk"
    let auto_chunk_producer = client.create_producer(
      auto_chunk_topic,
      automatic_chunking=true,
    )
    let auto_chunk_consumer = client.create_consumer(
      auto_chunk_topic,
      "\{prefix}-auto-chunk-sub",
      initial_position=Earliest,
    )
    let block = @buffer.Buffer()
    for _ in 0..<1024 {
      block.write_byte(b'x')
    }
    let large_payload = @buffer.Buffer()
    for _ in 0..<6200 {
      large_payload.write_bytes(block.to_bytes())
    }
    let large_bytes = large_payload.to_bytes()
    ignore(auto_chunk_producer.send(@pulsar.ProducerMessage::new(large_bytes)))
    let auto_chunked = @async.with_timeout(30000, async fn() {
      auto_chunk_consumer.receive()
    })
    assert_eq(auto_chunked.payload().length(), large_bytes.length())
    assert_eq(
      @pulsar.xxhash32(auto_chunked.payload()),
      @pulsar.xxhash32(large_bytes),
    )
    auto_chunked.ack()
    let reader_position_topic = "persistent://public/default/\{prefix}-reader-position"
    let reader_position_producer = client.create_producer(reader_position_topic)
    let first_position = reader_position_producer.send(
      @pulsar.ProducerMessage::new(b"position-first"),
    )
    ignore(
      reader_position_producer.send(
        @pulsar.ProducerMessage::new(b"position-second"),
      ),
    )
    let exclusive_reader = client.create_reader(
      reader_position_topic,
      start_message_id=first_position.message_id,
    )
    let exclusive_message = @async.with_timeout(10000, async fn() {
      exclusive_reader.next()
    })
    assert_eq(
      exclusive_message.map(fn(m) { m.payload() }),
      Some(b"position-second"),
    )
    exclusive_reader.close()
    let inclusive_reader = client.create_reader(
      reader_position_topic,
      start_message_id=first_position.message_id,
      include_start_message_id=true,
    )
    let inclusive_message = @async.with_timeout(10000, async fn() {
      inclusive_reader.next()
    })
    assert_eq(
      inclusive_message.map(fn(m) { m.payload() }),
      Some(b"position-first"),
    )
    assert_true(
      inclusive_reader.last_message_ids().contains(reader_position_topic),
    )
    inclusive_reader.close()
    let latest_inclusive_reader = client.create_reader(
      reader_position_topic,
      start_message_id=@pulsar.MessageId::latest(),
      include_start_message_id=true,
    )
    let latest_inclusive_message = @async.with_timeout(10000, async fn() {
      latest_inclusive_reader.next()
    })
    assert_eq(
      latest_inclusive_message.map(fn(m) { m.payload() }),
      Some(b"position-second"),
    )
    latest_inclusive_reader.close()
    reader_position_producer.close()
    let typed_view_topic = "persistent://public/default/\{prefix}-typed-view"
    let table_codec = @pulsar.SchemaCodec::int32()
    let typed_view_producer = client.create_producer(
      typed_view_topic,
      schema=table_codec.info(),
    )
    ignore(
      typed_view_producer.send(
        @pulsar.ProducerMessage::new(table_codec.encode(11), key="typed-key"),
      ),
    )
    let typed_view = @async.with_timeout(10000, async fn() {
      client.create_typed_table_view(typed_view_topic, table_codec)
    })
    assert_eq(typed_view.get("typed-key"), Some(11))
    ignore(
      typed_view_producer.send(
        @pulsar.ProducerMessage::new(table_codec.encode(12), key="typed-key"),
      ),
    )
    @async.with_timeout(10000, async fn() {
      while typed_view.get("typed-key") != Some(12) {
        @async.sleep(20)
      }
    })
    typed_view.close()
    typed_view_producer.close()
    let coord = client.transaction_coordinator(0UL)
    let txn = coord.new_transaction()
    txn.abort()
    coord.close()
    pattern_consumer.close()
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
      let string_schema_topic = "persistent://public/default/\{prefix}-schema-string"
      let json_schema_topic = "persistent://public/default/\{prefix}-schema-json"
      let int64_schema_topic = "persistent://public/default/\{prefix}-schema-int64"
      let protobuf_native_topic = "persistent://public/default/\{prefix}-schema-protobuf-native"
      let admin_backlog_topic = "persistent://public/default/\{prefix}-admin-backlog"
      admin.create_topic(admin_topic)
      admin.update_topic_properties(admin_topic, {
        "owner": "moonpulsar",
        "run": prefix,
      })
      assert_eq(admin.topic_properties(admin_topic).get("run"), Some(prefix))
      admin.remove_topic_property(admin_topic, "owner")
      assert_true(admin.topic_properties(admin_topic).get("owner") is None)
      admin.create_topic(admin_backlog_topic)
      admin.create_topic(string_schema_topic)
      admin.create_topic(json_schema_topic)
      admin.create_topic(int64_schema_topic)
      admin.create_topic(protobuf_native_topic)
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
      for mode in 0..<3 {
        let subscription = "\{prefix}-ordinary-retry-\{mode}"
        let policy = @pulsar.DeadLetterPolicy::new(1U, retry_dead_letter_topic)
        let ordinary = match mode {
          0 =>
            client.create_consumer(
              retry_source_topic,
              subscription,
              sub_type=Shared,
              dead_letter_policy=policy,
              retry_topic~,
            )
          1 =>
            client.create_multi_topic_consumer(
              [retry_source_topic, topic_a],
              subscription,
              sub_type=Shared,
              dead_letter_policy=policy,
              retry_topic~,
            )
          _ =>
            client.create_pattern_consumer(
              "public/default",
              "persistent://public/default/\{prefix}-retry-.*",
              subscription,
              refresh_interval_ms=100,
              sub_type=Shared,
              dead_letter_policy=policy,
              retry_topic~,
            )
        }
        assert_true(ordinary.topics().contains(retry_topic))
        let body = @utf8.encode("ordinary-retry-\{mode}")
        ignore(retry_producer.send(@pulsar.ProducerMessage::new(body)))
        let first = @async.with_timeout(10000, async fn() { ordinary.receive() })
        assert_eq(first.topic(), retry_source_topic)
        assert_eq(first.payload(), body)
        ordinary.reconsume_later(first)
        let retried = @async.with_timeout(10000, async fn() {
          ordinary.receive()
        })
        assert_eq(retried.topic(), retry_topic)
        assert_eq(retried.payload(), body)
        retried.ack()
        ordinary.close()
      }
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
      let string_codec = @pulsar.SchemaCodec::string()
      let string_producer = client.create_producer(
        string_schema_topic,
        schema=string_codec.info(),
      )
      let string_consumer = client.create_consumer(
        string_schema_topic,
        "\{prefix}-schema-string-sub",
        initial_position=Earliest,
        schema=string_codec.info(),
      )
      ignore(
        string_producer.send(
          @pulsar.ProducerMessage::new(string_codec.encode("typed string")),
        ),
      )
      let string_message = @async.with_timeout(10000, async fn() {
        string_consumer.receive()
      })
      assert_eq(string_message.decode(string_codec), "typed string")
      let registered_string_schema = client.get_schema(string_schema_topic)
      assert_eq(
        registered_string_schema.info.type_,
        @pulsar.SchemaInfo::string().type_,
      )
      assert_eq(
        string_message.schema_version(),
        registered_string_schema.version,
      )
      let historical_string_schema = client.get_schema(
        string_schema_topic,
        version?=registered_string_schema.version,
      )
      assert_eq(
        historical_string_schema.info.type_,
        registered_string_schema.info.type_,
      )
      string_message.ack(await_response=true)
      string_consumer.close()
      string_producer.close()
      let json_codec : @pulsar.SchemaCodec[Map[String, Int]] = @pulsar.SchemaCodec::json(
        b"{\"type\":\"record\",\"name\":\"Count\",\"fields\":[{\"name\":\"count\",\"type\":\"int\"}]}",
      )
      let json_producer = client.create_producer(
        json_schema_topic,
        schema=json_codec.info(),
      )
      let json_consumer = client.create_consumer(
        json_schema_topic,
        "\{prefix}-schema-json-sub",
        initial_position=Earliest,
        schema=json_codec.info(),
      )
      let value : Map[String, Int] = { "count": 7 }
      ignore(
        json_producer.send(
          @pulsar.ProducerMessage::new(json_codec.encode(value)),
        ),
      )
      let json_message = @async.with_timeout(10000, async fn() {
        json_consumer.receive()
      })
      assert_eq(json_message.decode(json_codec), value)
      json_message.ack()
      json_consumer.close()
      json_producer.close()
      let int64_codec = @pulsar.SchemaCodec::int64()
      let int64_producer = client.create_producer(
        int64_schema_topic,
        schema=int64_codec.info(),
      )
      let int64_consumer = client.create_consumer(
        int64_schema_topic,
        "\{prefix}-schema-int64-sub",
        initial_position=Earliest,
        schema=int64_codec.info(),
      )
      ignore(
        int64_producer.send(
          @pulsar.ProducerMessage::new(int64_codec.encode(-123456789L)),
        ),
      )
      let int64_message = @async.with_timeout(10000, async fn() {
        int64_consumer.receive()
      })
      assert_eq(int64_message.decode(int64_codec), -123456789L)
      int64_message.ack()
      int64_consumer.close()
      int64_producer.close()
      let descriptor_set = @protobuf.base64_decode(
        "CmwKKHRlc3RkYXRhL3Byb3RvYnVmX25hdGl2ZV9rZXlfdmFsdWUucHJvdG8SDHB1bHNhci5wcm90byIyCghLZXlWYWx1ZRIQCgNrZXkYASACKAlSA2tleRIUCgV2YWx1ZRgCIAIoCVIFdmFsdWU=",
      )
      let protobuf_codec : @pulsar.SchemaCodec[@proto.KeyValue] = @pulsar.SchemaCodec::protobuf_native(
        descriptor_set, "testdata/protobuf_native_key_value.proto", "pulsar.proto.KeyValue",
      )
      let protobuf_producer = client.create_producer(
        protobuf_native_topic,
        schema=protobuf_codec.info(),
      )
      let protobuf_consumer = client.create_consumer(
        protobuf_native_topic,
        "\{prefix}-schema-protobuf-native-sub",
        initial_position=Earliest,
        schema=protobuf_codec.info(),
      )
      let protobuf_value = @proto.KeyValue::KeyValue("language", "moonbit")
      ignore(
        protobuf_producer.send(
          @pulsar.ProducerMessage::new(protobuf_codec.encode(protobuf_value)),
        ),
      )
      let protobuf_message = @async.with_timeout(10000, async fn() {
        protobuf_consumer.receive()
      })
      assert_eq(protobuf_message.decode(protobuf_codec), protobuf_value)
      let registered_protobuf_schema = client.get_schema(protobuf_native_topic)
      assert_eq(
        registered_protobuf_schema.info.type_,
        protobuf_codec.info().type_,
      )
      assert_eq(
        registered_protobuf_schema.info.schema_data,
        protobuf_codec.info().schema_data,
      )
      protobuf_message.ack(await_response=true)
      protobuf_consumer.close()
      protobuf_producer.close()
      let backlog_producer = client.create_producer(admin_backlog_topic)
      ignore(backlog_producer.send(@pulsar.ProducerMessage::new(b"skip-this")))
      ignore(backlog_producer.send(@pulsar.ProducerMessage::new(b"keep-this")))
      let skip_sub = "\{prefix}-skip-sub"
      admin.create_subscription(
        admin_backlog_topic,
        skip_sub,
        initial_position=Earliest,
      )
      admin.skip_messages(admin_backlog_topic, skip_sub, 1)
      let skip_consumer = client.create_consumer(admin_backlog_topic, skip_sub)
      let kept = @async.with_timeout(10000, async fn() {
        skip_consumer.receive()
      })
      assert_eq(kept.payload(), b"keep-this")
      kept.ack()
      skip_consumer.close()
      let all_sub = "\{prefix}-skip-all-sub"
      admin.create_subscription(
        admin_backlog_topic,
        all_sub,
        initial_position=Earliest,
      )
      admin.skip_all_messages(admin_backlog_topic, all_sub)
      ignore(backlog_producer.send(@pulsar.ProducerMessage::new(b"after-skip")))
      let all_consumer = client.create_consumer(admin_backlog_topic, all_sub)
      let after_skip = @async.with_timeout(10000, async fn() {
        all_consumer.receive()
      })
      assert_eq(after_skip.payload(), b"after-skip")
      after_skip.ack()
      all_consumer.close()
      let expire_sub = "\{prefix}-expire-sub"
      admin.create_subscription(
        admin_backlog_topic,
        expire_sub,
        initial_position=Earliest,
      )
      // The broker expires by whole-second age; leave room for rounding.
      @async.sleep(2200)
      admin.expire_messages(admin_backlog_topic, expire_sub, 1)
      ignore(
        backlog_producer.send(@pulsar.ProducerMessage::new(b"after-expire")),
      )
      let expire_consumer = client.create_consumer(
        admin_backlog_topic, expire_sub,
      )
      let after_expire = @async.with_timeout(10000, async fn() {
        expire_consumer.receive()
      })
      assert_eq(after_expire.payload(), b"after-expire")
      after_expire.ack()
      expire_consumer.close()
      backlog_producer.close()
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
      let routed_producer = client.create_producer(
        partitioned_topic,
        routing=Custom(fn(_message, _partitions) { 1 }),
      )
      let routed_receipt = routed_producer.send(
        @pulsar.ProducerMessage::new(b"custom-route", key="route-check"),
      )
      assert_eq(
        routed_producer.last_sequence_id(),
        Some(routed_receipt.sequence_id),
      )
      let routed_message = @async.with_timeout(10000, async fn() {
        scaling_consumer.receive()
      })
      assert_eq(routed_message.topic(), "\{partitioned_topic}-partition-1")
      assert_eq(routed_message.payload(), b"custom-route")
      routed_message.ack()
      routed_producer.close()
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
      admin.delete_topic(admin_backlog_topic, force=true)
      admin.delete_topic(string_schema_topic, force=true)
      admin.delete_topic(json_schema_topic, force=true)
      admin.delete_topic(int64_schema_topic, force=true)
      admin.delete_topic(protobuf_native_topic, force=true)
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
    auto_chunk_consumer.close()
    chunk_reader.close()
    chunk_consumer.close()
    timeout_consumer.close()
    consumer.close()
    let on_demand_consumer = client.create_consumer(
      topic_a,
      "\{prefix}-zero-queue-sub",
      initial_position=Latest,
      receiver_queue_size=0,
    )
    ignore(producer_a.send(@pulsar.ProducerMessage::new(b"on-demand")))
    let on_demand_message = @async.with_timeout(10000, async fn() {
      on_demand_consumer.receive()
    })
    assert_eq(on_demand_message.payload(), b"on-demand")
    on_demand_message.ack()
    on_demand_consumer.close()
    let bounded_producer = client.create_producer(
      topic_a,
      max_pending_messages=1,
      block_if_queue_full=true,
      send_timeout_ms=10000,
    )
    ignore(bounded_producer.send(@pulsar.ProducerMessage::new(b"bounded-1")))
    ignore(bounded_producer.send(@pulsar.ProducerMessage::new(b"bounded-2")))
    bounded_producer.close()
    producer_a.close()
    producer_b.close()
    producer_c.close()
    chunk_producer.close()
    compressed_producer.close()
    auto_chunk_producer.close()
    if @env.get_env_var("PULSAR_ADMIN_URL") is Some(admin_url) {
      let admin = @pulsar.Admin::new(
        admin_url,
        token?=@env.get_env_var("PULSAR_TOKEN"),
      )
      for
        topic in [
          topic_a, topic_b, topic_c, topic_d, topic_e, auto_chunk_topic, reader_position_topic,
          typed_view_topic,
        ] {
        admin.delete_topic(topic, force=true)
      }
    }
    let snapshot = metrics.snapshot()
    assert_true(snapshot.sends > 0L)
    assert_true(snapshot.received > 0L)
    assert_true(snapshot.acknowledgments > 0L)
    client.close()
    println(
      "live multi-topic, pattern discovery, dead-letter/retry, schema codecs, admin properties/backlog, ack timeout, chunking, partition expansion, and coordinator lookup OK",
    )
  })
}
