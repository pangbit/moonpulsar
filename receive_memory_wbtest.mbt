// Retained receive messages share the producer and chunk byte budget.

///|
async test "consumer queue charges payload and metadata until delivery" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/memory-receive-delivery"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.partition_key = Some("route")
      let prop = @proto.KeyValue::default()
      prop.key = "source"
      prop.value = "mock"
      metadata.properties.push(prop)
      let (cmd, meta, body) = message_frame(1UL, 2, 1UL, b"hello", metadata)
      peer.send_message(cmd, meta, body)
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(512)
    let consumer = SingleConsumer::create_with_memory(
      group,
      manager.get(addr),
      topic,
      "memory-sub",
      memory_limiter=limiter,
    )
    @async.with_timeout(1000, async fn() {
      while limiter.used == 0 {
        @async.sleep(1)
      }
    })
    assert_true(limiter.used > b"hello".length())
    let message = consumer.receive()
    assert_eq(message.payload(), b"hello")
    assert_eq(message.key(), Some("route"))
    assert_eq(message.properties().get("source"), Some("mock"))
    assert_eq(limiter.used, 0)
    consumer.close()
    manager.close()
  })
}

///|
async test "delivering one message restores receive budget for the next" {
  @async.with_task_group(async fn(group) {
    let advance : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let topic = "persistent://public/default/memory-receive-reuse"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for entry in 1UL..<=2UL {
        if entry == 2UL {
          advance.get()
        }
        let (cmd, meta, body) = message_frame(
          1UL,
          2,
          entry,
          b"reused",
          test_metadata(),
        )
        peer.send_message(cmd, meta, body)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let sample = build_message(
      { val: conn, },
      2UL,
      topic,
      { ledger_id: 1UL, entry_id: 1UL, batch_index: -1, partition: -1, },
      -1,
      test_metadata(),
      None,
      b"reused",
      0U,
      subscription="memory-sub",
    )
    let limiter = ClientMemoryLimiter::new(sample.retained_bytes() + 1)
    let consumer = SingleConsumer::create_with_memory(
      group,
      conn,
      topic,
      "memory-sub",
      memory_limiter=limiter,
    )
    assert_eq(consumer.receive().payload(), b"reused")
    assert_eq(limiter.used, 0)
    advance.put(())
    assert_eq(consumer.receive().payload(), b"reused")
    assert_eq(limiter.used, 0)
    consumer.close()
    manager.close()
  })
}

///|
async test "receive budget failure drops queued messages and releases their bytes" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/memory-receive-overflow"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for entry in 1UL..<=2UL {
        let payload = Bytes::make(40, b'x')
        let (cmd, meta, body) = message_frame(
          1UL,
          2,
          entry,
          payload,
          test_metadata(),
        )
        peer.send_message(cmd, meta, body)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let example = build_message(
      { val: conn, },
      2UL,
      topic,
      { ledger_id: 1UL, entry_id: 1UL, batch_index: -1, partition: -1, },
      -1,
      test_metadata(),
      None,
      Bytes::make(40, b'x'),
      0U,
      subscription="memory-sub",
    )
    let limiter = ClientMemoryLimiter::new(example.retained_bytes() + 1)
    let consumer = SingleConsumer::create_with_memory(
      group,
      conn,
      topic,
      "memory-sub",
      memory_limiter=limiter,
    )
    @async.with_timeout(1000, async fn() {
      while conn.message_handlers.contains(consumer.consumer_id) {
        @async.sleep(1)
      }
    })
    let result : Result[Message, Error] = Ok(
      @async.with_timeout(1000, async fn() { consumer.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ClientMemoryFull))
    assert_eq(limiter.used, 0)
    consumer.close()
    manager.close()
  })
}

///|
async test "reader queue retains bytes through peek and releases on close" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/memory-reader"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for entry in 1UL..<=2UL {
        let (cmd, meta, body) = message_frame(
          1UL,
          2,
          entry,
          b"reader-data",
          test_metadata(),
        )
        peer.send_message(cmd, meta, body)
      }
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(512)
    let reader = SingleReader::create_with_memory(
      group,
      manager.get(addr),
      topic,
      start_message_id=MessageId::earliest(),
      memory_limiter=limiter,
    )
    @async.with_timeout(1000, async fn() {
      while limiter.used == 0 {
        @async.sleep(1)
      }
    })
    assert_true(reader.has_message_available())
    assert_true(limiter.used > 0)
    guard reader.next() is Some(message) else { fail("expected first message") }
    assert_eq(message.payload(), b"reader-data")
    @async.with_timeout(1000, async fn() {
      while limiter.used == 0 {
        @async.sleep(1)
      }
    })
    reader.close()
    assert_eq(limiter.used, 0)
    manager.close()
  })
}

///|
async test "composite consumer holds a source message until application delivery" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let topic = "persistent://public/default/memory-composite"
    let source = SingleConsumer::create(group, conn, topic, "memory-sub")
    let consumer = Consumer::partitioned(group, [Consumer::single(source)])
    guard consumer.inner is ConsumerImpl::Partitioned(state) else {
      fail("expected composite consumer")
    }
    let limiter = ClientMemoryLimiter::new(512)
    let first = buffered_test_message(
      conn,
      source.consumer_id,
      topic,
      1UL,
      b"first",
    )
    first.retain_for_queue(Some(limiter))
    assert_true(state.queue.try_put({ source_id: 0, message: first, }))
    assert_true(limiter.used > 0)
    assert_eq(consumer.receive().payload(), b"first")
    assert_eq(limiter.used, 0)
    let stale = buffered_test_message(
      conn,
      source.consumer_id,
      topic,
      2UL,
      b"stale",
    )
    stale.retain_for_queue(Some(limiter))
    assert_true(state.queue.try_put({ source_id: 0, message: stale, }))
    state.active_sources.remove(0)
    state.prune_inactive()
    assert_eq(limiter.used, 0)
    consumer.close()
    manager.close()
  })
}

///|
async test "composite reader keeps peeked bytes and frees stale or closed events" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let topic = "persistent://public/default/memory-partition-0"
    let source = SingleReader::create(group, conn, topic)
    let reader = Reader::partitioned(group, [source])
    guard reader.inner is ReaderImpl::Many(state) else {
      fail("expected partitioned reader")
    }
    let limiter = ClientMemoryLimiter::new(512)
    let first = buffered_test_message(
      conn,
      source.consumer_id,
      topic,
      1UL,
      b"peeked",
    )
    first.retain_for_queue(Some(limiter))
    assert_true(state.queue.try_put({ source: 0, event: Msg(first), }))
    assert_true(reader.has_message_available())
    assert_true(limiter.used > 0)
    guard reader.next() is Some(message) else {
      fail("expected peeked message")
    }
    assert_eq(message.payload(), b"peeked")
    assert_eq(limiter.used, 0)
    let stale = buffered_test_message(
      conn,
      source.consumer_id + 1UL,
      topic,
      2UL,
      b"stale",
    )
    stale.retain_for_queue(Some(limiter))
    state.peeked = Some({ source: 0, event: Msg(stale), })
    let last = buffered_test_message(
      conn,
      source.consumer_id,
      topic,
      3UL,
      b"closed",
    )
    last.retain_for_queue(Some(limiter))
    assert_true(state.queue.try_put({ source: 0, event: Msg(last), }))
    reader.close()
    assert_eq(limiter.used, 0)
    manager.close()
  })
}
