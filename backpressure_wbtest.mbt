// Composite streams stop forwarding when the application stops reading.

///|
fn buffered_test_message(
  conn : Connection,
  consumer_id : UInt64,
  topic : String,
  entry : UInt64,
  payload : Bytes,
) -> Message {
  let id : MessageId = {
    ledger_id: 1UL,
    entry_id: entry,
    batch_index: -1,
    partition: -1,
  }
  build_message(
    { val: conn, },
    consumer_id,
    topic,
    id,
    -1,
    test_metadata(),
    None,
    payload,
    0U,
  )
}

///|
async test "slow composite consumer stops source forwarding at its queue limit" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let topic = "persistent://public/default/slow-consumer"
    let source = SingleConsumer::create(group, conn, topic, "slow-sub")
    let consumer = Consumer::partitioned(group, [Consumer::single(source)])
    guard consumer.inner is ConsumerImpl::Partitioned(state) else {
      fail("expected composite consumer")
    }
    let filler = buffered_test_message(
      conn,
      source.consumer_id,
      topic,
      0UL,
      b"filler",
    )
    for _ in 0..<COMPOSITE_CONSUMER_QUEUE_CAPACITY {
      assert_true(state.queue.try_put({ source_id: 0, message: filler, }))
    }
    assert_true(!state.queue.try_put({ source_id: 0, message: filler, }))
    source.messages.put(
      buffered_test_message(conn, source.consumer_id, topic, 1UL, b"first"),
    )
    source.messages.put(
      buffered_test_message(conn, source.consumer_id, topic, 2UL, b"second"),
    )
    @async.with_timeout(3000, async fn() {
      while source.available_permits == source.receiver_permits {
        @async.sleep(1)
      }
    })
    guard source.messages.try_get() is Some(second) else {
      fail("source drained while the composite queue was full")
    }
    assert_eq(second.payload(), b"second")
    source.messages.put(second)
    for _ in 0..<COMPOSITE_CONSUMER_QUEUE_CAPACITY {
      assert_eq(consumer.receive().payload(), b"filler")
    }
    let first = @async.with_timeout(3000, async fn() { consumer.receive() })
    let second = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(first.payload(), b"first")
    assert_eq(second.payload(), b"second")
    consumer.close()
    manager.close()
  })
}

///|
async test "slow partitioned reader preserves its bounded queue and resumes" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let topic = "persistent://public/default/slow-reader-partition-0"
    let source = SingleReader::create(group, conn, topic)
    let reader = Reader::partitioned(group, [source])
    guard reader.inner is ReaderImpl::Many(state) else {
      fail("expected partitioned reader")
    }
    let filler = buffered_test_message(
      conn,
      source.consumer_id,
      topic,
      0UL,
      b"filler",
    )
    for _ in 0..<COMPOSITE_READER_QUEUE_CAPACITY {
      assert_true(state.queue.try_put({ source: 0, event: Msg(filler), }))
    }
    assert_true(!state.queue.try_put({ source: 0, event: Msg(filler), }))
    source.events.put(
      Msg(buffered_test_message(conn, source.consumer_id, topic, 1UL, b"first")),
    )
    source.events.put(
      Msg(
        buffered_test_message(conn, source.consumer_id, topic, 2UL, b"second"),
      ),
    )
    @async.with_timeout(3000, async fn() {
      while source.available_permits == source.receiver_permits {
        @async.sleep(1)
      }
    })
    guard source.events.try_get() is Some(Msg(second)) else {
      fail("reader source drained while the composite queue was full")
    }
    assert_eq(second.payload(), b"second")
    source.events.put(Msg(second))
    assert_true(reader.has_message_available())
    assert_true(reader.has_message_available())
    for _ in 0..<COMPOSITE_READER_QUEUE_CAPACITY {
      assert_eq(reader.next().map(fn(m) { m.payload() }), Some(b"filler"))
    }
    let first = @async.with_timeout(3000, async fn() { reader.next() })
    let second = @async.with_timeout(3000, async fn() { reader.next() })
    assert_eq(first.map(fn(m) { m.payload() }), Some(b"first"))
    assert_eq(second.map(fn(m) { m.payload() }), Some(b"second"))
    reader.close()
    manager.close()
  })
}
