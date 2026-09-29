///|
test "deadline heap removes arbitrary completions without retaining tombstones" {
  let deadlines = SendDeadlines::new()
  let expected : Map[UInt64, Int64] = Map([])
  for i in 0..<1000 {
    let id = i.to_uint64()
    let due = (i * 7919 % 101).to_int64()
    deadlines.add(id, due)
    expected[id] = due
  }
  // Exercise root, middle, tail and equal-deadline removals in a permutation.
  for i in 0..<1000 {
    let id = (i * 37 % 1000).to_uint64()
    deadlines.remove(id)
    deadlines.remove(id)
    expected.remove(id)
    assert_eq(deadlines.heap.length(), expected.length())
    assert_eq(deadlines.positions.length(), expected.length())
    for index, (key, due) in deadlines.heap {
      assert_eq(expected.get(key), Some(due))
      assert_eq(deadlines.positions.get(key), Some(index))
      if index > 0 {
        assert_true(deadlines.heap[(index - 1) / 2].1 <= due)
      }
    }
  }
}

///|
fn assert_producer_drained(producer : SingleProducer) -> Unit raise {
  assert_eq(producer.sends.length(), 0)
  assert_eq(producer.pending.length(), 0)
  assert_eq(producer.payloads.length(), 0)
  assert_eq(producer.frame_charges.length(), 0)
  assert_eq(producer.deadlines.heap.length(), 0)
  assert_eq(producer.deadlines.positions.length(), 0)
  assert_eq(producer.batch_entries.length(), 0)
  assert_eq(producer.batch_bytes, 0)
  assert_true(producer.batch_timer is None)
}

///|
async test "successful sends remove all timeout state before long deadlines" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      for _ in 0..<1024 {
        let frame = peer.next()
        let send = frame.cmd.send.unwrap()
        send_receipt(peer, send.producer_id, send.sequence_id)
      }
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(1000000)
    let producer = SingleProducer::create_with_memory(
      group,
      manager.get(addr),
      "persistent://public/default/deadline-drain",
      max_pending_messages=32,
      send_timeout_ms=60000,
      memory_limiter=limiter,
    )
    let payload = Bytes::make(1024, b'x')
    for _ in 0..<32 {
      let futures : Array[ReceiptFuture] = []
      for _ in 0..<32 {
        futures.push(producer.send_async(ProducerMessage::new(payload)))
      }
      assert_true(producer.deadlines.heap.length() <= 32)
      for future in futures {
        ignore(future.wait())
      }
      assert_producer_drained(producer)
      assert_eq(limiter.used, 0)
    }
    producer.close()
    assert_true(producer.deadline_worker is None)
    manager.close()
  })
}

///|
async test "partial batch timeout preserves live members and releases shared frame once" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      let frame = peer.next()
      let send = frame.cmd.send.unwrap()
      assert_eq(send.num_messages, Some(2))
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(1000000)
    let producer = SingleProducer::create_with_memory(
      group,
      manager.get(addr),
      "persistent://public/default/partial-batch",
      batching={ max_messages: 2, max_bytes: 100000, max_delay_ms: 60000, },
      max_pending_messages=2,
      send_timeout_ms=400,
      memory_limiter=limiter,
    )
    let first = producer.send_async(ProducerMessage::new(b"first"))
    @async.sleep(200)
    let second = producer.send_async(ProducerMessage::new(b"second"))
    let result : Result[SendReceipt, Error] = Ok(first.wait()) catch {
      e => Err(e)
    }
    assert_true(result is Err(RequestTimeout))
    assert_eq(producer.sends.length(), 1)
    assert_eq(producer.deadlines.heap.length(), 1)
    assert_eq(producer.pending.length(), 1)
    assert_eq(producer.payloads.length(), 1)
    assert_true(limiter.used > 6)
    release.put(())
    assert_eq(second.wait().message_id.batch_index, 1)
    assert_producer_drained(producer)
    assert_eq(limiter.used, 0)
    // Closing after partial expiry must not release either reservation twice.
    producer.close()
    assert_eq(limiter.used, 0)
    manager.close()
  })
}

///|
async test "full batch expiry removes replay frame without waiting for another frame deadline" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      let frame = peer.next()
      let send = frame.cmd.send.unwrap()
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(1000000)
    let producer = SingleProducer::create_with_memory(
      group,
      manager.get(addr),
      "persistent://public/default/expired-frame",
      batching={ max_messages: 10, max_bytes: 100000, max_delay_ms: 60000, },
      send_timeout_ms=100,
      memory_limiter=limiter,
    )
    let first = producer.send_async(ProducerMessage::new(b"expired"))
    @async.sleep(50)
    producer.flush()
    let result : Result[SendReceipt, Error] = Ok(first.wait()) catch {
      e => Err(e)
    }
    assert_true(result is Err(RequestTimeout))
    assert_producer_drained(producer)
    assert_eq(limiter.used, 0)
    release.put(())
    producer.close()
    assert_producer_drained(producer)
    assert_eq(limiter.used, 0)
    manager.close()
  })
}

///|
async test "closing clears unexpired sends and cancels both producer timers" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      ignore(peer.next())
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(1000000)
    let producer = SingleProducer::create_with_memory(
      group,
      manager.get(addr),
      "persistent://public/default/close-deadlines",
      batching={ max_messages: 10, max_bytes: 100000, max_delay_ms: 60000, },
      max_pending_messages=2,
      send_timeout_ms=60000,
      memory_limiter=limiter,
    )
    let first = producer.send_async(ProducerMessage::new(b"one"))
    let second = producer.send_async(ProducerMessage::new(b"two"))
    producer.close()
    for future in [first, second] {
      let result : Result[SendReceipt, Error] = Ok(future.wait()) catch {
        e => Err(e)
      }
      assert_true(result is Err(ProducerClosed))
    }
    assert_producer_drained(producer)
    assert_true(producer.deadline_worker is None)
    assert_eq(limiter.used, 0)
    manager.close()
  })
}

///|
async test "chunked sends share one admission deadline across chunk receipts" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      // Each chunk is individually fast, but their combined time exceeds 120 ms.
      for _ in 0..<3 {
        let frame = peer.next()
        // Under load the shared deadline can expire before all chunks arrive.
        // Leave the close handshake to the mock broker's normal close handler.
        if frame.cmd.close_producer is Some(_) {
          peer.unread(frame)
          return
        }
        let send = frame.cmd.send.unwrap()
        @async.sleep(50)
        send_receipt(peer, send.producer_id, send.sequence_id)
      }
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(1000000)
    let producer = SingleProducer::create_with_memory(
      group,
      manager.get(addr),
      "persistent://public/default/chunk-deadline",
      chunk_size=2,
      max_pending_messages=1,
      send_timeout_ms=120,
      memory_limiter=limiter,
    )
    let result : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"abcdef")),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(RequestTimeout))
    assert_producer_drained(producer)
    assert_eq(limiter.used, 0)
    producer.close()
    manager.close()
  })
}

///|
async test "reconnect replay preserves admission deadline and expiry clears replay data" {
  @async.with_task_group(async fn(group) {
    let replayed : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let second_addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      let frame = peer.next()
      assert_true(frame.cmd.send is Some(_))
      replayed.put(())
    })
    let first_addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      ignore(peer.next())
      peer.tcp.close()
    })
    let manager = ConnectionManager::new(group)
    let limiter = ClientMemoryLimiter::new(1000000)
    let producer = SingleProducer::create_with_memory(
      group,
      manager.get(first_addr),
      "persistent://public/default/replay-deadline",
      send_timeout_ms=300,
      memory_limiter=limiter,
      reconnect_with=async fn() {
        @async.sleep(50)
        manager.get(second_addr)
      },
    )
    let future = producer.send_async(ProducerMessage::new(b"replay"))
    let original = producer.deadlines.heap[0].1
    @async.with_timeout(1000, async fn() { replayed.get() })
    assert_eq(producer.deadlines.heap[0].1, original)
    let result : Result[SendReceipt, Error] = Ok(future.wait()) catch {
      e => Err(e)
    }
    assert_true(result is Err(RequestTimeout))
    assert_producer_drained(producer)
    assert_eq(limiter.used, 0)
    producer.close()
    manager.close()
  })
}

///|
async test "send deadline starts after waiting for producer admission" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      ignore(peer.next()) // First message deliberately receives no receipt.
      let second = peer.next()
      let send = second.cmd.send.unwrap()
      @async.sleep(80)
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = SingleProducer::create(
      group,
      manager.get(addr),
      "persistent://public/default/admission-clock",
      max_pending_messages=1,
      block_if_queue_full=true,
      send_timeout_ms=2000,
    )
    let first = producer.send_async(ProducerMessage::new(b"first"))
    let first_deadline = producer.deadlines.heap[0].1
    let second = producer.send_async(ProducerMessage::new(b"second"))
    // Admission waits for the first timeout. The second send must get a fresh
    // deadline, independently of how promptly the OS schedules its receipt.
    assert_true(producer.deadlines.heap[0].1 >= first_deadline + 1000L)
    let result : Result[SendReceipt, Error] = Ok(first.wait()) catch {
      e => Err(e)
    }
    assert_true(result is Err(RequestTimeout))
    assert_eq(second.wait().sequence_id, 1UL)
    assert_producer_drained(producer)
    producer.close()
    manager.close()
  })
}

///|
async test "broker producer closure retires scheduler and connection handler" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      let frame = peer.next()
      let send = frame.cmd.send.unwrap()
      let cmd = @proto.BaseCommand::default()
      cmd.type_ = @proto.BaseCommand_Type::CLOSE_PRODUCER
      let close = @proto.CommandCloseProducer::default()
      close.producer_id = send.producer_id
      close.request_id = 0UL
      cmd.close_producer = Some(close)
      peer.send_command(cmd)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/broker-close",
      send_timeout_ms=60000,
    )
    let result : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"one")),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConnectionClosed))
    assert_producer_drained(producer)
    assert_true(producer.closed)
    assert_true(producer.deadline_worker is None)
    assert_true(!conn.producer_handlers.contains(producer.producer_id))
    let next : Result[ReceiptFuture, Error] = Ok(
      producer.send_async(ProducerMessage::new(b"two")),
    ) catch {
      e => Err(e)
    }
    assert_true(next is Err(ProducerClosed))
    producer.close()
    manager.close()
  })
}
