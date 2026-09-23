// Chunked message wire and reassembly tests.

///|
async test "producer sends sequential chunks under one message identity" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let parts = [b"abc", b"def", b"g"]
      let mut sequence_id : UInt64? = None
      let mut uuid : String? = None
      for index in 0..<3 {
        let frame = peer.next()
        guard frame.cmd.send is Some(send) &&
          frame.metadata is Some(meta) &&
          frame.payload is Some(body) else {
          fail("expected chunk SEND")
        }
        assert_eq(body, parts[index])
        assert_eq(meta.chunk_id, Some(index))
        assert_eq(meta.num_chunks_from_msg, Some(3))
        assert_eq(meta.total_chunk_msg_size, Some(7))
        assert_true(meta.uuid is Some(_))
        if index == 0 {
          sequence_id = Some(send.sequence_id)
          uuid = meta.uuid
        } else {
          assert_eq(Some(send.sequence_id), sequence_id)
          assert_eq(meta.uuid, uuid)
        }
        send_receipt(peer, send.producer_id, send.sequence_id)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/chunked",
      chunk_size=3,
    )
    let receipt = producer.send(ProducerMessage::new(b"abcdefg"))
    assert_eq(receipt.sequence_id, 0UL)
    producer.close()
    manager.close()
  })
}

///|
async test "automatic chunking fits the broker's negotiated max message size" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(
      group,
      async fn(_addr, peer) {
        expect_producer(peer)
        let first = peer.next()
        guard first.metadata is Some(first_meta) else {
          fail("expected chunk metadata")
        }
        let count = first_meta.num_chunks_from_msg.unwrap_or(0)
        assert_true(count > 1)
        let assembled = @buffer.Buffer()
        let mut sequence_id : UInt64? = None
        for index in 0..<count {
          let frame = if index == 0 { first } else { peer.next() }
          guard frame.cmd.send is Some(send) &&
            frame.metadata is Some(meta) &&
            frame.payload is Some(body) else {
            fail("expected chunk SEND")
          }
          assert_true(frame.wire_size <= 256)
          assert_eq(meta.chunk_id, Some(index))
          if index == 0 {
            sequence_id = Some(send.sequence_id)
          } else {
            assert_eq(Some(send.sequence_id), sequence_id)
          }
          assembled.write_bytes(body)
          send_receipt(peer, send.producer_id, send.sequence_id)
        }
        assert_eq(assembled.to_bytes().length(), 600)
      },
      max_message_size=256,
    )
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/automatic-chunk",
      automatic_chunking=true,
    )
    let payload = @buffer.Buffer()
    for _ in 0..<600 {
      payload.write_byte(b'x')
    }
    ignore(producer.send(ProducerMessage::new(payload.to_bytes())))
    producer.close()
    manager.close()
  })
}

///|
async test "producer rejects oversize unchunked messages and unknown auto limits" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(
      group,
      async fn(_addr, peer) { expect_producer(peer) },
      max_message_size=128,
    )
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group, conn, "persistent://public/default/too-large",
    )
    let payload = @buffer.Buffer()
    for _ in 0..<256 {
      payload.write_byte(b'x')
    }
    let result : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(payload.to_bytes())),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(InvalidFrame(_)))
    producer.close()
    manager.close()
  })
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/no-max",
      automatic_chunking=true,
    )
    let result : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"small")),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(InvalidFrame(_)))
    producer.close()
    manager.close()
  })
}

///|
async test "consumer reassembles chunks and acknowledges every chunk" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for index in 0..<2 {
        let metadata = test_metadata()
        metadata.uuid = Some("chunk-test-1")
        metadata.num_chunks_from_msg = Some(2)
        metadata.total_chunk_msg_size = Some(6)
        metadata.chunk_id = Some(index)
        let payload = if index == 0 { b"abc" } else { b"def" }
        let (cmd, meta, body) = message_frame(
          1UL,
          2,
          (index + 1).to_uint64(),
          payload,
          metadata,
        )
        peer.send_message(cmd, meta, body)
        if index == 0 {
          assert_true(peer.next().cmd.flow is Some(_))
        }
      }
      let frame = peer.next()
      guard frame.cmd.ack is Some(ack) else { fail("expected ACK") }
      assert_eq(ack.message_id.length(), 2)
      assert_eq(ack.message_id[0].entry_id, 1UL)
      assert_eq(ack.message_id[1].entry_id, 2UL)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/chunked", "chunk-sub",
    )
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"abcdef")
    assert_eq(message.id().entry_id, 2UL)
    message.ack()
    consumer.close()
    manager.close()
  })
}

///|
async test "reader reassembles chunks before delivering a message" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for index in 0..<2 {
        let metadata = test_metadata()
        metadata.uuid = Some("reader-chunk-1")
        metadata.num_chunks_from_msg = Some(2)
        metadata.total_chunk_msg_size = Some(6)
        metadata.chunk_id = Some(index)
        let payload = if index == 0 { b"abc" } else { b"def" }
        let (cmd, meta, body) = message_frame(
          1UL,
          2,
          (index + 1).to_uint64(),
          payload,
          metadata,
        )
        peer.send_message(cmd, meta, body)
        if index == 0 {
          assert_true(peer.next().cmd.flow is Some(_))
        }
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/chunked",
      start_message_id=MessageId::earliest(),
    )
    let message = @async.with_timeout(3000, async fn() { reader.next() })
    guard message is Some(msg) else { fail("expected chunked message") }
    assert_eq(msg.payload(), b"abcdef")
    assert_eq(msg.id().entry_id, 2UL)
    reader.close()
    manager.close()
  })
}

///|
test "chunk assembler rejects gaps and oversized declarations" {
  let id : MessageId = {
    ledger_id: 1UL,
    entry_id: 1UL,
    batch_index: -1,
    partition: -1,
  }
  let chunks = ChunkAssembler::new()
  let meta = test_metadata()
  meta.uuid = Some("incomplete")
  meta.num_chunks_from_msg = Some(2)
  meta.total_chunk_msg_size = Some(6)
  meta.chunk_id = Some(1)
  assert_true(chunks.accept(meta, id, b"def").payload is None)
  assert_eq(chunks.pending.size(), 0)
  meta.chunk_id = Some(0)
  meta.total_chunk_msg_size = Some(MAX_CHUNKED_MESSAGE_BYTES + 1)
  assert_true(chunks.accept(meta, id, b"abc").payload is None)
  assert_eq(chunks.pending.size(), 0)
  meta.total_chunk_msg_size = Some(6)
  meta.uuid = None
  assert_true(chunks.accept(meta, id, b"abc").payload is None)
  meta.uuid = Some("duplicate")
  assert_true(chunks.accept(meta, id, b"abc").payload is None)
  meta.chunk_id = Some(1)
  assert_true(chunks.accept(meta, id, b"defg").payload is None)
  assert_eq(chunks.pending.size(), 0)
  meta.uuid = Some("short")
  meta.total_chunk_msg_size = Some(7)
  meta.chunk_id = Some(0)
  assert_true(chunks.accept(meta, id, b"abc").payload is None)
  meta.chunk_id = Some(1)
  assert_true(chunks.accept(meta, id, b"def").payload is None)
  assert_eq(chunks.pending.size(), 0)
}

///|
test "chunk assembler releases completed payloads and caps pending messages" {
  let chunks = ChunkAssembler::new()
  let id : MessageId = {
    ledger_id: 1UL,
    entry_id: 1UL,
    batch_index: -1,
    partition: -1,
  }
  for i in 0..<(MAX_PENDING_CHUNKED_MESSAGES + 1) {
    let meta = test_metadata()
    meta.uuid = Some("pending-\{i}")
    meta.num_chunks_from_msg = Some(2)
    meta.total_chunk_msg_size = Some(2)
    meta.chunk_id = Some(0)
    assert_true(chunks.accept(meta, id, b"a").payload is None)
  }
  assert_eq(chunks.pending.size(), MAX_PENDING_CHUNKED_MESSAGES)
  assert_eq(chunks.buffered_bytes, MAX_PENDING_CHUNKED_MESSAGES)
  let meta = test_metadata()
  meta.uuid = Some("pending-100")
  meta.num_chunks_from_msg = Some(2)
  meta.total_chunk_msg_size = Some(2)
  meta.chunk_id = Some(1)
  assert_eq(chunks.accept(meta, id, b"b").payload, Some(b"ab"))
  assert_eq(chunks.buffered_bytes, MAX_PENDING_CHUNKED_MESSAGES - 1)
  chunks.clear()
  assert_eq(chunks.buffered_bytes, 0)
}

///|
async test "chunk policy limits pending messages and expires incomplete buffers" {
  let invalid : Result[ChunkAssemblyPolicy, Error] = Ok(
    ChunkAssemblyPolicy::new(expiry_ms=0),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
  let policy = ChunkAssemblyPolicy::new(expiry_ms=20, max_pending_messages=1)
  let chunks = ChunkAssembler::new(policy~)
  let id : MessageId = {
    ledger_id: 1UL,
    entry_id: 1UL,
    batch_index: -1,
    partition: -1,
  }
  for uuid in ["first", "second"] {
    let meta = test_metadata()
    meta.uuid = Some(uuid)
    meta.num_chunks_from_msg = Some(2)
    meta.total_chunk_msg_size = Some(2)
    meta.chunk_id = Some(0)
    ignore(chunks.accept(meta, id, b"a"))
  }
  assert_eq(chunks.pending.size(), 1)
  assert_eq(chunks.buffered_bytes, 1)
  @async.sleep(30)
  chunks.evict_expired()
  assert_eq(chunks.pending.size(), 0)
  assert_eq(chunks.buffered_bytes, 0)
}

///|
async test "consumer sweeps incomplete chunks without new messages" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.uuid = Some("expires")
      metadata.num_chunks_from_msg = Some(2)
      metadata.total_chunk_msg_size = Some(2)
      metadata.chunk_id = Some(0)
      let (cmd, meta, body) = message_frame(1UL, 1UL, 1UL, b"a", metadata)
      peer.send_message(cmd, meta, body)
      ignore(peer.next())
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/chunk-expiry",
      "chunk-expiry-sub",
      chunk_policy=ChunkAssemblyPolicy::new(expiry_ms=20),
    )
    @async.with_timeout(1000, async fn() {
      while consumer.chunks.pending.is_empty() {
        @async.sleep(1)
      }
    })
    @async.with_timeout(1000, async fn() {
      while !consumer.chunks.pending.is_empty() {
        @async.sleep(5)
      }
    })
    assert_eq(consumer.chunks.buffered_bytes, 0)
    consumer.close()
    manager.close()
  })
}

///|
async test "reader sweeps incomplete chunks without new messages" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.uuid = Some("reader-expires")
      metadata.num_chunks_from_msg = Some(2)
      metadata.total_chunk_msg_size = Some(2)
      metadata.chunk_id = Some(0)
      let (cmd, meta, body) = message_frame(1UL, 1UL, 1UL, b"a", metadata)
      peer.send_message(cmd, meta, body)
      ignore(peer.next())
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/chunk-expiry-reader",
      chunk_policy=ChunkAssemblyPolicy::new(expiry_ms=20),
    )
    @async.with_timeout(1000, async fn() {
      while reader.chunks.pending.is_empty() {
        @async.sleep(1)
      }
    })
    @async.with_timeout(1000, async fn() {
      while !reader.chunks.pending.is_empty() {
        @async.sleep(5)
      }
    })
    assert_eq(reader.chunks.buffered_bytes, 0)
    reader.close()
    manager.close()
  })
}
