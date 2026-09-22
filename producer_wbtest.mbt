// Producer tests against the in-process mock broker.

///|
/// Mock broker side: expect a PRODUCER command, answer PRODUCER_SUCCESS,
/// then hand over to `on_send` for each SEND frame.
async fn expect_producer(
  peer : MockPeer,
  producer_name? : String = "mock-producer",
) -> Unit {
  let frame = peer.next()
  guard frame.cmd.producer is Some(req) else { fail("expected PRODUCER") }
  let reply = @proto.BaseCommand::default()
  reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
  let success = @proto.CommandProducerSuccess::default()
  success.request_id = req.request_id
  success.producer_name = producer_name
  reply.producer_success = Some(success)
  peer.send_command(reply)
}

///|
async fn send_receipt(
  peer : MockPeer,
  producer_id : UInt64,
  sequence_id : UInt64,
) -> Unit {
  let reply = @proto.BaseCommand::default()
  reply.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
  let receipt = @proto.CommandSendReceipt::default()
  receipt.producer_id = producer_id
  receipt.sequence_id = sequence_id
  let id = @proto.MessageIdData::default()
  id.ledger_id = 10
  id.entry_id = 20
  receipt.message_id = Some(id)
  reply.send_receipt = Some(receipt)
  peer.send_command(reply)
}

///|
async test "producer send waits for send receipt" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      guard frame.metadata is Some(meta) && frame.payload is Some(payload) else {
        fail("SEND without payload")
      }
      assert_eq(payload, b"hello")
      assert_eq(meta.producer_name, "mock-producer")
      assert_eq(meta.partition_key, Some("k1"))
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = Producer::create(
      group, conn, "persistent://public/default/t",
    )
    assert_eq(producer.name(), "mock-producer")
    let receipt = producer.send(ProducerMessage::new(b"hello", key="k1"))
    assert_eq(receipt.message_id.ledger_id, 10)
    assert_eq(receipt.message_id.entry_id, 20)
    producer.close()
    manager.close()
  })
}

///|
async test "producer send_async resolves independently" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      for _ in 0..<3 {
        let frame = peer.next()
        guard frame.cmd.send is Some(send) else { fail("expected SEND") }
        send_receipt(peer, send.producer_id, send.sequence_id)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = Producer::create(
      group, conn, "persistent://public/default/t",
    )
    let futures = []
    for i in 0..<3 {
      futures.push(
        producer.send_async(ProducerMessage::new(@utf8.encode("msg\{i}"))),
      )
    }
    for f in futures {
      let receipt = f.wait()
      assert_eq(receipt.message_id.ledger_id, 10)
    }
    producer.close()
    manager.close()
  })
}

///|
async test "producer batches messages and flushes" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      assert_eq(send.num_messages, Some(3))
      guard frame.metadata is Some(meta) && frame.payload is Some(payload) else {
        fail("SEND without payload")
      }
      assert_eq(meta.num_messages_in_batch, Some(3))
      let singles = split_batch_payload(payload, 3)
      assert_eq(singles.length(), 3)
      assert_eq(singles[0].1, b"m0")
      assert_eq(singles[1].1, b"m1")
      assert_eq(singles[2].1, b"m2")
      assert_eq(singles[0].0.partition_key, Some("a"))
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let batching : BatchingConfig = {
      max_messages: 100,
      max_bytes: 131072,
      max_delay_ms: 60000,
    }
    let producer = Producer::create(
      group,
      conn,
      "persistent://public/default/t",
      batching~,
    )
    let f0 = producer.send_async(ProducerMessage::new(b"m0", key="a"))
    let f1 = producer.send_async(ProducerMessage::new(b"m1"))
    let f2 = producer.send_async(ProducerMessage::new(b"m2"))
    producer.flush()
    assert_eq(f0.wait().message_id.batch_index, 0)
    assert_eq(f1.wait().message_id.batch_index, 1)
    assert_eq(f2.wait().message_id.batch_index, 2)
    producer.close()
    manager.close()
  })
}

///|
async test "send error fails the send" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SEND_ERROR
      let err = @proto.CommandSendError::default()
      err.producer_id = send.producer_id
      err.sequence_id = send.sequence_id
      err.error = @proto.ServerError::PersistenceError
      err.message = "disk full"
      reply.send_error = Some(err)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = Producer::create(
      group, conn, "persistent://public/default/t",
    )
    let result : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"x")),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ServerError(message="disk full", ..)))
    producer.close()
    manager.close()
  })
}
