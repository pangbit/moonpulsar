// Batch acknowledgment wire behavior and retry state.

///|
fn batch_test_payload(size : Int) -> Bytes raise {
  let messages : Array[ProducerMessage] = []
  for index in 0..<size {
    messages.push(ProducerMessage::new(@utf8.encode(index.to_string())))
  }
  encode_batch_payload(messages)
}

///|
async test "default batch ACK waits for every index before acknowledging entry" {
  @async.with_task_group(async fn(group) {
    let valid : @aqueue.Queue[Int] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(2)
      let (cmd, meta, payload) = message_frame(
        1UL,
        10UL,
        11UL,
        batch_test_payload(2),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      let first = peer.next()
      if first.cmd.ack is Some(ack) {
        let mut error = 0
        if ack.message_id.length() != 1 {
          error += 1
        }
        if ack.message_id[0].entry_id != 11UL {
          error += 2
        }
        if ack.message_id[0].batch_index != Some(-1) {
          error += 4
        }
        if !ack.message_id[0].ack_set.is_empty() {
          error += 8
        }
        let next = peer.next()
        if next.cmd.close_consumer is Some(close) {
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = close.request_id
          reply.success = Some(success)
          peer.send_command(reply)
          valid.put(if error == 0 { 0 } else { error })
        } else {
          valid.put(2)
        }
      } else {
        if first.cmd.close_consumer is Some(close) {
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = close.request_id
          reply.success = Some(success)
          peer.send_command(reply)
        }
        valid.put(16)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/batch-ack", "sub",
    )
    let first = consumer.receive()
    let second = consumer.receive()
    let cumulative : Result[Unit, Error] = Ok(
      consumer.ack_cumulative(first.id()),
    ) catch {
      e => Err(e)
    }
    let first_ack : Result[Unit, Error] = Ok(first.ack()) catch { e => Err(e) }
    let second_ack : Result[Unit, Error] = Ok(second.ack()) catch {
      e => Err(e)
    }
    consumer.close()
    manager.close()
    assert_true(cumulative is Err(InvalidFrame(_)))
    assert_true(first_ack is Ok(_))
    assert_true(second_ack is Ok(_))
    assert_eq(valid.get(), 0)
  })
}

///|
async test "batch-index ACK sends remaining-bit sets and confirms each index" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[@proto.MessageIdData] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(3)
      let (cmd, meta, payload) = message_frame(
        1UL,
        20UL,
        21UL,
        batch_test_payload(3),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      for _ in 0..<3 {
        let frame = peer.next()
        guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
          fail("expected confirmed batch-index ACK")
        }
        observed.put(ack.message_id[0])
        let response = @proto.BaseCommand::default()
        response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
        let result = @proto.CommandAckResponse::default()
        result.consumer_id = ack.consumer_id
        result.request_id = Some(id)
        response.ack_response = Some(result)
        peer.send_command(response)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/batch-index-ack",
      "sub",
      enable_batch_index_ack=true,
    )
    let zero = consumer.receive()
    let one = consumer.receive()
    let two = consumer.receive()
    one.ack(await_response=true)
    zero.ack(await_response=true)
    two.ack(await_response=true)
    consumer.close()
    manager.close()
    for bits in [5L, 4L, 0L] {
      let id = observed.get()
      assert_eq(id.entry_id, 21UL)
      assert_eq(id.batch_index, Some(-1))
      assert_eq(id.batch_size, Some(3))
      assert_eq(id.ack_set, [bits])
    }
  })
}

///|
async test "transactional batch-index ACK registers subscription and retries rejected bitmap" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/txn-batch-index"
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let connect = peer.next()
      guard connect.cmd.tc_client_connect_request is Some(req) else {
        fail("expected TC_CLIENT_CONNECT_REQUEST")
      }
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
      let result = @proto.CommandTcClientConnectResponse::default()
      result.request_id = req.request_id
      response.tc_client_connect_response = Some(result)
      peer.send_command(response)
      let create = peer.next()
      guard create.cmd.new_txn is Some(req) else { fail("expected NEW_TXN") }
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::NEW_TXN_RESPONSE
      let result = @proto.CommandNewTxnResponse::default()
      result.request_id = req.request_id
      result.txnid_most_bits = Some(3UL)
      result.txnid_least_bits = Some(42UL)
      response.new_txn_response = Some(result)
      peer.send_command(response)
      let subscribe = peer.next()
      guard subscribe.cmd.subscribe is Some(req) else {
        fail("expected SUBSCRIBE")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else { fail("expected FLOW") }
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(2)
      let (cmd, meta, payload) = message_frame(
        req.consumer_id,
        60UL,
        61UL,
        batch_test_payload(2),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      for attempt in 0..<3 {
        let bits = [2L, 2L, 1L][attempt]
        let register = peer.next()
        guard register.cmd.add_subscription_to_txn is Some(req) else {
          fail("expected ADD_SUBSCRIPTION_TO_TXN")
        }
        assert_eq(req.subscription[0].topic, topic)
        assert_eq(req.subscription[0].subscription, "sub")
        let response = @proto.BaseCommand::default()
        response.type_ = @proto.BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE
        let result = @proto.CommandAddSubscriptionToTxnResponse::default()
        result.request_id = req.request_id
        response.add_subscription_to_txn_response = Some(result)
        peer.send_command(response)
        let frame = peer.next()
        guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
          fail("expected transactional confirmed batch ACK")
        }
        assert_eq(ack.txnid_most_bits, Some(3UL))
        assert_eq(ack.txnid_least_bits, Some(42UL))
        assert_eq(ack.message_id[0].ack_set, [bits])
        assert_eq(ack.message_id[0].batch_size, Some(2))
        let response = @proto.BaseCommand::default()
        response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
        let result = @proto.CommandAckResponse::default()
        result.consumer_id = ack.consumer_id
        result.request_id = Some(id)
        if attempt == 0 {
          result.error = Some(@proto.ServerError::MetadataError)
          result.message = Some("retry transaction ACK")
        }
        response.ack_response = Some(result)
        peer.send_command(response)
      }
      let end = peer.next()
      guard end.cmd.end_txn is Some(req) else { fail("expected END_TXN") }
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::END_TXN_RESPONSE
      let result = @proto.CommandEndTxnResponse::default()
      result.request_id = req.request_id
      response.end_txn_response = Some(result)
      peer.send_command(response)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let coord = @async.with_timeout(3000, async fn() {
      TransactionCoordinator::attach(conn, 0UL, owns_connection=false)
    })
    let txn = @async.with_timeout(3000, async fn() { coord.new_transaction() })
    let consumer = @async.with_timeout(3000, async fn() {
      SingleConsumer::create(
        group,
        conn,
        topic,
        "sub",
        enable_batch_index_ack=true,
      )
    })
    let first = @async.with_timeout(3000, async fn() { consumer.receive() })
    let second = @async.with_timeout(3000, async fn() { consumer.receive() })
    let rejected : Result[Unit, Error] = Ok(
      @async.with_timeout(3000, async fn() { first.ack_txn(txn) }),
    ) catch {
      e => Err(e)
    }
    assert_true(
      rejected is Err(ServerError(message="retry transaction ACK", ..)),
    )
    @async.with_timeout(3000, async fn() { first.ack_txn(txn) })
    @async.with_timeout(3000, async fn() { second.ack_txn(txn) })
    @async.with_timeout(3000, async fn() { txn.abort() })
    consumer.close()
    manager.close()
  })
}

///|
test "batch ACK bitmap crosses 64-bit word boundary" {
  let acknowledged : Array[Bool] = []
  for _ in 0..<65 {
    acknowledged.push(false)
  }
  let bits = batch_ack_set(acknowledged, 0, false)
  assert_eq(bits.length(), 2)
  assert_eq(bits[0], -2L)
  assert_eq(bits[1], 1L)
  let cumulative = batch_ack_set(acknowledged, 63, true)
  assert_eq(cumulative, [0L, 1L])
}

///|
async test "rejected confirmed batch-index ACK keeps index retryable" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(2)
      let (cmd, meta, payload) = message_frame(
        1UL,
        30UL,
        31UL,
        batch_test_payload(2),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      for attempt in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
          fail("expected confirmed ACK")
        }
        assert_eq(ack.message_id[0].ack_set, [2L])
        let response = @proto.BaseCommand::default()
        response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
        let result = @proto.CommandAckResponse::default()
        result.consumer_id = ack.consumer_id
        result.request_id = Some(id)
        if attempt == 0 {
          result.error = Some(@proto.ServerError::MetadataError)
          result.message = Some("try again")
        }
        response.ack_response = Some(result)
        peer.send_command(response)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/batch-index-retry",
      "sub",
      enable_batch_index_ack=true,
    )
    let first = consumer.receive()
    let _second = consumer.receive()
    let failure : Result[Unit, Error] = Ok(first.ack(await_response=true)) catch {
      e => Err(e)
    }
    assert_true(failure is Err(ServerError(message="try again", ..)))
    first.ack(await_response=true)
    consumer.close()
    manager.close()
  })
}

///|
async test "partial batch ACK rejects confirmation without index ACK" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(2)
      let (cmd, meta, payload) = message_frame(
        1UL,
        40UL,
        41UL,
        batch_test_payload(2),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      if frame.cmd.ack is Some(ack) {
        observed.put(ack.message_id[0].entry_id == 41UL)
      } else {
        peer.unread(frame)
        observed.put(false)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/batch-confirm", "sub",
    )
    let first = consumer.receive()
    let second = consumer.receive()
    let failure : Result[Unit, Error] = Ok(first.ack(await_response=true)) catch {
      e => Err(e)
    }
    assert_true(failure is Err(InvalidFrame(_)))
    first.ack()
    second.ack()
    consumer.close()
    manager.close()
    assert_true(observed.get())
  })
}

///|
async test "broker batch ACK set excludes acknowledged indexes" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Array[Int64]] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(3)
      let (cmd, meta, payload) = message_frame(
        1UL,
        50UL,
        51UL,
        batch_test_payload(3),
        metadata,
      )
      if cmd.message is Some(message) {
        message.ack_set = [2L]
      }
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      if frame.cmd.ack is Some(ack) {
        observed.put(ack.message_id[0].ack_set)
      } else {
        peer.unread(frame)
        observed.put([1L])
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/batch-broker-set",
      "sub",
      enable_batch_index_ack=true,
    )
    let message = consumer.receive()
    assert_eq(message.id().batch_index, 1)
    assert_true(consumer.try_receive() is None)
    message.ack()
    consumer.close()
    manager.close()
    assert_eq(observed.get(), [0L])
  })
}

///|
async test "cumulative batch-index ACK leaves later indexes unacknowledged" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[(@proto.CommandAck_AckType, Array[Int64])] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(3)
      let (cmd, meta, payload) = message_frame(
        1UL,
        60UL,
        61UL,
        batch_test_payload(3),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
        fail("expected confirmed cumulative ACK")
      }
      observed.put((ack.ack_type, ack.message_id[0].ack_set))
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
      let result = @proto.CommandAckResponse::default()
      result.consumer_id = ack.consumer_id
      result.request_id = Some(id)
      response.ack_response = Some(result)
      peer.send_command(response)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/batch-cumulative",
      "sub",
      enable_batch_index_ack=true,
    )
    let _zero = consumer.receive()
    let one = consumer.receive()
    let _two = consumer.receive()
    consumer.ack_cumulative(one.id(), await_response=true)
    consumer.close()
    manager.close()
    let (ack_type, bits) = observed.get()
    assert_eq(ack_type, @proto.CommandAck_AckType::Cumulative)
    assert_eq(bits, [4L])
  })
}

///|
async test "redelivery during confirmed ACK keeps the newer batch state" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(2)
      let (cmd, meta, payload) = message_frame(
        1UL,
        70UL,
        71UL,
        batch_test_payload(2),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      for attempt in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
          fail("expected confirmed batch ACK")
        }
        assert_eq(ack.message_id[0].ack_set, [2L])
        if attempt == 0 {
          let (redelivery, redelivery_meta, redelivery_payload) = message_frame(
            1UL,
            70UL,
            71UL,
            batch_test_payload(2),
            metadata,
          )
          if redelivery.message is Some(message) {
            message.ack_set = [3L]
            message.redelivery_count = Some(1U)
          }
          peer.send_message(redelivery, redelivery_meta, redelivery_payload)
        }
        let response = @proto.BaseCommand::default()
        response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
        let result = @proto.CommandAckResponse::default()
        result.consumer_id = ack.consumer_id
        result.request_id = Some(id)
        response.ack_response = Some(result)
        peer.send_command(response)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/batch-redelivery",
      "sub",
      enable_batch_index_ack=true,
    )
    let first = consumer.receive()
    let _second = consumer.receive()
    first.ack(await_response=true)
    let again = consumer.receive()
    let _again_second = consumer.receive()
    assert_eq(again.id(), first.id())
    assert_eq(again.redelivery_count(), 1U)
    again.ack(await_response=true)
    consumer.close()
    manager.close()
  })
}
