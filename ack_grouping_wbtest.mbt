// ACK grouping must preserve wire order and flush on lifecycle boundaries.

///|
test "ACK grouping rejects negative limits" {
  let size : Result[AckGroupingOptions, Error] = Ok(
    AckGroupingOptions::new(max_size=-1),
  ) catch {
    e => Err(e)
  }
  assert_true(size is Err(InvalidFrame(_)))
  let time : Result[AckGroupingOptions, Error] = Ok(
    AckGroupingOptions::new(max_time_ms=-1),
  ) catch {
    e => Err(e)
  }
  assert_true(time is Err(InvalidFrame(_)))
}

///|
async test "ACK grouping sends two message IDs in one frame at size limit" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Array[@proto.MessageIdData]] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for entry in [1UL, 2UL] {
        let (cmd, meta, payload) = message_frame(
          1UL,
          1UL,
          entry,
          b"x",
          test_metadata(),
        )
        peer.send_message(cmd, meta, payload)
      }
      let frame = peer.next()
      if frame.cmd.ack is Some(ack) {
        observed.put(ack.message_id)
      } else {
        peer.unread(frame)
        observed.put([])
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-group-size",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=2, max_time_ms=0),
    )
    let first = consumer.receive()
    let second = consumer.receive()
    first.ack()
    second.ack()
    let ids = @async.with_timeout(1000, async fn() { observed.get() })
    assert_eq(ids.length(), 2)
    assert_eq(ids[0].entry_id, 1UL)
    assert_eq(ids[1].entry_id, 2UL)
    consumer.close()
    manager.close()
  })
}

///|
async test "ACK grouping flushes one pending message on timer" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Array[@proto.MessageIdData]] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        1UL,
        2UL,
        3UL,
        b"x",
        test_metadata(),
      )
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      if frame.cmd.ack is Some(ack) {
        observed.put(ack.message_id)
      } else {
        peer.unread(frame)
        observed.put([])
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-group-timer",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=10, max_time_ms=50),
    )
    let message = consumer.receive()
    message.ack()
    let ids = @async.with_timeout(1000, async fn() { observed.get() })
    assert_eq(ids.length(), 1)
    assert_eq(ids[0].entry_id, 3UL)
    consumer.close()
    manager.close()
  })
}

///|
async test "consumer close flushes pending ACK without a timer" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        1UL,
        3UL,
        4UL,
        b"x",
        test_metadata(),
      )
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      if frame.cmd.ack is Some(ack) {
        observed.put(
          ack.message_id.length() == 1 && ack.message_id[0].entry_id == 4UL,
        )
      } else {
        peer.unread(frame)
        observed.put(false)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-group-close",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=10, max_time_ms=0),
    )
    let message = consumer.receive()
    message.ack()
    consumer.close()
    manager.close()
    assert_true(observed.get())
  })
}

///|
async test "confirmed ACK flushes grouped ACK before requesting a response" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for entry in [5UL, 6UL] {
        let (cmd, meta, payload) = message_frame(
          1UL,
          4UL,
          entry,
          b"x",
          test_metadata(),
        )
        peer.send_message(cmd, meta, payload)
      }
      let grouped = peer.next()
      let confirmed = peer.next()
      guard confirmed.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
        fail("expected confirmed ACK")
      }
      let prior_correct = grouped.cmd.ack is Some(before) &&
        before.message_id.length() == 1 &&
        before.message_id[0].entry_id == 5UL &&
        before.request_id is None
      observed.put(prior_correct && ack.message_id[0].entry_id == 6UL)
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
      "persistent://public/default/ack-group-confirmed",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=10, max_time_ms=0),
    )
    let first = consumer.receive()
    let second = consumer.receive()
    first.ack()
    second.ack(await_response=true)
    consumer.close()
    manager.close()
    assert_true(observed.get())
  })
}

///|
async test "grouped batch-index ACK preserves successive bitmaps" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[Array[Int64]] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Unbounded,
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.num_messages_in_batch = Some(2)
      let (cmd, meta, payload) = message_frame(
        1UL,
        5UL,
        6UL,
        batch_test_payload(2),
        metadata,
      )
      peer.send_message(cmd, meta, payload)
      for _ in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.ack is Some(ack) else {
          fail("expected batch-index ACK")
        }
        observed.put(ack.message_id[0].ack_set)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-group-batch",
      "sub",
      enable_batch_index_ack=true,
      ack_grouping=AckGroupingOptions::new(max_size=2, max_time_ms=0),
    )
    let first = consumer.receive()
    let second = consumer.receive()
    first.ack()
    second.ack()
    assert_eq(observed.get(), [2L])
    assert_eq(observed.get(), [0L])
    consumer.close()
    manager.close()
  })
}

///|
async test "ACK grouping keeps only the latest cumulative position" {
  @async.with_task_group(async fn(group) {
    let observed : @aqueue.Queue[@proto.CommandAck] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      for entry in [7UL, 8UL] {
        let (cmd, meta, payload) = message_frame(
          1UL,
          6UL,
          entry,
          b"x",
          test_metadata(),
        )
        peer.send_message(cmd, meta, payload)
      }
      let frame = peer.next()
      if frame.cmd.ack is Some(ack) {
        observed.put(ack)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-group-cumulative",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=10, max_time_ms=0),
    )
    let first = consumer.receive()
    let second = consumer.receive()
    consumer.ack_cumulative(first.id())
    consumer.ack_cumulative(second.id())
    consumer.close()
    manager.close()
    let ack = observed.get()
    assert_eq(ack.ack_type, @proto.CommandAck_AckType::Cumulative)
    assert_eq(ack.message_id.length(), 1)
    assert_eq(ack.message_id[0].entry_id, 8UL)
  })
}

///|
async test "grouped message ACK after consumer close reports closed" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        1UL,
        7UL,
        9UL,
        b"x",
        test_metadata(),
      )
      peer.send_message(cmd, meta, payload)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-group-closed",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=10, max_time_ms=0),
    )
    let message = consumer.receive()
    consumer.close()
    let failure : Result[Unit, Error] = Ok(message.ack()) catch { e => Err(e) }
    assert_true(failure is Err(ConsumerClosed))
    manager.close()
  })
}

///|
async test "reconnect drops buffered ACKs and confirms the redelivered message" {
  @async.with_task_group(async fn(group) {
    let locally_acked : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let confirmed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let b2 = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        1UL,
        8UL,
        9UL,
        b"redelivered",
        test_metadata(),
      )
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
        fail("expected one confirmed ACK after reconnect")
      }
      confirmed.put(
        ack.message_id.length() == 1 && ack.message_id[0].entry_id == 9UL,
      )
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
      let result = @proto.CommandAckResponse::default()
      result.consumer_id = ack.consumer_id
      result.request_id = Some(id)
      response.ack_response = Some(result)
      peer.send_command(response)
    })
    let b1 = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let (cmd, meta, payload) = message_frame(
        1UL,
        8UL,
        9UL,
        b"original",
        test_metadata(),
      )
      peer.send_message(cmd, meta, payload)
      locally_acked.get()
      peer.tcp.close()
    })
    let lookup_addr = start_mock_broker(group, async fn(_addr, peer) {
      two_stage_lookup(peer, b1, b2)
    })
    let client = Client::connect(group, "pulsar://\{lookup_addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/ack-group-reconnect",
      "sub",
      ack_grouping=AckGroupingOptions::new(max_size=10, max_time_ms=0),
    )
    let original = @async.with_timeout(10000, async fn() { consumer.receive() })
    original.ack()
    locally_acked.put(())
    let again = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(again.payload(), b"redelivered")
    again.ack(await_response=true)
    assert_true(confirmed.get())
    consumer.close()
    client.close()
  })
}
