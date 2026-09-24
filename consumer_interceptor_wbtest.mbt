// Delivery and ACK hooks belong to the logical consumer and delivered message.

///|
async test "consumer interceptor observes message and ID acknowledgments and nack" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      guard peer.next().cmd.flow is Some(_) else { fail("expected FLOW") }
      for entry in 1..<=3 {
        let (cmd, metadata, payload) = message_frame(
          sub.consumer_id,
          1UL,
          entry.to_uint64(),
          b"data",
          test_metadata(),
        )
        peer.send_message(cmd, metadata, payload)
      }
      guard peer.next().cmd.ack is Some(_) else { fail("expected ACK") }
      guard peer.next().cmd.redeliver_unacknowledged_messages is Some(_) else {
        fail("expected negative ACK")
      }
      guard peer.next().cmd.ack is Some(_) else { fail("expected ID ACK") }
    })
    let manager = ConnectionManager::new(group)
    let consumer = Consumer::single(
      SingleConsumer::create(
        group,
        manager.get(addr),
        "persistent://public/default/interceptor-consume",
        "sub",
      ),
    )
    let delivered : Array[UInt64] = []
    let acked : Array[UInt64] = []
    let nacked : Array[UInt64] = []
    consumer.set_interceptors([
      ConsumerInterceptor::new(
        before_consume=fn(message) { delivered.push(message.id().entry_id) },
        on_ack=fn(id, result) { if result is Ok(_) { acked.push(id.entry_id) } },
        on_nack=fn(id, result) {
          if result is Ok(_) {
            nacked.push(id.entry_id)
          }
        },
      ),
    ])
    let first = consumer.receive()
    first.ack()
    let second = consumer.receive()
    second.nack()
    let third = consumer.receive()
    consumer.ack(third.id())
    assert_eq(delivered, [1UL, 2UL, 3UL])
    assert_eq(acked, [1UL, 3UL])
    assert_eq(nacked, [2UL])
    consumer.close()
    manager.close()
  })
}

///|
async test "consumer interceptor observes receive failure after close" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let manager = ConnectionManager::new(group)
    let consumer = Consumer::single(
      SingleConsumer::create(
        group,
        manager.get(addr),
        "persistent://public/default/interceptor-receive-error",
        "sub",
      ),
    )
    let failures : Array[Error] = []
    consumer.set_interceptors([
      ConsumerInterceptor::new(on_receive_error=fn(error) {
        failures.push(error)
      }),
    ])
    consumer.close()
    let result : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConsumerClosed))
    let nonblocking : Result[Message?, Error] = Ok(consumer.try_receive()) catch {
      e => Err(e)
    }
    assert_true(nonblocking is Err(ConsumerClosed))
    assert_eq(failures.length(), 2)
    assert_true(failures[0] is ConsumerClosed)
    manager.close()
  })
}

///|
async test "client event and metrics record failed asynchronous receive" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let metrics = ClientMetrics::new()
    let failed_topics : Array[String] = []
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{addr}", on_event=fn(event) {
        if event is ClientEvent::ReceiveFailed(topic, _) {
          failed_topics.push(topic)
        }
        metrics.record(event)
      }),
    )
    let topic = "persistent://public/default/client-receive-error"
    let consumer = client.observe_consumer(
      Consumer::single(
        SingleConsumer::create(group, client.manager.get(addr), topic, "sub"),
      ),
      [],
    )
    consumer.close()
    let result : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(result is Err(ConsumerClosed))
    let nonblocking : Result[Message?, Error] = Ok(consumer.try_receive()) catch {
      e => Err(e)
    }
    assert_true(nonblocking is Err(ConsumerClosed))
    assert_eq(failed_topics, [topic, topic])
    assert_eq(metrics.snapshot().receive_failures, 2L)
    client.close()
  })
}

///|
async test "consumer interceptor sees rejected confirmed message ACK" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      guard peer.next().cmd.flow is Some(_) else { fail("expected FLOW") }
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        b"data",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      let frame = peer.next()
      guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
        fail("expected confirmed ACK")
      }
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
      let ack_response = @proto.CommandAckResponse::default()
      ack_response.consumer_id = ack.consumer_id
      ack_response.request_id = Some(id)
      ack_response.error = Some(@proto.ServerError::MetadataError)
      ack_response.message = Some("ack denied")
      response.ack_response = Some(ack_response)
      peer.send_command(response)
    })
    let manager = ConnectionManager::new(group)
    let consumer = Consumer::single(
      SingleConsumer::create(
        group,
        manager.get(addr),
        "persistent://public/default/interceptor-ack-error",
        "sub",
      ),
    )
    let seen : Array[Bool] = []
    consumer.set_interceptors([
      ConsumerInterceptor::new(on_ack=fn(_id, result) {
        seen.push(result is Err(ServerError(message="ack denied", ..)))
      }),
    ])
    let message = consumer.receive()
    let failed : Result[Unit, Error] = Ok(message.ack(await_response=true)) catch {
      e => Err(e)
    }
    assert_true(failed is Err(ServerError(message="ack denied", ..)))
    assert_eq(seen, [true])
    consumer.close()
    manager.close()
  })
}

///|
async test "composite consumer intercepts only application delivery" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      guard peer.next().cmd.flow is Some(_) else { fail("expected FLOW") }
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        b"data",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      guard peer.next().cmd.ack is Some(_) else { fail("expected ACK") }
    })
    let manager = ConnectionManager::new(group)
    let source = Consumer::single(
      SingleConsumer::create(
        group,
        manager.get(addr),
        "persistent://public/default/interceptor-source",
        "sub",
      ),
    )
    let combined = Consumer::partitioned(group, [source])
    let delivered : Ref[Int] = { val: 0, }
    let acked : Ref[Int] = { val: 0, }
    combined.set_interceptors([
      ConsumerInterceptor::new(
        before_consume=fn(_message) { delivered.val += 1 },
        on_ack=fn(_id, result) { if result is Ok(_) { acked.val += 1 } },
      ),
    ])
    let message = combined.receive()
    message.ack()
    assert_eq(delivered.val, 1)
    assert_eq(acked.val, 1)
    combined.close()
    manager.close()
  })
}
