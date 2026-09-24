// Metrics count structured events without a broker or exported runtime.

///|
test "client metrics classify success failure ACK and reconnect events" {
  let metrics = ClientMetrics::new()
  let message = ProducerMessage::new(b"data")
  let receipt : SendReceipt = {
    message_id: MessageId::earliest(),
    sequence_id: 1UL,
    producer_id: 2UL,
    producer_name: "metrics",
  }
  let id = receipt.message_id
  metrics.record(ClientEvent::SendCompleted("topic", message, Ok(receipt)))
  metrics.record(
    ClientEvent::SendCompleted("topic", message, Err(ProducerQueueFull)),
  )
  metrics.record(ClientEvent::Acknowledged("topic", id, Ok(())))
  metrics.record(ClientEvent::Acknowledged("topic", id, Err(ConnectionClosed)))
  metrics.record(ClientEvent::ReceiveFailed("topic", ConnectionClosed))
  metrics.record(ClientEvent::NegativelyAcknowledged("topic", id, Ok(())))
  metrics.record(
    ClientEvent::NegativelyAcknowledged("topic", id, Err(ConnectionClosed)),
  )
  metrics.record(ClientEvent::ProducerReconnecting("topic"))
  metrics.record(ClientEvent::ProducerReconnected("topic", Ok(())))
  metrics.record(ClientEvent::ConsumerReconnecting("topic"))
  metrics.record(
    ClientEvent::ConsumerReconnected("topic", Err(ConnectionClosed)),
  )
  let snapshot = metrics.snapshot()
  assert_eq(snapshot.sends, 2L)
  assert_eq(snapshot.send_failures, 1L)
  assert_eq(snapshot.received, 0L)
  assert_eq(snapshot.receive_failures, 1L)
  assert_eq(snapshot.acknowledgments, 2L)
  assert_eq(snapshot.ack_failures, 1L)
  assert_eq(snapshot.negative_acknowledgments, 2L)
  assert_eq(snapshot.negative_ack_failures, 1L)
  assert_eq(snapshot.producer_reconnects, 1L)
  assert_eq(snapshot.consumer_reconnects, 1L)
  assert_eq(snapshot.reconnect_failures, 1L)
}

///|
test "Prometheus export uses bounded operation labels and cumulative buckets" {
  let metrics = ClientMetrics::new()
  let send : ClientSpan = {
    id: 1L,
    operation: Send,
    topic: "persistent://public/default/private-topic",
    message_id: None,
    started_at_ms: 0L,
  }
  metrics.record_span(Started(send))
  metrics.record_span(Finished(send, 7L, true, None))
  metrics.record_span(Finished(send, 80L, false, Some("failure")))
  let output = metrics.prometheus_text()
  assert_true(
    output.contains(
      "moonpulsar_operation_duration_ms_bucket{operation=\"send\",le=\"10\"} 1",
    ),
  )
  assert_true(
    output.contains(
      "moonpulsar_operation_duration_ms_bucket{operation=\"send\",le=\"100\"} 2",
    ),
  )
  assert_true(
    output.contains(
      "moonpulsar_operation_duration_ms_sum{operation=\"send\"} 87",
    ),
  )
  assert_true(!output.contains("private-topic"))
  let negative = ClientMetrics::new()
  negative.record_span(Finished(send, -5L, false, Some("clock skew")))
  assert_true(
    negative
    .prometheus_text()
    .contains("moonpulsar_operation_duration_ms_sum{operation=\"send\"} 0"),
  )
}

///|
async test "client spans pair send receive and ACK outcomes" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/telemetry-spans"
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      guard peer.next().cmd.flow is Some(_) else { fail("expected FLOW") }
      let sent = peer.next()
      guard sent.cmd.send is Some(send) else { fail("expected SEND") }
      send_receipt(peer, send.producer_id, send.sequence_id)
      let (cmd, metadata, payload) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        b"observed",
        test_metadata(),
      )
      peer.send_message(cmd, metadata, payload)
      guard peer.next().cmd.ack is Some(_) else { fail("expected ACK") }
    })
    let events : Array[ClientSpanEvent] = []
    let metrics = ClientMetrics::new()
    let client = Client::connect_with_options(
      group,
      ClientOptions::new(
        "pulsar://\{addr}",
        on_event=fn(event) { metrics.record(event) },
        on_span=fn(event) {
          events.push(event)
          metrics.record_span(event)
        },
      ),
    )
    let producer = client.create_producer(topic)
    let consumer = client.create_consumer(topic, "telemetry-sub")
    ignore(producer.send(ProducerMessage::new(b"observed")))
    let message = consumer.receive()
    message.ack()
    assert_eq(events.length(), 6)
    for index in 0..<3 {
      guard events[index * 2] is Started(start) &&
        events[index * 2 + 1] is Finished(finish, duration, true, None) else {
        fail("expected paired successful span")
      }
      assert_eq(start.id, finish.id)
      assert_true(duration >= 0L)
    }
    let output = metrics.prometheus_text()
    assert_true(
      output.contains(
        "moonpulsar_operation_duration_ms_count{operation=\"send\"} 1",
      ),
    )
    assert_true(
      output.contains(
        "moonpulsar_operation_duration_ms_count{operation=\"receive\"} 1",
      ),
    )
    assert_true(
      output.contains(
        "moonpulsar_operation_duration_ms_count{operation=\"ack\"} 1",
      ),
    )
    consumer.close()
    producer.close()
    let ack_error : Result[Unit, Error] = Ok(message.ack()) catch {
      e => Err(e)
    }
    assert_true(ack_error is Err(ConsumerClosed))
    let receive_error : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(receive_error is Err(ConsumerClosed))
    let send_error : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"after-close")),
    ) catch {
      e => Err(e)
    }
    assert_true(send_error is Err(ProducerClosed))
    assert_eq(events.length(), 12)
    for index in 3..<6 {
      assert_true(events[index * 2 + 1] is Finished(_, _, false, Some(_)))
    }
    let snapshot = metrics.snapshot()
    assert_eq(snapshot.send_failures, 1L)
    assert_eq(snapshot.receive_failures, 1L)
    assert_eq(snapshot.ack_failures, 1L)
    client.close()
  })
}

///|
async test "cancelled receive closes its span" {
  @async.with_task_group(async fn(group) {
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      expect_subscribe(peer)
    })
    let events : Array[ClientSpanEvent] = []
    let client = Client::connect_with_options(
      group,
      ClientOptions::new("pulsar://\{addr}", on_span=fn(event) {
        events.push(event)
      }),
    )
    let consumer = client.create_consumer(
      "persistent://public/default/span-cancel", "sub",
    )
    let timed_out : Result[Message, Error] = Ok(
      @async.with_timeout(25, async fn() { consumer.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(timed_out is Err(@async.TimeoutError))
    assert_eq(events.length(), 2)
    guard events[0] is Started(start) &&
      events[1] is Finished(finish, _, false, Some(_)) else {
      fail("expected a cancelled receive span")
    }
    assert_eq(start.id, finish.id)
    consumer.close()
    client.close()
  })
}
