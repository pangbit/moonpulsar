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
