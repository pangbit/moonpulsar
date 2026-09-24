// Structured lifecycle events can be forwarded to metrics and tracing sinks.

///|
pub(all) enum ClientEvent {
  SendCompleted(String, ProducerMessage, Result[SendReceipt, Error])
  MessageReceived(Message)
  Acknowledged(String, MessageId, Result[Unit, Error])
  NegativelyAcknowledged(String, MessageId, Result[Unit, Error])
  ProducerReconnecting(String)
  ProducerReconnected(String, Result[Unit, Error])
  ConsumerReconnecting(String)
  ConsumerReconnected(String, Result[Unit, Error])
}

///|
pub struct ClientMetrics {
  priv mut sends : Int64
  priv mut send_failures : Int64
  priv mut received : Int64
  priv mut acknowledgments : Int64
  priv mut ack_failures : Int64
  priv mut negative_acknowledgments : Int64
  priv mut producer_reconnects : Int64
  priv mut consumer_reconnects : Int64
  priv mut reconnect_failures : Int64
}

///|
pub(all) struct ClientMetricsSnapshot {
  sends : Int64
  send_failures : Int64
  received : Int64
  acknowledgments : Int64
  ack_failures : Int64
  negative_acknowledgments : Int64
  producer_reconnects : Int64
  consumer_reconnects : Int64
  reconnect_failures : Int64
} derive(Eq, Debug)

///|
pub fn ClientMetrics::new() -> ClientMetrics {
  {
    sends: 0L,
    send_failures: 0L,
    received: 0L,
    acknowledgments: 0L,
    ack_failures: 0L,
    negative_acknowledgments: 0L,
    producer_reconnects: 0L,
    consumer_reconnects: 0L,
    reconnect_failures: 0L,
  }
}

///|
pub fn ClientMetrics::record(self : ClientMetrics, event : ClientEvent) -> Unit {
  match event {
    SendCompleted(_, _, result) => {
      self.sends += 1L
      if result is Err(_) {
        self.send_failures += 1L
      }
    }
    MessageReceived(_) => self.received += 1L
    Acknowledged(_, _, result) => {
      self.acknowledgments += 1L
      if result is Err(_) {
        self.ack_failures += 1L
      }
    }
    NegativelyAcknowledged(_, _, _) => self.negative_acknowledgments += 1L
    ProducerReconnecting(_) | ConsumerReconnecting(_) => ()
    ProducerReconnected(_, result) => {
      self.producer_reconnects += 1L
      if result is Err(_) {
        self.reconnect_failures += 1L
      }
    }
    ConsumerReconnected(_, result) => {
      self.consumer_reconnects += 1L
      if result is Err(_) {
        self.reconnect_failures += 1L
      }
    }
  }
}

///|
pub fn ClientMetrics::snapshot(self : ClientMetrics) -> ClientMetricsSnapshot {
  {
    sends: self.sends,
    send_failures: self.send_failures,
    received: self.received,
    acknowledgments: self.acknowledgments,
    ack_failures: self.ack_failures,
    negative_acknowledgments: self.negative_acknowledgments,
    producer_reconnects: self.producer_reconnects,
    consumer_reconnects: self.consumer_reconnects,
    reconnect_failures: self.reconnect_failures,
  }
}
