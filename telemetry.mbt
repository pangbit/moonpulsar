// Structured lifecycle events can be forwarded to metrics and tracing sinks.

///|
pub(all) enum ClientEvent {
  SendCompleted(String, ProducerMessage, Result[SendReceipt, Error])
  MessageReceived(Message)
  ReceiveFailed(String, Error)
  Acknowledged(String, MessageId, Result[Unit, Error])
  NegativelyAcknowledged(String, MessageId, Result[Unit, Error])
  ProducerReconnecting(String)
  ProducerReconnected(String, Result[Unit, Error])
  ConsumerReconnecting(String)
  ConsumerReconnected(String, Result[Unit, Error])
}

///|
/// Operations with optional span and latency reporting.
pub(all) enum ClientOperation {
  Send
  Receive
  Ack
  Reconnect
}

///|
/// A span's stable correlation ID and start time. `message_id` is present
/// when the operation already knows the broker message position.
pub(all) struct ClientSpan {
  id : Int64
  operation : ClientOperation
  topic : String
  message_id : MessageId?
  started_at_ms : Int64
}

///|
pub(all) enum ClientSpanEvent {
  Started(ClientSpan)
  Finished(ClientSpan, Int64, Bool, String?)
}

///|
priv struct ClientSpanRecorder {
  sink : (ClientSpanEvent) -> Unit
  mut next_id : Int64
}

///|
fn ClientSpanRecorder::new(
  sink : (ClientSpanEvent) -> Unit,
) -> ClientSpanRecorder {
  { sink, next_id: 0L, }
}

///|
fn ClientSpanRecorder::begin(
  self : ClientSpanRecorder,
  operation : ClientOperation,
  topic : String,
  message_id? : MessageId,
) -> ClientSpan {
  self.next_id += 1L
  let span = {
    id: self.next_id,
    operation,
    topic,
    message_id,
    started_at_ms: @async.now(),
  }
  (self.sink)(Started(span))
  span
}

///|
fn ClientSpanRecorder::finish(
  self : ClientSpanRecorder,
  span : ClientSpan,
  success : Bool,
  error? : String,
) -> Unit {
  let duration = @async.now() - span.started_at_ms
  (self.sink)(
    Finished(span, if duration < 0L { 0L } else { duration }, success, error),
  )
}

///|
fn[T] ClientSpanRecorder::finish_result(
  self : ClientSpanRecorder,
  span : ClientSpan,
  result : Result[T, Error],
) -> Unit {
  match result {
    Ok(_) => self.finish(span, true)
    Err(e) => self.finish(span, false, error="\{e}")
  }
}

///|
priv struct DurationHistogram {
  mut count : Int64
  mut sum_ms : Int64
  buckets : Array[Int64]
}

///|
fn DurationHistogram::new() -> DurationHistogram {
  { count: 0L, sum_ms: 0L, buckets: [0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L], }
}

///|
let duration_bounds_ms : Array[Int64] = [
  1L, 5L, 10L, 50L, 100L, 500L, 1000L, 5000L,
]

///|
fn ClientOperation::label(self : ClientOperation) -> String {
  match self {
    Send => "send"
    Receive => "receive"
    Ack => "ack"
    Reconnect => "reconnect"
  }
}

///|
pub struct ClientMetrics {
  priv mut sends : Int64
  priv mut send_failures : Int64
  priv mut received : Int64
  priv mut receive_failures : Int64
  priv mut acknowledgments : Int64
  priv mut ack_failures : Int64
  priv mut negative_acknowledgments : Int64
  priv mut negative_ack_failures : Int64
  priv mut producer_reconnects : Int64
  priv mut consumer_reconnects : Int64
  priv mut reconnect_failures : Int64
  priv durations : Map[String, DurationHistogram]
}

///|
pub(all) struct ClientMetricsSnapshot {
  sends : Int64
  send_failures : Int64
  received : Int64
  receive_failures : Int64
  acknowledgments : Int64
  ack_failures : Int64
  negative_acknowledgments : Int64
  negative_ack_failures : Int64
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
    receive_failures: 0L,
    acknowledgments: 0L,
    ack_failures: 0L,
    negative_acknowledgments: 0L,
    negative_ack_failures: 0L,
    producer_reconnects: 0L,
    consumer_reconnects: 0L,
    reconnect_failures: 0L,
    durations: Map([]),
  }
}

///|
/// Record a span's completion in fixed millisecond buckets.
pub fn ClientMetrics::record_span(
  self : ClientMetrics,
  event : ClientSpanEvent,
) -> Unit {
  match event {
    Started(_) => ()
    Finished(span, duration, _, _) => {
      let duration = if duration < 0L { 0L } else { duration }
      let label = span.operation.label()
      let histogram = match self.durations.get(label) {
        Some(value) => value
        None => {
          let value = DurationHistogram::new()
          self.durations[label] = value
          value
        }
      }
      histogram.count += 1L
      histogram.sum_ms += duration
      for index, bound in duration_bounds_ms {
        if duration <= bound {
          histogram.buckets[index] += 1L
        }
      }
    }
  }
}

///|
/// Export bounded-label counters and operation-duration histograms.
pub fn ClientMetrics::prometheus_text(self : ClientMetrics) -> String {
  let mut output = "# TYPE moonpulsar_client_events_total counter\n"
  let counts = [
    ("sends", self.sends),
    ("send_failures", self.send_failures),
    ("received", self.received),
    ("receive_failures", self.receive_failures),
    ("acknowledgments", self.acknowledgments),
    ("ack_failures", self.ack_failures),
    ("negative_acknowledgments", self.negative_acknowledgments),
    ("negative_ack_failures", self.negative_ack_failures),
    ("producer_reconnects", self.producer_reconnects),
    ("consumer_reconnects", self.consumer_reconnects),
    ("reconnect_failures", self.reconnect_failures),
  ]
  for entry in counts {
    let (label, value) = entry
    output += "moonpulsar_client_events_total{event=\"\{label}\"} \{value.to_string()}\n"
  }
  output += "# TYPE moonpulsar_operation_duration_ms histogram\n"
  for operation in [Send, Receive, Ack, Reconnect] {
    let label = operation.label()
    let histogram = self.durations
      .get(label)
      .unwrap_or(DurationHistogram::new())
    for index, bound in duration_bounds_ms {
      output += "moonpulsar_operation_duration_ms_bucket{operation=\"\{label}\",le=\"\{bound}\"} \{histogram.buckets[index]}\n"
    }
    output += "moonpulsar_operation_duration_ms_bucket{operation=\"\{label}\",le=\"+Inf\"} \{histogram.count}\n"
    output += "moonpulsar_operation_duration_ms_sum{operation=\"\{label}\"} \{histogram.sum_ms}\n"
    output += "moonpulsar_operation_duration_ms_count{operation=\"\{label}\"} \{histogram.count}\n"
  }
  output
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
    ReceiveFailed(_, _) => self.receive_failures += 1L
    Acknowledged(_, _, result) => {
      self.acknowledgments += 1L
      if result is Err(_) {
        self.ack_failures += 1L
      }
    }
    NegativelyAcknowledged(_, _, result) => {
      self.negative_acknowledgments += 1L
      if result is Err(_) {
        self.negative_ack_failures += 1L
      }
    }
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
    receive_failures: self.receive_failures,
    acknowledgments: self.acknowledgments,
    ack_failures: self.ack_failures,
    negative_acknowledgments: self.negative_acknowledgments,
    negative_ack_failures: self.negative_ack_failures,
    producer_reconnects: self.producer_reconnects,
    consumer_reconnects: self.consumer_reconnects,
    reconnect_failures: self.reconnect_failures,
  }
}
