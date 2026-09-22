// Consumer: subscribes to a topic and receives messages with flow control.

///|
/// Subscription mode, mirroring `CommandSubscribe.SubType`.
pub(all) enum SubscriptionType {
  Exclusive
  Shared
  Failover
  KeyShared
} derive(Eq, Debug)

///|
fn SubscriptionType::to_proto(
  self : SubscriptionType,
) -> @proto.CommandSubscribe_SubType {
  match self {
    Exclusive => @proto.CommandSubscribe_SubType::Exclusive
    Shared => @proto.CommandSubscribe_SubType::Shared
    Failover => @proto.CommandSubscribe_SubType::Failover
    KeyShared => @proto.CommandSubscribe_SubType::Key_Shared
  }
}

///|
/// Where a subscription starts consuming.
pub(all) enum SubscriptionInitialPosition {
  Latest
  Earliest
} derive(Eq, Debug)

///|
/// A consumer bound to a topic subscription.
pub struct Consumer {
  priv conn : Connection
  priv topic : String
  priv subscription : String
  priv consumer_id : UInt64
  priv messages : @aqueue.Queue[Message]
  priv receiver_permits : Int
  priv mut available_permits : Int
  priv closed : Ref[Bool]
}

///|
/// Create a consumer on `topic` through an already-resolved connection.
pub async fn Consumer::create(
  conn : Connection,
  topic : String,
  subscription : String,
  sub_type? : SubscriptionType = Exclusive,
  consumer_name? : String,
  initial_position? : SubscriptionInitialPosition = Latest,
  receiver_queue_size? : Int = 1000,
) -> Consumer {
  let request_id = conn.next_request_id()
  let consumer_id = conn.next_request_id()
  let consumer : Consumer = {
    conn,
    topic,
    subscription,
    consumer_id,
    messages: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
    receiver_permits: receiver_queue_size,
    available_permits: 0,
    closed: { val: false, },
  }
  conn.message_handlers[consumer_id] = fn(frame) { consumer.on_frame(frame) }
  conn.on_close(fn(e) { consumer.on_connection_closed(e) })
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SUBSCRIBE
  let subscribe = @proto.CommandSubscribe::default()
  subscribe.topic = topic
  subscribe.subscription = subscription
  subscribe.sub_type = sub_type.to_proto()
  subscribe.consumer_id = consumer_id
  subscribe.request_id = request_id
  if consumer_name is Some(name) {
    subscribe.consumer_name = Some(name)
  }
  match initial_position {
    Latest =>
      subscribe.initial_position = Some(
        @proto.CommandSubscribe_InitialPosition::Latest,
      )
    Earliest =>
      subscribe.initial_position = Some(
        @proto.CommandSubscribe_InitialPosition::Earliest,
      )
  }
  cmd.subscribe = Some(subscribe)
  errdefer conn.message_handlers.remove(consumer_id)
  let reply = conn.request(cmd)
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for SUBSCRIBE")
  }
  // start the flow of messages
  consumer.send_flow(receiver_queue_size)
  consumer.available_permits = receiver_queue_size
  consumer
}

///|
fn Consumer::on_connection_closed(self : Consumer, error : Error?) -> Unit {
  ignore(error)
  if !self.closed.val {
    self.closed.val = true
    self.messages.close(error=ConnectionClosed, clear=true)
  }
}

///|
async fn Consumer::send_flow(self : Consumer, permits : Int) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::FLOW
  let flow = @proto.CommandFlow::default()
  flow.consumer_id = self.consumer_id
  flow.message_permits = permits.reinterpret_as_uint()
  cmd.flow = Some(flow)
  self.conn.send_command(cmd)
}

///|
fn Consumer::on_frame(self : Consumer, frame : IncomingFrame) -> Unit {
  match frame.cmd.type_ {
    @proto.BaseCommand_Type::MESSAGE =>
      self.dispatch_message(frame) catch {
        _ => ()
      }
    @proto.BaseCommand_Type::CLOSE_CONSUMER =>
      if !self.closed.val {
        self.closed.val = true
        self.messages.close(error=ConsumerClosed, clear=true)
      }
    _ => ()
  }
}

///|
fn Consumer::dispatch_message(
  self : Consumer,
  frame : IncomingFrame,
) -> Unit raise {
  guard frame.cmd.message is Some(message) else { return }
  guard frame.metadata is Some(metadata) && frame.payload is Some(payload) else {
    raise InvalidFrame("MESSAGE without payload")
  }
  let redelivery_count = message.redelivery_count.unwrap_or(0)
  let base_id = MessageId::from_proto(message.message_id)
  // proto2 default for num_messages_in_batch is Some(1): only >1 is a batch
  let batch_size = metadata.num_messages_in_batch.unwrap_or(0)
  if batch_size > 1 {
    let singles = split_batch_payload(payload, batch_size)
    for i, single in singles {
      let (meta, body) = single
      self.push_message(
        base_id,
        i,
        metadata,
        Some(meta),
        body,
        redelivery_count,
      )
    }
  } else {
    self.push_message(base_id, -1, metadata, None, payload, redelivery_count)
  }
}

///|
fn Consumer::push_message(
  self : Consumer,
  base_id : MessageId,
  batch_index : Int,
  metadata : @proto.MessageMetadata,
  single : @proto.SingleMessageMetadata?,
  payload : Bytes,
  redelivery_count : UInt,
) -> Unit raise {
  let message : Message = {
    conn: self.conn,
    consumer_id: self.consumer_id,
    id: { ..base_id, batch_index, },
    topic: self.topic,
    payload,
    key: match single {
      Some(s) =>
        if s.partition_key is Some(_) {
          s.partition_key
        } else {
          metadata.partition_key
        }
      None => metadata.partition_key
    },
    properties: match single {
      Some(s) =>
        if s.properties.is_empty() {
          properties_of(metadata.properties)
        } else {
          properties_of(s.properties)
        }
      None => properties_of(metadata.properties)
    },
    publish_time: metadata.publish_time,
    event_time: match single {
      Some(s) =>
        if s.event_time is Some(_) {
          s.event_time
        } else {
          metadata.event_time
        }
      None => metadata.event_time
    },
    producer_name: metadata.producer_name,
    sequence_id: match single {
      Some(s) => s.sequence_id
      None => Some(metadata.sequence_id)
    },
    redelivery_count,
  }
  ignore(self.messages.try_put(message))
}

///|
/// Split a batched payload into per-message metadata and payload.
fn split_batch_payload(
  payload : Bytes,
  expected : Int,
) -> Array[(@proto.SingleMessageMetadata, Bytes)] raise {
  let result = []
  let mut offset = 0
  for _ in 0..<expected {
    let meta_size = read_be32(payload, offset).reinterpret_as_int()
    offset += 4
    if offset + meta_size > payload.length() {
      raise InvalidFrame("single message metadata exceeds payload")
    }
    let meta : @proto.SingleMessageMetadata = read_proto(
      payload[offset:offset + meta_size].to_owned(),
    )
    offset += meta_size
    let payload_size = read_be32(payload, offset).reinterpret_as_int()
    offset += 4
    if offset + payload_size > payload.length() {
      raise InvalidFrame("single message payload exceeds payload")
    }
    let body = payload[offset:offset + payload_size].to_owned()
    offset += payload_size
    result.push((meta, body))
  }
  result
}

///|
/// Receive the next message, blocking until one is available.
pub async fn Consumer::receive(self : Consumer) -> Message {
  if self.closed.val {
    raise ConsumerClosed
  }
  let message = self.messages.get()
  self.available_permits -= 1
  let threshold = self.receiver_permits / 2
  if self.available_permits <= threshold {
    let refill = self.receiver_permits - self.available_permits
    self.send_flow(refill) catch {
      _ => ()
    }
    self.available_permits += refill
  }
  message
}

///|
/// Non-blocking receive: returns `None` if no message is buffered.
pub fn Consumer::try_receive(self : Consumer) -> Message? raise {
  self.messages.try_get() catch {
    _ => raise ConsumerClosed
  }
}

///|
/// Acknowledge a message individually.
pub async fn Consumer::ack(self : Consumer, id : MessageId) -> Unit {
  send_ack(self.conn, self.consumer_id, @proto.CommandAck_AckType::Individual, [
    id,
  ])
}

///|
/// Acknowledge cumulatively: this message and all before it.
pub async fn Consumer::ack_cumulative(self : Consumer, id : MessageId) -> Unit {
  send_ack(self.conn, self.consumer_id, @proto.CommandAck_AckType::Cumulative, [
    id,
  ])
}

///|
/// Negatively acknowledge a message, triggering redelivery.
pub async fn Consumer::nack(self : Consumer, id : MessageId) -> Unit {
  send_redeliver(self.conn, self.consumer_id, [id])
}

///|
/// Close the consumer and its subscription cursor is kept (durable).
pub async fn Consumer::close(self : Consumer) -> Unit {
  if self.closed.val {
    return
  }
  self.closed.val = true
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
  let close = @proto.CommandCloseConsumer::default()
  close.consumer_id = self.consumer_id
  close.request_id = self.conn.next_request_id()
  cmd.close_consumer = Some(close)
  ignore(self.conn.request(cmd) catch { _ => @proto.BaseCommand::default() })
  self.conn.message_handlers.remove(self.consumer_id)
  self.messages.close(error=ConsumerClosed, clear=true)
}

///|
pub fn Consumer::topic(self : Consumer) -> String {
  self.topic
}

///|
pub fn Consumer::subscription(self : Consumer) -> String {
  self.subscription
}

///|
pub fn Consumer::is_closed(self : Consumer) -> Bool {
  self.closed.val
}
