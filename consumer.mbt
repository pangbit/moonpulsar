// SingleConsumer: subscribes to a topic and receives messages with flow control.

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
/// Broker-side dispatch policy for a KeyShared subscription.
pub struct KeySharedPolicy {
  priv mode : @proto.KeySharedMode
  priv ranges : Array[(Int, Int)]
  priv allow_out_of_order_delivery : Bool
}

///|
pub fn KeySharedPolicy::auto_split(
  allow_out_of_order_delivery? : Bool = false,
) -> KeySharedPolicy {
  {
    mode: @proto.KeySharedMode::AUTO_SPLIT,
    ranges: [],
    allow_out_of_order_delivery,
  }
}

///|
/// Reserve non-overlapping inclusive hash ranges within 0..65535.
pub fn KeySharedPolicy::sticky(
  ranges : Array[(Int, Int)],
  allow_out_of_order_delivery? : Bool = false,
) -> KeySharedPolicy raise PulsarError {
  guard !ranges.is_empty() else {
    raise InvalidFrame("sticky KeyShared requires hash ranges")
  }
  let checked : Array[(Int, Int)] = []
  for i, range in ranges {
    let (start, end) = range
    guard start >= 0 && start < end && end <= 65535 else {
      raise InvalidFrame("invalid KeyShared hash range")
    }
    for j in 0..<i {
      let (other_start, other_end) = checked[j]
      guard end < other_start || start > other_end else {
        raise InvalidFrame("overlapping KeyShared hash ranges")
      }
    }
    checked.push(range)
  }
  {
    mode: @proto.KeySharedMode::STICKY,
    ranges: checked,
    allow_out_of_order_delivery,
  }
}

///|
fn KeySharedPolicy::to_proto(self : KeySharedPolicy) -> @proto.KeySharedMeta {
  let meta = @proto.KeySharedMeta::default()
  meta.key_shared_mode = self.mode
  meta.allow_out_of_order_delivery = Some(self.allow_out_of_order_delivery)
  for range in self.ranges {
    let (start, end) = range
    let proto_range = @proto.IntRange::default()
    proto_range.start = start
    proto_range.end = end
    meta.hash_ranges.push(proto_range)
  }
  meta
}

///|
/// Where a subscription starts consuming.
pub(all) enum SubscriptionInitialPosition {
  Latest
  Earliest
} derive(Eq, Debug)

///|
/// Exponential delay before redelivering negatively acknowledged messages.
pub struct NackBackoff {
  priv initial_ms : Int
  priv max_ms : Int
}

///|
pub fn NackBackoff::new(
  initial_ms : Int,
  max_ms : Int,
) -> NackBackoff raise PulsarError {
  guard initial_ms > 0 && max_ms >= initial_ms else {
    raise InvalidFrame("invalid negative-acknowledgment backoff")
  }
  { initial_ms, max_ms, }
}

///|
/// Default to one second initially, capped at ten minutes.
pub fn NackBackoff::default() -> NackBackoff {
  { initial_ms: 1000, max_ms: 600000, }
}

///|
pub fn NackBackoff::delay_ms(
  self : NackBackoff,
  redelivery_count : UInt,
) -> Int {
  let mut delay = self.initial_ms
  let mut remaining = redelivery_count
  while remaining > 0U && delay < self.max_ms {
    delay = if delay > self.max_ms / 2 { self.max_ms } else { delay * 2 }
    remaining -= 1U
  }
  delay
}

///|
/// A consumer bound to a topic subscription.
pub struct SingleConsumer {
  /// current connection; swapped on reconnect (shared with `Message` acks)
  priv conn_ref : Ref[Connection]
  priv topic : String
  priv subscription : String
  priv mut consumer_id : UInt64
  priv sub_type : SubscriptionType
  priv key_shared_policy : KeySharedPolicy?
  priv schema : SchemaInfo?
  priv consumer_name : String?
  priv initial_position : SubscriptionInitialPosition
  priv priority_level : Int
  priv durable : Bool
  priv read_compacted : Bool
  priv properties : Map[String, String]
  priv subscription_properties : Map[String, String]
  priv messages : @aqueue.Queue[Message]
  priv receiver_permits : Int
  priv mut available_permits : Int
  priv ack_timeout_ms : Int
  priv nack_backoff : NackBackoff?
  priv nack_epoch : Ref[Int]
  priv ack_pending : Map[MessageId, Int]
  priv mut next_ack_token : Int
  priv chunks : ChunkAssembler
  priv chunk_acks : Map[MessageId, Array[MessageId]]
  priv reconnect_with : (async () -> Connection)?
  priv reconnecting : Ref[Bool]
  priv seeking : Ref[Bool]
  priv seek_mu : @async.Mutex
  priv group : @async.TaskGroup[Unit]
  priv closed : Ref[Bool]
}

///|
/// Create a consumer on `topic` through an already-resolved connection.
pub async fn SingleConsumer::create(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  subscription : String,
  sub_type? : SubscriptionType = Exclusive,
  key_shared_policy? : KeySharedPolicy,
  consumer_name? : String,
  initial_position? : SubscriptionInitialPosition = Latest,
  priority_level? : Int = 0,
  durable? : Bool = true,
  read_compacted? : Bool = false,
  properties? : Map[String, String] = {},
  subscription_properties? : Map[String, String] = {},
  receiver_queue_size? : Int = 1000,
  ack_timeout_ms? : Int = 0,
  nack_backoff? : NackBackoff,
  reconnect_with? : async () -> Connection,
  schema? : SchemaInfo,
) -> SingleConsumer {
  guard ack_timeout_ms >= 0 else {
    raise InvalidFrame("ack timeout must not be negative")
  }
  guard receiver_queue_size > 0 else {
    raise InvalidFrame("receiver queue size must be positive")
  }
  if key_shared_policy is Some(_) && sub_type != KeyShared {
    raise InvalidFrame("KeyShared policy requires KeyShared subscription")
  }
  let request_id = conn.next_request_id()
  let consumer_id = conn.next_request_id()
  let consumer : SingleConsumer = {
    conn_ref: { val: conn, },
    topic,
    subscription,
    consumer_id,
    sub_type,
    key_shared_policy,
    schema,
    consumer_name,
    initial_position,
    priority_level,
    durable,
    read_compacted,
    properties,
    subscription_properties,
    messages: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
    receiver_permits: receiver_queue_size,
    available_permits: 0,
    ack_timeout_ms,
    nack_backoff,
    nack_epoch: { val: 0, },
    ack_pending: {},
    next_ack_token: 0,
    chunks: ChunkAssembler::new(),
    chunk_acks: {},
    reconnect_with,
    reconnecting: { val: false, },
    seeking: { val: false, },
    seek_mu: @async.Mutex::Mutex(),
    group,
    closed: { val: false, },
  }
  consumer.attach(conn)
  errdefer conn.message_handlers.remove(consumer_id)
  let reply = conn.request(consumer.subscribe_command(request_id))
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for SUBSCRIBE")
  }
  // start the flow of messages
  consumer.send_flow(receiver_queue_size)
  consumer.available_permits = receiver_queue_size
  consumer
}

///|
fn SingleConsumer::subscribe_command(
  self : SingleConsumer,
  request_id : UInt64,
) -> @proto.BaseCommand {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SUBSCRIBE
  let subscribe = @proto.CommandSubscribe::default()
  subscribe.topic = self.topic
  subscribe.subscription = self.subscription
  subscribe.sub_type = self.sub_type.to_proto()
  if self.key_shared_policy is Some(policy) {
    subscribe.key_shared_meta = Some(policy.to_proto())
  }
  subscribe.consumer_id = self.consumer_id
  subscribe.request_id = request_id
  subscribe.priority_level = Some(self.priority_level)
  subscribe.durable = Some(self.durable)
  subscribe.read_compacted = Some(self.read_compacted)
  subscribe.metadata = key_values_of(self.properties)
  subscribe.subscription_properties = key_values_of(
    self.subscription_properties,
  )
  if self.schema is Some(schema) {
    subscribe.schema = Some(schema.to_proto())
  }
  if self.consumer_name is Some(name) {
    subscribe.consumer_name = Some(name)
  }
  match self.initial_position {
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
  cmd
}

///|
/// Wire the message handler and the close hook onto a connection.
fn SingleConsumer::attach(self : SingleConsumer, conn : Connection) -> Unit {
  conn.message_handlers[self.consumer_id] = fn(frame) { self.on_frame(frame) }
  conn.on_close(fn(e) { self.on_connection_closed(e) })
}

///|
/// Re-subscribe on a fresh connection and resume the message flow.
async fn SingleConsumer::reconnect(self : SingleConsumer) -> Unit {
  guard self.reconnect_with is Some(provider) else { return }
  let policy = RetryPolicy::default()
  let method = @async.RetryMethod::ExponentialDelay(
    initial=policy.initial_backoff_ms,
    factor=2.0,
    maximum=policy.max_backoff_ms,
  )
  let attempt = async fn() -> Unit {
    if self.closed.val {
      raise ConsumerClosed
    }
    let conn = provider()
    self.attach(conn)
    let request_id = conn.next_request_id()
    let reply = conn.request(self.subscribe_command(request_id))
    guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
      raise UnexpectedResponse("expected SUCCESS for SUBSCRIBE")
    }
    self.conn_ref.val = conn
    self.nack_epoch.val += 1
    self.ack_pending.clear()
    self.chunks.clear()
    self.chunk_acks.clear()
    // the broker forgot our permits with the old connection; re-open the
    // flow window fully (messages buffered locally may over-deliver slightly)
    self.send_flow(self.receiver_permits)
    self.available_permits = self.receiver_permits
  }
  @async.retry(method, fatal_error=fn(e) { e is ConsumerClosed }, attempt) catch {
    _ =>
      if !self.closed.val {
        self.closed.val = true
        self.messages.close(error=ConnectionClosed, clear=true)
      }
  }
  self.reconnecting.val = false
}

///|
fn SingleConsumer::on_connection_closed(
  self : SingleConsumer,
  error : Error?,
) -> Unit {
  ignore(error)
  self.nack_epoch.val += 1
  if self.closed.val || self.reconnect_with is None {
    if !self.closed.val {
      self.closed.val = true
      self.messages.close(error=ConnectionClosed, clear=true)
    }
    return
  }
  if self.reconnecting.val {
    return
  }
  self.reconnecting.val = true
  self.group.spawn_bg(
    async fn() { self.reconnect() },
    no_wait=true,
    allow_failure=true,
  )
}

///|
async fn SingleConsumer::send_flow(
  self : SingleConsumer,
  permits : Int,
) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::FLOW
  let flow = @proto.CommandFlow::default()
  flow.consumer_id = self.consumer_id
  flow.message_permits = permits.reinterpret_as_uint()
  cmd.flow = Some(flow)
  self.conn_ref.val.send_command(cmd)
}

///|
fn SingleConsumer::on_frame(
  self : SingleConsumer,
  frame : IncomingFrame,
) -> Unit {
  match frame.cmd.type_ {
    @proto.BaseCommand_Type::MESSAGE =>
      if !self.seeking.val {
        self.dispatch_message(frame) catch {
          _ => ()
        }
      }
    @proto.BaseCommand_Type::CLOSE_CONSUMER =>
      if self.seeking.val {
        ()
      } else if self.reconnect_with is Some(_) {
        self.conn_ref.val.message_handlers.remove(self.consumer_id)
        self.on_connection_closed(None)
      } else if !self.closed.val {
        self.closed.val = true
        self.messages.close(error=ConsumerClosed, clear=true)
      }
    _ => ()
  }
}

///|
fn SingleConsumer::dispatch_message(
  self : SingleConsumer,
  frame : IncomingFrame,
) -> Unit raise {
  guard frame.cmd.message is Some(message) else { return }
  guard frame.metadata is Some(metadata) && frame.payload is Some(raw_payload) else {
    raise InvalidFrame("MESSAGE without payload")
  }
  let base_id = MessageId::from_proto(message.message_id)
  let chunk = self.chunks.accept(metadata, base_id, raw_payload)
  guard chunk.payload is Some(assembled) else {
    if chunk.is_chunk {
      self.group.spawn_bg(
        async fn() { self.send_flow(1) catch { _ => () } },
        no_wait=true,
        allow_failure=true,
      )
    }
    return
  }
  let payload = match metadata.compression {
    Some(t) =>
      decompress(
        CompressionCodec::from_proto(t),
        assembled,
        uncompressed_size?=metadata.uncompressed_size.map(fn(u : UInt) {
          u.reinterpret_as_int()
        }),
      )
    None => assembled
  }
  let redelivery_count = message.redelivery_count.unwrap_or(0)
  // proto2 default for num_messages_in_batch is Some(1): only >1 is a batch
  let batch_size = metadata.num_messages_in_batch.unwrap_or(0)
  if chunk.is_chunk && batch_size > 1 {
    raise InvalidFrame("chunked and batched message")
  }
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
    let chunk_ids = if chunk.is_chunk { Some(chunk.ids) } else { None }
    if chunk_ids is Some(ids) {
      self.chunk_acks[base_id] = ids
    }
    self.push_message(
      base_id,
      -1,
      metadata,
      None,
      payload,
      redelivery_count,
      chunk_ids?,
    )
  }
}

///|
fn SingleConsumer::push_message(
  self : SingleConsumer,
  base_id : MessageId,
  batch_index : Int,
  metadata : @proto.MessageMetadata,
  single : @proto.SingleMessageMetadata?,
  payload : Bytes,
  redelivery_count : UInt,
  chunk_ids? : Array[MessageId],
) -> Unit raise {
  let scheduler : ((Message) -> Unit)? = match self.nack_backoff {
    Some(_) => Some(fn(m) { self.schedule_nack(m) })
    None => None
  }
  let message = build_message(
    self.conn_ref,
    self.consumer_id,
    self.topic,
    base_id,
    batch_index,
    metadata,
    single,
    payload,
    redelivery_count,
    ack_tracker=self.ack_pending,
    chunk_ids?,
    chunk_tracker=self.chunk_acks,
    on_delivery=fn(m) { self.track_ack(m) },
    nack_scheduler?=scheduler,
  )
  ignore(self.messages.try_put(message))
}

///|
fn build_message(
  conn_ref : Ref[Connection],
  consumer_id : UInt64,
  topic : String,
  base_id : MessageId,
  batch_index : Int,
  metadata : @proto.MessageMetadata,
  single : @proto.SingleMessageMetadata?,
  payload : Bytes,
  redelivery_count : UInt,
  ack_tracker? : Map[MessageId, Int],
  chunk_ids? : Array[MessageId],
  chunk_tracker? : Map[MessageId, Array[MessageId]],
  on_delivery? : (Message) -> Unit,
  nack_scheduler? : (Message) -> Unit,
) -> Message {
  {
    conn_ref,
    consumer_id,
    id: { ..base_id, batch_index, },
    topic,
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
    ack_tracker,
    chunk_ids,
    chunk_tracker,
    on_delivery,
    nack_scheduler,
  }
}

///|
fn SingleConsumer::track_ack(self : SingleConsumer, message : Message) -> Unit {
  if self.ack_timeout_ms == 0 {
    return
  }
  let id = message.id()
  let ids = message.chunk_ids.unwrap_or([id])
  self.next_ack_token += 1
  let token = self.next_ack_token
  self.ack_pending[id] = token
  self.group.spawn_bg(
    async fn() {
      @async.sleep(self.ack_timeout_ms)
      if !self.closed.val && self.ack_pending.get(id) == Some(token) {
        self.ack_pending.remove(id)
        send_redeliver(self.conn_ref.val, self.consumer_id, ids) catch {
          _ => ()
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
fn SingleConsumer::schedule_nack(
  self : SingleConsumer,
  message : Message,
) -> Unit {
  guard self.nack_backoff is Some(policy) else { return }
  let delay = policy.delay_ms(message.redelivery_count())
  let ids = message.chunk_ids.unwrap_or([message.id()])
  let epoch = self.nack_epoch.val
  self.group.spawn_bg(
    async fn() {
      @async.sleep(delay)
      if !self.closed.val && self.nack_epoch.val == epoch {
        send_redeliver(self.conn_ref.val, self.consumer_id, ids) catch {
          _ => ()
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
fn SingleConsumer::clear_cumulative_ack(
  self : SingleConsumer,
  id : MessageId,
) -> Unit {
  let acknowledged = []
  for pending_id, _ in self.ack_pending {
    if message_id_at_or_before(pending_id, id) {
      acknowledged.push(pending_id)
    }
  }
  for pending_id in acknowledged {
    self.ack_pending.remove(pending_id)
  }
  let chunks = []
  for chunk_id, _ in self.chunk_acks {
    if message_id_at_or_before(chunk_id, id) {
      chunks.push(chunk_id)
    }
  }
  for chunk_id in chunks {
    self.chunk_acks.remove(chunk_id)
  }
}

///|
fn message_id_at_or_before(a : MessageId, b : MessageId) -> Bool {
  a.ledger_id < b.ledger_id ||
  (
    a.ledger_id == b.ledger_id &&
    (
      a.entry_id < b.entry_id ||
      (a.entry_id == b.entry_id && a.batch_index <= b.batch_index)
    )
  )
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
async fn SingleConsumer::receive_untracked(self : SingleConsumer) -> Message {
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
pub async fn SingleConsumer::receive(self : SingleConsumer) -> Message {
  let message = self.receive_untracked()
  message.mark_delivered()
  message
}

///|
/// Non-blocking receive: returns `None` if no message is buffered.
fn SingleConsumer::try_receive_untracked(
  self : SingleConsumer,
) -> Message? raise {
  let message = self.messages.try_get() catch { _ => raise ConsumerClosed }
  if message is Some(_) {
    self.available_permits -= 1
    if self.available_permits <= self.receiver_permits / 2 {
      let refill = self.receiver_permits - self.available_permits
      self.available_permits += refill
      self.group.spawn_bg(
        async fn() { self.send_flow(refill) catch { _ => () } },
        no_wait=true,
        allow_failure=true,
      )
    }
  }
  message
}

///|
pub fn SingleConsumer::try_receive(self : SingleConsumer) -> Message? raise {
  let message = self.try_receive_untracked()
  if message is Some(msg) {
    msg.mark_delivered()
  }
  message
}

///|
/// Acknowledge a message individually.
pub async fn SingleConsumer::ack(self : SingleConsumer, id : MessageId) -> Unit {
  self.ack_pending.remove(id)
  let ids = self.chunk_acks.get(id).unwrap_or([id])
  self.chunk_acks.remove(id)
  send_ack(
    self.conn_ref.val,
    self.consumer_id,
    @proto.CommandAck_AckType::Individual,
    ids,
  )
}

///|
/// Acknowledge cumulatively: this message and all before it.
pub async fn SingleConsumer::ack_cumulative(
  self : SingleConsumer,
  id : MessageId,
) -> Unit {
  self.clear_cumulative_ack(id)
  send_ack(
    self.conn_ref.val,
    self.consumer_id,
    @proto.CommandAck_AckType::Cumulative,
    [id],
  )
}

///|
/// Acknowledge a message as part of a transaction.
pub async fn SingleConsumer::ack_txn(
  self : SingleConsumer,
  txn : Transaction,
  id : MessageId,
) -> Unit {
  self.ack_pending.remove(id)
  let ids = self.chunk_acks.get(id).unwrap_or([id])
  self.chunk_acks.remove(id)
  send_ack(
    self.conn_ref.val,
    self.consumer_id,
    @proto.CommandAck_AckType::Individual,
    ids,
    txn~,
  )
}

///|
/// Negatively acknowledge a message, triggering redelivery.
pub async fn SingleConsumer::nack(
  self : SingleConsumer,
  id : MessageId,
) -> Unit {
  self.ack_pending.remove(id)
  let ids = self.chunk_acks.get(id).unwrap_or([id])
  self.chunk_acks.remove(id)
  send_redeliver(self.conn_ref.val, self.consumer_id, ids)
}

///|
/// Reset this subscription to a message id or publish timestamp.
/// Already buffered messages are discarded after the broker accepts the seek.
pub async fn SingleConsumer::seek(
  self : SingleConsumer,
  position : SeekPosition,
) -> Unit {
  self.seek_mu.acquire()
  defer self.seek_mu.release()
  if self.closed.val {
    raise ConsumerClosed
  }
  if self.reconnecting.val {
    raise ConnectionClosed
  }
  let conn = self.conn_ref.val
  self.seeking.val = true
  defer {
    self.seeking.val = false
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SEEK
  let seek = @proto.CommandSeek::default()
  seek.consumer_id = self.consumer_id
  seek.request_id = conn.next_request_id()
  match position {
    PositionId(id) => seek.message_id = Some(id.to_proto())
    PositionTimestamp(time) => seek.message_publish_time = Some(time)
  }
  cmd.seek = Some(seek)
  let reply = conn.request(cmd)
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for SEEK")
  }
  while (self.messages.try_get() catch { _ => None }) is Some(_) {

  }
  self.ack_pending.clear()
  self.chunks.clear()
  self.chunk_acks.clear()
  self.nack_epoch.val += 1
  conn.message_handlers.remove(self.consumer_id)
  let close_cmd = @proto.BaseCommand::default()
  close_cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
  let close = @proto.CommandCloseConsumer::default()
  close.consumer_id = self.consumer_id
  close.request_id = conn.next_request_id()
  close_cmd.close_consumer = Some(close)
  ignore(conn.request(close_cmd) catch { _ => @proto.BaseCommand::default() })
  self.consumer_id = conn.next_request_id()
  conn.message_handlers[self.consumer_id] = fn(frame) { self.on_frame(frame) }
  let subscribed = conn.request(self.subscribe_command(conn.next_request_id())) catch {
    e => {
      self.on_connection_closed(Some(e))
      raise e
    }
  }
  guard subscribed.type_ is @proto.BaseCommand_Type::SUCCESS else {
    self.on_connection_closed(None)
    raise UnexpectedResponse("expected SUCCESS for re-SUBSCRIBE after SEEK")
  }
  self.seeking.val = false
  self.send_flow(self.receiver_permits)
  self.available_permits = self.receiver_permits
}

///|
/// Close the consumer and its subscription cursor is kept (durable).
pub async fn SingleConsumer::close(self : SingleConsumer) -> Unit {
  if self.closed.val {
    return
  }
  self.closed.val = true
  self.nack_epoch.val += 1
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
  let close = @proto.CommandCloseConsumer::default()
  close.consumer_id = self.consumer_id
  close.request_id = self.conn_ref.val.next_request_id()
  cmd.close_consumer = Some(close)
  ignore(
    self.conn_ref.val.request(cmd) catch {
      _ => @proto.BaseCommand::default()
    },
  )
  self.conn_ref.val.message_handlers.remove(self.consumer_id)
  self.ack_pending.clear()
  self.chunks.clear()
  self.chunk_acks.clear()
  self.messages.close(error=ConsumerClosed, clear=true)
}

///|
/// Remove this subscription from the broker and close the consumer.
pub async fn SingleConsumer::unsubscribe(
  self : SingleConsumer,
  force? : Bool = false,
) -> Unit {
  if self.closed.val {
    raise ConsumerClosed
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::UNSUBSCRIBE
  let req = @proto.CommandUnsubscribe::default()
  req.consumer_id = self.consumer_id
  req.request_id = self.conn_ref.val.next_request_id()
  req.force = Some(force)
  cmd.unsubscribe = Some(req)
  let reply = self.conn_ref.val.request(cmd)
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for UNSUBSCRIBE")
  }
  self.closed.val = true
  self.nack_epoch.val += 1
  self.conn_ref.val.message_handlers.remove(self.consumer_id)
  self.ack_pending.clear()
  self.chunks.clear()
  self.chunk_acks.clear()
  self.messages.close(error=ConsumerClosed, clear=true)
}

///|
pub fn SingleConsumer::topic(self : SingleConsumer) -> String {
  self.topic
}

///|
pub fn SingleConsumer::subscription(self : SingleConsumer) -> String {
  self.subscription
}

///|
pub fn SingleConsumer::is_closed(self : SingleConsumer) -> Bool {
  self.closed.val
}

///|
priv enum ConsumerImpl {
  Single(SingleConsumer)
  Partitioned(PartitionedConsumerState)
}

///|
priv struct PartitionedConsumerState {
  consumers : Array[Consumer]
  source_ids : Array[Int]
  mut next_source_id : Int
  active_sources : Map[Int, Bool]
  queue : @aqueue.Queue[ForwardedMessage]
  closed : Ref[Bool]
  topic_label : String
  subscription_label : String
}

///|
priv struct ForwardedMessage {
  source_id : Int
  message : Message
}

///|
/// A consumer for a topic, partitioned or not. Partitioned consumers merge
/// the message streams of every partition.
pub struct Consumer {
  priv inner : ConsumerImpl
}

///|
fn Consumer::single(c : SingleConsumer) -> Consumer {
  { inner: ConsumerImpl::Single(c), }
}

///|
/// Create a consumer aggregating all partitions of a partitioned topic.
pub fn Consumer::partitioned(
  group : @async.TaskGroup[Unit],
  consumers : Array[Consumer],
) -> Consumer {
  let first_topic = if consumers.is_empty() { "" } else { consumers[0].topic() }
  let topic_label = match first_topic.rev_find("-partition-") {
    Some(i) => first_topic[:i].to_owned()
    None => first_topic
  }
  let subscription_label = if consumers.is_empty() {
    ""
  } else {
    consumers[0].subscription()
  }
  let state : PartitionedConsumerState = {
    consumers: [],
    source_ids: [],
    next_source_id: 0,
    active_sources: {},
    queue: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
    closed: { val: false, },
    topic_label,
    subscription_label,
  }
  for sub in consumers {
    state.add_source(group, sub)
  }
  { inner: ConsumerImpl::Partitioned(state), }
}

///|
fn Consumer::pattern(pattern : String, subscription : String) -> Consumer {
  {
    inner: ConsumerImpl::Partitioned({
      consumers: [],
      source_ids: [],
      next_source_id: 0,
      active_sources: {},
      queue: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
      closed: { val: false, },
      topic_label: pattern,
      subscription_label: subscription,
    }),
  }
}

///|
fn PartitionedConsumerState::add_source(
  self : PartitionedConsumerState,
  group : @async.TaskGroup[Unit],
  sub : Consumer,
) -> Unit {
  let source_id = self.next_source_id
  self.next_source_id += 1
  self.source_ids.push(source_id)
  self.active_sources[source_id] = true
  self.consumers.push(sub)
  group.spawn_bg(
    async fn() {
      try {
        while true {
          let msg = sub.receive_untracked()
          self.queue.put({ source_id, message: msg, })
        }
      } catch {
        _ => ()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
async fn Consumer::remove_source(self : Consumer, topic : String) -> Unit {
  match self.inner {
    ConsumerImpl::Single(_) => raise InvalidFrame("not a composite consumer")
    ConsumerImpl::Partitioned(state) => {
      let mut index = 0
      while index < state.consumers.length() {
        if base_topic_name(state.consumers[index].topic()) == topic {
          let removed = state.consumers.remove(index)
          let source_id = state.source_ids.remove(index)
          state.active_sources.remove(source_id)
          removed.close()
        } else {
          index += 1
        }
      }
    }
  }
}

///|
async fn Consumer::add_source(
  self : Consumer,
  group : @async.TaskGroup[Unit],
  sub : Consumer,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(_) => raise InvalidFrame("not a composite consumer")
    ConsumerImpl::Partitioned(state) =>
      if state.closed.val {
        sub.close()
      } else {
        state.add_source(group, sub)
      }
  }
}

///|
/// Receive a message without starting its application ack deadline.
async fn Consumer::receive_untracked(self : Consumer) -> Message {
  match self.inner {
    ConsumerImpl::Single(c) => c.receive_untracked()
    ConsumerImpl::Partitioned(state) => {
      while true {
        let item = state.queue.get()
        if state.active_sources.contains(item.source_id) {
          return item.message
        }
      }
      raise ConsumerClosed
    }
  }
}

///|
/// Receive the next message, blocking until one is available.
pub async fn Consumer::receive(self : Consumer) -> Message {
  let message = self.receive_untracked()
  message.mark_delivered()
  message
}

///|
/// Non-blocking receive: returns `None` if no message is buffered.
pub fn Consumer::try_receive(self : Consumer) -> Message? raise {
  let message = match self.inner {
    ConsumerImpl::Single(c) => c.try_receive_untracked()
    ConsumerImpl::Partitioned(state) => {
      let mut result : Message? = None
      let mut done = false
      while !done {
        let next = state.queue.try_get() catch { _ => raise ConsumerClosed }
        match next {
          Some(item) =>
            if state.active_sources.contains(item.source_id) {
              result = Some(item.message)
              done = true
            }
          None => done = true
        }
      }
      result
    }
  }
  if message is Some(msg) {
    msg.mark_delivered()
  }
  message
}

///|
/// Acknowledge a message individually.
pub async fn Consumer::ack(self : Consumer, id : MessageId) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.ack(id)
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "ack by id needs the message's own consumer; use message.ack()",
      )
  }
}

///|
/// Acknowledge cumulatively: this message and all before it.
pub async fn Consumer::ack_cumulative(self : Consumer, id : MessageId) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.ack_cumulative(id)
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "cumulative ack needs the message's own consumer; use message.ack()",
      )
  }
}

///|
/// Acknowledge a message as part of a transaction.
pub async fn Consumer::ack_txn(
  self : Consumer,
  txn : Transaction,
  id : MessageId,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.ack_txn(txn, id)
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "txn ack needs the message's own consumer; use message.ack_txn()",
      )
  }
}

///|
/// Negatively acknowledge a message, triggering redelivery.
pub async fn Consumer::nack(self : Consumer, id : MessageId) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.nack(id)
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "nack by id needs the message's own consumer; use message.nack()",
      )
  }
}

///|
/// Reset a subscription cursor. Timestamp, earliest, and latest positions
/// apply to every source. A regular message id must select exactly one source.
pub async fn Consumer::seek(self : Consumer, position : SeekPosition) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.seek(position)
    ConsumerImpl::Partitioned(state) => {
      guard !state.closed.val else { raise ConsumerClosed }
      match position {
        PositionTimestamp(_) =>
          for sub in state.consumers {
            sub.seek(position)
          }
        PositionId(id) =>
          if id == MessageId::earliest() || id == MessageId::latest() {
            for sub in state.consumers {
              sub.seek(position)
            }
          } else {
            let candidates = []
            for sub in state.consumers {
              if state.consumers.length() == 1 ||
                (
                  id.partition >= 0 &&
                  sub.topic().ends_with("-partition-\{id.partition}")
                ) {
                candidates.push(sub)
              }
            }
            guard candidates.length() == 1 else {
              raise InvalidFrame("message id must identify one source topic")
            }
            candidates[0].seek(position)
          }
      }
      while (state.queue.try_get() catch { _ => None }) is Some(_) {

      }
    }
  }
}

///|
/// Close the consumer (all partitions).
pub async fn Consumer::close(self : Consumer) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.close()
    ConsumerImpl::Partitioned(state) => {
      state.closed.val = true
      for c in state.consumers {
        c.close()
      }
      state.queue.close(error=ConsumerClosed, clear=true)
    }
  }
}

///|
/// Remove the subscription from every source topic and close the consumer.
pub async fn Consumer::unsubscribe(
  self : Consumer,
  force? : Bool = false,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.unsubscribe(force~)
    ConsumerImpl::Partitioned(state) => {
      let mut failure : Error? = None
      for c in state.consumers {
        c.unsubscribe(force~) catch {
          e => if failure is None { failure = Some(e) }
        }
      }
      state.closed.val = true
      state.queue.close(error=ConsumerClosed, clear=true)
      if failure is Some(error) {
        raise error
      }
    }
  }
}

///|
pub fn Consumer::topic(self : Consumer) -> String {
  match self.inner {
    ConsumerImpl::Single(c) => c.topic()
    ConsumerImpl::Partitioned(state) => state.topic_label
  }
}

///|
/// Current source topics of this consumer. Pattern subscriptions gain topics
/// as discovery refreshes succeed.
pub fn Consumer::topics(self : Consumer) -> Array[String] {
  match self.inner {
    ConsumerImpl::Single(c) => [c.topic()]
    ConsumerImpl::Partitioned(state) => {
      let topics = []
      let seen : Map[String, Bool] = {}
      for sub in state.consumers {
        let topic = base_topic_name(sub.topic())
        if !seen.contains(topic) {
          seen[topic] = true
          topics.push(topic)
        }
      }
      topics
    }
  }
}

///|
pub fn Consumer::subscription(self : Consumer) -> String {
  match self.inner {
    ConsumerImpl::Single(c) => c.subscription()
    ConsumerImpl::Partitioned(state) => state.subscription_label
  }
}

///|
pub fn Consumer::is_closed(self : Consumer) -> Bool {
  match self.inner {
    ConsumerImpl::Single(c) => c.is_closed()
    ConsumerImpl::Partitioned(state) => state.closed.val
  }
}
