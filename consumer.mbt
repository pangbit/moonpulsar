// SingleConsumer: subscribes to a topic and receives messages with flow control.

///|
const COMPOSITE_CONSUMER_QUEUE_CAPACITY : Int = 1000

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
/// Route a message to a dead-letter topic after `max_deliveries`
/// application deliveries. For example, one delivery permits the initial
/// attempt and routes the next redelivery.
pub struct DeadLetterPolicy {
  priv max_deliveries : UInt
  priv topic : String
}

///|
pub fn DeadLetterPolicy::new(
  max_deliveries : UInt,
  topic : String,
) -> DeadLetterPolicy raise PulsarError {
  guard max_deliveries > 0U && topic != "" else {
    raise InvalidFrame(
      "dead-letter policy requires a positive delivery limit and topic",
    )
  }
  { max_deliveries, topic, }
}

///|
pub fn DeadLetterPolicy::topic(self : DeadLetterPolicy) -> String {
  self.topic
}

///|
pub fn DeadLetterPolicy::max_deliveries(self : DeadLetterPolicy) -> UInt {
  self.max_deliveries
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
  priv message_crypto : MessageCrypto?
  priv decryption_failure_action : DecryptionFailureAction
  priv consumer_name : String?
  priv initial_position : SubscriptionInitialPosition
  priv priority_level : Int
  priv durable : Bool
  priv read_compacted : Bool
  priv properties : Map[String, String]
  priv subscription_properties : Map[String, String]
  priv messages : @aqueue.Queue[Message]
  priv mut buffered_messages : Int
  priv receiver_permits : Int
  priv auto_scaled_receiver_queue : Bool
  priv mut current_receiver_permits : Int
  priv mut scale_receiver_hint : Bool
  priv mut available_permits : Int
  priv mut demand_active : Bool
  priv mut waiting_receives : Int
  priv ack_timeout_ms : Int
  priv nack_backoff : NackBackoff?
  priv nack_epoch : Ref[Int]
  priv ack_pending : Map[MessageId, Int]
  priv batch_acks : BatchAckTracker
  priv ack_dispatcher : AckDispatcher
  priv mut next_ack_token : Int
  priv chunks : ChunkAssembler
  priv chunk_sweeper_stop : @aqueue.Queue[Unit]
  priv chunk_acks : Map[MessageId, Array[MessageId]]
  priv reconnect_with : (async () -> Connection)?
  priv mut reconnect_on_broker_close : Bool
  priv reconnect_policy : RetryPolicy
  priv reconnecting : Ref[Bool]
  priv on_event : ((ClientEvent) -> Unit)?
  priv memory_limiter : ClientMemoryLimiter?
  priv mut span_recorder : ClientSpanRecorder?
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
  message_crypto? : MessageCrypto,
  decryption_failure_action? : DecryptionFailureAction = Fail,
  reconnect_policy? : RetryPolicy = RetryPolicy::default(),
  chunk_policy? : ChunkAssemblyPolicy,
  enable_batch_index_ack? : Bool = false,
  ack_grouping? : AckGroupingOptions,
  auto_scaled_receiver_queue? : Bool = false,
  on_event? : (ClientEvent) -> Unit,
) -> SingleConsumer {
  SingleConsumer::create_with_memory(
    group,
    conn,
    topic,
    subscription,
    sub_type~,
    key_shared_policy?,
    consumer_name?,
    initial_position~,
    priority_level~,
    durable~,
    read_compacted~,
    properties~,
    subscription_properties~,
    receiver_queue_size~,
    ack_timeout_ms~,
    nack_backoff?,
    reconnect_with?,
    schema?,
    message_crypto?,
    decryption_failure_action~,
    reconnect_policy~,
    chunk_policy?,
    enable_batch_index_ack~,
    ack_grouping?,
    auto_scaled_receiver_queue~,
    on_event?,
  )
}

///|
async fn SingleConsumer::create_with_memory(
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
  message_crypto? : MessageCrypto,
  decryption_failure_action? : DecryptionFailureAction = Fail,
  reconnect_policy? : RetryPolicy = RetryPolicy::default(),
  chunk_policy? : ChunkAssemblyPolicy,
  enable_batch_index_ack? : Bool = false,
  ack_grouping? : AckGroupingOptions,
  auto_scaled_receiver_queue? : Bool = false,
  on_event? : (ClientEvent) -> Unit,
  memory_limiter? : ClientMemoryLimiter,
) -> SingleConsumer {
  guard ack_timeout_ms >= 0 else {
    raise InvalidFrame("ack timeout must not be negative")
  }
  guard receiver_queue_size >= 0 else {
    raise InvalidFrame("receiver queue size must not be negative")
  }
  if auto_scaled_receiver_queue && receiver_queue_size == 0 {
    raise InvalidFrame("auto-scaled receiver queue requires a positive maximum")
  }
  if key_shared_policy is Some(_) && sub_type != KeyShared {
    raise InvalidFrame("KeyShared policy requires KeyShared subscription")
  }
  let request_id = conn.next_request_id()
  let consumer_id = conn.next_request_id()
  let conn_ref : Ref[Connection] = { val: conn, }
  let consumer : SingleConsumer = {
    conn_ref,
    topic,
    subscription,
    consumer_id,
    sub_type,
    key_shared_policy,
    schema,
    message_crypto,
    decryption_failure_action,
    consumer_name,
    initial_position,
    priority_level,
    durable,
    read_compacted,
    properties,
    subscription_properties,
    messages: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
    buffered_messages: 0,
    receiver_permits: receiver_queue_size,
    auto_scaled_receiver_queue,
    current_receiver_permits: if auto_scaled_receiver_queue {
      1
    } else {
      receiver_queue_size
    },
    scale_receiver_hint: false,
    available_permits: 0,
    demand_active: false,
    waiting_receives: 0,
    ack_timeout_ms,
    nack_backoff,
    nack_epoch: { val: 0, },
    ack_pending: {},
    batch_acks: BatchAckTracker::new(enable_batch_index_ack),
    ack_dispatcher: AckDispatcher::new(
      conn_ref,
      consumer_id,
      options?=ack_grouping,
    ),
    next_ack_token: 0,
    chunks: ChunkAssembler::new(policy?=chunk_policy, memory_limiter?),
    chunk_sweeper_stop: @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1)),
    chunk_acks: {},
    reconnect_with,
    reconnect_on_broker_close: true,
    reconnect_policy,
    reconnecting: { val: false, },
    on_event,
    memory_limiter,
    span_recorder: None,
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
  if receiver_queue_size > 0 {
    consumer.send_flow(consumer.current_receiver_permits)
    consumer.available_permits = consumer.current_receiver_permits
  }
  consumer.ack_dispatcher.start(group)
  group.spawn_bg(
    async fn() {
      while !consumer.closed.val {
        let stopped = @async.with_timeout_opt(
          consumer.chunks.policy.expiry_ms,
          async fn() { consumer.chunk_sweeper_stop.get() },
        )
        if stopped is Some(_) || consumer.closed.val {
          return
        }
        consumer.chunks.evict_expired()
        consumer.ack_evicted_chunks()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  consumer
}

///|
fn SingleConsumer::stop_chunk_sweeper(self : SingleConsumer) -> Unit {
  ignore(self.chunk_sweeper_stop.try_put(()) catch { _ => false })
}

///|
fn SingleConsumer::ack_evicted_chunks(self : SingleConsumer) -> Unit {
  let ids = self.chunks.take_evicted()
  if ids.is_empty() {
    return
  }
  let dispatcher = self.ack_dispatcher
  self.group.spawn_bg(
    async fn() {
      dispatcher.ack(@proto.CommandAck_AckType::Individual, ids) catch {
        _ => ()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
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
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Reconnect, self.topic)
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let policy = self.reconnect_policy
  let retry_method = @async.RetryMethod::ExponentialDelay(
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
    self.ack_dispatcher.reset()
    self.conn_ref.val = conn
    self.nack_epoch.val += 1
    self.ack_pending.clear()
    self.batch_acks.clear()
    self.chunks.clear()
    self.chunk_acks.clear()
    // the broker forgot our permits with the old connection; re-open the
    // flow window fully (messages buffered locally may over-deliver slightly)
    self.reset_flow()
  }
  let run = async fn() -> Unit {
    if policy.max_retries < 0 {
      @async.retry(
        retry_method,
        fatal_error=fn(e) { e is ConsumerClosed },
        attempt,
      )
    } else {
      @async.retry(
        retry_method,
        max_retry=policy.max_retries,
        fatal_error=fn(e) { e is ConsumerClosed },
        attempt,
      )
    }
  }
  let result : Result[Unit, Error] = Ok(run()) catch { e => Err(e) }
  if self.span_recorder is Some(recorder) && span is Some(start) {
    recorder.finish_result(start, result)
    span_done.val = true
  }
  if self.on_event is Some(sink) {
    sink(ClientEvent::ConsumerReconnected(self.topic, result))
  }
  if result is Err(_) {
    if !self.closed.val {
      self.closed.val = true
      self.stop_chunk_sweeper()
      self.ack_dispatcher.stop()
      self.clear_buffered_messages()
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
  self.nack_epoch.val += 1
  if error is Some(ClientMemoryFull) && !self.closed.val {
    self.closed.val = true
    self.stop_chunk_sweeper()
    self.ack_dispatcher.stop()
    self.ack_dispatcher.pending = []
    self.ack_pending.clear()
    self.batch_acks.clear()
    self.chunk_acks.clear()
    self.clear_buffered_messages()
    self.chunks.clear()
    self.conn_ref.val.message_handlers.remove(self.consumer_id)
    self.messages.close(error=ClientMemoryFull, clear=true)
    return
  }
  if self.closed.val || self.reconnect_with is None {
    if !self.closed.val {
      self.closed.val = true
      self.stop_chunk_sweeper()
      self.ack_dispatcher.stop()
      self.clear_buffered_messages()
      self.messages.close(error=ConnectionClosed, clear=true)
    }
    return
  }
  if self.reconnecting.val {
    return
  }
  self.reconnecting.val = true
  if self.on_event is Some(sink) {
    sink(ClientEvent::ConsumerReconnecting(self.topic))
  }
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
async fn SingleConsumer::reset_flow(self : SingleConsumer) -> Unit {
  self.demand_active = false
  self.available_permits = 0
  if self.receiver_permits == 0 {
    if self.waiting_receives > 0 && self.buffered_messages == 0 {
      self.request_one()
    }
  } else {
    self.send_flow(self.current_receiver_permits)
    self.available_permits = self.current_receiver_permits
  }
}

///|
async fn SingleConsumer::request_one(self : SingleConsumer) -> Unit {
  if self.receiver_permits == 0 &&
    !self.demand_active &&
    self.buffered_messages == 0 {
    self.demand_active = true
    errdefer {
      self.demand_active = false
    }
    self.send_flow(1)
  }
}

///|
fn SingleConsumer::on_frame(
  self : SingleConsumer,
  frame : IncomingFrame,
) -> Unit {
  match frame.cmd.type_ {
    @proto.BaseCommand_Type::MESSAGE =>
      if !self.closed.val && !self.seeking.val {
        self.dispatch_message(frame) catch {
          error => self.fail_delivery(error)
        }
      }
    @proto.BaseCommand_Type::CLOSE_CONSUMER =>
      if self.seeking.val {
        ()
      } else if self.reconnect_with is Some(_) && self.reconnect_on_broker_close {
        self.conn_ref.val.message_handlers.remove(self.consumer_id)
        self.on_connection_closed(None)
      } else if !self.closed.val {
        self.closed.val = true
        self.stop_chunk_sweeper()
        self.ack_dispatcher.stop()
        self.ack_dispatcher.pending = []
        self.ack_pending.clear()
        self.batch_acks.clear()
        self.chunk_acks.clear()
        self.chunks.clear()
        self.clear_buffered_messages()
        self.conn_ref.val.message_handlers.remove(self.consumer_id)
        self.messages.close(error=ConsumerClosed, clear=true)
      }
    _ => ()
  }
}

///|
fn SingleConsumer::fail_delivery(self : SingleConsumer, error : Error) -> Unit {
  self.chunks.clear()
  self.clear_buffered_messages()
  self.messages.close(error~, clear=true)
  self.conn_ref.val.message_handlers.remove(self.consumer_id)
  self.stop_chunk_sweeper()
  self.group.spawn_bg(
    async fn() {
      let cmd = @proto.BaseCommand::default()
      cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
      let close = @proto.CommandCloseConsumer::default()
      close.consumer_id = self.consumer_id
      close.request_id = self.conn_ref.val.next_request_id()
      cmd.close_consumer = Some(close)
      self.conn_ref.val.send_command(cmd) catch {
        _ => ()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
fn SingleConsumer::discard_encrypted(
  self : SingleConsumer,
  ids : Array[MessageId],
) -> Unit {
  let conn = self.conn_ref.val
  let consumer_id = self.consumer_id
  self.group.spawn_bg(
    async fn() {
      send_ack(conn, consumer_id, @proto.CommandAck_AckType::Individual, ids) catch {
        e => {
          self.clear_buffered_messages()
          self.messages.close(error=e, clear=true)
          return
        }
      }
      if self.receiver_permits == 0 {
        if self.waiting_receives > 0 {
          self.request_one() catch {
            _ => ()
          }
        }
      } else {
        self.send_flow(1) catch {
          _ => ()
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
fn SingleConsumer::dispatch_message(
  self : SingleConsumer,
  frame : IncomingFrame,
) -> Unit raise {
  guard frame.cmd.message is Some(message) else { return }
  if self.receiver_permits == 0 {
    self.demand_active = false
  }
  guard frame.metadata is Some(metadata) && frame.payload is Some(raw_payload) else {
    raise InvalidFrame("MESSAGE without payload")
  }
  let base_id = MessageId::from_proto(message.message_id)
  let chunk = self.chunks.accept(metadata, base_id, raw_payload)
  self.ack_evicted_chunks()
  guard chunk.payload is Some(assembled) else {
    if chunk.is_chunk {
      if self.receiver_permits == 0 {
        self.demand_active = true
      }
      self.group.spawn_bg(
        async fn() { self.send_flow(1) catch { _ => () } },
        no_wait=true,
        allow_failure=true,
      )
    }
    return
  }
  let redelivery_count = message.redelivery_count.unwrap_or(0)
  let batch_size = metadata.num_messages_in_batch.unwrap_or(0)
  let mut decryption_failed = false
  let assembled = if metadata.encryption_keys.is_empty() {
    assembled
  } else {
    let decrypted : Result[Bytes, Error] = Ok(
      match self.message_crypto {
        Some(crypto) => crypto.decrypt(metadata, assembled)
        None => raise InvalidFrame("encrypted message requires decryption keys")
      },
    ) catch {
      e => Err(e)
    }
    match decrypted {
      Ok(plaintext) => plaintext
      Err(e) =>
        match self.decryption_failure_action {
          Fail => raise e
          Consume => {
            guard batch_size == 0 else {
              raise InvalidFrame(
                "cannot consume encrypted batch without decryption",
              )
            }
            decryption_failed = true
            assembled
          }
          Discard => {
            self.discard_encrypted(
              if chunk.is_chunk {
                chunk.ids
              } else {
                [base_id]
              },
            )
            return
          }
        }
    }
  }
  let payload = if decryption_failed {
    assembled
  } else {
    match metadata.compression {
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
  }
  if chunk.is_chunk && batch_size > 0 {
    raise InvalidFrame("chunked and batched message")
  }
  if batch_size > 0 {
    self.batch_acks.register(base_id, batch_size, message.ack_set)
    let singles = split_batch_payload(payload, batch_size)
    for i, single in singles {
      if !message.ack_set.is_empty() &&
        (
          i / 64 >= message.ack_set.length() ||
          ((message.ack_set[i / 64].reinterpret_as_uint64() >> (i % 64)) & 1UL) ==
          0UL
        ) {
        continue
      }
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
      decryption_failed~,
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
  decryption_failed? : Bool = false,
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
    subscription=self.subscription,
    ack_tracker=self.ack_pending,
    batch_ack_tracker=self.batch_acks,
    ack_dispatcher=self.ack_dispatcher,
    chunk_ids?,
    decryption_failed~,
    chunk_tracker=self.chunk_acks,
    on_delivery=fn(m) { self.track_ack(m) },
    nack_scheduler?=scheduler,
  )
  message.retain_for_queue(self.memory_limiter)
  if (self.messages.try_put(message) catch { _ => false }) {
    self.buffered_messages += 1
    if self.auto_scaled_receiver_queue &&
      self.buffered_messages >= self.current_receiver_permits {
      self.scale_receiver_hint = true
    }
  } else {
    message.release_retained_memory()
  }
}

///|
fn SingleConsumer::clear_buffered_messages(self : SingleConsumer) -> Unit {
  while true {
    match (self.messages.try_get() catch { _ => None }) {
      Some(message) => message.release_retained_memory()
      None => break
    }
  }
  self.buffered_messages = 0
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
  subscription? : String,
  ack_tracker? : Map[MessageId, Int],
  batch_ack_tracker? : BatchAckTracker,
  ack_dispatcher? : AckDispatcher,
  chunk_ids? : Array[MessageId],
  decryption_failed? : Bool = false,
  chunk_tracker? : Map[MessageId, Array[MessageId]],
  on_delivery? : (Message) -> Unit,
  nack_scheduler? : (Message) -> Unit,
) -> Message {
  {
    conn_ref,
    consumer_id,
    id: { ..base_id, batch_index, },
    topic,
    subscription,
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
    ordering_key: match single {
      Some(s) =>
        if s.ordering_key is Some(_) {
          s.ordering_key
        } else {
          metadata.ordering_key
        }
      None => metadata.ordering_key
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
    schema_version: metadata.schema_version,
    decryption_failed,
    redelivery_count,
    ack_tracker,
    batch_ack_tracker,
    ack_dispatcher,
    chunk_ids,
    chunk_tracker,
    on_delivery,
    on_ack: None,
    on_nack: None,
    interceptors: [],
    span_recorder: None,
    nack_scheduler,
    memory_lease: None,
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
    let payload_size = meta.payload_size
    if payload_size < 0 || offset + payload_size > payload.length() {
      raise InvalidFrame("single message payload exceeds payload")
    }
    let body = payload[offset:offset + payload_size].to_owned()
    offset += payload_size
    result.push((meta, body))
  }
  result
}

///|
fn SingleConsumer::scale_receiver_queue_on_empty(self : SingleConsumer) -> Unit {
  if self.auto_scaled_receiver_queue &&
    self.scale_receiver_hint &&
    self.buffered_messages == 0 &&
    self.current_receiver_permits < self.receiver_permits {
    self.current_receiver_permits = if self.current_receiver_permits >
      self.receiver_permits / 2 {
      self.receiver_permits
    } else {
      self.current_receiver_permits * 2
    }
    self.scale_receiver_hint = false
  }
}

///|
/// Receive the next message, blocking until one is available.
async fn SingleConsumer::receive_untracked(self : SingleConsumer) -> Message {
  if self.closed.val {
    raise ConsumerClosed
  }
  if self.receiver_permits == 0 {
    self.waiting_receives += 1
    defer {
      self.waiting_receives -= 1
    }
    self.request_one()
    let message = self.messages.get()
    self.buffered_messages -= 1
    if self.waiting_receives > 1 && self.buffered_messages == 0 {
      self.request_one() catch {
        _ => ()
      }
    }
    return message
  }
  self.scale_receiver_queue_on_empty()
  let message = self.messages.get()
  self.buffered_messages -= 1
  self.available_permits -= 1
  let threshold = self.current_receiver_permits / 2
  if self.available_permits <= threshold {
    let refill = self.current_receiver_permits - self.available_permits
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
    self.buffered_messages -= 1
  }
  if self.receiver_permits == 0 {
    if message is None && !self.demand_active {
      self.demand_active = true
      self.group.spawn_bg(
        async fn() {
          self.send_flow(1) catch {
            _ => self.demand_active = false
          }
        },
        no_wait=true,
        allow_failure=true,
      )
    }
    return message
  }
  if message is None {
    self.scale_receiver_queue_on_empty()
  }
  if message is Some(_) {
    self.available_permits -= 1
    if self.available_permits <= self.current_receiver_permits / 2 {
      let refill = self.current_receiver_permits - self.available_permits
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
pub async fn SingleConsumer::ack(
  self : SingleConsumer,
  id : MessageId,
  await_response? : Bool = false,
) -> Unit {
  if id.batch_index >= 0 {
    self.batch_acks.ack(
      self.conn_ref.val,
      self.consumer_id,
      id,
      @proto.CommandAck_AckType::Individual,
      await_response~,
      dispatcher=self.ack_dispatcher,
    )
  } else {
    let ids = self.chunk_acks.get(id).unwrap_or([id])
    self.ack_dispatcher.ack(
      @proto.CommandAck_AckType::Individual,
      ids,
      await_response~,
    )
  }
  self.ack_pending.remove(id)
  self.chunk_acks.remove(id)
}

///|
/// Acknowledge several IDs. Ordinary IDs share one ACK frame; batch indexes
/// retain their per-entry bitmap handling. An empty list has no effect.
pub async fn SingleConsumer::ack_ids(
  self : SingleConsumer,
  ids : Array[MessageId],
  await_response? : Bool = false,
) -> Unit {
  let ordinary : Array[MessageId] = []
  for id in ids {
    if id.batch_index >= 0 {
      self.ack(id, await_response~)
    } else {
      for chunk_id in self.chunk_acks.get(id).unwrap_or([id]) {
        ordinary.push(chunk_id)
      }
    }
  }
  if !ordinary.is_empty() {
    self.ack_dispatcher.ack(
      @proto.CommandAck_AckType::Individual,
      ordinary,
      await_response~,
    )
    for id in ids {
      if id.batch_index < 0 {
        self.ack_pending.remove(id)
        self.chunk_acks.remove(id)
      }
    }
  }
}

///|
/// Acknowledge cumulatively: this message and all before it.
pub async fn SingleConsumer::ack_cumulative(
  self : SingleConsumer,
  id : MessageId,
  await_response? : Bool = false,
) -> Unit {
  if id.batch_index >= 0 {
    self.batch_acks.ack(
      self.conn_ref.val,
      self.consumer_id,
      id,
      @proto.CommandAck_AckType::Cumulative,
      await_response~,
      dispatcher=self.ack_dispatcher,
    )
  } else {
    self.ack_dispatcher.ack(
      @proto.CommandAck_AckType::Cumulative,
      [id],
      await_response~,
    )
  }
  self.clear_cumulative_ack(id)
}

///|
/// Acknowledge a message as part of a transaction.
pub async fn SingleConsumer::ack_txn(
  self : SingleConsumer,
  txn : Transaction,
  id : MessageId,
  await_response? : Bool = false,
) -> Unit {
  txn.add_subscription(self.topic, self.subscription)
  if id.batch_index >= 0 {
    self.batch_acks.ack(
      self.conn_ref.val,
      self.consumer_id,
      id,
      @proto.CommandAck_AckType::Individual,
      txn~,
      await_response~,
      dispatcher=self.ack_dispatcher,
    )
  } else {
    let ids = self.chunk_acks.get(id).unwrap_or([id])
    self.ack_dispatcher.ack(
      @proto.CommandAck_AckType::Individual,
      ids,
      txn~,
      await_response~,
    )
  }
  self.ack_pending.remove(id)
  self.chunk_acks.remove(id)
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
/// Return the broker's last message ID for this consumer's topic.
pub async fn SingleConsumer::last_message_id(
  self : SingleConsumer,
) -> MessageId {
  guard !self.closed.val else { raise ConsumerClosed }
  guard !self.reconnecting.val else { raise ConnectionClosed }
  query_last_message_id(self.conn_ref.val, self.consumer_id)
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
  self.ack_dispatcher.flush()
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
  self.clear_buffered_messages()
  self.scale_receiver_hint = false
  self.ack_pending.clear()
  self.batch_acks.clear()
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
  self.ack_dispatcher.consumer_id = self.consumer_id
  conn.message_handlers[self.consumer_id] = fn(frame) { self.on_frame(frame) }
  let subscribe_result = Ok(
    conn.request(self.subscribe_command(conn.next_request_id())),
  ) catch {
    e => Err(e)
  }
  let subscribed = match subscribe_result {
    Ok(value) => value
    Err(e) => {
      self.on_connection_closed(Some(e))
      raise e
    }
  }
  guard subscribed.type_ is @proto.BaseCommand_Type::SUCCESS else {
    self.on_connection_closed(None)
    raise UnexpectedResponse("expected SUCCESS for re-SUBSCRIBE after SEEK")
  }
  self.seeking.val = false
  self.reset_flow()
}

///|
/// Close the consumer and its subscription cursor is kept (durable).
pub async fn SingleConsumer::close(self : SingleConsumer) -> Unit {
  if self.closed.val {
    return
  }
  self.closed.val = true
  self.clear_buffered_messages()
  self.chunks.clear()
  self.stop_chunk_sweeper()
  self.ack_dispatcher.stop()
  self.ack_dispatcher.flush() catch {
    _ => ()
  }
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
  self.batch_acks.clear()
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
  self.stop_chunk_sweeper()
  self.ack_dispatcher.stop()
  self.ack_dispatcher.reset()
  self.nack_epoch.val += 1
  self.conn_ref.val.message_handlers.remove(self.consumer_id)
  self.ack_pending.clear()
  self.batch_acks.clear()
  self.chunks.clear()
  self.chunk_acks.clear()
  self.clear_buffered_messages()
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
  reconnect_on_broker_close : Ref[Bool]
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
fn PartitionedConsumerState::clear_buffered_messages(
  self : PartitionedConsumerState,
) -> Unit {
  while true {
    match (self.queue.try_get() catch { _ => None }) {
      Some(item) => item.message.release_retained_memory()
      None => break
    }
  }
}

///|
fn PartitionedConsumerState::prune_inactive(
  self : PartitionedConsumerState,
) -> Unit {
  let active = []
  while true {
    match (self.queue.try_get() catch { _ => None }) {
      Some(item) =>
        if self.active_sources.contains(item.source_id) {
          active.push(item)
        } else {
          item.message.release_retained_memory()
        }
      None => break
    }
  }
  for item in active {
    if !(self.queue.try_put(item) catch { _ => false }) {
      item.message.release_retained_memory()
    }
  }
}

///|
/// A consumer for a topic, partitioned or not. Partitioned consumers merge
/// the message streams of every partition.
pub struct Consumer {
  priv inner : ConsumerImpl
  priv mut dead_letter : DeadLetterState?
  priv mut retry_letter : RetryLetterState?
  priv mut interceptors : Array[ConsumerInterceptor]
  priv mut span_recorder : ClientSpanRecorder?
}

///|
fn Consumer::set_span_recorder(
  self : Consumer,
  recorder : ClientSpanRecorder?,
) -> Unit {
  self.span_recorder = recorder
  match self.inner {
    ConsumerImpl::Single(c) => c.span_recorder = recorder
    ConsumerImpl::Partitioned(state) =>
      for sub in state.consumers {
        sub.set_span_recorder(recorder)
      }
  }
}

///|
fn Consumer::set_reconnect_on_broker_close(
  self : Consumer,
  enabled : Bool,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => c.reconnect_on_broker_close = enabled
    ConsumerImpl::Partitioned(state) => {
      state.reconnect_on_broker_close.val = enabled
      for sub in state.consumers {
        sub.set_reconnect_on_broker_close(enabled)
      }
    }
  }
}

///|
fn Consumer::has_closed_source(self : Consumer, topic : String) -> Bool {
  match self.inner {
    ConsumerImpl::Single(c) =>
      base_topic_name(c.topic()) == topic && c.is_closed()
    ConsumerImpl::Partitioned(state) => {
      for sub in state.consumers {
        if sub.has_closed_source(topic) {
          return true
        }
      }
      false
    }
  }
}

///|
fn Consumer::set_interceptors(
  self : Consumer,
  interceptors : Array[ConsumerInterceptor],
) -> Unit {
  self.interceptors = interceptors
}

///|
fn Consumer::delivered(self : Consumer, message : Message) -> Unit {
  message.interceptors = self.interceptors
  message.span_recorder = self.span_recorder
  for interceptor in self.interceptors {
    interceptor.delivered(message)
  }
  message.mark_delivered()
}

///|
fn Consumer::observe_ack(
  self : Consumer,
  id : MessageId,
  result : Result[Unit, Error],
) -> Unit {
  for interceptor in self.interceptors {
    interceptor.acked(id, result)
  }
}

///|
fn Consumer::observe_nack(
  self : Consumer,
  id : MessageId,
  result : Result[Unit, Error],
) -> Unit {
  for interceptor in self.interceptors {
    interceptor.nacked(id, result)
  }
}

///|
priv struct DeadLetterState {
  policy : DeadLetterPolicy
  producer : Producer
  group : @async.TaskGroup[Unit]
  nacked : Map[(String, MessageId), UInt]
}

///|
fn Consumer::set_dead_letter(
  self : Consumer,
  policy : DeadLetterPolicy,
  producer : Producer,
  group : @async.TaskGroup[Unit],
) -> Unit {
  self.dead_letter = Some({ policy, producer, group, nacked: {}, })
}

///|
fn dead_letter_key(message : Message) -> (String, MessageId) {
  (message.topic(), message.id())
}

///|
fn DeadLetterState::note_nack(
  self : DeadLetterState,
  message : Message,
) -> Unit {
  let key = dead_letter_key(message)
  self.nacked[key] = self.nacked.get(key).unwrap_or(0U) + 1U
}

///|
fn DeadLetterState::note_nack_id(
  self : DeadLetterState,
  topic : String,
  id : MessageId,
) -> Unit {
  let key = (topic, id)
  self.nacked[key] = self.nacked.get(key).unwrap_or(0U) + 1U
}

///|
fn DeadLetterState::clear(self : DeadLetterState, message : Message) -> Unit {
  self.nacked.remove(dead_letter_key(message))
}

///|
fn DeadLetterState::should_route(
  self : DeadLetterState,
  message : Message,
) -> Bool {
  let nacks = self.nacked.get(dead_letter_key(message)).unwrap_or(0U)
  message.redelivery_count() >= self.policy.max_deliveries ||
  nacks >= self.policy.max_deliveries
}

///|
fn DeadLetterState::watch(self : DeadLetterState, message : Message) -> Unit {
  message.on_nack = Some(fn(m) { self.note_nack(m) })
  message.on_ack = Some(fn(m) { self.clear(m) })
}

///|
async fn DeadLetterState::route(
  self : DeadLetterState,
  message : Message,
) -> Unit {
  defer message.release_retained_memory()
  let outgoing = ProducerMessage::new(
    message.payload(),
    key?=message.key(),
    ordering_key?=message.ordering_key(),
    properties=message.properties(),
    event_time?=message.event_time(),
  )
  let receipt = self.producer.send(outgoing) catch {
    _ => {
      message.nack()
      return
    }
  }
  ignore(receipt)
  message.ack()
  self.clear(message)
}

///|
fn Consumer::single(c : SingleConsumer) -> Consumer {
  {
    inner: ConsumerImpl::Single(c),
    dead_letter: None,
    retry_letter: None,
    interceptors: [],
    span_recorder: None,
  }
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
    reconnect_on_broker_close: { val: true, },
    source_ids: [],
    next_source_id: 0,
    active_sources: {},
    queue: @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(COMPOSITE_CONSUMER_QUEUE_CAPACITY),
    ),
    closed: { val: false, },
    topic_label,
    subscription_label,
  }
  for sub in consumers {
    state.add_source(group, sub)
  }
  {
    inner: ConsumerImpl::Partitioned(state),
    dead_letter: None,
    retry_letter: None,
    interceptors: [],
    span_recorder: None,
  }
}

///|
fn Consumer::pattern(pattern : String, subscription : String) -> Consumer {
  {
    inner: ConsumerImpl::Partitioned({
      consumers: [],
      reconnect_on_broker_close: { val: true, },
      source_ids: [],
      next_source_id: 0,
      active_sources: {},
      queue: @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Blocking(COMPOSITE_CONSUMER_QUEUE_CAPACITY),
      ),
      closed: { val: false, },
      topic_label: pattern,
      subscription_label: subscription,
    }),
    dead_letter: None,
    retry_letter: None,
    interceptors: [],
    span_recorder: None,
  }
}

///|
fn PartitionedConsumerState::add_source(
  self : PartitionedConsumerState,
  group : @async.TaskGroup[Unit],
  sub : Consumer,
) -> Unit {
  sub.set_reconnect_on_broker_close(self.reconnect_on_broker_close.val)
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
          while self.active_sources.contains(source_id) && !self.closed.val {
            if (self.queue.try_put({ source_id, message: msg, }) catch {
                _ => false
              }) {
              break
            }
            @async.sleep(5)
          }
          if !self.active_sources.contains(source_id) || self.closed.val {
            msg.release_retained_memory()
            return
          }
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
          state.prune_inactive()
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
async fn Consumer::receive_raw(self : Consumer) -> Message {
  match self.inner {
    ConsumerImpl::Single(c) => c.receive_untracked()
    ConsumerImpl::Partitioned(state) => {
      while true {
        let item = state.queue.get()
        if state.active_sources.contains(item.source_id) {
          return item.message
        }
        item.message.release_retained_memory()
      }
      raise ConsumerClosed
    }
  }
}

///|
async fn Consumer::receive_untracked(self : Consumer) -> Message {
  while true {
    let message = self.receive_raw()
    if self.dead_letter is Some(dlq) {
      if dlq.should_route(message) {
        dlq.route(message)
        continue
      }
      dlq.watch(message)
    }
    return message
  }
  raise ConsumerClosed
}

///|
/// Receive the next message, blocking until one is available.
pub async fn Consumer::receive(self : Consumer) -> Message {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Receive, self.topic())
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let result = Ok(self.receive_untracked()) catch { error => Err(error) }
  let message = match result {
    Ok(value) => value
    Err(error) => {
      if self.span_recorder is Some(recorder) && span is Some(start) {
        recorder.finish(start, false, error="\{error}")
        span_done.val = true
      }
      self.observe_receive_error(error)
      raise error
    }
  }
  self.delivered(message)
  if self.span_recorder is Some(recorder) && span is Some(start) {
    recorder.finish({ ..start, message_id: Some(message.id()), }, true)
    span_done.val = true
  }
  message
}

///|
/// Non-blocking receive: returns `None` if no message is buffered.
pub fn Consumer::try_receive(self : Consumer) -> Message? raise {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Receive, self.topic())
  })
  let result = Ok(self.try_receive_raw()) catch { error => Err(error) }
  match result {
    Ok(value) => {
      if self.span_recorder is Some(recorder) && span is Some(start) {
        let completed = match value {
          Some(message) => { ..start, message_id: Some(message.id()), }
          None => start
        }
        recorder.finish(completed, true)
      }
      value
    }
    Err(error) => {
      if self.span_recorder is Some(recorder) && span is Some(start) {
        recorder.finish(start, false, error="\{error}")
      }
      self.observe_receive_error(error)
      raise error
    }
  }
}

///|
fn Consumer::observe_receive_error(self : Consumer, error : Error) -> Unit {
  for interceptor in self.interceptors {
    interceptor.receive_failed(error)
  }
}

///|
fn Consumer::try_receive_raw(self : Consumer) -> Message? raise {
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
            } else {
              item.message.release_retained_memory()
            }
          None => done = true
        }
      }
      result
    }
  }
  if message is Some(msg) {
    if self.dead_letter is Some(dlq) {
      if dlq.should_route(msg) {
        dlq.group.spawn_bg(
          async fn() { dlq.route(msg) catch { _ => () } },
          no_wait=true,
          allow_failure=true,
        )
        return None
      }
      dlq.watch(msg)
    }
    self.delivered(msg)
  }
  message
}

///|
/// Acknowledge a message individually.
pub async fn Consumer::ack(
  self : Consumer,
  id : MessageId,
  await_response? : Bool = false,
) -> Unit {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Ack, self.topic(), message_id=id)
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let result : Result[Unit, Error] = Ok(self.ack_raw(id, await_response)) catch {
    e => Err(e)
  }
  if self.span_recorder is Some(recorder) && span is Some(start) {
    recorder.finish_result(start, result)
    span_done.val = true
  }
  self.observe_ack(id, result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
/// Acknowledge multiple IDs belonging to a single-topic consumer.
pub async fn Consumer::ack_ids(
  self : Consumer,
  ids : Array[MessageId],
  await_response? : Bool = false,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => {
      let ordinary : Array[MessageId] = []
      for id in ids {
        if id.batch_index >= 0 {
          self.ack(id, await_response~)
        } else {
          ordinary.push(id)
        }
      }
      if !ordinary.is_empty() {
        let span = self.span_recorder.map(fn(recorder) {
          recorder.begin(Ack, c.topic())
        })
        let span_done : Ref[Bool] = { val: false, }
        defer (if !span_done.val &&
          self.span_recorder is Some(recorder) &&
          span is Some(start) {
          recorder.finish(start, false, error="cancelled")
        })
        let result : Result[Unit, Error] = Ok(
          c.ack_ids(ordinary, await_response~),
        ) catch {
          e => Err(e)
        }
        if self.span_recorder is Some(recorder) && span is Some(start) {
          recorder.finish_result(start, result)
          span_done.val = true
        }
        for id in ordinary {
          self.observe_ack(id, result)
        }
        match result {
          Ok(_) =>
            if self.dead_letter is Some(dlq) {
              for id in ordinary {
                dlq.nacked.remove((c.topic(), id))
              }
            }
          Err(e) => raise e
        }
      }
    }
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "ack by id needs the message's own consumer; use message.ack()",
      )
  }
}

///|
async fn Consumer::ack_raw(
  self : Consumer,
  id : MessageId,
  await_response : Bool,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => {
      c.ack(id, await_response~)
      if self.dead_letter is Some(dlq) {
        dlq.nacked.remove((c.topic(), id))
      }
    }
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "ack by id needs the message's own consumer; use message.ack()",
      )
  }
}

///|
/// Acknowledge cumulatively: this message and all before it.
pub async fn Consumer::ack_cumulative(
  self : Consumer,
  id : MessageId,
  await_response? : Bool = false,
) -> Unit {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Ack, self.topic(), message_id=id)
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let result : Result[Unit, Error] = Ok(
    self.ack_cumulative_raw(id, await_response),
  ) catch {
    e => Err(e)
  }
  if self.span_recorder is Some(recorder) && span is Some(start) {
    recorder.finish_result(start, result)
    span_done.val = true
  }
  self.observe_ack(id, result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
async fn Consumer::ack_cumulative_raw(
  self : Consumer,
  id : MessageId,
  await_response : Bool,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => {
      c.ack_cumulative(id, await_response~)
      if self.dead_letter is Some(dlq) {
        let cleared : Array[(String, MessageId)] = []
        for key, _ in dlq.nacked {
          let (topic, pending_id) = key
          if topic == c.topic() && pending_id <= id {
            cleared.push(key)
          }
        }
        for key in cleared {
          dlq.nacked.remove(key)
        }
      }
    }
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
  await_response? : Bool = false,
) -> Unit {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Ack, self.topic(), message_id=id)
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let result : Result[Unit, Error] = Ok(
    self.ack_txn_raw(txn, id, await_response),
  ) catch {
    e => Err(e)
  }
  if self.span_recorder is Some(recorder) && span is Some(start) {
    recorder.finish_result(start, result)
    span_done.val = true
  }
  self.observe_ack(id, result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
async fn Consumer::ack_txn_raw(
  self : Consumer,
  txn : Transaction,
  id : MessageId,
  await_response : Bool,
) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => {
      c.ack_txn(txn, id, await_response~)
      if self.dead_letter is Some(dlq) {
        dlq.nacked.remove((c.topic(), id))
      }
    }
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "txn ack needs the message's own consumer; use message.ack_txn()",
      )
  }
}

///|
/// Negatively acknowledge a message, triggering redelivery.
pub async fn Consumer::nack(self : Consumer, id : MessageId) -> Unit {
  let result : Result[Unit, Error] = Ok(self.nack_raw(id)) catch { e => Err(e) }
  self.observe_nack(id, result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
async fn Consumer::nack_raw(self : Consumer, id : MessageId) -> Unit {
  match self.inner {
    ConsumerImpl::Single(c) => {
      c.nack(id)
      if self.dead_letter is Some(dlq) {
        dlq.note_nack_id(c.topic(), id)
      }
    }
    ConsumerImpl::Partitioned(_) =>
      raise InvalidFrame(
        "nack by id needs the message's own consumer; use message.nack()",
      )
  }
}

///|
/// Return the broker's last message ID for each subscribed topic.
pub async fn Consumer::last_message_ids(
  self : Consumer,
) -> Map[String, MessageId] {
  let result : Map[String, MessageId] = Map([])
  match self.inner {
    ConsumerImpl::Single(c) => result[c.topic()] = c.last_message_id()
    ConsumerImpl::Partitioned(state) => {
      guard !state.closed.val else { raise ConsumerClosed }
      for sub in state.consumers {
        for topic, id in sub.last_message_ids() {
          result[topic] = id
        }
      }
    }
  }
  result
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
                  sub.topic().has_suffix("-partition-\{id.partition}")
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
      state.clear_buffered_messages()
    }
  }
}

///|
/// Close the consumer (all partitions).
pub async fn Consumer::close(self : Consumer) -> Unit {
  let mut failure : Error? = None
  try {
    match self.inner {
      ConsumerImpl::Single(c) => c.close()
      ConsumerImpl::Partitioned(state) => {
        state.closed.val = true
        state.clear_buffered_messages()
        state.queue.close(error=ConsumerClosed, clear=true)
        for c in state.consumers {
          c.close()
        }
      }
    }
  } catch {
    e => failure = Some(e)
  }
  if self.dead_letter is Some(dlq) {
    dlq.producer.close() catch {
      e => if failure is None { failure = Some(e) }
    }
  }
  if self.retry_letter is Some(retry) {
    retry.producer.close() catch {
      e => if failure is None { failure = Some(e) }
    }
  }
  if failure is Some(error) {
    raise error
  }
}

///|
/// Remove the subscription from every source topic and close the consumer.
pub async fn Consumer::unsubscribe(
  self : Consumer,
  force? : Bool = false,
) -> Unit {
  let mut failure : Error? = None
  try {
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
        state.clear_buffered_messages()
        state.queue.close(error=ConsumerClosed, clear=true)
        if failure is Some(error) {
          raise error
        }
      }
    }
  } catch {
    e => failure = Some(e)
  }
  if self.dead_letter is Some(dlq) {
    dlq.producer.close() catch {
      e => if failure is None { failure = Some(e) }
    }
  }
  if self.retry_letter is Some(retry) {
    retry.producer.close() catch {
      e => if failure is None { failure = Some(e) }
    }
  }
  if failure is Some(error) {
    raise error
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
      let seen : Map[String, Bool] = Map([])
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
