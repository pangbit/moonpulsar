// SingleProducer: publishes messages to a topic with optional batching.
//
// Reconnection: when the connection breaks and the producer was created
// with a `reconnect_with` provider (which `Client::create_producer` always
// passes), the producer re-registers itself on a fresh connection and
// replays every message that has not received a receipt yet.

///|
/// How a producer acquires permission to publish to a topic.
pub(all) enum ProducerAccessMode {
  Shared
  Exclusive
  WaitForExclusive
  ExclusiveWithFencing
} derive(Eq, Debug)

///|
/// Choose a partition for a partitioned producer. The default hashes keyed
/// messages and distributes keyless messages in round-robin order.
pub(all) enum ProducerRoutingMode {
  KeyHash
  RoundRobin
  SinglePartition
  Custom((ProducerMessage, Int) -> Int)
}

///|
fn ProducerAccessMode::to_proto(
  self : ProducerAccessMode,
) -> @proto.ProducerAccessMode {
  match self {
    Shared => @proto.ProducerAccessMode::Shared
    Exclusive => @proto.ProducerAccessMode::Exclusive
    WaitForExclusive => @proto.ProducerAccessMode::WaitForExclusive
    ExclusiveWithFencing => @proto.ProducerAccessMode::ExclusiveWithFencing
  }
}

///|
/// Batching configuration for a producer.
pub(all) struct BatchingConfig {
  /// flush the batch after this many messages
  max_messages : Int
  /// flush the batch after this many payload bytes
  max_bytes : Int
  /// flush the batch this long after the first message
  max_delay_ms : Int
} derive(Debug)

///|
pub fn BatchingConfig::default() -> BatchingConfig {
  { max_messages: 1000, max_bytes: 131072, max_delay_ms: 10, }
}

///|
/// A future receipt for a message sent with `SingleProducer::send_async`.
pub struct ReceiptFuture {
  priv reply : @aqueue.Queue[SendReceipt]
}

///|
/// Wait for the broker's send receipt.
pub async fn ReceiptFuture::wait(self : ReceiptFuture) -> SendReceipt {
  self.reply.get()
}

///|
priv struct BufferedMessage {
  message : ProducerMessage
  waiter : @aqueue.Queue[SendReceipt]
  token : UInt64
  done : Ref[Bool]
}

///|
/// A producer bound to a single topic.
pub struct SingleProducer {
  priv group : @async.TaskGroup[Unit]
  priv mut conn : Connection
  priv topic : String
  priv producer_id : UInt64
  priv requested_name : String?
  priv properties : Map[String, String]
  priv access_mode : ProducerAccessMode
  priv mut topic_epoch : UInt64?
  priv mut epoch : UInt64
  priv mut name : String
  priv mut sequence_id : UInt64
  priv mut last_confirmed_sequence_id : UInt64?
  priv pending : Map[UInt64, @aqueue.Queue[SendReceipt]]
  priv timeout_generation : Map[UInt64, UInt64]
  priv mut next_timeout_generation : UInt64
  /// encoded SEND frames awaiting a receipt, for replay after reconnect
  priv payloads : Map[UInt64, Bytes]
  priv reconnect_with : (async () -> Connection)?
  priv reconnect_policy : RetryPolicy
  priv reconnecting : Ref[Bool]
  priv pending_slots : @aqueue.Queue[Unit]?
  priv memory_limiter : ClientMemoryLimiter?
  priv block_if_queue_full : Bool
  priv send_timeout_ms : Int?
  priv compression : CompressionCodec
  priv chunk_size : Int?
  priv automatic_chunking : Bool
  priv chunk_mu : @async.Mutex
  priv schema : SchemaInfo?
  priv mut schema_version : Bytes?
  priv batching : BatchingConfig?
  priv batch_mu : @async.Mutex
  priv mut batch_entries : Array[BufferedMessage]
  priv mut batch_bytes : Int
  priv mut batch_timer_running : Bool
  priv mut batch_timer_generation : UInt64
  priv mut next_batch_token : UInt64
  priv mut closed : Bool
}

///|
fn SingleProducer::next_sequence_id(self : SingleProducer) -> UInt64 {
  let seq = self.sequence_id
  self.sequence_id += 1
  seq
}

///|
/// Create a producer on `topic` through an already-resolved connection.
/// `reconnect_with` should yield a fresh connection to the owning broker;
/// it enables automatic re-registration and pending message replay.
pub async fn SingleProducer::create(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  name? : String,
  properties? : Map[String, String] = {},
  access_mode? : ProducerAccessMode = Shared,
  batching? : BatchingConfig,
  reconnect_with? : async () -> Connection,
  compression? : CompressionCodec = None,
  chunk_size? : Int,
  automatic_chunking? : Bool = false,
  schema? : SchemaInfo,
  reconnect_policy? : RetryPolicy = RetryPolicy::default(),
  max_pending_messages? : Int,
  block_if_queue_full? : Bool = false,
  send_timeout_ms? : Int,
) -> SingleProducer {
  SingleProducer::create_with_memory(
    group,
    conn,
    topic,
    name?,
    properties~,
    access_mode~,
    batching?,
    reconnect_with?,
    compression~,
    chunk_size?,
    automatic_chunking~,
    schema?,
    reconnect_policy~,
    max_pending_messages?,
    block_if_queue_full~,
    send_timeout_ms?,
  )
}

///|
async fn SingleProducer::create_with_memory(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  name? : String,
  properties? : Map[String, String] = {},
  access_mode? : ProducerAccessMode = Shared,
  batching? : BatchingConfig,
  reconnect_with? : async () -> Connection,
  compression? : CompressionCodec = None,
  chunk_size? : Int,
  automatic_chunking? : Bool = false,
  schema? : SchemaInfo,
  reconnect_policy? : RetryPolicy = RetryPolicy::default(),
  max_pending_messages? : Int,
  block_if_queue_full? : Bool = false,
  send_timeout_ms? : Int,
  memory_limiter? : ClientMemoryLimiter,
) -> SingleProducer {
  if max_pending_messages is Some(limit) && limit <= 0 {
    raise InvalidFrame("max pending messages must be positive")
  }
  if send_timeout_ms is Some(timeout) && timeout <= 0 {
    raise InvalidFrame("send timeout must be positive")
  }
  if chunk_size is Some(size) && size <= 0 {
    raise InvalidFrame("chunk size must be positive")
  }
  if (chunk_size is Some(_) || automatic_chunking) && batching is Some(_) {
    raise InvalidFrame("chunking and batching cannot be combined")
  }
  let request_id = conn.next_request_id()
  let producer_id = conn.next_request_id()
  let pending_slots : @aqueue.Queue[Unit]? = match max_pending_messages {
    Some(limit) => {
      let slots : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=@aqueue.Kind::Blocking(limit),
      )
      for _ in 0..<limit {
        ignore(slots.try_put(()))
      }
      Some(slots)
    }
    None => None
  }
  let producer : SingleProducer = {
    group,
    conn,
    topic,
    producer_id,
    requested_name: name,
    properties,
    access_mode,
    topic_epoch: None,
    epoch: 0UL,
    name: name.unwrap_or(""),
    sequence_id: 0,
    last_confirmed_sequence_id: None,
    pending: {},
    timeout_generation: {},
    next_timeout_generation: 0UL,
    payloads: {},
    reconnect_with,
    reconnect_policy,
    reconnecting: { val: false, },
    pending_slots,
    memory_limiter,
    block_if_queue_full,
    send_timeout_ms,
    compression,
    chunk_size,
    automatic_chunking,
    chunk_mu: @async.Mutex::Mutex(),
    schema,
    schema_version: None,
    batching,
    batch_mu: @async.Mutex::Mutex(),
    batch_entries: [],
    batch_bytes: 0,
    batch_timer_running: false,
    batch_timer_generation: 0UL,
    next_batch_token: 0UL,
    closed: false,
  }
  producer.attach(conn)
  errdefer conn.producer_handlers.remove(producer_id)
  let reply = conn.request(producer.producer_command(request_id))
  guard reply.producer_success is Some(success) else {
    raise UnexpectedResponse("expected PRODUCER_SUCCESS")
  }
  producer.name = success.producer_name
  producer.topic_epoch = success.topic_epoch
  producer.schema_version = success.schema_version
  if success.last_sequence_id is Some(last) && last >= 0 {
    producer.sequence_id = (last + 1).reinterpret_as_uint64()
    producer.last_confirmed_sequence_id = Some(last.reinterpret_as_uint64())
  }
  producer
}

///|
fn SingleProducer::producer_command(
  self : SingleProducer,
  request_id : UInt64,
) -> @proto.BaseCommand {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::PRODUCER
  let produce = @proto.CommandProducer::default()
  produce.topic = self.topic
  produce.producer_id = self.producer_id
  produce.request_id = request_id
  produce.producer_name = if self.name == "" {
    self.requested_name
  } else {
    Some(self.name)
  }
  produce.metadata = key_values_of(self.properties)
  produce.epoch = Some(self.epoch)
  produce.user_provided_producer_name = Some(self.requested_name is Some(_))
  produce.producer_access_mode = Some(self.access_mode.to_proto())
  produce.topic_epoch = self.topic_epoch
  if self.schema is Some(schema) {
    produce.schema = Some(schema.to_proto())
  }
  cmd.producer = Some(produce)
  cmd
}

///|
/// Wire dispatch handlers and the close hook onto a connection.
fn SingleProducer::attach(self : SingleProducer, conn : Connection) -> Unit {
  conn.producer_handlers[self.producer_id] = fn(cmd) {
    self.on_command(cmd) catch {
      _ => ()
    }
  }
  conn.on_close(fn(e) { self.on_connection_closed(e) })
}

///|
fn SingleProducer::on_command(
  self : SingleProducer,
  cmd : @proto.BaseCommand,
) -> Unit raise {
  if cmd.send_receipt is Some(receipt) {
    if self.pending.get(receipt.sequence_id) is Some(waiter) {
      if self.last_confirmed_sequence_id is None ||
        self.last_confirmed_sequence_id.unwrap() < receipt.sequence_id {
        self.last_confirmed_sequence_id = Some(receipt.sequence_id)
      }
      self.pending.remove(receipt.sequence_id)
      self.timeout_generation.remove(receipt.sequence_id)
      self.payloads.remove(receipt.sequence_id)
      let message_id = match receipt.message_id {
        Some(data) => MessageId::from_proto(data)
        None => MessageId::latest()
      }
      let result : SendReceipt = {
        message_id,
        sequence_id: receipt.sequence_id,
        producer_id: receipt.producer_id,
        producer_name: self.name,
      }
      ignore(waiter.try_put(result))
    }
  } else if cmd.send_error is Some(err) {
    if self.pending.get(err.sequence_id) is Some(waiter) {
      self.pending.remove(err.sequence_id)
      self.timeout_generation.remove(err.sequence_id)
      self.payloads.remove(err.sequence_id)
      waiter.close(
        error=ServerError(
          code=err.error.to_enum().0.reinterpret_as_int(),
          message=err.message,
        ),
        clear=true,
      )
    }
  } else if cmd.close_producer is Some(_) {
    self.fail_all(ConnectionClosed)
  }
}

///|
fn SingleProducer::on_connection_closed(
  self : SingleProducer,
  error : Error?,
) -> Unit {
  ignore(error)
  if self.closed || self.reconnect_with is None {
    self.fail_all(ConnectionClosed)
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
/// Re-register on a fresh connection and replay unconfirmed messages.
async fn SingleProducer::reconnect(self : SingleProducer) -> Unit {
  guard self.reconnect_with is Some(provider) else { return }
  let policy = self.reconnect_policy
  let method = @async.RetryMethod::ExponentialDelay(
    initial=policy.initial_backoff_ms,
    factor=2.0,
    maximum=policy.max_backoff_ms,
  )
  let attempt = async fn() -> Unit {
    if self.closed {
      raise ProducerClosed
    }
    let conn = provider()
    self.attach(conn)
    let request_id = conn.next_request_id()
    self.epoch += 1UL
    let reply = conn.request(self.producer_command(request_id))
    guard reply.producer_success is Some(success) else {
      raise UnexpectedResponse("expected PRODUCER_SUCCESS")
    }
    self.topic_epoch = success.topic_epoch
    self.name = success.producer_name
    self.schema_version = success.schema_version
    if success.last_sequence_id is Some(last) && last >= 0 {
      let confirmed = last.reinterpret_as_uint64()
      if self.last_confirmed_sequence_id is None ||
        self.last_confirmed_sequence_id.unwrap() < confirmed {
        self.last_confirmed_sequence_id = Some(confirmed)
      }
      if self.sequence_id <= confirmed {
        self.sequence_id = confirmed + 1UL
      }
    }
    // swap the connection, then replay unconfirmed messages in order
    self.conn = conn
    let seqs = self.payloads.keys().collect()
    seqs.sort()
    for seq in seqs {
      if self.payloads.get(seq) is Some(bytes) {
        conn.send_bytes(bytes)
      }
    }
  }
  let run = async fn() -> Unit {
    if policy.max_retries < 0 {
      @async.retry(method, fatal_error=fn(e) { e is ProducerClosed }, attempt)
    } else {
      @async.retry(
        method,
        max_retry=policy.max_retries,
        fatal_error=fn(e) { e is ProducerClosed },
        attempt,
      )
    }
  }
  run() catch {
    _ => {
      self.closed = true
      self.fail_all(ConnectionClosed)
    }
  }
  self.reconnecting.val = false
}

///|
fn SingleProducer::fail_all(self : SingleProducer, error : PulsarError) -> Unit {
  if self.pending_slots is Some(slots) {
    slots.close(error~, clear=true)
  }
  for _, waiter in self.pending {
    waiter.close(error~, clear=true)
  }
  self.pending.clear()
  self.timeout_generation.clear()
  self.payloads.clear()
  for entry in self.batch_entries {
    entry.done.val = true
    entry.waiter.close(error~, clear=true)
  }
  self.batch_entries.clear()
  self.batch_bytes = 0
}

///|
fn SingleProducer::build_metadata(
  self : SingleProducer,
  msg : ProducerMessage,
  sequence_id : UInt64,
) -> @proto.MessageMetadata {
  let metadata = @proto.MessageMetadata::default()
  metadata.producer_name = self.name
  metadata.sequence_id = sequence_id
  metadata.schema_version = self.schema_version
  metadata.publish_time = @async.now().reinterpret_as_uint64()
  metadata.properties = key_values_of(msg.properties)
  // clear proto2 defaults that only apply to batched/timed messages
  metadata.num_messages_in_batch = None
  metadata.event_time = None
  if msg.key is Some(key) {
    metadata.partition_key = Some(key)
  }
  if msg.event_time is Some(t) {
    metadata.event_time = Some(t)
  }
  if msg.ordering_key is Some(k) {
    metadata.ordering_key = Some(k)
  }
  if msg.deliver_at is Some(t) {
    metadata.deliver_at_time = Some(t.reinterpret_as_int64())
  } else if msg.deliver_after is Some(delay) {
    let now = @async.now()
    metadata.deliver_at_time = Some(now + delay.reinterpret_as_int64())
  }
  metadata
}

///|
/// Send a message and wait for the broker's receipt.
pub async fn SingleProducer::send(
  self : SingleProducer,
  msg : ProducerMessage,
) -> SendReceipt {
  self.send_async(msg).wait()
}

///|
/// Send a message without waiting for the receipt; the returned future
/// resolves once the broker confirms.
pub async fn SingleProducer::send_async(
  self : SingleProducer,
  msg : ProducerMessage,
) -> ReceiptFuture {
  if self.closed {
    raise ProducerClosed
  }
  let slots = self.pending_slots
  if slots is Some(queue) {
    if self.block_if_queue_full {
      queue.get() catch {
        _ => raise ProducerClosed
      }
    } else if queue.try_get() is None {
      raise ProducerQueueFull
    }
  }
  let size = msg.payload.length()
  if self.memory_limiter is Some(limiter) {
    limiter.acquire(size, self.block_if_queue_full) catch {
      e => {
        if slots is Some(queue) {
          ignore(queue.try_put(()) catch { _ => false })
        }
        raise e
      }
    }
  }
  if self.closed {
    if self.memory_limiter is Some(limiter) {
      limiter.release(size)
    }
    if slots is Some(queue) {
      ignore(queue.try_put(()) catch { _ => false })
    }
    raise ProducerClosed
  }
  let inner = self.send_async_unbounded(msg) catch {
    e => {
      if self.memory_limiter is Some(limiter) {
        limiter.release(size)
      }
      if slots is Some(queue) {
        ignore(queue.try_put(()) catch { _ => false })
      }
      raise e
    }
  }
  if slots is None && self.memory_limiter is None {
    return inner
  }
  let reply : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Blocking(1),
  )
  self.group.spawn_bg(
    async fn() {
      let result : Result[SendReceipt, Error] = Ok(inner.wait()) catch {
        e => Err(e)
      }
      if self.memory_limiter is Some(limiter) {
        limiter.release(size)
      }
      if slots is Some(queue) {
        ignore(queue.try_put(()) catch { _ => false })
      }
      match result {
        Ok(receipt) => ignore(reply.try_put(receipt))
        Err(e) => reply.close(error=e, clear=true)
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  { reply, }
}

///|
async fn SingleProducer::send_async_unbounded(
  self : SingleProducer,
  msg : ProducerMessage,
) -> ReceiptFuture {
  if self.closed {
    raise ProducerClosed
  }
  if self.chunk_size is Some(_) || self.automatic_chunking {
    let reply : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    self.group.spawn_bg(
      async fn() {
        let receipt = self.send_chunked(msg) catch {
          e => {
            reply.close(error=e, clear=true)
            return
          }
        }
        ignore(reply.try_put(receipt))
      },
      no_wait=true,
      allow_failure=true,
    )
    return { reply, }
  }
  if msg.deliver_at is Some(_) || msg.deliver_after is Some(_) {
    self.flush()
    return { reply: self.send_messages([msg]), }
  }
  if self.batching is Some(_) {
    return self.enqueue_batch(msg)
  }
  let waiter = self.send_messages([msg])
  { reply: waiter, }
}

///|
/// Send a message as one or more sequential chunks with one sequence id.
async fn SingleProducer::send_chunked(
  self : SingleProducer,
  msg : ProducerMessage,
  txn? : Transaction,
) -> SendReceipt {
  self.chunk_mu.acquire()
  defer self.chunk_mu.release()
  if self.closed {
    raise ProducerClosed
  }
  let sequence_id = self.next_sequence_id()
  let metadata = self.build_metadata(msg, sequence_id)
  let payload = if self.compression is None {
    msg.payload
  } else {
    metadata.compression = self.compression.to_proto()
    metadata.uncompressed_size = Some(
      msg.payload.length().reinterpret_as_uint(),
    )
    compress(self.compression, msg.payload)
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SEND
  let send = @proto.CommandSend::default()
  send.producer_id = self.producer_id
  send.sequence_id = sequence_id
  match txn {
    Some(t) => {
      let (most, least) = t.id()
      send.txnid_least_bits = Some(least)
      send.txnid_most_bits = Some(most)
      metadata.txnid_least_bits = Some(least)
      metadata.txnid_most_bits = Some(most)
    }
    None => {
      send.txnid_least_bits = None
      send.txnid_most_bits = None
      metadata.txnid_least_bits = None
      metadata.txnid_most_bits = None
    }
  }
  cmd.send = Some(send)
  let limit = self.conn.max_message_size()
  if self.automatic_chunking && self.chunk_size is None && limit is None {
    raise InvalidFrame("broker did not advertise a max message size")
  }
  let whole = encode_send_frame(cmd, metadata, payload)
  if (self.chunk_size is None || payload.length() <= self.chunk_size.unwrap()) &&
    (limit is None || whole.length() <= limit.unwrap()) {
    return self.send_encoded(sequence_id, whole).get()
  }
  guard self.chunk_size is Some(_) || limit is Some(_) else {
    raise InvalidFrame("broker did not advertise a max message size")
  }
  guard !payload.is_empty() else {
    raise InvalidFrame("message metadata exceeds the broker max size")
  }
  metadata.uuid = Some("\{self.name}-\{sequence_id}")
  metadata.total_chunk_msg_size = Some(payload.length())
  let mut size = self.chunk_size.unwrap_or(payload.length())
  if limit is Some(max_size) {
    while true {
      let count = (payload.length() - 1) / size + 1
      metadata.num_chunks_from_msg = Some(count)
      metadata.chunk_id = Some(count - 1)
      let overhead = encode_send_frame(cmd, metadata, b"").length()
      let allowed = max_size - overhead
      guard allowed > 0 else {
        raise InvalidFrame("message metadata exceeds the broker max size")
      }
      if size <= allowed {
        break
      }
      size = allowed
    }
  }
  let count = (payload.length() - 1) / size + 1
  guard count <= 10000 else {
    raise InvalidFrame("message requires too many chunks")
  }
  metadata.num_chunks_from_msg = Some(count)
  let mut last : SendReceipt? = None
  for index in 0..<count {
    if self.closed {
      raise ProducerClosed
    }
    metadata.chunk_id = Some(index)
    let start = index * size
    let end = if start + size < payload.length() {
      start + size
    } else {
      payload.length()
    }
    let frame = encode_send_frame(cmd, metadata, payload[start:end].to_owned())
    if limit is Some(max_size) && frame.length() > max_size {
      raise InvalidFrame("chunk exceeds the broker max size")
    }
    last = Some(self.send_encoded(sequence_id, frame).get())
  }
  last.unwrap()
}

///|
async fn SingleProducer::send_encoded(
  self : SingleProducer,
  sequence_id : UInt64,
  frame : Bytes,
) -> @aqueue.Queue[SendReceipt] {
  let waiter : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Blocking(1),
  )
  self.pending[sequence_id] = waiter
  self.payloads[sequence_id] = frame
  if self.send_timeout_ms is Some(timeout) {
    self.next_timeout_generation += 1UL
    let generation = self.next_timeout_generation
    self.timeout_generation[sequence_id] = generation
    self.group.spawn_bg(
      async fn() {
        @async.sleep(timeout)
        if self.timeout_generation.get(sequence_id) == Some(generation) {
          self.pending.remove(sequence_id)
          self.payloads.remove(sequence_id)
          self.timeout_generation.remove(sequence_id)
          waiter.close(error=RequestTimeout, clear=true)
        }
      },
      no_wait=true,
      allow_failure=true,
    )
  }
  if !self.reconnecting.val {
    self.conn.send_bytes(frame)
  }
  waiter
}

///|
/// Send one or more messages as a single (optionally batched) SEND command.
/// Returns the waiter queue for the whole batch.
async fn SingleProducer::send_messages(
  self : SingleProducer,
  messages : Array[ProducerMessage],
  txn? : Transaction,
) -> @aqueue.Queue[SendReceipt] {
  let sequence_id = self.next_sequence_id()
  let metadata = self.build_metadata(messages[0], sequence_id)
  let payload = if messages.length() > 1 {
    metadata.num_messages_in_batch = Some(messages.length())
    encode_batch_payload(messages)
  } else {
    messages[0].payload
  }
  let payload = if self.compression is None {
    payload
  } else {
    metadata.compression = self.compression.to_proto()
    metadata.uncompressed_size = Some(payload.length().reinterpret_as_uint())
    compress(self.compression, payload)
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SEND
  let send = @proto.CommandSend::default()
  send.producer_id = self.producer_id
  send.sequence_id = sequence_id
  if messages.length() > 1 {
    send.num_messages = Some(messages.length())
  }
  match txn {
    Some(t) => {
      let (most, least) = t.id()
      send.txnid_least_bits = Some(least)
      send.txnid_most_bits = Some(most)
      metadata.txnid_least_bits = Some(least)
      metadata.txnid_most_bits = Some(most)
    }
    None => {
      // proto2 default 0 would make the broker treat the message as
      // transactional and reject it when transactions are disabled
      send.txnid_least_bits = None
      send.txnid_most_bits = None
      metadata.txnid_least_bits = None
      metadata.txnid_most_bits = None
    }
  }
  cmd.send = Some(send)
  let frame = encode_send_frame(cmd, metadata, payload)
  if self.conn.max_message_size() is Some(max_size) && frame.length() > max_size {
    raise InvalidFrame("message exceeds broker max size; enable chunking")
  }
  self.send_encoded(sequence_id, frame)
}

///|
/// Send a message as part of a transaction.
pub async fn SingleProducer::send_txn(
  self : SingleProducer,
  txn : Transaction,
  msg : ProducerMessage,
) -> SendReceipt {
  if self.closed {
    raise ProducerClosed
  }
  let slots = self.pending_slots
  if slots is Some(slots) {
    if self.block_if_queue_full {
      slots.get() catch {
        _ => raise ProducerClosed
      }
    } else if slots.try_get() is None {
      raise ProducerQueueFull
    }
  }
  let size = msg.payload.length()
  if self.memory_limiter is Some(limiter) {
    limiter.acquire(size, self.block_if_queue_full) catch {
      e => {
        if slots is Some(queue) {
          ignore(queue.try_put(()) catch { _ => false })
        }
        raise e
      }
    }
  }
  if self.closed {
    if self.memory_limiter is Some(limiter) {
      limiter.release(size)
    }
    if slots is Some(queue) {
      ignore(queue.try_put(()) catch { _ => false })
    }
    raise ProducerClosed
  }
  let result : Result[SendReceipt, Error] = Ok(
    if self.chunk_size is Some(_) || self.automatic_chunking {
      self.send_chunked(msg, txn~)
    } else {
      self.send_messages([msg], txn~).get()
    },
  ) catch {
    e => Err(e)
  }
  if self.memory_limiter is Some(limiter) {
    limiter.release(size)
  }
  if slots is Some(queue) {
    ignore(queue.try_put(()) catch { _ => false })
  }
  match result {
    Ok(receipt) => receipt
    Err(e) => raise e
  }
}

///|
/// Encode messages as a batch payload: repeated
/// [meta_size u32][SingleMessageMetadata][payload_size u32][payload].
fn encode_batch_payload(messages : Array[ProducerMessage]) -> Bytes raise {
  let buf = @buffer.Buffer()
  for msg in messages {
    let single = @proto.SingleMessageMetadata::default()
    single.payload_size = msg.payload.length()
    single.properties = key_values_of(msg.properties)
    if msg.key is Some(key) {
      single.partition_key = Some(key)
    }
    if msg.event_time is Some(t) {
      single.event_time = Some(t)
    }
    if msg.sequence_id is Some(s) {
      single.sequence_id = Some(s)
    }
    if msg.ordering_key is Some(k) {
      single.ordering_key = Some(k)
    }
    let meta_buf = @buffer.Buffer()
    write_proto(meta_buf, single)
    let meta_bytes = meta_buf.to_bytes()
    write_be32(buf, meta_bytes.length().reinterpret_as_uint())
    buf.write_bytes(meta_bytes)
    write_be32(buf, msg.payload.length().reinterpret_as_uint())
    buf.write_bytes(msg.payload)
  }
  buf.to_bytes()
}

///|
async fn SingleProducer::enqueue_batch(
  self : SingleProducer,
  msg : ProducerMessage,
) -> ReceiptFuture {
  let waiter : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Blocking(1),
  )
  let done : Ref[Bool] = { val: false, }
  self.batch_mu.acquire()
  if self.closed {
    self.batch_mu.release()
    raise ProducerClosed
  }
  self.next_batch_token += 1UL
  let entry : BufferedMessage = {
    message: msg,
    waiter,
    token: self.next_batch_token,
    done,
  }
  self.batch_entries.push(entry)
  self.batch_bytes += msg.payload.length()
  guard self.batching is Some(config) else {
    self.batch_mu.release()
    raise ProducerClosed
  }
  let full = self.batch_entries.length() >= config.max_messages ||
    self.batch_bytes >= config.max_bytes
  if !full && !self.batch_timer_running {
    self.batch_timer_running = true
    self.batch_timer_generation += 1UL
    let generation = self.batch_timer_generation
    let delay = config.max_delay_ms
    self.group.spawn_bg(
      async fn() { self.flush_after(delay, generation) },
      no_wait=true,
      allow_failure=true,
    )
  }
  self.batch_mu.release()
  if self.send_timeout_ms is Some(timeout) {
    self.group.spawn_bg(
      async fn() { self.expire_buffered(entry, timeout) },
      no_wait=true,
      allow_failure=true,
    )
  }
  if full {
    self.flush()
  }
  { reply: waiter, }
}

///|
async fn SingleProducer::expire_buffered(
  self : SingleProducer,
  entry : BufferedMessage,
  timeout_ms : Int,
) -> Unit {
  @async.sleep(timeout_ms)
  self.batch_mu.acquire()
  defer self.batch_mu.release()
  if entry.done.val {
    return
  }
  for index in 0..<self.batch_entries.length() {
    if self.batch_entries[index].token == entry.token {
      ignore(self.batch_entries.remove(index))
      self.batch_bytes -= entry.message.payload.length()
      if self.batch_entries.is_empty() {
        self.batch_timer_running = false
        self.batch_timer_generation += 1UL
      }
      break
    }
  }
  entry.done.val = true
  entry.waiter.close(error=RequestTimeout, clear=true)
}

///|
async fn SingleProducer::flush_after(
  self : SingleProducer,
  delay_ms : Int,
  generation : UInt64,
) -> Unit {
  @async.sleep(delay_ms)
  self.flush_with_generation(expected_generation=generation) catch {
    _ => ()
  }
}

///|
/// Flush any buffered batch immediately.
pub async fn SingleProducer::flush(self : SingleProducer) -> Unit {
  self.flush_with_generation()
}

///|
async fn SingleProducer::flush_with_generation(
  self : SingleProducer,
  expected_generation? : UInt64,
) -> Unit {
  self.batch_mu.acquire()
  defer self.batch_mu.release()
  if expected_generation is Some(generation) &&
    generation != self.batch_timer_generation {
    return
  }
  self.batch_timer_running = false
  self.batch_timer_generation += 1UL
  if self.batch_entries.is_empty() {
    return
  }
  let entries = self.batch_entries
  let messages = entries.map(fn(entry) { entry.message })
  self.batch_entries = []
  self.batch_bytes = 0
  errdefer (for entry in entries {
    entry.done.val = true
    entry.waiter.close(error=ConnectionClosed, clear=true)
  })
  let batch_waiter = self.send_messages(messages)
  // forward the batch receipt to every message waiter
  let n = messages.length()
  self.group.spawn_bg(
    async fn() {
      let receipt : SendReceipt = batch_waiter.get() catch {
        e => {
          for entry in entries {
            if !entry.done.val {
              entry.done.val = true
              entry.waiter.close(error=e, clear=true)
            }
          }
          return
        }
      }
      for i, entry in entries {
        if entry.done.val {
          continue
        }
        entry.done.val = true
        let message_id : MessageId = {
          ..receipt.message_id,
          batch_index: if n > 1 {
            i
          } else {
            receipt.message_id.batch_index
          },
        }
        ignore(entry.waiter.try_put({ ..receipt, message_id, }))
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
/// Close the producer, flushing any buffered batch first.
pub async fn SingleProducer::close(self : SingleProducer) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  if self.pending_slots is Some(slots) {
    slots.close(error=ProducerClosed, clear=true)
  }
  self.flush() catch {
    _ => ()
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::CLOSE_PRODUCER
  let close = @proto.CommandCloseProducer::default()
  close.producer_id = self.producer_id
  close.request_id = self.conn.next_request_id()
  cmd.close_producer = Some(close)
  ignore(self.conn.request(cmd) catch { _ => @proto.BaseCommand::default() })
  self.conn.producer_handlers.remove(self.producer_id)
  self.fail_all(ProducerClosed)
}

///|
/// The broker-assigned producer name.
pub fn SingleProducer::name(self : SingleProducer) -> String {
  self.name
}

///|
/// The topic this producer publishes to.
pub fn SingleProducer::topic(self : SingleProducer) -> String {
  self.topic
}

///|
/// Highest sequence id confirmed by this producer or reported by the broker
/// when it registered. `None` means that no sequence has been confirmed.
pub fn SingleProducer::last_sequence_id(self : SingleProducer) -> UInt64? {
  self.last_confirmed_sequence_id
}

///|
priv enum ProducerImpl {
  Single(SingleProducer)
  Partitioned(Array[Producer], Ref[Int], ProducerRoutingMode)
}

///|
/// A producer for a topic, partitioned or not. Partitioned producers route
/// keyed messages by `xxhash32(key) % partitions` and keyless messages
/// round-robin.
pub struct Producer {
  priv inner : ProducerImpl
  priv closed : Ref[Bool]
  priv mut interceptors : Array[ProducerInterceptor]
}

///|
fn Producer::single(p : SingleProducer) -> Producer {
  { inner: ProducerImpl::Single(p), closed: { val: false, }, interceptors: [], }
}

///|
/// Create a producer on a partitioned topic from one producer per partition.
pub fn Producer::partitioned(
  partitions : Array[Producer],
  routing? : ProducerRoutingMode = KeyHash,
) -> Producer {
  {
    inner: ProducerImpl::Partitioned(partitions, { val: 0, }, routing),
    closed: { val: false, },
    interceptors: [],
  }
}

///|
fn Producer::set_interceptors(
  self : Producer,
  interceptors : Array[ProducerInterceptor],
) -> Unit {
  self.interceptors = interceptors
}

///|
fn Producer::before_send_message(
  self : Producer,
  message : ProducerMessage,
) -> ProducerMessage {
  let mut current = message
  for interceptor in self.interceptors {
    if interceptor.before_send is Some(callback) {
      current = callback(current)
    }
  }
  current
}

///|
fn Producer::on_send_result(
  self : Producer,
  message : ProducerMessage,
  result : Result[SendReceipt, Error],
) -> Unit {
  for interceptor in self.interceptors {
    if interceptor.on_send_result is Some(callback) {
      callback(message, result)
    }
  }
}

///|
fn Producer::has_result_interceptor(self : Producer) -> Bool {
  for interceptor in self.interceptors {
    if interceptor.on_send_result is Some(_) {
      return true
    }
  }
  false
}

///|
async fn Producer::add_partition(self : Producer, partition : Producer) -> Unit {
  if self.closed.val {
    partition.close()
    return
  }
  match self.inner {
    ProducerImpl::Single(_) => raise InvalidFrame("not a partitioned producer")
    ProducerImpl::Partitioned(parts, _, _) => parts.push(partition)
  }
}

///|
/// Number of producer partitions currently available for routing.
pub fn Producer::partition_count(self : Producer) -> Int {
  match self.inner {
    ProducerImpl::Single(_) => 1
    ProducerImpl::Partitioned(parts, _, _) => parts.length()
  }
}

///|
pub fn Producer::is_closed(self : Producer) -> Bool {
  if self.closed.val {
    return true
  }
  match self.inner {
    ProducerImpl::Single(p) => p.closed
    ProducerImpl::Partitioned(partitions, _, _) => {
      for partition in partitions {
        if !partition.is_closed() {
          return false
        }
      }
      true
    }
  }
}

///|
fn Producer::route(
  self : Producer,
  msg : ProducerMessage,
) -> Producer raise PulsarError {
  guard self.inner is ProducerImpl::Partitioned(partitions, rr, routing) else {
    return self
  }
  let n = partitions.length()
  guard n > 0 else {
    raise InvalidFrame("partitioned producer has no partitions")
  }
  let index = match routing {
    KeyHash =>
      match msg.key {
        Some(key) =>
          (xxhash32(@utf8.encode(key)).reinterpret_as_int() & 0x7fffffff) % n
        None => {
          let index = rr.val % n
          rr.val += 1
          index
        }
      }
    RoundRobin => {
      let index = rr.val % n
      rr.val += 1
      index
    }
    SinglePartition => 0
    Custom(router) => router(msg, n)
  }
  guard index >= 0 && index < n else {
    raise InvalidFrame("producer router returned an invalid partition")
  }
  partitions[index]
}

///|
/// Send a message and wait for the broker's receipt.
pub async fn Producer::send(
  self : Producer,
  msg : ProducerMessage,
) -> SendReceipt {
  self.send_async(msg).wait()
}

///|
async fn Producer::send_async_raw(
  self : Producer,
  msg : ProducerMessage,
) -> (ReceiptFuture, @async.TaskGroup[Unit]) {
  match self.inner {
    ProducerImpl::Single(p) => (p.send_async(msg), p.group)
    ProducerImpl::Partitioned(_, _, _) => self.route(msg).send_async_raw(msg)
  }
}

///|
/// Send a message without waiting for the receipt.
pub async fn Producer::send_async(
  self : Producer,
  msg : ProducerMessage,
) -> ReceiptFuture {
  let message = self.before_send_message(msg)
  let (inner, group) = self.send_async_raw(message) catch {
    e => {
      self.on_send_result(message, Err(e))
      raise e
    }
  }
  if !self.has_result_interceptor() {
    return inner
  }
  let reply : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Blocking(1),
  )
  group.spawn_bg(
    async fn() {
      let result : Result[SendReceipt, Error] = Ok(inner.wait()) catch {
        e => Err(e)
      }
      self.on_send_result(message, result)
      match result {
        Ok(receipt) => ignore(reply.try_put(receipt))
        Err(e) => reply.close(error=e, clear=true)
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  { reply, }
}

///|
/// Send a message as part of a transaction.
pub async fn Producer::send_txn(
  self : Producer,
  txn : Transaction,
  msg : ProducerMessage,
) -> SendReceipt {
  let message = self.before_send_message(msg)
  let result : Result[SendReceipt, Error] = Ok(self.send_txn_raw(txn, message)) catch {
    e => Err(e)
  }
  self.on_send_result(message, result)
  match result {
    Ok(receipt) => receipt
    Err(e) => raise e
  }
}

///|
async fn Producer::send_txn_raw(
  self : Producer,
  txn : Transaction,
  msg : ProducerMessage,
) -> SendReceipt {
  match self.inner {
    ProducerImpl::Single(p) => p.send_txn(txn, msg)
    ProducerImpl::Partitioned(_, _, _) => self.route(msg).send_txn_raw(txn, msg)
  }
}

///|
/// Flush buffered batches on every partition.
pub async fn Producer::flush(self : Producer) -> Unit {
  match self.inner {
    ProducerImpl::Single(p) => p.flush()
    ProducerImpl::Partitioned(partitions, _, _) =>
      for p in partitions {
        p.flush()
      }
  }
}

///|
/// Close the producer (all partitions), flushing pending batches first.
pub async fn Producer::close(self : Producer) -> Unit {
  if self.closed.val {
    return
  }
  self.closed.val = true
  match self.inner {
    ProducerImpl::Single(p) => p.close()
    ProducerImpl::Partitioned(partitions, _, _) =>
      for p in partitions {
        p.close()
      }
  }
}

///|
/// The broker-assigned producer name (of the first partition).
pub fn Producer::name(self : Producer) -> String {
  match self.inner {
    ProducerImpl::Single(p) => p.name()
    ProducerImpl::Partitioned(partitions, _, _) => partitions[0].name()
  }
}

///|
/// The topic this producer publishes to.
pub fn Producer::topic(self : Producer) -> String {
  match self.inner {
    ProducerImpl::Single(p) => p.topic()
    ProducerImpl::Partitioned(partitions, _, _) =>
      match partitions[0].topic().rev_find("-partition-") {
        Some(i) => partitions[0].topic()[:i].to_owned()
        None => partitions[0].topic()
      }
  }
}

///|
/// Highest confirmed sequence id across this producer's partitions.
pub fn Producer::last_sequence_id(self : Producer) -> UInt64? {
  match self.inner {
    ProducerImpl::Single(p) => p.last_sequence_id()
    ProducerImpl::Partitioned(partitions, _, _) => {
      let mut highest : UInt64? = None
      for partition in partitions {
        if partition.last_sequence_id() is Some(seq) &&
          (highest is None || highest.unwrap() < seq) {
          highest = Some(seq)
        }
      }
      highest
    }
  }
}
