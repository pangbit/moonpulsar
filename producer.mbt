// SingleProducer: publishes messages to a topic with optional batching.
//
// Reconnection: when the connection breaks and the producer was created
// with a `reconnect_with` provider (which `Client::create_producer` always
// passes), the producer re-registers itself on a fresh connection and
// replays every message that has not received a receipt yet.

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
/// A producer bound to a single topic.
pub struct SingleProducer {
  priv group : @async.TaskGroup[Unit]
  priv mut conn : Connection
  priv topic : String
  priv producer_id : UInt64
  priv requested_name : String?
  priv mut name : String
  priv mut sequence_id : UInt64
  priv pending : Map[UInt64, @aqueue.Queue[SendReceipt]]
  /// encoded SEND frames awaiting a receipt, for replay after reconnect
  priv payloads : Map[UInt64, Bytes]
  priv reconnect_with : (async () -> Connection)?
  priv reconnecting : Ref[Bool]
  priv compression : CompressionCodec
  priv batching : BatchingConfig?
  priv batch_mu : @async.Mutex
  priv mut batch_messages : Array[ProducerMessage]
  priv mut batch_waiters : Array[@aqueue.Queue[SendReceipt]]
  priv mut batch_bytes : Int
  priv mut batch_timer_running : Bool
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
  batching? : BatchingConfig,
  reconnect_with? : async () -> Connection,
  compression? : CompressionCodec = None,
) -> SingleProducer {
  let request_id = conn.next_request_id()
  let producer_id = conn.next_request_id()
  let producer : SingleProducer = {
    group,
    conn,
    topic,
    producer_id,
    requested_name: name,
    name: name.unwrap_or(""),
    sequence_id: 0,
    pending: {},
    payloads: {},
    reconnect_with,
    reconnecting: { val: false, },
    compression,
    batching,
    batch_mu: @async.Mutex::Mutex(),
    batch_messages: [],
    batch_waiters: [],
    batch_bytes: 0,
    batch_timer_running: false,
    closed: false,
  }
  producer.attach(conn)
  errdefer conn.producer_handlers.remove(producer_id)
  let reply = conn.request(producer.producer_command(request_id))
  guard reply.producer_success is Some(success) else {
    raise UnexpectedResponse("expected PRODUCER_SUCCESS")
  }
  producer.name = success.producer_name
  if success.last_sequence_id is Some(last) && last >= 0 {
    producer.sequence_id = (last + 1).reinterpret_as_uint64()
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
  produce.producer_name = self.requested_name
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
      self.pending.remove(receipt.sequence_id)
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
  let policy = RetryPolicy::default()
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
    let reply = conn.request(self.producer_command(request_id))
    guard reply.producer_success is Some(_) else {
      raise UnexpectedResponse("expected PRODUCER_SUCCESS")
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
  @async.retry(method, fatal_error=fn(e) { e is ProducerClosed }, attempt) catch {
    _ => self.fail_all(ConnectionClosed)
  }
  self.reconnecting.val = false
}

///|
fn SingleProducer::fail_all(self : SingleProducer, error : PulsarError) -> Unit {
  for _, waiter in self.pending {
    waiter.close(error~, clear=true)
  }
  self.pending.clear()
  self.payloads.clear()
  for waiter in self.batch_waiters {
    waiter.close(error~, clear=true)
  }
  self.batch_waiters.clear()
  self.batch_messages.clear()
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
  if self.batching is Some(_) {
    return self.enqueue_batch(msg)
  }
  let waiter = self.send_messages([msg])
  { reply: waiter, }
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
  let waiter : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Blocking(1),
  )
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
  if txn is Some(t) {
    let (most, least) = t.id()
    send.txnid_least_bits = Some(least)
    send.txnid_most_bits = Some(most)
    metadata.txnid_least_bits = Some(least)
    metadata.txnid_most_bits = Some(most)
  }
  cmd.send = Some(send)
  let frame = encode_send_frame(cmd, metadata, payload)
  self.pending[sequence_id] = waiter
  self.payloads[sequence_id] = frame
  // while reconnecting the frame stays queued for replay; a write failure
  // is handled by the connection close handler which fails or replays
  // every pending waiter
  if !self.reconnecting.val {
    self.conn.send_bytes(frame)
  }
  waiter
}

///|
/// Send a message as part of a transaction.
pub async fn SingleProducer::send_txn(
  self : SingleProducer,
  txn : Transaction,
  msg : ProducerMessage,
) -> SendReceipt {
  self.send_messages([msg], txn~).get()
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
  self.batch_mu.acquire()
  self.batch_messages.push(msg)
  self.batch_waiters.push(waiter)
  self.batch_bytes += msg.payload.length()
  guard self.batching is Some(config) else {
    self.batch_mu.release()
    raise ProducerClosed
  }
  let full = self.batch_messages.length() >= config.max_messages ||
    self.batch_bytes >= config.max_bytes
  if !full && !self.batch_timer_running {
    self.batch_timer_running = true
    let delay = config.max_delay_ms
    self.group.spawn_bg(
      async fn() { self.flush_after(delay) },
      no_wait=true,
      allow_failure=true,
    )
  }
  self.batch_mu.release()
  if full {
    self.flush()
  }
  { reply: waiter, }
}

///|
async fn SingleProducer::flush_after(
  self : SingleProducer,
  delay_ms : Int,
) -> Unit {
  @async.sleep(delay_ms)
  self.flush() catch {
    _ => ()
  }
}

///|
/// Flush any buffered batch immediately.
pub async fn SingleProducer::flush(self : SingleProducer) -> Unit {
  self.batch_mu.acquire()
  defer self.batch_mu.release()
  self.batch_timer_running = false
  if self.batch_messages.is_empty() {
    return
  }
  let messages = self.batch_messages
  let waiters = self.batch_waiters
  self.batch_messages = []
  self.batch_waiters = []
  self.batch_bytes = 0
  errdefer (for waiter in waiters {
    waiter.close(error=ConnectionClosed, clear=true)
  })
  let batch_waiter = self.send_messages(messages)
  // forward the batch receipt to every message waiter
  let n = messages.length()
  self.group.spawn_bg(
    async fn() {
      errdefer (for waiter in waiters {
        waiter.close(error=ConnectionClosed, clear=true)
      })
      let receipt : SendReceipt = batch_waiter.get()
      for i, waiter in waiters {
        let message_id : MessageId = {
          ..receipt.message_id,
          batch_index: if n > 1 {
            i
          } else {
            receipt.message_id.batch_index
          },
        }
        ignore(waiter.try_put({ ..receipt, message_id, }))
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
priv enum ProducerImpl {
  Single(SingleProducer)
  Partitioned(Array[Producer], Ref[Int])
}

///|
/// A producer for a topic, partitioned or not. Partitioned producers route
/// keyed messages by `xxhash32(key) % partitions` and keyless messages
/// round-robin.
pub struct Producer {
  priv inner : ProducerImpl
}

///|
fn Producer::single(p : SingleProducer) -> Producer {
  { inner: ProducerImpl::Single(p), }
}

///|
/// Create a producer on a partitioned topic from one producer per partition.
pub fn Producer::partitioned(partitions : Array[Producer]) -> Producer {
  { inner: ProducerImpl::Partitioned(partitions, { val: 0, }), }
}

///|
fn Producer::route(self : Producer, msg : ProducerMessage) -> Producer {
  guard self.inner is ProducerImpl::Partitioned(partitions, rr) else {
    return self
  }
  let n = partitions.length()
  match msg.key {
    Some(key) => {
      let h = xxhash32(@utf8.encode(key)).reinterpret_as_int() & 0x7fffffff
      partitions[h % n]
    }
    None => {
      let i = rr.val % n
      rr.val += 1
      partitions[i]
    }
  }
}

///|
/// Send a message and wait for the broker's receipt.
pub async fn Producer::send(
  self : Producer,
  msg : ProducerMessage,
) -> SendReceipt {
  match self.inner {
    ProducerImpl::Single(p) => p.send(msg)
    ProducerImpl::Partitioned(_, _) => self.route(msg).send(msg)
  }
}

///|
/// Send a message without waiting for the receipt.
pub async fn Producer::send_async(
  self : Producer,
  msg : ProducerMessage,
) -> ReceiptFuture {
  match self.inner {
    ProducerImpl::Single(p) => p.send_async(msg)
    ProducerImpl::Partitioned(_, _) => self.route(msg).send_async(msg)
  }
}

///|
/// Send a message as part of a transaction.
pub async fn Producer::send_txn(
  self : Producer,
  txn : Transaction,
  msg : ProducerMessage,
) -> SendReceipt {
  match self.inner {
    ProducerImpl::Single(p) => p.send_txn(txn, msg)
    ProducerImpl::Partitioned(_, _) => self.route(msg).send_txn(txn, msg)
  }
}

///|
/// Flush buffered batches on every partition.
pub async fn Producer::flush(self : Producer) -> Unit {
  match self.inner {
    ProducerImpl::Single(p) => p.flush()
    ProducerImpl::Partitioned(partitions, _) =>
      for p in partitions {
        p.flush()
      }
  }
}

///|
/// Close the producer (all partitions), flushing pending batches first.
pub async fn Producer::close(self : Producer) -> Unit {
  match self.inner {
    ProducerImpl::Single(p) => p.close()
    ProducerImpl::Partitioned(partitions, _) =>
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
    ProducerImpl::Partitioned(partitions, _) => partitions[0].name()
  }
}

///|
/// The topic this producer publishes to.
pub fn Producer::topic(self : Producer) -> String {
  match self.inner {
    ProducerImpl::Single(p) => p.topic()
    ProducerImpl::Partitioned(partitions, _) =>
      match partitions[0].topic().rev_find("-partition-") {
        Some(i) => partitions[0].topic()[:i].to_owned()
        None => partitions[0].topic()
      }
  }
}
