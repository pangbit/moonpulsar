// Producer: publishes messages to a topic with optional batching.

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
/// A future receipt for a message sent with `Producer::send_async`.
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
pub struct Producer {
  priv group : @async.TaskGroup[Unit]
  priv conn : Connection
  priv topic : String
  priv producer_id : UInt64
  priv mut name : String
  priv mut sequence_id : UInt64
  priv pending : Map[UInt64, @aqueue.Queue[SendReceipt]]
  priv batching : BatchingConfig?
  priv batch_mu : @async.Mutex
  priv mut batch_messages : Array[ProducerMessage]
  priv mut batch_waiters : Array[@aqueue.Queue[SendReceipt]]
  priv mut batch_bytes : Int
  priv mut batch_timer_running : Bool
  priv mut closed : Bool
}

///|
fn Producer::next_sequence_id(self : Producer) -> UInt64 {
  let seq = self.sequence_id
  self.sequence_id += 1
  seq
}

///|
/// Create a producer on `topic` through an already-resolved connection.
pub async fn Producer::create(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  name? : String,
  batching? : BatchingConfig,
) -> Producer {
  let request_id = conn.next_request_id()
  let producer_id = conn.next_request_id()
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::PRODUCER
  let produce = @proto.CommandProducer::default()
  produce.topic = topic
  produce.producer_id = producer_id
  produce.request_id = request_id
  produce.producer_name = name
  cmd.producer = Some(produce)
  let producer : Producer = {
    group,
    conn,
    topic,
    producer_id,
    name: name.unwrap_or(""),
    sequence_id: 0,
    pending: {},
    batching,
    batch_mu: @async.Mutex::Mutex(),
    batch_messages: [],
    batch_waiters: [],
    batch_bytes: 0,
    batch_timer_running: false,
    closed: false,
  }
  conn.producer_handlers[producer_id] = fn(cmd) {
    producer.on_command(cmd) catch {
      _ => ()
    }
  }
  conn.on_close(fn(e) { producer.on_connection_closed(e) })
  errdefer conn.producer_handlers.remove(producer_id)
  let reply = conn.request(cmd)
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
fn Producer::on_command(
  self : Producer,
  cmd : @proto.BaseCommand,
) -> Unit raise {
  if cmd.send_receipt is Some(receipt) {
    if self.pending.get(receipt.sequence_id) is Some(waiter) {
      self.pending.remove(receipt.sequence_id)
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
fn Producer::on_connection_closed(self : Producer, error : Error?) -> Unit {
  ignore(error)
  self.fail_all(ConnectionClosed)
}

///|
fn Producer::fail_all(self : Producer, error : PulsarError) -> Unit {
  for _, waiter in self.pending {
    waiter.close(error~, clear=true)
  }
  self.pending.clear()
  for waiter in self.batch_waiters {
    waiter.close(error~, clear=true)
  }
  self.batch_waiters.clear()
  self.batch_messages.clear()
}

///|
fn Producer::build_metadata(
  self : Producer,
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
pub async fn Producer::send(
  self : Producer,
  msg : ProducerMessage,
) -> SendReceipt {
  self.send_async(msg).wait()
}

///|
/// Send a message without waiting for the receipt; the returned future
/// resolves once the broker confirms.
pub async fn Producer::send_async(
  self : Producer,
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
async fn Producer::send_messages(
  self : Producer,
  messages : Array[ProducerMessage],
) -> @aqueue.Queue[SendReceipt] {
  let sequence_id = self.next_sequence_id()
  let waiter : @aqueue.Queue[SendReceipt] = @aqueue.Queue::Queue(
    kind=@aqueue.Kind::Blocking(1),
  )
  self.pending[sequence_id] = waiter
  let metadata = self.build_metadata(messages[0], sequence_id)
  let payload = if messages.length() > 1 {
    metadata.num_messages_in_batch = Some(messages.length())
    encode_batch_payload(messages)
  } else {
    messages[0].payload
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SEND
  let send = @proto.CommandSend::default()
  send.producer_id = self.producer_id
  send.sequence_id = sequence_id
  if messages.length() > 1 {
    send.num_messages = Some(messages.length())
  }
  cmd.send = Some(send)
  // if the write fails the connection is broken; the close handler fails
  // every pending waiter, so no cleanup is needed here
  self.conn.send_payload_frame(cmd, metadata, payload)
  waiter
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
async fn Producer::enqueue_batch(
  self : Producer,
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
async fn Producer::flush_after(self : Producer, delay_ms : Int) -> Unit {
  @async.sleep(delay_ms)
  self.flush() catch {
    _ => ()
  }
}

///|
/// Flush any buffered batch immediately.
pub async fn Producer::flush(self : Producer) -> Unit {
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
pub async fn Producer::close(self : Producer) -> Unit {
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
pub fn Producer::name(self : Producer) -> String {
  self.name
}

///|
/// The topic this producer publishes to.
pub fn Producer::topic(self : Producer) -> String {
  self.topic
}
