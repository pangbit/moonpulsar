// SingleReader: a lightweight non-durable cursor over a topic, starting from an
// arbitrary message id. Unlike `Consumer`, a reader does not ack messages
// and can seek freely.

///|
const COMPOSITE_READER_QUEUE_CAPACITY : Int = 1000

///|
priv enum ReaderEvent {
  Msg(Message)
  EndOfTopic(UInt64)
}

///|
fn ReaderEvent::release_retained_memory(self : ReaderEvent) -> Unit {
  if self is Msg(message) {
    message.release_retained_memory()
  }
}

///|
fn ReaderEvent::consumer_id(self : ReaderEvent) -> UInt64 {
  match self {
    Msg(msg) => msg.consumer_id
    EndOfTopic(id) => id
  }
}

///|
/// A reader over a topic starting at a given position.
pub struct SingleReader {
  priv group : @async.TaskGroup[Unit]
  priv conn : Connection
  priv topic : String
  priv sub_name : String
  priv reader_name : String?
  priv properties : Map[String, String]
  priv mut consumer_id : UInt64
  priv events : @aqueue.Queue[ReaderEvent]
  priv conn_ref : Ref[Connection]
  priv receiver_permits : Int
  priv read_compacted : Bool
  priv schema : SchemaInfo?
  priv message_crypto : MessageCrypto?
  priv memory_limiter : ClientMemoryLimiter?
  priv decryption_failure_action : DecryptionFailureAction
  priv include_start_message_id : Bool
  priv start_bound : Ref[MessageId?]
  priv mut available_permits : Int
  /// last message seen, for `has_message_available`
  priv last_seen : Ref[MessageId?]
  priv closed : Ref[Bool]
  priv close_error : Ref[Error?]
  priv chunks : ChunkAssembler
  priv chunk_sweeper_stop : @aqueue.Queue[Unit]
}

///|
/// Create a reader on `topic` through an already-resolved connection,
/// starting after `start_message_id` (default: the latest message).
/// The subscription is non-durable and exclusive.
pub async fn SingleReader::create(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  start_message_id? : MessageId,
  receiver_queue_size? : Int = 1000,
  read_compacted? : Bool = false,
  include_start_message_id? : Bool = false,
  name? : String,
  properties? : Map[String, String] = {},
  subscription_name? : String,
  subscription_role_prefix? : String = "reader",
  chunk_policy? : ChunkAssemblyPolicy,
  schema? : SchemaInfo,
  message_crypto? : MessageCrypto,
  decryption_failure_action? : DecryptionFailureAction = Fail,
) -> SingleReader {
  SingleReader::create_with_memory(
    group,
    conn,
    topic,
    start_message_id?,
    receiver_queue_size~,
    read_compacted~,
    include_start_message_id~,
    name?,
    properties~,
    subscription_name?,
    subscription_role_prefix~,
    chunk_policy?,
    schema?,
    message_crypto?,
    decryption_failure_action~,
  )
}

///|
async fn SingleReader::create_with_memory(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  start_message_id? : MessageId,
  receiver_queue_size? : Int = 1000,
  read_compacted? : Bool = false,
  include_start_message_id? : Bool = false,
  name? : String,
  properties? : Map[String, String] = {},
  subscription_name? : String,
  subscription_role_prefix? : String = "reader",
  chunk_policy? : ChunkAssemblyPolicy,
  schema? : SchemaInfo,
  message_crypto? : MessageCrypto,
  decryption_failure_action? : DecryptionFailureAction = Fail,
  memory_limiter? : ClientMemoryLimiter,
) -> SingleReader {
  guard subscription_name != Some("") && subscription_role_prefix != "" else {
    raise InvalidFrame("reader subscription name or prefix must not be empty")
  }
  let request_id = conn.next_request_id()
  let consumer_id = conn.next_request_id()
  let reader : SingleReader = {
    group,
    conn,
    topic,
    // unique per process and reader: non-durable subscriptions linger
    // briefly on the broker after disconnect, and a name collision would
    // inherit the previous cursor position
    sub_name: subscription_name.unwrap_or(
      subscription_role_prefix +
      "-" +
      consumer_id.to_string() +
      "-" +
      @async.now().to_string(),
    ),
    reader_name: name,
    properties,
    consumer_id,
    events: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
    conn_ref: { val: conn, },
    receiver_permits: receiver_queue_size,
    read_compacted,
    schema,
    message_crypto,
    memory_limiter,
    decryption_failure_action,
    include_start_message_id,
    start_bound: { val: reader_start_bound(start_message_id), },
    available_permits: 0,
    last_seen: { val: start_message_id, },
    closed: { val: false, },
    close_error: { val: None, },
    chunks: ChunkAssembler::new(policy?=chunk_policy, memory_limiter?),
    chunk_sweeper_stop: @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1)),
  }
  conn.message_handlers[consumer_id] = fn(frame) { reader.on_frame(frame) }
  errdefer conn.message_handlers.remove(consumer_id)
  let cmd = reader.subscribe_command(request_id, start_message_id)
  let reply = conn.request(cmd)
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for reader SUBSCRIBE")
  }
  reader.send_flow(receiver_queue_size)
  reader.available_permits = receiver_queue_size
  conn.on_close(fn(error) { reader.on_connection_closed(error) })
  if conn.is_closed() {
    reader.on_connection_closed(None)
    raise ConnectionClosed
  }
  if include_start_message_id && start_message_id == Some(MessageId::latest()) {
    errdefer (reader.close() catch { _ => () })
    let last = reader.last_message_id()
    if last.entry_id != 18446744073709551615 {
      reader.start_bound.val = Some(last)
      reader.last_seen.val = Some(last)
      reader.resubscribe(Some(last))
    }
  }
  group.spawn_bg(
    async fn() {
      while !reader.closed.val {
        let stopped = @async.with_timeout_opt(reader.chunks.policy.expiry_ms, async fn() {
          reader.chunk_sweeper_stop.get()
        })
        if stopped is Some(_) || reader.closed.val {
          return
        }
        reader.chunks.evict_expired()
        reader.ack_evicted_chunks()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  reader
}

///|
fn reader_start_bound(id : MessageId?) -> MessageId? {
  match id {
    Some(value) if value != MessageId::earliest() &&
      value != MessageId::latest() => Some(value)
    _ => None
  }
}

///|
fn SingleReader::should_deliver(self : SingleReader, id : MessageId) -> Bool {
  guard self.start_bound.val is Some(bound) else { return true }
  if message_id_before(bound, id) {
    self.start_bound.val = None
    return true
  }
  self.include_start_message_id && !message_id_before(id, bound)
}

///|
fn SingleReader::discard_before_start(
  self : SingleReader,
  id : MessageId,
) -> Bool {
  if self.should_deliver(id) {
    return false
  }
  self.group.spawn_bg(
    async fn() { self.send_flow(1) catch { _ => () } },
    no_wait=true,
    allow_failure=true,
  )
  true
}

///|
fn SingleReader::stop_chunk_sweeper(self : SingleReader) -> Unit {
  ignore(self.chunk_sweeper_stop.try_put(()) catch { _ => false })
}

///|
fn SingleReader::ack_evicted_chunks(self : SingleReader) -> Unit {
  let ids = self.chunks.take_evicted()
  if ids.is_empty() {
    return
  }
  let conn = self.conn_ref.val
  let consumer_id = self.consumer_id
  self.group.spawn_bg(
    async fn() {
      send_ack(conn, consumer_id, @proto.CommandAck_AckType::Individual, ids) catch {
        _ => ()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
fn SingleReader::on_connection_closed(
  self : SingleReader,
  error : Error?,
) -> Unit {
  if self.closed.val {
    return
  }
  self.closed.val = true
  self.stop_chunk_sweeper()
  let close_error = if error is Some(ClientMemoryFull) {
    ClientMemoryFull
  } else {
    ConnectionClosed
  }
  self.close_error.val = Some(close_error)
  self.conn.message_handlers.remove(self.consumer_id)
  self.chunks.clear()
  self.clear_buffered_events()
  self.events.close(error=close_error, clear=true)
}

///|
fn SingleReader::clear_buffered_events(self : SingleReader) -> Unit {
  while true {
    match (self.events.try_get() catch { _ => None }) {
      Some(event) => event.release_retained_memory()
      None => break
    }
  }
}

///|
fn SingleReader::subscribe_command(
  self : SingleReader,
  request_id : UInt64,
  start_message_id : MessageId?,
) -> @proto.BaseCommand {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SUBSCRIBE
  let subscribe = @proto.CommandSubscribe::default()
  subscribe.topic = self.topic
  subscribe.subscription = self.sub_name
  subscribe.sub_type = @proto.CommandSubscribe_SubType::Exclusive
  subscribe.consumer_id = self.consumer_id
  subscribe.request_id = request_id
  subscribe.durable = Some(false)
  subscribe.consumer_name = self.reader_name
  subscribe.metadata = key_values_of(self.properties)
  match start_message_id {
    Some(id) => subscribe.start_message_id = Some(id.to_proto())
    None =>
      // omitting start_message_id entirely: the broker starts at the
      // subscription's initial position instead
      ()
  }
  subscribe.read_compacted = Some(self.read_compacted)
  if self.schema is Some(schema) {
    subscribe.schema = Some(schema.to_proto())
  }
  subscribe.initial_position = Some(
    match start_message_id {
      Some(id) =>
        if id.ledger_id == 18446744073709551615 &&
          id.entry_id == 18446744073709551615 {
          @proto.CommandSubscribe_InitialPosition::Earliest
        } else {
          @proto.CommandSubscribe_InitialPosition::Latest
        }
      None => @proto.CommandSubscribe_InitialPosition::Latest
    },
  )
  cmd.subscribe = Some(subscribe)
  cmd
}

///|
/// Drop the current subscription and re-subscribe under the same name.
/// After a broker-side seek (or to start at an explicit message id) the
/// broker disconnects consumers anyway; re-subscribing reattaches at the
/// (reset) cursor position.
async fn SingleReader::resubscribe(
  self : SingleReader,
  start_message_id : MessageId?,
) -> Unit {
  self.chunks.clear()
  self.clear_buffered_events()
  let conn = self.conn_ref.val
  conn.message_handlers.remove(self.consumer_id)
  // best-effort close of the old consumer
  let close_cmd = @proto.BaseCommand::default()
  close_cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
  let close = @proto.CommandCloseConsumer::default()
  close.consumer_id = self.consumer_id
  close.request_id = conn.next_request_id()
  close_cmd.close_consumer = Some(close)
  ignore(conn.request(close_cmd) catch { _ => @proto.BaseCommand::default() })
  self.consumer_id = conn.next_request_id()
  conn.message_handlers[self.consumer_id] = fn(frame) { self.on_frame(frame) }
  let reply = conn.request(
    self.subscribe_command(conn.next_request_id(), start_message_id),
  )
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for reader re-SUBSCRIBE")
  }
  self.send_flow(self.receiver_permits)
  self.available_permits = self.receiver_permits
}

///|
async fn SingleReader::send_flow(self : SingleReader, permits : Int) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::FLOW
  let flow = @proto.CommandFlow::default()
  flow.consumer_id = self.consumer_id
  flow.message_permits = permits.reinterpret_as_uint()
  cmd.flow = Some(flow)
  self.conn.send_command(cmd)
}

///|
fn SingleReader::on_frame(self : SingleReader, frame : IncomingFrame) -> Unit {
  match frame.cmd.type_ {
    @proto.BaseCommand_Type::MESSAGE if !self.closed.val =>
      self.dispatch_message(frame) catch {
        ClientMemoryFull => {
          self.chunks.clear()
          self.clear_buffered_events()
          self.events.close(error=ClientMemoryFull, clear=true)
          self.conn.message_handlers.remove(self.consumer_id)
          self.stop_chunk_sweeper()
          self.group.spawn_bg(
            async fn() {
              let cmd = @proto.BaseCommand::default()
              cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
              let close = @proto.CommandCloseConsumer::default()
              close.consumer_id = self.consumer_id
              close.request_id = self.conn.next_request_id()
              cmd.close_consumer = Some(close)
              self.conn.send_command(cmd) catch {
                _ => ()
              }
            },
            no_wait=true,
            allow_failure=true,
          )
        }
        error =>
          if frame.metadata is Some(meta) && !meta.encryption_keys.is_empty() {
            self.clear_buffered_events()
            self.events.close(error~, clear=true)
          }
      }
    @proto.BaseCommand_Type::REACHED_END_OF_TOPIC =>
      if frame.cmd.reached_end_of_topic is Some(end) {
        ignore(
          self.events.try_put(EndOfTopic(end.consumer_id)) catch {
            _ => false
          },
        )
      }
    _ => ()
  }
}

///|
fn SingleReader::dispatch_message(
  self : SingleReader,
  frame : IncomingFrame,
) -> Unit raise {
  guard frame.cmd.message is Some(message) else { return }
  guard frame.metadata is Some(metadata) && frame.payload is Some(raw_payload) else {
    raise InvalidFrame("MESSAGE without payload")
  }
  let base_id = MessageId::from_proto(message.message_id)
  let chunk = self.chunks.accept(metadata, base_id, raw_payload)
  self.ack_evicted_chunks()
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
            self.group.spawn_bg(
              async fn() { self.send_flow(1) catch { _ => () } },
              no_wait=true,
              allow_failure=true,
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
    let singles = split_batch_payload(payload, batch_size)
    for i, single in singles {
      let (meta, body) = single
      let msg = build_message(
        self.conn_ref,
        self.consumer_id,
        self.topic,
        base_id,
        i,
        metadata,
        Some(meta),
        body,
        redelivery_count,
      )
      if !self.discard_before_start(msg.id()) {
        msg.retain_for_queue(self.memory_limiter)
        if !(self.events.try_put(Msg(msg)) catch { _ => false }) {
          msg.release_retained_memory()
        }
      }
    }
  } else {
    let chunk_ids = if chunk.is_chunk { Some(chunk.ids) } else { None }
    let msg = build_message(
      self.conn_ref,
      self.consumer_id,
      self.topic,
      base_id,
      -1,
      metadata,
      None,
      payload,
      redelivery_count,
      chunk_ids?,
      decryption_failed~,
    )
    if !self.discard_before_start(msg.id()) {
      msg.retain_for_queue(self.memory_limiter)
      if !(self.events.try_put(Msg(msg)) catch { _ => false }) {
        msg.release_retained_memory()
      }
    }
  }
}

///|
/// Read the next message, or `None` once the current end of the topic is
/// reached. Messages published later can still be read by calling `next`
/// again.
pub async fn SingleReader::next(self : SingleReader) -> Message? {
  match self.next_event() {
    Msg(msg) => {
      msg.release_retained_memory()
      Some(msg)
    }
    EndOfTopic(_) => None
  }
}

///|
async fn SingleReader::next_event(self : SingleReader) -> ReaderEvent {
  if self.closed.val {
    if self.close_error.val is Some(error) {
      raise error
    }
    raise ConsumerClosed
  }
  let event = self.events.get()
  match event {
    Msg(msg) => {
      if self.start_bound.val is Some(bound) && msg.id() == bound {
        self.start_bound.val = None
      }
      self.last_seen.val = Some(msg.id())
      self.available_permits -= 1
      let threshold = self.receiver_permits / 2
      if self.available_permits <= threshold {
        let refill = self.receiver_permits - self.available_permits
        self.send_flow(refill) catch {
          _ => ()
        }
        self.available_permits += refill
      }
      event
    }
    EndOfTopic(_) => event
  }
}

///|
/// True when there are buffered messages or the broker reports unseen
/// messages beyond the last one read. Query failures are raised.
pub async fn SingleReader::has_message_available(self : SingleReader) -> Bool {
  if self.closed.val {
    if self.close_error.val is Some(error) {
      raise error
    }
    raise ConsumerClosed
  }
  if self.events.try_get() is Some(event) {
    // put it back conceptually: peeking is not supported by the queue,
    // so push the event back through a fresh front slot
    self.requeue_front(event)
    return true
  }
  let last = self.last_message_id()
  if last.ledger_id == 18446744073709551615 &&
    last.entry_id == 18446744073709551615 {
    return false
  }
  match self.last_seen.val {
    Some(seen) =>
      // the "earliest" wire marker is (max, max): always before anything
      if seen.ledger_id == 18446744073709551615 &&
        seen.entry_id == 18446744073709551615 {
        true
      } else if self.include_start_message_id &&
        self.start_bound.val is Some(bound) &&
        seen == bound &&
        last == bound {
        true
      } else {
        message_id_before(seen, last)
      }
    None => true
  }
}

///|
/// Return the broker's last message ID for this single-topic reader.
pub async fn SingleReader::last_message_id(self : SingleReader) -> MessageId {
  if self.closed.val {
    if self.close_error.val is Some(error) {
      raise error
    }
    raise ConsumerClosed
  }
  query_last_message_id(self.conn, self.consumer_id)
}

///|
async fn query_last_message_id(
  conn : Connection,
  consumer_id : UInt64,
) -> MessageId {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID
  let req = @proto.CommandGetLastMessageId::default()
  req.consumer_id = consumer_id
  req.request_id = conn.next_request_id()
  cmd.get_last_message_id = Some(req)
  let reply = conn.request(cmd)
  guard reply.get_last_message_id_response is Some(resp) else {
    raise UnexpectedResponse("expected GET_LAST_MESSAGE_ID_RESPONSE")
  }
  MessageId::from_proto(resp.last_message_id)
}

///|
fn message_id_before(a : MessageId, b : MessageId) -> Bool {
  if (a.ledger_id, a.entry_id) != (b.ledger_id, b.entry_id) {
    (a.ledger_id, a.entry_id) < (b.ledger_id, b.entry_id)
  } else {
    a.batch_index < b.batch_index
  }
}

///|
/// Re-insert an event taken by `try_get` at the front of the queue.
fn SingleReader::requeue_front(
  self : SingleReader,
  event : ReaderEvent,
) -> Unit raise {
  let rest = []
  while true {
    match self.events.try_get() {
      Some(e) => rest.push(e)
      None => break
    }
  }
  if !(self.events.try_put(event) catch { _ => false }) {
    event.release_retained_memory()
  }
  for e in rest {
    if !(self.events.try_put(e) catch { _ => false }) {
      e.release_retained_memory()
    }
  }
}

///|
/// A position a reader can seek to.
pub(all) enum SeekPosition {
  PositionId(MessageId)
  PositionTimestamp(UInt64)
}

///|
/// Reposition the reader. Buffered messages are dropped.
///
/// PositionId seeks re-subscribe starting at the given message id.
/// Timestamp seeks issue a broker-side cursor reset; the broker
/// disconnects the consumer during the reset, so the reader re-subscribes
/// to the (reset) cursor afterwards.
pub async fn SingleReader::seek(
  self : SingleReader,
  position : SeekPosition,
) -> Unit {
  if self.closed.val {
    raise ConsumerClosed
  }
  // drop everything buffered before the seek
  self.clear_buffered_events()
  match position {
    PositionId(id) => {
      self.last_seen.val = Some(id)
      self.start_bound.val = reader_start_bound(Some(id))
      self.resubscribe(Some(id))
    }
    PositionTimestamp(ts) => {
      let cmd = @proto.BaseCommand::default()
      cmd.type_ = @proto.BaseCommand_Type::SEEK
      let seek = @proto.CommandSeek::default()
      seek.consumer_id = self.consumer_id
      seek.request_id = self.conn.next_request_id()
      seek.message_publish_time = Some(ts)
      cmd.seek = Some(seek)
      let reply = self.conn.request(cmd)
      guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
        raise UnexpectedResponse("expected SUCCESS for SEEK")
      }
      // the broker disconnected us during the cursor reset; reattach
      self.last_seen.val = None
      self.start_bound.val = None
      self.resubscribe(None)
    }
  }
}

///|
/// Close the reader.
pub async fn SingleReader::close(self : SingleReader) -> Unit {
  if self.closed.val {
    return
  }
  self.closed.val = true
  self.clear_buffered_events()
  self.chunks.clear()
  self.stop_chunk_sweeper()
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::CLOSE_CONSUMER
  let close = @proto.CommandCloseConsumer::default()
  close.consumer_id = self.consumer_id
  close.request_id = self.conn.next_request_id()
  cmd.close_consumer = Some(close)
  ignore(self.conn.request(cmd) catch { _ => @proto.BaseCommand::default() })
  self.conn.message_handlers.remove(self.consumer_id)
  self.events.close(error=ConsumerClosed, clear=true)
}

///|
priv enum ReaderImpl {
  One(SingleReader)
  Many(PartitionedReaderState)
}

///|
priv struct PartitionedReaderEvent {
  source : Int
  event : ReaderEvent
}

///|
priv struct PartitionedReaderState {
  queue : @aqueue.Queue[PartitionedReaderEvent]
  mut ended : Int
  ended_sources : Map[Int, Bool]
  mut total : Int
  readers : Array[SingleReader]
  closed : Ref[Bool]
  mut peeked : PartitionedReaderEvent?
}

///|
fn PartitionedReaderState::clear_buffered_events(
  self : PartitionedReaderState,
) -> Unit {
  if self.peeked is Some(item) {
    item.event.release_retained_memory()
    self.peeked = None
  }
  while true {
    match (self.queue.try_get() catch { _ => None }) {
      Some(item) => item.event.release_retained_memory()
      None => break
    }
  }
}

///|
/// A reader over a topic, partitioned or not.
pub struct Reader {
  priv inner : ReaderImpl
}

///|
fn Reader::one(r : SingleReader) -> Reader {
  { inner: ReaderImpl::One(r), }
}

///|
/// Aggregate one reader per partition. The merged stream ends
/// (`next` returns `None`) once every partition reached its end.
pub fn Reader::partitioned(
  group : @async.TaskGroup[Unit],
  readers : Array[SingleReader],
) -> Reader {
  let state : PartitionedReaderState = {
    queue: @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(COMPOSITE_READER_QUEUE_CAPACITY),
    ),
    ended: 0,
    ended_sources: {},
    total: readers.length(),
    readers,
    closed: { val: false, },
    peeked: None,
  }
  for source, r in readers {
    state.forward_source(group, source, r)
  }
  { inner: ReaderImpl::Many(state), }
}

///|
fn PartitionedReaderState::forward_source(
  self : PartitionedReaderState,
  group : @async.TaskGroup[Unit],
  source : Int,
  reader : SingleReader,
) -> Unit {
  group.spawn_bg(
    async fn() {
      while true {
        let event = reader.next_event() catch {
          e => {
            self.clear_buffered_events()
            self.queue.close(error=e, clear=true)
            return
          }
        }
        if event.consumer_id() == reader.consumer_id {
          self.queue.put({ source, event, }) catch {
            _ => {
              event.release_retained_memory()
              return
            }
          }
        } else {
          event.release_retained_memory()
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
async fn Reader::add_partition(
  self : Reader,
  group : @async.TaskGroup[Unit],
  reader : SingleReader,
) -> Unit {
  match self.inner {
    ReaderImpl::One(_) => raise InvalidFrame("not a partitioned reader")
    ReaderImpl::Many(state) => {
      if state.closed.val {
        reader.close()
        return
      }
      let source = state.readers.length()
      state.readers.push(reader)
      state.total += 1
      state.forward_source(group, source, reader)
    }
  }
}

///|
pub fn Reader::is_closed(self : Reader) -> Bool {
  match self.inner {
    ReaderImpl::One(r) => r.closed.val
    ReaderImpl::Many(state) => state.closed.val
  }
}

///|
fn PartitionedReaderState::is_current(
  self : PartitionedReaderState,
  item : PartitionedReaderEvent,
) -> Bool {
  self.readers[item.source].consumer_id == item.event.consumer_id()
}

///|
/// Read the next message, or `None` once the end of the topic is reached.
pub async fn Reader::next(self : Reader) -> Message? {
  match self.inner {
    ReaderImpl::One(r) => r.next()
    ReaderImpl::Many(state) =>
      for ;; {
        let item = match state.peeked {
          Some(saved) => {
            state.peeked = None
            saved
          }
          None => state.queue.get()
        }
        if !state.is_current(item) {
          item.event.release_retained_memory()
          continue
        }
        match item.event {
          Msg(msg) => {
            if state.ended_sources.get(item.source) is Some(_) {
              state.ended_sources.remove(item.source)
              state.ended -= 1
            }
            msg.release_retained_memory()
            return Some(msg)
          }
          EndOfTopic(_) => {
            if state.ended_sources.get(item.source) is None {
              state.ended_sources[item.source] = true
              state.ended += 1
            }
            if state.ended == state.total {
              return None
            }
          }
        }
      }
  }
}

///|
/// True when there are buffered messages or the broker reports unseen
/// messages beyond the last one read (any partition). Query failures are raised.
pub async fn Reader::has_message_available(self : Reader) -> Bool {
  match self.inner {
    ReaderImpl::One(r) => r.has_message_available()
    ReaderImpl::Many(state) => {
      if state.peeked is Some(item) && !state.is_current(item) {
        item.event.release_retained_memory()
        state.peeked = None
      }
      while state.peeked is None {
        match state.queue.try_get() {
          Some(item) =>
            if state.is_current(item) {
              state.peeked = Some(item)
            } else {
              item.event.release_retained_memory()
            }
          None => break
        }
      }
      if state.peeked is Some(item) && item.event is Msg(_) {
        return true
      }
      for r in state.readers {
        if r.has_message_available() {
          return true
        }
      }
      false
    }
  }
}

///|
/// Return the last ID for a single-topic reader. A partitioned reader has
/// multiple independent last IDs and must be queried per partition.
pub async fn Reader::last_message_id(self : Reader) -> MessageId {
  match self.inner {
    ReaderImpl::One(r) => r.last_message_id()
    ReaderImpl::Many(_) =>
      raise InvalidFrame("partitioned reader has no single last message ID")
  }
}

///|
/// Return the broker's last message ID for each topic this reader follows.
/// Partitioned readers report each partition topic separately.
pub async fn Reader::last_message_ids(self : Reader) -> Map[String, MessageId] {
  let result : Map[String, MessageId] = Map([])
  match self.inner {
    ReaderImpl::One(r) => result[r.topic] = r.last_message_id()
    ReaderImpl::Many(state) => {
      guard !state.closed.val else { raise ConsumerClosed }
      for r in state.readers {
        result[r.topic] = r.last_message_id()
      }
    }
  }
  result
}

///|
/// Reposition all partition readers.
pub async fn Reader::seek(self : Reader, position : SeekPosition) -> Unit {
  match self.inner {
    ReaderImpl::One(r) => r.seek(position)
    ReaderImpl::Many(state) => {
      for r in state.readers {
        r.seek(position)
      }
      state.ended = 0
      state.ended_sources.clear()
      state.clear_buffered_events()
    }
  }
}

///|
/// Close the reader (all partitions).
pub async fn Reader::close(self : Reader) -> Unit {
  match self.inner {
    ReaderImpl::One(r) => r.close()
    ReaderImpl::Many(state) => {
      state.closed.val = true
      state.clear_buffered_events()
      state.queue.close(error=ConsumerClosed, clear=true)
      for r in state.readers {
        r.close()
      }
    }
  }
}
