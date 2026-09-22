// Reader: a lightweight non-durable cursor over a topic, starting from an
// arbitrary message id. Unlike `Consumer`, a reader does not ack messages
// and can seek freely.

///|
priv enum ReaderEvent {
  Msg(Message)
  EndOfTopic
}

///|
/// A reader over a topic starting at a given position.
pub struct Reader {
  priv conn : Connection
  priv topic : String
  priv consumer_id : UInt64
  priv events : @aqueue.Queue[ReaderEvent]
  priv conn_ref : Ref[Connection]
  priv receiver_permits : Int
  priv mut available_permits : Int
  /// last message seen, for `has_message_available`
  priv last_seen : Ref[MessageId?]
  priv closed : Ref[Bool]
}

///|
/// Create a reader on `topic` through an already-resolved connection,
/// starting after `start_message_id` (default: the latest message).
/// The subscription is non-durable and exclusive.
pub async fn Reader::create(
  group : @async.TaskGroup[Unit],
  conn : Connection,
  topic : String,
  start_message_id? : MessageId,
  receiver_queue_size? : Int = 1000,
) -> Reader {
  let request_id = conn.next_request_id()
  let consumer_id = conn.next_request_id()
  let reader : Reader = {
    conn,
    topic,
    consumer_id,
    events: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
    conn_ref: { val: conn, },
    receiver_permits: receiver_queue_size,
    available_permits: 0,
    last_seen: { val: start_message_id, },
    closed: { val: false, },
  }
  conn.message_handlers[consumer_id] = fn(frame) { reader.on_frame(frame) }
  errdefer conn.message_handlers.remove(consumer_id)
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SUBSCRIBE
  let subscribe = @proto.CommandSubscribe::default()
  subscribe.topic = topic
  subscribe.subscription = "reader-" + consumer_id.to_string()
  subscribe.sub_type = @proto.CommandSubscribe_SubType::Exclusive
  subscribe.consumer_id = consumer_id
  subscribe.request_id = request_id
  subscribe.durable = Some(false)
  subscribe.start_message_id = Some(
    start_message_id.unwrap_or(MessageId::latest()).to_proto(),
  )
  subscribe.read_compacted = Some(false)
  match start_message_id {
    Some(_) =>
      subscribe.initial_position = Some(
        @proto.CommandSubscribe_InitialPosition::Latest,
      )
    None => ()
  }
  cmd.subscribe = Some(subscribe)
  let reply = conn.request(cmd)
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for reader SUBSCRIBE")
  }
  reader.send_flow(receiver_queue_size)
  reader.available_permits = receiver_queue_size
  reader
}

///|
async fn Reader::send_flow(self : Reader, permits : Int) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::FLOW
  let flow = @proto.CommandFlow::default()
  flow.consumer_id = self.consumer_id
  flow.message_permits = permits.reinterpret_as_uint()
  cmd.flow = Some(flow)
  self.conn.send_command(cmd)
}

///|
fn Reader::on_frame(self : Reader, frame : IncomingFrame) -> Unit {
  match frame.cmd.type_ {
    @proto.BaseCommand_Type::MESSAGE =>
      self.dispatch_message(frame) catch {
        _ => ()
      }
    @proto.BaseCommand_Type::REACHED_END_OF_TOPIC =>
      ignore(self.events.try_put(EndOfTopic) catch { _ => false })
    _ => ()
  }
}

///|
fn Reader::dispatch_message(self : Reader, frame : IncomingFrame) -> Unit raise {
  guard frame.cmd.message is Some(message) else { return }
  guard frame.metadata is Some(metadata) && frame.payload is Some(raw_payload) else {
    raise InvalidFrame("MESSAGE without payload")
  }
  let payload = match metadata.compression {
    Some(t) => decompress(CompressionCodec::from_proto(t), raw_payload)
    None => raw_payload
  }
  let redelivery_count = message.redelivery_count.unwrap_or(0)
  let base_id = MessageId::from_proto(message.message_id)
  let batch_size = metadata.num_messages_in_batch.unwrap_or(0)
  if batch_size > 1 {
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
      self.last_seen.val = Some(msg.id())
      ignore(self.events.try_put(Msg(msg)))
    }
  } else {
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
    )
    self.last_seen.val = Some(msg.id())
    ignore(self.events.try_put(Msg(msg)))
  }
}

///|
/// Read the next message, or `None` once the current end of the topic is
/// reached. Messages published later can still be read by calling `next`
/// again.
pub async fn Reader::next(self : Reader) -> Message? {
  if self.closed.val {
    raise ConsumerClosed
  }
  match self.events.get() {
    Msg(msg) => {
      self.available_permits -= 1
      let threshold = self.receiver_permits / 2
      if self.available_permits <= threshold {
        let refill = self.receiver_permits - self.available_permits
        self.send_flow(refill) catch {
          _ => ()
        }
        self.available_permits += refill
      }
      Some(msg)
    }
    EndOfTopic => None
  }
}

///|
/// True when there are buffered messages or the broker reports unseen
/// messages beyond the last one read.
pub async fn Reader::has_message_available(self : Reader) -> Bool {
  if (self.events.try_get() catch { _ => None }) is Some(event) {
    // put it back conceptually: peeking is not supported by the queue,
    // so push the event back through a fresh front slot
    self.requeue_front(event)
    return true
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID
  let req = @proto.CommandGetLastMessageId::default()
  req.consumer_id = self.consumer_id
  req.request_id = self.conn.next_request_id()
  cmd.get_last_message_id = Some(req)
  let reply = self.conn.request(cmd) catch { _ => return false }
  guard reply.get_last_message_id_response is Some(resp) else { return false }
  let last = MessageId::from_proto(resp.last_message_id)
  match self.last_seen.val {
    Some(seen) => message_id_before(seen, last)
    None => true
  }
}

///|
fn message_id_before(a : MessageId, b : MessageId) -> Bool {
  (a.ledger_id, a.entry_id) < (b.ledger_id, b.entry_id)
}

///|
/// Re-insert an event taken by `try_get` at the front of the queue.
fn Reader::requeue_front(self : Reader, event : ReaderEvent) -> Unit raise {
  let rest = []
  while true {
    match self.events.try_get() {
      Some(e) => rest.push(e)
      None => break
    }
  }
  ignore(self.events.try_put(event))
  for e in rest {
    ignore(self.events.try_put(e))
  }
}

///|
/// Reposition the reader after `message_id`. Buffered messages are dropped.
pub async fn Reader::seek(self : Reader, message_id : MessageId) -> Unit {
  if self.closed.val {
    raise ConsumerClosed
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::SEEK
  let seek = @proto.CommandSeek::default()
  seek.consumer_id = self.consumer_id
  seek.request_id = self.conn.next_request_id()
  seek.message_id = Some(message_id.to_proto())
  cmd.seek = Some(seek)
  let reply = self.conn.request(cmd)
  guard reply.type_ is @proto.BaseCommand_Type::SUCCESS else {
    raise UnexpectedResponse("expected SUCCESS for SEEK")
  }
  // drop everything buffered before the seek
  while (self.events.try_get() catch { _ => None }) is Some(_) {

  }
  self.last_seen.val = Some(message_id)
  self.available_permits = 0
  self.send_flow(self.receiver_permits)
  self.available_permits = self.receiver_permits
}

///|
/// Close the reader.
pub async fn Reader::close(self : Reader) -> Unit {
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
  self.events.close(error=ConsumerClosed, clear=true)
}
