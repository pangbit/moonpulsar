// Public message types shared by producer and consumer.

///|
/// Identifies a persisted message in a topic.
pub(all) struct MessageId {
  ledger_id : UInt64
  entry_id : UInt64
  /// index within a batch, -1 when not batched
  batch_index : Int
  /// partition index, -1 for non-partitioned topics
  partition : Int
} derive(Eq, Compare, Hash, Debug)

///|
/// The earliest available position.
pub fn MessageId::earliest() -> MessageId {
  {
    ledger_id: 18446744073709551615,
    entry_id: 18446744073709551615,
    batch_index: -1,
    partition: -1,
  }
}

///|
/// The latest available position.
pub fn MessageId::latest() -> MessageId {
  {
    ledger_id: 9223372036854775807,
    entry_id: 9223372036854775807,
    batch_index: -1,
    partition: -1,
  }
}

///|
fn MessageId::to_proto(self : MessageId) -> @proto.MessageIdData {
  let data = @proto.MessageIdData::default()
  data.ledger_id = self.ledger_id
  data.entry_id = self.entry_id
  if self.batch_index >= 0 {
    data.batch_index = Some(self.batch_index)
  }
  if self.partition >= 0 {
    data.partition = Some(self.partition)
  }
  data
}

///|
fn MessageId::from_proto(data : @proto.MessageIdData) -> MessageId {
  {
    ledger_id: data.ledger_id,
    entry_id: data.entry_id,
    batch_index: data.batch_index.unwrap_or(-1),
    partition: data.partition.unwrap_or(-1),
  }
}

///|
fn key_values_of(properties : Map[String, String]) -> Array[@proto.KeyValue] {
  let result = []
  for k, v in properties {
    let kv = @proto.KeyValue::default()
    kv.key = k
    kv.value = v
    result.push(kv)
  }
  result
}

///|
fn properties_of(kvs : Array[@proto.KeyValue]) -> Map[String, String] {
  let result : Map[String, String] = Map([])
  for kv in kvs {
    result[kv.key] = kv.value
  }
  result
}

///|
/// A message to be produced.
pub(all) struct ProducerMessage {
  payload : Bytes
  key : String?
  properties : Map[String, String]
  event_time : UInt64?
  sequence_id : UInt64?
  ordering_key : Bytes?
  /// absolute epoch-millis at which the broker should deliver the message
  deliver_at : UInt64?
  /// relative delay in milliseconds after which to deliver
  deliver_after : UInt64?
}

///|
/// Create a message to produce. Only `payload` is required.
pub fn ProducerMessage::new(
  payload : Bytes,
  key? : String,
  properties? : Map[String, String] = {},
  event_time? : UInt64,
  sequence_id? : UInt64,
  ordering_key? : Bytes,
  deliver_at? : UInt64,
  deliver_after? : UInt64,
) -> ProducerMessage {
  {
    payload,
    key,
    properties,
    event_time,
    sequence_id,
    ordering_key,
    deliver_at,
    deliver_after,
  }
}

///|
fn ProducerMessage::retained_bytes(self : ProducerMessage) -> Int {
  let mut size = self.payload.length()
  if self.key is Some(key) {
    size += @utf8.encode(key).length()
  }
  if self.ordering_key is Some(key) {
    size += key.length()
  }
  for key, value in self.properties {
    size += @utf8.encode(key).length() + @utf8.encode(value).length()
  }
  size
}

///|
/// Confirmation that the broker persisted a message.
pub(all) struct SendReceipt {
  message_id : MessageId
  sequence_id : UInt64
  producer_id : UInt64
  producer_name : String
} derive(Debug)

///|
/// A message received by a consumer.
pub struct Message {
  /// current connection of the owning consumer; follows reconnects
  priv conn_ref : Ref[Connection]
  priv consumer_id : UInt64
  priv id : MessageId
  priv topic : String
  priv subscription : String?
  priv payload : Bytes
  priv key : String?
  priv ordering_key : Bytes?
  priv properties : Map[String, String]
  publish_time : UInt64
  priv event_time : UInt64?
  priv producer_name : String
  priv sequence_id : UInt64?
  priv schema_version : Bytes?
  priv decryption_failed : Bool
  priv redelivery_count : UInt
  priv ack_tracker : Map[MessageId, Int]?
  priv batch_ack_tracker : BatchAckTracker?
  priv ack_dispatcher : AckDispatcher?
  priv chunk_ids : Array[MessageId]?
  priv chunk_tracker : Map[MessageId, Array[MessageId]]?
  priv on_delivery : ((Message) -> Unit)?
  priv mut on_ack : ((Message) -> Unit)?
  priv mut on_nack : ((Message) -> Unit)?
  priv mut interceptors : Array[ConsumerInterceptor]
  priv mut span_recorder : ClientSpanRecorder?
  priv nack_scheduler : ((Message) -> Unit)?
}

///|
fn Message::observe_ack(self : Message, result : Result[Unit, Error]) -> Unit {
  for interceptor in self.interceptors {
    interceptor.acked(self.id, result)
  }
}

///|
fn Message::observe_nack(self : Message, result : Result[Unit, Error]) -> Unit {
  for interceptor in self.interceptors {
    interceptor.nacked(self.id, result)
  }
}

///|
fn Message::mark_delivered(self : Message) -> Unit {
  if self.on_delivery is Some(callback) {
    callback(self)
  }
}

///|
pub fn Message::id(self : Message) -> MessageId {
  self.id
}

///|
pub fn Message::topic(self : Message) -> String {
  self.topic
}

///|
pub fn Message::payload(self : Message) -> Bytes {
  self.payload
}

///|
pub fn Message::key(self : Message) -> String? {
  self.key
}

///|
pub fn Message::ordering_key(self : Message) -> Bytes? {
  self.ordering_key
}

///|
pub fn Message::properties(self : Message) -> Map[String, String] {
  self.properties
}

///|
pub fn Message::publish_time(self : Message) -> UInt64 {
  self.publish_time
}

///|
pub fn Message::event_time(self : Message) -> UInt64? {
  self.event_time
}

///|
pub fn Message::producer_name(self : Message) -> String {
  self.producer_name
}

///|
pub fn Message::sequence_id(self : Message) -> UInt64? {
  self.sequence_id
}

///|
/// Version of the schema used to publish this message, when supplied by the broker.
pub fn Message::schema_version(self : Message) -> Bytes? {
  self.schema_version
}

///|
/// Whether the payload is encrypted ciphertext delivered by the `Consume`
/// decryption failure action.
pub fn Message::decryption_failed(self : Message) -> Bool {
  self.decryption_failed
}

///|
/// How many times the broker redelivered this message.
pub fn Message::redelivery_count(self : Message) -> UInt {
  self.redelivery_count
}

///|
/// Acknowledge this message individually.
pub async fn Message::ack(
  self : Message,
  await_response? : Bool = false,
) -> Unit {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Ack, self.topic, message_id=self.id)
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let result : Result[Unit, Error] = Ok(self.ack_raw(await_response)) catch {
    e => Err(e)
  }
  if self.span_recorder is Some(recorder) && span is Some(start) {
    match result {
      Ok(_) => recorder.finish(start, true)
      Err(e) => recorder.finish(start, false, error="\{e}")
    }
    span_done.val = true
  }
  self.observe_ack(result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
async fn Message::ack_raw(self : Message, await_response : Bool) -> Unit {
  if self.id.batch_index >= 0 && self.batch_ack_tracker is Some(tracker) {
    tracker.ack(
      self.conn_ref.val,
      self.consumer_id,
      self.id,
      @proto.CommandAck_AckType::Individual,
      await_response~,
      dispatcher?=self.ack_dispatcher,
    )
  } else if self.ack_dispatcher is Some(dispatcher) {
    dispatcher.ack(
      @proto.CommandAck_AckType::Individual,
      self.chunk_ids.unwrap_or([self.id]),
      await_response~,
    )
  } else {
    send_ack(
      self.conn_ref.val,
      self.consumer_id,
      @proto.CommandAck_AckType::Individual,
      self.chunk_ids.unwrap_or([self.id]),
      await_response~,
    )
  }
  if self.ack_tracker is Some(pending) {
    pending.remove(self.id)
  }
  if self.chunk_tracker is Some(chunks) {
    chunks.remove(self.id)
  }
  if self.on_ack is Some(callback) {
    callback(self)
  }
}

///|
/// Acknowledge this message as part of a transaction.
pub async fn Message::ack_txn(
  self : Message,
  txn : Transaction,
  await_response? : Bool = false,
) -> Unit {
  let span = self.span_recorder.map(fn(recorder) {
    recorder.begin(Ack, self.topic, message_id=self.id)
  })
  let span_done : Ref[Bool] = { val: false, }
  defer (if !span_done.val &&
    self.span_recorder is Some(recorder) &&
    span is Some(start) {
    recorder.finish(start, false, error="cancelled")
  })
  let result : Result[Unit, Error] = Ok(self.ack_txn_raw(txn, await_response)) catch {
    e => Err(e)
  }
  if self.span_recorder is Some(recorder) && span is Some(start) {
    match result {
      Ok(_) => recorder.finish(start, true)
      Err(e) => recorder.finish(start, false, error="\{e}")
    }
    span_done.val = true
  }
  self.observe_ack(result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
async fn Message::ack_txn_raw(
  self : Message,
  txn : Transaction,
  await_response : Bool,
) -> Unit {
  guard self.subscription is Some(subscription) else {
    raise InvalidFrame("transactional ACK requires a consumer subscription")
  }
  txn.add_subscription(self.topic, subscription)
  if self.id.batch_index >= 0 && self.batch_ack_tracker is Some(tracker) {
    tracker.ack(
      self.conn_ref.val,
      self.consumer_id,
      self.id,
      @proto.CommandAck_AckType::Individual,
      txn~,
      await_response~,
      dispatcher?=self.ack_dispatcher,
    )
  } else if self.ack_dispatcher is Some(dispatcher) {
    dispatcher.ack(
      @proto.CommandAck_AckType::Individual,
      self.chunk_ids.unwrap_or([self.id]),
      txn~,
      await_response~,
    )
  } else {
    send_ack(
      self.conn_ref.val,
      self.consumer_id,
      @proto.CommandAck_AckType::Individual,
      self.chunk_ids.unwrap_or([self.id]),
      txn~,
      await_response~,
    )
  }
  if self.ack_tracker is Some(pending) {
    pending.remove(self.id)
  }
  if self.chunk_tracker is Some(chunks) {
    chunks.remove(self.id)
  }
  if self.on_ack is Some(callback) {
    callback(self)
  }
}

///|
/// Negatively acknowledge this message, triggering redelivery.
pub async fn Message::nack(self : Message) -> Unit {
  let result : Result[Unit, Error] = Ok(self.nack_raw()) catch { e => Err(e) }
  self.observe_nack(result)
  match result {
    Ok(_) => ()
    Err(e) => raise e
  }
}

///|
async fn Message::nack_raw(self : Message) -> Unit {
  if self.ack_tracker is Some(pending) {
    pending.remove(self.id)
  }
  if self.chunk_tracker is Some(chunks) {
    chunks.remove(self.id)
  }
  if self.nack_scheduler is Some(schedule) {
    schedule(self)
  } else {
    send_redeliver(
      self.conn_ref.val,
      self.consumer_id,
      self.chunk_ids.unwrap_or([self.id]),
    )
  }
  if self.on_nack is Some(callback) {
    callback(self)
  }
}

///|
async fn send_ack(
  conn : Connection,
  consumer_id : UInt64,
  ack_type : @proto.CommandAck_AckType,
  ids : Array[MessageId],
  txn? : Transaction,
  await_response? : Bool = false,
  batch_ack_set? : Array[Int64],
  batch_size? : Int,
) -> Unit {
  if await_response && conn.remote_protocol_version() < 17 {
    raise InvalidFrame("broker protocol does not support ACK responses")
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::ACK
  let ack = @proto.CommandAck::default()
  ack.consumer_id = consumer_id
  ack.ack_type = ack_type
  ack.message_id = ids.map(fn(id) { id.to_proto() })
  if batch_ack_set is Some(bits) {
    guard ids.length() == 1 && batch_size is Some(size) && size > 0 else {
      raise InvalidFrame("batch ACK requires one message ID and batch size")
    }
    ack.message_id[0].batch_index = None
    ack.message_id[0].batch_size = Some(size)
    ack.message_id[0].ack_set = bits
  }
  if await_response {
    ack.request_id = Some(conn.next_request_id())
  }
  match txn {
    Some(t) => {
      let (most, least) = t.id()
      ack.txnid_least_bits = Some(least)
      ack.txnid_most_bits = Some(most)
    }
    None => {
      // proto2 default 0 would make the broker treat the ack as
      // transactional and reject it when transactions are disabled
      ack.txnid_least_bits = None
      ack.txnid_most_bits = None
    }
  }
  cmd.ack = Some(ack)
  if await_response {
    let reply = conn.request(cmd)
    guard reply.ack_response is Some(response) else {
      raise UnexpectedResponse("expected ACK_RESPONSE")
    }
    if response.error is Some(code) {
      raise ServerError(
        code=code.to_enum().0.reinterpret_as_int(),
        message=response.message.unwrap_or("ACK failed"),
      )
    }
  } else {
    conn.send_command(cmd)
  }
}

///|
async fn send_redeliver(
  conn : Connection,
  consumer_id : UInt64,
  ids : Array[MessageId],
) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::REDELIVER_UNACKNOWLEDGED_MESSAGES
  let redeliver = @proto.CommandRedeliverUnacknowledgedMessages::default()
  redeliver.consumer_id = consumer_id
  redeliver.message_ids = ids.map(fn(id) { id.to_proto() })
  cmd.redeliver_unacknowledged_messages = Some(redeliver)
  conn.send_command(cmd)
}
