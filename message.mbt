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
  priv payload : Bytes
  priv key : String?
  priv properties : Map[String, String]
  publish_time : UInt64
  priv event_time : UInt64?
  priv producer_name : String
  priv sequence_id : UInt64?
  priv redelivery_count : UInt
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
/// How many times the broker redelivered this message.
pub fn Message::redelivery_count(self : Message) -> UInt {
  self.redelivery_count
}

///|
/// Acknowledge this message individually.
pub async fn Message::ack(self : Message) -> Unit {
  send_ack(
    self.conn_ref.val,
    self.consumer_id,
    @proto.CommandAck_AckType::Individual,
    [self.id],
  )
}

///|
/// Acknowledge this message as part of a transaction.
pub async fn Message::ack_txn(self : Message, txn : Transaction) -> Unit {
  send_ack(
    self.conn_ref.val,
    self.consumer_id,
    @proto.CommandAck_AckType::Individual,
    [self.id],
    txn~,
  )
}

///|
/// Negatively acknowledge this message, triggering redelivery.
pub async fn Message::nack(self : Message) -> Unit {
  send_redeliver(self.conn_ref.val, self.consumer_id, [self.id])
}

///|
async fn send_ack(
  conn : Connection,
  consumer_id : UInt64,
  ack_type : @proto.CommandAck_AckType,
  ids : Array[MessageId],
  txn? : Transaction,
) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::ACK
  let ack = @proto.CommandAck::default()
  ack.consumer_id = consumer_id
  ack.ack_type = ack_type
  ack.message_id = ids.map(fn(id) { id.to_proto() })
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
  conn.send_command(cmd)
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
