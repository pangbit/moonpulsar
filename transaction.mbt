// Transactions: coordinator client (TC channel) plus produce/ack within a
// transaction, mirroring the transaction commands of the binary protocol.
//
// The coordinator connects to the broker owning its assignment partition;
// the broker must have transactions enabled.

///|
const TRANSACTION_COORDINATOR_ASSIGN_TOPIC : String = "persistent://pulsar/system/transaction_coordinator_assign"

///|
/// A handle to a transaction coordinator channel on a broker.
pub struct TransactionCoordinator {
  priv conn : Connection
  priv tc_id : UInt64
  priv owns_connection : Bool
}

///|
/// Open a transaction coordinator channel on a fresh connection to `addr`.
pub async fn TransactionCoordinator::connect(
  group : @async.TaskGroup[Unit],
  addr : String,
  tc_id : UInt64,
  auth? : AuthData,
  tls? : Bool = false,
) -> TransactionCoordinator {
  let conn = Connection::connect(group, addr, auth?, tls~)
  errdefer conn.close()
  TransactionCoordinator::attach(conn, tc_id, owns_connection=true)
}

///|
async fn TransactionCoordinator::attach(
  conn : Connection,
  tc_id : UInt64,
  owns_connection~ : Bool,
) -> TransactionCoordinator {
  let coord : TransactionCoordinator = { conn, tc_id, owns_connection, }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_REQUEST
  let req = @proto.CommandTcClientConnectRequest::default()
  req.request_id = conn.next_request_id()
  req.tc_id = tc_id
  cmd.tc_client_connect_request = Some(req)
  let reply = conn.request(cmd)
  guard reply.tc_client_connect_response is Some(resp) else {
    raise UnexpectedResponse("expected TC_CLIENT_CONNECT_RESPONSE")
  }
  if resp.error is Some(err) {
    raise ServerError(
      code=err.to_enum().0.reinterpret_as_int(),
      message=resp.message.unwrap_or("TC connect failed"),
    )
  }
  coord
}

///|
/// Close the coordinator's connection.
pub fn TransactionCoordinator::close(self : TransactionCoordinator) -> Unit {
  if self.owns_connection {
    self.conn.close()
  }
}

///|
/// A transaction handle; pass to `Producer::send_txn` / `Consumer::ack_txn`
/// and finish with `commit` or `abort`.
pub struct Transaction {
  priv coord : TransactionCoordinator
  priv least : UInt64
  priv most : UInt64
}

///|
/// The (most, least) bits of the transaction id.
pub fn Transaction::id(self : Transaction) -> (UInt64, UInt64) {
  (self.most, self.least)
}

///|
/// Start a new transaction with the given time-to-live.
pub async fn TransactionCoordinator::new_transaction(
  self : TransactionCoordinator,
  ttl_ms? : UInt64 = 60000,
) -> Transaction {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::NEW_TXN
  let req = @proto.CommandNewTxn::default()
  req.request_id = self.conn.next_request_id()
  req.txn_ttl_millis = Some(ttl_ms)
  req.tc_id = Some(self.tc_id)
  cmd.new_txn = Some(req)
  let reply = self.conn.request(cmd)
  guard reply.new_txn_response is Some(resp) else {
    raise UnexpectedResponse("expected NEW_TXN_RESPONSE")
  }
  if resp.error is Some(err) {
    raise ServerError(
      code=err.to_enum().0.reinterpret_as_int(),
      message=resp.message.unwrap_or("NEW_TXN failed"),
    )
  }
  {
    coord: self,
    least: resp.txnid_least_bits.unwrap_or(0),
    most: resp.txnid_most_bits.unwrap_or(0),
  }
}

///|
async fn Transaction::end(
  self : Transaction,
  action : @proto.TxnAction,
) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::END_TXN
  let req = @proto.CommandEndTxn::default()
  req.request_id = self.coord.conn.next_request_id()
  req.txnid_least_bits = Some(self.least)
  req.txnid_most_bits = Some(self.most)
  req.txn_action = Some(action)
  cmd.end_txn = Some(req)
  let reply = self.coord.conn.request(cmd)
  guard reply.end_txn_response is Some(resp) else {
    raise UnexpectedResponse("expected END_TXN_RESPONSE")
  }
  if resp.error is Some(err) {
    raise ServerError(
      code=err.to_enum().0.reinterpret_as_int(),
      message=resp.message.unwrap_or("END_TXN failed"),
    )
  }
}

///|
/// Commit the transaction.
pub async fn Transaction::commit(self : Transaction) -> Unit {
  self.end(@proto.TxnAction::COMMIT)
}

///|
/// Abort the transaction, rolling back all its messages and acks.
pub async fn Transaction::abort(self : Transaction) -> Unit {
  self.end(@proto.TxnAction::ABORT)
}

///|
/// Register a topic as a producer participant of this transaction.
pub async fn Transaction::add_partition(
  self : Transaction,
  topic : String,
) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::ADD_PARTITION_TO_TXN
  let req = @proto.CommandAddPartitionToTxn::default()
  req.request_id = self.coord.conn.next_request_id()
  req.txnid_least_bits = Some(self.least)
  req.txnid_most_bits = Some(self.most)
  req.partitions.push(topic)
  cmd.add_partition_to_txn = Some(req)
  let reply = self.coord.conn.request(cmd)
  guard reply.add_partition_to_txn_response is Some(resp) else {
    raise UnexpectedResponse("expected ADD_PARTITION_TO_TXN_RESPONSE")
  }
  if resp.error is Some(err) {
    raise ServerError(
      code=err.to_enum().0.reinterpret_as_int(),
      message=resp.message.unwrap_or("ADD_PARTITION_TO_TXN failed"),
    )
  }
}

///|
/// Register a subscription as a consumer participant of this transaction.
pub async fn Transaction::add_subscription(
  self : Transaction,
  topic : String,
  subscription : String,
) -> Unit {
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN
  let req = @proto.CommandAddSubscriptionToTxn::default()
  req.request_id = self.coord.conn.next_request_id()
  req.txnid_least_bits = Some(self.least)
  req.txnid_most_bits = Some(self.most)
  let sub = @proto.Subscription::default()
  sub.topic = topic
  sub.subscription = subscription
  req.subscription.push(sub)
  cmd.add_subscription_to_txn = Some(req)
  let reply = self.coord.conn.request(cmd)
  guard reply.add_subscription_to_txn_response is Some(resp) else {
    raise UnexpectedResponse("expected ADD_SUBSCRIPTION_TO_TXN_RESPONSE")
  }
  if resp.error is Some(err) {
    raise ServerError(
      code=err.to_enum().0.reinterpret_as_int(),
      message=resp.message.unwrap_or("ADD_SUBSCRIPTION_TO_TXN failed"),
    )
  }
}
