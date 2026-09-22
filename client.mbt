// Client facade: service URL parsing, authentication, and producer /
// consumer construction with topic lookup.

///|
/// Pluggable authentication for the CONNECT handshake.
pub(open) trait Authentication {
  /// Auth method name announced to the broker, e.g. "token".
  fn method_name(Self) -> String
  /// Auth data bytes sent with CONNECT, and resent on auth challenges.
  fn auth_data(Self) -> Bytes
}

///|
/// Token authentication (`AuthenticationProviderToken` on the broker).
pub(all) struct TokenAuth {
  token : String
}

///|
pub fn TokenAuth::new(token : String) -> TokenAuth {
  { token, }
}

///|
pub impl Authentication for TokenAuth with fn method_name(_) -> String {
  "token"
}

///|
pub impl Authentication for TokenAuth with fn auth_data(self) -> Bytes {
  @utf8.encode(self.token)
}

///|
/// A Pulsar client. Background tasks of every connection it opens run in
/// the task group passed to `Client::connect`.
pub struct Client {
  priv manager : ConnectionManager
  priv service_addr : String
}

///|
/// Connect to a Pulsar cluster, e.g.
/// `Client::connect(group, "pulsar://localhost:6650")`. Connectivity and
/// the protocol handshake are verified eagerly.
pub async fn Client::connect(
  group : @async.TaskGroup[Unit],
  service_url : String,
  auth? : &Authentication,
) -> Client {
  let auth_data = auth.map(fn(a) {
    { method_name: a.method_name(), data: a.auth_data(), }
  })
  let manager = ConnectionManager::new(group, auth?=auth_data)
  let addr = broker_addr_of(service_url)
  ignore(manager.get(addr))
  { manager, service_addr: addr, }
}

///|
/// Close all connections opened by this client.
pub fn Client::close(self : Client) -> Unit {
  self.manager.close()
}

///|
/// Create a producer for `topic`. The topic is looked up and the producer
/// is attached to the owning broker.
pub async fn Client::create_producer(
  self : Client,
  topic : String,
  name? : String,
  batching? : BatchingConfig,
) -> Producer {
  let conn = lookup_topic(self.manager, self.service_addr, topic)
  Producer::create(self.manager.group, conn, topic, name?, batching?)
}

///|
/// Create a consumer subscribed to `topic`.
pub async fn Client::create_consumer(
  self : Client,
  topic : String,
  subscription : String,
  sub_type? : SubscriptionType = Exclusive,
  consumer_name? : String,
  initial_position? : SubscriptionInitialPosition = Latest,
  receiver_queue_size? : Int = 1000,
) -> Consumer {
  let conn = lookup_topic(self.manager, self.service_addr, topic)
  Consumer::create(
    conn,
    topic,
    subscription,
    sub_type~,
    consumer_name?,
    initial_position~,
    receiver_queue_size~,
  )
}

///|
/// Number of partitions of a topic (0 or 1 for non-partitioned topics).
pub async fn Client::partitions_of(self : Client, topic : String) -> Int {
  get_partition_count(self.manager, self.service_addr, topic)
}
