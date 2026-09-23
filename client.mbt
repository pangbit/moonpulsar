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
  let (addr, tls) = broker_addr_of(service_url)
  let manager = ConnectionManager::new(group, auth?=auth_data, tls~)
  ignore(manager.get(addr))
  { manager, service_addr: addr, }
}

///|
/// Connect with TLS certificate verification disabled
/// (for testing against self-signed broker certificates).
pub async fn Client::connect_tls_insecure(
  group : @async.TaskGroup[Unit],
  service_url : String,
  auth? : &Authentication,
) -> Client {
  let auth_data = auth.map(fn(a) {
    { method_name: a.method_name(), data: a.auth_data(), }
  })
  let (addr, tls) = broker_addr_of(service_url)
  let manager = ConnectionManager::new(
    group,
    auth?=auth_data,
    tls~,
    tls_insecure=true,
  )
  ignore(manager.get(addr))
  { manager, service_addr: addr }
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
  compression? : CompressionCodec = None,
) -> Producer {
  let manager = self.manager
  let addr = self.service_addr
  let partitions = get_partition_count(manager, addr, topic)
  if partitions <= 1 {
    let conn = lookup_topic(manager, addr, topic)
    let single = SingleProducer::create(
      self.manager.group,
      conn,
      topic,
      name?,
      batching?,
      reconnect_with=async fn() { lookup_topic(manager, addr, topic) },
      compression~,
    )
    return Producer::single(single)
  }
  let subs = []
  for i in 0..<partitions {
    let partition_topic = "\{topic}-partition-\{i}"
    let conn = lookup_topic(manager, addr, partition_topic)
    subs.push(
      Producer::single(
        SingleProducer::create(
          self.manager.group,
          conn,
          partition_topic,
          name?,
          batching?,
          reconnect_with=async fn() {
            lookup_topic(manager, addr, partition_topic)
          },
          compression~,
        ),
      ),
    )
  }
  Producer::partitioned(subs)
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
  let manager = self.manager
  let addr = self.service_addr
  let partitions = get_partition_count(manager, addr, topic)
  if partitions <= 1 {
    let conn = lookup_topic(manager, addr, topic)
    let single = SingleConsumer::create(
      self.manager.group,
      conn,
      topic,
      subscription,
      sub_type~,
      consumer_name?,
      initial_position~,
      receiver_queue_size~,
      reconnect_with=async fn() { lookup_topic(manager, addr, topic) },
    )
    return Consumer::single(single)
  }
  let subs = []
  for i in 0..<partitions {
    let partition_topic = "\{topic}-partition-\{i}"
    let conn = lookup_topic(manager, addr, partition_topic)
    subs.push(
      Consumer::single(
        SingleConsumer::create(
          self.manager.group,
          conn,
          partition_topic,
          subscription,
          sub_type~,
          consumer_name?,
          initial_position~,
          receiver_queue_size~,
          reconnect_with=async fn() {
            lookup_topic(manager, addr, partition_topic)
          },
        ),
      ),
    )
  }
  Consumer::partitioned(self.manager.group, subs)
}

///|
/// Create a reader over `topic` starting after `start_message_id`
/// (default: latest). Readers are non-durable and do not ack.
/// Partitioned topics are aggregated over one reader per partition.
pub async fn Client::create_reader(
  self : Client,
  topic : String,
  start_message_id? : MessageId,
  receiver_queue_size? : Int = 1000,
) -> Reader {
  let manager = self.manager
  let addr = self.service_addr
  let partitions = get_partition_count(manager, addr, topic)
  if partitions <= 1 {
    let conn = lookup_topic(manager, addr, topic)
    return Reader::one(
      SingleReader::create(
        self.manager.group,
        conn,
        topic,
        start_message_id?,
        receiver_queue_size~,
      ),
    )
  }
  let subs = []
  for i in 0..<partitions {
    let partition_topic = "\{topic}-partition-\{i}"
    let conn = lookup_topic(manager, addr, partition_topic)
    subs.push(
      SingleReader::create(
        self.manager.group,
        conn,
        partition_topic,
        start_message_id?,
        receiver_queue_size~,
      ),
    )
  }
  Reader::partitioned(self.manager.group, subs)
}

///|
/// Number of partitions of a topic (0 or 1 for non-partitioned topics).
pub async fn Client::partitions_of(self : Client, topic : String) -> Int {
  get_partition_count(self.manager, self.service_addr, topic)
}
