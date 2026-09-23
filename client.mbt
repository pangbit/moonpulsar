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
/// Username/password authentication for a broker's basic auth provider.
pub(all) struct BasicAuth {
  username : String
  password : String
}

///|
pub fn BasicAuth::new(
  username : String,
  password : String,
) -> BasicAuth raise PulsarError {
  guard username != "" && password != "" else {
    raise InvalidFrame("basic authentication requires username and password")
  }
  { username, password, }
}

///|
pub impl Authentication for BasicAuth with fn method_name(_) -> String {
  "basic"
}

///|
pub impl Authentication for BasicAuth with fn auth_data(self) -> Bytes {
  @utf8.encode("\{self.username}:\{self.password}")
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
  let auth_provider = auth.map(fn(a) {
    fn() -> AuthData { { method_name: a.method_name(), data: a.auth_data(), } }
  })
  let (addr, tls) = broker_addr_of(service_url)
  let manager = ConnectionManager::new(group, auth_provider?, tls~)
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
  let auth_provider = auth.map(fn(a) {
    fn() -> AuthData { { method_name: a.method_name(), data: a.auth_data(), } }
  })
  let (addr, tls) = broker_addr_of(service_url)
  let manager = ConnectionManager::new(
    group,
    auth_provider?,
    tls~,
    tls_insecure=true,
  )
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
  compression? : CompressionCodec = None,
  chunk_size? : Int,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
) -> Producer {
  guard partition_refresh_ms > 0 else {
    raise InvalidFrame("partition refresh interval must be positive")
  }
  if chunk_size is Some(size) && size <= 0 {
    raise InvalidFrame("chunk size must be positive")
  }
  if chunk_size is Some(_) && batching is Some(_) {
    raise InvalidFrame("chunking and batching cannot be combined")
  }
  let manager = self.manager
  let addr = self.service_addr
  let partitions = get_partition_count(manager, addr, topic)
  if partitions == 0 {
    let conn = lookup_topic(manager, addr, topic)
    let single = SingleProducer::create(
      self.manager.group,
      conn,
      topic,
      name?,
      batching?,
      reconnect_with=async fn() { lookup_topic(manager, addr, topic) },
      compression~,
      chunk_size?,
      schema?,
    )
    return Producer::single(single)
  }
  let create_partition = async fn(i : Int) -> Producer {
    let partition_topic = "\{topic}-partition-\{i}"
    let conn = lookup_topic(manager, addr, partition_topic)
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
        chunk_size?,
        schema?,
      ),
    )
  }
  let subs : Array[Producer] = []
  for i in 0..<partitions {
    let partition = create_partition(i) catch {
      e => {
        for existing in subs {
          existing.close() catch {
            _ => ()
          }
        }
        raise e
      }
    }
    subs.push(partition)
  }
  let producer = Producer::partitioned(subs)
  self.manager.group.spawn_bg(
    async fn() {
      while !producer.is_closed() && !manager.is_closed() {
        @async.sleep(partition_refresh_ms)
        if producer.is_closed() || manager.is_closed() {
          break
        }
        let count = get_partition_count(manager, addr, topic) catch {
          _ => continue
        }
        while producer.partition_count() < count && !producer.is_closed() {
          let partition = create_partition(producer.partition_count()) catch {
            _ => break
          }
          producer.add_partition(partition) catch {
            _ => break
          }
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  producer
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
  priority_level? : Int = 0,
  durable? : Bool = true,
  read_compacted? : Bool = false,
  properties? : Map[String, String] = {},
  subscription_properties? : Map[String, String] = {},
  receiver_queue_size? : Int = 1000,
  ack_timeout_ms? : Int = 0,
  nack_backoff? : NackBackoff,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
) -> Consumer {
  guard partition_refresh_ms > 0 else {
    raise InvalidFrame("partition refresh interval must be positive")
  }
  guard ack_timeout_ms >= 0 else {
    raise InvalidFrame("ack timeout must not be negative")
  }
  guard receiver_queue_size > 0 else {
    raise InvalidFrame("receiver queue size must be positive")
  }
  let manager = self.manager
  let addr = self.service_addr
  let partitions = get_partition_count(manager, addr, topic)
  if partitions == 0 {
    let conn = lookup_topic(manager, addr, topic)
    let single = SingleConsumer::create(
      self.manager.group,
      conn,
      topic,
      subscription,
      sub_type~,
      consumer_name?,
      initial_position~,
      priority_level~,
      durable~,
      read_compacted~,
      properties~,
      subscription_properties~,
      receiver_queue_size~,
      ack_timeout_ms~,
      nack_backoff?,
      reconnect_with=async fn() { lookup_topic(manager, addr, topic) },
      schema?,
    )
    return Consumer::single(single)
  }
  let create_partition = async fn(i : Int) -> Consumer {
    let partition_topic = "\{topic}-partition-\{i}"
    let conn = lookup_topic(manager, addr, partition_topic)
    Consumer::single(
      SingleConsumer::create(
        self.manager.group,
        conn,
        partition_topic,
        subscription,
        sub_type~,
        consumer_name?,
        initial_position~,
        priority_level~,
        durable~,
        read_compacted~,
        properties~,
        subscription_properties~,
        receiver_queue_size~,
        ack_timeout_ms~,
        nack_backoff?,
        reconnect_with=async fn() {
          lookup_topic(manager, addr, partition_topic)
        },
        schema?,
      ),
    )
  }
  let subs : Array[Consumer] = []
  for i in 0..<partitions {
    let sub = create_partition(i) catch {
      e => {
        for existing in subs {
          existing.close() catch {
            _ => ()
          }
        }
        raise e
      }
    }
    subs.push(sub)
  }
  let consumer = Consumer::partitioned(self.manager.group, subs)
  let known : Ref[Int] = { val: partitions, }
  self.manager.group.spawn_bg(
    async fn() {
      while !consumer.is_closed() && !manager.is_closed() {
        @async.sleep(partition_refresh_ms)
        if consumer.is_closed() || manager.is_closed() {
          break
        }
        let count = get_partition_count(manager, addr, topic) catch {
          _ => continue
        }
        while known.val < count && !consumer.is_closed() {
          let sub = create_partition(known.val) catch { _ => break }
          consumer.add_source(self.manager.group, sub) catch {
            _ => break
          }
          known.val += 1
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  consumer
}

///|
/// Subscribe to an explicit list of topics with one logical consumer.
/// Received messages retain their source topic in `Message::topic`.
pub async fn Client::create_multi_topic_consumer(
  self : Client,
  topics : Array[String],
  subscription : String,
  sub_type? : SubscriptionType = Exclusive,
  consumer_name? : String,
  initial_position? : SubscriptionInitialPosition = Latest,
  priority_level? : Int = 0,
  durable? : Bool = true,
  read_compacted? : Bool = false,
  properties? : Map[String, String] = {},
  subscription_properties? : Map[String, String] = {},
  receiver_queue_size? : Int = 1000,
  ack_timeout_ms? : Int = 0,
  nack_backoff? : NackBackoff,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
) -> Consumer {
  guard !topics.is_empty() else {
    raise InvalidFrame("multi-topic consumer requires at least one topic")
  }
  let consumers : Array[Consumer] = []
  let seen : Map[String, Bool] = {}
  for topic in topics {
    if !seen.contains(topic) {
      seen[topic] = true
      let sub = self.create_consumer(
        topic,
        subscription,
        sub_type~,
        consumer_name?,
        initial_position~,
        priority_level~,
        durable~,
        read_compacted~,
        properties~,
        subscription_properties~,
        receiver_queue_size~,
        ack_timeout_ms~,
        nack_backoff?,
        schema?,
        partition_refresh_ms~,
      ) catch {
        e => {
          for existing in consumers {
            existing.close() catch {
              _ => ()
            }
          }
          raise e
        }
      }
      consumers.push(sub)
    }
  }
  Consumer::partitioned(self.manager.group, consumers)
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
  partition_refresh_ms? : Int = 60000,
) -> Reader {
  guard partition_refresh_ms > 0 else {
    raise InvalidFrame("partition refresh interval must be positive")
  }
  let manager = self.manager
  let addr = self.service_addr
  let partitions = get_partition_count(manager, addr, topic)
  if partitions == 0 {
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
  let create_partition = async fn(i : Int) -> SingleReader {
    let partition_topic = "\{topic}-partition-\{i}"
    let conn = lookup_topic(manager, addr, partition_topic)
    SingleReader::create(
      self.manager.group,
      conn,
      partition_topic,
      start_message_id?,
      receiver_queue_size~,
    )
  }
  let subs : Array[SingleReader] = []
  for i in 0..<partitions {
    let sub = create_partition(i) catch {
      e => {
        for existing in subs {
          existing.close() catch {
            _ => ()
          }
        }
        raise e
      }
    }
    subs.push(sub)
  }
  let reader = Reader::partitioned(self.manager.group, subs)
  let known : Ref[Int] = { val: partitions, }
  self.manager.group.spawn_bg(
    async fn() {
      while !reader.is_closed() && !manager.is_closed() {
        @async.sleep(partition_refresh_ms)
        if reader.is_closed() || manager.is_closed() {
          break
        }
        let count = get_partition_count(manager, addr, topic) catch {
          _ => continue
        }
        while known.val < count && !reader.is_closed() {
          let sub = create_partition(known.val) catch { _ => break }
          reader.add_partition(self.manager.group, sub) catch {
            _ => break
          }
          known.val += 1
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  reader
}

///|
/// Number of partitions of a topic (0 or 1 for non-partitioned topics).
pub async fn Client::partitions_of(self : Client, topic : String) -> Int {
  get_partition_count(self.manager, self.service_addr, topic)
}

///|
/// List topics in `tenant/namespace`, optionally filtered by a MoonBit regular
/// expression.
pub async fn Client::topics_of_namespace(
  self : Client,
  namespace : String,
  pattern? : String,
  mode? : NamespaceTopicMode = Persistent,
) -> Array[String] {
  get_topics_of_namespace(
    self.manager,
    self.service_addr,
    namespace,
    pattern?,
    mode~,
  )
}

///|
/// Subscribe to topics matching a regular expression in `tenant/namespace`.
/// New topics are discovered at the configured polling interval.
pub async fn Client::create_pattern_consumer(
  self : Client,
  namespace : String,
  pattern : String,
  subscription : String,
  refresh_interval_ms? : Int = 60000,
  sub_type? : SubscriptionType = Exclusive,
  consumer_name? : String,
  initial_position? : SubscriptionInitialPosition = Latest,
  priority_level? : Int = 0,
  durable? : Bool = true,
  read_compacted? : Bool = false,
  properties? : Map[String, String] = {},
  subscription_properties? : Map[String, String] = {},
  receiver_queue_size? : Int = 1000,
  ack_timeout_ms? : Int = 0,
  nack_backoff? : NackBackoff,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
) -> Consumer {
  guard refresh_interval_ms > 0 else {
    raise InvalidFrame("topic refresh interval must be positive")
  }
  guard partition_refresh_ms > 0 else {
    raise InvalidFrame("partition refresh interval must be positive")
  }
  let regex = @string.Regex::Regex(pattern)
  let consumer = Consumer::pattern(pattern, subscription)
  let seen : Map[String, Bool] = {}
  let refresh = async fn() -> Unit {
    let discovered = self.topics_of_namespace(namespace)
    let current : Map[String, Bool] = {}
    for name in discovered {
      let topic = base_topic_name(name)
      if regex.execute(topic) is Some(_) {
        current[topic] = true
      }
      if current.contains(topic) && !seen.contains(topic) {
        let sub = self.create_consumer(
          topic,
          subscription,
          sub_type~,
          consumer_name?,
          initial_position~,
          priority_level~,
          durable~,
          read_compacted~,
          properties~,
          subscription_properties~,
          receiver_queue_size~,
          ack_timeout_ms~,
          nack_backoff?,
          schema?,
          partition_refresh_ms~,
        )
        consumer.add_source(self.manager.group, sub)
        seen[topic] = true
      }
    }
    let removed = []
    for topic, _ in seen {
      if !current.contains(topic) {
        removed.push(topic)
      }
    }
    for topic in removed {
      consumer.remove_source(topic)
      seen.remove(topic)
    }
  }
  refresh() catch {
    e => {
      consumer.close()
      raise e
    }
  }
  self.manager.group.spawn_bg(
    async fn() {
      while !consumer.is_closed() && !self.manager.is_closed() {
        @async.sleep(refresh_interval_ms)
        if !consumer.is_closed() && !self.manager.is_closed() {
          refresh() catch {
            _ => ()
          }
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  consumer
}

///|
/// Connect to the broker owning transaction coordinator `tc_id`.
/// The coordinator uses a connection managed by this client.
pub async fn Client::transaction_coordinator(
  self : Client,
  tc_id : UInt64,
) -> TransactionCoordinator {
  let count = get_partition_count(
    self.manager,
    self.service_addr,
    TRANSACTION_COORDINATOR_ASSIGN_TOPIC,
  )
  guard count > 0 else {
    raise LookupFailed("transaction coordinators are not enabled")
  }
  guard tc_id < count.to_uint64() else {
    raise LookupFailed("transaction coordinator id is out of range")
  }
  let topic = "\{TRANSACTION_COORDINATOR_ASSIGN_TOPIC}-partition-\{tc_id}"
  let conn = lookup_topic(self.manager, self.service_addr, topic)
  TransactionCoordinator::attach(conn, tc_id, owns_connection=false)
}
