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
  Client::connect_with_options(group, ClientOptions::new(service_url, auth?))
}

///|
/// Connect using settings shared by all broker connections, including
/// redirected lookups and reconnects.
pub async fn Client::connect_with_options(
  group : @async.TaskGroup[Unit],
  options : ClientOptions,
) -> Client {
  let auth_provider = options.auth.map(fn(a) {
    fn() -> AuthData { { method_name: a.method_name(), data: a.auth_data(), } }
  })
  let (addr, tls) = broker_addr_of(options.service_url)
  if options.ca_pem_file is Some(_) && !tls {
    raise InvalidFrame("TLS CA requires a pulsar+ssl URL")
  }
  let tls_trust = options.ca_pem_file.map(fn(path) {
    @tls.TrustedRoot::CustomPemFile(path)
  })
  let manager = ConnectionManager::new(
    group,
    auth_provider?,
    async_auth_provider?=options.auth_provider,
    tls~,
    tls_insecure=options.tls_insecure,
    tls_trust?,
    connection_timeout_ms=options.connection_timeout_ms,
    operation_timeout_ms=options.operation_timeout_ms,
    keepalive_interval_ms=options.keepalive_interval_ms,
    listener_name?=options.listener_name,
    lookup_properties=options.lookup_properties,
  )
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
  Client::connect_with_options(
    group,
    ClientOptions::new(service_url, auth?, tls_insecure=true),
  )
}

///|
/// Connect to a TLS broker using a PEM file as the trusted root. The same
/// trust root is used for topic lookup redirects and reconnects.
pub async fn Client::connect_tls_with_ca(
  group : @async.TaskGroup[Unit],
  service_url : String,
  ca_pem_file : String,
  auth? : &Authentication,
) -> Client {
  let (_, tls) = broker_addr_of(service_url)
  guard tls && ca_pem_file != "" else {
    raise InvalidFrame(
      "TLS CA connection requires a pulsar+ssl URL and PEM file",
    )
  }
  Client::connect_with_options(
    group,
    ClientOptions::new(service_url, auth?, ca_pem_file~),
  )
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
  properties? : Map[String, String] = {},
  access_mode? : ProducerAccessMode = Shared,
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
      properties~,
      access_mode~,
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
        properties~,
        access_mode~,
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
  key_shared_policy? : KeySharedPolicy,
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
  dead_letter_policy? : DeadLetterPolicy,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
  chunk_policy? : ChunkAssemblyPolicy,
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
  if dead_letter_policy is Some(policy) && policy.topic() == topic {
    raise InvalidFrame("dead-letter topic must differ from source topic")
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
      key_shared_policy?,
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
      chunk_policy?,
    )
    return self.with_dead_letter(Consumer::single(single), dead_letter_policy)
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
        key_shared_policy?,
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
        chunk_policy?,
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
  self.with_dead_letter(consumer, dead_letter_policy)
}

///|
async fn Client::with_dead_letter(
  self : Client,
  consumer : Consumer,
  policy : DeadLetterPolicy?,
) -> Consumer {
  if policy is Some(config) {
    let producer = self.create_producer(config.topic()) catch {
      e => {
        consumer.close() catch {
          _ => ()
        }
        raise e
      }
    }
    consumer.set_dead_letter(config, producer, self.manager.group)
  }
  consumer
}

///|
/// Consume a source and its retry-letter topic as one stream. Call
/// `Consumer::reconsume_later` to publish a failed message with a delay;
/// retries beyond the dead-letter limit are sent to the dead-letter topic.
pub async fn Client::create_retry_consumer(
  self : Client,
  topic : String,
  subscription : String,
  retry_topic : String,
  dead_letter_policy : DeadLetterPolicy,
  sub_type? : SubscriptionType = Shared,
  key_shared_policy? : KeySharedPolicy,
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
  chunk_policy? : ChunkAssemblyPolicy,
) -> Consumer {
  guard retry_topic != "" &&
    retry_topic != topic &&
    retry_topic != dead_letter_policy.topic() &&
    topic != dead_letter_policy.topic() else {
    raise InvalidFrame("source, retry, and dead-letter topics must differ")
  }
  let create = async fn(name : String) -> Consumer {
    self.create_consumer(
      name,
      subscription,
      sub_type~,
      key_shared_policy?,
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
      chunk_policy?,
    )
  }
  let source = create(topic)
  let retry_source = create(retry_topic) catch {
    e => {
      source.close() catch {
        _ => ()
      }
      raise e
    }
  }
  let combined = Consumer::partitioned(self.manager.group, [
    source, retry_source,
  ])
  let combined = self.with_dead_letter(combined, Some(dead_letter_policy))
  let producer = self.create_producer(retry_topic) catch {
    e => {
      combined.close() catch {
        _ => ()
      }
      raise e
    }
  }
  combined.set_retry_letter(retry_topic, producer)
  combined
}

///|
/// Subscribe to an explicit list of topics with one logical consumer.
/// Received messages retain their source topic in `Message::topic`.
pub async fn Client::create_multi_topic_consumer(
  self : Client,
  topics : Array[String],
  subscription : String,
  sub_type? : SubscriptionType = Exclusive,
  key_shared_policy? : KeySharedPolicy,
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
  dead_letter_policy? : DeadLetterPolicy,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
  chunk_policy? : ChunkAssemblyPolicy,
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
        key_shared_policy?,
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
        dead_letter_policy?,
        schema?,
        partition_refresh_ms~,
        chunk_policy?,
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
  read_compacted? : Bool = false,
  partition_refresh_ms? : Int = 60000,
  name? : String,
  properties? : Map[String, String] = {},
  subscription_name? : String,
  subscription_role_prefix? : String = "reader",
  chunk_policy? : ChunkAssemblyPolicy,
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
        read_compacted~,
        name?,
        properties~,
        subscription_name?,
        subscription_role_prefix~,
        chunk_policy?,
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
      read_compacted~,
      name?,
      properties~,
      subscription_name?,
      subscription_role_prefix~,
      chunk_policy?,
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
/// Build a live raw-bytes view of a compacted topic from its earliest message.
/// Messages without keys are ignored; an empty payload removes the key.
/// Initial last-message queries must succeed before the view is returned.
pub async fn Client::create_table_view(
  self : Client,
  topic : String,
  partition_refresh_ms? : Int = 60000,
) -> TableView {
  let reader = self.create_reader(
    topic,
    start_message_id=MessageId::earliest(),
    read_compacted=true,
    partition_refresh_ms~,
  )
  TableView::create(self.manager.group, reader) catch {
    e => {
      reader.close() catch {
        _ => ()
      }
      raise e
    }
  }
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
  key_shared_policy? : KeySharedPolicy,
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
  dead_letter_policy? : DeadLetterPolicy,
  schema? : SchemaInfo,
  partition_refresh_ms? : Int = 60000,
  chunk_policy? : ChunkAssemblyPolicy,
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
          key_shared_policy?,
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
          dead_letter_policy?,
          schema?,
          partition_refresh_ms~,
          chunk_policy?,
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
