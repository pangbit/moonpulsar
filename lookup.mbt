// Topic lookup: resolve a topic name to the owning broker, following
// redirects as described by the Pulsar binary protocol.

///|
/// Maximum number of lookup redirects followed.
const MAX_LOOKUP_REDIRECTS : Int = 20

///|
/// Topic kinds included in namespace discovery.
pub(all) enum NamespaceTopicMode {
  Persistent
  NonPersistent
  AllTopics
} derive(Eq, Debug)

///|
fn NamespaceTopicMode::to_proto(
  self : NamespaceTopicMode,
) -> @proto.CommandGetTopicsOfNamespace_Mode {
  match self {
    Persistent => @proto.CommandGetTopicsOfNamespace_Mode::PERSISTENT
    NonPersistent => @proto.CommandGetTopicsOfNamespace_Mode::NON_PERSISTENT
    AllTopics => @proto.CommandGetTopicsOfNamespace_Mode::ALL
  }
}

///|
/// Strip the scheme of a pulsar service URL, returning ("host:port", tls?).
pub fn broker_addr_of(url : String) -> (String, Bool) raise PulsarError {
  if url.strip_prefix("pulsar+ssl://") is Some(rest) {
    return (rest.to_owned(), true)
  }
  if url.strip_prefix("pulsar://") is Some(rest) {
    return (rest.to_owned(), false)
  }
  raise LookupFailed("unsupported service URL: \{url}")
}

///|
/// Look up `topic` starting from the broker at `addr` and return a
/// connection to the owning broker. Redirects are followed transparently;
/// if the broker answers with `proxy_through_service_url`, the connection
/// used for the lookup is returned.
pub async fn lookup_topic(
  manager : ConnectionManager,
  addr : String,
  topic : String,
  authoritative? : Bool = false,
) -> Connection {
  let tls = manager.uses_tls()
  let mut current = addr
  let mut authoritative = authoritative
  for _ in 0..<MAX_LOOKUP_REDIRECTS {
    let conn = manager.get(current)
    let lookup = @proto.CommandLookupTopic::default()
    lookup.topic = topic
    lookup.request_id = conn.next_request_id()
    lookup.authoritative = Some(authoritative)
    lookup.advertised_listener_name = manager.listener_name
    lookup.properties = key_values_of(manager.lookup_properties)
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::LOOKUP
    cmd.lookup_topic = Some(lookup)
    let reply = conn.request(cmd)
    guard reply.lookup_topic_response is Some(resp) else {
      raise UnexpectedResponse("expected LOOKUP_RESPONSE")
    }
    match resp.response {
      Some(@proto.CommandLookupTopicResponse_LookupType::Connect) | None => {
        if resp.proxy_through_service_url == Some(true) {
          return conn
        }
        let url = if tls && resp.broker_service_url_tls is Some(_) {
          resp.broker_service_url_tls
        } else {
          resp.broker_service_url
        }
        match url {
          Some(url) => {
            let (broker_addr, _) = broker_addr_of(url)
            return manager.get(broker_addr)
          }
          None =>
            raise LookupFailed("broker returned no service url for \{topic}")
        }
      }
      Some(@proto.CommandLookupTopicResponse_LookupType::Redirect) => {
        let url = if tls && resp.broker_service_url_tls is Some(_) {
          resp.broker_service_url_tls
        } else {
          resp.broker_service_url
        }
        match url {
          Some(url) => {
            let (broker_addr, _) = broker_addr_of(url)
            current = broker_addr
            authoritative = resp.authoritative.unwrap_or(false)
          }
          None =>
            raise LookupFailed("redirect without service url for \{topic}")
        }
      }
      Some(@proto.CommandLookupTopicResponse_LookupType::Failed) => {
        let msg = resp.message.unwrap_or("lookup failed")
        raise LookupFailed("\{msg} (topic: \{topic})")
      }
    }
  }
  raise TooManyRedirects
}

///|
/// Number of partitions of a topic; 0 means non-partitioned.
pub async fn get_partition_count(
  manager : ConnectionManager,
  addr : String,
  topic : String,
) -> Int {
  let conn = manager.get(addr)
  let req = @proto.CommandPartitionedTopicMetadata::default()
  req.topic = topic
  req.request_id = conn.next_request_id()
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA
  cmd.partition_metadata = Some(req)
  let reply = conn.request(cmd)
  guard reply.partition_metadata_response is Some(resp) else {
    raise UnexpectedResponse("expected PARTITIONED_METADATA_RESPONSE")
  }
  if resp.error is Some(err) {
    raise ServerError(
      code=err.to_enum().0.reinterpret_as_int(),
      message=resp.message.unwrap_or("partition metadata failed"),
    )
  }
  match resp.partitions {
    Some(p) => p.reinterpret_as_int()
    None => 0
  }
}

///|
/// Ask a broker for topics in `tenant/namespace` and apply an optional
/// MoonBit regular expression locally. This also works with brokers that do
/// not implement server-side pattern filtering.
pub async fn get_topics_of_namespace(
  manager : ConnectionManager,
  addr : String,
  namespace : String,
  pattern? : String,
  mode? : NamespaceTopicMode = Persistent,
) -> Array[String] {
  let conn = manager.get(addr)
  let req = @proto.CommandGetTopicsOfNamespace::default()
  req.request_id = conn.next_request_id()
  req.namespace_ = namespace
  req.mode = Some(mode.to_proto())
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::GET_TOPICS_OF_NAMESPACE
  cmd.get_topics_of_namespace = Some(req)
  let reply = conn.request(cmd)
  guard reply.get_topics_of_namespace_response is Some(resp) else {
    raise UnexpectedResponse("expected GET_TOPICS_OF_NAMESPACE_RESPONSE")
  }
  match pattern {
    Some(source) => {
      let regex = @string.Regex::Regex(source)
      let result = []
      for topic in resp.topics {
        if regex.execute(topic) is Some(_) {
          result.push(topic)
        }
      }
      result
    }
    None => resp.topics
  }
}

///|
fn base_topic_name(topic : String) -> String {
  match topic.rev_find("-partition-") {
    Some(index) => {
      let suffix = topic[index + 11:]
      let part = @string.parse_int(suffix) catch { _ => -1 }
      if part >= 0 {
        topic[:index].to_owned()
      } else {
        topic
      }
    }
    None => topic
  }
}
