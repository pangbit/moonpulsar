// Topic lookup: resolve a topic name to the owning broker, following
// redirects as described by the Pulsar binary protocol.

///|
/// Maximum number of lookup redirects followed.
const MAX_LOOKUP_REDIRECTS : Int = 20

///|
/// Strip the scheme of a pulsar service URL, returning "host:port".
pub fn broker_addr_of(url : String) -> String raise PulsarError {
  if url.strip_prefix("pulsar://") is Some(rest) {
    return rest.to_owned()
  }
  if url.strip_prefix("pulsar+ssl://") is Some(_) {
    raise LookupFailed("TLS service URLs are not supported yet: \{url}")
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
  let mut current = addr
  let mut authoritative = authoritative
  for _ in 0..<MAX_LOOKUP_REDIRECTS {
    let conn = manager.get(current)
    let lookup = @proto.CommandLookupTopic::default()
    lookup.topic = topic
    lookup.request_id = conn.next_request_id()
    lookup.authoritative = Some(authoritative)
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
        match resp.broker_service_url {
          Some(url) => return manager.get(broker_addr_of(url))
          None =>
            raise LookupFailed("broker returned no service url for \{topic}")
        }
      }
      Some(@proto.CommandLookupTopicResponse_LookupType::Redirect) =>
        match resp.broker_service_url {
          Some(url) => {
            current = broker_addr_of(url)
            authoritative = resp.authoritative.unwrap_or(false)
          }
          None =>
            raise LookupFailed("redirect without service url for \{topic}")
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
/// Number of partitions of a topic; 0 or 1 means non-partitioned.
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
