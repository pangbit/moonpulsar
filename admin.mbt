// Minimal Pulsar admin API (REST, v2) over HTTP.

///|
/// Components of a fully-qualified topic name.
pub(all) struct TopicName {
  /// "persistent" or "non-persistent"
  domain : String
  tenant : String
  namespace : String
  topic : String
} derive(Eq, Debug)

///|
fn safe_admin_segment(segment : StringView) -> Bool {
  segment != "" &&
  !segment.contains("/") &&
  !segment.contains("?") &&
  !segment.contains("#") &&
  !segment.contains("%")
}

///|
/// Parse "persistent://tenant/namespace/topic" or its non-persistent form.
pub fn TopicName::parse(full : String) -> TopicName raise PulsarError {
  let (domain, rest) = match full.strip_prefix("persistent://") {
    Some(rest) => ("persistent", rest.to_owned())
    None =>
      match full.strip_prefix("non-persistent://") {
        Some(rest) => ("non-persistent", rest.to_owned())
        None =>
          raise LookupFailed("expected a fully qualified topic name: \{full}")
      }
  }
  match rest.split("/").collect() {
    [tenant, namespace, topic] if safe_admin_segment(tenant) &&
      safe_admin_segment(namespace) &&
      safe_admin_segment(topic) =>
      {
        domain,
        tenant: tenant.to_owned(),
        namespace: namespace.to_owned(),
        topic: topic.to_owned(),
      }
    _ => raise LookupFailed("malformed topic name: \{full}")
  }
}

///|
/// The Pulsar admin REST API (usually http://host:8080).
pub struct Admin {
  priv base_url : String
  priv token : String?
}

///|
/// Create an admin client, e.g. `Admin::new("http://localhost:8080")`.
pub fn Admin::new(base_url : String, token? : String) -> Admin {
  { base_url, token, }
}

///|
async fn Admin::request(
  self : Admin,
  method : @http.RequestMethod,
  path : String,
  body? : Bytes,
) -> (Int, Bytes) {
  let headers = {}
  if body is Some(_) {
    headers[@http.CaseInsensitiveString("Content-Type")] = "application/json"
  }
  if self.token is Some(t) {
    headers[@http.CaseInsensitiveString("Authorization")] = "Bearer " + t
  }
  let url = self.base_url + path
  let (resp, data) = match (method, body) {
    (@http.RequestMethod::Get, _) => @http.get(url, headers~)
    (@http.RequestMethod::Delete, _) => @http.request(url, method, headers, b"")
    (_, Some(b)) => @http.request(url, method, headers, b)
    (_, None) => @http.request(url, method, headers, b"")
  }
  let payload = data.binary() catch {
    _ => raise InvalidFrame("admin: unreadable response body")
  }
  if resp.code < 200 || resp.code >= 300 {
    let text = @utf8.decode(payload) catch { _ => "<binary>" }
    raise ServerError(code=resp.code, message="admin \{path}: \{text}")
  }
  (resp.code, payload)
}

///|
fn topic_path(name : TopicName) -> String {
  "/admin/v2/\{name.domain}/\{name.tenant}/\{name.namespace}/\{name.topic}"
}

///|
fn subscription_path(
  name : TopicName,
  subscription : String,
) -> String raise PulsarError {
  guard safe_admin_segment(subscription) else {
    raise LookupFailed("invalid subscription name")
  }
  topic_path(name) + "/subscription/" + subscription
}

///|
/// Create a non-partitioned topic.
pub async fn Admin::create_topic(self : Admin, topic : String) -> Unit {
  let name = TopicName::parse(topic)
  ignore(self.request(Put, topic_path(name)))
}

///|
/// Create a partitioned topic with the given number of partitions.
pub async fn Admin::create_partitioned_topic(
  self : Admin,
  topic : String,
  partitions : Int,
) -> Unit {
  guard partitions > 0 else {
    raise LookupFailed("partition count must be positive")
  }
  let name = TopicName::parse(topic)
  ignore(
    self.request(
      Put,
      topic_path(name) + "/partitions",
      body=@utf8.encode(partitions.to_string()),
    ),
  )
}

///|
/// Increase the number of partitions of an existing topic.
pub async fn Admin::update_partitioned_topic(
  self : Admin,
  topic : String,
  partitions : Int,
) -> Unit {
  guard partitions > 0 else {
    raise LookupFailed("partition count must be positive")
  }
  let name = TopicName::parse(topic)
  ignore(
    self.request(
      Post,
      topic_path(name) + "/partitions",
      body=@utf8.encode(partitions.to_string()),
    ),
  )
}

///|
/// Return the partition count, or zero for a non-partitioned topic.
pub async fn Admin::partition_count(self : Admin, topic : String) -> Int {
  let name = TopicName::parse(topic)
  let (_, payload) = self.request(Get, topic_path(name) + "/partitions")
  let text = @utf8.decode(payload) catch {
    _ => raise InvalidFrame("admin: response is not UTF-8")
  }
  let json = @json.parse(text) catch {
    _ => raise InvalidFrame("admin: invalid JSON response")
  }
  guard json is Object(fields) &&
    fields.get("partitions") is Some(Number(n, ..)) &&
    n >= 0.0 &&
    n.to_int().to_double() == n else {
    raise InvalidFrame("admin: expected integer partition count")
  }
  n.to_int()
}

///|
/// Delete a partitioned topic.
pub async fn Admin::delete_partitioned_topic(
  self : Admin,
  topic : String,
  force? : Bool = false,
) -> Unit {
  let name = TopicName::parse(topic)
  let force_q = if force { "?force=true" } else { "" }
  ignore(self.request(Delete, topic_path(name) + "/partitions" + force_q))
}

///|
/// Delete a non-partitioned topic.
pub async fn Admin::delete_topic(
  self : Admin,
  topic : String,
  force? : Bool = false,
) -> Unit {
  let name = TopicName::parse(topic)
  let force_q = if force { "?force=true" } else { "" }
  ignore(self.request(Delete, topic_path(name) + force_q))
}

///|
fn admin_string_array(payload : Bytes) -> Array[String] raise PulsarError {
  let text = @utf8.decode(payload) catch {
    _ => raise InvalidFrame("admin: response is not UTF-8")
  }
  let json = @json.parse(text) catch {
    _ => raise InvalidFrame("admin: invalid JSON response")
  }
  guard json is Array(items) else {
    raise InvalidFrame("admin: expected JSON array")
  }
  let result = []
  for item in items {
    guard item is String(s) else {
      raise InvalidFrame("admin: expected string topic name")
    }
    result.push(s)
  }
  result
}

///|
/// List topics of a namespace.
pub async fn Admin::list_topics(
  self : Admin,
  tenant : String,
  namespace : String,
  domain? : String = "persistent",
) -> Array[String] {
  guard domain == "persistent" || domain == "non-persistent" else {
    raise LookupFailed("invalid topic domain: \{domain}")
  }
  guard safe_admin_segment(tenant) && safe_admin_segment(namespace) else {
    raise LookupFailed("invalid namespace")
  }
  let (_, payload) = self.request(
    Get,
    "/admin/v2/\{domain}/\{tenant}/\{namespace}",
  )
  admin_string_array(payload)
}

///|
/// List partitioned topics of a namespace.
pub async fn Admin::list_partitioned_topics(
  self : Admin,
  tenant : String,
  namespace : String,
  domain? : String = "persistent",
) -> Array[String] {
  guard domain == "persistent" || domain == "non-persistent" else {
    raise LookupFailed("invalid topic domain: \{domain}")
  }
  guard safe_admin_segment(tenant) && safe_admin_segment(namespace) else {
    raise LookupFailed("invalid namespace")
  }
  let (_, payload) = self.request(
    Get,
    "/admin/v2/\{domain}/\{tenant}/\{namespace}/partitioned",
  )
  admin_string_array(payload)
}

///|
/// Topic statistics as raw JSON (see Pulsar's TopicStats schema).
pub async fn Admin::topic_stats(self : Admin, topic : String) -> Json {
  let name = TopicName::parse(topic)
  let (_, payload) = self.request(Get, topic_path(name) + "/stats")
  let text = @utf8.decode(payload) catch {
    _ => raise InvalidFrame("admin: response is not UTF-8")
  }
  @json.parse(text) catch {
    _ => raise InvalidFrame("admin: invalid JSON response")
  }
}

///|
/// Partitioned topic statistics as raw JSON. Include per-partition detail
/// when `per_partition` is true.
pub async fn Admin::partitioned_topic_stats(
  self : Admin,
  topic : String,
  per_partition? : Bool = false,
) -> Json {
  let name = TopicName::parse(topic)
  let detail = if per_partition { "true" } else { "false" }
  let (_, payload) = self.request(
    Get,
    topic_path(name) + "/partitioned-stats?perPartition=" + detail,
  )
  let text = @utf8.decode(payload) catch {
    _ => raise InvalidFrame("admin: response is not UTF-8")
  }
  @json.parse(text) catch {
    _ => raise InvalidFrame("admin: invalid JSON response")
  }
}

///|
/// List subscription names for a topic.
pub async fn Admin::list_subscriptions(
  self : Admin,
  topic : String,
) -> Array[String] {
  let name = TopicName::parse(topic)
  let (_, payload) = self.request(Get, topic_path(name) + "/subscriptions")
  let text = @utf8.decode(payload) catch {
    _ => raise InvalidFrame("admin: response is not UTF-8")
  }
  let json = @json.parse(text) catch {
    _ => raise InvalidFrame("admin: invalid JSON response")
  }
  guard json is Array(items) else {
    raise InvalidFrame("admin: expected JSON array")
  }
  let names = []
  for item in items {
    guard item is String(value) else {
      raise InvalidFrame("admin: expected subscription name")
    }
    names.push(value)
  }
  names
}

///|
/// Delete a topic subscription.
pub async fn Admin::delete_subscription(
  self : Admin,
  topic : String,
  subscription : String,
  force? : Bool = false,
) -> Unit {
  let name = TopicName::parse(topic)
  let force_q = if force { "?force=true" } else { "" }
  ignore(self.request(Delete, subscription_path(name, subscription) + force_q))
}

///|
/// Create a durable subscription at the earliest or latest position.
pub async fn Admin::create_subscription(
  self : Admin,
  topic : String,
  subscription : String,
  initial_position? : SubscriptionInitialPosition = Latest,
) -> Unit {
  let name = TopicName::parse(topic)
  let cursor = match initial_position {
    Earliest => b"{\"ledgerId\":-1,\"entryId\":-1}"
    Latest =>
      b"{\"ledgerId\":9223372036854775807,\"entryId\":9223372036854775807}"
  }
  ignore(self.request(Put, subscription_path(name, subscription), body=cursor))
}

///|
/// Advance a subscription by `count` messages.
pub async fn Admin::skip_messages(
  self : Admin,
  topic : String,
  subscription : String,
  count : Int,
) -> Unit {
  guard count > 0 else { raise LookupFailed("skip count must be positive") }
  let name = TopicName::parse(topic)
  ignore(
    self.request(Post, subscription_path(name, subscription) + "/skip/\{count}"),
  )
}

///|
/// Clear all pending messages for a subscription.
pub async fn Admin::skip_all_messages(
  self : Admin,
  topic : String,
  subscription : String,
) -> Unit {
  let name = TopicName::parse(topic)
  ignore(
    self.request(Post, subscription_path(name, subscription) + "/skip_all"),
  )
}

///|
/// Expire messages older than the given number of seconds.
pub async fn Admin::expire_messages(
  self : Admin,
  topic : String,
  subscription : String,
  older_than_seconds : Int,
) -> Unit {
  guard older_than_seconds > 0 else {
    raise LookupFailed("expiration interval must be positive")
  }
  let name = TopicName::parse(topic)
  ignore(
    self.request(
      Post,
      subscription_path(name, subscription) +
      "/expireMessages/\{older_than_seconds}",
    ),
  )
}
