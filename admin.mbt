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
    [tenant, namespace, topic] if tenant != "" && namespace != "" && topic != "" =>
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
  let (_, payload) = self.request(
    Get,
    "/admin/v2/\{domain}/\{tenant}/\{namespace}",
  )
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
  guard subscription != "" && !subscription.contains("/") else {
    raise LookupFailed("invalid subscription name")
  }
  let name = TopicName::parse(topic)
  let force_q = if force { "?force=true" } else { "" }
  ignore(
    self.request(
      Delete,
      topic_path(name) + "/subscription/" + subscription + force_q,
    ),
  )
}
