// Connection pooling by broker address.

///|
/// Caches one connection per broker address.
pub struct ConnectionManager {
  priv group : @async.TaskGroup[Unit]
  priv auth : AuthData?
  priv auth_provider : (() -> AuthData)?
  priv async_auth_provider : (async () -> AuthData)?
  priv connections : Map[String, Connection]
  priv default_tls : Bool
  priv tls_insecure : Bool
  priv tls_trust : @tls.TrustedRoot?
  priv connection_timeout_ms : Int
  priv operation_timeout_ms : Int
  priv keepalive_interval_ms : Int
  priv listener_name : String?
  priv lookup_properties : Map[String, String]
  priv mut closed : Bool
}

///|
pub fn ConnectionManager::new(
  group : @async.TaskGroup[Unit],
  auth? : AuthData,
  auth_provider? : () -> AuthData,
  tls? : Bool = false,
  tls_insecure? : Bool = false,
  tls_trust? : @tls.TrustedRoot,
  connection_timeout_ms? : Int = 10000,
  operation_timeout_ms? : Int = 60000,
  keepalive_interval_ms? : Int = 30000,
  listener_name? : String,
  lookup_properties? : Map[String, String] = {},
  async_auth_provider? : async () -> AuthData,
) -> ConnectionManager {
  {
    group,
    auth,
    auth_provider,
    async_auth_provider,
    connections: {},
    default_tls: tls,
    tls_insecure,
    tls_trust,
    connection_timeout_ms,
    operation_timeout_ms,
    keepalive_interval_ms,
    listener_name,
    lookup_properties,
    closed: false,
  }
}

///|
/// Whether connections opened by this manager use TLS.
pub fn ConnectionManager::uses_tls(self : ConnectionManager) -> Bool {
  self.default_tls
}

///|
/// Get an open connection to `addr`, connecting on first use or after the
/// cached connection broke.
pub async fn ConnectionManager::get(
  self : ConnectionManager,
  addr : String,
) -> Connection {
  if self.closed {
    raise ConnectionClosed
  }
  let key = "\{self.default_tls}|\{addr}"
  if self.connections.get(key) is Some(conn) && !conn.is_closed() {
    return conn
  }
  let conn = Connection::connect(
    self.group,
    addr,
    auth?=self.auth,
    auth_provider?=self.auth_provider,
    async_auth_provider?=self.async_auth_provider,
    tls=self.default_tls,
    tls_insecure=self.tls_insecure,
    tls_trust?=self.tls_trust,
    connection_timeout_ms=self.connection_timeout_ms,
    timeout_ms=self.operation_timeout_ms,
    keepalive_interval_ms=self.keepalive_interval_ms,
  )
  self.connections[key] = conn
  conn
}

///|
/// Whether the manager has been closed.
pub fn ConnectionManager::is_closed(self : ConnectionManager) -> Bool {
  self.closed
}

///|
/// Close all cached connections; further `get` calls fail.
pub fn ConnectionManager::close(self : ConnectionManager) -> Unit {
  self.closed = true
  for _, conn in self.connections {
    conn.close()
  }
}
