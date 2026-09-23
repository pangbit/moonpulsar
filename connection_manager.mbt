// Connection pooling by broker address.

///|
/// Caches one connection per broker address.
pub struct ConnectionManager {
  priv group : @async.TaskGroup[Unit]
  priv auth : AuthData?
  priv auth_provider : (() -> AuthData)?
  priv connections : Map[String, Connection]
  priv default_tls : Bool
  priv tls_insecure : Bool
  priv tls_trust : @tls.TrustedRoot?
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
) -> ConnectionManager {
  {
    group,
    auth,
    auth_provider,
    connections: {},
    default_tls: tls,
    tls_insecure,
    tls_trust,
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
    tls=self.default_tls,
    tls_insecure=self.tls_insecure,
    tls_trust?=self.tls_trust,
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
