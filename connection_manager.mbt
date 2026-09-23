// Connection pooling by broker address.

///|
/// Caches up to the configured number of connections per broker address.
pub struct ConnectionManager {
  priv group : @async.TaskGroup[Unit]
  priv auth : AuthData?
  priv auth_provider : (() -> AuthData)?
  priv async_auth_provider : (async () -> AuthData)?
  priv connections : Map[String, Array[PoolConnection]]
  priv next_connection : Map[String, Int]
  priv max_connections_per_broker : Int
  priv connection_idle_timeout_ms : Int?
  priv pool_mu : @async.Mutex
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
struct PoolConnection {
  conn : Connection
  mut last_used_ms : Int64
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
  max_connections_per_broker? : Int = 1,
  connection_idle_timeout_ms? : Int,
) -> ConnectionManager raise PulsarError {
  guard max_connections_per_broker > 0 else {
    raise InvalidFrame("max connections per broker must be positive")
  }
  if connection_idle_timeout_ms is Some(timeout) && timeout <= 0 {
    raise InvalidFrame("connection idle timeout must be positive")
  }
  let manager : ConnectionManager = {
    group,
    auth,
    auth_provider,
    async_auth_provider,
    connections: {},
    next_connection: {},
    max_connections_per_broker,
    connection_idle_timeout_ms,
    pool_mu: @async.Mutex::Mutex(),
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
  if connection_idle_timeout_ms is Some(timeout) {
    group.spawn_bg(
      async fn() {
        while !manager.closed {
          @async.sleep(timeout)
          manager.evict_idle()
        }
      },
      no_wait=true,
      allow_failure=true,
    )
  }
  manager
}

///|
fn ConnectionManager::prune_pool(
  self : ConnectionManager,
  key : String,
  now : Int64,
) -> Array[PoolConnection] {
  let pool = self.connections.get(key).unwrap_or([])
  let alive : Array[PoolConnection] = []
  for entry in pool {
    if entry.conn.is_closed() {
      continue
    }
    if self.connection_idle_timeout_ms is Some(timeout) &&
      now - entry.last_used_ms >= timeout.to_int64() &&
      entry.conn.pending.is_empty() &&
      entry.conn.producer_handlers.is_empty() &&
      entry.conn.message_handlers.is_empty() {
      entry.conn.close()
      continue
    }
    alive.push(entry)
  }
  self.connections[key] = alive
  alive
}

///|
async fn ConnectionManager::evict_idle(self : ConnectionManager) -> Unit {
  self.pool_mu.acquire()
  defer self.pool_mu.release()
  if self.closed {
    return
  }
  let now = @async.now()
  for key in self.connections.keys().collect() {
    ignore(self.prune_pool(key, now))
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
  self.pool_mu.acquire()
  defer self.pool_mu.release()
  if self.closed {
    raise ConnectionClosed
  }
  let key = "\{self.default_tls}|\{addr}"
  let now = @async.now()
  let alive = self.prune_pool(key, now)
  if alive.length() >= self.max_connections_per_broker {
    let next = self.next_connection.get(key).unwrap_or(0)
    let entry = alive[next % alive.length()]
    self.next_connection[key] = next + 1
    entry.last_used_ms = now
    return entry.conn
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
  if self.closed {
    conn.close()
    raise ConnectionClosed
  }
  alive.push({ conn, last_used_ms: now, })
  self.connections[key] = alive
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
  for _, pool in self.connections {
    for entry in pool {
      entry.conn.close()
    }
  }
}
