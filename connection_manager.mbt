// Connection pooling by broker address.

///|
/// Caches one connection per broker address.
pub struct ConnectionManager {
  priv group : @async.TaskGroup[Unit]
  priv auth : AuthData?
  priv connections : Map[String, Connection]
  priv mut closed : Bool
}

///|
pub fn ConnectionManager::new(
  group : @async.TaskGroup[Unit],
  auth? : AuthData,
) -> ConnectionManager {
  { group, auth, connections: {}, closed: false, }
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
  if self.connections.get(addr) is Some(conn) && !conn.is_closed() {
    return conn
  }
  let conn = Connection::connect(self.group, addr, auth?=self.auth)
  self.connections[addr] = conn
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
