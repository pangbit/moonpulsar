// Connection pooling by broker address.

///|
/// Caches one connection per broker address.
pub struct ConnectionManager {
  group : @async.TaskGroup[Unit]
  auth : AuthData?
  connections : Map[String, Connection]
}

///|
pub fn ConnectionManager::new(
  group : @async.TaskGroup[Unit],
  auth? : AuthData,
) -> ConnectionManager {
  { group, auth, connections: {} }
}

///|
/// Get an open connection to `addr`, connecting on first use.
pub async fn ConnectionManager::get(
  self : ConnectionManager,
  addr : String,
) -> Connection raise {
  if self.connections.get(addr) is Some(conn) && !conn.is_closed() {
    return conn
  }
  let conn = Connection::connect(self.group, addr, auth?=self.auth)
  self.connections[addr] = conn
  conn
}

///|
/// Close all cached connections.
pub fn ConnectionManager::close(self : ConnectionManager) -> Unit {
  for _, conn in self.connections {
    conn.close()
  }
}
