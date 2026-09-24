// Broker connection: TCP transport, handshake, keepalive and
// request/response correlation for the Pulsar binary protocol.

///|
/// Protocol version advertised in the CONNECT command.
const PROTOCOL_VERSION : Int = 21

///|
/// Default interval between keepalive PING commands.
const KEEPALIVE_INTERVAL_MS : Int = 30000

///|
/// Default timeout for request/response commands.
const DEFAULT_REQUEST_TIMEOUT_MS : Int = 60000

///|
struct PendingRequest {
  reply : @aqueue.Queue[@proto.BaseCommand]
}

///|
/// A single connection to a Pulsar broker speaking the binary protocol.
pub struct Connection {
  priv reader : &@io.Reader
  priv writer : &@io.Writer
  priv auth : AuthData?
  priv auth_provider : (() -> AuthData)?
  priv async_auth_provider : (async () -> AuthData)?
  priv close_transport : () -> Unit
  priv mut next_id : UInt64
  priv pending : Map[UInt64, PendingRequest]
  /// consumer_id -> handler for MESSAGE / ACK_RESPONSE / CLOSE_CONSUMER
  priv message_handlers : Map[UInt64, (IncomingFrame) -> Unit]
  /// producer_id -> handler for SEND_RECEIPT / SEND_ERROR / CLOSE_PRODUCER
  priv producer_handlers : Map[UInt64, (@proto.BaseCommand) -> Unit]
  /// notified once when the connection breaks
  priv close_handlers : Array[(Error?) -> Unit]
  priv mut closed : Bool
  /// signalled on close to interrupt the keepalive timer
  priv stop : @aqueue.Queue[Unit]
  priv write_mu : @async.Mutex
  priv mut remote_version : String
  priv mut remote_protocol_version : Int
  priv mut max_message_size : Int?
  priv request_timeout_ms : Int
}

///|
/// Authentication data attached to the CONNECT command.
pub(all) struct AuthData {
  method_name : String
  data : Bytes
} derive(Debug)

///|
/// Connect to `addr` ("host:port"), perform the protocol handshake and
/// start the background reader/keepalive tasks in `group`.
pub async fn Connection::connect(
  group : @async.TaskGroup[Unit],
  addr : String,
  auth? : AuthData,
  auth_provider? : () -> AuthData,
  tls? : Bool = false,
  /// skip certificate verification (self-signed / testing only)
  tls_insecure? : Bool = false,
  tls_trust? : @tls.TrustedRoot,
  tls_identity? : TlsIdentity,
  tls_identity_provider? : async () -> TlsIdentity,
  tls_min_version? : TlsVersion,
  tls_max_version? : TlsVersion,
  tls_cipher_list? : String,
  tls13_ciphersuites? : String,
  keepalive_interval_ms? : Int = KEEPALIVE_INTERVAL_MS,
  timeout_ms? : Int = DEFAULT_REQUEST_TIMEOUT_MS,
  connection_timeout_ms? : Int = 10000,
  async_auth_provider? : async () -> AuthData,
) -> Connection {
  guard timeout_ms > 0 && connection_timeout_ms > 0 && keepalive_interval_ms > 0 else {
    raise InvalidFrame("connection timeouts must be positive")
  }
  guard tls ||
    (
      tls_identity is None &&
      tls_identity_provider is None &&
      tls_min_version is None &&
      tls_max_version is None &&
      tls_cipher_list is None &&
      tls13_ciphersuites is None
    ) else {
    raise InvalidFrame("TLS settings require TLS transport")
  }
  let auth = match async_auth_provider {
    Some(provider) => Some(provider())
    None =>
      match auth_provider {
        Some(provider) => Some(provider())
        None =>
          if auth is Some(_) {
            auth
          } else if tls_identity is Some(_) || tls_identity_provider is Some(_) {
            Some({ method_name: "tls", data: b"", })
          } else {
            None
          }
      }
  }
  let (host, port) = split_host_port(addr)
  let tcp = @async.with_timeout(connection_timeout_ms, async fn() {
    @socket.Tcp::connect_to_host(host, port~)
  }) catch {
    _ => raise ConnectFailed(addr)
  }
  errdefer tcp.close()
  let (reader, writer, close_transport) : (&@io.Reader, &@io.Writer, () -> Unit) = if tls {
    let trust = match tls_trust {
      Some(root) => root
      None =>
        if tls_insecure {
          @tls.TrustedRoot::NoVerification
        } else {
          @tls.TrustedRoot::SystemRoot
        }
    }
    if tls_identity is Some(_) ||
      tls_identity_provider is Some(_) ||
      tls_min_version is Some(_) ||
      tls_max_version is Some(_) ||
      tls_cipher_list is Some(_) ||
      tls13_ciphersuites is Some(_) {
      let identity = match tls_identity_provider {
        Some(provider) => Some(provider())
        None => tls_identity
      }
      let ca_file = match trust {
        @tls.TrustedRoot::CustomPemFile(path) => path
        _ => ""
      }
      let native = @async.with_timeout(connection_timeout_ms, async fn() {
        @native_tls.NativeTls::connect(
          tcp,
          host,
          ca_file~,
          insecure=trust is @tls.TrustedRoot::NoVerification,
          cert_file=identity.map(fn(id) { id.certificate_file }).unwrap_or(""),
          key_file=identity.map(fn(id) { id.private_key_file }).unwrap_or(""),
          min_version=tls_min_version
            .map(fn(v) { v.native_value() })
            .unwrap_or(0),
          max_version=tls_max_version
            .map(fn(v) { v.native_value() })
            .unwrap_or(0),
          cipher_list=tls_cipher_list.unwrap_or(""),
          tls13_ciphersuites=tls13_ciphersuites.unwrap_or(""),
        )
      }) catch {
        e => raise ConnectFailed("TLS handshake with \{addr} failed: \{e}")
      }
      (native, native, fn() { native.close() })
    } else {
      let tls_conn = @async.with_timeout(connection_timeout_ms, async fn() {
        @tls.Tls::client(tcp, host~, trust~)
      }) catch {
        e => raise ConnectFailed("TLS handshake with \{addr} failed: \{e}")
      }
      (tls_conn, tls_conn, fn() { tls_conn.close() })
    }
  } else {
    (tcp, tcp, fn() { tcp.close() })
  }
  let conn = Connection::{
    reader,
    writer,
    auth,
    auth_provider,
    async_auth_provider,
    close_transport,
    next_id: 0,
    pending: {},
    message_handlers: {},
    producer_handlers: {},
    close_handlers: [],
    closed: false,
    stop: @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1)),
    write_mu: @async.Mutex::Mutex(),
    remote_version: "",
    remote_protocol_version: 0,
    max_message_size: None,
    request_timeout_ms: timeout_ms,
  }
  errdefer conn.close()
  // handshake before any background traffic
  let connect = @proto.CommandConnect::default()
  connect.client_version = "moonpulsar 0.1.0"
  connect.protocol_version = Some(PROTOCOL_VERSION)
  if auth is Some(a) {
    connect.auth_method_name = Some(a.method_name)
    connect.auth_data = Some(a.data)
  }
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::CONNECT
  cmd.connect = Some(connect)
  conn.send_bytes(encode_command_frame(cmd))
  let mut connected = false
  for _ in 0..<8 {
    let reply = @async.with_timeout(timeout_ms, async fn() { conn.read_frame() }) catch {
      @async.TimeoutError => raise ConnectFailed("handshake timeout: \{addr}")
      e => raise e
    }
    match reply.cmd.type_ {
      @proto.BaseCommand_Type::CONNECTED => {
        guard reply.cmd.connected is Some(info) else {
          raise ConnectFailed("CONNECTED without broker information")
        }
        conn.remote_version = info.server_version
        conn.remote_protocol_version = info.protocol_version.unwrap_or(0)
        conn.max_message_size = info.max_message_size
        connected = true
        break
      }
      @proto.BaseCommand_Type::AUTH_CHALLENGE => conn.respond_to_challenge()
      @proto.BaseCommand_Type::ERROR =>
        if reply.cmd.error is Some(err) {
          raise ConnectFailed("broker rejected CONNECT: " + err.message)
        } else {
          raise ConnectFailed("broker rejected CONNECT")
        }
      _ => raise ConnectFailed("unexpected handshake response from \{addr}")
    }
  }
  if !connected {
    raise ConnectFailed("too many authentication challenges from \{addr}")
  }
  group.spawn_bg(
    async fn() { conn.reader_loop() },
    no_wait=true,
    allow_failure=true,
  )
  group.spawn_bg(
    async fn() { conn.keepalive_loop(keepalive_interval_ms) },
    no_wait=true,
    allow_failure=true,
  )
  conn
}

///|
async fn Connection::respond_to_challenge(self : Connection) -> Unit {
  let current = match self.async_auth_provider {
    Some(provider) => Some(provider())
    None =>
      match self.auth_provider {
        Some(provider) => Some(provider())
        None => self.auth
      }
  }
  guard current is Some(auth) else {
    raise ConnectFailed("broker requested authentication without credentials")
  }
  let response = @proto.CommandAuthResponse::default()
  response.client_version = Some("moonpulsar 0.1.0")
  response.protocol_version = Some(PROTOCOL_VERSION)
  let data = @proto.AuthData::default()
  data.auth_method_name = Some(auth.method_name)
  data.auth_data = Some(auth.data)
  response.response = Some(data)
  let cmd = @proto.BaseCommand::default()
  cmd.type_ = @proto.BaseCommand_Type::AUTH_RESPONSE
  cmd.auth_response = Some(response)
  self.send_command(cmd)
}

///|
async fn Connection::read_frame(self : Connection) -> IncomingFrame {
  read_frame(self.reader)
}

///|
async fn Connection::send_bytes(self : Connection, bytes : Bytes) -> Unit {
  if self.closed {
    raise ConnectionClosed
  }
  self.write_mu.acquire()
  defer self.write_mu.release()
  self.writer.write(bytes) catch {
    e => {
      self.notify_closed(Some(e))
      raise ConnectionClosed
    }
  }
}

///|
/// Send a fire-and-forget command frame (ACK, FLOW, PONG, ...).
pub async fn Connection::send_command(
  self : Connection,
  cmd : @proto.BaseCommand,
) -> Unit {
  self.send_bytes(encode_command_frame(cmd))
}

///|
/// Send a frame with attached message metadata and payload (SEND).
pub async fn Connection::send_payload_frame(
  self : Connection,
  cmd : @proto.BaseCommand,
  metadata : @proto.MessageMetadata,
  payload : Bytes,
) -> Unit {
  self.send_bytes(encode_send_frame(cmd, metadata, payload))
}

///|
/// Allocate a fresh request id for this connection.
pub fn Connection::next_request_id(self : Connection) -> UInt64 {
  let id = self.next_id
  self.next_id += 1
  id
}

///|
fn request_id_of(cmd : @proto.BaseCommand) -> UInt64? {
  if cmd.lookup_topic is Some(c) {
    return Some(c.request_id)
  }
  if cmd.lookup_topic_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.partition_metadata is Some(c) {
    return Some(c.request_id)
  }
  if cmd.partition_metadata_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.producer is Some(c) {
    return Some(c.request_id)
  }
  if cmd.producer_success is Some(c) {
    return Some(c.request_id)
  }
  if cmd.subscribe is Some(c) {
    return Some(c.request_id)
  }
  if cmd.unsubscribe is Some(c) {
    return Some(c.request_id)
  }
  if cmd.close_producer is Some(c) {
    return Some(c.request_id)
  }
  if cmd.close_consumer is Some(c) {
    return Some(c.request_id)
  }
  if cmd.success is Some(c) {
    return Some(c.request_id)
  }
  if cmd.error is Some(c) {
    return Some(c.request_id)
  }
  if cmd.get_last_message_id is Some(c) {
    return Some(c.request_id)
  }
  if cmd.get_last_message_id_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.get_schema is Some(c) {
    return Some(c.request_id)
  }
  if cmd.get_schema_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.seek is Some(c) {
    return Some(c.request_id)
  }
  if cmd.ack is Some(c) {
    return c.request_id
  }
  if cmd.ack_response is Some(c) {
    return c.request_id
  }
  if cmd.get_topics_of_namespace is Some(c) {
    return Some(c.request_id)
  }
  if cmd.get_topics_of_namespace_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.tc_client_connect_request is Some(c) {
    return Some(c.request_id)
  }
  if cmd.tc_client_connect_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.new_txn is Some(c) {
    return Some(c.request_id)
  }
  if cmd.add_partition_to_txn is Some(c) {
    return Some(c.request_id)
  }
  if cmd.add_subscription_to_txn is Some(c) {
    return Some(c.request_id)
  }
  if cmd.end_txn is Some(c) {
    return Some(c.request_id)
  }
  if cmd.new_txn_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.add_partition_to_txn_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.add_subscription_to_txn_response is Some(c) {
    return Some(c.request_id)
  }
  if cmd.end_txn_response is Some(c) {
    return Some(c.request_id)
  }
  None
}

///|
/// Send a request command and wait for the correlated response.
/// `CommandError` responses are raised as `ServerError`.
pub async fn Connection::request(
  self : Connection,
  cmd : @proto.BaseCommand,
  timeout_ms? : Int,
) -> @proto.BaseCommand {
  let timeout_ms = timeout_ms.unwrap_or(self.request_timeout_ms)
  guard request_id_of(cmd) is Some(id) else {
    raise UnexpectedResponse("command carries no request id")
  }
  let pending : PendingRequest = {
    reply: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
  }
  // register before sending: send_command awaits internally, and the reader
  // task may dispatch the response during those awaits
  self.pending[id] = pending
  defer self.pending.remove(id)
  self.send_command(cmd)
  let mut waiting_for_exclusive = false
  while true {
    let reply = if waiting_for_exclusive {
      pending.reply.get()
    } else {
      @async.with_timeout(timeout_ms, async fn() { pending.reply.get() }) catch {
        @async.TimeoutError => raise RequestTimeout
        e => raise e
      }
    }
    if reply.producer_success is Some(success) &&
      success.producer_ready == Some(false) {
      waiting_for_exclusive = true
      continue
    }
    if reply.type_ is @proto.BaseCommand_Type::ERROR && reply.error is Some(err) {
      raise ServerError(
        code=err.error.to_enum().0.reinterpret_as_int(),
        message=err.message,
      )
    }
    return reply
  }
  raise UnexpectedResponse("unreachable request state")
}

///|
fn Connection::notify_closed(self : Connection, error : Error?) -> Unit {
  if self.closed {
    return
  }
  self.closed = true
  ignore(self.stop.try_put(()) catch { _ => false })
  for id, pending in self.pending {
    pending.reply.close(error?, clear=true)
    ignore(id)
  }
  self.pending.clear()
  for handler in self.close_handlers {
    handler(error)
  }
}

///|
/// Register a handler invoked once when the connection breaks.
pub fn Connection::on_close(
  self : Connection,
  handler : (Error?) -> Unit,
) -> Unit {
  self.close_handlers.push(handler)
}

///|
/// Close the connection and fail all pending requests.
pub fn Connection::close(self : Connection) -> Unit {
  self.notify_closed(None)
  (self.close_transport)()
}

///|
/// Server version reported by the broker during the handshake.
pub fn Connection::server_version(self : Connection) -> String {
  self.remote_version
}

///|
/// Protocol version reported by the broker during the handshake.
pub fn Connection::remote_protocol_version(self : Connection) -> Int {
  self.remote_protocol_version
}

///|
/// Maximum frame payload announced by the broker, when available.
pub fn Connection::max_message_size(self : Connection) -> Int? {
  self.max_message_size
}

///|
/// Whether the connection is still usable.
pub fn Connection::is_closed(self : Connection) -> Bool {
  self.closed
}

///|
async fn Connection::reader_loop(self : Connection) -> Unit {
  while true {
    if self.closed {
      return
    }
    let frame = self.read_frame() catch {
      e => {
        self.notify_closed(Some(e))
        return
      }
    }
    self.dispatch(frame)
  }
}

///|
async fn Connection::keepalive_loop(
  self : Connection,
  interval_ms : Int,
) -> Unit {
  while true {
    // sleep, but wake up immediately when the connection closes
    let stopped = @async.with_timeout_opt(interval_ms, async fn() {
      self.stop.get()
    })
    if stopped is Some(_) || self.closed {
      return
    }
    let cmd = @proto.BaseCommand::default()
    cmd.type_ = @proto.BaseCommand_Type::PING
    cmd.ping = Some(@proto.CommandPing::default())
    self.send_command(cmd) catch {
      _ => return
    }
  }
}

///|
/// Split "host:port" into (host, port); a missing port defaults to 6650.
pub fn split_host_port(addr : String) -> (String, Int) raise PulsarError {
  match addr.split(":").collect() {
    [host, port] =>
      (
        host.to_owned(),
        @string.parse_int(port) catch {
          _ => raise InvalidFrame("invalid port in address: \{addr}")
        },
      )
    [host] => (host.to_owned(), 6650)
    _ => raise InvalidFrame("invalid address: \{addr}")
  }
}

///|
async fn Connection::dispatch(self : Connection, frame : IncomingFrame) -> Unit {
  let cmd = frame.cmd
  match cmd.type_ {
    @proto.BaseCommand_Type::AUTH_CHALLENGE =>
      self.respond_to_challenge() catch {
        e => {
          self.notify_closed(Some(e))
          (self.close_transport)()
        }
      }
    @proto.BaseCommand_Type::PING => {
      let pong = @proto.BaseCommand::default()
      pong.type_ = @proto.BaseCommand_Type::PONG
      pong.pong = Some(@proto.CommandPong::default())
      self.send_command(pong) catch {
        _ => ()
      }
    }
    @proto.BaseCommand_Type::MESSAGE =>
      if cmd.message is Some(message) &&
        self.message_handlers.get(message.consumer_id) is Some(handler) {
        handler(frame)
      }
    @proto.BaseCommand_Type::ACK_RESPONSE =>
      if cmd.ack_response is Some(resp) {
        if resp.request_id is Some(id) && self.pending.get(id) is Some(pending) {
          ignore(pending.reply.try_put(cmd))
        } else if self.message_handlers.get(resp.consumer_id) is Some(handler) {
          handler(frame)
        }
      }
    @proto.BaseCommand_Type::CLOSE_CONSUMER =>
      if cmd.close_consumer is Some(c) &&
        self.message_handlers.get(c.consumer_id) is Some(handler) {
        handler(frame)
      }
    @proto.BaseCommand_Type::ACTIVE_CONSUMER_CHANGE =>
      if cmd.active_consumer_change is Some(c) &&
        self.message_handlers.get(c.consumer_id) is Some(handler) {
        handler(frame)
      }
    @proto.BaseCommand_Type::REACHED_END_OF_TOPIC =>
      if cmd.reached_end_of_topic is Some(c) &&
        self.message_handlers.get(c.consumer_id) is Some(handler) {
        handler(frame)
      }
    @proto.BaseCommand_Type::SEND_RECEIPT =>
      if cmd.send_receipt is Some(receipt) &&
        self.producer_handlers.get(receipt.producer_id) is Some(handler) {
        handler(cmd)
      }
    @proto.BaseCommand_Type::SEND_ERROR =>
      if cmd.send_error is Some(err) &&
        self.producer_handlers.get(err.producer_id) is Some(handler) {
        handler(cmd)
      }
    @proto.BaseCommand_Type::CLOSE_PRODUCER =>
      if cmd.close_producer is Some(c) &&
        self.producer_handlers.get(c.producer_id) is Some(handler) {
        handler(cmd)
      }
    _ =>
      if request_id_of(cmd) is Some(id) && self.pending.get(id) is Some(pending) {
        if !(cmd.producer_success is Some(success) &&
          success.producer_ready == Some(false)) {
          self.pending.remove(id)
        }
        ignore(pending.reply.try_put(cmd))
      }
  }
}
