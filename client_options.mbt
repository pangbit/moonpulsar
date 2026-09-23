// Connection settings shared by the service connection, lookups and redirects.

///|
/// Options for a Pulsar client. Durations are milliseconds.
pub struct ClientOptions {
  priv service_url : String
  priv auth : &Authentication?
  priv auth_provider : (async () -> AuthData)?
  priv connection_timeout_ms : Int
  priv operation_timeout_ms : Int
  priv keepalive_interval_ms : Int
  priv tls_insecure : Bool
  priv ca_pem_file : String?
  priv listener_name : String?
  priv lookup_properties : Map[String, String]
}

///|
pub fn ClientOptions::new(
  service_url : String,
  auth? : &Authentication,
  auth_provider? : async () -> AuthData,
  connection_timeout_ms? : Int = 10000,
  operation_timeout_ms? : Int = 60000,
  keepalive_interval_ms? : Int = 30000,
  tls_insecure? : Bool = false,
  ca_pem_file? : String,
  listener_name? : String,
  lookup_properties? : Map[String, String] = {},
) -> ClientOptions raise PulsarError {
  guard connection_timeout_ms > 0 &&
    operation_timeout_ms > 0 &&
    keepalive_interval_ms > 0 else {
    raise InvalidFrame("client timeouts must be positive")
  }
  guard !(auth is Some(_) && auth_provider is Some(_)) else {
    raise InvalidFrame("choose one authentication provider")
  }
  if ca_pem_file is Some(path) && path == "" {
    raise InvalidFrame("TLS CA path must not be empty")
  }
  guard !(tls_insecure && ca_pem_file is Some(_)) else {
    raise InvalidFrame("choose TLS verification or insecure TLS")
  }
  {
    service_url,
    auth,
    auth_provider,
    connection_timeout_ms,
    operation_timeout_ms,
    keepalive_interval_ms,
    tls_insecure,
    ca_pem_file,
    listener_name,
    lookup_properties,
  }
}

///|
/// Read a token each time a connection or authentication challenge needs it.
/// This permits token rotation without reconstructing the client.
pub fn token_file_provider(path : String) -> async () -> AuthData {
  async fn() {
    let token = @fs.read_file(path).text().trim().to_owned()
    guard token != "" else { raise InvalidFrame("token file is empty") }
    { method_name: "token", data: @utf8.encode(token), }
  }
}
