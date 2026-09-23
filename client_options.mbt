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
  priv reconnect_policy : RetryPolicy
  priv max_connections_per_broker : Int
  priv connection_idle_timeout_ms : Int?
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
  reconnect_policy? : RetryPolicy = RetryPolicy::default(),
  max_connections_per_broker? : Int = 1,
  connection_idle_timeout_ms? : Int,
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
  guard reconnect_policy.max_retries >= -1 &&
    reconnect_policy.initial_backoff_ms > 0 &&
    reconnect_policy.max_backoff_ms >= reconnect_policy.initial_backoff_ms else {
    raise InvalidFrame("invalid reconnect retry policy")
  }
  guard max_connections_per_broker > 0 else {
    raise InvalidFrame("max connections per broker must be positive")
  }
  if connection_idle_timeout_ms is Some(timeout) && timeout <= 0 {
    raise InvalidFrame("connection idle timeout must be positive")
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
    reconnect_policy,
    max_connections_per_broker,
    connection_idle_timeout_ms,
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

///|
fn oauth2_form_component(value : String) -> String {
  let encoded = @buffer.Buffer()
  let digits = b"0123456789ABCDEF"
  for byte in @utf8.encode(value) {
    let code = byte.to_int()
    if (code >= 65 && code <= 90) ||
      (code >= 97 && code <= 122) ||
      (code >= 48 && code <= 57) ||
      code == 45 ||
      code == 46 ||
      code == 95 ||
      code == 126 {
      encoded.write_byte(byte)
    } else {
      encoded.write_byte(b'%')
      encoded.write_byte(digits[code >> 4])
      encoded.write_byte(digits[code & 15])
    }
  }
  @utf8.decode(encoded.to_bytes()) catch {
    _ => ""
  }
}

///|
/// OAuth2 client-credentials authentication using `client_secret_post`.
/// The provider requests a fresh access token on each connection or broker
/// challenge; pass it as `ClientOptions::new(auth_provider=...)`.
pub fn oauth2_client_credentials_provider(
  token_endpoint : String,
  client_id : String,
  client_secret : String,
  audience? : String,
  scope? : String,
) -> (async () -> AuthData) raise PulsarError {
  guard token_endpoint != "" && client_id != "" && client_secret != "" else {
    raise InvalidFrame("OAuth2 endpoint, client id and secret are required")
  }
  let mut form = "grant_type=client_credentials&client_id=" +
    oauth2_form_component(client_id) +
    "&client_secret=" +
    oauth2_form_component(client_secret)
  if audience is Some(value) {
    form += "&audience=" + oauth2_form_component(value)
  }
  if scope is Some(value) {
    form += "&scope=" + oauth2_form_component(value)
  }
  async fn() {
    let headers = {}
    headers[@http.CaseInsensitiveString("Content-Type")] = "application/x-www-form-urlencoded"
    let (response, data) = @http.request(
      token_endpoint,
      @http.RequestMethod::Post,
      headers,
      @utf8.encode(form),
    )
    guard response.code >= 200 && response.code < 300 else {
      raise ServerError(
        code=response.code,
        message="OAuth2 token endpoint rejected credentials",
      )
    }
    let payload = data.binary() catch {
      _ => raise InvalidFrame("OAuth2 token response is unreadable")
    }
    let json = @json.parse(@utf8.decode(payload)) catch {
      _ => raise InvalidFrame("OAuth2 token response is not JSON")
    }
    guard json is Object(fields) &&
      fields.get("access_token") is Some(String(token)) &&
      token != "" else {
      raise InvalidFrame("OAuth2 token response has no access token")
    }
    { method_name: "token", data: @utf8.encode(token), }
  }
}
