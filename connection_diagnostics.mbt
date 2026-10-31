// Opt-in diagnostics deliberately omit peer addresses and free-form error text.

///|
fn connection_openssl_code(message : String) -> String {
  if message.has_prefix("error:") && message.length() >= 14 {
    let code = message[6:14]
    for unit in code {
      if !(unit is ('0'..<='9' | 'a'..<='f' | 'A'..<='F')) {
        return ""
      }
    }
    return " openssl=" + code.to_owned()
  }
  ""
}

///|
fn connection_error_kind(error : Error) -> String {
  match error {
    @tls.TlsError(message) =>
      "async-tls-error" + connection_openssl_code(message)
    @native_tls.TlsError(message) =>
      "native-tls-error" + connection_openssl_code(message)
    @tls.ConnectionClosed => "tls-peer-closed"
    @io.ReaderClosed => "reader-closed"
    @os_error.OSError(errno, context=_) => "os-error errno=\{errno}"
    ConnectionClosed => "connection-closed"
    InvalidFrame(_) => "invalid-frame"
    ChecksumMismatch => "checksum-mismatch"
    UnexpectedResponse(_) => "unexpected-response"
    AuthenticationFailed(_) => "authentication-failed"
    ServerError(code~, message=_) => "server-error code=\{code}"
    RequestTimeout => "request-timeout"
    @async.TimeoutError => "async-timeout"
    _ => "other-error"
  }
}

///|
fn connection_diagnostic(phase : String, error : Error?) -> Unit {
  if @env.get_env_var("MOONPULSAR_CONNECTION_DIAGNOSTICS") == Some("1") {
    let reason = match error {
      None => "checkpoint"
      Some(error) => connection_error_kind(error)
    }
    println("[moonpulsar diagnostic] phase=\{phase} reason=\{reason}")
  }
}
