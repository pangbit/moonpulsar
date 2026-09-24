// The async HTTP client does not expose a client-certificate transport. Keep
// this HTTPS GET narrowly scoped to the ZTS role-token exchange.

///|
async fn athenz_certificate_get(
  url : String,
  certificate_file : String,
  private_key_file : String,
  ca_pem_file : String?,
) -> (Int, Bytes) {
  @async.with_timeout(10000, async fn() {
    guard url.split_once("https://") is Some(("", rest)) &&
      rest.split_once("/") is Some((authority, path_tail)) &&
      authority != "" else {
      raise AuthenticationFailed("invalid Athenz HTTPS ZTS URL")
    }
    let (host, port) = match authority.split_once(":") {
      Some((host, port_text)) => {
        let port = @string.parse_int(port_text) catch {
          _ => raise AuthenticationFailed("invalid Athenz ZTS port")
        }
        guard host != "" && port > 0 && port < 65536 else {
          raise AuthenticationFailed("invalid Athenz ZTS host or port")
        }
        (host.to_owned(), port)
      }
      None => (authority.to_owned(), 443)
    }
    let tcp = @socket.Tcp::connect_to_host(host, port~)
    errdefer tcp.close()
    let tls = @native_tls.NativeTls::connect(
      tcp,
      host,
      ca_file=ca_pem_file.unwrap_or(""),
      cert_file=certificate_file,
      key_file=private_key_file,
    )
    defer tls.close()
    tls.write(
      @utf8.encode(
        "GET /\{path_tail} HTTP/1.1\r\nHost: \{authority}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
      ),
    )
    let header = tls.read_until("\r\n\r\n") catch {
      e => raise AuthenticationFailed("invalid Athenz ZTS HTTP response: \{e}")
    }
    guard header is Some(header) && header.length() <= 16384 else {
      raise AuthenticationFailed("invalid Athenz ZTS HTTP header")
    }
    let status_line = match header.split_once("\r\n") {
      Some((line, _)) => line
      None => header
    }
    guard status_line.has_prefix("HTTP/1.") &&
      status_line.split_once(" ") is Some((_, rest)) else {
      raise AuthenticationFailed("invalid Athenz ZTS HTTP status")
    }
    let code_text = match rest.split_once(" ") {
      Some((code, _)) => code
      None => rest
    }
    let status = @string.parse_int(code_text) catch {
      _ => raise AuthenticationFailed("invalid Athenz ZTS HTTP status")
    }
    let mut content_length : Int? = None
    let mut chunked = false
    for line in header.split("\r\n") {
      if line.split_once(":") is Some((name, value)) {
        match name.to_lower() {
          "content-length" => {
            let length = @string.parse_int(value.trim()) catch {
              _ =>
                raise AuthenticationFailed("invalid Athenz ZTS content length")
            }
            guard length >= 0 && length <= 1048576 else {
              raise AuthenticationFailed("Athenz ZTS response is too large")
            }
            content_length = Some(length)
          }
          "transfer-encoding" =>
            if value.to_lower().contains("chunked") {
              chunked = true
            }
          _ => ()
        }
      }
    }
    let body = if chunked {
      athenz_read_chunked(tls)
    } else if content_length is Some(length) {
      tls.read_exactly(length)
    } else {
      athenz_read_until_close(tls)
    }
    (status, body)
  })
}

///|
async fn athenz_read_chunked(tls : @native_tls.NativeTls) -> Bytes {
  let body = @buffer.Buffer()
  for ;; {
    let line = tls.read_until("\r\n") catch {
      _ => raise AuthenticationFailed("invalid Athenz ZTS chunk header")
    }
    guard line is Some(line) else {
      raise AuthenticationFailed("truncated Athenz ZTS chunk header")
    }
    let size_text = match line.split_once(";") {
      Some((size, _)) => size
      None => line
    }
    let size = @string.parse_int(size_text.trim(), base=16) catch {
      _ => raise AuthenticationFailed("invalid Athenz ZTS chunk size")
    }
    guard size >= 0 && size <= 1048576 - body.length() else {
      raise AuthenticationFailed("Athenz ZTS response is too large")
    }
    if size == 0 {
      for ;; {
        let trailer = tls.read_until("\r\n")
        guard trailer is Some(value) else {
          raise AuthenticationFailed("truncated Athenz ZTS chunk trailer")
        }
        if value == "" {
          return body.to_bytes()
        }
      }
    }
    body.write_bytes(tls.read_exactly(size))
    guard tls.read_exactly(2) == b"\r\n" else {
      raise AuthenticationFailed("invalid Athenz ZTS chunk delimiter")
    }
  }
  body.to_bytes()
}

///|
async fn athenz_read_until_close(tls : @native_tls.NativeTls) -> Bytes {
  let body = @buffer.Buffer()
  let chunk = FixedArray::make(4096, b'\x00')
  for ;; {
    let count = tls.read(chunk)
    if count == 0 {
      return body.to_bytes()
    }
    guard count <= 1048576 - body.length() else {
      raise AuthenticationFailed("Athenz ZTS response is too large")
    }
    body.write_bytes(chunk.unsafe_reinterpret_as_bytes()[:count].to_owned())
  }
  body.to_bytes()
}
