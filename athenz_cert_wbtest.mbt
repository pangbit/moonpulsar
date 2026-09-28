///|
#warnings("-alert_internal")
async fn start_mock_certificate_zts(
  group : @async.TaskGroup[Unit],
  statuses : Array[Int],
  response_format? : Int = 0,
) -> String {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let addr = server.addr.to_string()
  group.spawn_bg(
    async fn() {
      defer server.close()
      for status in statuses {
        let (tcp, _) = server.accept()
        let tls = @tls.Tls::server(
          tcp,
          private_key_file="testdata/tls/server.key",
          private_key_type=@tls.X509FileType::PEM,
          certificate_file="testdata/tls/server.crt",
          certificate_type=@tls.X509FileType::PEM,
        )
        let request = tls.read_until("\r\n\r\n")
        assert_true(
          request is Some(text) &&
          text.has_prefix(
            "GET /zts/v1/domain/provider/token?minExpiryTime=7200&maxExpiryTime=86400 HTTP/1.1",
          ),
        )
        assert_true(
          request is Some(text) && !text.contains("Athenz-Principal-Auth"),
        )
        let response_body = if response_format == 1 && status == 200 {
          "{\"token\":\"certificate-role\",\"expiryTime\":4102444800}"
        } else if status == 200 {
          let expiry = @async.now() / 1000L + 3600L
          "{\"token\":\"certificate-role\",\"expiryTime\":\{expiry}}"
        } else {
          "unavailable"
        }
        let response = match response_format {
          1 if status == 200 =>
            "HTTP/1.1 \{status} Result\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n34\r\n\{response_body}\r\n0\r\n\r\n"
          2 =>
            "HTTP/1.1 \{status} Result\r\nConnection: close\r\n\r\n\{response_body}"
          _ =>
            "HTTP/1.1 \{status} Result\r\nContent-Length: \{response_body.length()}\r\nConnection: close\r\n\r\n\{response_body}"
        }
        tls.write(@utf8.encode(response))
        // The client may close after reading a retryable HTTP response.
        // A close-notify failure must not prevent accepting the retry.
        tls.shutdown() catch {
          _ => ()
        }
        tls.close()
        tcp.close()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  "https://\{addr}"
}

///|
async test "Athenz certificate ZTS reads chunked and close-delimited responses" {
  @async.with_task_group(async fn(group) {
    for format in [1, 2] {
      let url = start_mock_certificate_zts(group, [200], response_format=format)
      let provider = athenz_zts_provider(
        AthenzZtsOptions::new(
          url,
          "provider",
          "",
          "",
          "testdata/tls/server.key",
          certificate_file="testdata/tls/server.crt",
          ca_pem_file="testdata/tls/server.crt",
          max_retries=0,
        ),
      )
      let auth = provider() catch { e => fail("format \{format}: \{e}") }
      assert_eq(auth.data, b"certificate-role")
    }
  })
}

///|
async test "Athenz certificate ZTS exchange caches and retries over verified TLS" {
  @async.with_task_group(async fn(group) {
    // OpenSSL records an error when an RSA key is probed as an EC key. TLS
    // reads must clear that queue before calling SSL_get_error.
    let rsa = @fs.read_file("testdata/encryption/rsa_public.pem").binary()
    assert_true(!@native_ecies.is_public_key(rsa))
    let url = start_mock_certificate_zts(group, [503, 200])
    let provider = athenz_zts_provider(
      AthenzZtsOptions::new(
        url,
        "provider",
        "",
        "",
        "testdata/tls/server.key",
        certificate_file="testdata/tls/server.crt",
        ca_pem_file="testdata/tls/server.crt",
        max_retries=1,
        retry_delay_ms=0,
      ),
    )
    assert_eq(provider().data, b"certificate-role")
    assert_eq(provider().data, b"certificate-role")
  })
}

///|
async test "Athenz certificate ZTS rejects wrong CA and missing key" {
  @async.with_task_group(async fn(group) {
    let url = start_mock_certificate_zts(group, [200])
    let untrusted = athenz_zts_provider(
      AthenzZtsOptions::new(
        url,
        "provider",
        "",
        "",
        "testdata/tls/server.key",
        certificate_file="testdata/tls/server.crt",
        max_retries=0,
      ),
    )
    let error : Result[AuthData, Error] = Ok(untrusted()) catch { e => Err(e) }
    assert_true(error is Err(_))
    let missing = athenz_zts_provider(
      AthenzZtsOptions::new(
        url,
        "provider",
        "",
        "",
        "testdata/tls/missing.key",
        certificate_file="testdata/tls/server.crt",
        ca_pem_file="testdata/tls/server.crt",
        max_retries=0,
      ),
    )
    let missing_error : Result[AuthData, Error] = Ok(missing()) catch {
      e => Err(e)
    }
    assert_true(missing_error is Err(_))
  })
}

///|
test "Athenz certificate ZTS requires HTTPS and a nonempty certificate" {
  let wrong_scheme : Result[AthenzZtsOptions, Error] = Ok(
    AthenzZtsOptions::new(
      "http://zts",
      "provider",
      "",
      "",
      "key.pem",
      certificate_file="cert.pem",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(wrong_scheme is Err(InvalidFrame(_)))
  let empty : Result[AthenzZtsOptions, Error] = Ok(
    AthenzZtsOptions::new(
      "https://zts",
      "provider",
      "",
      "",
      "key.pem",
      certificate_file="",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(empty is Err(InvalidFrame(_)))
  let ca_without_cert : Result[AthenzZtsOptions, Error] = Ok(
    AthenzZtsOptions::new(
      "https://zts",
      "provider",
      "tenant",
      "service",
      "key.pem",
      ca_pem_file="ca.pem",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(ca_without_cert is Err(InvalidFrame(_)))
}
