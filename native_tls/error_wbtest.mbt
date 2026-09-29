///|
test "missing private key does not leave errors for another TLS connection" {
  let handle = tls_new(
    b"localhost", b"", true, b"testdata/tls/server.crt", b"testdata/tls/no-key.pem",
    0, 0, b"", b"",
  )
  defer tls_close(handle)
  assert_true(tls_is_null(handle))
  let message = @utf8.decode_lossy(tls_error())
  assert_true(!message.contains("error:00000000:"))
  // OpenSSL reports several errors for a missing key. Reporting the failure
  // must consume all of them before another coroutine uses SSL_get_error.
  let remaining = @utf8.decode_lossy(tls_error())
  assert_true(remaining.contains("error:00000000:"), msg=remaining)
}
