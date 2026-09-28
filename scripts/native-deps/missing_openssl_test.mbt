// Copied into the extracted module ONLY by verify-native-deps.mbtx.
///|
async test "missing OpenSSL keeps RSA independent and reports ECIES dependency" {
  let marker = @env.get_env_var("MOONPULSAR_TEST_LOADER_MARKER").unwrap()
  assert_false(@fs.exists(marker))
  let crypto = MessageCrypto::new()
  crypto.load_public_key_file("rsa", "testdata/encryption/rsa_public.pem")
  crypto.load_private_key_file("rsa", "testdata/encryption/rsa_private.pem")
  let metadata = @proto.MessageMetadata::default()
  let ciphertext = crypto.encrypt(metadata, b"RSA without ECIES", ["rsa"])
  assert_eq(crypto.decrypt(metadata, ciphertext), b"RSA without ECIES")
  assert_false(@fs.exists(marker))
  let public = @fs.read_file("testdata/encryption/ec_public.pem").binary()
  let private = @fs.read_file("testdata/encryption/ec_private.pem").binary()
  for _ in 0..<2 {
    let result : Result[EncryptionPublicKey, Error] = Ok(
      EncryptionPublicKey::from_pem(public),
    ) catch { e => Err(e) }
    assert_true(result is Err(InvalidFrame(message)) && message.contains("ECIES unavailable: OpenSSL 3 library unavailable"))
    let result : Result[EncryptionPrivateKey, Error] = Ok(
      EncryptionPrivateKey::from_pem(private),
    ) catch { e => Err(e) }
    assert_true(result is Err(InvalidFrame(message)) && message.contains("ECIES unavailable: OpenSSL 3 library unavailable"))
  }
  assert_true(@fs.exists(marker))
  // Failure of the optional EC path must not poison later RSA operations.
  crypto.load_public_key_file("rsa", "testdata/encryption/rsa_public.pem")
  assert_eq(crypto.decrypt(metadata, ciphertext), b"RSA without ECIES")
}

///|
async test "missing OpenSSL TLS error follows successful TCP connect" {
  @async.with_task_group(async fn(group) {
    let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let accepted = @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded)
    group.spawn_bg(async fn() {
      let (tcp, _) = server.accept()
      defer tcp.close()
      accepted.put(())
      @async.sleep(1000)
    })
    let result : Result[Client, Error] = Ok(Client::connect_with_options(
      group,
      ClientOptions::new("pulsar+ssl://\{server.addr}", tls_min_version=TLS12),
    )) catch { e => Err(e) }
    assert_true(result is Err(ConnectFailed(message)) && message.contains("OpenSSL 3 library unavailable"))
    accepted.get()
  })
}
