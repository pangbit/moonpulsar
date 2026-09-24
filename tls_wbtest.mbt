// TLS connection tests use a disposable localhost certificate in testdata/tls.

///|
#warnings("-alert_internal")
async fn start_tls_mock_broker(
  group : @async.TaskGroup[Unit],
  connections? : Int = 1,
  keep_open? : Bool = false,
) -> String {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let addr = server.addr.to_string()
  group.spawn_bg(
    async fn() {
      defer server.close()
      for _ in 0..<connections {
        let (tcp, _) = server.accept()
        let tls = @tls.Tls::server(
          tcp,
          private_key_file="testdata/tls/server.key",
          private_key_type=@tls.X509FileType::PEM,
          certificate_file="testdata/tls/server.crt",
          certificate_type=@tls.X509FileType::PEM,
        ) catch {
          _ => return
        }
        let hello = read_frame(tls) catch { _ => return }
        guard hello.cmd.type_ is @proto.BaseCommand_Type::CONNECT else {
          fail("expected CONNECT over TLS")
        }
        let connected = @proto.BaseCommand::default()
        connected.type_ = @proto.BaseCommand_Type::CONNECTED
        let info = @proto.CommandConnected::default()
        info.server_version = "tls-mock"
        info.protocol_version = Some(21)
        connected.connected = Some(info)
        tls.write(encode_command_frame(connected))
        if keep_open {
          group.spawn_bg(
            async fn() {
              defer tls.close()
              try {
                while true {
                  ignore(read_frame(tls))
                }
              } catch {
                _ => ()
              }
            },
            no_wait=true,
            allow_failure=true,
          )
        } else {
          tls.close()
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  addr
}

///|
async test "custom CA is reused after reconnect" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group, connections=2, keep_open=true)
    let client = Client::connect_tls_with_ca(
      group,
      "pulsar+ssl://\{addr}",
      "testdata/tls/server.crt",
    )
    let first = client.manager.get(addr)
    first.close()
    let second = @async.with_timeout(5000, async fn() {
      client.manager.get(addr)
    })
    assert_true(!second.is_closed())
    client.close()
  })
}

///|
async test "custom CA validates a TLS broker" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group)
    let client = @async.with_timeout(5000, async fn() {
      Client::connect_tls_with_ca(
        group,
        "pulsar+ssl://\{addr}",
        "testdata/tls/server.crt",
      )
    })
    client.close()
  })
}

///|
async test "native TLS policy verifies CA and completes Pulsar handshake" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group)
    let client = @async.with_timeout(5000, async fn() {
      Client::connect_with_options(
        group,
        ClientOptions::new(
          "pulsar+ssl://\{addr}",
          ca_pem_file="testdata/tls/server.crt",
          tls_min_version=TLS12,
          tls_identity=TlsIdentity::new(
            "testdata/tls/server.crt", "testdata/tls/server.key",
          ),
        ),
      )
    })
    client.close()
  })
}

///|
async test "native TLS rejects untrusted broker and missing client key" {
  @async.with_task_group(async fn(group) {
    let untrusted_addr = start_tls_mock_broker(group)
    let untrusted : Result[Client, Error] = Ok(
      @async.with_timeout(5000, async fn() {
        Client::connect_with_options(
          group,
          ClientOptions::new(
            "pulsar+ssl://\{untrusted_addr}",
            tls_min_version=TLS12,
          ),
        )
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(untrusted is Err(_))
    let missing_addr = start_tls_mock_broker(group)
    let missing : Result[Client, Error] = Ok(
      @async.with_timeout(5000, async fn() {
        Client::connect_with_options(
          group,
          ClientOptions::new(
            "pulsar+ssl://\{missing_addr}",
            ca_pem_file="testdata/tls/server.crt",
            tls_identity=TlsIdentity::new(
              "testdata/tls/server.crt", "testdata/tls/no-key.pem",
            ),
          ),
        )
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(missing is Err(ConnectFailed(_)))
  })
}

///|
async test "native TLS identity supplier refreshes on reconnect" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group, connections=2)
    let calls : Ref[Int] = { val: 0, }
    let client = Client::connect_with_options(
      group,
      ClientOptions::new(
        "pulsar+ssl://\{addr}",
        ca_pem_file="testdata/tls/server.crt",
        tls_identity_provider=fn() raise {
          calls.val += 1
          TlsIdentity::new("testdata/tls/server.crt", "testdata/tls/server.key")
        },
      ),
    )
    assert_eq(calls.val, 1)
    let first = client.manager.get(addr)
    first.close()
    ignore(@async.with_timeout(5000, async fn() { client.manager.get(addr) }))
    assert_eq(calls.val, 2)
    client.close()
  })
}

///|
async test "native TLS negotiates TLS 1.3 with an allowed cipher suite" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group)
    let client = @async.with_timeout(5000, async fn() {
      Client::connect_with_options(
        group,
        ClientOptions::new(
          "pulsar+ssl://\{addr}",
          ca_pem_file="testdata/tls/server.crt",
          tls_min_version=TLS13,
          tls_max_version=TLS13,
          tls13_ciphersuites="TLS_AES_128_GCM_SHA256",
        ),
      )
    })
    client.close()
  })
}

///|
async test "native TLS rejects unsupported cipher policy" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group)
    let bad : Result[Client, Error] = Ok(
      @async.with_timeout(5000, async fn() {
        Client::connect_with_options(
          group,
          ClientOptions::new(
            "pulsar+ssl://\{addr}",
            ca_pem_file="testdata/tls/server.crt",
            tls_cipher_list="NOT_A_CIPHER",
          ),
        )
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(bad is Err(ConnectFailed(_)))
  })
}

///|
test "TLS identity and version policy reject invalid combinations" {
  let missing : Result[TlsIdentity, Error] = Ok(
    TlsIdentity::new("testdata/tls/server.crt", ""),
  ) catch {
    e => Err(e)
  }
  assert_true(missing is Err(InvalidFrame(_)))
  let reversed : Result[ClientOptions, Error] = Ok(
    ClientOptions::new(
      "pulsar+ssl://localhost:6651",
      tls_min_version=TLS13,
      tls_max_version=TLS12,
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(reversed is Err(InvalidFrame(_)))
}

///|
async test "system roots reject an untrusted TLS broker" {
  @async.with_task_group(async fn(group) {
    let addr = start_tls_mock_broker(group)
    let result : Result[Client, Error] = Ok(
      @async.with_timeout(5000, async fn() {
        Client::connect(group, "pulsar+ssl://\{addr}")
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(_))
  })
}

///|
async test "custom CA requires a TLS URL and PEM path" {
  @async.with_task_group(async fn(group) {
    let wrong_scheme : Result[Client, Error] = Ok(
      Client::connect_tls_with_ca(
        group, "pulsar://127.0.0.1:6650", "testdata/tls/server.crt",
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(wrong_scheme is Err(_))
    let empty_file : Result[Client, Error] = Ok(
      Client::connect_tls_with_ca(group, "pulsar+ssl://127.0.0.1:6651", ""),
    ) catch {
      e => Err(e)
    }
    assert_true(empty_file is Err(_))
  })
}
