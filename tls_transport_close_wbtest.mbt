///|
// Observe the actual peer socket, independently of Connection's closed flag.
// The fixture owns and closes every accepted socket, including on cancellation.
#warnings("-alert_internal")
async fn start_tls_close_probe(
  group : @async.TaskGroup[Unit],
  malformed~ : Bool = false,
) -> (String, @aqueue.Queue[Unit]) {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let addr = server.addr.to_string()
  let eof = @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1))
  group.spawn_bg(async fn() {
    defer server.close()
    let (tcp, _) = server.accept()
    defer tcp.close()
    let tls = @tls.Tls::server(
      tcp,
      private_key_file="testdata/tls/server.key",
      private_key_type=@tls.X509FileType::PEM,
      certificate_file="testdata/tls/server.crt",
      certificate_type=@tls.X509FileType::PEM,
    )
    defer tls.close()
    let hello = read_frame(tls)
    assert_true(hello.cmd.type_ is @proto.BaseCommand_Type::CONNECT)
    let connected = @proto.BaseCommand::default()
    connected.type_ = @proto.BaseCommand_Type::CONNECTED
    let info = @proto.CommandConnected::default()
    info.server_version = "tls-close-probe"
    info.protocol_version = Some(21)
    connected.connected = Some(info)
    tls.write(encode_command_frame(connected))
    if malformed {
      // A zero-sized Pulsar frame deterministically fails the client reader.
      tls.write(b"\x00\x00\x00\x00")
    }
    // No TLS application data follows CONNECT. EOF here proves the client's
    // underlying TCP was closed, rather than just its SSL object being freed.
    assert_true(tcp.read_some() is None)
    eof.put(())
  })
  (addr, eof)
}

///|
async fn check_tls_transport_close(
  native_policy~ : Bool,
  malformed~ : Bool,
) -> Unit {
  @async.with_task_group(async fn(group) {
    let (addr, eof) = start_tls_close_probe(group, malformed~)
    let client = Client::connect_with_options(
      group,
      ClientOptions::new(
        "pulsar+ssl://\{addr}",
        ca_pem_file="testdata/tls/server.crt",
        tls_min_version?=if native_policy { Some(TLS12) } else { None },
      ),
    )
    defer client.close()
    if !malformed {
      client.close()
      client.close()
    }
    @async.with_timeout(5000, async fn() { eof.get() })
  })
}

///|
async test "closing ordinary TLS also closes its owned TCP socket" {
  check_tls_transport_close(native_policy=false, malformed=false)
}

///|
async test "closing native TLS also closes its owned TCP socket" {
  check_tls_transport_close(native_policy=true, malformed=false)
}

///|
async test "ordinary TLS reader failure closes its owned TCP socket" {
  check_tls_transport_close(native_policy=false, malformed=true)
}

///|
async test "native TLS reader failure closes its owned TCP socket" {
  check_tls_transport_close(native_policy=true, malformed=true)
}
