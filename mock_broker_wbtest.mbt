// In-process mock broker used by the whitebox tests: a TCP server speaking
// just enough of the Pulsar binary protocol to drive client behavior.
// Each mock broker accepts exactly one connection, so task groups in tests
// can terminate once the client disconnects.

///|
/// One accepted client connection on the mock broker side.
struct MockPeer {
  tcp : @socket.Tcp
  /// frames received from the client after the handshake
  received : @aqueue.Queue[IncomingFrame]
}

///|
async fn MockPeer::send_command(
  self : MockPeer,
  cmd : @proto.BaseCommand,
) -> Unit raise {
  self.tcp.write(encode_command_frame(cmd))
}

///|
async fn MockPeer::send_message(
  self : MockPeer,
  cmd : @proto.BaseCommand,
  metadata : @proto.MessageMetadata,
  payload : Bytes,
) -> Unit raise {
  self.tcp.write(encode_send_frame(cmd, metadata, payload))
}

///|
/// Next frame received from the client.
async fn MockPeer::next(self : MockPeer) -> IncomingFrame {
  self.received.get()
}

///|
/// Start a mock broker on a random local port. `on_peer` runs once a
/// client has connected and completed the CONNECT handshake, and receives
/// the broker's own "host:port" address.
/// Returns the "host:port" address to connect to.
async fn start_mock_broker(
  group : @async.TaskGroup[Unit],
  on_peer : async (String, MockPeer) -> Unit raise,
) -> String raise {
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let addr = server.addr.to_string()
  group.spawn_bg(
    async fn() {
      defer server.close()
      let (tcp, _peer_addr) = server.accept()
      let peer : MockPeer = {
        tcp,
        received: @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded),
      }
      group.spawn_bg(
        async fn() {
          while true {
            let frame = read_frame(tcp)
            peer.received.put(frame)
          }
        },
        no_wait=true,
        allow_failure=true,
      )
      // complete the handshake
      let hello = peer.next()
      guard hello.cmd.type_ is @proto.BaseCommand_Type::CONNECT else {
        fail("expected CONNECT")
      }
      let connected = @proto.BaseCommand::default()
      connected.type_ = @proto.BaseCommand_Type::CONNECTED
      let c = @proto.CommandConnected::default()
      c.server_version = "mock-broker 1.0"
      c.protocol_version = Some(21)
      connected.connected = Some(c)
      peer.send_command(connected)
      on_peer(addr, peer)
      // drain: keep answering CLOSE_* like a real broker until disconnect
      while true {
        let frame = peer.next()
        let close_reply = @proto.BaseCommand::default()
        close_reply.type_ = @proto.BaseCommand_Type::SUCCESS
        if frame.cmd.close_producer is Some(c) {
          let success = @proto.CommandSuccess::default()
          success.request_id = c.request_id
          close_reply.success = Some(success)
        } else if frame.cmd.close_consumer is Some(c) {
          let success = @proto.CommandSuccess::default()
          success.request_id = c.request_id
          close_reply.success = Some(success)
        } else {
          continue
        }
        peer.send_command(close_reply)
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  addr
}
