///|
async test "native dependency base TCP RSA and Zlib" {
  let crypto = MessageCrypto::new()
  crypto.load_public_key_file("rsa", "testdata/encryption/rsa_public.pem")
  crypto.load_private_key_file("rsa", "testdata/encryption/rsa_private.pem")
  let metadata = @proto.MessageMetadata::default()
  let encrypted = crypto.encrypt(metadata, b"independent RSA", ["rsa"])
  assert_eq(crypto.decrypt(metadata, encrypted), b"independent RSA")
  let flushed = b"\x78\x9c\xca\x48\xcd\xc9\xc9\x07\x00\x00\x00\xff\xff"
  assert_eq(decompress(Zlib, flushed, uncompressed_size=5), b"hello")
  for codec in [Zlib, Lz4, Snappy, Zstd] {
    assert_eq(decompress(codec, compress(codec, b"hello"), uncompressed_size=5), b"hello")
  }
  @async.with_task_group(async fn(group) {
    let done : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded)
    let addr = start_mock_broker(group, async fn(_, peer) {
      let producer = peer.next().cmd.producer.unwrap()
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = producer.request_id
      success.producer_name = "dependency-probe"
      reply.producer_success = Some(success)
      peer.send_command(reply)
      let subscription = peer.next().cmd.subscribe.unwrap()
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = subscription.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
      let frame = peer.next()
      let send = frame.cmd.send.unwrap()
      assert_eq(frame.payload, Some(b"roundtrip"))
      let id = @proto.MessageIdData::default()
      id.ledger_id = 1
      id.entry_id = 2
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
      let receipt = @proto.CommandSendReceipt::default()
      receipt.producer_id = send.producer_id
      receipt.sequence_id = send.sequence_id
      receipt.message_id = Some(id)
      reply.send_receipt = Some(receipt)
      peer.send_command(reply)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::MESSAGE
      let message = @proto.CommandMessage::default()
      message.consumer_id = subscription.consumer_id
      message.message_id = id
      reply.message = Some(message)
      peer.send_message(reply, frame.metadata.unwrap(), b"roundtrip")
      assert_true(peer.next().cmd.ack is Some(_))
      done.put(())
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(group, conn, "persistent://public/default/deps")
    let consumer = Consumer::single(SingleConsumer::create(group, conn, "persistent://public/default/deps", "deps"))
    ignore(producer.send(ProducerMessage::new(b"roundtrip")))
    let message = consumer.receive()
    assert_eq(message.payload(), b"roundtrip")
    message.ack()
    done.get()
    consumer.close()
    producer.close()
    manager.close()
  })
}

///|
async test "native dependency missing ECIES" {
  let key = @fs.read_file("testdata/encryption/ec_public.pem").binary()
  for _ in 0..<2 {
    let result : Result[EncryptionPublicKey, Error] = Ok(EncryptionPublicKey::from_pem(key)) catch { e => Err(e) }
    assert_true(result is Err(InvalidFrame(message)) && message.contains("ECIES unavailable: OpenSSL 3 library unavailable"))
  }
}

///|
async test "native dependency missing TLS after TCP connect" {
  @async.with_task_group(async fn(group) {
    let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let accepted : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=@aqueue.Kind::Unbounded)
    group.spawn_bg(async fn() {
      let (tcp, _) = server.accept()
      defer tcp.close()
      accepted.put(())
      @async.sleep(1000)
    })
    let result : Result[Client, Error] = Ok(Client::connect_with_options(group,
      ClientOptions::new("pulsar+ssl://\{server.addr}", tls_min_version=TLS12))) catch { e => Err(e) }
    assert_true(result is Err(ConnectFailed(message)) && message.contains("OpenSSL 3 library unavailable"))
    accepted.get()
  })
}
