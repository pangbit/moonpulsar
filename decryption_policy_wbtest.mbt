// Consumer and reader decryption failure actions against encrypted wire frames.

///|
async fn decryption_test_subscribe(peer : MockPeer) -> UInt64 {
  let frame = peer.next()
  guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
  let reply = @proto.BaseCommand::default()
  reply.type_ = @proto.BaseCommand_Type::SUCCESS
  let success = @proto.CommandSuccess::default()
  success.request_id = sub.request_id
  reply.success = Some(success)
  peer.send_command(reply)
  guard peer.next().cmd.flow is Some(_) else { fail("expected FLOW") }
  sub.consumer_id
}

///|
async fn encrypted_test_payload() -> (@proto.MessageMetadata, Bytes) {
  let crypto = MessageCrypto::new()
  crypto.load_public_key_file("test", "testdata/encryption/rsa_public.pem")
  let metadata = test_metadata()
  let ciphertext = crypto.encrypt(metadata, b"secret", ["test"])
  (metadata, ciphertext)
}

///|
async test "default decryption failure stops consumer without ACK" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      let (cmd, _, _) = message_frame(
        consumer_id, 1UL, 1UL, ciphertext, metadata,
      )
      peer.send_message(cmd, metadata, ciphertext)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/crypto-fail", "sub",
    )
    let failed : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(failed is Err(InvalidFrame(_)))
    consumer.close()
    client.close()
  })
}

///|
async test "Consume delivers ciphertext and preserves plaintext delivery" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      let (encrypted_cmd, _, _) = message_frame(
        consumer_id, 1UL, 1UL, ciphertext, metadata,
      )
      peer.send_message(encrypted_cmd, metadata, ciphertext)
      let (plain_cmd, plain_meta, plain_body) = message_frame(
        consumer_id,
        1UL,
        2UL,
        b"plain",
        test_metadata(),
      )
      peer.send_message(plain_cmd, plain_meta, plain_body)
      let mut acks = 0
      while acks < 2 {
        if peer.next().cmd.ack is Some(_) {
          acks += 1
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/crypto-consume",
      "sub",
      decryption_failure_action=Consume,
    )
    let encrypted = consumer.receive()
    assert_true(encrypted.decryption_failed())
    assert_eq(encrypted.payload(), ciphertext)
    encrypted.ack()
    let plain = consumer.receive()
    assert_true(!plain.decryption_failed())
    assert_eq(plain.payload(), b"plain")
    plain.ack()
    consumer.close()
    client.close()
  })
}

///|
async test "wrong private key follows Consume policy and remains ACKable" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      let (cmd, _, _) = message_frame(
        consumer_id, 1UL, 1UL, ciphertext, metadata,
      )
      peer.send_message(cmd, metadata, ciphertext)
      let ack = peer.next()
      guard ack.cmd.ack is Some(request) else {
        fail("expected ciphertext ACK")
      }
      assert_eq(request.message_id[0].entry_id, 1UL)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let wrong_key = MessageCrypto::new()
    wrong_key.load_private_key_file(
      "test", "testdata/encryption/rsa_rotated_private.pem",
    )
    let consumer = client.create_consumer(
      "persistent://public/default/crypto-wrong-key",
      "sub",
      message_crypto=wrong_key,
      decryption_failure_action=Consume,
    )
    let encrypted = consumer.receive()
    assert_true(encrypted.decryption_failed())
    assert_eq(encrypted.payload(), ciphertext)
    encrypted.ack()
    consumer.close()
    client.close()
  })
}

///|
async test "Discard ACKs ciphertext and restores zero-queue demand" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      guard peer.next().cmd.flow is Some(_) else {
        fail("expected on-demand FLOW")
      }
      let (encrypted_cmd, _, _) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        ciphertext,
        metadata,
      )
      peer.send_message(encrypted_cmd, metadata, ciphertext)
      let ack = peer.next()
      guard ack.cmd.ack is Some(request) else { fail("expected discard ACK") }
      assert_eq(request.message_id[0].entry_id, 1UL)
      guard peer.next().cmd.flow is Some(_) else {
        fail("expected replacement FLOW")
      }
      let (plain_cmd, plain_meta, plain_body) = message_frame(
        sub.consumer_id,
        1UL,
        2UL,
        b"after-discard",
        test_metadata(),
      )
      peer.send_message(plain_cmd, plain_meta, plain_body)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/crypto-discard",
      "sub",
      receiver_queue_size=0,
      decryption_failure_action=Discard,
    )
    let message = @async.with_timeout(2000, async fn() { consumer.receive() })
    assert_eq(message.payload(), b"after-discard")
    assert_true(!message.decryption_failed())
    consumer.close()
    client.close()
  })
}

///|
async test "Consume rejects an encrypted batch without its key" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    metadata.num_messages_in_batch = Some(2)
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      let (cmd, _, _) = message_frame(
        consumer_id, 1UL, 1UL, ciphertext, metadata,
      )
      peer.send_message(cmd, metadata, ciphertext)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consumer = client.create_consumer(
      "persistent://public/default/crypto-batch",
      "sub",
      decryption_failure_action=Consume,
    )
    let failed : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(failed is Err(InvalidFrame(_)))
    consumer.close()
    client.close()
  })
}

///|
async test "reader Consume exposes ciphertext and Discard skips it" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      for index in 0..<2 {
        let consumer_id = decryption_test_subscribe(peer)
        let (encrypted_cmd, _, _) = message_frame(
          consumer_id, 1UL, 1UL, ciphertext, metadata,
        )
        peer.send_message(encrypted_cmd, metadata, ciphertext)
        let (plain_cmd, plain_meta, plain_body) = message_frame(
          consumer_id,
          1UL,
          2UL,
          b"reader-plain",
          test_metadata(),
        )
        peer.send_message(plain_cmd, plain_meta, plain_body)
        if index == 0 {
          let close_frame = peer.next()
          guard close_frame.cmd.close_consumer is Some(close) else {
            fail("expected reader close")
          }
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = close.request_id
          reply.success = Some(success)
          peer.send_command(reply)
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let consume = client.create_reader(
      "persistent://public/default/crypto-reader-consume",
      decryption_failure_action=Consume,
    )
    let first = consume.next().unwrap()
    assert_true(first.decryption_failed())
    assert_eq(first.payload(), ciphertext)
    assert_eq(consume.next().unwrap().payload(), b"reader-plain")
    consume.close()
    let discard = client.create_reader(
      "persistent://public/default/crypto-reader-discard",
      decryption_failure_action=Discard,
    )
    let kept = @async.with_timeout(2000, async fn() { discard.next() })
    assert_eq(
      kept.map(fn(message) { message.payload() }),
      Some(b"reader-plain"),
    )
    discard.close()
    client.close()
  })
}
