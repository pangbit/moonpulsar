// Invalid compressed data must wake a waiting consumer or Reader.

///|
async test "consumer reports invalid compressed message" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.compression = Some(@proto.CompressionType::ZLIB)
      metadata.uncompressed_size = Some(5U)
      let (cmd, meta, body) = message_frame(1UL, 1, 1UL, b"not zlib", metadata)
      peer.send_message(cmd, meta, body)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group, conn, "persistent://public/default/bad-zlib", "bad-zlib-sub",
    )
    let received : Result[Message, Error] = Ok(
      @async.with_timeout(3000, async fn() { consumer.receive() }),
    ) catch {
      error => Err(error)
    }
    assert_true(received is Err(InvalidFrame(_)))
    assert_true(!conn.message_handlers.contains(consumer.consumer_id))
    consumer.close()
    manager.close()
  })
}

///|
async test "reader reports invalid compressed message" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_subscribe(peer)
      let metadata = test_metadata()
      metadata.compression = Some(@proto.CompressionType::ZLIB)
      metadata.uncompressed_size = Some(5U)
      let (cmd, meta, body) = message_frame(1UL, 1, 1UL, b"not zlib", metadata)
      peer.send_message(cmd, meta, body)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let reader = SingleReader::create(
      group,
      conn,
      "persistent://public/default/bad-zlib",
      start_message_id=MessageId::earliest(),
    )
    let received : Result[Message?, Error] = Ok(
      @async.with_timeout(3000, async fn() { reader.next() }),
    ) catch {
      error => Err(error)
    }
    assert_true(received is Err(InvalidFrame(_)))
    assert_true(!conn.message_handlers.contains(reader.consumer_id))
    reader.close()
    manager.close()
  })
}
