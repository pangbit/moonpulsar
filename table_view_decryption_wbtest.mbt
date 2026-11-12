///|
async test "table view preserves decryption failure while closing a waiting reader" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    metadata.partition_key = Some("key")
    let close_seen : @aqueue.Queue[Unit] = @aqueue.Queue(kind=Unbounded)
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      let frame = peer.next()
      guard frame.cmd.get_last_message_id is Some(req) else {
        fail("expected last-message query")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
      let response = @proto.CommandGetLastMessageIdResponse::default()
      response.request_id = req.request_id
      let id = @proto.MessageIdData::default()
      id.ledger_id = 1
      id.entry_id = 1
      response.last_message_id = id
      reply.get_last_message_id_response = Some(response)
      peer.send_command(reply)
      // Let TableView enter Reader::next before decryption rejects the frame.
      @async.sleep(20)
      let (cmd, _, _) = message_frame(
        consumer_id, 1UL, 1UL, ciphertext, metadata,
      )
      peer.send_message(cmd, metadata, ciphertext)
      while true {
        let frame = peer.next()
        if frame.cmd.close_consumer is Some(req) {
          ignore(close_seen.try_put(()))
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = req.request_id
          reply.success = Some(success)
          peer.send_command(reply)
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let result : Result[TypedTableView[String], Error] = Ok(
      @async.with_timeout(3000, async fn() {
        client.create_typed_table_view(
          "persistent://public/default/table-decryption-failure",
          SchemaCodec::string(),
        )
      }),
    ) catch {
      error => Err(error)
    }
    assert_true(result is Err(InvalidFrame(_)))
    @async.with_timeout(1000, () => close_seen.get())
    client.close()
  })
}

///|
async test "reader decryption failure is terminal and preserves its error" {
  @async.with_task_group(async fn(group) {
    let (metadata, ciphertext) = encrypted_test_payload()
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      @async.sleep(20)
      let (cmd, _, _) = message_frame(
        consumer_id, 1UL, 1UL, ciphertext, metadata,
      )
      peer.send_message(cmd, metadata, ciphertext)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let reader = client.create_reader(
      "persistent://public/default/reader-decryption-failure",
    )
    for _ in 0..<2 {
      let result : Result[Message?, Error] = Ok(reader.next()) catch {
        error => Err(error)
      }
      assert_true(result is Err(InvalidFrame(_)))
      assert_true(reader.is_closed())
      reader.close()
    }
    client.close()
  })
}
