// Broker-confirmed acknowledgments and protocol negotiation.

///|
async test "ACK response reports broker failure and rejects old protocols" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      for index in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.ack is Some(req) && req.request_id is Some(id) else {
          fail("expected ACK with request id")
        }
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
        let response = @proto.CommandAckResponse::default()
        response.consumer_id = req.consumer_id
        response.request_id = Some(id)
        if index == 1 {
          response.error = Some(@proto.ServerError::MetadataError)
          response.message = Some("ack rejected")
        }
        reply.ack_response = Some(response)
        peer.send_command(reply)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let id : MessageId = {
      ledger_id: 1UL,
      entry_id: 2UL,
      batch_index: -1,
      partition: -1,
    }
    send_ack(
      conn,
      1UL,
      @proto.CommandAck_AckType::Individual,
      [id],
      await_response=true,
    )
    let failure : Result[Unit, Error] = Ok(
      send_ack(
        conn,
        1UL,
        @proto.CommandAck_AckType::Individual,
        [id],
        await_response=true,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(failure is Err(ServerError(message="ack rejected", ..)))
    conn.remote_protocol_version = 16
    let unsupported : Result[Unit, Error] = Ok(
      send_ack(
        conn,
        1UL,
        @proto.CommandAck_AckType::Individual,
        [id],
        await_response=true,
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(unsupported is Err(InvalidFrame(_)))
    manager.close()
  })
}

///|
async test "failed confirmed ACK retains the consumer's pending deadline" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      guard peer.next().cmd.flow is Some(_) else { fail("expected FLOW") }
      let (cmd, meta, payload) = message_frame(
        sub.consumer_id,
        1UL,
        1UL,
        b"pending-ack",
        test_metadata(),
      )
      peer.send_message(cmd, meta, payload)
      let frame = peer.next()
      guard frame.cmd.ack is Some(ack) && ack.request_id is Some(id) else {
        fail("expected confirmed ACK")
      }
      let response = @proto.BaseCommand::default()
      response.type_ = @proto.BaseCommand_Type::ACK_RESPONSE
      let result = @proto.CommandAckResponse::default()
      result.consumer_id = sub.consumer_id
      result.request_id = Some(id)
      result.error = Some(@proto.ServerError::MetadataError)
      result.message = Some("try again")
      response.ack_response = Some(result)
      peer.send_command(response)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/ack-response",
      "sub",
      ack_timeout_ms=5000,
    )
    let message = consumer.receive()
    let result : Result[Unit, Error] = Ok(message.ack(await_response=true)) catch {
      e => Err(e)
    }
    assert_true(result is Err(ServerError(message="try again", ..)))
    assert_true(consumer.ack_pending.contains(message.id()))
    consumer.close()
    manager.close()
  })
}
