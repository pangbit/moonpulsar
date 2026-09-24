// Transaction tests: coordinator channel and transactional produce.

///|
async test "transaction lifecycle over one connection" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      // TC channel
      let frame = peer.next()
      guard frame.cmd.tc_client_connect_request is Some(tc_req) else {
        fail("expected TC_CLIENT_CONNECT_REQUEST")
      }
      assert_eq(tc_req.tc_id, 7)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
      let resp = @proto.CommandTcClientConnectResponse::default()
      resp.request_id = tc_req.request_id
      reply.tc_client_connect_response = Some(resp)
      peer.send_command(reply)
      // NEW_TXN
      let frame = peer.next()
      guard frame.cmd.new_txn is Some(txn_req) else { fail("expected NEW_TXN") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::NEW_TXN_RESPONSE
      let resp = @proto.CommandNewTxnResponse::default()
      resp.request_id = txn_req.request_id
      resp.txnid_least_bits = Some(100)
      resp.txnid_most_bits = Some(9)
      reply.new_txn_response = Some(resp)
      peer.send_command(reply)
      // ADD_PARTITION_TO_TXN
      let frame = peer.next()
      guard frame.cmd.add_partition_to_txn is Some(add) else {
        fail("expected ADD_PARTITION_TO_TXN")
      }
      assert_eq(add.partitions, ["persistent://public/default/tx-topic"])
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE
      let resp = @proto.CommandAddPartitionToTxnResponse::default()
      resp.request_id = add.request_id
      reply.add_partition_to_txn_response = Some(resp)
      peer.send_command(reply)
      // PRODUCER on the same connection
      expect_producer(peer)
      // SEND with transaction id bits
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      assert_eq(send.txnid_least_bits, Some(100))
      assert_eq(send.txnid_most_bits, Some(9))
      send_receipt(peer, send.producer_id, send.sequence_id)
      // END_TXN commit
      let frame = peer.next()
      guard frame.cmd.end_txn is Some(end) else { fail("expected END_TXN") }
      assert_eq(end.txn_action, Some(@proto.TxnAction::COMMIT))
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::END_TXN_RESPONSE
      let resp = @proto.CommandEndTxnResponse::default()
      resp.request_id = end.request_id
      reply.end_txn_response = Some(resp)
      peer.send_command(reply)
    })
    let coord = TransactionCoordinator::connect(group, addr, 7)
    let txn = coord.new_transaction(ttl_ms=30000)
    assert_eq(txn.id(), (9, 100))
    txn.add_partition("persistent://public/default/tx-topic")
    let producer = SingleProducer::create(
      group,
      coord.conn,
      "persistent://public/default/tx-topic",
    )
    let receipt = producer.send_txn(txn, ProducerMessage::new(b"in txn"))
    assert_eq(receipt.message_id.ledger_id, 10)
    txn.commit()
    producer.close()
    coord.close()
  })
}

///|
async test "coordinator attach rejects error and malformed broker replies" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      for index in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.tc_client_connect_request is Some(req) else {
          fail("expected TC_CLIENT_CONNECT_REQUEST")
        }
        let reply = @proto.BaseCommand::default()
        if index == 0 {
          reply.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
          let response = @proto.CommandTcClientConnectResponse::default()
          response.request_id = req.request_id
          response.error = Some(@proto.ServerError::InvalidTxnStatus)
          response.message = Some("coordinator rejected")
          reply.tc_client_connect_response = Some(response)
        } else {
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = req.request_id
          reply.success = Some(success)
        }
        peer.send_command(reply)
      }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let rejected : Result[TransactionCoordinator, Error] = Ok(
      TransactionCoordinator::attach(conn, 0UL, owns_connection=false),
    ) catch {
      e => Err(e)
    }
    assert_true(
      rejected is Err(ServerError(message="coordinator rejected", ..)),
    )
    let malformed : Result[TransactionCoordinator, Error] = Ok(
      TransactionCoordinator::attach(conn, 0UL, owns_connection=false),
    ) catch {
      e => Err(e)
    }
    assert_true(malformed is Err(UnexpectedResponse(_)))
    assert_true(conn.pending.is_empty())
    manager.close()
  })
}

///|
async test "coordinator connect rejects broker failure" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.tc_client_connect_request is Some(req) else {
        fail("expected TC_CLIENT_CONNECT_REQUEST")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
      let response = @proto.CommandTcClientConnectResponse::default()
      response.request_id = req.request_id
      response.error = Some(@proto.ServerError::InvalidTxnStatus)
      response.message = Some("coordinator rejected")
      reply.tc_client_connect_response = Some(response)
      peer.send_command(reply)
    })
    let result : Result[TransactionCoordinator, Error] = Ok(
      TransactionCoordinator::connect(group, addr, 7UL),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ServerError(message="coordinator rejected", ..)))
  })
}

///|
async test "client resolves transaction coordinator ownership" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(addr, peer) {
      for _ in 0..<4 {
        let frame = peer.next()
        if frame.cmd.partition_metadata is Some(req) {
          assert_eq(req.topic, TRANSACTION_COORDINATOR_ASSIGN_TOPIC)
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
          let resp = @proto.CommandPartitionedTopicMetadataResponse::default()
          resp.request_id = req.request_id
          resp.partitions = Some(2U)
          reply.partition_metadata_response = Some(resp)
          peer.send_command(reply)
        } else if frame.cmd.lookup_topic is Some(req) {
          assert_eq(
            req.topic,
            TRANSACTION_COORDINATOR_ASSIGN_TOPIC + "-partition-1",
          )
          peer.send_command(
            lookup_reply(
              req.request_id,
              @proto.CommandLookupTopicResponse_LookupType::Connect,
              broker_url="pulsar://\{addr}",
            ),
          )
        } else if frame.cmd.tc_client_connect_request is Some(req) {
          assert_eq(req.tc_id, 1UL)
          let reply = @proto.BaseCommand::default()
          reply.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
          let resp = @proto.CommandTcClientConnectResponse::default()
          resp.request_id = req.request_id
          reply.tc_client_connect_response = Some(resp)
          peer.send_command(reply)
        } else {
          fail("unexpected coordinator setup command")
        }
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let coord = client.transaction_coordinator(1UL)
    coord.close()
    assert_eq(client.partitions_of(TRANSACTION_COORDINATOR_ASSIGN_TOPIC), 2)
    client.close()
  })
}

///|
async test "client rejects disabled and out-of-range transaction coordinators" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      for partitions in [0U, 2U] {
        let frame = peer.next()
        guard frame.cmd.partition_metadata is Some(req) else {
          fail("expected transaction coordinator partition metadata")
        }
        assert_eq(req.topic, TRANSACTION_COORDINATOR_ASSIGN_TOPIC)
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
        let response = @proto.CommandPartitionedTopicMetadataResponse::default()
        response.request_id = req.request_id
        response.partitions = Some(partitions)
        reply.partition_metadata_response = Some(response)
        peer.send_command(reply)
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let disabled : Result[TransactionCoordinator, Error] = Ok(
      client.transaction_coordinator(0UL),
    ) catch {
      e => Err(e)
    }
    assert_true(
      disabled is Err(LookupFailed("transaction coordinators are not enabled")),
    )
    let out_of_range : Result[TransactionCoordinator, Error] = Ok(
      client.transaction_coordinator(2UL),
    ) catch {
      e => Err(e)
    }
    assert_true(
      out_of_range
      is Err(LookupFailed("transaction coordinator id is out of range")),
    )
    client.close()
  })
}

///|
async test "transaction participant registration rejects broker errors and wrong replies" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let connect = peer.next()
      guard connect.cmd.tc_client_connect_request is Some(tc_req) else {
        fail("expected TC_CLIENT_CONNECT_REQUEST")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
      let resp = @proto.CommandTcClientConnectResponse::default()
      resp.request_id = tc_req.request_id
      reply.tc_client_connect_response = Some(resp)
      peer.send_command(reply)
      let create = peer.next()
      guard create.cmd.new_txn is Some(txn_req) else {
        fail("expected NEW_TXN")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::NEW_TXN_RESPONSE
      let resp = @proto.CommandNewTxnResponse::default()
      resp.request_id = txn_req.request_id
      resp.txnid_least_bits = Some(42UL)
      resp.txnid_most_bits = Some(3UL)
      reply.new_txn_response = Some(resp)
      peer.send_command(reply)
      for index in 0..<4 {
        let frame = peer.next()
        let request_id = if index < 2 {
          guard frame.cmd.add_partition_to_txn is Some(req) else {
            fail("expected ADD_PARTITION_TO_TXN")
          }
          assert_eq(req.partitions, ["persistent://public/default/tx-topic"])
          req.request_id
        } else {
          guard frame.cmd.add_subscription_to_txn is Some(req) else {
            fail("expected ADD_SUBSCRIPTION_TO_TXN")
          }
          assert_eq(req.subscription[0].subscription, "tx-sub")
          req.request_id
        }
        let reply = @proto.BaseCommand::default()
        if index % 2 == 1 {
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let success = @proto.CommandSuccess::default()
          success.request_id = request_id
          reply.success = Some(success)
        } else if index < 2 {
          reply.type_ = @proto.BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE
          let resp = @proto.CommandAddPartitionToTxnResponse::default()
          resp.request_id = request_id
          resp.error = Some(@proto.ServerError::InvalidTxnStatus)
          resp.message = Some("partition rejected")
          reply.add_partition_to_txn_response = Some(resp)
        } else {
          reply.type_ = @proto.BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE
          let resp = @proto.CommandAddSubscriptionToTxnResponse::default()
          resp.request_id = request_id
          resp.error = Some(@proto.ServerError::InvalidTxnStatus)
          resp.message = Some("subscription rejected")
          reply.add_subscription_to_txn_response = Some(resp)
        }
        peer.send_command(reply)
      }
    })
    let coord = TransactionCoordinator::connect(group, addr, 7UL)
    let txn = coord.new_transaction()
    let partition_error : Result[Unit, Error] = Ok(
      txn.add_partition("persistent://public/default/tx-topic"),
    ) catch {
      e => Err(e)
    }
    assert_true(
      partition_error is Err(ServerError(message="partition rejected", ..)),
    )
    let partition_reply : Result[Unit, Error] = Ok(
      txn.add_partition("persistent://public/default/tx-topic"),
    ) catch {
      e => Err(e)
    }
    assert_true(partition_reply is Err(UnexpectedResponse(_)))
    let subscription_error : Result[Unit, Error] = Ok(
      txn.add_subscription("persistent://public/default/tx-topic", "tx-sub"),
    ) catch {
      e => Err(e)
    }
    assert_true(
      subscription_error
      is Err(ServerError(message="subscription rejected", ..)),
    )
    let subscription_reply : Result[Unit, Error] = Ok(
      txn.add_subscription("persistent://public/default/tx-topic", "tx-sub"),
    ) catch {
      e => Err(e)
    }
    assert_true(subscription_reply is Err(UnexpectedResponse(_)))
    coord.close()
  })
}

///|
async test "transaction begin and end report broker errors and wrong replies" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let connect = peer.next()
      guard connect.cmd.tc_client_connect_request is Some(tc_req) else {
        fail("expected TC_CLIENT_CONNECT_REQUEST")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
      let resp = @proto.CommandTcClientConnectResponse::default()
      resp.request_id = tc_req.request_id
      reply.tc_client_connect_response = Some(resp)
      peer.send_command(reply)
      for index in 0..<3 {
        let frame = peer.next()
        guard frame.cmd.new_txn is Some(req) else { fail("expected NEW_TXN") }
        let reply = @proto.BaseCommand::default()
        if index == 1 {
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let resp = @proto.CommandSuccess::default()
          resp.request_id = req.request_id
          reply.success = Some(resp)
        } else {
          reply.type_ = @proto.BaseCommand_Type::NEW_TXN_RESPONSE
          let resp = @proto.CommandNewTxnResponse::default()
          resp.request_id = req.request_id
          if index == 0 {
            resp.error = Some(@proto.ServerError::InvalidTxnStatus)
            resp.message = Some("begin rejected")
          } else {
            resp.txnid_least_bits = Some(42UL)
            resp.txnid_most_bits = Some(3UL)
          }
          reply.new_txn_response = Some(resp)
        }
        peer.send_command(reply)
      }
      for index in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.end_txn is Some(req) else { fail("expected END_TXN") }
        let expected = if index == 0 {
          @proto.TxnAction::COMMIT
        } else {
          @proto.TxnAction::ABORT
        }
        assert_eq(req.txn_action, Some(expected))
        let reply = @proto.BaseCommand::default()
        if index == 1 {
          reply.type_ = @proto.BaseCommand_Type::SUCCESS
          let resp = @proto.CommandSuccess::default()
          resp.request_id = req.request_id
          reply.success = Some(resp)
        } else {
          reply.type_ = @proto.BaseCommand_Type::END_TXN_RESPONSE
          let resp = @proto.CommandEndTxnResponse::default()
          resp.request_id = req.request_id
          resp.error = Some(@proto.ServerError::InvalidTxnStatus)
          resp.message = Some("commit rejected")
          reply.end_txn_response = Some(resp)
        }
        peer.send_command(reply)
      }
    })
    let coord = TransactionCoordinator::connect(group, addr, 7UL)
    let begin_error : Result[Transaction, Error] = Ok(coord.new_transaction()) catch {
      e => Err(e)
    }
    assert_true(begin_error is Err(ServerError(message="begin rejected", ..)))
    let begin_reply : Result[Transaction, Error] = Ok(coord.new_transaction()) catch {
      e => Err(e)
    }
    assert_true(begin_reply is Err(UnexpectedResponse(_)))
    let txn = coord.new_transaction()
    let commit_error : Result[Unit, Error] = Ok(txn.commit()) catch {
      e => Err(e)
    }
    assert_true(commit_error is Err(ServerError(message="commit rejected", ..)))
    let abort_reply : Result[Unit, Error] = Ok(txn.abort()) catch {
      e => Err(e)
    }
    assert_true(abort_reply is Err(UnexpectedResponse(_)))
    coord.close()
  })
}
