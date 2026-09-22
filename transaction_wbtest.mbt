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
