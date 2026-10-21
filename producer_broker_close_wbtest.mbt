///|
async test "broker CLOSE_PRODUCER re-registers and replays pending send" {
  @async.with_timeout(5000, async fn() {
    @async.with_task_group(async fn(group) {
      let second = start_mock_broker(group, async fn(_addr, peer) {
        expect_producer(peer)
        let replay = peer.next()
        guard replay.cmd.send is Some(send) else { fail("expected replay") }
        assert_eq(replay.payload, Some(b"survives-unload"))
        send_receipt(peer, send.producer_id, send.sequence_id)
      })
      let first = start_mock_broker(group, async fn(_addr, peer) {
        expect_producer(peer)
        let pending = peer.next()
        guard pending.cmd.send is Some(send) else { fail("expected SEND") }
        let cmd = @proto.BaseCommand::default()
        cmd.type_ = @proto.BaseCommand_Type::CLOSE_PRODUCER
        let close = @proto.CommandCloseProducer::default()
        close.producer_id = send.producer_id
        close.request_id = 0UL
        cmd.close_producer = Some(close)
        peer.send_command(cmd)
        // Connection remains healthy, just as during a real Topic unload.
      })
      let manager = ConnectionManager::new(group)
      let budget = ClientMemoryLimiter::new(4096)
      let producer = SingleProducer::create_with_memory(
        group,
        manager.get(first),
        "persistent://public/default/unload",
        memory_limiter=budget,
        reconnect_with=() => manager.get(second),
      )
      let receipt = producer.send(ProducerMessage::new(b"survives-unload"))
      assert_eq(receipt.message_id.ledger_id, 10UL)
      assert_false(producer.closed)
      assert_eq(producer.pending.length(), 0)
      assert_eq(producer.payloads.length(), 0)
      assert_eq(budget.used, 0)
      producer.close()
      manager.close()
    })
  })
}
