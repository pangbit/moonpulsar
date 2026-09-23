// Schema declaration wire tests.

///|
async test "producer declares schema in PRODUCER command" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.producer is Some(req) else { fail("expected PRODUCER") }
      guard req.schema is Some(schema) else { fail("schema missing") }
      assert_eq(schema.name, "String")
      assert_eq(schema.type_, @proto.Schema_Type::String)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = req.request_id
      success.producer_name = "p"
      reply.producer_success = Some(success)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let producer = SingleProducer::create(
      group,
      conn,
      "persistent://public/default/t",
      schema=SchemaInfo::string(),
    )
    assert_true(producer.name() == "p")
    producer.close()
    manager.close()
  })
}

///|
async test "consumer declares schema in SUBSCRIBE command" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(req) else { fail("expected SUBSCRIBE") }
      guard req.schema is Some(schema) else { fail("schema missing") }
      assert_eq(schema.type_, @proto.Schema_Type::String)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = req.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      let flow = peer.next()
      guard flow.cmd.flow is Some(_) else { fail("expected FLOW") }
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/t",
      "sub-1",
      schema=SchemaInfo::string(),
    )
    consumer.close()
    manager.close()
  })
}
