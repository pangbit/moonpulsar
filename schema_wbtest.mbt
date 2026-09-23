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
      success.schema_version = Some(b"v1")
      reply.producer_success = Some(success)
      peer.send_command(reply)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) && frame.metadata is Some(metadata) else {
        fail("expected schema SEND")
      }
      assert_eq(metadata.schema_version, Some(b"v1"))
      send_receipt(peer, send.producer_id, send.sequence_id)
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
    ignore(producer.send(ProducerMessage::new(b"value")))
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
      let metadata = test_metadata()
      metadata.schema_version = Some(b"v1")
      let (cmd, meta, payload) = message_frame(
        req.consumer_id,
        1,
        1,
        b"schema-value",
        metadata,
      )
      peer.send_message(cmd, meta, payload)
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
    let message = @async.with_timeout(3000, async fn() { consumer.receive() })
    assert_eq(message.schema_version(), Some(b"v1"))
    consumer.close()
    manager.close()
  })
}

///|
async test "client fetches latest and specific topic schema versions" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/schema-versions"
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      for index in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.get_schema is Some(req) else {
          fail("expected GET_SCHEMA")
        }
        assert_eq(req.topic, topic)
        assert_eq(
          req.schema_version,
          if index == 0 {
            None
          } else {
            Some(b"v1")
          },
        )
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::GET_SCHEMA_RESPONSE
        let response = @proto.CommandGetSchemaResponse::default()
        response.request_id = req.request_id
        response.schema = Some(SchemaInfo::string().to_proto())
        response.schema_version = Some(b"v1")
        reply.get_schema_response = Some(response)
        peer.send_command(reply)
      }
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let latest = client.get_schema(topic)
    assert_eq(latest.info.type_, @proto.Schema_Type::String)
    assert_eq(latest.version, Some(b"v1"))
    let specific = client.get_schema(topic, version=b"v1")
    assert_eq(specific, latest)
    client.close()
  })
}

///|
async test "client reports broker schema lookup errors" {
  @async.with_task_group(async fn(group) {
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.get_schema is Some(req) else {
        fail("expected GET_SCHEMA")
      }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::GET_SCHEMA_RESPONSE
      let response = @proto.CommandGetSchemaResponse::default()
      response.request_id = req.request_id
      response.error_code = Some(@proto.ServerError::IncompatibleSchema)
      response.error_message = Some("incompatible version")
      reply.get_schema_response = Some(response)
      peer.send_command(reply)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let result : Result[VersionedSchema, Error] = Ok(
      client.get_schema(
        "persistent://public/default/schema-error",
        version=b"v2",
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(ServerError(message="incompatible version", ..)))
    client.close()
  })
}
