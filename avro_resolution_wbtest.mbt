///|
test "Avro resolution applies field aliases, defaults, unions and promotion" {
  let writer = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"old\",\"type\":\"int\"},{\"name\":\"state\",\"type\":[\"null\",\"int\"]}]}",
  )
  let reader = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"new\",\"type\":\"long\",\"aliases\":[\"old\"]},{\"name\":\"state\",\"type\":[\"null\",\"long\"]},{\"name\":\"country\",\"type\":\"string\",\"default\":\"CN\"},{\"name\":\"score\",\"type\":\"long\",\"default\":42},{\"name\":\"maybe\",\"type\":[\"null\",\"string\"],\"default\":null}]}",
  )
  let original = @avro_codec.Datum::Record([
    ("old", @avro_codec.Datum::Int(7)),
    ("state", @avro_codec.Datum::Union(1, @avro_codec.Datum::Int(9))),
  ])
  let converted = resolve_avro_datum(
    writer, reader, writer, reader, original, 0,
  )
  assert_eq(
    converted,
    @avro_codec.Datum::Record([
      ("new", @avro_codec.Datum::Long(7L)),
      ("state", @avro_codec.Datum::Union(1, @avro_codec.Datum::Long(9L))),
      ("country", @avro_codec.Datum::String("CN")),
      ("score", @avro_codec.Datum::Long(42L)),
      ("maybe", @avro_codec.Datum::Union(0, @avro_codec.Datum::Null)),
    ]),
  )
  assert_eq(
    @avro_codec.decode(reader, @avro_codec.encode(reader, converted)),
    converted,
  )
}

///|
test "Avro resolution rejects incompatible field and name changes" {
  let writer = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
  )
  let incompatible = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
  )
  let result : Result[@avro_codec.Datum, Error] = Ok(
    resolve_avro_datum(
      writer,
      incompatible,
      writer,
      incompatible,
      @avro_codec.Datum::Record([("id", @avro_codec.Datum::String("x"))]),
      0,
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(result is Err(SchemaIncompatible(_)))
  let wrong_name = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"Different\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
  )
  let name_result : Result[@avro_codec.Datum, Error] = Ok(
    resolve_avro_datum(
      writer,
      wrong_name,
      writer,
      wrong_name,
      @avro_codec.Datum::Record([("id", @avro_codec.Datum::String("x"))]),
      0,
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(name_result is Err(SchemaIncompatible(_)))
}

///|
test "Avro resolution handles collection values and enum fallback" {
  let writer = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"Data\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"labels\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"state\",\"type\":{\"type\":\"enum\",\"name\":\"State\",\"symbols\":[\"OLD\",\"NEW\"]}}]}",
  )
  let reader = @avro_schema.parse(
    "{\"type\":\"record\",\"name\":\"Data\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"labels\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}},{\"name\":\"state\",\"type\":{\"type\":\"enum\",\"name\":\"State\",\"symbols\":[\"NEW\"],\"default\":\"NEW\"}}]}",
  )
  let source = @avro_codec.Datum::Record([
    ("items", @avro_codec.Datum::Array([@avro_codec.Datum::Int(4)])),
    (
      "labels",
      @avro_codec.Datum::Map(Map([("x", @avro_codec.Datum::String("y"))])),
    ),
    ("state", @avro_codec.Datum::Enum(0, "OLD")),
  ])
  let resolved = resolve_avro_datum(writer, reader, writer, reader, source, 0)
  assert_eq(
    resolved,
    @avro_codec.Datum::Record([
      ("items", @avro_codec.Datum::Array([@avro_codec.Datum::Long(4L)])),
      (
        "labels",
        @avro_codec.Datum::Map(Map([("x", @avro_codec.Datum::Bytes(b"y"))])),
      ),
      ("state", @avro_codec.Datum::Enum(0, "NEW")),
    ]),
  )
}

///|
test "Avro long defaults preserve exact integer bounds" {
  assert_eq(
    avro_default_integer(Json::number(0.0, repr="9223372036854775807")),
    9223372036854775807L,
  )
  assert_eq(
    avro_default_integer(Json::number(0.0, repr="-9223372036854775808")),
    -9223372036854775807L - 1L,
  )
  for text in ["9223372036854775808", "1.5", "-9223372036854775809"] {
    let result : Result[Int64, Error] = Ok(
      avro_default_integer(Json::number(0.0, repr=text)),
    ) catch {
      e => Err(e)
    }
    assert_true(result is Err(SchemaIncompatible(_)))
  }
}

///|
async test "Client decode_avro retries failed lookup and caches writer version" {
  @async.with_task_group(async fn(group) {
    let topic = "persistent://public/default/avro-evolution"
    let writer_definition = b"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"old\",\"type\":\"int\"}]}"
    let reader_definition = b"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"new\",\"type\":\"long\",\"aliases\":[\"old\"]},{\"name\":\"active\",\"type\":\"boolean\",\"default\":true}]}"
    let writer_codec = SchemaCodec::avro_datum(writer_definition)
    let payload = writer_codec.encode(
      @avro_codec.Datum::Record([("old", @avro_codec.Datum::Int(17))]),
    )
    let addr = start_partitioned_broker(group, 0, async fn(_addr, peer) {
      let consumer_id = decryption_test_subscribe(peer)
      for entry in 1UL..<=3UL {
        let metadata = test_metadata()
        metadata.schema_version = Some(b"writer-v1")
        let (cmd, _, _) = message_frame(
          consumer_id, 1UL, entry, payload, metadata,
        )
        peer.send_message(cmd, metadata, payload)
      }
      for attempt in 0..<2 {
        let frame = peer.next()
        guard frame.cmd.get_schema is Some(request) else {
          fail("expected GET_SCHEMA for writer version")
        }
        assert_eq(request.topic, topic)
        assert_eq(request.schema_version, Some(b"writer-v1"))
        let reply = @proto.BaseCommand::default()
        reply.type_ = @proto.BaseCommand_Type::GET_SCHEMA_RESPONSE
        let response = @proto.CommandGetSchemaResponse::default()
        response.request_id = request.request_id
        if attempt == 0 {
          response.error_code = Some(@proto.ServerError::ServiceNotReady)
          response.error_message = Some("schema temporarily unavailable")
        } else {
          response.schema = Some(SchemaInfo::avro(writer_definition).to_proto())
          response.schema_version = Some(b"writer-v1")
        }
        reply.get_schema_response = Some(response)
        peer.send_command(reply)
      }
      let close = peer.next()
      guard close.cmd.close_consumer is Some(request) else {
        fail("second decode unexpectedly fetched schema")
      }
      let closed = @proto.BaseCommand::default()
      closed.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = request.request_id
      closed.success = Some(success)
      peer.send_command(closed)
    })
    let client = Client::connect(group, "pulsar://\{addr}")
    let reader = SchemaCodec::avro_datum(reader_definition)
    let consumer = client.create_consumer(topic, "sub", schema=reader.info())
    let expected = @avro_codec.Datum::Record([
      ("new", @avro_codec.Datum::Long(17L)),
      ("active", @avro_codec.Datum::Boolean(true)),
    ])
    let first : Result[@avro_codec.Datum, Error] = Ok(
      client.decode_avro(consumer.receive(), reader),
    ) catch {
      e => Err(e)
    }
    assert_true(
      first is Err(ServerError(message="schema temporarily unavailable", ..)),
    )
    assert_eq(client.decode_avro(consumer.receive(), reader), expected)
    assert_eq(client.decode_avro(consumer.receive(), reader), expected)
    consumer.close()
    client.close()
  })
}
