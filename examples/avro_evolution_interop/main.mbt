///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-avro-evolution-interop",
  )
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url),
    )
    let reader = @pulsar.SchemaCodec::avro_datum(
      b"{\"type\":\"record\",\"name\":\"EvolvingUser\",\"fields\":[{\"name\":\"identifier\",\"type\":\"long\",\"aliases\":[\"id\"]},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"active\",\"type\":\"boolean\",\"default\":true}]}",
    )
    let consumer = client.create_consumer(
      topic + "-java-v1",
      "moonpulsar-avro-java-\{@async.now()}",
      initial_position=Earliest,
    )
    let message = @async.with_timeout(20000, async fn() { consumer.receive() })
    assert_true(message.schema_version() is Some(_))
    assert_eq(
      client.decode_avro(message, reader),
      @avro_codec.Datum::Record([
        ("identifier", @avro_codec.Datum::Long(19L)),
        ("name", @avro_codec.Datum::String("from-java-v1")),
        ("active", @avro_codec.Datum::Boolean(true)),
      ]),
    )
    message.ack(await_response=true)
    consumer.close()
    let writer = @pulsar.SchemaCodec::avro_datum(
      b"{\"type\":\"record\",\"name\":\"EvolvingUser\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}",
    )
    let producer = client.create_producer(
      topic + "-moonbit-v1",
      schema=writer.info(),
    )
    ignore(
      producer.send(
        @pulsar.ProducerMessage::new(
          writer.encode(
            @avro_codec.Datum::Record([
              ("id", @avro_codec.Datum::Int(23)),
              ("name", @avro_codec.Datum::String("from-moonbit-v1")),
            ]),
          ),
        ),
      ),
    )
    producer.close()
    client.close()
    println("MoonBit Avro v2 reader and v1 writer OK")
  })
}
