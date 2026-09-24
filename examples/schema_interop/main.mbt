///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-schema-interop",
  )
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let descriptor_set = @protobuf.base64_decode(
    "CmwKKHRlc3RkYXRhL3Byb3RvYnVmX25hdGl2ZV9rZXlfdmFsdWUucHJvdG8SDHB1bHNhci5wcm90byIyCghLZXlWYWx1ZRIQCgNrZXkYASACKAlSA2tleRIUCgV2YWx1ZRgCIAIoCVIFdmFsdWU=",
  )
  let codec : @pulsar.SchemaCodec[@proto.KeyValue] = @pulsar.SchemaCodec::protobuf_native(
    descriptor_set, "testdata/protobuf_native_key_value.proto", "pulsar.proto.KeyValue",
  )
  @async.with_task_group(async fn(group) {
    let options = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.ClientOptions::new(url, auth=@pulsar.TokenAuth::new(token))
      None => @pulsar.ClientOptions::new(url)
    }
    let client = @pulsar.Client::connect_with_options(group, options)
    let consumer = client.create_consumer(
      topic,
      "moonpulsar-schema-interop-\{@async.now()}",
      initial_position=Earliest,
      schema=codec.info(),
    )
    let received = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(received.decode(codec), @proto.KeyValue::KeyValue("source", "go"))
    received.ack(await_response=true)
    let producer = client.create_producer(topic, schema=codec.info())
    ignore(
      producer.send(
        @pulsar.ProducerMessage::new(
          codec.encode(@proto.KeyValue::KeyValue("source", "moonbit")),
        ),
      ),
    )
    producer.close()
    consumer.close()
    client.close()
    println("Protobuf Native Go-to-MoonBit and MoonBit-to-Go handoff OK")
  })
}
