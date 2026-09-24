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
  let int64_codec = @pulsar.SchemaCodec::int64(little_endian=true)
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
    let int64_topic = topic + "-int64"
    let int64_consumer = client.create_consumer(
      int64_topic,
      "moonpulsar-int64-interop-\{@async.now()}",
      initial_position=Earliest,
      schema=int64_codec.info(),
    )
    let int64_received = @async.with_timeout(10000, async fn() {
      int64_consumer.receive()
    })
    assert_eq(int64_received.decode(int64_codec), -123456789L)
    assert_eq(int64_received.id().batch_index, 0)
    int64_received.ack()
    let int64_second = @async.with_timeout(10000, async fn() {
      int64_consumer.receive()
    })
    assert_eq(int64_second.decode(int64_codec), 42L)
    assert_eq(int64_second.id().batch_index, 1)
    int64_second.ack(await_response=true)
    let int64_producer = client.create_producer(
      int64_topic,
      schema=int64_codec.info(),
    )
    ignore(
      int64_producer.send(
        @pulsar.ProducerMessage::new(int64_codec.encode(987654321L)),
      ),
    )
    int64_producer.close()
    int64_consumer.close()
    let string_codec = @pulsar.SchemaCodec::string()
    let batch_producer = client.create_producer(
      topic + "-batch",
      schema=string_codec.info(),
      batching=@pulsar.BatchingConfig::{
        max_messages: 2,
        max_bytes: 4096,
        max_delay_ms: 1000,
      },
    )
    let first = batch_producer.send_async(
      @pulsar.ProducerMessage::new(string_codec.encode("first")),
    )
    let second = batch_producer.send_async(
      @pulsar.ProducerMessage::new(string_codec.encode("second")),
    )
    assert_eq(first.wait().message_id.batch_index, 0)
    assert_eq(second.wait().message_id.batch_index, 1)
    batch_producer.close()
    client.close()
    println("Protobuf Native, INT64, and batch Go/MoonBit handoff OK")
  })
}
