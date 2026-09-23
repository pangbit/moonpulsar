// Broker URL from PULSAR_URL, defaulting to a local standalone.

///|
fn service_url() -> String {
  @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
}

// Compressed producer: payloads are compressed before sending and
// transparently decompressed by consumers.
// Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, service_url())
    let topic = "persistent://public/default/moonpulsar-compression"
    let producer = client.create_producer(
      topic,
      compression=@pulsar.CompressionCodec::Zstd,
    )
    let consumer = client.create_consumer(
      topic,
      "compression-subscription",
      initial_position=Earliest,
    )
    let payload = @utf8.encode(
      "compressible payload payload payload payload payload payload payload",
    )
    ignore(producer.send(@pulsar.ProducerMessage::new(payload)))
    let message = consumer.receive()
    assert_eq(message.payload(), payload)
    println("compressed roundtrip OK, \{payload.length()} bytes")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
