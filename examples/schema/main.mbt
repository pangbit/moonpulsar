// Schema declaration and typed payload conversion.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let client = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.Client::connect(group, url, auth=@pulsar.TokenAuth::new(token))
      None => @pulsar.Client::connect(group, url)
    }
    let topic = "persistent://public/default/moonpulsar-schema"
    let codec = @pulsar.SchemaCodec::string()
    let producer = client.create_producer(topic, schema=codec.info())
    ignore(
      producer.send(
        @pulsar.ProducerMessage::new(codec.encode("string-schema message")),
      ),
    )
    let consumer = client.create_consumer(
      topic,
      "schema-subscription",
      initial_position=Earliest,
      schema=codec.info(),
    )
    let message = consumer.receive()
    println("received: \{message.decode(codec)}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
