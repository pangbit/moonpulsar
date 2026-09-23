// Schema declaration: produce with the built-in String schema so schema-
// aware consumers (Java/Python) decode payloads as strings.

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
    let producer = client.create_producer(
      topic,
      schema=@pulsar.SchemaInfo::string(),
    )
    ignore(
      producer.send(
        @pulsar.ProducerMessage::new(@utf8.encode("string-schema message")),
      ),
    )
    let consumer = client.create_consumer(
      topic,
      "schema-subscription",
      initial_position=Earliest,
      schema=@pulsar.SchemaInfo::string(),
    )
    let message = consumer.receive()
    println("received: \{@utf8.decode(message.payload())}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
