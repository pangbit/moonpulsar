// Authenticate with a role token maintained by an Athenz ZTS sidecar.
// Set PULSAR_ATHENZ_ROLE_TOKEN_FILE to its rotating token file.

///|
async fn main {
  guard @env.get_env_var("PULSAR_ATHENZ_ROLE_TOKEN_FILE") is Some(path) else {
    println("set PULSAR_ATHENZ_ROLE_TOKEN_FILE")
    return
  }
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let provider = @pulsar.athenz_role_token_file_provider(path)
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, auth_provider=provider),
    )
    let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
      "persistent://public/default/moonpulsar-athenz",
    )
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"authenticated hello")))
    let consumer = client.create_consumer(
      topic,
      "athenz-subscription",
      initial_position=Earliest,
    )
    let message = consumer.receive()
    println("received over Athenz: \{@utf8.decode(message.payload())}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
