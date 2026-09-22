// Token authentication: connect with `authenticationPluginClassName:
// org.apache.pulsar.broker.authentication.AuthenticationProviderToken`.
// Set the token via the PULSAR_TOKEN environment variable.
// Requires a Pulsar broker at pulsar://127.0.0.1:6650 with token auth on.

///|
async fn main {
  guard @env.get_env_var("PULSAR_TOKEN") is Some(token) else {
    println("set PULSAR_TOKEN to run this example")
    return
  }
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(
      group,
      "pulsar://127.0.0.1:6650",
      auth=@pulsar.TokenAuth::new(token),
    )
    let topic = "persistent://public/default/moonpulsar-auth"
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"authenticated hello")))
    let consumer = client.create_consumer(
      topic,
      "auth-subscription",
      initial_position=Earliest,
    )
    let message = consumer.receive()
    println("received over token auth: \{@utf8.decode(message.payload())}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
