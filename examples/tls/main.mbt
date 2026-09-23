// TLS roundtrip. Set PULSAR_TLS_CA_FILE to verify a private broker CA.
// Without it this example skips certificate verification for local testing.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_TLS_URL").unwrap_or(
    "pulsar+ssl://127.0.0.1:6651",
  )
  @async.with_task_group(async fn(group) {
    let client = match @env.get_env_var("PULSAR_TLS_CA_FILE") {
      Some(ca_file) => @pulsar.Client::connect_tls_with_ca(group, url, ca_file)
      None => @pulsar.Client::connect_tls_insecure(group, url)
    }
    let topic = "persistent://public/default/moonpulsar-tls"
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"over TLS")))
    let consumer = client.create_consumer(
      topic,
      "tls-subscription",
      initial_position=Earliest,
    )
    let message = consumer.receive()
    println("received over TLS: \{@utf8.decode(message.payload())}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
