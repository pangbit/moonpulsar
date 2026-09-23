// TLS roundtrip against a broker with a self-signed certificate.
// For real deployments use Client::connect with a trusted CA instead of
// connect_tls_insecure.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_TLS_URL").unwrap_or(
    "pulsar+ssl://127.0.0.1:6651",
  )
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect_tls_insecure(group, url)
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
