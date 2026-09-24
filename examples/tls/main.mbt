// TLS roundtrip. Set PULSAR_TLS_CA_FILE to verify a private broker CA.
// Set PULSAR_TOKEN when the broker requires token authentication.
// System roots are used by default; PULSAR_TLS_INSECURE skips verification.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_TLS_URL").unwrap_or(
    "pulsar+ssl://127.0.0.1:6651",
  )
  @async.with_task_group(async fn(group) {
    let ca = @env.get_env_var("PULSAR_TLS_CA_FILE")
    let insecure = @env.get_env_var("PULSAR_TLS_INSECURE") is Some(_)
    let options = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.ClientOptions::new(
          url,
          auth=@pulsar.TokenAuth::new(token),
          ca_pem_file?=ca,
          tls_insecure=insecure,
        )
      None =>
        @pulsar.ClientOptions::new(url, ca_pem_file?=ca, tls_insecure=insecure)
    }
    let client = @pulsar.Client::connect_with_options(group, options)
    let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
      "persistent://public/default/moonpulsar-tls",
    )
    let subscription = @env.get_env_var("PULSAR_SUBSCRIPTION").unwrap_or(
      "tls-subscription",
    )
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"over TLS")))
    let consumer = client.create_consumer(
      topic,
      subscription,
      initial_position=Earliest,
    )
    let message = consumer.receive()
    println("received over TLS: \{@utf8.decode(message.payload())}")
    message.ack()
    if @env.get_env_var("PULSAR_TLS_RECONNECT_CHECK") is Some(_) {
      println("TLS roundtrip complete; restart the broker now")
      @async.sleep(30000)
      ignore(producer.send(@pulsar.ProducerMessage::new(b"over TLS again")))
      let next = consumer.receive()
      assert_eq(next.payload(), b"over TLS again")
      next.ack()
      println("received after TLS broker restart")
    }
    consumer.close()
    producer.close()
    client.close()
  })
}
