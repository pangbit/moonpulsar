// TLS roundtrip. Set PULSAR_TLS_CA_FILE to verify a private broker CA.
// Set PULSAR_TOKEN when the broker requires token authentication.
// Without a CA file this example skips certificate verification for local testing.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_TLS_URL").unwrap_or(
    "pulsar+ssl://127.0.0.1:6651",
  )
  @async.with_task_group(async fn(group) {
    let client = match
      (@env.get_env_var("PULSAR_TLS_CA_FILE"), @env.get_env_var("PULSAR_TOKEN")) {
      (Some(ca_file), Some(token)) =>
        @pulsar.Client::connect_tls_with_ca(
          group,
          url,
          ca_file,
          auth=@pulsar.TokenAuth::new(token),
        )
      (Some(ca_file), None) =>
        @pulsar.Client::connect_tls_with_ca(group, url, ca_file)
      (None, Some(token)) =>
        @pulsar.Client::connect_tls_insecure(
          group,
          url,
          auth=@pulsar.TokenAuth::new(token),
        )
      (None, None) => @pulsar.Client::connect_tls_insecure(group, url)
    }
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
