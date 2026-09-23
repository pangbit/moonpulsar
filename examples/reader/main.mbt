// Broker URL from PULSAR_URL, defaulting to a local standalone.
///|
fn service_url() -> String {
  @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
}

// Reader: replay a topic from the beginning without a durable
// subscription. Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, service_url())
    let topic = "persistent://public/default/moonpulsar-reader"
    let producer = client.create_producer(topic)
    for i in 0..<5 {
      ignore(
        producer.send(@pulsar.ProducerMessage::new(@utf8.encode("entry \{i}"))),
      )
    }
    // read everything from the start
    let reader = client.create_reader(
      topic,
      start_message_id=@pulsar.MessageId::earliest(),
    )
    while true {
      // has_message_available asks the broker for the last message id
      if !reader.has_message_available() {
        println("reached end of topic")
        break
      }
      match reader.next() {
        Some(message) => println("read: \{@utf8.decode(message.payload())} id=\{message.id().ledger_id}:\{message.id().entry_id}")
        None => println("reached end of topic (broker signalled)")
      }
    }
    reader.close()
    producer.close()
    client.close()
  })
}
