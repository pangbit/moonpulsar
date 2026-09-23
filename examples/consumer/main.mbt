// Broker URL from PULSAR_URL, defaulting to a local standalone.

///|
fn service_url() -> String {
  @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
}

// Exclusive consumer: receive messages and acknowledge each one.
// Requires a Pulsar broker at pulsar://127.0.0.1:6650.
// Produce some messages first, e.g. with the `producer` example.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, service_url())
    let consumer = client.create_consumer(
      "persistent://public/default/moonpulsar-producer",
      "example-subscription",
      sub_type=Exclusive,
      initial_position=Earliest,
    )
    for _ in 0..<10 {
      let message = consumer.receive()
      let body = @utf8.decode(message.payload()) catch { _ => "<binary>" }
      println(
        "received: \{body} (id: \{message.id().ledger_id}:\{message.id().entry_id})",
      )
      message.ack()
    }
    consumer.close()
    client.close()
  })
}
