// Shared subscription: two consumers on the same subscription split the
// message stream. Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn consume_loop(
  name : String,
  consumer : @pulsar.Consumer,
  count : Int,
) -> Unit {
  for _ in 0..<count {
    let message = consumer.receive()
    let body = @utf8.decode(message.payload()) catch { _ => "<binary>" }
    println("\{name} received: \{body}")
    message.ack()
  }
}

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let c1 = client.create_consumer(
      "persistent://public/default/moonpulsar-producer",
      "shared-subscription",
      sub_type=Shared,
      consumer_name="worker-1",
      initial_position=Earliest,
    )
    let c2 = client.create_consumer(
      "persistent://public/default/moonpulsar-producer",
      "shared-subscription",
      sub_type=Shared,
      consumer_name="worker-2",
      initial_position=Earliest,
    )
    // both workers race on the same subscription; the broker balances them
    let t1 = group.spawn(async fn() { consume_loop("worker-1", c1, 5) })
    let t2 = group.spawn(async fn() { consume_loop("worker-2", c2, 5) })
    t1.wait()
    t2.wait()
    c1.close()
    c2.close()
    client.close()
  })
}
