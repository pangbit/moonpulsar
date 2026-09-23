// Broker URL from PULSAR_URL, defaulting to a local standalone.
///|
fn service_url() -> String {
  @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
}

///|
async fn consume_loop(
  name : String,
  consumer : @pulsar.Consumer,
  budget : Ref[Int],
) -> Unit {
  // workers race for the remaining budget; whichever the broker's shared
  // dispatcher picks drains the queue
  while budget.val > 0 {
    let message = @async.with_timeout(10000, async fn() { consumer.receive() })
    budget.val -= 1
    let body = @utf8.decode(message.payload()) catch { _ => "<binary>" }
    println("\{name} received: \{body}")
    message.ack()
  }
}

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, service_url())
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
    // 10 messages total, split between the two workers
    let budget : Ref[Int] = { val: 10, }
    let t1 = group.spawn(async fn() { consume_loop("worker-1", c1, budget) })
    let t2 = group.spawn(async fn() { consume_loop("worker-2", c2, budget) })
    t1.wait()
    t2.wait()
    println("shared subscription drained")
    c1.close()
    c2.close()
    client.close()
  })
}
