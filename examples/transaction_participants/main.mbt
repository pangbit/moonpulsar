///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-txn-participants",
  )
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let options = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.ClientOptions::new(url, auth=@pulsar.TokenAuth::new(token))
      None => @pulsar.ClientOptions::new(url)
    }
    let client = @pulsar.Client::connect_with_options(group, options)
    let subscription = "moonpulsar-txn-participants-\{@async.now()}"
    let consumer = client.create_consumer(topic, subscription)
    let producer = client.create_producer(topic)
    let coordinator = client.transaction_coordinator(0UL)
    let transaction = coordinator.new_transaction()
    transaction.add_partition(topic)
    transaction.add_subscription(topic, subscription)
    transaction.abort()
    coordinator.close()
    producer.close()
    consumer.close()
    client.close()
    println("Transaction producer and subscription registration OK")
  })
}
