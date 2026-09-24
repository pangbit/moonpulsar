// Real-broker pattern topic removal and re-addition check.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let admin_url = @env.get_env_var("PULSAR_ADMIN_URL").unwrap_or(
    "http://127.0.0.1:8080",
  )
  let ns = @env.get_env_var("PULSAR_NAMESPACE").unwrap_or("public/default")
  let prefix = @env.get_env_var("PULSAR_TEST_PREFIX").unwrap_or(
    "moonpulsar-pattern-removal",
  )
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, url)
    let admin = @pulsar.Admin::new(admin_url)
    let topic = "persistent://\{ns}/\{prefix}-item"
    let producer = client.create_producer(topic)
    let consumer = client.create_pattern_consumer(
      ns,
      "^persistent://\{ns}/\{prefix}-.*$",
      "\{prefix}-sub",
      refresh_interval_ms=200,
      initial_position=Earliest,
    )
    @async.with_timeout(10000, async fn() {
      while !consumer.topics().contains(topic) {
        @async.sleep(20)
      }
    })
    ignore(producer.send(@pulsar.ProducerMessage::new(b"before-removal")))
    let first = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(first.topic(), topic)
    assert_eq(first.payload(), b"before-removal")
    first.ack(await_response=true)
    producer.close()
    admin.delete_topic(topic, force=true)
    @async.with_timeout(10000, async fn() {
      while consumer.topics().contains(topic) {
        @async.sleep(20)
      }
    })
    let recreated = client.create_producer(topic)
    @async.with_timeout(10000, async fn() {
      while !consumer.topics().contains(topic) {
        @async.sleep(20)
      }
    })
    ignore(recreated.send(@pulsar.ProducerMessage::new(b"after-readd")))
    let second = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(second.topic(), topic)
    assert_eq(second.payload(), b"after-readd")
    second.ack(await_response=true)
    recreated.close()
    let partitioned = "persistent://\{ns}/\{prefix}-partitioned"
    admin.create_partitioned_topic(partitioned, 2)
    let partitioned_producer = client.create_producer(partitioned)
    @async.with_timeout(10000, async fn() {
      while !consumer.topics().contains(partitioned) {
        @async.sleep(20)
      }
    })
    ignore(
      partitioned_producer.send(
        @pulsar.ProducerMessage::new(b"partitioned-before"),
      ),
    )
    let partitioned_first = @async.with_timeout(10000, async fn() {
      consumer.receive()
    })
    assert_eq(partitioned_first.payload(), b"partitioned-before")
    partitioned_first.ack(await_response=true)
    partitioned_producer.close()
    admin.delete_partitioned_topic(partitioned, force=true)
    @async.with_timeout(10000, async fn() {
      while consumer.topics().contains(partitioned) {
        @async.sleep(20)
      }
    })
    admin.create_partitioned_topic(partitioned, 2)
    let partitioned_recreated = client.create_producer(partitioned)
    @async.with_timeout(10000, async fn() {
      while !consumer.topics().contains(partitioned) {
        @async.sleep(20)
      }
    })
    ignore(
      partitioned_recreated.send(
        @pulsar.ProducerMessage::new(b"partitioned-after"),
      ),
    )
    let partitioned_second = @async.with_timeout(10000, async fn() {
      consumer.receive()
    })
    assert_eq(partitioned_second.payload(), b"partitioned-after")
    partitioned_second.ack(await_response=true)
    partitioned_recreated.close()
    consumer.close()
    client.close()
    admin.delete_topic(topic, force=true)
    admin.delete_partitioned_topic(partitioned, force=true)
    println("pattern topic removal and re-addition passed")
  })
}
