// Admin + partitioned topics: create a partitioned topic via the admin REST
// API, then produce and consume through the partitioned facades.
// Env: PULSAR_URL (broker), PULSAR_ADMIN_URL (admin REST, default http://127.0.0.1:8080)

///|
async fn main {
  let admin_url = @env.get_env_var("PULSAR_ADMIN_URL").unwrap_or(
    "http://127.0.0.1:8080",
  )
  let broker_url = @env.get_env_var("PULSAR_URL").unwrap_or(
    "pulsar://127.0.0.1:6650",
  )
  let admin = @pulsar.Admin::new(admin_url)
  let topic = "persistent://public/default/moonpulsar-partitioned"
  // idempotent-ish setup: tolerate 409 (already exists)
  ignore(
    admin.create_partitioned_topic(topic, 3) catch {
      @pulsar.PulsarError::ServerError(code=409, ..) => ()
      e => raise e
    },
  )
  let topics = admin.list_topics("public", "default")
  println("topics: \{topics.length()}")
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, broker_url)
    let partitions = client.partitions_of(topic)
    println("partitions: \{partitions}")
    let producer = client.create_producer(topic)
    let consumer = client.create_consumer(
      topic,
      "partitioned-sub",
      initial_position=Earliest,
    )
    for i in 0..<9 {
      ignore(
        producer.send(
          @pulsar.ProducerMessage::new(
            @utf8.encode("partitioned \{i}"),
            key="key-\{i % 3}",
          ),
        ),
      )
    }
    for _ in 0..<9 {
      let message = consumer.receive()
      println("got \{@utf8.decode(message.payload())} from \{message.topic()}")
      message.ack()
    }
    consumer.close()
    producer.close()
    client.close()
  })
  println("partitioned roundtrip OK")
}
