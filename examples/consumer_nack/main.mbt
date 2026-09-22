// Negative acknowledgement: a nacked message is redelivered by the broker.
// Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let producer = client.create_producer(
      "persistent://public/default/moonpulsar-nack",
    )
    ignore(producer.send(@pulsar.ProducerMessage::new(b"try again")))
    let consumer = client.create_consumer(
      "persistent://public/default/moonpulsar-nack",
      "nack-subscription",
      initial_position=Earliest,
    )
    // first delivery: reject it
    let first = consumer.receive()
    println(
      "first delivery, redelivered \{first.redelivery_count()} times; nacking",
    )
    first.nack()
    // the broker redelivers the same message
    let second = consumer.receive()
    println(
      "second delivery, redelivered \{second.redelivery_count()} times; acking",
    )
    assert_eq(second.payload(), first.payload())
    assert_true(second.redelivery_count() > 0)
    second.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
