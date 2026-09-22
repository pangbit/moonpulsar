// End-to-end roundtrip: produce a batch of messages, then consume and
// verify them. Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let topic = "persistent://public/default/moonpulsar-roundtrip"
    let producer = client.create_producer(topic)
    let consumer = client.create_consumer(
      topic,
      "roundtrip-subscription",
      initial_position=Earliest,
    )
    let sent = []
    for i in 0..<10 {
      let payload = "roundtrip \{i}"
      ignore(producer.send(@pulsar.ProducerMessage::new(@utf8.encode(payload))))
      sent.push(payload)
    }
    for expected in sent {
      let message = consumer.receive()
      let body = @utf8.decode(message.payload())
      assert_eq(body, expected)
      message.ack()
      println("verified: \{body}")
    }
    println("roundtrip OK")
    consumer.close()
    producer.close()
    client.close()
  })
}
