// Reader: replay a topic from the beginning without a durable
// subscription. Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
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
      if @async.with_timeout_opt(2000, async fn() { reader.next() })
        is Some(result) {
        match result {
          Some(message) => println("read: \{@utf8.decode(message.payload())}")
          None => {
            println("reached end of topic")
            break
          }
        }
      } else {
        println("no message within 2s")
        break
      }
    }
    reader.close()
    producer.close()
    client.close()
  })
}
