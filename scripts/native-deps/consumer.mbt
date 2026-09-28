///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let consumer = client.create_consumer("persistent://public/default/audit", "audit")
    let producer = client.create_producer("persistent://public/default/audit")
    ignore(producer.send(@pulsar.ProducerMessage::new(b"hello")))
    let message = consumer.receive()
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
