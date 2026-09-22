// Basic producer: synchronous and asynchronous sends.
// Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let producer = client.create_producer(
      "persistent://public/default/moonpulsar-producer",
    )
    // synchronous send: waits for the broker's receipt
    for i in 0..<5 {
      let receipt = producer.send(
        @pulsar.ProducerMessage::new(
          @utf8.encode("sync message \{i}"),
          key="sync-\{i}",
        ),
      )
      println(
        "sent sync \{i}: ledger=\{receipt.message_id.ledger_id} entry=\{receipt.message_id.entry_id}",
      )
    }
    // asynchronous send: resolve futures afterwards
    let futures = []
    for i in 0..<5 {
      futures.push(
        producer.send_async(
          @pulsar.ProducerMessage::new(@utf8.encode("async message \{i}")),
        ),
      )
    }
    for f in futures {
      let receipt = f.wait()
      println("sent async, entry=\{receipt.message_id.entry_id}")
    }
    producer.close()
    client.close()
  })
}
