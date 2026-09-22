// Batched producer: messages are buffered and flushed as one batch.
// Requires a Pulsar broker at pulsar://127.0.0.1:6650.

///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let producer = client.create_producer(
      "persistent://public/default/moonpulsar-batching",
      batching=@pulsar.BatchingConfig::{
        max_messages: 100,
        max_bytes: 131072,
        max_delay_ms: 50,
      },
    )
    let futures = []
    for i in 0..<20 {
      futures.push(
        producer.send_async(
          @pulsar.ProducerMessage::new(@utf8.encode("batched \{i}")),
        ),
      )
    }
    // flush manually instead of waiting for max_delay_ms
    producer.flush()
    for i, f in futures {
      let receipt = f.wait()
      println(
        "sent \{i}: batch_index=\{receipt.message_id.batch_index} entry=\{receipt.message_id.entry_id}",
      )
    }
    producer.close()
    client.close()
  })
}
