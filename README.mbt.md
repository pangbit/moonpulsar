# pangbit/moonpulsar

Apache Pulsar binary protocol client for MoonBit: producer (sync/async send,
batching) and consumer (subscription modes, ack/nack, flow control), with
topic lookup, connection pooling and token authentication.

```mbt
async fn boot(group : @async.TaskGroup[Unit]) -> Unit raise {
  let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
  let producer = client.create_producer("persistent://public/default/my-topic")
  let receipt = producer.send(@pulsar.ProducerMessage::new(b"hello"))
  println(receipt.message_id)

  let consumer = client.create_consumer(
    "persistent://public/default/my-topic",
    "my-subscription",
  )
  let message = consumer.receive()
  message.ack()
  client.close()
}
```

See `README.md` for the full feature matrix and the `examples/` directory for
runnable scenarios.
