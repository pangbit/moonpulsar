// Real-broker regression for the shared producer, frame and receive budget.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let ns = @env.get_env_var("PULSAR_NAMESPACE").unwrap_or("public/default")
  let prefix = @env.get_env_var("PULSAR_TEST_PREFIX").unwrap_or(
    "moonpulsar-memory-budget",
  )
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, max_memory_bytes=1048576),
    )
    let topic = "persistent://\{ns}/\{prefix}-messages"
    let consumer = client.create_consumer(
      topic,
      "\{prefix}-sub",
      initial_position=Earliest,
      receiver_queue_size=1,
    )
    let producer = client.create_producer(topic)
    for _ in 0..<20 {
      ignore(
        producer.send(@pulsar.ProducerMessage::new(Bytes::make(65536, b'x'))),
      )
      let message = @async.with_timeout(10000, async fn() { consumer.receive() })
      assert_eq(message.payload().length(), 65536)
      message.ack()
    }
    producer.close()
    let chunk_producer = client.create_producer(topic, chunk_size=32768)
    ignore(
      chunk_producer.send(
        @pulsar.ProducerMessage::new(Bytes::make(180000, b'c')),
      ),
    )
    let chunked = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(chunked.payload().length(), 180000)
    chunked.ack()
    chunk_producer.close()
    consumer.close()
    let reader_topic = "persistent://\{ns}/\{prefix}-reader"
    let reader = client.create_reader(
      reader_topic,
      start_message_id=@pulsar.MessageId::earliest(),
      receiver_queue_size=1,
    )
    let reader_producer = client.create_producer(reader_topic)
    ignore(reader_producer.send(@pulsar.ProducerMessage::new(b"reader")))
    let read = @async.with_timeout(10000, async fn() {
      for ;; {
        if reader.next() is Some(message) {
          return message
        }
      }
      raise @pulsar.ConsumerClosed
    })
    assert_eq(read.payload(), b"reader")
    reader_producer.close()
    reader.close()
    client.close()
    println("shared memory budget roundtrip passed")
  })
}
