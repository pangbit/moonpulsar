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
    let source = @pulsar.Client::connect(group, url)
    let oversized_topic = "persistent://\{ns}/\{prefix}-oversized"
    let limited = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, max_memory_bytes=65536),
    )
    let limited_consumer = limited.create_consumer(
      oversized_topic,
      "\{prefix}-oversized-sub",
      initial_position=Earliest,
      receiver_queue_size=1,
    )
    let oversized_producer = source.create_producer(oversized_topic)
    ignore(
      oversized_producer.send(
        @pulsar.ProducerMessage::new(Bytes::make(131072, b'o')),
      ),
    )
    let receive_failure : Result[@pulsar.Message, Error] = Ok(
      @async.with_timeout(10000, async fn() { limited_consumer.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(receive_failure is Err(@pulsar.ClientMemoryFull))
    limited_consumer.close()
    limited.close()
    let recovered = source.create_consumer(
      oversized_topic,
      "\{prefix}-recovered-sub",
      initial_position=Earliest,
    )
    let recovered_message = @async.with_timeout(10000, async fn() {
      recovered.receive()
    })
    assert_eq(recovered_message.payload().length(), 131072)
    recovered_message.ack()
    recovered.close()
    oversized_producer.close()
    let oversized_reader_topic = "persistent://\{ns}/\{prefix}-reader-oversized"
    let limited_reader_client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, max_memory_bytes=65536),
    )
    let limited_reader = limited_reader_client.create_reader(
      oversized_reader_topic,
      start_message_id=@pulsar.MessageId::earliest(),
      receiver_queue_size=1,
    )
    let oversized_reader_producer = source.create_producer(
      oversized_reader_topic,
    )
    ignore(
      oversized_reader_producer.send(
        @pulsar.ProducerMessage::new(Bytes::make(131072, b'r')),
      ),
    )
    let reader_failure : Result[@pulsar.Message?, Error] = Ok(
      @async.with_timeout(10000, async fn() { limited_reader.next() }),
    ) catch {
      e => Err(e)
    }
    assert_true(reader_failure is Err(@pulsar.ClientMemoryFull))
    limited_reader.close()
    limited_reader_client.close()
    let recovered_reader = source.create_reader(
      oversized_reader_topic,
      start_message_id=@pulsar.MessageId::earliest(),
    )
    let replayed = @async.with_timeout(10000, async fn() {
      recovered_reader.next()
    })
    assert_eq(replayed.unwrap().payload().length(), 131072)
    recovered_reader.close()
    oversized_reader_producer.close()
    source.close()
    println("shared memory budget roundtrip passed")
  })
}
