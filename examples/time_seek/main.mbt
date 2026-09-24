// Verify broker timestamp seek for a Consumer and a Reader.

///|
async fn main {
  guard @env.get_env_var("PULSAR_TOPIC") is Some(topic) else {
    println("set PULSAR_TOPIC")
    return
  }
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, url)
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"before")))
    @async.sleep(50)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"after")))
    let consumer = client.create_consumer(
      topic,
      "moonpulsar-time-seek",
      initial_position=Earliest,
    )
    let first = @async.with_timeout(10000, async fn() { consumer.receive() })
    let second = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(first.payload(), b"before")
    assert_eq(second.payload(), b"after")
    let first_time = first.publish_time()
    let second_time = second.publish_time()
    assert_true(first_time > 0UL && second_time > first_time + 1UL)
    first.ack()
    second.ack()
    consumer.seek(@pulsar.SeekPosition::PositionTimestamp(first_time - 1UL))
    let replay_first = @async.with_timeout(10000, async fn() {
      consumer.receive()
    })
    assert_eq(replay_first.payload(), b"before")
    replay_first.ack()
    consumer.seek(@pulsar.SeekPosition::PositionTimestamp(second_time - 1UL))
    let replay_second = @async.with_timeout(10000, async fn() {
      consumer.receive()
    })
    assert_eq(replay_second.payload(), b"after")
    replay_second.ack()
    consumer.close()
    let reader = client.create_reader(
      topic,
      start_message_id=@pulsar.MessageId::earliest(),
    )
    let reader_first = @async.with_timeout(10000, async fn() { reader.next() })
    assert_eq(reader_first.map(fn(m) { m.payload() }), Some(b"before"))
    reader.seek(@pulsar.SeekPosition::PositionTimestamp(second_time - 1UL))
    let reader_second = @async.with_timeout(10000, async fn() { reader.next() })
    assert_eq(reader_second.map(fn(m) { m.payload() }), Some(b"after"))
    reader.seek(@pulsar.SeekPosition::PositionTimestamp(first_time - 1UL))
    let reader_replay = @async.with_timeout(10000, async fn() { reader.next() })
    assert_eq(reader_replay.map(fn(m) { m.payload() }), Some(b"before"))
    reader.close()
    producer.close()
    client.close()
    println("Consumer and Reader timestamp seek passed")
  })
}
