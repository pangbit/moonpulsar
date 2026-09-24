// Optional real-broker regression for queued receive bytes. Run with
// PULSAR_LIVE_QUEUE_URL and a fresh PULSAR_LIVE_QUEUE_TOPIC prefix.

///|
async test "live consumer reports a retained receive-queue budget failure" {
  guard @env.get_env_var("PULSAR_LIVE_QUEUE_URL") is Some(url) &&
    @env.get_env_var("PULSAR_LIVE_QUEUE_TOPIC") is Some(prefix) else {
    return
  }
  @async.with_task_group(async fn(group) {
    let topic = prefix + "-consumer"
    let source = Client::connect(group, url)
    let limited = Client::connect_with_options(
      group,
      ClientOptions::new(url, max_memory_bytes=100000),
    )
    let consumer = limited.create_consumer(
      topic,
      "queue-budget-live",
      initial_position=Earliest,
      receiver_queue_size=2,
    )
    guard consumer.inner is ConsumerImpl::Single(single) else {
      fail("expected a single-topic consumer")
    }
    let producer = source.create_producer(topic)
    ignore(producer.send(ProducerMessage::new(Bytes::make(40000, b'a'))))
    @async.with_timeout(10000, async fn() {
      while single.buffered_messages == 0 ||
            limited.memory_limiter.unwrap().used >= 60000 {
        @async.sleep(10)
      }
    })
    assert_true(limited.memory_limiter.unwrap().used >= 40000)
    ignore(producer.send(ProducerMessage::new(Bytes::make(40000, b'b'))))
    @async.with_timeout(10000, async fn() {
      while limited.memory_limiter.unwrap().used != 0 {
        @async.sleep(10)
      }
    })
    let failed : Result[Message, Error] = Ok(consumer.receive()) catch {
      e => Err(e)
    }
    assert_true(failed is Err(ClientMemoryFull))
    consumer.close()
    limited.close()
    let recovered = source.create_consumer(
      topic,
      "queue-budget-recovered",
      initial_position=Earliest,
    )
    for expected in [b'a', b'b'] {
      let message = @async.with_timeout(10000, async fn() {
        recovered.receive()
      })
      assert_eq(message.payload().length(), 40000)
      assert_eq(message.payload()[0], expected)
      message.ack(await_response=true)
    }
    recovered.close()
    producer.close()
    source.close()
  })
}

///|
async test "live reader reports a retained receive-queue budget failure" {
  guard @env.get_env_var("PULSAR_LIVE_QUEUE_URL") is Some(url) &&
    @env.get_env_var("PULSAR_LIVE_QUEUE_TOPIC") is Some(prefix) else {
    return
  }
  @async.with_task_group(async fn(group) {
    let topic = prefix + "-reader"
    let source = Client::connect(group, url)
    let limited = Client::connect_with_options(
      group,
      ClientOptions::new(url, max_memory_bytes=100000),
    )
    let reader = limited.create_reader(
      topic,
      start_message_id=MessageId::earliest(),
      receiver_queue_size=2,
    )
    let producer = source.create_producer(topic)
    ignore(producer.send(ProducerMessage::new(Bytes::make(40000, b'a'))))
    @async.with_timeout(10000, async fn() {
      while limited.memory_limiter.unwrap().used < 40000 ||
            limited.memory_limiter.unwrap().used >= 60000 {
        @async.sleep(10)
      }
    })
    ignore(producer.send(ProducerMessage::new(Bytes::make(40000, b'b'))))
    @async.with_timeout(10000, async fn() {
      while limited.memory_limiter.unwrap().used != 0 {
        @async.sleep(10)
      }
    })
    let failed : Result[Message?, Error] = Ok(reader.next()) catch {
      e => Err(e)
    }
    assert_true(failed is Err(ClientMemoryFull))
    reader.close()
    limited.close()
    let recovered = source.create_reader(
      topic,
      start_message_id=MessageId::earliest(),
    )
    for expected in [b'a', b'b'] {
      let message = @async.with_timeout(10000, async fn() {
        for ;; {
          if recovered.next() is Some(message) {
            return message
          }
        }
        raise ConsumerClosed
      })
      assert_eq(message.payload().length(), 40000)
      assert_eq(message.payload()[0], expected)
    }
    recovered.close()
    producer.close()
    source.close()
  })
}
