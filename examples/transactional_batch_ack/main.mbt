///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-transactional-batch-ack",
  )
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let index_ack = @env.get_env_var("PULSAR_BATCH_INDEX_ACK") == Some("true")
  @async.with_task_group(async fn(group) {
    let options = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.ClientOptions::new(url, auth=@pulsar.TokenAuth::new(token))
      None => @pulsar.ClientOptions::new(url)
    }
    let client = @pulsar.Client::connect_with_options(group, options)
    let subscription = "moonpulsar-txn-batch-\{@async.now()}"
    let consumer = client.create_consumer(
      topic,
      subscription,
      initial_position=Earliest,
      enable_batch_index_ack=index_ack,
    )
    let producer = client.create_producer(topic, batching=@pulsar.BatchingConfig::{
      max_messages: 2,
      max_bytes: 4096,
      max_delay_ms: 60000,
    })
    let first_receipt = producer.send_async(
      @pulsar.ProducerMessage::new(b"txn-first"),
    )
    let second_receipt = producer.send_async(
      @pulsar.ProducerMessage::new(b"txn-second"),
    )
    producer.flush()
    assert_eq(first_receipt.wait().message_id.batch_index, 0)
    assert_eq(second_receipt.wait().message_id.batch_index, 1)
    let first = @async.with_timeout(10000, async fn() { consumer.receive() })
    let second = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(first.payload(), b"txn-first")
    assert_eq(second.payload(), b"txn-second")
    let coordinator = client.transaction_coordinator(0UL)
    let aborted = coordinator.new_transaction()
    first.ack_txn(aborted)
    second.ack_txn(aborted)
    aborted.abort()
    consumer.close()
    let reopened = client.create_consumer(
      topic,
      subscription,
      enable_batch_index_ack=index_ack,
    )
    let redelivered_first = @async.with_timeout(10000, async fn() {
      reopened.receive()
    })
    let redelivered_second = @async.with_timeout(10000, async fn() {
      reopened.receive()
    })
    assert_eq(redelivered_first.payload(), b"txn-first")
    assert_eq(redelivered_second.payload(), b"txn-second")
    let committed = coordinator.new_transaction()
    redelivered_first.ack_txn(committed)
    redelivered_second.ack_txn(committed)
    committed.commit()
    reopened.close()
    let verified = client.create_consumer(
      topic,
      subscription,
      enable_batch_index_ack=index_ack,
    )
    let no_more : Result[@pulsar.Message, Error] = Ok(
      @async.with_timeout(1000, async fn() { verified.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(no_more is Err(@async.TimeoutError))
    verified.close()
    coordinator.close()
    producer.close()
    client.close()
    println(
      "Transactional batch ACK abort and commit OK; index_ack=\{index_ack}",
    )
  })
}
