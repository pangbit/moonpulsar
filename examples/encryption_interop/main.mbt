///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-encryption-interop",
  )
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let options = match @env.get_env_var("PULSAR_TOKEN") {
      Some(token) =>
        @pulsar.ClientOptions::new(url, auth=@pulsar.TokenAuth::new(token))
      None => @pulsar.ClientOptions::new(url)
    }
    let client = @pulsar.Client::connect_with_options(group, options)
    let crypto = @pulsar.MessageCrypto::new()
    crypto.load_public_key_file(
      "test-key", "testdata/encryption/rsa_public.pem",
    )
    crypto.load_private_key_file(
      "test-key", "testdata/encryption/rsa_private.pem",
    )
    let public_pem = @fs.read_file("testdata/encryption/rsa_public.pem").binary()
    let private_pem = @fs.read_file("testdata/encryption/rsa_private.pem").binary()
    let callback_crypto = @pulsar.MessageCrypto::new(
      public_key_reader=fn(name) {
        if name == "test-key" {
          Some(public_pem)
        } else {
          None
        }
      },
      private_key_reader=fn(name) {
        if name == "test-key" {
          Some(private_pem)
        } else {
          None
        }
      },
    )
    let consumer = client.create_consumer(
      topic + "-go",
      "moonpulsar-encryption-interop-\{@async.now()}",
      initial_position=Earliest,
      message_crypto=crypto,
    )
    let incoming = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(incoming.payload(), b"encrypted-from-go")
    incoming.ack(await_response=true)
    consumer.close()
    let reader = client.create_reader(
      topic + "-go",
      start_message_id=@pulsar.MessageId::earliest(),
      message_crypto=callback_crypto,
    )
    let read_back = @async.with_timeout(10000, async fn() { reader.next() })
    assert_eq(read_back.unwrap().payload(), b"encrypted-from-go")
    reader.close()
    let producer = client.create_producer(
      topic + "-moonbit",
      message_crypto=crypto,
      encryption_key_names=["test-key"],
    )
    ignore(
      producer.send(@pulsar.ProducerMessage::new(b"encrypted-from-moonbit")),
    )
    producer.close()
    let compressed_producer = client.create_producer(
      topic + "-compressed",
      compression=Zlib,
      message_crypto=callback_crypto,
      encryption_key_names=["test-key"],
    )
    ignore(
      compressed_producer.send(@pulsar.ProducerMessage::new(b"encrypted-zlib")),
    )
    compressed_producer.close()
    let batch_producer = client.create_producer(
      topic + "-batch",
      batching=@pulsar.BatchingConfig::{
        max_messages: 2,
        max_bytes: 4096,
        max_delay_ms: 1000,
      },
      message_crypto=crypto,
      encryption_key_names=["test-key"],
    )
    let batch_first = batch_producer.send_async(
      @pulsar.ProducerMessage::new(b"encrypted-batch-first"),
    )
    let batch_second = batch_producer.send_async(
      @pulsar.ProducerMessage::new(b"encrypted-batch-second"),
    )
    assert_eq(batch_first.wait().message_id.batch_index, 0)
    assert_eq(batch_second.wait().message_id.batch_index, 1)
    batch_producer.close()
    let without_key = client.create_consumer(
      topic + "-moonbit",
      "moonpulsar-encryption-missing-key-\{@async.now()}",
      initial_position=Earliest,
    )
    let unexpected : Result[@pulsar.Message, Error] = Ok(
      @async.with_timeout(10000, async fn() { without_key.receive() }),
    ) catch {
      e => Err(e)
    }
    assert_true(unexpected is Err(@pulsar.PulsarError::InvalidFrame(_)))
    without_key.close()
    let retry_source = topic + "-retry-source"
    let retry_topic = topic + "-retry"
    let dlq_topic = topic + "-dlq"
    let retry_consumer = client.create_retry_consumer(
      retry_source,
      "moonpulsar-crypto-retry-\{@async.now()}",
      retry_topic,
      @pulsar.DeadLetterPolicy::new(1U, dlq_topic),
      initial_position=Earliest,
      message_crypto=crypto,
      encryption_key_names=["test-key"],
    )
    let dlq_consumer = client.create_consumer(
      dlq_topic,
      "moonpulsar-crypto-dlq-\{@async.now()}",
      initial_position=Earliest,
      message_crypto=crypto,
    )
    let retry_producer = client.create_producer(
      retry_source,
      message_crypto=crypto,
      encryption_key_names=["test-key"],
    )
    ignore(
      retry_producer.send(@pulsar.ProducerMessage::new(b"encrypted-retry")),
    )
    let first = @async.with_timeout(10000, async fn() {
      retry_consumer.receive()
    })
    assert_eq(first.payload(), b"encrypted-retry")
    retry_consumer.reconsume_later(first)
    let retried = @async.with_timeout(10000, async fn() {
      retry_consumer.receive()
    })
    assert_eq(retried.topic(), retry_topic)
    assert_eq(retried.payload(), b"encrypted-retry")
    retry_consumer.reconsume_later(retried)
    let dead_lettered = @async.with_timeout(10000, async fn() {
      dlq_consumer.receive()
    })
    assert_eq(dead_lettered.payload(), b"encrypted-retry")
    dead_lettered.ack()
    retry_producer.close()
    retry_consumer.close()
    dlq_consumer.close()
    let retry_reader = client.create_reader(
      retry_topic,
      start_message_id=@pulsar.MessageId::earliest(),
    )
    let clear_retry : Result[@pulsar.Message?, Error] = Ok(
      @async.with_timeout(10000, async fn() { retry_reader.next() }),
    ) catch {
      e => Err(e)
    }
    assert_true(clear_retry is Err(@pulsar.PulsarError::InvalidFrame(_)))
    retry_reader.close()
    let table_topic = topic + "-table"
    let table_codec = @pulsar.SchemaCodec::string()
    let table_producer = client.create_producer(
      table_topic,
      schema=table_codec.info(),
      message_crypto=crypto,
      encryption_key_names=["test-key"],
    )
    ignore(
      table_producer.send(
        @pulsar.ProducerMessage::new(
          table_codec.encode("encrypted-table"),
          key="table-key",
        ),
      ),
    )
    let table = @async.with_timeout(10000, async fn() {
      client.create_typed_table_view(
        table_topic,
        table_codec,
        message_crypto=callback_crypto,
      )
    })
    assert_eq(table.get("table-key"), Some("encrypted-table"))
    table.close()
    let table_without_key : Result[@pulsar.TypedTableView[String], Error] = Ok(
      @async.with_timeout(10000, async fn() {
        client.create_typed_table_view(table_topic, table_codec)
      }),
    ) catch {
      e => Err(e)
    }
    assert_true(table_without_key is Err(@pulsar.PulsarError::InvalidFrame(_)))
    table_producer.close()
    client.close()
    println("Go/MoonBit crypto, Reader, TableView, retry and DLQ OK")
  })
}
