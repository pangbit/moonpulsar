// Verify that a token file is reread for each new broker connection.
// Supply two valid tokens for the same broker in separate files.

///|
async fn main {
  guard @env.get_env_var("PULSAR_TOKEN_INITIAL_FILE") is Some(initial_file) &&
    @env.get_env_var("PULSAR_TOKEN_NEXT_FILE") is Some(next_file) else {
    println("set PULSAR_TOKEN_INITIAL_FILE and PULSAR_TOKEN_NEXT_FILE")
    return
  }
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-token-rotation",
  )
  let dir = @fs.tmpdir(prefix="moonpulsar-token-rotation-")
  let token_file = dir + "/token"
  defer {
    @fs.remove(token_file) catch {
      _ => ()
    }
    @fs.rmdir(dir) catch {
      _ => ()
    }
  }
  @fs.write_file(token_file, @fs.read_file(initial_file).text())
  @async.with_task_group(async fn(group) {
    let provider = @pulsar.token_file_provider(token_file)
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(
        url,
        auth_provider=provider,
        max_connections_per_broker=2,
      ),
    )
    @fs.write_file(token_file, "invalid-token\n")
    let rejected : Result[Unit, Error] = Ok(
      client.create_producer(topic).close(),
    ) catch {
      error => Err(error)
    }
    assert_true(rejected is Err(@pulsar.ConnectFailed(_)))
    println("broker rejected the rotated invalid token")
    @fs.write_file(token_file, @fs.read_file(next_file).text())
    let producer = client.create_producer(topic)
    let consumer = client.create_consumer(
      topic,
      @env.get_env_var("PULSAR_SUBSCRIPTION").unwrap_or(
        "token-rotation-subscription",
      ),
      initial_position=Earliest,
    )
    ignore(producer.send(@pulsar.ProducerMessage::new(b"rotated token")))
    let message = consumer.receive()
    assert_eq(message.payload(), b"rotated token")
    message.ack()
    println("received after token rotation")
    consumer.close()
    producer.close()
    client.close()
  })
}
