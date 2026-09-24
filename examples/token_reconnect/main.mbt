// Live token rotation: an external test runner switches the broker signing
// key and restarts the isolated broker after the ready file appears.

///|
async fn main {
  guard @env.get_env_var("PULSAR_TOKEN_INITIAL_FILE") is Some(initial_file) &&
    @env.get_env_var("PULSAR_TOKEN_NEXT_FILE") is Some(next_file) &&
    @env.get_env_var("PULSAR_RECONNECT_READY_FILE") is Some(ready_file) &&
    @env.get_env_var("PULSAR_RECONNECT_DONE_FILE") is Some(done_file) else {
    println("set initial/next token and reconnect ready/done file paths")
    return
  }
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-token-reconnect",
  )
  let dir = @fs.tmpdir(prefix="moonpulsar-token-reconnect-")
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
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(
        url,
        auth_provider=@pulsar.token_file_provider(token_file),
      ),
    )
    let consumer = client.create_consumer(
      topic,
      "token-reconnect-subscription",
      initial_position=Earliest,
    )
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"before rotation")))
    let first = @async.with_timeout(10000, async fn() { consumer.receive() })
    assert_eq(first.payload(), b"before rotation")
    first.ack(await_response=true)
    @fs.write_file(token_file, @fs.read_file(next_file).text())
    @fs.write_file(ready_file, "ready")
    @async.with_timeout(60000, async fn() {
      for ;; {
        let marked : Result[Unit, Error] = Ok(ignore(@fs.read_file(done_file))) catch {
          e => Err(e)
        }
        if marked is Ok(_) {
          break
        }
        @async.sleep(100)
      }
    })
    let rejected : Result[Unit, Error] = Ok(
      @pulsar.Client::connect_with_options(
        group,
        @pulsar.ClientOptions::new(
          url,
          auth_provider=@pulsar.token_file_provider(initial_file),
        ),
      ).close(),
    ) catch {
      e => Err(e)
    }
    assert_true(rejected is Err(@pulsar.ConnectFailed(_)))
    ignore(
      @async.with_timeout(30000, async fn() {
        producer.send(@pulsar.ProducerMessage::new(b"after rotation"))
      }),
    )
    let second = @async.with_timeout(30000, async fn() { consumer.receive() })
    assert_eq(second.payload(), b"after rotation")
    second.ack(await_response=true)
    consumer.close()
    producer.close()
    client.close()
    println("producer and consumer recovered with rotated token")
  })
}
