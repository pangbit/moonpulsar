///|
async fn main {
  @async.with_task_group(async fn(group) {
    let url = @env.get_env_var("PULSAR_URL").unwrap()
    let host = if url.contains("127.0.0.2") { "127.0.0.2" } else { "127.0.0.1" }
    let tcp = @socket.Tcp::connect_to_host(host, port=27666)
    tcp.close()
    let cert = @env.get_env_var("PULSAR_TLS_CLIENT_CERT_FILE").unwrap_or("")
    let key = @env.get_env_var("PULSAR_TLS_CLIENT_KEY_FILE").unwrap_or("")
    let identity = if cert == "" { None } else { Some(@pulsar.TlsIdentity::new(cert, key)) }
    let result : Result[@pulsar.Client, Error] = Ok(@pulsar.Client::connect_with_options(group,
      @pulsar.ClientOptions::new(@env.get_env_var("PULSAR_URL").unwrap(),
        ca_pem_file=@env.get_env_var("PULSAR_TLS_CA_FILE").unwrap(),
        tls_identity?=identity, tls_min_version=TLS12, connection_timeout_ms=5000))) catch { e => Err(e) }
    if @env.get_env_var("EXPECT_TLS_FAILURE") is Some("true") {
      guard result is Err(error) else { fail("TLS unexpectedly accepted invalid credentials") }
      println("Observed TLS rejection: \{error.to_json().stringify()}")
      println("PASS TLS certificate rejection")
    } else {
      let client = result.unwrap()
      let topic = @env.get_env_var("PULSAR_TOPIC").unwrap()
      let consumer = client.create_consumer(topic, "probe", initial_position=Earliest)
      let producer = client.create_producer(topic)
      ignore(producer.send(@pulsar.ProducerMessage::new(b"mTLS roundtrip")))
      let message = @async.with_timeout(10000, async fn() { consumer.receive() })
      assert_eq(message.payload(), b"mTLS roundtrip")
      message.ack(await_response=true)
      consumer.close()
      producer.close()
      client.close()
      println("PASS mTLS full topic roundtrip")
    }
  })
}
