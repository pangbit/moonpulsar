///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
    "persistent://public/default/moonpulsar-ec-interop",
  )
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let ca = @env.get_env_var("PULSAR_TLS_CA_FILE")
    let cert = @env.get_env_var("PULSAR_TLS_CLIENT_CERT_FILE").unwrap_or("")
    let key = @env.get_env_var("PULSAR_TLS_CLIENT_KEY_FILE").unwrap_or("")
    let identity = if cert != "" || key != "" {
      Some(@pulsar.TlsIdentity::new(cert, key))
    } else {
      None
    }
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, ca_pem_file?=ca, tls_identity?=identity),
    )
    let crypto = @pulsar.MessageCrypto::new()
    crypto.load_public_key_file("ec", "testdata/encryption/ec_public.pem")
    crypto.load_private_key_file("ec", "testdata/encryption/ec_private.pem")
    let consumer = client.create_consumer(
      topic + "-java",
      "moonpulsar-ec-interop-\{@async.now()}",
      initial_position=Earliest,
      message_crypto=crypto,
    )
    let incoming = @async.with_timeout(20000, async fn() { consumer.receive() })
    assert_eq(incoming.payload(), b"from-java-ec")
    incoming.ack(await_response=true)
    consumer.close()
    let producer = client.create_producer(
      topic + "-moonbit",
      message_crypto=crypto,
      encryption_key_names=["ec"],
    )
    ignore(producer.send(@pulsar.ProducerMessage::new(b"from-moonbit-ec")))
    producer.close()
    client.close()
    println("MoonBit ECIES receive/send OK")
  })
}
