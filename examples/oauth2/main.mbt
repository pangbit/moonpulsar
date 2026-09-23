// Authenticate to a token-enabled broker with OAuth2 client credentials.
// Set PULSAR_OAUTH2_TOKEN_URL, PULSAR_OAUTH2_CLIENT_ID and
// PULSAR_OAUTH2_CLIENT_SECRET before running this example.

///|
async fn main {
  guard @env.get_env_var("PULSAR_OAUTH2_TOKEN_URL") is Some(token_url) &&
    @env.get_env_var("PULSAR_OAUTH2_CLIENT_ID") is Some(client_id) &&
    @env.get_env_var("PULSAR_OAUTH2_CLIENT_SECRET") is Some(client_secret) else {
    println(
      "set PULSAR_OAUTH2_TOKEN_URL, PULSAR_OAUTH2_CLIENT_ID and PULSAR_OAUTH2_CLIENT_SECRET",
    )
    return
  }
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let provider = @pulsar.oauth2_client_credentials_provider(
    token_url,
    client_id,
    client_secret,
    audience?=@env.get_env_var("PULSAR_OAUTH2_AUDIENCE"),
    scope?=@env.get_env_var("PULSAR_OAUTH2_SCOPE"),
  )
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, auth_provider=provider),
    )
    let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
      "persistent://public/default/moonpulsar-oauth2",
    )
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"authenticated hello")))
    let consumer = client.create_consumer(
      topic,
      "oauth2-subscription",
      initial_position=Earliest,
    )
    let message = consumer.receive()
    println("received over OAuth2: \{@utf8.decode(message.payload())}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
