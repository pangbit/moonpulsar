// Authenticate with an Athenz ZTS service NToken or a sidecar role-token file.
// Set PULSAR_ATHENZ_ZTS_URL, PULSAR_ATHENZ_PROVIDER_DOMAIN,
// PULSAR_ATHENZ_TENANT_DOMAIN, PULSAR_ATHENZ_TENANT_SERVICE and
// PULSAR_ATHENZ_PRIVATE_KEY_FILE for ZTS; otherwise set
// PULSAR_ATHENZ_ROLE_TOKEN_FILE for a rotating sidecar token.

///|
async fn main {
  let provider = match @env.get_env_var("PULSAR_ATHENZ_ZTS_URL") {
    Some(zts_url) => {
      let provider_domain = @env.get_env_var("PULSAR_ATHENZ_PROVIDER_DOMAIN").unwrap_or(
        "",
      )
      let tenant_domain = @env.get_env_var("PULSAR_ATHENZ_TENANT_DOMAIN").unwrap_or(
        "",
      )
      let tenant_service = @env.get_env_var("PULSAR_ATHENZ_TENANT_SERVICE").unwrap_or(
        "",
      )
      let private_key_file = @env.get_env_var("PULSAR_ATHENZ_PRIVATE_KEY_FILE").unwrap_or(
        "",
      )
      @pulsar.athenz_zts_provider(
        @pulsar.AthenzZtsOptions::new(
          zts_url, provider_domain, tenant_domain, tenant_service, private_key_file,
        ),
      )
    }
    None => {
      guard @env.get_env_var("PULSAR_ATHENZ_ROLE_TOKEN_FILE") is Some(path) else {
        println("set PULSAR_ATHENZ_ZTS_URL or PULSAR_ATHENZ_ROLE_TOKEN_FILE")
        return
      }
      @pulsar.athenz_role_token_file_provider(path)
    }
  }
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, auth_provider=provider),
    )
    let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or(
      "persistent://public/default/moonpulsar-athenz",
    )
    let producer = client.create_producer(topic)
    ignore(producer.send(@pulsar.ProducerMessage::new(b"authenticated hello")))
    let consumer = client.create_consumer(
      topic,
      "athenz-subscription",
      initial_position=Earliest,
    )
    let message = consumer.receive()
    println("received over Athenz: \{@utf8.decode(message.payload())}")
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
