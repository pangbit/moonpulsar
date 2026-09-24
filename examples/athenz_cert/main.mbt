///|
async fn main {
  let url = @env.get_env_var("ZTS_URL").unwrap()
  let cert = @env.get_env_var("ZTS_CLIENT_CERT_FILE").unwrap()
  let key = @env.get_env_var("ZTS_CLIENT_KEY_FILE").unwrap()
  let ca = @env.get_env_var("ZTS_CA_FILE").unwrap()
  let options = @pulsar.AthenzZtsOptions::new(
    url,
    "provider",
    "",
    "",
    key,
    certificate_file=cert,
    ca_pem_file=ca,
    max_retries=1,
    retry_delay_ms=0,
  )
  let auth = @pulsar.athenz_zts_provider(options)()
  assert_eq(auth.method_name, "athenz")
  assert_eq(auth.data, b"certificate-role")
  println("Athenz certificate ZTS exchange OK")
}
