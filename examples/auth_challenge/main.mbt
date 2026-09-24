// The isolated test Broker requires a response to an AUTH_CHALLENGE before
// it sends CONNECTED.

///|
async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:7665")
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(url, auth_provider=fn() {
        calls.val += 1
        { method_name: "challenge-test", data: b"hello", }
      }),
    )
    assert_eq(calls.val, 2)
    client.close()
    let rejected_calls : Ref[Int] = { val: 0, }
    let rejected : Result[Unit, Error] = Ok(
      @pulsar.Client::connect_with_options(
        group,
        @pulsar.ClientOptions::new(url, auth_provider=fn() {
          rejected_calls.val += 1
          {
            method_name: "challenge-test",
            data: if rejected_calls.val == 1 {
              b"hello"
            } else {
              b"wrong"
            },
          }
        }),
      ).close(),
    ) catch {
      error => Err(error)
    }
    assert_true(rejected is Err(_))
    assert_eq(rejected_calls.val, 2)
    println(
      "Broker challenge accepted a refreshed response and rejected a wrong one",
    )
  })
}
