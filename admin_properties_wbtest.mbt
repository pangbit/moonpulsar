///|
async test "admin topic properties update read and remove" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      (request, payload) => {
        let path = "/admin/v2/persistent/public/default/events/properties"
        match calls.val {
          0 => {
            assert_eq(request.meth, @http.RequestMethod::Put)
            assert_eq(request.path, path)
            let json = @json.parse(@utf8.decode(payload))
            assert_eq(
              json,
              Json::object({
                "owner": Json::string("moonpulsar"),
                "use": Json::string("test"),
              }),
            )
            calls.val += 1
            (204, "")
          }
          1 => {
            assert_eq(request.meth, @http.RequestMethod::Get)
            assert_eq(request.path, path)
            calls.val += 1
            (200, "{\"owner\":\"moonpulsar\",\"use\":\"test\"}")
          }
          _ => {
            assert_eq(request.meth, @http.RequestMethod::Delete)
            assert_eq(request.path, path + "?key=owner")
            calls.val += 1
            (204, "")
          }
        }
      },
      async fn(url) {
        let admin = Admin::new(url)
        let topic = "persistent://public/default/events"
        admin.update_topic_properties(topic, {
          "owner": "moonpulsar",
          "use": "test",
        })
        assert_eq(
          admin.topic_properties(topic).get("owner"),
          Some("moonpulsar"),
        )
        admin.remove_topic_property(topic, "owner")
        assert_eq(calls.val, 3)
      },
    )
  })
}

///|
async test "admin topic properties reject wrong JSON shapes and unsafe keys" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(group, (_request, _payload) => (200, "{\"owner\":7}"), async fn(
      url,
    ) {
      let result : Result[Map[String, String], Error] = Ok(
        Admin::new(url).topic_properties("persistent://public/default/events"),
      ) catch {
        e => Err(e)
      }
      assert_true(result is Err(InvalidFrame(_)))
    })
  })
  let invalid : Result[Unit, Error] = Ok(
    Admin::new("http://127.0.0.1:1").remove_topic_property(
      "persistent://public/default/events", "owner&force=true",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(LookupFailed(_)))
}
