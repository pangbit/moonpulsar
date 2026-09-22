// Admin API tests against an in-process mock HTTP server.

///|
/// Serve canned HTTP responses: `respond` maps a request to
/// (status code, body). Stops when the client disconnects.
async fn with_mock_admin(
  group : @async.TaskGroup[Unit],
  respond : async (@http.Request, Bytes) -> (Int, String),
  body : async (String) -> Unit,
) -> Unit {
  let server = @http.Server::Server(@socket.Addr::parse("127.0.0.1:0"))
  let addr = server.addr
  let url = "http://\{addr}"
  group.spawn_bg(
    async fn() {
      defer server.close()
      let (conn, _peer) = server.accept()
      while true {
        let request = conn.read_request()
        let payload = conn.read_all().binary()
        let (code, text) = respond(request, payload)
        conn.send_response(code, "")
        conn.write(text)
        conn.end_response()
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  body(url)
}

///|
async test "create partitioned topic" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, payload) {
        assert_eq(request.meth, @http.RequestMethod::Put)
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/pt/partitions",
        )
        assert_eq(@utf8.decode(payload), "3")
        (204, "")
      },
      async fn(url) {
        let admin = Admin::new(url)
        admin.create_partitioned_topic("persistent://public/default/pt", 3)
      },
    )
  })
}

///|
async test "list topics parses JSON" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(request.meth, @http.RequestMethod::Get)
        assert_true(
          request.path.has_prefix("/admin/v2/persistent/public/default/topics"),
        )
        (
          200, "[\"persistent://public/default/a\",\"persistent://public/default/b\"]",
        )
      },
      async fn(url) {
        let admin = Admin::new(url)
        let topics = admin.list_topics("public", "default")
        assert_eq(topics, [
          "persistent://public/default/a", "persistent://public/default/b",
        ])
      },
    )
  })
}

///|
async test "admin error surfaces status code and body" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(_request, _payload) { (409, "Partitioned topic already exists") },
      async fn(url) {
        let admin = Admin::new(url)
        let result : Result[Unit, Error] = Ok(
          admin.create_partitioned_topic("persistent://public/default/pt", 3),
        ) catch {
          e => Err(e)
        }
        assert_true(result is Err(ServerError(code=409, message=_)))
      },
    )
  })
}

///|
test "topic name parsing" {
  let name = TopicName::parse("persistent://public/default/my-topic")
  assert_eq(name.tenant, "public")
  assert_eq(name.namespace, "default")
  assert_eq(name.topic, "my-topic")
  assert_eq(name.domain, "persistent")
  let bad : Result[TopicName, Error] = Ok(TopicName::parse("public/default/t")) catch {
    e => Err(e)
  }
  assert_true(bad is Err(LookupFailed(_)))
}
