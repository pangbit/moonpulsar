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
      while true {
        let (conn, _peer) = server.accept()
        try {
          while true {
            let request = conn.read_request()
            let payload = conn.read_all().binary()
            let (code, text) = respond(request, payload)
            conn.send_response(code, "")
            conn.write(text)
            conn.end_response()
          }
        } catch {
          _ => ()
        }
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
        assert_true(request.path == "/admin/v2/persistent/public/default")
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
async test "list non-persistent topics selects the correct domain" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(request.path, "/admin/v2/non-persistent/public/default")
        (200, "[\"non-persistent://public/default/events\"]")
      },
      async fn(url) {
        assert_eq(
          Admin::new(url).list_topics(
            "public",
            "default",
            domain="non-persistent",
          ),
          ["non-persistent://public/default/events"],
        )
      },
    )
  })
}

///|
async test "list partitioned topics uses partitioned namespace endpoint" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/partitioned",
        )
        (200, "[\"persistent://public/default/pt\"]")
      },
      async fn(url) {
        assert_eq(
          Admin::new(url).list_partitioned_topics("public", "default"),
          ["persistent://public/default/pt"],
        )
      },
    )
  })
}

///|
async test "partitioned topic stats request per partition detail" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/pt/partitioned-stats?perPartition=true",
        )
        (200, "{\"metadata\":{\"partitions\":2},\"partitions\":{}}")
      },
      async fn(url) {
        let stats = Admin::new(url).partitioned_topic_stats(
          "persistent://public/default/pt",
          per_partition=true,
        )
        assert_true(stats is Object(_))
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
  let non_persistent = TopicName::parse(
    "non-persistent://public/default/events",
  )
  assert_eq(non_persistent.domain, "non-persistent")
  let empty : Result[TopicName, Error] = Ok(
    TopicName::parse("persistent://public//events"),
  ) catch {
    e => Err(e)
  }
  assert_true(empty is Err(LookupFailed(_)))
}

///|
async test "create non-persistent topic" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, payload) {
        assert_eq(request.meth, @http.RequestMethod::Put)
        assert_eq(
          request.path,
          "/admin/v2/non-persistent/public/default/events",
        )
        assert_eq(payload.length(), 0)
        (204, "")
      },
      async fn(url) {
        Admin::new(url).create_topic("non-persistent://public/default/events")
      },
    )
  })
}

///|
async test "update partition count" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, payload) {
        assert_eq(request.meth, @http.RequestMethod::Post)
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/events/partitions",
        )
        assert_eq(@utf8.decode(payload), "8")
        (204, "")
      },
      async fn(url) {
        Admin::new(url).update_partitioned_topic(
          "persistent://public/default/events", 8,
        )
      },
    )
  })
}

///|
async test "partition count parses metadata" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/events/partitions",
        )
        (200, "{\"partitions\":4,\"deleted\":false}")
      },
      async fn(url) {
        assert_eq(
          Admin::new(url).partition_count("persistent://public/default/events"),
          4,
        )
      },
    )
  })
}

///|
async test "partition metadata rejects missing or fractional counts" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(_request, _payload) { (200, "{\"partitions\":1.5}") },
      async fn(url) {
        let result : Result[Int, Error] = Ok(
          Admin::new(url).partition_count("persistent://public/default/events"),
        ) catch {
          e => Err(e)
        }
        assert_true(result is Err(InvalidFrame(_)))
      },
    )
  })
}

///|
async test "admin rejects invalid partition and subscription inputs" {
  let admin = Admin::new("http://127.0.0.1:1")
  let topic = "persistent://public/default/events"
  let create : Result[Unit, Error] = Ok(
    admin.create_partitioned_topic(topic, 0),
  ) catch {
    e => Err(e)
  }
  assert_true(create is Err(LookupFailed(_)))
  let update : Result[Unit, Error] = Ok(
    admin.update_partitioned_topic(topic, -1),
  ) catch {
    e => Err(e)
  }
  assert_true(update is Err(LookupFailed(_)))
  let subscription : Result[Unit, Error] = Ok(
    admin.delete_subscription(topic, "bad/name"),
  ) catch {
    e => Err(e)
  }
  assert_true(subscription is Err(LookupFailed(_)))
}

///|
async test "list subscriptions" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/events/subscriptions",
        )
        (200, "[\"worker\",\"audit\"]")
      },
      async fn(url) {
        assert_eq(
          Admin::new(url).list_subscriptions(
            "persistent://public/default/events",
          ),
          ["worker", "audit"],
        )
      },
    )
  })
}

///|
async test "delete subscription" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      async fn(request, _payload) {
        assert_eq(request.meth, @http.RequestMethod::Delete)
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/events/subscription/worker?force=true",
        )
        (204, "")
      },
      async fn(url) {
        Admin::new(url).delete_subscription(
          "persistent://public/default/events",
          "worker",
          force=true,
        )
      },
    )
  })
}

///|
async test "create subscription sends earliest and latest cursor positions" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      async fn(request, payload) {
        assert_eq(request.meth, @http.RequestMethod::Put)
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/events/subscription/worker",
        )
        if calls.val == 0 {
          assert_eq(@utf8.decode(payload), "{\"ledgerId\":-1,\"entryId\":-1}")
        } else {
          assert_eq(
            @utf8.decode(payload),
            "{\"ledgerId\":9223372036854775807,\"entryId\":9223372036854775807}",
          )
        }
        calls.val += 1
        (204, "")
      },
      async fn(url) {
        let admin = Admin::new(url)
        let topic = "persistent://public/default/events"
        admin.create_subscription(topic, "worker", initial_position=Earliest)
        admin.create_subscription(topic, "worker")
        assert_eq(calls.val, 2)
      },
    )
  })
}

///|
async test "admin subscription backlog operations use documented endpoints" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      async fn(request, payload) {
        assert_eq(request.meth, @http.RequestMethod::Post)
        assert_eq(payload.length(), 0)
        let suffix = match calls.val {
          0 => "skip/3"
          1 => "skip_all"
          _ => "expireMessages/60"
        }
        assert_eq(
          request.path,
          "/admin/v2/persistent/public/default/events/subscription/worker/" +
          suffix,
        )
        calls.val += 1
        (204, "")
      },
      async fn(url) {
        let admin = Admin::new(url)
        let topic = "persistent://public/default/events"
        admin.skip_messages(topic, "worker", 3)
        admin.skip_all_messages(topic, "worker")
        admin.expire_messages(topic, "worker", 60)
        assert_eq(calls.val, 3)
      },
    )
  })
}

///|
async test "admin rejects unsafe path segments and invalid backlog counts" {
  let admin = Admin::new("http://127.0.0.1:1")
  let topic = "persistent://public/default/events"
  let unsafe_topic : Result[TopicName, Error] = Ok(
    TopicName::parse("persistent://public/default/events?force=true"),
  ) catch {
    e => Err(e)
  }
  assert_true(unsafe_topic is Err(LookupFailed(_)))
  let unsafe_sub : Result[Unit, Error] = Ok(
    admin.create_subscription(topic, "worker?force=true"),
  ) catch {
    e => Err(e)
  }
  assert_true(unsafe_sub is Err(LookupFailed(_)))
  let unsafe_namespace : Result[Array[String], Error] = Ok(
    admin.list_topics("public", "default?x"),
  ) catch {
    e => Err(e)
  }
  assert_true(unsafe_namespace is Err(LookupFailed(_)))
  let zero : Result[Unit, Error] = Ok(admin.skip_messages(topic, "worker", 0)) catch {
    e => Err(e)
  }
  assert_true(zero is Err(LookupFailed(_)))
  let negative : Result[Unit, Error] = Ok(
    admin.expire_messages(topic, "worker", -1),
  ) catch {
    e => Err(e)
  }
  assert_true(negative is Err(LookupFailed(_)))
}
