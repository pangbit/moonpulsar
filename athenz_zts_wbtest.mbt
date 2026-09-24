///|
test "Athenz ZTS options validate identity, expiry and retry bounds" {
  let invalid : Result[AthenzZtsOptions, Error] = Ok(
    AthenzZtsOptions::new("", "provider", "tenant", "service", "key.pem"),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
  let bad_retry : Result[AthenzZtsOptions, Error] = Ok(
    AthenzZtsOptions::new(
      "http://zts",
      "provider",
      "tenant",
      "service",
      "key.pem",
      max_retries=-1,
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(bad_retry is Err(InvalidFrame(_)))
}

///|
async test "Athenz service NToken carries a verifiable RSA signature" {
  let options = AthenzZtsOptions::new(
    "http://zts",
    "provider",
    "tenant",
    "service",
    "testdata/encryption/rsa_private.pem",
    key_id="v1",
  )
  let token = athenz_service_token(options)
  guard token.split_once(";s=") is Some((unsigned, signature)) else {
    fail("missing Athenz signature")
  }
  assert_true(unsigned.has_prefix("v=S1;d=tenant;n=service;k=v1;a="))
  let ordinary = signature
    .replace_all(old=".", new="+")
    .replace_all(old="_", new="/")
    .replace_all(old="-", new="=")
  let raw = @base64.decode(ordinary)
  let public_key = EncryptionPublicKey::from_pem(
    @fs.read_file("testdata/encryption/rsa_public.pem").binary(),
  )
  assert_true(
    @crypto.rsa_pkcs1_v15_verify_with(
      @utf8.encode(unsigned),
      raw,
      public_key.modulus,
      public_key.exponent,
      @crypto.rsa_hash_sha256,
    ),
  )
}

///|
async test "Athenz ZTS role-token exchange caches and refreshes early" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      (request, _) => {
        assert_eq(request.meth, @http.RequestMethod::Get)
        assert_eq(
          request.path,
          "/zts/v1/domain/provider/token?minExpiryTime=7200&maxExpiryTime=86400&role=reader",
        )
        let principal = request.headers.get(
          @http.CaseInsensitiveString("Athenz-Principal-Auth"),
        )
        assert_true(principal is Some(token) && token.contains(";s="))
        calls.val += 1
        let expiry = @async.now() / 1000L + 30L
        (200, "{\"token\":\"role-\{calls.val}\",\"expiryTime\":\{expiry}}")
      },
      async fn(url) {
        let options = AthenzZtsOptions::new(
          url,
          "provider",
          "tenant",
          "service",
          "testdata/encryption/rsa_private.pem",
          role="reader",
          refresh_before_ms=0,
        )
        let provider = athenz_zts_provider(options)
        assert_eq(provider().data, b"role-1")
        assert_eq(provider().data, b"role-1")
        assert_eq(calls.val, 1)
        let refreshing = athenz_zts_provider(
          AthenzZtsOptions::new(
            url,
            "provider",
            "tenant",
            "service",
            "testdata/encryption/rsa_private.pem",
            role="reader",
            refresh_before_ms=60000,
          ),
        )
        assert_eq(refreshing().data, b"role-2")
        assert_eq(refreshing().data, b"role-3")
      },
    )
  })
}

///|
async test "Athenz ZTS rejects client errors without retry and reports missing key" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      fn(_request, _) {
        calls.val += 1
        (401, "unauthorized")
      },
      async fn(url) {
        let rejected = athenz_zts_provider(
          AthenzZtsOptions::new(
            url, "provider", "tenant", "service", "testdata/encryption/rsa_private.pem",
          ),
        )
        let result : Result[AuthData, Error] = Ok(rejected()) catch {
          e => Err(e)
        }
        assert_true(result is Err(ServerError(code=401, ..)))
        assert_eq(calls.val, 1)
        let missing_key = athenz_zts_provider(
          AthenzZtsOptions::new(
            url,
            "provider",
            "tenant",
            "service",
            "testdata/encryption/no-key.pem",
            max_retries=0,
          ),
        )
        let key_result : Result[AuthData, Error] = Ok(missing_key()) catch {
          e => Err(e)
        }
        assert_true(key_result is Err(AuthenticationFailed(_)))
        assert_eq(calls.val, 1)
      },
    )
  })
}

///|
async test "Athenz ZTS retries server failure and keeps valid cached token" {
  @async.with_task_group(async fn(group) {
    let calls : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      fn(_request, _) {
        calls.val += 1
        if calls.val == 1 || calls.val >= 3 {
          (503, "unavailable")
        } else {
          let expiry = @async.now() / 1000L + 30L
          (200, "{\"token\":\"recovered\",\"expiryTime\":\{expiry}}")
        }
      },
      async fn(url) {
        let provider = athenz_zts_provider(
          AthenzZtsOptions::new(
            url,
            "provider",
            "tenant",
            "service",
            "testdata/encryption/rsa_private.pem",
            refresh_before_ms=60000,
            max_retries=1,
            retry_delay_ms=0,
          ),
        )
        assert_eq(provider().data, b"recovered")
        assert_eq(calls.val, 2)
        assert_eq(provider().data, b"recovered")
        assert_eq(calls.val, 4)
      },
    )
  })
}

///|
async test "Athenz ZTS rejects malformed and expired role tokens" {
  @async.with_task_group(async fn(group) {
    with_mock_admin(
      group,
      fn(request, _) {
        if request.path.contains("role=expired") {
          (200, "{\"token\":\"old\",\"expiryTime\":1}")
        } else {
          (200, "{\"token\":\"missing-expiry\"}")
        }
      },
      async fn(url) {
        for role in ["expired", "malformed"] {
          let provider = athenz_zts_provider(
            AthenzZtsOptions::new(
              url,
              "provider",
              "tenant",
              "service",
              "testdata/encryption/rsa_private.pem",
              role~,
              max_retries=0,
            ),
          )
          let result : Result[AuthData, Error] = Ok(provider()) catch {
            e => Err(e)
          }
          assert_true(result is Err(AuthenticationFailed(_)))
        }
      },
    )
  })
}

///|
async test "Athenz ZTS refreshes role token on broker challenge" {
  @async.with_task_group(async fn(group) {
    let issued : Ref[Int] = { val: 0, }
    with_mock_admin(
      group,
      fn(_request, _) {
        issued.val += 1
        let expiry = @async.now() / 1000L + 30L
        (200, "{\"token\":\"role-\{issued.val}\",\"expiryTime\":\{expiry}}")
      },
      async fn(url) {
        let seen : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
          kind=@aqueue.Kind::Unbounded,
        )
        let addr = start_mock_broker(group, async fn(_addr, peer) {
          let challenge = @proto.BaseCommand::default()
          challenge.type_ = @proto.BaseCommand_Type::AUTH_CHALLENGE
          challenge.auth_challenge = Some(
            @proto.CommandAuthChallenge::default(),
          )
          peer.send_command(challenge)
          let reply = peer.next()
          guard reply.cmd.auth_response is Some(response) &&
            response.response is Some(data) else {
            fail("expected Athenz ZTS AUTH_RESPONSE")
          }
          seen.put(
            data.auth_method_name == Some("athenz") &&
            data.auth_data == Some(b"role-2"),
          )
        })
        let provider = athenz_zts_provider(
          AthenzZtsOptions::new(
            url,
            "provider",
            "tenant",
            "service",
            "testdata/encryption/rsa_private.pem",
            refresh_before_ms=60000,
          ),
        )
        let client = Client::connect_with_options(
          group,
          ClientOptions::new("pulsar://\{addr}", auth_provider=provider),
        )
        assert_true(@async.with_timeout(2000, async fn() { seen.get() }))
        assert_eq(issued.val, 2)
        client.close()
      },
    )
  })
}
