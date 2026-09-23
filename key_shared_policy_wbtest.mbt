// KeyShared policies are validated locally and encoded in SUBSCRIBE.

///|
test "sticky KeyShared ranges reject invalid and overlapping values" {
  let empty : Result[KeySharedPolicy, Error] = Ok(KeySharedPolicy::sticky([])) catch {
    e => Err(e)
  }
  assert_true(empty is Err(InvalidFrame(_)))
  let singleton : Result[KeySharedPolicy, Error] = Ok(
    KeySharedPolicy::sticky([(4, 4)]),
  ) catch {
    e => Err(e)
  }
  assert_true(singleton is Err(InvalidFrame(_)))
  let out_of_bounds : Result[KeySharedPolicy, Error] = Ok(
    KeySharedPolicy::sticky([(0, 65536)]),
  ) catch {
    e => Err(e)
  }
  assert_true(out_of_bounds is Err(InvalidFrame(_)))
  let overlap : Result[KeySharedPolicy, Error] = Ok(
    KeySharedPolicy::sticky([(0, 100), (100, 200)]),
  ) catch {
    e => Err(e)
  }
  assert_true(overlap is Err(InvalidFrame(_)))
  ignore(KeySharedPolicy::sticky([(100, 200), (0, 99)]))
  let mutable_ranges = [(0, 100)]
  let stable = KeySharedPolicy::sticky(mutable_ranges)
  mutable_ranges[0] = (300, 400)
  assert_eq(stable.to_proto().hash_ranges[0].start, 0)
  let automatic = KeySharedPolicy::auto_split(allow_out_of_order_delivery=true).to_proto()
  assert_eq(automatic.key_shared_mode, @proto.KeySharedMode::AUTO_SPLIT)
  assert_eq(automatic.allow_out_of_order_delivery, Some(true))
  assert_eq(automatic.hash_ranges.length(), 0)
}

///|
async test "KeyShared policy is sent with subscription" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      let frame = peer.next()
      guard frame.cmd.subscribe is Some(sub) else { fail("expected SUBSCRIBE") }
      assert_eq(sub.sub_type, @proto.CommandSubscribe_SubType::Key_Shared)
      guard sub.key_shared_meta is Some(meta) else {
        fail("missing KeyShared policy")
      }
      assert_eq(meta.key_shared_mode, @proto.KeySharedMode::STICKY)
      assert_eq(meta.allow_out_of_order_delivery, Some(true))
      assert_eq(meta.hash_ranges.length(), 2)
      assert_eq(meta.hash_ranges[0].start, 0)
      assert_eq(meta.hash_ranges[0].end, 100)
      assert_eq(meta.hash_ranges[1].start, 200)
      assert_eq(meta.hash_ranges[1].end, 65535)
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SUCCESS
      let success = @proto.CommandSuccess::default()
      success.request_id = sub.request_id
      reply.success = Some(success)
      peer.send_command(reply)
      assert_true(peer.next().cmd.flow is Some(_))
    })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let policy = KeySharedPolicy::sticky(
      [(0, 100), (200, 65535)],
      allow_out_of_order_delivery=true,
    )
    let consumer = SingleConsumer::create(
      group,
      conn,
      "persistent://public/default/key-shared-policy",
      "key-shared-sub",
      sub_type=KeyShared,
      key_shared_policy=policy,
    )
    consumer.close()
    manager.close()
  })
}

///|
async test "KeyShared policy cannot be used with another subscription mode" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, _peer) {  })
    let manager = ConnectionManager::new(group)
    let conn = manager.get(addr)
    let invalid : Result[SingleConsumer, Error] = Ok(
      SingleConsumer::create(
        group,
        conn,
        "persistent://public/default/key-shared-policy",
        "ordinary-sub",
        sub_type=Shared,
        key_shared_policy=KeySharedPolicy::auto_split(),
      ),
    ) catch {
      e => Err(e)
    }
    assert_true(invalid is Err(InvalidFrame(_)))
    manager.close()
  })
}
