///|
fn send_record(publish_time? : UInt64 = 0UL) -> Bytes raise {
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::SEND
  let send = @proto.CommandSend::default()
  send.producer_id = 7UL
  send.sequence_id = 23UL
  command.send = Some(send)
  let metadata = @proto.MessageMetadata::default()
  metadata.producer_name = "fixture-producer"
  metadata.sequence_id = 23UL
  metadata.publish_time = publish_time
  @pulsar.encode_send_frame(command, metadata, b"payload")
}

///|
fn receipt_record(
  producer? : UInt64 = 7UL,
  sequence? : UInt64 = 23UL,
) -> Bytes raise {
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
  let receipt = @proto.CommandSendReceipt::default()
  receipt.producer_id = producer
  receipt.sequence_id = sequence
  receipt.message_id = Some(@proto.MessageIdData::default())
  command.send_receipt = Some(receipt)
  @pulsar.encode_command_frame(command)
}

///|
fn new_gate() -> RelayGate raise {
  RelayGate::new("127.0.0.1:19001", b"payload", "fixture-producer")
}

///|
fn pending_attempt() -> AttemptState {
  let state = AttemptState::new()
  state.attempts = 1
  state.waits = 1
  state
}

///|
fn expect_rejected(action : () -> Unit raise) -> Unit raise {
  let result : Result[Unit, Error] = Ok(action()) catch { error => Err(error) }
  assert_true(result is Err(FixtureError::Rejected(_)))
}

///|
fn replacement_gate() -> (RelayGate, Int, CommandLedger, AttemptState) raise {
  let gate = new_gate()
  let old = gate.open()
  ignore(gate.client_frame(old, send_record()))
  ignore(gate.broker_frame(old, receipt_record()))
  gate.persisted_old_receipt(1)
  let ledger = CommandLedger::new()
  let state = pending_attempt()
  assert_true(
    ledger.apply(
      1, "arm_fault", "11111111111111111111111111111111", gate, state,
    ),
  )
  gate.retire(old)
  assert_true(
    ledger.apply(2, "old_gone", "22222222222222222222222222222222", gate, state),
  )
  assert_true(
    ledger.apply(
      3, "grant_replacement", "33333333333333333333333333333333", gate, state,
    ),
  )
  (gate, old, ledger, state)
}

///|
test "frame fragmentation preserves every original byte" {
  let raw = send_record()
  let decoder = FrameDecoder::new()
  assert_eq(decoder.feed(raw[:1].to_owned()).length(), 0)
  assert_eq(decoder.feed(raw[1:5].to_owned()).length(), 0)
  let frames = decoder.feed(raw[5:].to_owned())
  assert_eq(frames, [raw])
  decoder.finish()
}

///|
test "coalesced frames split without reencoding" {
  let send = send_record()
  let receipt = receipt_record()
  let decoder = FrameDecoder::new()
  assert_eq(decoder.feed(join_bytes(send, receipt)), [send, receipt])
  decoder.finish()
}

///|
test "oversized prefixes and truncated EOF reject before allocation" {
  let decoder = FrameDecoder::new()
  expect_rejected(() => ignore(decoder.feed(b"\xff\xff\xff\xff")))
  let partial = FrameDecoder::new()
  ignore(partial.feed(b"\x00\x00"))
  expect_rejected(() => partial.finish())
  for prefix in [b"\x00\x00\x00\x03", b"\x00\x01\x00\x01"] {
    expect_rejected(() => ignore(FrameDecoder::new().feed(prefix)))
  }
  // This checks framing size only; payload/command validation is separate.
  let maximum = join_bytes(b"\x00\x01\x00\x00", Bytes::make(65536, b'\x00'))
  let bounded = FrameDecoder::new()
  assert_eq(bounded.feed(maximum), [maximum])
  bounded.finish()
}

///|
test "wrong producer receipt passes unchanged and is not held" {
  let gate = new_gate()
  let id = gate.open()
  ignore(gate.client_frame(id, send_record()))
  let raw = receipt_record(producer=8UL)
  assert_true(
    gate.broker_frame(id, raw) is Forward(value, Other) && value == raw,
  )
  assert_eq(gate.held_count, 0)
  let malformed = decode_raw(receipt_record()).cmd
  malformed.type_ = @proto.BaseCommand_Type::SEND
  expect_rejected(() => {
    ignore(gate.broker_frame(id, @pulsar.encode_command_frame(malformed)))
  })
  assert_eq(gate.held_count, 0)
}

///|
test "wrong sequence receipt passes unchanged and is not held" {
  let gate = new_gate()
  let id = gate.open()
  ignore(gate.client_frame(id, send_record()))
  let raw = receipt_record(sequence=24UL)
  assert_true(
    gate.broker_frame(id, raw) is Forward(value, Other) && value == raw,
  )
  assert_eq(gate.held_count, 0)
}

///|
test "same numeric producer and sequence on another connection are unrelated" {
  let gate = new_gate()
  let target = gate.open()
  let unrelated = gate.open()
  ignore(gate.client_frame(target, send_record()))
  let raw = receipt_record()
  assert_true(
    gate.broker_frame(unrelated, raw) is Forward(value, Other) && value == raw,
  )
  assert_eq(gate.held_count, 0)
}

///|
test "replacement receipt on another connection cannot complete the target" {
  let (gate, _, _, _) = replacement_gate()
  let replacement = gate.open()
  let unrelated = gate.open()
  ignore(gate.client_frame(replacement, send_record()))
  let raw = receipt_record()
  assert_true(
    gate.broker_frame(unrelated, raw) is Forward(value, Other) && value == raw,
  )
  assert_false(gate.new_receipt)
}

///|
test "retired old receipt stays quarantined after replacement grant" {
  let (gate, old, _, _) = replacement_gate()
  let raw = receipt_record()
  assert_true(
    gate.broker_frame(old, raw) is Quarantine(value, 2) && value == raw,
  )
  assert_false(gate.new_receipt)
}

///|
test "LOOKUP address changes while routing fields survive" {
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::LOOKUP_RESPONSE
  let response = @proto.CommandLookupTopicResponse::default()
  response.request_id = 81UL
  response.response = Some(
    @proto.CommandLookupTopicResponse_LookupType::Redirect,
  )
  response.authoritative = Some(true)
  response.broker_service_url = Some("pulsar://unreachable.invalid:6650")
  response.broker_service_url_tls = Some(
    "pulsar+ssl://unreachable.invalid:6651",
  )
  command.lookup_topic_response = Some(response)
  let rewritten = lookup_to_frontend(
    @pulsar.encode_command_frame(command),
    "127.0.0.1:19001",
  )
  let result = decode_raw(rewritten).cmd.lookup_topic_response.unwrap()
  assert_eq(result.broker_service_url, Some("pulsar://127.0.0.1:19001"))
  assert_eq(result.broker_service_url_tls, response.broker_service_url_tls)
  assert_eq(result.request_id, 81UL)
  assert_eq(result.authoritative, Some(true))
  assert_eq(result.response, response.response)
}

///|
test "non LOOKUP frames retain identical wire bytes" {
  let raw = receipt_record()
  assert_eq(lookup_to_frontend(raw, "127.0.0.1:19001"), raw)
}

///|
test "new receipt requires the identical replay first" {
  let (gate, _, _, _) = replacement_gate()
  let replacement = gate.open()
  expect_rejected(() => ignore(gate.broker_frame(replacement, receipt_record())))
  let raw = send_record()
  assert_true(
    gate.client_frame(replacement, raw) is Forward(value, Replay) &&
    value == raw,
  )
  let receipt = receipt_record()
  assert_true(
    gate.broker_frame(replacement, receipt) is Forward(value, NewReceipt) &&
    value == receipt,
  )
}

///|
test "equal payload with changed SEND metadata is not an identical replay" {
  let (gate, _, _, _) = replacement_gate()
  let replacement = gate.open()
  expect_rejected(() => {
    ignore(gate.client_frame(replacement, send_record(publish_time=1UL)))
  })
  assert_false(gate.replay_seen)
}

///|
test "duplicate replay and duplicate new receipt have distinct outcomes" {
  let (gate, _, _, _) = replacement_gate()
  let replacement = gate.open()
  ignore(gate.client_frame(replacement, send_record()))
  expect_rejected(() => ignore(gate.client_frame(replacement, send_record())))
  ignore(gate.broker_frame(replacement, receipt_record()))
  assert_true(
    gate.broker_frame(replacement, receipt_record()) is Drop(DuplicateReceipt),
  )
}

///|
test "fault and replacement cannot skip pending or old connection gates" {
  let gate = new_gate()
  let id = gate.open()
  let state = pending_attempt()
  let ledger = CommandLedger::new()
  expect_rejected(() => {
    ignore(
      ledger.apply(
        1, "arm_fault", "11111111111111111111111111111111", gate, state,
      ),
    )
  })
  ignore(gate.client_frame(id, send_record()))
  ignore(gate.broker_frame(id, receipt_record()))
  expect_rejected(() => {
    ignore(
      ledger.apply(
        1, "arm_fault", "11111111111111111111111111111111", gate, state,
      ),
    )
  })
  gate.persisted_old_receipt(1)
  ignore(
    ledger.apply(
      1, "arm_fault", "11111111111111111111111111111111", gate, state,
    ),
  )
  expect_rejected(() => {
    ignore(
      ledger.apply(
        2, "old_gone", "22222222222222222222222222222222", gate, state,
      ),
    )
  })
  assert_false(gate.admit_backend())
  gate.retire(id)
  state.completed = true
  expect_rejected(() => {
    ignore(
      ledger.apply(
        2, "old_gone", "22222222222222222222222222222222", gate, state,
      ),
    )
  })
}

///|
test "identical command retry is idempotent while stale or conflicting commands reject" {
  let gate = new_gate()
  let id = gate.open()
  ignore(gate.client_frame(id, send_record()))
  ignore(gate.broker_frame(id, receipt_record()))
  gate.persisted_old_receipt(1)
  let ledger = CommandLedger::new()
  let state = pending_attempt()
  let nonce = "11111111111111111111111111111111"
  assert_true(ledger.apply(1, "arm_fault", nonce, gate, state))
  assert_false(ledger.apply(1, "arm_fault", nonce, gate, state))
  expect_rejected(() => {
    ignore(
      ledger.apply(
        1, "arm_fault", "22222222222222222222222222222222", gate, state,
      ),
    )
  })
  gate.retire(id)
  expect_rejected(() => ignore(ledger.apply(2, "old_gone", nonce, gate, state)))
  ignore(
    ledger.apply(2, "old_gone", "22222222222222222222222222222222", gate, state),
  )
  expect_rejected(() => ignore(ledger.apply(1, "arm_fault", nonce, gate, state)))
}

///|
test "finish needs original waiter success and confirmed consumer ACK" {
  let (gate, _, ledger, state) = replacement_gate()
  let replacement = gate.open()
  ignore(gate.client_frame(replacement, send_record()))
  ignore(gate.broker_frame(replacement, receipt_record()))
  let nonce = "44444444444444444444444444444444"
  expect_rejected(() => ignore(ledger.apply(4, "finish", nonce, gate, state)))
  state.completed = true
  state.success = true
  state.received = 1
  expect_rejected(() => ignore(ledger.apply(4, "finish", nonce, gate, state)))
  state.acks = 1
  expect_rejected(() => ignore(ledger.apply(4, "finish", nonce, gate, state)))
  state.producer_reconnects = 1
  state.consumer_reconnects = 1
  assert_true(ledger.apply(4, "finish", nonce, gate, state))
}

///|
test "nonliteral and noncanonical endpoints cannot enter relay setup" {
  for
    endpoint in [
      "localhost:6650", "0.0.0.0:6650", "[::1]:6650", "127.0.0.1:0", "127.0.0.1:06650",
      "127.0.0.1:65536",
    ] {
    let result : Result[String, Error] = Ok(loopback_endpoint(endpoint)) catch {
      error => Err(error)
    }
    assert_true(result is Err(_))
  }
  assert_eq(loopback_endpoint("127.0.0.1:6650"), "127.0.0.1:6650")
}
