// Deterministic replay admission probes using in-memory pipes only.
// Neither probe opens a socket or needs a Broker, Docker or credentials.

///|
// Event callbacks cannot raise. Deliver queue errors to the waiting test
// through close instead; never turn a failed notification into success.
fn replay_notify_event(queue : @aqueue.Queue[Bool], value : Bool) -> Unit {
  let queued : Result[Bool, Error] = Ok(queue.try_put(value)) catch {
    error => Err(error)
  }
  match queued {
    Ok(true) => ()
    Ok(false) =>
      queue.close(
        error=InvalidFrame(
          "test reconnect notification queue is unexpectedly full",
        ),
        clear=true,
      )
    Err(error) => queue.close(error~, clear=true)
  }
}

///|
async test "replay drains newly admitted frames without repeating unconfirmed writes" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let original : Array[IncomingFrame] = []
      let replacement : Array[IncomingFrame] = []
      let received : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let replayed : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(4),
      )
      let reconnected : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let (first, _) = lifecycle_memory_connection(
        group,
        limiter,
        blocked=("send", received),
        observe=frame => original.push(frame),
      )
      defer first.close()
      let transport_closed : Ref[Bool] = { val: false, }
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let (second, peer) = lifecycle_memory_connection(
        group,
        limiter,
        transport_closed~,
        blocked=("send", replayed),
        observe=frame => replacement.push(frame),
        wrap_writer=inner => {
          let gated = LifecycleBWriter::new(inner, transport_closed~)
          writer.val = Some(gated)
          gated
        },
      )
      defer second.close()
      let owner : Ref[SingleProducer?] = { val: None, }
      let attempts : Ref[Int] = { val: 0, }
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/replay-admission",
        memory_limiter=limiter,
        max_pending_messages=2,
        send_timeout_ms=60000,
        reconnect_with=fn() {
          attempts.val += 1
          second
        },
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
        on_event=fn(event) {
          if event is ProducerReconnected(_, Ok(_)) {
            replay_notify_event(
              reconnected,
              owner.val.unwrap().reconnecting.val,
            )
          }
        },
      )
      owner.val = Some(producer)
      let before = producer.send_async(ProducerMessage::new(b"before-fault"))
      received.get()
      let original_send = original.filter(f => f.cmd.send is Some(_))[0]
      let original_seq = original_send.cmd.send.unwrap().sequence_id
      let original_entry = producer.sends.values().collect()[0]
      let original_deadline = original_entry.deadline
      first.close()
      let gated = writer.val.unwrap()
      gated.started.get()
      assert_true(producer.reconnecting.val)
      // The snapshot already exists, and its first replay is still writing.
      let during = producer.send_async(ProducerMessage::new(b"during-replay"))
      assert_eq(producer.pending.length(), 2)
      assert_eq(original_entry.deadline, original_deadline)
      gated.armed = false
      gated.unblock.put(())
      let reconnecting_at_event = reconnected.get()
      replayed.get()
      replayed.get()
      assert_false(reconnecting_at_event)
      let sends = replacement.filter(f => f.cmd.send is Some(_))
      assert_eq(sends.length(), 2)
      assert_eq(sends[0].payload, original_send.payload)
      assert_eq(sends[0].cmd.send.unwrap().sequence_id, original_seq)
      assert_eq(
        sends[0].cmd.send.unwrap().producer_id,
        original_send.cmd.send.unwrap().producer_id,
      )
      assert_eq(sends[1].payload, Some(b"during-replay"))
      let during_seq = sends[1].cmd.send.unwrap().sequence_id
      assert_true(during_seq > original_seq)
      // Withhold both receipts until replay has finished. Pending old frames
      // must be written once per registration, rather than drained repeatedly.
      assert_eq(producer.pending.length(), 2)
      assert_eq(original_entry.deadline, original_deadline)
      for send in sends {
        let command = @proto.BaseCommand::default()
        command.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
        let receipt = @proto.CommandSendReceipt::default()
        receipt.producer_id = send.cmd.send.unwrap().producer_id
        receipt.sequence_id = send.cmd.send.unwrap().sequence_id
        command.send_receipt = Some(receipt)
        peer.write(encode_command_frame(command))
      }
      assert_eq(before.wait().sequence_id, original_seq)
      assert_eq(during.wait().sequence_id, during_seq)
      assert_eq(attempts.val, 1)
      assert_eq(gated.injected_errors, 0)
      assert_false(producer.closed)
      assert_producer_drained(producer)
      assert_eq(limiter.used, 0)
      producer.close()
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
    })
  })
}

///|
async test "replay retry rewrites surviving frames in the next registration epoch" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let received : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let (first, _) = lifecycle_memory_connection(
        group,
        limiter,
        blocked=("send", received),
      )
      defer first.close()
      let transport_closed : Ref[Bool] = { val: false, }
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let (second, _) = lifecycle_memory_connection(
        group,
        limiter,
        transport_closed~,
        wrap_writer=inner => {
          let gated = LifecycleBWriter::new(inner, transport_closed~)
          writer.val = Some(gated)
          gated
        },
      )
      defer second.close()
      let replacement : Array[IncomingFrame] = []
      let (third, _) = lifecycle_memory_connection(group, limiter, observe=frame => {
        replacement.push(frame)
      })
      defer third.close()
      let attempts : Ref[Int] = { val: 0, }
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/replay-admission-retry",
        memory_limiter=limiter,
        send_timeout_ms=60000,
        reconnect_with=fn() {
          attempts.val += 1
          if attempts.val == 1 {
            second
          } else {
            third
          }
        },
        reconnect_policy={
          max_retries: 1,
          initial_backoff_ms: 1,
          max_backoff_ms: 1,
        },
      )
      let before = producer.send_async(ProducerMessage::new(b"survives-retry"))
      received.get()
      let seq = producer.pending.keys().collect()[0]
      first.close()
      let gated = writer.val.unwrap()
      gated.started.get()
      let during = producer.send_async(
        ProducerMessage::new(b"queued-before-retry"),
      )
      // Fail after the first replay has begun. Its write marker belongs only
      // to this registration and must not suppress the next attempt's replay.
      second.close()
      gated.armed = false
      gated.unblock.put(())
      assert_eq(before.wait().sequence_id, seq)
      let receipt = during.wait()
      assert_true(receipt.sequence_id > seq)
      while producer.reconnecting.val {
        @async.pause()
      }
      let sends = replacement.filter(f => f.cmd.send is Some(_))
      assert_eq(sends.length(), 2)
      assert_eq(sends[0].payload, Some(b"survives-retry"))
      assert_eq(sends[0].cmd.send.unwrap().sequence_id, seq)
      assert_eq(sends[1].payload, Some(b"queued-before-retry"))
      assert_eq(sends[1].cmd.send.unwrap().sequence_id, receipt.sequence_id)
      assert_eq(attempts.val, 2)
      assert_eq(gated.active, 0)
      assert_eq(gated.prefix_bytes, 1)
      assert_eq(gated.injected_errors, 1)
      assert_eq(gated.last_failed_offset, Some(1))
      assert_false(producer.closed)
      assert_producer_drained(producer)
      assert_eq(limiter.used, 0)
      producer.close()
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
      lifecycle_assert_connection_empty(third, limiter)
    })
  })
}
