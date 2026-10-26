// Close during registration must not revive an already retired owner.
// Uses in-memory pipes only, including the delayed protocol response.

///|
async test "close during producer registration targets the replacement and rejects late success" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let received : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let registering : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let completed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let (first, _) = lifecycle_memory_connection(
        group,
        limiter,
        blocked=("send", received),
      )
      defer first.close()
      let replacement : Array[IncomingFrame] = []
      let (second, peer) = lifecycle_memory_connection(
        group,
        limiter,
        blocked=("create", registering),
        observe=frame => replacement.push(frame),
      )
      defer second.close()
      let attempts : Ref[Int] = { val: 0, }
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/replay-registration-close",
        memory_limiter=limiter,
        send_timeout_ms=60000,
        reconnect_with=fn() {
          attempts.val += 1
          second
        },
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
        on_event=fn(event) {
          if event is ProducerReconnected(_, result) {
            replay_notify_event(completed, result is Ok(_))
          }
        },
      )
      let pending = producer.send_async(
        ProducerMessage::new(b"retire-on-close"),
      )
      received.get()
      first.close()
      registering.get()
      let request = replacement[0].cmd.producer.unwrap()
      producer.close()
      let result : Result[SendReceipt, Error] = Ok(pending.wait()) catch {
        e => Err(e)
      }
      assert_true(result is Err(ProducerClosed))
      assert_true(producer.closed)
      let closes = replacement.filter(f => f.cmd.close_producer is Some(_))
      assert_eq(closes.length(), 1)
      assert_eq(
        closes[0].cmd.close_producer.unwrap().producer_id,
        request.producer_id,
      )
      assert_eq(replacement.filter(f => f.cmd.send is Some(_)).length(), 0)
      assert_eq(second.close_handlers.length(), 0)
      assert_eq(second.producer_handlers.length(), 0)
      assert_producer_drained(producer)
      // Finish the previously withheld registration only after close completed.
      let command = @proto.BaseCommand::default()
      command.type_ = @proto.BaseCommand_Type::PRODUCER_SUCCESS
      let success = @proto.CommandProducerSuccess::default()
      success.request_id = request.request_id
      success.producer_name = "memory-producer"
      command.producer_success = Some(success)
      peer.write(encode_command_frame(command))
      assert_false(completed.get())
      assert_false(producer.reconnecting.val)
      assert_true(producer.closed)
      assert_eq(attempts.val, 1)
      assert_eq(replacement.filter(f => f.cmd.send is Some(_)).length(), 0)
      assert_false(second.is_closed())
      assert_producer_drained(producer)
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
    })
  })
}

///|
async test "close during a held replay cannot report reconnect success" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let received : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let completed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
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
      let held : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Blocking(1))
      let (second, _) = lifecycle_memory_connection(
        group,
        limiter,
        transport_closed~,
        blocked=("send", held),
        wrap_writer=inner => {
          let gated = LifecycleBWriter::new(inner, transport_closed~)
          writer.val = Some(gated)
          gated
        },
      )
      defer second.close()
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/replay-close-writer",
        memory_limiter=limiter,
        send_timeout_ms=60000,
        reconnect_with=fn() { second },
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
        on_event=fn(event) {
          if event is ProducerReconnected(_, result) {
            replay_notify_event(completed, result is Ok(_))
          }
        },
      )
      let pending = producer.send_async(
        ProducerMessage::new(b"close-during-replay"),
      )
      received.get()
      first.close()
      let gated = writer.val.unwrap()
      gated.started.get()
      let closing = group.spawn(
        () => producer.close(),
        no_wait=true,
        allow_failure=true,
      )
      while !producer.closed {
        @async.pause()
      }
      gated.armed = false
      gated.unblock.put(())
      assert_false(completed.get())
      closing.wait()
      let result : Result[SendReceipt, Error] = Ok(pending.wait()) catch {
        e => Err(e)
      }
      assert_true(result is Err(ProducerClosed))
      assert_false(producer.reconnecting.val)
      assert_true(producer.closed)
      assert_eq(gated.active, 0)
      assert_eq(gated.injected_errors, 0)
      assert_false(second.is_closed())
      assert_producer_drained(producer)
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
    })
  })
}
