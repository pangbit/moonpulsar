// Shared-handle consequences of cancelling a partially written SEND.
// All transports are bounded in-memory pipes; no network listeners or Broker.

///|
async test "partial write failure terminates shared producer and consumer waiters without reconnect" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let closes : Ref[Int32] = { val: 0, }
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let frames : Array[IncomingFrame] = []
      let (conn, _) = lifecycle_memory_connection(
        group,
        limiter,
        close_calls=closes,
        observe=frame => frames.push(frame),
        wrap_writer=inner => {
          let gated = LifecycleBWriter::new(inner)
          writer.val = Some(gated)
          gated
        },
      )
      defer conn.close()
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/shared-expired",
        memory_limiter=limiter,
        send_timeout_ms=150,
      )
      let sibling = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/shared-sibling",
        memory_limiter=limiter,
        max_pending_messages=1,
        block_if_queue_full=true,
        send_timeout_ms=2000,
      )
      let consumer = SingleConsumer::create_with_memory(
        group,
        conn,
        "persistent://public/default/shared-consumer",
        "subscription",
        memory_limiter=limiter,
      )
      let receiving = group.spawn(
        () => {
          let result : Result[Message, Error] = Ok(consumer.receive()) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      let sending = group.spawn(
        () => {
          let result : Result[SendReceipt, Error] = Ok(
            producer.send(ProducerMessage::new(b"partial")),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      let gated = writer.val.unwrap()
      gated.started.get()
      let waiting_for_lock = group.spawn(
        () => {
          let result : Result[SendReceipt, Error] = Ok(
            sibling.send(ProducerMessage::new(b"queued-at-lock")),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      while sibling.pending.is_empty() {
        @async.sleep(1)
      }
      let pending_frame = sibling.pending.values().collect()[0]
      let slot_started : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let waiting_for_slot = group.spawn(
        () => {
          slot_started.put(())
          let result : Result[SendReceipt, Error] = Ok(
            sibling.send(ProducerMessage::new(b"not-admitted")),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      slot_started.get()
      @async.pause()
      assert_true(receiving.try_wait() is None)
      assert_true(waiting_for_slot.try_wait() is None)
      assert_true(sending.wait() is Err(RequestTimeout))
      assert_true(waiting_for_lock.wait() is Err(ConnectionClosed))
      assert_true(waiting_for_slot.wait() is Err(ProducerClosed))
      assert_true(receiving.wait() is Err(ConnectionClosed))
      assert_eq(gated.prefix_bytes, 1)
      assert_eq(gated.active, 0)
      assert_true(pending_frame.write_task.val is None)
      assert_eq(frames.filter(f => f.cmd.send is Some(_)).length(), 0)
      // A waiter that queued behind the partial frame must never enter Writer.
      assert_eq(closes.val, 1)
      assert_true(conn.is_closed())
      assert_true(producer.closed && sibling.closed && consumer.closed.val)
      lifecycle_b_assert_drained(producer)
      lifecycle_b_assert_drained(sibling)
      producer.close()
      sibling.close()
      consumer.close()
      conn.close()
      assert_eq(closes.val, 1)
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async fn lifecycle_b_shared_rebuild(reject : Bool) -> Unit {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let closes : Ref[Int32] = { val: 0, }
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let original : Array[IncomingFrame] = []
      let replacement : Array[IncomingFrame] = []
      let received : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let (first, _) = lifecycle_memory_connection(
        group,
        limiter,
        close_calls=closes,
        blocked=("send", received),
        observe=frame => original.push(frame),
        wrap_writer=inner => {
          let gated = LifecycleBWriter::new(inner)
          gated.armed = false
          writer.val = Some(gated)
          gated
        },
      )
      defer first.close()
      let (second, peer) = lifecycle_memory_connection(
        group,
        limiter,
        reject~,
        observe=frame => replacement.push(frame),
      )
      defer second.close()
      let attempts = [0, 0, 0]
      let policy = {
        ..RetryPolicy::default(),
        max_retries: if reject {
          0
        } else {
          2
        },
      }
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/shared-rebuild-expired",
        memory_limiter=limiter,
        send_timeout_ms=150,
        reconnect_policy=policy,
        reconnect_with=async fn() {
          attempts[0] += 1
          @async.pause()
          second
        },
      )
      let sibling = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/shared-rebuild-sibling",
        memory_limiter=limiter,
        send_timeout_ms=2000,
        reconnect_policy=policy,
        reconnect_with=async fn() {
          attempts[1] += 1
          @async.pause()
          second
        },
      )
      let consumer = SingleConsumer::create_with_memory(
        group,
        first,
        "persistent://public/default/shared-rebuild-consumer",
        "subscription",
        memory_limiter=limiter,
        reconnect_policy=policy,
        reconnect_with=async fn() {
          attempts[2] += 1
          @async.pause()
          second
        },
      )
      let receiving = group.spawn(
        () => {
          let result : Result[Message, Error] = Ok(consumer.receive()) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      let unconfirmed = sibling.send_async(
        ProducerMessage::new(b"accepted-without-receipt"),
      )
      received.get()
      let sibling_entry = sibling.sends.values().collect()[0]
      let original_deadline = sibling_entry.deadline
      let gated = writer.val.unwrap()
      gated.armed = true
      let sending = group.spawn(
        () => {
          let result : Result[SendReceipt, Error] = Ok(
            producer.send(ProducerMessage::new(b"partial-expiring")),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      gated.started.get()
      let expired_seq = producer.pending.keys().collect()[0]
      let expired_entry = producer.sends.values().collect()[0]
      assert_true(sending.wait() is Err(RequestTimeout))
      while producer.reconnecting.val ||
            sibling.reconnecting.val ||
            consumer.reconnecting.val {
        @async.sleep(1)
      }
      assert_eq(attempts, [1, 1, 1])
      assert_eq(closes.val, 1)
      assert_eq(gated.active, 0)
      assert_eq(gated.prefix_bytes, 1)
      assert_eq(sibling_entry.deadline, original_deadline)
      assert_eq(first.close_handlers.length(), 0)
      assert_eq(first.producer_handlers.length(), 0)
      assert_eq(first.message_handlers.length(), 0)
      assert_eq(first.pending.length(), 0)
      let original_sends = original.filter(f => f.cmd.send is Some(_))
      let replayed = replacement.filter(f => f.cmd.send is Some(_))
      assert_eq(original_sends.length(), 1)
      assert_eq(
        original_sends[0].cmd.send.unwrap().producer_id,
        sibling.producer_id,
      )
      assert_eq(
        replayed
        .filter(f => f.cmd.send.unwrap().producer_id == producer.producer_id)
        .length(),
        0,
      )
      assert_eq(replacement.filter(f => f.cmd.producer is Some(_)).length(), 2)
      assert_eq(replacement.filter(f => f.cmd.subscribe is Some(_)).length(), 1)
      let sibling_result : Result[SendReceipt, Error] = Ok(unconfirmed.wait()) catch {
        e => Err(e)
      }
      if reject {
        assert_true(sibling_result is Err(ConnectionClosed))
        assert_true(receiving.wait() is Err(ConnectionClosed))
        assert_eq(replayed.length(), 0)
        assert_true(producer.closed && sibling.closed && consumer.closed.val)
      } else {
        assert_true(sibling_result is Ok(_))
        assert_eq(replayed.length(), 1)
        assert_eq(
          replayed[0].cmd.send.unwrap().sequence_id,
          original_sends[0].cmd.send.unwrap().sequence_id,
        )
        assert_eq(
          replayed[0].cmd.send.unwrap().producer_id,
          original_sends[0].cmd.send.unwrap().producer_id,
        )
        assert_eq(replayed[0].payload, original_sends[0].payload)
        assert_true(!producer.closed && !sibling.closed && !consumer.closed.val)
        assert_true(receiving.try_wait() is None)
        assert_eq(second.close_handlers.length(), 3)
        assert_eq(second.producer_handlers.length(), 2)
        assert_eq(second.message_handlers.length(), 1)
        let delivery = lifecycle_delivery(consumer.consumer_id)
        peer.write(
          encode_send_frame(
            delivery.cmd,
            delivery.metadata.unwrap(),
            delivery.payload.unwrap(),
          ),
        )
        assert_true(receiving.wait() is Ok(_))
        // Explicit new sends work; neither is an automatic retry of expired data.
        ignore(producer.send(ProducerMessage::new(b"next-after-expiry")))
        ignore(sibling.send(ProducerMessage::new(b"next-after-replay")))
        assert_eq(replacement.filter(f => f.cmd.send is Some(_)).length(), 3)
      }
      let confirmed = producer.last_confirmed_sequence_id
      lifecycle_b_late_receipt(producer, expired_seq)
      producer.finish_send(expired_entry, Err(ConnectionClosed))
      assert_eq(producer.last_confirmed_sequence_id, confirmed)
      let unchanged : Result[SendReceipt, Error] = Ok(
        expired_entry.waiter.get(),
      ) catch {
        e => Err(e)
      }
      assert_true(unchanged is Err(RequestTimeout))
      producer.close()
      sibling.close()
      consumer.close()
      lifecycle_b_assert_drained(producer)
      lifecycle_b_assert_drained(sibling)
      assert_eq(attempts, [1, 1, 1])
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
    })
  })
}

///|
async test "partial write reconnects shared handles once and replays only the live sibling frame" {
  lifecycle_b_shared_rebuild(false)
}

///|
async test "rejected shared rebuild terminates all waiters without replay or extra attempts" {
  lifecycle_b_shared_rebuild(true)
}
