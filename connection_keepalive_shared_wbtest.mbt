// Shared owners use the same pure pipe fixture as lifecycle B. A silent peer
// consumes SEND/PING frames without replying; this is not real broker delivery.

///|
async test "keepalive timeout retires shared sends slot admission and consumer chunks once" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let closes : Ref[Int32] = { val: 0, }
      let sent : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let frames : Array[IncomingFrame] = []
      let (conn, _) = lifecycle_memory_connection(
        group,
        limiter,
        close_calls=closes,
        blocked=("send", sent),
        observe=frame => frames.push(frame),
      )
      defer conn.close()
      let producers : Array[SingleProducer] = []
      let futures : Array[ReceiptFuture] = []
      for index in 0..<2 {
        let producer = SingleProducer::create_with_memory(
          group,
          conn,
          "persistent://public/default/keepalive-terminal-\{index}",
          max_pending_messages=1,
          block_if_queue_full=true,
          send_timeout_ms=2000,
          memory_limiter=limiter,
        )
        producers.push(producer)
        futures.push(producer.send_async(ProducerMessage::new(b"unconfirmed")))
        sent.get()
      }
      let consumer = SingleConsumer::create_with_memory(
        group,
        conn,
        "persistent://public/default/keepalive-terminal-consumer",
        "subscription",
        memory_limiter=limiter,
      )
      consumer.on_frame(
        lifecycle_delivery(consumer.consumer_id, incomplete=true),
      )
      assert_true(limiter.used > 0)
      let entry = producers[0].sends.values().collect()[0]
      let seq = producers[0].pending.keys().collect()[0]
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
      let admission = group.spawn(
        () => {
          let result : Result[SendReceipt, Error] = Ok(
            producers[0].send(ProducerMessage::new(b"not-admitted")),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      @async.pause()
      assert_true(admission.try_wait() is None)
      let heartbeat = group.spawn(
        () => conn.keepalive_loop(100),
        no_wait=true,
        allow_failure=true,
      )
      heartbeat.wait()
      assert_true(conn.is_closed())
      assert_true(admission.wait() is Err(ProducerClosed))
      assert_true(receiving.wait() is Err(ConnectionClosed))
      for future in futures {
        let result : Result[SendReceipt, Error] = Ok(future.wait()) catch {
          e => Err(e)
        }
        assert_true(result is Err(ConnectionClosed))
      }
      assert_eq(frames.filter(f => f.cmd.send is Some(_)).length(), 2)
      assert_eq(frames.filter(f => f.cmd.ping is Some(_)).length(), 1)
      assert_eq(closes.val, 1)
      // Late receipt, duplicate completion and explicit close cannot change the
      // already terminal send or restore its budget/deadline registrations.
      lifecycle_b_late_receipt(producers[0], seq)
      producers[0].finish_send(entry, Err(RequestTimeout))
      let repeated : Result[SendReceipt, Error] = Ok(entry.waiter.get()) catch {
        e => Err(e)
      }
      assert_true(repeated is Err(ConnectionClosed))
      assert_true(producers[0].last_confirmed_sequence_id is None)
      for producer in producers {
        assert_true(producer.closed)
        producer.close()
        lifecycle_b_assert_drained(producer)
      }
      assert_true(consumer.closed.val)
      consumer.close()
      conn.close()
      assert_eq(closes.val, 1)
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async fn keepalive_shared_rebuild(reject : Bool) -> Unit {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let closes : Ref[Int32] = { val: 0, }
      let sent : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let original : Array[IncomingFrame] = []
      let replacement : Array[IncomingFrame] = []
      let (first, _) = lifecycle_memory_connection(
        group,
        limiter,
        close_calls=closes,
        blocked=("send", sent),
        observe=frame => original.push(frame),
      )
      let (second, peer) = lifecycle_memory_connection(
        group,
        limiter,
        reject~,
        observe=frame => replacement.push(frame),
      )
      defer first.close()
      defer second.close()
      let attempts = [0, 0, 0]
      let producers : Array[SingleProducer] = []
      let futures : Array[ReceiptFuture] = []
      let policy = { ..RetryPolicy::default(), max_retries: 0, }
      for index in 0..<2 {
        let producer = SingleProducer::create_with_memory(
          group,
          first,
          "persistent://public/default/keepalive-rebuild-\{index}",
          send_timeout_ms=2000,
          memory_limiter=limiter,
          reconnect_policy=policy,
          reconnect_with=() => {
            attempts[index] += 1
            @async.pause()
            second
          },
        )
        producers.push(producer)
        futures.push(
          producer.send_async(ProducerMessage::new(b"replay-synthetic")),
        )
        sent.get()
      }
      let consumer = SingleConsumer::create_with_memory(
        group,
        first,
        "persistent://public/default/keepalive-rebuild-consumer",
        "subscription",
        memory_limiter=limiter,
        reconnect_policy=policy,
        reconnect_with=() => {
          attempts[2] += 1
          @async.pause()
          second
        },
      )
      consumer.on_frame(
        lifecycle_delivery(consumer.consumer_id, incomplete=true),
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
      let heartbeat = group.spawn(
        () => first.keepalive_loop(100),
        no_wait=true,
        allow_failure=true,
      )
      heartbeat.wait()
      while producers.any(p => p.reconnecting.val) || consumer.reconnecting.val {
        @async.sleep(1)
      }
      assert_eq(attempts, [1, 1, 1])
      assert_eq(closes.val, 1)
      assert_eq(first.close_handlers.length(), 0)
      assert_eq(first.producer_handlers.length(), 0)
      assert_eq(first.message_handlers.length(), 0)
      assert_eq(first.pending.length(), 0)
      let originals = original.filter(f => f.cmd.send is Some(_))
      let replays = replacement.filter(f => f.cmd.send is Some(_))
      assert_eq(originals.length(), 2)
      assert_eq(replacement.filter(f => f.cmd.producer is Some(_)).length(), 2)
      assert_eq(replacement.filter(f => f.cmd.subscribe is Some(_)).length(), 1)
      if reject {
        assert_true(receiving.wait() is Err(ConnectionClosed))
        assert_eq(replays.length(), 0)
        assert_true(producers.all(p => p.closed) && consumer.closed.val)
      } else {
        assert_eq(replays.length(), 2)
        for original in originals {
          let send = original.cmd.send.unwrap()
          let matches = replays.filter(f => {
            f.cmd.send.unwrap().producer_id == send.producer_id
          })
          assert_eq(matches.length(), 1)
          let replay = matches[0]
          assert_eq(replay.cmd.send.unwrap().sequence_id, send.sequence_id)
          assert_eq(replay.payload, original.payload)
        }
        assert_true(producers.all(p => !p.closed) && !consumer.closed.val)
        assert_eq(second.close_handlers.length(), 3)
        assert_eq(second.producer_handlers.length(), 2)
        assert_eq(second.message_handlers.length(), 1)
        assert_true(receiving.try_wait() is None)
        let delivery = lifecycle_delivery(consumer.consumer_id)
        peer.write(
          encode_send_frame(
            delivery.cmd,
            delivery.metadata.unwrap(),
            delivery.payload.unwrap(),
          ),
        )
        assert_true(receiving.wait() is Ok(_))
      }
      for future in futures {
        let result : Result[SendReceipt, Error] = Ok(future.wait()) catch {
          e => Err(e)
        }
        if reject {
          assert_true(result is Err(ConnectionClosed))
        } else {
          assert_true(result is Ok(_))
        }
      }
      for producer in producers {
        producer.close()
        lifecycle_b_assert_drained(producer)
      }
      consumer.close()
      assert_eq(attempts, [1, 1, 1])
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
    })
  })
}

///|
async test "keepalive timeout rebuilds shared owners and replays each synthetic pending frame once" {
  keepalive_shared_rebuild(false)
}

///|
async test "keepalive timeout with rejected rebuild drains all shared owners without replay" {
  keepalive_shared_rebuild(true)
}
