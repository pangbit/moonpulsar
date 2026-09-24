// Interceptors transform a logical send before routing and observe completion.

///|
async test "producer interceptors run in order before routing and after receipt" {
  @async.with_task_group(async fn(group) {
    let unused = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
    })
    let selected = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) && frame.payload is Some(payload) else {
        fail("expected intercepted SEND")
      }
      assert_eq(payload, b"one-ab")
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let first = Producer::single(
      SingleProducer::create(
        group,
        manager.get(unused),
        "persistent://public/default/interceptor-partition-0",
      ),
    )
    let second = Producer::single(
      SingleProducer::create(
        group,
        manager.get(selected),
        "persistent://public/default/interceptor-partition-1",
      ),
    )
    let producer = Producer::partitioned(
      [first, second],
      routing=Custom(fn(message, _count) {
        if message.key == Some("reroute") {
          1
        } else {
          0
        }
      }),
    )
    let events : Array[String] = []
    let before_second_payload : Ref[Bytes] = { val: b"", }
    let after_payload : Ref[Bytes] = { val: b"", }
    let results : Array[Bool] = []
    producer.set_interceptors([
      ProducerInterceptor::new(
        before_send=fn(_message) {
          events.push("before-1")
          ProducerMessage::new(b"one-a", key="reroute")
        },
        on_send_result=fn(message, result) {
          after_payload.val = message.payload
          results.push(result is Ok(_))
          events.push("after-1")
        },
      ),
      ProducerInterceptor::new(
        before_send=fn(message) {
          before_second_payload.val = message.payload
          events.push("before-2")
          ProducerMessage::new(b"one-ab", key="reroute")
        },
        on_send_result=fn(_message, result) {
          results.push(result is Ok(_))
          events.push("after-2")
        },
      ),
    ])
    ignore(producer.send(ProducerMessage::new(b"one")))
    assert_eq(events, ["before-1", "before-2", "after-1", "after-2"])
    assert_eq(before_second_payload.val, b"one-a")
    assert_eq(after_payload.val, b"one-ab")
    assert_eq(results, [true, true])
    producer.close()
    manager.close()
  })
}

///|
async test "producer interceptor observes broker send failure" {
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      let reply = @proto.BaseCommand::default()
      reply.type_ = @proto.BaseCommand_Type::SEND_ERROR
      let error = @proto.CommandSendError::default()
      error.producer_id = send.producer_id
      error.sequence_id = send.sequence_id
      error.error = @proto.ServerError::PersistenceError
      error.message = "failed write"
      reply.send_error = Some(error)
      peer.send_command(reply)
    })
    let manager = ConnectionManager::new(group)
    let producer = Producer::single(
      SingleProducer::create(
        group,
        manager.get(addr),
        "persistent://public/default/interceptor-error",
      ),
    )
    let observed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    producer.set_interceptors([
      ProducerInterceptor::new(on_send_result=fn(_message, result) {
        ignore(
          observed.try_put(
            result is Err(ServerError(message="failed write", ..)),
          ) catch {
            _ => false
          },
        )
      }),
    ])
    let failed : Result[SendReceipt, Error] = Ok(
      producer.send(ProducerMessage::new(b"one")),
    ) catch {
      e => Err(e)
    }
    assert_true(failed is Err(ServerError(message="failed write", ..)))
    assert_true(observed.get())
    producer.close()
    manager.close()
  })
}

///|
async test "producer interceptor sees queue rejection and async receipt without wait" {
  @async.with_task_group(async fn(group) {
    let release : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    let addr = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) else { fail("expected SEND") }
      release.get()
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let manager = ConnectionManager::new(group)
    let producer = Producer::single(
      SingleProducer::create(
        group,
        manager.get(addr),
        "persistent://public/default/interceptor-full",
        max_pending_messages=1,
      ),
    )
    let starts : Array[Int64] = []
    let finishes : Map[Int64, Bool] = Map([])
    producer.set_span_recorder(
      Some(
        ClientSpanRecorder::new(fn(event) {
          match event {
            Started(span) => starts.push(span.id)
            Finished(span, _, success, _) => finishes[span.id] = success
          }
        }),
      ),
    )
    let observed : @aqueue.Queue[Bool] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(2),
    )
    producer.set_interceptors([
      ProducerInterceptor::new(on_send_result=fn(_message, result) {
        ignore(observed.try_put(result is Ok(_)) catch { _ => false })
      }),
    ])
    let pending = producer.send_async(ProducerMessage::new(b"one"))
    let rejected : Result[ReceiptFuture, Error] = Ok(
      producer.send_async(ProducerMessage::new(b"two")),
    ) catch {
      e => Err(e)
    }
    assert_true(rejected is Err(ProducerQueueFull))
    assert_true(!observed.get())
    release.put(())
    assert_true(@async.with_timeout(1000, async fn() { observed.get() }))
    ignore(pending.wait())
    assert_eq(starts.length(), 2)
    assert_eq(finishes.get(starts[0]), Some(true))
    assert_eq(finishes.get(starts[1]), Some(false))
    producer.close()
    manager.close()
  })
}

///|
async test "producer interceptor runs once across unconfirmed send replay" {
  @async.with_task_group(async fn(group) {
    let recovered = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(send) && frame.payload is Some(payload) else {
        fail("expected replayed SEND")
      }
      assert_eq(payload, b"intercepted")
      send_receipt(peer, send.producer_id, send.sequence_id)
    })
    let disconnected = start_mock_broker(group, async fn(_addr, peer) {
      expect_producer(peer)
      let frame = peer.next()
      guard frame.cmd.send is Some(_) && frame.payload is Some(payload) else {
        fail("expected initial SEND")
      }
      assert_eq(payload, b"intercepted")
      peer.tcp.close()
    })
    let lookup = start_mock_broker(group, async fn(_addr, peer) {
      two_stage_lookup(peer, disconnected, recovered)
    })
    let client = Client::connect(group, "pulsar://\{lookup}")
    let before_count : Ref[Int] = { val: 0, }
    let after_count : Ref[Int] = { val: 0, }
    let producer = client.create_producer(
      "persistent://public/default/interceptor-reconnect",
      interceptors=[
        ProducerInterceptor::new(
          before_send=fn(_message) {
            before_count.val += 1
            ProducerMessage::new(b"intercepted")
          },
          on_send_result=fn(_message, result) {
            if result is Ok(_) {
              after_count.val += 1
            }
          },
        ),
      ],
    )
    ignore(
      @async.with_timeout(10000, async fn() {
        producer.send(ProducerMessage::new(b"original"))
      }),
    )
    assert_eq(before_count.val, 1)
    assert_eq(after_count.val, 1)
    producer.close()
    client.close()
  })
}
