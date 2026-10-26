// Stage B fixtures use in-memory pipes; no sockets, DNS or external Broker.

///|
async test "producer close retires its memory admission without closing a sibling" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(32768)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      let sibling = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-sibling",
        memory_limiter=limiter,
        batching={ max_messages: 10, max_bytes: 40000, max_delay_ms: 60000, },
      )
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-admission",
        memory_limiter=limiter,
        max_pending_messages=1,
        block_if_queue_full=true,
      )
      let held = sibling.send_async(
        ProducerMessage::new(Bytes::make(16000, b'a')),
      )
      let blocked = group.spawn(
        () => {
          let result : Result[Unit, Error] = Ok(
            ignore(
              producer.send_async(
                ProducerMessage::new(Bytes::make(18000, b'b')),
              ),
            ),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      @async.with_timeout(1000, () => {
        while limiter.waiters.length() != 1 {
          @async.sleep(1)
        }
      })
      producer.close()
      let completed = @async.with_timeout_opt(200, () => blocked.wait())
      let remaining_waiters = limiter.waiters.length()
      let sibling_held = limiter.used
      if completed is None {
        lifecycle_wait_cancelled(blocked)
      }
      sibling.flush()
      ignore(held.wait())
      ignore(sibling.send_async(ProducerMessage::new(b"still-alive")))
      sibling.close()
      lifecycle_assert_connection_empty(conn, limiter)
      assert_eq(sibling_held, 16000)
      assert_true(
        completed is Some(Err(ProducerClosed)),
        msg="closed producer must wake its own memory admission",
      )
      assert_eq(remaining_waiters, 0)
    })
  })
}

///|
async test "send deadline ends a call blocked on the connection write lock" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-write-lock",
        memory_limiter=limiter,
        send_timeout_ms=50,
      )
      conn.write_mu.acquire()
      let blocked = group.spawn(
        () => {
          let result : Result[SendReceipt, Error] = Ok(
            producer.send(ProducerMessage::new(b"must-not-write-after-expiry")),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      @async.with_timeout(1000, () => {
        while producer.sends.is_empty() {
          @async.sleep(1)
        }
        while !producer.sends.is_empty() {
          @async.sleep(1)
        }
      })
      let completed = @async.with_timeout_opt(200, () => blocked.wait())
      assert_true(!conn.is_closed())
      conn.write_mu.release()
      let result = @async.with_timeout(1000, () => blocked.wait())
      assert_true(result is Err(RequestTimeout))
      ignore(producer.send(ProducerMessage::new(b"next-send")))
      producer.close()
      assert_producer_drained(producer)
      lifecycle_assert_connection_empty(conn, limiter)
      assert_true(
        completed is Some(Err(RequestTimeout)),
        msg="deadline must end the send call before the write lock is released",
      )
    })
  })
}

///|
suberror LifecycleTransportError {
  ClosedTransport
} derive(Debug)

///|
struct LifecycleBWriter {
  inner : &@io.Writer
  transport_closed : Ref[Bool]?
  started : @aqueue.Queue[Unit]
  unblock : @aqueue.Queue[Unit]
  mut active : Int
  mut prefix_bytes : Int
  mut armed : Bool
  mut injected_errors : Int
  mut last_failed_offset : Int?
}

///|
impl @io.Writer for LifecycleBWriter with fn write_once(
  self,
  bytes,
  offset~,
  len~,
) {
  // PipeWrite asserts after its own write end is closed. A closed transport
  // instead fails the next write with an I/O error, just as Tcp's invalid-fd
  // path does. Preserve the accepted prefix and Writer::write's offset loop.
  if self.transport_closed is Some(state) && state.val {
    self.injected_errors += 1
    self.last_failed_offset = Some(offset)
    raise LifecycleTransportError::ClosedTransport
  }
  if self.armed &&
    offset == 0 &&
    decode_frame_body(bytes[4:].to_owned()).cmd.send is Some(_) {
    self.active += 1
    defer {
      self.active -= 1
    }
    // A real byte crosses the pipe before we block, modelling a partial frame.
    let count = self.inner.write_once(bytes, offset~, len=1)
    self.prefix_bytes += count
    self.started.put(())
    self.unblock.get()
    count
  } else {
    self.inner.write_once(bytes, offset~, len~)
  }
}

///|
fn LifecycleBWriter::new(
  inner : &@io.Writer,
  transport_closed? : Ref[Bool],
) -> LifecycleBWriter {
  {
    inner,
    transport_closed,
    started: @aqueue.Queue::Queue(kind=Blocking(1)),
    unblock: @aqueue.Queue::Queue(kind=Blocking(1)),
    active: 0,
    prefix_bytes: 0,
    armed: true,
    injected_errors: 0,
    last_failed_offset: None,
  }
}

///|
async test "a blocked batch writer stays live until its last message expires" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let (conn, _) = lifecycle_memory_connection(group, limiter, wrap_writer=inner => {
        let gated = LifecycleBWriter::new(inner)
        writer.val = Some(gated)
        gated
      })
      defer conn.close()
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-partial-batch",
        memory_limiter=limiter,
        send_timeout_ms=300,
        batching={ max_messages: 2, max_bytes: 4096, max_delay_ms: 60000, },
      )
      let first = producer.send_async(ProducerMessage::new(b"older"))
      @async.sleep(150)
      let second = group.spawn(
        () => {
          let result : Result[SendReceipt, Error] = Ok(
            producer.send(ProducerMessage::new(b"younger")),
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
      let seq = producer.pending.keys().collect()[0]
      let frame = producer.pending.get(seq).unwrap()
      let result : Result[SendReceipt, Error] = Ok(first.wait()) catch {
        e => Err(e)
      }
      assert_true(result is Err(RequestTimeout))
      assert_eq(frame.remaining, 1)
      assert_eq(gated.active, 1)
      assert_true(!conn.is_closed())
      assert_true(second.wait() is Err(RequestTimeout))
      assert_eq(frame.remaining, 0)
      assert_true(frame.write_task.val is None)
      assert_eq(gated.active, 0)
      assert_eq(gated.prefix_bytes, 1)
      producer.close()
      lifecycle_b_assert_drained(producer)
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
fn lifecycle_b_late_receipt(producer : SingleProducer, seq : UInt64) -> Unit {
  let command = @proto.BaseCommand::default()
  command.type_ = @proto.BaseCommand_Type::SEND_RECEIPT
  let receipt = @proto.CommandSendReceipt::default()
  receipt.producer_id = producer.producer_id
  receipt.sequence_id = seq
  command.send_receipt = Some(receipt)
  producer.on_command(command)
}

///|
fn lifecycle_b_assert_drained(producer : SingleProducer) -> Unit raise {
  assert_producer_drained(producer)
  assert_eq(producer.memory_admission.waiters.length(), 0)
}

///|
async test "cancelled memory admission releases its slot without starting a deadline" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(32768)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      let sibling = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-memory-holder",
        memory_limiter=limiter,
        batching={ max_messages: 10, max_bytes: 40000, max_delay_ms: 60000, },
      )
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-cancel-admission",
        memory_limiter=limiter,
        max_pending_messages=1,
        block_if_queue_full=true,
        send_timeout_ms=30,
      )
      let held = sibling.send_async(
        ProducerMessage::new(Bytes::make(16000, b'a')),
      )
      let blocked = group.spawn(
        () => {
          ignore(producer.send(ProducerMessage::new(Bytes::make(18000, b'b'))))
        },
        no_wait=true,
        allow_failure=true,
      )
      while limiter.waiters.length() != 1 {
        @async.sleep(1)
      }
      @async.sleep(60)
      assert_eq(producer.sends.length(), 0)
      assert_eq(producer.deadlines.heap.length(), 0)
      assert_eq(producer.memory_admission.waiters.length(), 1)
      lifecycle_wait_cancelled(blocked)
      lifecycle_b_assert_drained(producer)
      assert_eq(limiter.waiters.length(), 0)
      assert_eq(limiter.used, 16000)
      // max_pending_messages=1: this also proves the cancelled admission returned its slot.
      ignore(producer.send(ProducerMessage::new(b"slot-returned")))
      producer.close()
      sibling.flush()
      ignore(held.wait())
      sibling.close()
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async test "caller cancellation before the write lock joins the writer and ignores late receipts" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let (conn, _) = lifecycle_memory_connection(group, limiter)
      defer conn.close()
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-cancel-write",
        memory_limiter=limiter,
        max_pending_messages=1,
        send_timeout_ms=1000,
      )
      conn.write_mu.acquire()
      let blocked = group.spawn(
        () => {
          ignore(producer.send(ProducerMessage::new(b"cancel-before-write")))
        },
        no_wait=true,
        allow_failure=true,
      )
      while producer.pending.is_empty() {
        @async.sleep(1)
      }
      let seq = producer.pending.keys().collect()[0]
      let frame = producer.pending.get(seq).unwrap()
      let send = frame.entries[0].0
      lifecycle_wait_cancelled(blocked)
      assert_true(frame.write_task.val is None)
      assert_true(send.done)
      lifecycle_b_assert_drained(producer)
      lifecycle_b_late_receipt(producer, seq)
      producer.finish_send(
        send,
        Ok({
          message_id: MessageId::latest(),
          sequence_id: seq,
          producer_id: producer.producer_id,
          producer_name: producer.name,
        }),
      )
      let terminal : Result[SendReceipt, Error] = Ok(send.waiter.get()) catch {
        e => Err(e)
      }
      assert_true(terminal is Err(ConnectionClosed))
      assert_true(producer.last_confirmed_sequence_id is None)
      assert_eq(limiter.used, 0)
      assert_true(!conn.is_closed())
      conn.write_mu.release()
      ignore(producer.send(ProducerMessage::new(b"still-usable")))
      producer.close()
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async test "deadline during a partial write joins the writer and retires only its transport" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let closes : Ref[Int] = { val: 0, }
      let (conn, _) = lifecycle_memory_connection(
        group,
        limiter,
        close_calls=closes,
        wrap_writer=inner => {
          let gated = LifecycleBWriter::new(inner)
          writer.val = Some(gated)
          gated
        },
      )
      defer conn.close()
      let (healthy, _) = lifecycle_memory_connection(group, limiter)
      defer healthy.close()
      let other = SingleProducer::create_with_memory(
        group,
        healthy,
        "persistent://public/default/b-unrelated",
        memory_limiter=limiter,
      )
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-partial-write",
        memory_limiter=limiter,
        send_timeout_ms=80,
      )
      let blocked = group.spawn(
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
      let seq = producer.pending.keys().collect()[0]
      let frame = producer.pending.get(seq).unwrap()
      let send = frame.entries[0].0
      assert_true(blocked.wait() is Err(RequestTimeout))
      assert_true(frame.write_task.val is None)
      assert_eq(gated.active, 0)
      assert_eq(gated.prefix_bytes, 1)
      assert_true(conn.is_closed())
      assert_eq(closes.val, 1)
      lifecycle_b_late_receipt(producer, seq)
      producer.finish_send(send, Err(ProducerClosed))
      let terminal : Result[SendReceipt, Error] = Ok(send.waiter.get()) catch {
        e => Err(e)
      }
      assert_true(terminal is Err(RequestTimeout))
      assert_true(producer.last_confirmed_sequence_id is None)
      lifecycle_b_assert_drained(producer)
      ignore(other.send(ProducerMessage::new(b"unrelated-connection-alive")))
      other.close()
      producer.close()
      lifecycle_assert_connection_empty(conn, limiter)
      lifecycle_assert_connection_empty(healthy, limiter)
    })
  })
}

///|
async test "close keeps a buffered send deadline alive while flushing a blocked writer" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let (conn, _) = lifecycle_memory_connection(group, limiter, wrap_writer=inner => {
        let gated = LifecycleBWriter::new(inner)
        writer.val = Some(gated)
        gated
      })
      defer conn.close()
      let producer = SingleProducer::create_with_memory(
        group,
        conn,
        "persistent://public/default/b-close-flush",
        memory_limiter=limiter,
        send_timeout_ms=100,
        batching={ max_messages: 10, max_bytes: 4096, max_delay_ms: 60000, },
      )
      let future = producer.send_async(ProducerMessage::new(b"buffered"))
      let entry = producer.batch_entries[0]
      let deadline = entry.deadline
      let closing = group.spawn(
        () => producer.close(),
        no_wait=true,
        allow_failure=true,
      )
      writer.val.unwrap().started.get()
      assert_eq(entry.deadline, deadline)
      closing.wait()
      let result : Result[SendReceipt, Error] = Ok(future.wait()) catch {
        e => Err(e)
      }
      assert_true(result is Err(RequestTimeout))
      assert_eq(writer.val.unwrap().active, 0)
      assert_true(producer.deadline_worker is None)
      lifecycle_b_assert_drained(producer)
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async test "blocked replay inherits the original deadline and cannot accept a late receipt" {
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
      let writer : Ref[LifecycleBWriter?] = { val: None, }
      let (second, _) = lifecycle_memory_connection(group, limiter, wrap_writer=inner => {
        let gated = LifecycleBWriter::new(inner)
        writer.val = Some(gated)
        gated
      })
      defer second.close()
      let producer = SingleProducer::create_with_memory(
        group,
        first,
        "persistent://public/default/b-replay",
        memory_limiter=limiter,
        send_timeout_ms=300,
        reconnect_with=async fn() {
          @async.pause()
          second
        },
        reconnect_policy={ ..RetryPolicy::default(), max_retries: 0, },
      )
      let future = producer.send_async(ProducerMessage::new(b"replay-once"))
      received.get()
      let seq = producer.pending.keys().collect()[0]
      let frame = producer.pending.get(seq).unwrap()
      let entry = frame.entries[0].0
      let deadline = entry.deadline
      @async.sleep(50)
      first.close()
      writer.val.unwrap().started.get()
      assert_eq(entry.deadline, deadline)
      let result : Result[SendReceipt, Error] = Ok(future.wait()) catch {
        e => Err(e)
      }
      assert_true(result is Err(RequestTimeout))
      while producer.reconnecting.val {
        @async.sleep(1)
      }
      assert_true(frame.write_task.val is None)
      assert_eq(writer.val.unwrap().active, 0)
      lifecycle_b_late_receipt(producer, seq)
      assert_true(producer.last_confirmed_sequence_id is None)
      lifecycle_b_assert_drained(producer)
      producer.close()
      lifecycle_assert_connection_empty(first, limiter)
      lifecycle_assert_connection_empty(second, limiter)
    })
  })
}
