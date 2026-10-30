// Keepalive regressions use in-memory pipes and synthetic protocol commands.
// No sockets, DNS, external brokers or real messages are involved.

///|
async test "keepalive silent peer closes once and fails a pending request" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let closes : Ref[Int32] = { val: 0, }
      let ping_count : Ref[Int32] = { val: 0, }
      let pings : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let requested : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
        kind=Blocking(1),
      )
      let stopped : @aqueue.Queue[Error?] = @aqueue.Queue::Queue(kind=Unbounded)
      let (conn, _) = lifecycle_memory_connection(
        group,
        limiter,
        close_calls=closes,
        blocked=("create", requested),
        observe=frame => {
          if frame.cmd.type_ is @proto.BaseCommand_Type::PING {
            ping_count.val += 1
            ignore(pings.try_put(()) catch { _ => false })
          }
        },
      )
      defer conn.close()
      conn.on_close(error => ignore(stopped.try_put(error) catch { _ => false }))
      let cmd = @proto.BaseCommand::default()
      cmd.type_ = @proto.BaseCommand_Type::PRODUCER
      let request = @proto.CommandProducer::default()
      request.request_id = conn.next_request_id()
      request.topic = "persistent://public/default/synthetic-keepalive"
      request.producer_id = 1UL
      cmd.producer = Some(request)
      let waiting = group.spawn(
        () => {
          let result : Result[@proto.BaseCommand, Error] = Ok(
            conn.request(cmd, timeout_ms=4000),
          ) catch {
            e => Err(e)
          }
          result
        },
        no_wait=true,
        allow_failure=true,
      )
      requested.get()
      let heartbeat = group.spawn(
        () => conn.keepalive_loop(100),
        no_wait=true,
        allow_failure=true,
      )
      pings.get()
      assert_eq(conn.pending.length(), 1)
      let closed = @async.with_timeout_opt(450, () => stopped.get())
      guard closed is Some(Some(ConnectionClosed)) else {
        fail(
          "silent peer: PING count=\{ping_count.val}, closed=\{conn.is_closed()}, pending=\{conn.pending.length()}, transport closes=\{closes.val}",
        )
      }
      assert_true(conn.is_closed())
      assert_true(waiting.wait() is Err(ConnectionClosed))
      heartbeat.wait()
      assert_true(pings.try_get() is None)
      assert_eq(closes.val, 1)
      conn.close()
      assert_eq(closes.val, 1)
      assert_true(stopped.try_get() is None)
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async test "keepalive PONG and other inbound commands satisfy successive probes" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let pings : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let replies : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let (conn, peer) = lifecycle_memory_connection(group, limiter, observe=frame => {
        if frame.cmd.type_ is @proto.BaseCommand_Type::PING {
          ignore(pings.try_put(()) catch { _ => false })
        } else if frame.cmd.type_ is @proto.BaseCommand_Type::PONG {
          ignore(replies.try_put(()) catch { _ => false })
        }
      })
      defer conn.close()
      let heartbeat = group.spawn(
        () => conn.keepalive_loop(100),
        no_wait=true,
        allow_failure=true,
      )
      for kind in [@proto.BaseCommand_Type::PONG, SUCCESS, PING] {
        pings.get()
        let cmd = @proto.BaseCommand::default()
        cmd.type_ = kind
        match kind {
          PONG => cmd.pong = Some(@proto.CommandPong::default())
          SUCCESS => {
            let success = @proto.CommandSuccess::default()
            success.request_id = 999UL
            cmd.success = Some(success)
          }
          PING => cmd.ping = Some(@proto.CommandPing::default())
          _ => fail("unsupported synthetic command")
        }
        peer.write(encode_command_frame(cmd))
        while conn.waiting_for_ping_response {
          @async.pause()
        }
        assert_false(conn.is_closed())
      }
      replies.get()
      pings.get()
      // Last probe receives no reply: earlier activity must not mask it.
      @async.with_timeout(500, () => heartbeat.wait())
      assert_true(conn.is_closed())
      assert_true(pings.try_get() is None)
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async test "keepalive close interrupts a long timer and cancellation retires transport" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      for cancel in [false, true] {
        let limiter = ClientMemoryLimiter::new(65536)
        let closes : Ref[Int32] = { val: 0, }
        let frames : Array[IncomingFrame] = []
        let (conn, _) = lifecycle_memory_connection(
          group,
          limiter,
          close_calls=closes,
          observe=frame => frames.push(frame),
        )
        defer conn.close()
        let heartbeat = group.spawn(
          () => conn.keepalive_loop(2000),
          no_wait=true,
          allow_failure=true,
        )
        @async.pause()
        if cancel {
          lifecycle_wait_cancelled(heartbeat)
        } else {
          conn.close()
          @async.with_timeout(500, () => heartbeat.wait())
        }
        assert_true(conn.is_closed())
        assert_eq(closes.val, 1)
        assert_eq(frames.length(), 0)
        lifecycle_assert_connection_empty(conn, limiter)
      }
    })
  })
}

///|
struct KeepaliveBlockedWriter {
  inner : &@io.Writer
  started : @aqueue.Queue[Unit]
  unblock : @aqueue.Queue[Unit]
  mut active : Int32
  mut prefix_bytes : Int32
}

///|
impl @io.Writer for KeepaliveBlockedWriter with fn write_once(
  self,
  bytes,
  offset~,
  len~,
) {
  if offset == 0 &&
    decode_frame_body(bytes[4:].to_owned()).cmd.type_
    is @proto.BaseCommand_Type::PING {
    self.active += 1
    defer {
      self.active -= 1
    }
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
async test "keepalive bounds partial PING writes and close cancels the writer" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      // Timeout, explicit close, and task cancellation while a byte is written.
      for action in 0..<3 {
        let limiter = ClientMemoryLimiter::new(65536)
        let closes : Ref[Int32] = { val: 0, }
        let writer : Ref[KeepaliveBlockedWriter?] = { val: None, }
        let (conn, _) = lifecycle_memory_connection(
          group,
          limiter,
          close_calls=closes,
          wrap_writer=inner => {
            let gated : KeepaliveBlockedWriter = {
              inner,
              started: @aqueue.Queue::Queue(kind=Blocking(1)),
              unblock: @aqueue.Queue::Queue(kind=Blocking(1)),
              active: 0,
              prefix_bytes: 0,
            }
            writer.val = Some(gated)
            gated
          },
        )
        defer conn.close()
        let heartbeat = group.spawn(
          () => conn.keepalive_loop(if action == 0 { 100 } else { 1000 }),
          no_wait=true,
          allow_failure=true,
        )
        let gated = writer.val.unwrap()
        gated.started.get()
        assert_eq(gated.active, 1)
        if action == 1 {
          conn.close()
        }
        if action == 2 {
          lifecycle_wait_cancelled(heartbeat)
        } else {
          @async.with_timeout(500, () => heartbeat.wait())
        }
        assert_eq(gated.prefix_bytes, 1)
        assert_eq(gated.active, 0)
        assert_true(conn.is_closed())
        assert_eq(closes.val, 1)
        assert_true(conn.write_mu.try_acquire())
        conn.write_mu.release()
        lifecycle_assert_connection_empty(conn, limiter)
      }
    })
  })
}

///|
async test "keepalive bounds write lock waits and close interrupts lock admission" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      for explicit_close in [false, true] {
        let limiter = ClientMemoryLimiter::new(65536)
        let closes : Ref[Int32] = { val: 0, }
        let frames : Array[IncomingFrame] = []
        let (conn, _) = lifecycle_memory_connection(
          group,
          limiter,
          close_calls=closes,
          observe=frame => frames.push(frame),
        )
        defer conn.close()
        conn.write_mu.acquire()
        let heartbeat = group.spawn(
          () => conn.keepalive_loop(if explicit_close { 1000 } else { 100 }),
          no_wait=true,
          allow_failure=true,
        )
        while !conn.waiting_for_ping_response {
          @async.sleep(1)
        }
        if explicit_close {
          conn.close()
        }
        @async.with_timeout(500, () => heartbeat.wait())
        assert_true(conn.is_closed())
        assert_eq(closes.val, 1)
        assert_eq(frames.length(), 0)
        // Cancellation before writing must not release another task's lock.
        assert_false(conn.write_mu.try_acquire())
        conn.write_mu.release()
        assert_true(conn.write_mu.try_acquire())
        conn.write_mu.release()
        lifecycle_assert_connection_empty(conn, limiter)
      }
    })
  })
}

///|
async test "keepalive skips probes for a peer predating protocol v1" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let frames : Array[IncomingFrame] = []
      let (conn, _) = lifecycle_memory_connection(group, limiter, observe=frame => {
        frames.push(frame)
      })
      defer conn.close()
      conn.remote_protocol_version = 0
      let heartbeat = group.spawn(
        () => conn.keepalive_loop(20),
        no_wait=true,
        allow_failure=true,
      )
      @async.sleep(100)
      assert_false(conn.is_closed())
      assert_eq(frames.length(), 0)
      conn.close()
      heartbeat.wait()
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}

///|
async test "keepalive partial inbound PONG does not prove liveness or retain frame budget" {
  @async.with_timeout(5000, () => {
    @async.with_task_group(group => {
      let limiter = ClientMemoryLimiter::new(65536)
      let pings : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
      let (conn, peer) = lifecycle_memory_connection(group, limiter, observe=frame => {
        if frame.cmd.ping is Some(_) {
          ignore(pings.try_put(()) catch { _ => false })
        }
      })
      defer conn.close()
      let heartbeat = group.spawn(
        () => conn.keepalive_loop(100),
        no_wait=true,
        allow_failure=true,
      )
      pings.get()
      let pong = @proto.BaseCommand::default()
      pong.type_ = @proto.BaseCommand_Type::PONG
      pong.pong = Some(@proto.CommandPong::default())
      let bytes = encode_command_frame(pong)
      // A declared frame and one body byte reach Reader, but cannot be decoded.
      peer.write(bytes[:5].to_owned())
      assert_true(limiter.used > 0)
      assert_true(conn.waiting_for_ping_response)
      heartbeat.wait()
      assert_true(conn.is_closed())
      while limiter.used > 0 {
        @async.pause()
      }
      lifecycle_assert_connection_empty(conn, limiter)
    })
  })
}
