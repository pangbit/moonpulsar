///|
// Indexed heap: completed sends are removed immediately, not left as tombstones.
priv struct SendDeadlines {
  heap : Array[(UInt64, Int64)]
  positions : Map[UInt64, Int]
  clock : @bench.Timestamp
}

///|
fn SendDeadlines::new() -> SendDeadlines {
  { heap: [], positions: {}, clock: @bench.monotonic_clock_start(), }
}

///|
fn SendDeadlines::now(self : SendDeadlines) -> Int64 {
  (@bench.monotonic_clock_end(self.clock) / 1000.0).to_int64()
}

///|
fn SendDeadlines::swap(self : SendDeadlines, a : Int, b : Int) -> Unit {
  let value = self.heap[a]
  self.heap[a] = self.heap[b]
  self.heap[b] = value
  self.positions[self.heap[a].0] = a
  self.positions[self.heap[b].0] = b
}

///|
fn SendDeadlines::up(self : SendDeadlines, start : Int) -> Unit {
  let mut i = start
  while i > 0 {
    let parent = (i - 1) / 2
    if self.heap[parent].1 <= self.heap[i].1 {
      break
    }
    self.swap(parent, i)
    i = parent
  }
}

///|
fn SendDeadlines::add(
  self : SendDeadlines,
  id : UInt64,
  deadline : Int64,
) -> Unit {
  self.positions[id] = self.heap.length()
  self.heap.push((id, deadline))
  self.up(self.heap.length() - 1)
}

///|
fn SendDeadlines::remove(self : SendDeadlines, id : UInt64) -> Unit {
  guard self.positions.get(id) is Some(index) else { return }
  self.positions.remove(id)
  let last = self.heap.pop().unwrap()
  if index == self.heap.length() {
    return
  }
  self.heap[index] = last
  self.positions[last.0] = index
  if index > 0 && self.heap[index].1 < self.heap[(index - 1) / 2].1 {
    self.up(index)
    return
  }
  let mut i = index
  while i * 2 + 1 < self.heap.length() {
    let left = i * 2 + 1
    let right = left + 1
    let child = if right < self.heap.length() &&
      self.heap[right].1 < self.heap[left].1 {
      right
    } else {
      left
    }
    if self.heap[i].1 <= self.heap[child].1 {
      break
    }
    self.swap(i, child)
    i = child
  }
}

///|
fn SingleProducer::wake_deadlines(self : SingleProducer) -> Unit {
  ignore(self.deadline_changed.try_put(()) catch { _ => false })
}

///|
async fn SingleProducer::run_deadlines(self : SingleProducer) -> Unit {
  while !self.closed {
    // Coalesce changes; no await between reading the heap and choosing a wait.
    ignore(self.deadline_changed.try_get())
    if self.deadlines.heap.get(0) is Some((id, deadline)) {
      let remaining = deadline - self.deadlines.now()
      if remaining <= 0L {
        if self.sends.get(id) is Some(send) {
          self.finish_send(send, Err(RequestTimeout))
        }
        continue
      }
      @async.with_timeout(remaining.min(2147483647L).to_int(), async fn() {
        self.deadline_changed.get()
      }) catch {
        @async.TimeoutError => ()
        e => raise e
      }
    } else {
      self.deadline_changed.get()
    }
  }
}

///|
async fn SingleProducer::admit_send(
  self : SingleProducer,
  msg : ProducerMessage,
) -> PendingSend {
  if self.closed {
    raise ProducerClosed
  }
  if self.pending_slots is Some(slots) {
    if self.block_if_queue_full {
      slots.get() catch {
        _ => raise ProducerClosed
      }
    } else if slots.try_get() is None {
      raise ProducerQueueFull
    }
  }
  errdefer (if self.pending_slots is Some(slots) {
    ignore(slots.try_put(()) catch { _ => false })
  })
  let size = msg.retained_bytes()
  if self.memory_limiter is Some(limiter) {
    limiter.acquire(size, self.block_if_queue_full)
  }
  errdefer (if self.memory_limiter is Some(limiter) {
    limiter.release_now(size)
  })
  if self.closed {
    raise ProducerClosed
  }
  self.next_send_token += 1UL
  let send : PendingSend = {
    message: Some(msg),
    waiter: @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1)),
    token: self.next_send_token,
    size,
    deadline: self.send_timeout_ms.map(ms => {
      self.deadlines.now() + ms.to_int64()
    }),
    done: false,
    frame: None,
    worker: None,
  }
  self.sends[send.token] = send
  if send.deadline is Some(deadline) {
    self.deadlines.add(send.token, deadline)
    self.wake_deadlines()
  }
  send
}

///|
// All terminal transitions are synchronous: receipt/timeout/close can win once.
fn SingleProducer::finish_send(
  self : SingleProducer,
  send : PendingSend,
  result : Result[SendReceipt, Error],
) -> Unit {
  if send.done {
    return
  }
  send.done = true
  self.sends.remove(send.token)
  self.deadlines.remove(send.token)
  if send.deadline is Some(_) {
    self.wake_deadlines()
  }
  for i in 0..<self.batch_entries.length() {
    if self.batch_entries[i].token == send.token {
      self.batch_bytes -= send.message.unwrap().payload.length()
      ignore(self.batch_entries.remove(i))
      break
    }
  }
  if self.batch_entries.is_empty() {
    self.stop_batch_timer()
  }
  send.message = None
  if send.frame is Some(seq) && self.pending.get(seq) is Some(frame) {
    frame.remaining -= 1
    if frame.remaining == 0 {
      self.pending.remove(seq)
      self.payloads.remove(seq)
      self.release_frame_charge(seq)
      if frame.chunk_waiter is Some(waiter) {
        match result {
          Err(e) => waiter.close(error=e, clear=true)
          Ok(receipt) => ignore(waiter.try_put(receipt) catch { _ => false })
        }
      }
    }
  }
  send.frame = None
  if self.memory_limiter is Some(limiter) {
    limiter.release_now(send.size)
  }
  if self.pending_slots is Some(slots) {
    ignore(slots.try_put(()) catch { _ => false })
  }
  let worker = send.worker
  send.worker = None
  match result {
    Ok(receipt) => ignore(send.waiter.try_put(receipt) catch { _ => false })
    Err(e) => {
      send.waiter.close(error=e, clear=true)
      if worker is Some(task) {
        task.cancel()
      }
    }
  }
}

///|
fn SingleProducer::stop_batch_timer(self : SingleProducer) -> Unit {
  if self.batch_timer is Some(task) {
    task.cancel()
  }
  self.batch_timer = None
  self.batch_timer_running = false
  self.batch_timer_generation += 1UL
}

///|
fn SingleProducer::finish_frame(
  self : SingleProducer,
  seq : UInt64,
  result : Result[SendReceipt, Error],
) -> Unit {
  guard self.pending.get(seq) is Some(frame) else { return }
  self.pending.remove(seq)
  self.payloads.remove(seq)
  self.release_frame_charge(seq)
  for (send, _) in frame.entries {
    send.frame = None
  }
  if frame.chunk_waiter is Some(waiter) {
    match result {
      Ok(receipt) => ignore(waiter.try_put(receipt) catch { _ => false })
      Err(e) => {
        waiter.close(error=e, clear=true)
        for (send, _) in frame.entries {
          self.finish_send(send, Err(e))
        }
      }
    }
    return
  }
  for (send, index) in frame.entries {
    let per_message = match result {
      Ok(receipt) =>
        Ok({
          ..receipt,
          message_id: {
            ..receipt.message_id,
            batch_index: if index >= 0 {
              index
            } else {
              receipt.message_id.batch_index
            },
          },
        })
      Err(e) => Err(e)
    }
    self.finish_send(send, per_message)
  }
}
