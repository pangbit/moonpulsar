// Shared retained-byte budget. Async tasks run cooperatively: synchronous
// reserve/release calls below cannot interleave with the mutex-protected path.

///|
priv struct MemoryWaiter {
  token : UInt64
  ready : @aqueue.Queue[Unit]
}

///|
struct ClientMemoryLimiter {
  limit : Int
  mut used : Int
  mu : @async.Mutex
  mut waiters : Array[MemoryWaiter]
  mut next_token : UInt64
  mut closed : Bool
}

///|
fn ClientMemoryLimiter::new(limit : Int) -> ClientMemoryLimiter {
  {
    limit,
    used: 0,
    mu: @async.Mutex::Mutex(),
    waiters: [],
    next_token: 0UL,
    closed: false,
  }
}

///|
async fn ClientMemoryLimiter::acquire(
  self : ClientMemoryLimiter,
  size : Int,
  block : Bool,
) -> Unit {
  if size > self.limit {
    raise ClientMemoryFull
  }
  while true {
    self.mu.acquire()
    if self.closed {
      self.mu.release()
      raise ProducerClosed
    }
    if size <= self.limit - self.used {
      self.used += size
      self.mu.release()
      return
    }
    if !block {
      self.mu.release()
      raise ClientMemoryFull
    }
    self.next_token += 1UL
    let token = self.next_token
    let ready : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(
      kind=@aqueue.Kind::Blocking(1),
    )
    self.waiters.push({ token, ready, })
    self.mu.release()
    errdefer self.remove_waiter(token)
    ready.get()
  }
}

///|
fn ClientMemoryLimiter::close(self : ClientMemoryLimiter) -> Unit {
  self.closed = true
  for waiter in self.waiters {
    waiter.ready.close(error=ProducerClosed, clear=true)
  }
  self.waiters = []
}

///|
fn ClientMemoryLimiter::reserve_now(
  self : ClientMemoryLimiter,
  size : Int,
) -> Unit raise PulsarError {
  if self.closed {
    raise ProducerClosed
  }
  if size > self.limit - self.used {
    raise ClientMemoryFull
  }
  self.used += size
}

///|
fn ClientMemoryLimiter::release_now(
  self : ClientMemoryLimiter,
  size : Int,
) -> Unit {
  self.used -= size
  let waiters = self.waiters
  self.waiters = []
  for waiter in waiters {
    ignore(waiter.ready.try_put(()) catch { _ => false })
  }
}

///|
async fn ClientMemoryLimiter::remove_waiter(
  self : ClientMemoryLimiter,
  token : UInt64,
) -> Unit {
  self.mu.acquire()
  defer self.mu.release()
  for index in 0..<self.waiters.length() {
    if self.waiters[index].token == token {
      ignore(self.waiters.remove(index))
      return
    }
  }
}

///|
async fn ClientMemoryLimiter::release(
  self : ClientMemoryLimiter,
  size : Int,
) -> Unit {
  self.mu.acquire()
  self.used -= size
  let waiters = self.waiters
  self.waiters = []
  self.mu.release()
  for waiter in waiters {
    ignore(waiter.ready.try_put(()))
  }
}
