// Optional consumer ACK buffering. Confirmed and transactional ACKs flush the
// buffer and go directly to the broker.

///|
pub struct AckGroupingOptions {
  priv max_size : Int
  priv max_time_ms : Int
}

///|
pub fn AckGroupingOptions::new(
  max_size? : Int = 1000,
  max_time_ms? : Int = 100,
) -> AckGroupingOptions raise PulsarError {
  guard max_size >= 0 && max_time_ms >= 0 else {
    raise InvalidFrame("ACK grouping size and time must not be negative")
  }
  { max_size, max_time_ms, }
}

///|
priv struct PendingAck {
  ack_type : @proto.CommandAck_AckType
  ids : Array[MessageId]
  batch_ack_set : Array[Int64]?
  batch_size : Int?
}

///|
struct AckDispatcher {
  conn_ref : Ref[Connection]
  mut consumer_id : UInt64
  options : AckGroupingOptions?
  mut pending : Array[PendingAck]
  mu : @async.Mutex
  stop : @aqueue.Queue[Unit]
  mut closed : Bool
}

///|
fn AckDispatcher::new(
  conn_ref : Ref[Connection],
  consumer_id : UInt64,
  options? : AckGroupingOptions,
) -> AckDispatcher {
  {
    conn_ref,
    consumer_id,
    options,
    pending: [],
    mu: @async.Mutex::Mutex(),
    stop: @aqueue.Queue::Queue(kind=@aqueue.Kind::Blocking(1)),
    closed: false,
  }
}

///|
fn AckDispatcher::start(
  self : AckDispatcher,
  group : @async.TaskGroup[Unit],
) -> Unit {
  guard self.options is Some(config) &&
    config.max_size > 1 &&
    config.max_time_ms > 0 else {
    return
  }
  group.spawn_bg(
    async fn() {
      while true {
        let stopped = @async.with_timeout_opt(config.max_time_ms, async fn() {
          self.stop.get()
        })
        if stopped is Some(_) {
          return
        }
        self.flush() catch {
          _ => ()
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
}

///|
fn AckDispatcher::stop(self : AckDispatcher) -> Unit {
  self.closed = true
  ignore(self.stop.try_put(()) catch { _ => false })
}

///|
async fn AckDispatcher::reset(self : AckDispatcher) -> Unit {
  self.mu.acquire()
  defer self.mu.release()
  self.pending = []
}

///|
async fn AckDispatcher::flush_locked(self : AckDispatcher) -> Unit {
  if self.pending.is_empty() {
    return
  }
  let ordinary : Array[MessageId] = []
  let mut cumulative : PendingAck? = None
  for request in self.pending {
    if request.ack_type is @proto.CommandAck_AckType::Individual &&
      request.batch_ack_set is None {
      for id in request.ids {
        ordinary.push(id)
      }
    } else if request.ack_type is @proto.CommandAck_AckType::Cumulative {
      match cumulative {
        Some(previous) if previous.ids[0] > request.ids[0] => ()
        _ => cumulative = Some(request)
      }
    }
  }
  if !ordinary.is_empty() {
    send_ack(
      self.conn_ref.val,
      self.consumer_id,
      @proto.CommandAck_AckType::Individual,
      ordinary,
    )
  }
  for request in self.pending {
    if request.ack_type is @proto.CommandAck_AckType::Individual &&
      request.batch_ack_set is None {
      continue
    }
    if request.ack_type is @proto.CommandAck_AckType::Cumulative {
      continue
    }
    send_ack(
      self.conn_ref.val,
      self.consumer_id,
      request.ack_type,
      request.ids,
      batch_ack_set?=request.batch_ack_set,
      batch_size?=request.batch_size,
    )
  }
  if cumulative is Some(request) {
    send_ack(
      self.conn_ref.val,
      self.consumer_id,
      request.ack_type,
      request.ids,
      batch_ack_set?=request.batch_ack_set,
      batch_size?=request.batch_size,
    )
  }
  self.pending = []
}

///|
async fn AckDispatcher::flush(self : AckDispatcher) -> Unit {
  self.mu.acquire()
  defer self.mu.release()
  self.flush_locked()
}

///|
async fn AckDispatcher::ack(
  self : AckDispatcher,
  ack_type : @proto.CommandAck_AckType,
  ids : Array[MessageId],
  txn? : Transaction,
  await_response? : Bool = false,
  batch_ack_set? : Array[Int64],
  batch_size? : Int,
) -> Unit {
  if self.closed {
    raise ConsumerClosed
  }
  guard self.options is Some(config) && config.max_size > 1 else {
    return send_ack(
      self.conn_ref.val,
      self.consumer_id,
      ack_type,
      ids,
      txn?,
      await_response~,
      batch_ack_set?,
      batch_size?,
    )
  }
  self.mu.acquire()
  defer self.mu.release()
  if self.closed {
    raise ConsumerClosed
  }
  if await_response || txn is Some(_) {
    self.flush_locked()
    return send_ack(
      self.conn_ref.val,
      self.consumer_id,
      ack_type,
      ids,
      txn?,
      await_response~,
      batch_ack_set?,
      batch_size?,
    )
  }
  self.pending.push({ ack_type, ids, batch_ack_set, batch_size, })
  if self.pending.length() >= config.max_size {
    self.flush_locked()
  }
}
