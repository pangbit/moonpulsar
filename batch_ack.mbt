// Track acknowledgments inside a broker batch. A batch entry must not be
// acknowledged as a whole until every contained message was acknowledged.

///|
priv struct BatchAckState {
  acknowledged : Array[Bool]
  generation : Int
}

///|
struct BatchAckTracker {
  states : Map[MessageId, BatchAckState]
  enabled : Bool
  mu : @async.Mutex
  mut next_generation : Int
}

///|
fn BatchAckTracker::new(enabled : Bool) -> BatchAckTracker {
  { states: {}, enabled, mu: @async.Mutex::Mutex(), next_generation: 0, }
}

///|
fn batch_entry_id(id : MessageId) -> MessageId {
  { ..id, batch_index: -1, }
}

///|
fn BatchAckTracker::register(
  self : BatchAckTracker,
  id : MessageId,
  size : Int,
  broker_ack_set : Array[Int64],
) -> Unit {
  let key = batch_entry_id(id)
  let acknowledged : Array[Bool] = []
  for _ in 0..<size {
    acknowledged.push(false)
  }
  if !broker_ack_set.is_empty() {
    for index in 0..<size {
      if index / 64 >= broker_ack_set.length() ||
        (
          (broker_ack_set[index / 64].reinterpret_as_uint64() >> (index % 64)) &
          1UL
        ) ==
        0UL {
        acknowledged[index] = true
      }
    }
  }
  self.next_generation += 1
  self.states[key] = { acknowledged, generation: self.next_generation, }
}

///|
fn BatchAckTracker::clear(self : BatchAckTracker) -> Unit {
  self.states.clear()
}

///|
fn batch_ack_set(
  acknowledged : Array[Bool],
  through : Int,
  cumulative : Bool,
  only_current? : Bool = false,
) -> Array[Int64] {
  let words : Array[Int64] = []
  for _ in 0..<((acknowledged.length() + 63) / 64) {
    words.push(0L)
  }
  for index in 0..<acknowledged.length() {
    let acked = (!only_current && acknowledged[index]) ||
      (if cumulative { index <= through } else { index == through })
    if !acked {
      words[index / 64] = words[index / 64] |
        (1UL << (index % 64)).reinterpret_as_int64()
    }
  }
  words
}

///|
async fn BatchAckTracker::ack(
  self : BatchAckTracker,
  conn : Connection,
  consumer_id : UInt64,
  id : MessageId,
  ack_type : @proto.CommandAck_AckType,
  txn? : Transaction,
  await_response? : Bool = false,
  dispatcher? : AckDispatcher,
) -> Unit {
  self.mu.acquire()
  defer self.mu.release()
  let key = batch_entry_id(id)
  guard self.states.get(key) is Some(state) &&
    id.batch_index >= 0 &&
    id.batch_index < state.acknowledged.length() else {
    raise InvalidFrame("batch ACK requires a received message and batch size")
  }
  if ack_type is @proto.CommandAck_AckType::Individual &&
    state.acknowledged[id.batch_index] {
    return
  }
  let cumulative = ack_type is @proto.CommandAck_AckType::Cumulative
  let mut all_acked = true
  for index in 0..<state.acknowledged.length() {
    if !state.acknowledged[index] &&
      !(if cumulative {
        index <= id.batch_index
      } else {
        index == id.batch_index
      }) {
      all_acked = false
      break
    }
  }
  if cumulative && !self.enabled && !all_acked {
    raise InvalidFrame("partial cumulative batch ACK requires batch-index ACK")
  }
  if self.enabled {
    // A transaction registers each index independently with the broker.
    // Repeating earlier cleared bits conflicts with its pending ACK state.
    let bits = batch_ack_set(
      state.acknowledged,
      id.batch_index,
      cumulative,
      only_current=txn is Some(_),
    )
    let confirmed = await_response || txn is Some(_)
    if dispatcher is Some(sender) {
      sender.ack(
        ack_type,
        [key],
        txn?,
        await_response=confirmed,
        batch_ack_set=bits,
        batch_size=state.acknowledged.length(),
      )
    } else {
      send_ack(
        conn,
        consumer_id,
        ack_type,
        [key],
        txn?,
        await_response=confirmed,
        batch_ack_set=bits,
        batch_size=state.acknowledged.length(),
      )
    }
  } else if all_acked {
    let confirmed = await_response || txn is Some(_)
    if dispatcher is Some(sender) {
      sender.ack(ack_type, [key], txn?, await_response=confirmed)
    } else {
      send_ack(
        conn,
        consumer_id,
        ack_type,
        [key],
        txn?,
        await_response=confirmed,
      )
    }
  } else if await_response {
    raise InvalidFrame(
      "partial batch ACK cannot await broker confirmation without batch-index ACK",
    )
  }
  guard self.states.get(key) is Some(current) &&
    current.generation == state.generation else {
    return
  }
  if cumulative {
    for index in 0..<(id.batch_index + 1) {
      state.acknowledged[index] = true
    }
  } else {
    state.acknowledged[id.batch_index] = true
  }
  if all_acked {
    self.states.remove(key)
  }
}
