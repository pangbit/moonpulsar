///|
enum RelayTag {
  Other
  Lookup
  Original
  Replay
  NewReceipt
  Late
  DuplicateReceipt
} derive(Eq, Debug)

///|
enum RelayDecision {
  Forward(Bytes, RelayTag)
  Quarantine(Bytes, Int32)
  Drop(RelayTag)
} derive(Debug)

///|
struct RelayGate {
  frontend : String
  payload : Bytes
  producer_name : String
  epochs : Map[Int32, Int32]
  live : Map[Int32, Bool]
  mut next_connection : Int32
  mut old_live : Int32
  mut target : (UInt64, UInt64)?
  mut target_connection : Int32?
  mut replay_connection : Int32?
  mut original : Bytes?
  mut held_count : Int32
  mut held_persisted : Int32
  mut fault_armed : Bool
  mut old_gone : Bool
  mut replacement_granted : Bool
  mut replay_seen : Bool
  mut new_receipt : Bool
}

///|
fn RelayGate::new(
  frontend : String,
  payload : Bytes,
  producer_name : String,
) -> RelayGate raise {
  ignore(loopback_endpoint(frontend))
  {
    frontend,
    payload,
    producer_name,
    epochs: {},
    live: {},
    next_connection: 0,
    old_live: 0,
    target: None,
    target_connection: None,
    replay_connection: None,
    original: None,
    held_count: 0,
    held_persisted: 0,
    fault_armed: false,
    old_gone: false,
    replacement_granted: false,
    replay_seen: false,
    new_receipt: false,
  }
}

///|
fn RelayGate::admit_backend(self : RelayGate) -> Bool {
  !self.fault_armed || self.replacement_granted
}

///|
fn RelayGate::open(self : RelayGate) -> Int32 raise {
  guard self.admit_backend() &&
    self.live.length() < 8 &&
    self.next_connection < 128 else {
    raise FixtureError::Rejected(
      "backend admission or connection bound violated",
    )
  }
  let id = self.next_connection
  self.next_connection += 1
  let epoch = if self.replacement_granted { 2 } else { 1 }
  self.epochs[id] = epoch
  self.live[id] = true
  if epoch == 1 {
    self.old_live += 1
  }
  id
}

///|
fn RelayGate::retire(self : RelayGate, id : Int32) -> Unit {
  if self.live.contains(id) {
    self.live.remove(id)
    if self.epochs.get(id) == Some(1) {
      self.old_live -= 1
    }
  }
}

///|
fn RelayGate::client_frame(
  self : RelayGate,
  id : Int32,
  raw : Bytes,
) -> RelayDecision raise {
  guard self.epochs.get(id) is Some(epoch) else {
    raise FixtureError::Rejected("unknown connection")
  }
  if !self.live.contains(id) {
    return Drop(Late)
  }
  let frame = decode_raw(raw)
  if frame.cmd.send is Some(send) {
    guard frame.cmd.type_ is @proto.BaseCommand_Type::SEND else {
      raise FixtureError::Rejected("SEND field disagrees with command type")
    }
    guard frame.payload == Some(self.payload) &&
      frame.metadata is Some(metadata) &&
      metadata.producer_name == self.producer_name &&
      metadata.sequence_id == send.sequence_id else {
      raise FixtureError::Rejected("unexpected SEND payload or identity")
    }
    let key = (send.producer_id, send.sequence_id)
    if self.target is None {
      guard epoch == 1 && !self.fault_armed else {
        raise FixtureError::Rejected(
          "original SEND arrived outside initial epoch",
        )
      }
      self.target = Some(key)
      self.target_connection = Some(id)
      self.original = Some(raw)
      Forward(raw, Original)
    } else {
      guard epoch == 2 &&
        self.replacement_granted &&
        self.old_gone &&
        self.target == Some(key) &&
        self.original == Some(raw) &&
        !self.replay_seen else {
        raise FixtureError::Rejected(
          "replay differs, repeats, or arrived in an ungranted epoch",
        )
      }
      self.replay_seen = true
      self.replay_connection = Some(id)
      Forward(raw, Replay)
    }
  } else {
    Forward(raw, Other)
  }
}

///|
fn RelayGate::broker_frame(
  self : RelayGate,
  id : Int32,
  raw : Bytes,
) -> RelayDecision raise {
  guard self.epochs.get(id) is Some(epoch) else {
    raise FixtureError::Rejected("unknown connection")
  }
  let frame = decode_raw(raw)
  if frame.cmd.partition_metadata_response is Some(response) &&
    response.partitions.unwrap_or(0U) > 0U {
    raise FixtureError::Rejected("fixture requires a nonpartitioned topic")
  }
  if frame.cmd.send_receipt is Some(receipt) &&
    self.target == Some((receipt.producer_id, receipt.sequence_id)) {
    guard frame.cmd.type_ is @proto.BaseCommand_Type::SEND_RECEIPT else {
      raise FixtureError::Rejected("receipt field disagrees with command type")
    }
    guard receipt.message_id is Some(_) else {
      raise FixtureError::Rejected("target receipt has no MessageId")
    }
    if epoch == 1 && self.target_connection == Some(id) {
      // No API ever releases a quarantined old-generation receipt.
      guard self.held_count < 8 else {
        raise FixtureError::Rejected("old receipt evidence bound")
      }
      self.held_count += 1
      return Quarantine(raw, self.held_count)
    }
    if !self.live.contains(id) {
      return Drop(Late)
    }
    if epoch == 1 ||
      (self.replay_connection is Some(connection) && connection != id) {
      return Forward(raw, Other)
    }
    guard self.replacement_granted && self.replay_seen else {
      raise FixtureError::Rejected("replacement receipt preceded replay")
    }
    if self.new_receipt {
      return Drop(DuplicateReceipt)
    }
    self.new_receipt = true
    return Forward(raw, NewReceipt)
  }
  if !self.live.contains(id) {
    return Drop(Late)
  }
  if frame.cmd.lookup_topic_response is Some(_) {
    Forward(lookup_to_frontend(raw, self.frontend), Lookup)
  } else {
    Forward(raw, Other)
  }
}

///|
fn RelayGate::persisted_old_receipt(
  self : RelayGate,
  number : Int32,
) -> Unit raise {
  guard number == self.held_persisted + 1 && number <= self.held_count else {
    raise FixtureError::Rejected("old receipt persistence order mismatch")
  }
  self.held_persisted = number
}

///|
struct AttemptState {
  mut attempts : Int32
  mut waits : Int32
  mut completed : Bool
  mut success : Bool
  mut received : Int32
  mut acks : Int32
  mut producer_reconnects : Int32
  mut consumer_reconnects : Int32
  mut terminal_reconnect_errors : Int32
}

///|
fn AttemptState::new() -> AttemptState {
  {
    attempts: 0,
    waits: 0,
    completed: false,
    success: false,
    received: 0,
    acks: 0,
    producer_reconnects: 0,
    consumer_reconnects: 0,
    terminal_reconnect_errors: 0,
  }
}

///|
fn AttemptState::pending(self : AttemptState) -> Bool {
  self.attempts == 1 && self.waits == 1 && !self.completed
}

///|
struct CommandLedger {
  mut index : Int32
  mut last : String
  challenges : Map[String, Bool]
}

///|
fn CommandLedger::new() -> CommandLedger {
  { index: 0, last: "", challenges: {}, }
}

///|
fn CommandLedger::apply(
  self : CommandLedger,
  index : Int32,
  command : String,
  challenge : String,
  gate : RelayGate,
  attempt : AttemptState,
) -> Bool raise {
  guard valid_nonce(challenge) else {
    raise FixtureError::Rejected("invalid controller challenge")
  }
  let value = command + ":" + challenge
  if index == self.index && value == self.last {
    return false
  }
  guard index == self.index + 1 else {
    raise FixtureError::Rejected("late or skipped command index")
  }
  guard !self.challenges.contains(challenge) else {
    raise FixtureError::Rejected("reused command challenge")
  }
  match command {
    "arm_fault" => {
      guard attempt.pending() && gate.held_persisted > 0 && !gate.fault_armed else {
        raise FixtureError::Rejected(
          "fault needs a held receipt and original pending waiter",
        )
      }
      gate.fault_armed = true
    }
    "old_gone" => {
      guard attempt.pending() &&
        gate.fault_armed &&
        !gate.old_gone &&
        gate.old_live == 0 else {
        raise FixtureError::Rejected("old generation has not retired")
      }
      gate.old_gone = true
    }
    "grant_replacement" => {
      guard attempt.pending() && gate.old_gone && !gate.replacement_granted else {
        raise FixtureError::Rejected(
          "replacement grant needs old-gone pending proof",
        )
      }
      gate.replacement_granted = true
    }
    "finish" => {
      guard attempt.attempts == 1 &&
        attempt.waits == 1 &&
        attempt.completed &&
        attempt.success &&
        attempt.received > 0 &&
        attempt.acks > 0 &&
        attempt.producer_reconnects > 0 &&
        attempt.consumer_reconnects > 0 &&
        attempt.terminal_reconnect_errors == 0 &&
        gate.replay_seen &&
        gate.new_receipt else {
        raise FixtureError::Rejected("incomplete acceptance")
      }
    }
    "abort" =>
      raise FixtureError::Rejected("controller aborted this invocation")
    _ => raise FixtureError::Rejected("unknown controller command")
  }
  self.index = index
  self.last = value
  self.challenges[challenge] = true
  true
}
