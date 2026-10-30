// This executable is an acceptance fixture, not a Broker launcher.

///|
struct Journal {
  directory : String
  run_id : String
  mutex : @async.Mutex
  mut next : Int32
}

///|
async fn Journal::emit(self : Journal, event : String, data : Json) -> Unit {
  self.mutex.acquire()
  defer self.mutex.release()
  guard self.next < 512 else {
    raise FixtureError::Rejected("journal event bound exceeded")
  }
  let sequence = self.next
  let record : Json = {
    "run_id": self.run_id,
    "seq": sequence,
    "event": event,
    "data": data,
  }
  let encoded = record.stringify()
  guard encoded.length() <= 2048 else {
    raise FixtureError::Rejected("journal record bound exceeded")
  }
  let path = self.directory + "/event-" + sequence.to_string() + ".json"
  @fs.write_file(
    path + ".tmp",
    encoded,
    create_mode=CreateNew,
    permission=0o600,
    sync=Full,
  )
  @fs.rename(path + ".tmp", path, replace=false)
  self.next += 1
}

///|
async fn Journal::save(self : Journal, name : String, raw : Bytes) -> Unit {
  self.mutex.acquire()
  defer self.mutex.release()
  guard raw.length() <= MAX_FRAME + 4 else {
    raise FixtureError::Rejected("raw evidence bound exceeded")
  }
  @fs.write_file(
    self.directory + "/" + name,
    raw,
    create_mode=CreateNew,
    permission=0o600,
    sync=Full,
  )
}

///|
fn snapshot(gate : RelayGate, state : AttemptState) -> Json {
  let (producer, sequence) = match gate.target {
    Some((producer, sequence)) => (producer.to_string(), sequence.to_string())
    None => ("", "")
  }
  {
    "attempts": state.attempts,
    "waits": state.waits,
    "completed": state.completed,
    "success": state.success,
    "received": state.received,
    "acks": state.acks,
    "producer_reconnects": state.producer_reconnects,
    "consumer_reconnects": state.consumer_reconnects,
    "terminal_reconnect_errors": state.terminal_reconnect_errors,
    "target_producer": producer,
    "target_sequence": sequence,
    "held": gate.held_count,
    "held_persisted": gate.held_persisted,
    "old_live": gate.old_live,
    "replay_seen": gate.replay_seen,
    "receipt_authorized": gate.new_receipt,
  }
}

///|
async fn relay_pump(
  reader : @socket.Tcp,
  writer : @socket.Tcp,
  client_to_broker : Bool,
  id : Int32,
  gate : RelayGate,
  state : AttemptState,
  journal : Journal,
  closed : Ref[Bool],
  read_chunk_limit~ : Int32 = 8192,
) -> Unit {
  defer {
    closed.val = true
    reader.close()
    writer.close()
    gate.retire(id)
  }
  guard read_chunk_limit > 0 && read_chunk_limit <= 8192 else {
    raise FixtureError::Rejected("invalid relay read chunk bound")
  }
  let decoder = FrameDecoder::new()
  while !closed.val {
    let chunk = reader.read_some(max_len=read_chunk_limit) catch {
      error => if closed.val { None } else { raise error }
    }
    guard chunk is Some(chunk) else {
      if decoder.pending.length() > 0 {
        if gate.epochs.get(id) == Some(1) && gate.fault_armed {
          journal.emit("old_partial_discarded", {
            "connection": id,
            "epoch": 1,
            "bytes": decoder.pending.length(),
          })
        } else {
          decoder.finish()
        }
      }
      break
    }
    for raw in decoder.feed(chunk) {
      let decision = if client_to_broker {
        gate.client_frame(id, raw)
      } else {
        gate.broker_frame(id, raw)
      }
      match decision {
        Quarantine(bytes, number) => {
          let name = "old-receipt-" + number.to_string() + ".bin"
          journal.save(name, bytes)
          gate.persisted_old_receipt(number)
          journal.emit("receipt_held", {
            "connection": id,
            "epoch": 1,
            "file": name,
            "state": snapshot(gate, state),
          })
        }
        Drop(tag) =>
          journal.emit("frame_dropped", {
            "connection": id,
            "reason": "\{Repr(tag)}",
          })
        Forward(bytes, tag) => {
          match tag {
            Original | Replay => {
              let name = if tag == Original {
                "original-send.bin"
              } else {
                "replayed-send.bin"
              }
              journal.save(name, bytes)
              journal.emit(
                if tag == Original {
                  "original_send"
                } else {
                  "replay_send"
                },
                {
                  "connection": id,
                  "epoch": gate.epochs.get(id).unwrap(),
                  "file": name,
                  "state": snapshot(gate, state),
                },
              )
            }
            Lookup =>
              journal.emit("lookup_rewritten", {
                "connection": id,
                "frontend": gate.frontend,
              })
            NewReceipt =>
              journal.emit("receipt_release_authorized", {
                "connection": id,
                "epoch": 2,
                "state": snapshot(gate, state),
              })
            _ => ()
          }
          writer.write(bytes)
          if tag == NewReceipt {
            journal.save("new-receipt.bin", bytes)
            journal.emit("new_receipt_forwarded", {
              "connection": id,
              "epoch": 2,
              "file": "new-receipt.bin",
              "state": snapshot(gate, state),
            })
          }
        }
      }
    }
  }
}

///|
async fn accept_relay(
  group : @async.TaskGroup[Unit],
  server : @socket.TcpServer,
  backend : String,
  gate : RelayGate,
  state : AttemptState,
  journal : Journal,
) -> Unit {
  let mut accepted = 0
  while true {
    let (frontend, _) = server.accept()
    accepted += 1
    guard accepted <= 128 else {
      frontend.close()
      raise FixtureError::Rejected("frontend connection attempt bound exceeded")
    }
    if !gate.admit_backend() {
      frontend.close()
      journal.emit("connection_rejected", { "attempt": accepted })
      continue
    }
    let id = gate.open()
    group.spawn_bg(
      async fn() {
        defer {
          frontend.close()
          gate.retire(id)
        }
        let upstream = @socket.Tcp::connect(@socket.Addr::parse(backend))
        defer upstream.close()
        let closed = Ref(false)
        journal.emit("connection_open", {
          "connection": id,
          "epoch": gate.epochs.get(id).unwrap(),
          "backend": backend,
          "upstream_local": upstream.addr.to_string(),
        })
        @async.with_task_group(async fn(pumps) {
          pumps.spawn_bg(
            async fn() {
              relay_pump(
                frontend, upstream, true, id, gate, state, journal, closed,
              )
            },
            no_wait=true,
          )
          relay_pump(
            upstream, frontend, false, id, gate, state, journal, closed,
          )
        })
        journal.emit("connection_retired", {
          "connection": id,
          "epoch": gate.epochs.get(id).unwrap(),
          "state": snapshot(gate, state),
        })
      },
      no_wait=true,
    )
  }
}

///|
async fn read_commands(
  gate : RelayGate,
  state : AttemptState,
  journal : Journal,
  finished : Ref[Bool],
) -> Unit {
  let ledger = CommandLedger::new()
  while !finished.val {
    let index = ledger.index + 1
    let path = journal.directory + "/command-" + index.to_string() + ".txt"
    if !@fs.exists(path) {
      @async.sleep(50)
      continue
    }
    guard @fs.kind(path, follow_symlink=false) == @fs.FileKind::Regular &&
      @fs.file_size(path) <= 512L else {
      raise FixtureError::Rejected("unsafe controller command file")
    }
    let lines = @fs.read_file(path).text().split("\n").to_array()
    guard lines.length() == 5 &&
      lines[1].to_owned() == journal.run_id &&
      lines[2].to_owned() == index.to_string() &&
      lines[4] == "" else {
      raise FixtureError::Rejected(
        "controller command run/index format mismatch",
      )
    }
    let command = lines[0].to_owned()
    let challenge = lines[3].to_owned()
    ignore(ledger.apply(index, command, challenge, gate, state))
    journal.emit("command_ack", {
      "index": index,
      "command": command,
      "challenge": challenge,
      "state": snapshot(gate, state),
    })
    if command == "finish" {
      finished.val = true
    }
  }
}
