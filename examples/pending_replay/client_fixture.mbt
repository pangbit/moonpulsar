// Shared live/test entry: private fixture APIs, not product APIs.

///|
struct FixtureRuntime {
  server : @socket.TcpServer
  gate : RelayGate
  state : AttemptState
  journal : Journal
  finished : Ref[Bool]
  topic : String
  producer_name : String
  payload : Bytes
}

///|
async fn start_fixture_runtime(
  group : @async.TaskGroup[Unit],
  backend : String,
  journal : Journal,
) -> FixtureRuntime {
  ignore(loopback_endpoint(backend))
  guard valid_nonce(journal.run_id) else {
    raise FixtureError::Rejected("invalid fixture run id")
  }
  let topic = "persistent://public/default/pending-replay-" + journal.run_id
  let producer_name = "pending-producer-" + journal.run_id
  let payload = @utf8.encode("pending-send:" + journal.run_id)
  let server = @socket.TcpServer::TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  errdefer server.close()
  let gate = RelayGate::new(server.addr.to_string(), payload, producer_name)
  let state = AttemptState::new()
  let finished = Ref(false)
  journal.emit("startup", {
    "topic": topic,
    "producer_name": producer_name,
    "frontend": gate.frontend,
    "backend": backend,
    "send_timeout_ms": 180000,
    "whole_client_timeout_ms": 240000,
  })
  group.spawn_bg(
    async fn() { accept_relay(group, server, backend, gate, state, journal) },
    no_wait=true,
  )
  { server, gate, state, journal, finished, topic, producer_name, payload, }
}

///|
async fn FixtureRuntime::run_client(
  self : FixtureRuntime,
  group : @async.TaskGroup[Unit],
) -> @pulsar.SendReceipt {
  let gate = self.gate
  let state = self.state
  let journal = self.journal
  let run_id = journal.run_id
  let finished = self.finished
  let topic = self.topic
  let producer_name = self.producer_name
  let payload = self.payload
  let completed_receipt : Ref[@pulsar.SendReceipt?] = Ref(None)
  let client = @pulsar.Client::connect_with_options(
    group,
    @pulsar.ClientOptions::new(
      "pulsar://" + gate.frontend,
      connection_timeout_ms=3000,
      operation_timeout_ms=3000,
      max_connections_per_broker=1,
      max_memory_bytes=1048576,
      reconnect_policy={
        max_retries: 64,
        initial_backoff_ms: 200,
        max_backoff_ms: 2000,
      },
      on_event=fn(event) {
        match event {
          ProducerReconnected(_, Ok(_)) => state.producer_reconnects += 1
          ConsumerReconnected(_, Ok(_)) => state.consumer_reconnects += 1
          ProducerReconnected(_, Err(_)) | ConsumerReconnected(_, Err(_)) =>
            state.terminal_reconnect_errors += 1
          _ => ()
        }
      },
    ),
  )
  defer client.close()
  let consumer = client.create_consumer(
    topic,
    "pending-subscription-" + run_id,
    consumer_name="pending-consumer-" + run_id,
    initial_position=Earliest,
    receiver_queue_size=16,
    durable=true,
  )
  let producer = client.create_producer(
    topic,
    name=producer_name,
    max_pending_messages=1,
    send_timeout_ms=180000,
  )
  group.spawn_bg(
    async fn() { read_commands(gate, state, journal, finished) },
    no_wait=true,
  )
  group.spawn_bg(
    async fn() {
      while true {
        let message = consumer.receive() catch {
          @pulsar.ConsumerClosed if finished.val => return
          @async.TaskCancelled if finished.val => return
          error => raise error
        }
        guard message.payload() == payload && state.received < 16 else {
          raise FixtureError::Rejected(
            "consumer payload mismatch or observation bound exceeded",
          )
        }
        state.received += 1
        journal.emit("received", {
          "message_id": "\{Repr(message.id())}",
          "redelivery_count": message.redelivery_count(),
          "state": snapshot(gate, state),
        })
        // Message::ack uses the retained consumer's current conn_ref.
        while !gate.new_receipt {
          @async.sleep(20)
        }
        message.ack(await_response=true) catch {
          @pulsar.ConsumerClosed if finished.val => return
          @pulsar.ConnectionClosed if finished.val => return
          @async.TaskCancelled if finished.val => return
          error => raise error
        }
        state.acks += 1
        journal.emit("ack_confirmed", { "state": snapshot(gate, state) })
      }
    },
    no_wait=true,
  )
  state.attempts += 1
  // Exactly one application admission, one immutable future, one waiter.
  let original_future = producer.send_async(
    @pulsar.ProducerMessage::new(payload),
  )
  let original_waiter = group.spawn(
    async fn() {
      state.waits += 1
      journal.emit("wait_entered", { "state": snapshot(gate, state) })
      let result : Result[@pulsar.SendReceipt, Error] = Ok(
        original_future.wait(),
      ) catch {
        error => Err(error)
      }
      match result {
        Ok(receipt) => completed_receipt.val = Some(receipt)
        Err(_) => ()
      }
      state.completed = true
      state.success = match result {
        Ok(receipt) =>
          gate.target is Some((_, sequence)) &&
          receipt.sequence_id == sequence &&
          receipt.producer_name == producer_name
        Err(_) => false
      }
      journal.emit("future_result", { "state": snapshot(gate, state) })
      match result {
        Ok(_) => {
          guard state.success else {
            raise FixtureError::Rejected("receipt identity mismatch")
          }
        }
        Err(error) => raise error
      }
    },
    no_wait=true,
  )
  while !finished.val {
    @async.sleep(50)
  }
  original_waiter.wait()
  consumer.close()
  producer.close()
  client.close()
  journal.emit("complete", { "state": snapshot(gate, state) })
  completed_receipt.val.unwrap()
}
