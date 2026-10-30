///|
fn setting(name : String, fallback : Int32) -> Int32 raise {
  @string.parse_int(@env.get_env_var(name).unwrap_or(fallback.to_string()))
}

///|
fn payload(id : Int32, size : Int32) -> Bytes {
  Bytes::makei(size, i => ((i + id) % 251).to_byte())
}

///|
async fn main {
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or("")
  let ready_file = @env.get_env_var("RECOVERY_READY_FILE").unwrap_or("")
  guard topic != "" && ready_file != "" else {
    fail("dedicated PULSAR_TOPIC and RECOVERY_READY_FILE required")
  }
  let duration = setting("RECOVERY_DURATION_MS", 60000)
  let rate = setting("RECOVERY_RATE", 100)
  let size = setting("RECOVERY_SIZE", 65536)
  let slow = setting("RECOVERY_SLOW_MS", 0)
  guard duration >= 1000 &&
    duration <= 300000 &&
    rate > 0 &&
    rate <= 1000 &&
    (size == 1024 || size == 65536) &&
    slow >= 0 &&
    slow <= 1000 else {
    fail("invalid recovery load")
  }
  let clock = @bench.monotonic_clock_start()
  let elapsed = fn() { @bench.monotonic_clock_end(clock) / 1000.0 }
  let log = fn(event : String, id : Int32, detail : String) {
    let record : Json = {
      "event": event,
      "id": id,
      "elapsed_ms": elapsed(),
      "detail": detail,
    }
    println(record.stringify())
  }
  @async.with_timeout(duration + 90000, async fn() {
    @async.with_task_group(async fn(group) {
      let client = @pulsar.Client::connect_with_options(
        group,
        @pulsar.ClientOptions::new(
          @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:16650"),
          operation_timeout_ms=3000,
          connection_timeout_ms=3000,
          max_memory_bytes=16777216,
          on_event=fn(event) {
            match event {
              ConsumerReconnecting(_) => log("consumer_reconnecting", -1, "")
              ConsumerReconnected(_, result) =>
                log("consumer_reconnected", -1, "\{Repr(result)}")
              ProducerReconnecting(_) => log("producer_reconnecting", -1, "")
              ProducerReconnected(_, result) =>
                log("producer_reconnected", -1, "\{Repr(result)}")
              _ => ()
            }
          },
        ),
      )
      defer client.close()
      let consumer = client.create_consumer(
        topic,
        "maintenance-recovery",
        initial_position=Earliest,
        receiver_queue_size=32,
        durable=true,
      )
      let producer = client.create_producer(
        topic,
        max_pending_messages=64,
        block_if_queue_full=true,
        send_timeout_ms=3000,
      )
      let done = Ref(false)
      let ticket = Ref(0)
      let start = elapsed()
      log(
        "ready", -1, "queue=32; max_pending=64; retained_byte_budget=16777216",
      )
      @fs.write_file(ready_file, "ready\n")
      @async.with_task_group(async fn(work) {
        work.spawn_bg(async fn() {
          let mut idle = 0
          while !done.val || idle < 5 {
            let message = @async.with_timeout(1000, async fn() {
              consumer.receive()
            }) catch {
              @async.TimeoutError => {
                if done.val {
                  idle += 1
                }
                continue
              }
              error => {
                log("receive_error", -1, error.to_string())
                raise error
              }
            }
            idle = 0
            let id = @string.parse_int(
              message.properties().get("maintenance-id").unwrap(),
            )
            assert_eq(message.payload(), payload(id, size))
            log(
              "received",
              id,
              "\{Repr(message.id())}; redelivery=\{message.redelivery_count()}",
            )
            let now = elapsed() - start
            if slow > 0 && now >= 5000.0 && now < 20000.0 {
              @async.sleep(slow)
            }
            log("processed", id, "")
            message.ack(await_response=true) catch {
              error => {
                log("ack_unknown", id, error.to_string())
                continue
              }
            }
            log("ack_confirmed", id, "\{Repr(message.id())}")
          }
          log("drained", -1, "five idle receive windows after send completion")
        })
        @async.with_task_group(fn(senders) {
          for _ in 0..<8 {
            senders.spawn_bg(async fn() {
              while true {
                let id = ticket.val
                ticket.val += 1
                let due = id.to_double() * 1000.0 / rate.to_double()
                if due >= duration.to_double() {
                  break
                }
                let wait = start + due - elapsed()
                if wait > 0.0 {
                  @async.sleep(wait.ceil().to_int())
                }
                // No application retry. Any replay is owned by the client.
                log("attempt", id, "")
                let receipt = producer.send(
                  @pulsar.ProducerMessage::new(payload(id, size), properties={
                    "maintenance-id": id.to_string(),
                  }),
                ) catch {
                  error => {
                    log("send_unknown", id, error.to_string())
                    continue
                  }
                }
                log("confirmed", id, "\{Repr(receipt.message_id)}")
              }
            })
          }
        })
        log("sending_done", -1, "")
        done.val = true
      })
      log("closing", -1, "")
      consumer.close()
      producer.close()
      client.close()
      log("closed", -1, "")
    })
  })
  log("complete", -1, "task group exited")
}
