///|
async fn produce(
  client : @pulsar.Client,
  config : Config,
  stats : Stats,
) -> Unit {
  let batching = if config.batch_messages == 0 {
    None
  } else {
    Some(@pulsar.BatchingConfig::{
      max_messages: config.batch_messages,
      max_bytes: 131072,
      max_delay_ms: config.batch_delay_ms,
    })
  }
  let producer = client.create_producer(
    config.topic,
    batching?,
    max_pending_messages=config.queue_size,
    block_if_queue_full=true,
    send_timeout_ms=config.timeout_ms,
  )
  defer producer.close()
  let payload = Bytes::makei(config.size, i => (i % 251).to_byte())
  let clock = @bench.monotonic_clock_start()
  let warmup_us = config.warmup_ms.to_double() * 1000.0
  let stop_us = warmup_us + config.duration_ms.to_double() * 1000.0
  let ticket : Ref[Int64] = { val: 0L, }
  @async.with_timeout(
    config.warmup_ms + config.duration_ms + config.timeout_ms,
    async fn() {
      @async.with_task_group(fn(workers) {
        for _ in 0..<config.concurrency {
          workers.spawn_bg(async fn() {
            while @bench.monotonic_clock_end(clock) < stop_us {
              if config.rate > 0 {
                // Reserve a global launch slot before yielding to another worker.
                let due = ticket.val.to_double() *
                  1000000.0 /
                  config.rate.to_double()
                ticket.val += 1L
                if due >= stop_us {
                  break
                }
                let delay = due - @bench.monotonic_clock_end(clock)
                if delay > 0.0 {
                  @async.sleep((delay / 1000.0).ceil().to_int())
                }
              }
              let started = @bench.monotonic_clock_end(clock)
              if started >= stop_us {
                break
              }
              // Includes client queue/batch wait and broker receipt; excludes rate pacing.
              ignore(producer.send(@pulsar.ProducerMessage::new(payload)))
              let ended = @bench.monotonic_clock_end(clock)
              if in_window(started, ended, warmup_us, stop_us) {
                stats.record(config.size, ended - started)
              }
            }
          })
        }
      })
      // A low target rate can exhaust launch slots before the window ends.
      let remaining = stop_us - @bench.monotonic_clock_end(clock)
      if remaining > 0.0 {
        @async.sleep((remaining / 1000.0).ceil().to_int())
      }
    },
  )
}

///|
async fn consume(
  client : @pulsar.Client,
  config : Config,
  stats : Stats,
) -> Unit {
  let consumer = client.create_consumer(
    config.topic,
    config.subscription,
    initial_position=Earliest,
    receiver_queue_size=config.queue_size,
    durable=false,
  )
  defer consumer.close()
  let clock = @bench.monotonic_clock_start()
  let warmup_us = config.warmup_ms.to_double() * 1000.0
  let stop_us = warmup_us + config.duration_ms.to_double() * 1000.0
  @async.with_timeout(
    config.warmup_ms + config.duration_ms + config.timeout_ms,
    async fn() {
      while @bench.monotonic_clock_end(clock) < stop_us {
        let remaining = stop_us - @bench.monotonic_clock_end(clock)
        if remaining <= 0.0 {
          break
        }
        let message = @async.with_timeout(
          (remaining / 1000.0).ceil().to_int(),
          async fn() { consumer.receive() },
        ) catch {
          @async.TimeoutError => break
          error => raise error
        }
        let received = @bench.monotonic_clock_end(clock)
        message.ack()
        if in_window(
            received,
            @bench.monotonic_clock_end(clock),
            warmup_us,
            stop_us,
          ) {
          stats.record(message.payload().length(), 0.0)
        }
      }
    },
  )
}

///|
async fn main {
  let config = Config::load()
  let stats = Stats::new()
  @async.with_task_group(async fn(group) {
    let auth : &@pulsar.Authentication? = @env.get_env_var("PULSAR_TOKEN").map(token => {
      @pulsar.TokenAuth::new(token)
    })
    let client = @pulsar.Client::connect_with_options(
      group,
      @pulsar.ClientOptions::new(
        config.url,
        auth?,
        connection_timeout_ms=config.timeout_ms,
        operation_timeout_ms=config.timeout_ms,
      ),
    )
    defer client.close()
    if config.mode == "produce" {
      produce(client, config, stats)
    } else {
      consume(client, config, stats)
    }
  })
  guard stats.count > 0L else {
    fail("No messages completed in the measurement window")
  }
  let seconds = config.duration_ms.to_double() / 1000.0
  let latency : Json = if config.mode == "produce" {
    {
      "unit": "us",
      "bucket_width_us": 100,
      "p50": stats.percentile(0.5),
      "p95": stats.percentile(0.95),
      "p99": stats.percentile(0.99),
      "p999": stats.percentile(0.999),
      "max": stats.max_us,
      "overflow_count": stats.buckets[600001],
    }
  } else {
    null
  }
  let result : Json = {
    "status": "complete",
    "mode": config.mode,
    "topic": config.topic,
    "warmup_ms": config.warmup_ms,
    "duration_ms": config.duration_ms,
    "timeout_ms": config.timeout_ms,
    "subscription": config.subscription,
    "message_size": config.size,
    "concurrency": config.concurrency,
    "rate": config.rate,
    "batch_messages": config.batch_messages,
    "batch_delay_ms": config.batch_delay_ms,
    "queue_size": config.queue_size,
    "messages": stats.count,
    "payload_bytes": stats.bytes,
    "messages_per_second": stats.count.to_double() / seconds,
    "payload_mib_per_second": stats.bytes.to_double() / seconds / 1048576.0,
    "send_receipt_latency": latency,
  }
  println(result.stringify())
}
