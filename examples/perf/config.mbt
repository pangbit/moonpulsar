///|
struct Config {
  mode : String
  url : String
  topic : String
  subscription : String
  warmup_ms : Int
  duration_ms : Int
  size : Int
  concurrency : Int
  rate : Int
  batch_messages : Int
  batch_delay_ms : Int
  queue_size : Int
  timeout_ms : Int
}

///|
fn bounded_int(
  name : String,
  value : String,
  min : Int,
  max : Int,
) -> Int raise {
  let parsed = @string.parse_int(value)
  guard parsed >= min && parsed <= max else {
    fail("\{name} must be in [\{min}, \{max}]")
  }
  parsed
}

///|
fn setting(name : String, default : Int, min : Int, max : Int) -> Int raise {
  bounded_int(
    name,
    @env.get_env_var(name).unwrap_or(default.to_string()),
    min,
    max,
  )
}

///|
fn Config::load() -> Config raise {
  let mode = @env.get_env_var("PULSAR_PERF_MODE").unwrap_or("produce")
  guard mode == "produce" || mode == "consume" else {
    fail("PULSAR_PERF_MODE must be produce or consume")
  }
  let topic = @env.get_env_var("PULSAR_TOPIC").unwrap_or("")
  guard topic != "" else {
    fail("PULSAR_TOPIC is required; use a dedicated test topic")
  }
  {
    mode,
    topic,
    url: @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650"),
    subscription: @env.get_env_var("PULSAR_SUBSCRIPTION").unwrap_or(
      "moonpulsar-perf",
    ),
    warmup_ms: setting("PERF_WARMUP_MS", 5000, 0, 86400000),
    duration_ms: setting("PERF_DURATION_MS", 30000, 1, 86400000),
    size: setting("PERF_SIZE", 1024, 1, 1048576),
    concurrency: setting("PERF_CONCURRENCY", 32, 1, 4096),
    rate: setting("PERF_RATE", 0, 0, 10000000),
    batch_messages: setting("PERF_BATCH_MESSAGES", 0, 0, 10000),
    batch_delay_ms: setting("PERF_BATCH_DELAY_MS", 1, 1, 60000),
    queue_size: setting("PERF_QUEUE_SIZE", 1000, 1, 1000000),
    timeout_ms: setting("PERF_TIMEOUT_MS", 10000, 1, 60000),
  }
}
