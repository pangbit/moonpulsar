// Opt-in Linux whitebox RSS probe. Copy this file to the module root, build it
// with `moon test rss_probe_wbtest.mbt --target native --release --build-only`,
// then run the generated whitebox executable with `rss_probe_wbtest.mbt:0-1`.
// RSS_TIMEOUT_MS defaults to 10000; RSS_BATCH=32 enables batching.
// The mock and producer share one process; this is not a Broker throughput test.
// Remove the temporary root copy after measurement.

///|
async fn rss_probe_sample(label : String) -> Unit {
  let status = (@fs.read_file("/proc/self/status")).text()
  let mut rss = ""
  let mut peak = ""
  for line in status.split("\n") {
    if line.strip_prefix("VmRSS:") is Some(value) { rss = value.trim().to_owned() }
    if line.strip_prefix("VmHWM:") is Some(value) { peak = value.trim().to_owned() }
  }
  let report : Json = { "phase": label, "rss": rss, "peak": peak }
  println(report.stringify())
}

///|
async test "isolated RSS probe" {
  let timeout = @string.parse_int(@env.get_env_var("RSS_TIMEOUT_MS").unwrap_or("10000"))
  let batch = @env.get_env_var("RSS_BATCH").unwrap_or("0") == "32"
  @async.with_task_group(async fn(group) {
    let addr = start_mock_broker(group, async fn(_, peer) {
      expect_producer(peer)
      for _ in 0..<(if batch { 3125 } else { 100000 }) {
        let frame = peer.next()
        let send = frame.cmd.send.unwrap()
        send_receipt(peer, send.producer_id, send.sequence_id)
      }
    })
    let manager = ConnectionManager::new(group)
    let batching : BatchingConfig? = if batch { Some({ max_messages: 32, max_bytes: 131072, max_delay_ms: 1000 }) } else { None }
    let producer = SingleProducer::create(group, manager.get(addr), "persistent://public/default/rss-probe",
      max_pending_messages=32, send_timeout_ms=timeout,
      batching?)
    let payload = Bytes::make(1024, b'x')
    let clock = @bench.monotonic_clock_start()
    rss_probe_sample("start")
    for i in 0..<3125 {
      let futures : Array[ReceiptFuture] = []
      for _ in 0..<32 { futures.push(producer.send_async(ProducerMessage::new(payload))) }
      for future in futures { ignore(future.wait()) }
      let due = (i + 1).to_double() * 3200.0
      let remaining = due - @bench.monotonic_clock_end(clock)
      if remaining > 0.0 { @async.sleep((remaining / 1000.0).ceil().to_int()) }
      if i % 312 == 0 { rss_probe_sample("sending") }
    }
    let elapsed = @bench.monotonic_clock_end(clock)
    println(({ "messages": 100000, "elapsed_ms": elapsed / 1000.0, "msg_s": 100000000000.0 / elapsed } : Json).stringify())
    rss_probe_sample("receipts-complete")
    @async.sleep(timeout + 200)
    rss_probe_sample("deadline-drained")
    producer.close()
    manager.close()
  })
}
