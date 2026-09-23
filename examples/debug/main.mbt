async fn main {
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let topic = "persistent://public/default/reader-eot2"
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, url)
    // subscribe FIRST (real-time catch-up scenario)
    let r = client.create_reader(topic, start_message_id=@pulsar.MessageId::latest())
    println("reader subscribed, producing after 2s...")
    group.spawn_bg(async fn() {
      @async.sleep(2000)
      let p = client.create_producer(topic)
      for i in 0..<3 {
        ignore(p.send(@pulsar.ProducerMessage::new(@utf8.encode("m\{i}"))))
      }
      println("produced")
      p.close()
    })
    for i in 0..<3 {
      let m = @async.with_timeout(15000, async fn() { r.next() })
      println("next \{i}: \{m.map(fn(m) { m.payload() })}")
    }
    println("waiting for end-of-topic (type 27)...")
    let m = @async.with_timeout(8000, async fn() { r.next() })
    println("next after end: \{m.is_some()}")
    r.close()
    client.close()
  })
}
