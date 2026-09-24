// Receive official-client compressed chunks and send MoonBit chunks.

///|
async fn main {
  guard @env.get_env_var("PULSAR_TOPIC") is Some(base) &&
    @env.get_env_var("PULSAR_PAYLOAD_FILE") is Some(path) else {
    println("set PULSAR_TOPIC and PULSAR_PAYLOAD_FILE")
    return
  }
  let payload = @fs.read_file(path).binary()
  let url = @env.get_env_var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650")
  let peer = @env.get_env_var("PULSAR_PEER").unwrap_or("go")
  let names = if peer == "java" {
    ["lz4", "zlib", "zstd"]
  } else {
    ["lz4", "zlib", "zstd", "snappy"]
  }
  let codecs = if peer == "java" {
    [@pulsar.CompressionCodec::Lz4, Zlib, Zstd]
  } else {
    [@pulsar.CompressionCodec::Lz4, Zlib, Zstd, Snappy]
  }
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, url)
    for index in 0..<names.length() {
      let consumer = client.create_consumer(
        base + "-" + peer + "-" + names[index],
        "moonpulsar-chunk-moonbit-\{names[index]}",
        initial_position=Earliest,
      )
      let message = @async.with_timeout(20000, async fn() { consumer.receive() })
      assert_eq(message.payload(), payload)
      println("MoonBit received \{names[index]} chunks")
      message.ack(await_response=true)
      consumer.close()
      let producer = client.create_producer(
        base + "-moonbit-" + names[index],
        compression=codecs[index],
        chunk_size=32768,
      )
      ignore(producer.send(@pulsar.ProducerMessage::new(payload)))
      producer.close()
    }
    client.close()
    println("MoonBit exchanged compressed chunks with \{peer}")
  })
}
