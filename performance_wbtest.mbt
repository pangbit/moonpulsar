///|
test "payload compression and decompression" (b : @bench.T) {
  for size in [1024, 65536] {
    // Deterministic, compressible input. This is not an incompressible-data baseline.
    let payload = Bytes::makei(size, i => (i % 251).to_byte())
    for codec in [Lz4, Zlib, Zstd, Snappy] {
      let encoded = compress(codec, payload)
      assert_eq(decompress(codec, encoded, uncompressed_size=size), payload)
      b.bench(
        name="compress/\{Repr(codec)}/\{size}B",
        fn() { b.keep(try! compress(codec, payload)) },
        count=3,
      )
      b.bench(
        name="decompress/\{Repr(codec)}/\{size}B",
        fn() { b.keep(try! decompress(codec, encoded, uncompressed_size=size)) },
        count=3,
      )
    }
  }
}

///|
test "partition routing without broker" (b : @bench.T) {
  // Routing only returns a child handle; no connection or send is performed.
  let children = Array::makei(200, _ => Producer::partitioned([]))
  let keyed = ProducerMessage::new(b"payload", key="customer-123456")
  for
    (name, routing) in [
      ("key-hash", KeyHash),
      ("round-robin", RoundRobin),
      ("single", SinglePartition),
    ] {
    let producer = Producer::partitioned(children, routing~)
    b.bench(
      name="route/\{name}/200-partitions",
      fn() { b.keep(try! producer.route(keyed)) },
      count=3,
    )
  }
}

///|
test "batch encoding without broker" (b : @bench.T) {
  for count in [1, 100] {
    let messages = Array::makei(count, _ => {
      ProducerMessage::new(Bytes::make(1024, b'x'))
    })
    b.bench(
      name="batch-encode/\{count}x1024B",
      fn() { b.keep(try! encode_batch_payload(messages)) },
      count=3,
    )
  }
}
