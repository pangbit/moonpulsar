///|
test "CRC throughput without broker" (b : @bench.T) {
  for size in [1024, 65536] {
    let payload = Bytes::makei(size, i => (i % 251).to_byte())
    b.bench(name="crc32c/\{size}B", fn() { b.keep(crc32c(payload)) }, count=3)
  }
}
