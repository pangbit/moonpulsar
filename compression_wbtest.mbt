// Compression codec tests: roundtrips and format sanity.

///|
fn sample_data() -> Bytes {
  let buf = @buffer.Buffer()
  for i in 0..<2000 {
    buf.write_string(
      "message number \{i}: the quick brown fox jumps over the lazy dog. ",
    )
  }
  buf.to_bytes()
}

///|
test "lz4 roundtrip" {
  let data = sample_data()
  let compressed = lz4_compress(data)
  // repetitive data must compress well
  assert_true(compressed.length() < data.length() / 4)
  assert_eq(lz4_decompress(compressed), data)
}

///|
test "lz4 roundtrip of incompressible data" {
  // pseudo-random bytes stored as uncompressed blocks
  let buf = @buffer.Buffer()
  let mut x : UInt = 0x12345678
  for _ in 0..<70000 {
    x = x * 1103515245 + 12345
    buf.write_byte((x >> 16).to_byte())
  }
  let data = buf.to_bytes()
  assert_eq(lz4_decompress(lz4_compress(data)), data)
}

///|
test "lz4 empty input" {
  assert_eq(lz4_decompress(lz4_compress(b"")), b"")
}

///|
test "lz4 rejects bad magic" {
  let result : Result[Bytes, Error] = Ok(lz4_decompress(b"not an lz4 frame")) catch {
    e => Err(e)
  }
  assert_true(result is Err(InvalidFrame(_)))
}

///|
test "zlib roundtrip" {
  let data = sample_data()
  assert_eq(decompress(Zlib, compress(Zlib, data)), data)
}

///|
test "zstd roundtrip" {
  let data = sample_data()
  assert_eq(decompress(Zstd, compress(Zstd, data)), data)
}

///|
test "xxhash32 known vectors" {
  assert_eq(xxhash32(b""), 0x02cc5d05)
  assert_eq(xxhash32(b"Hello, world!"), 0x31b7405d)
  assert_eq(xxhash32(b"abc"), 0x32d153ff)
}

///|
test "snappy roundtrip" {
  let data = sample_data()
  let compressed = compress(Snappy, data)
  assert_true(compressed.length() < data.length() / 4)
  assert_eq(decompress(Snappy, compressed), data)
  // empty and incompressible inputs
  assert_eq(decompress(Snappy, compress(Snappy, b"")), b"")
  let buf = @buffer.Buffer()
  let mut x : UInt = 99
  for _ in 0..<70000 {
    x = x * 1103515245 + 12345
    buf.write_byte((x >> 16).to_byte())
  }
  let noise = buf.to_bytes()
  assert_eq(decompress(Snappy, compress(Snappy, noise)), noise)
}

///|
test "snappy decodes cramjam reference frames" {
  // uncompressed chunk (0x01) produced by cramjam/snappy
  let raw1 = b"moonpulsar snappy interop: hello hello hello!"
  let framed1 : Bytes = b"\xff\x06\x00\x00\x73\x4e\x61\x50\x70\x59\x01\x31\x00\x00\xae\x00\x03\x9e\x6d\x6f\x6f\x6e\x70\x75\x6c\x73\x61\x72\x20\x73\x6e\x61\x70\x70\x79\x20\x69\x6e\x74\x65\x72\x6f\x70\x3a\x20\x68\x65\x6c\x6c\x6f\x20\x68\x65\x6c\x6c\x6f\x20\x68\x65\x6c\x6c\x6f\x21"
  assert_eq(snappy_decompress(framed1), raw1)
  // compressed chunk (0x00) produced by cramjam/snappy
  let framed2 : Bytes = b"\xff\x06\x00\x00\x73\x4e\x61\x50\x70\x59\x00\x36\x00\x00\xdc\xf9\xa9\x3d\xa0\x06\x1c\x61\x62\x63\x64\x65\x66\x67\x68\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\xfe\x08\x00\x5e\x08\x00"
  let expected2 = @buffer.Buffer()
  for _ in 0..<100 {
    expected2.write_bytes(b"abcdefgh")
  }
  assert_eq(snappy_decompress(framed2), expected2.to_bytes())
}

///|
test "lz4 raw block wire format roundtrip" {
  let data = sample_data()
  let compressed = compress(Lz4, data)
  assert_eq(decompress(Lz4, compressed, uncompressed_size=data.length()), data)
}
