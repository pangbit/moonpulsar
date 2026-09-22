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
