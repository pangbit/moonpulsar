///|
fn corpus_u32(data : Bytes, pos : Int) -> Int {
  data[pos].to_int() | (data[pos + 1].to_int() << 8) |
  (data[pos + 2].to_int() << 16) | (data[pos + 3].to_int() << 24)
}

///|
fn old_zlib_path(bytes : Bytes, size : Int) -> Bytes {
  @zlib.zlib_decompress(bytes) catch {
    _ => @reference.decompress_exact(bytes, size).unwrap()
  }
}

///|
async test "zlib corpus differential and bounded benchmark" {
  let data = @fs.read_file(@env.get_env_var("MOONPULSAR_ZLIB_CORPUS").unwrap()).binary()
  let mut pos = 0
  let mut count = 0
  while pos < data.length() {
    let compressed_size = corpus_u32(data, pos)
    let size = corpus_u32(data, pos + 4)
    pos += 8
    let compressed = data[pos:pos + compressed_size].to_owned()
    pos += compressed_size
    let expected = data[pos:pos + size].to_owned()
    pos += size
    assert_eq(decompress(Zlib, compressed, uncompressed_size=size), expected)
    assert_eq(old_zlib_path(compressed, size), expected)
    if count % 2 == 0 {
      assert_eq(zlib_sync_flush(compressed, size), expected)
      for offset in [0, 1, 2, compressed.length() / 2, compressed.length() - 5, compressed.length() - 1] {
        for bit in [0, 3, 7] {
          let mutation = @buffer.Buffer()
          mutation.write_bytes(compressed[:offset])
          mutation.write_byte(compressed[offset] ^ (1 << bit).to_byte())
          mutation.write_bytes(compressed[offset + 1:])
          let bytes = mutation.to_bytes()
          let actual = try { Some(zlib_sync_flush(bytes, size)) } catch { _ => None }
          assert_eq(actual, @reference.decompress_exact(bytes, size))
        }
      }
      for end in [0, 1, 2, compressed.length() / 2, compressed.length() - 1] {
        let result : Result[Bytes, Error] = Ok(zlib_sync_flush(compressed[:end].to_owned(), size)) catch { e => Err(e) }
        assert_true(result is Err(_))
      }
      for wrong in [0, -1, size - 1, size + 1, MAX_CHUNKED_MESSAGE_BYTES + 1] {
        let result : Result[Bytes, Error] = Ok(zlib_sync_flush(compressed, wrong)) catch { e => Err(e) }
        assert_true(result is Err(_))
      }
    }
    if count < 12 && count % 2 == 0 {
      for mode in ["old", "new"] {
        let timings : Array[Double] = []
        for _ in 0..<5 {
          let started = @bench.monotonic_clock_start()
          for _ in 0..<20 {
            let out = if mode == "old" { old_zlib_path(compressed, size) }
              else { decompress(Zlib, compressed, uncompressed_size=size) }
            assert_eq(out.length(), size)
          }
          timings.push(@bench.monotonic_clock_end(started) / 20.0)
        }
        println("BENCH size=\{size} sample=\{count / 2} mode=\{mode} us=\{Repr(timings)}")
      }
    }
    count += 1
  }
  assert_eq(count, 2000)
  println("PASS 1000 sync-flush and 1000 complete streams against native baseline")
}

///|
async test "zlib bounded allocation workload" {
  let compressed = @fs.read_file(@env.get_env_var("MOONPULSAR_ZLIB_CASE").unwrap()).binary()
  let size = @string.parse_int(@env.get_env_var("MOONPULSAR_ZLIB_SIZE").unwrap())
  let old = @env.get_env_var("MOONPULSAR_ZLIB_MODE") is Some("old")
  for _ in 0..<200 {
    let decoded = if old { old_zlib_path(compressed, size) }
      else { decompress(Zlib, compressed, uncompressed_size=size) }
    assert_eq(decoded.length(), size)
  }
  println("PASS bounded allocation workload")
}
