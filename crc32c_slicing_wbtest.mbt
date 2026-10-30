///|
// Independent bit-at-a-time polynomial oracle; no production lookup tables.
fn crc32c_bitwise(data : BytesView) -> UInt32 {
  let mut crc = 0xffffffffU
  for byte in data {
    crc = crc ^ byte.to_uint()
    for _ in 0..<8 {
      crc = if (crc & 1) == 1 { (crc >> 1) ^ 0x82f63b78U } else { crc >> 1 }
    }
  }
  crc ^ 0xffffffffU
}

///|
test "sliced CRC matches polynomial for offsets tails and large payloads" {
  let mut seed = 0xdeadbeefU
  let data = Bytes::makei(65552, _ => {
    seed = seed * 1664525U + 1013904223U
    (seed >> 24).to_byte()
  })
  for offset in 0..<8 {
    for length in 0..<80 {
      let view = data[offset:offset + length]
      assert_eq(crc32c(view), crc32c_bitwise(view))
    }
    for length in [255, 256, 257, 1023, 1024, 1025, 65535, 65536] {
      let view = data[offset:offset + length]
      assert_eq(crc32c(view), crc32c_bitwise(view))
    }
  }
}
