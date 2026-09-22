// CRC32-C (Castagnoli) checksum, used to verify broker entry metadata
// of incoming message frames.

///|
let crc32c_table : FixedArray[UInt] = FixedArray::makei(256, fn(i) {
  let mut crc = i.reinterpret_as_uint()
  for _ in 0..<8 {
    if (crc & 1) != 0 {
      crc = (crc >> 1) ^ 0x82f63b78
    } else {
      crc = crc >> 1
    }
  }
  crc
})

///|
/// Compute the CRC32-C (Castagnoli) checksum of `data`.
pub fn crc32c(data : BytesView) -> UInt {
  let mut crc : UInt = 0xffffffff
  for b in data {
    let idx = ((crc ^ b.to_uint()) & 0xff).reinterpret_as_int()
    crc = (crc >> 8) ^ crc32c_table[idx]
  }
  crc ^ 0xffffffff
}
