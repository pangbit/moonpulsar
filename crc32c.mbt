// CRC32-C (Castagnoli) checksum, used to verify broker entry metadata
// of incoming message frames.

///|
let crc32c_table : FixedArray[UInt32] = FixedArray::makei(256, fn(i) {
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
// Advance each byte through up to eight zero bytes. Slicing-by-eight breaks
// the per-byte dependency chain without a native CPU feature or dependency.
let crc32c_slices : FixedArray[FixedArray[UInt32]] = {
  let tables = FixedArray::make(8, crc32c_table)
  for slice in 1..<8 {
    let previous = tables[slice - 1]
    tables[slice] = FixedArray::makei(256, fn(i) {
      let crc = previous[i]
      (crc >> 8) ^ crc32c_table[(crc & 0xff).reinterpret_as_int()]
    })
  }
  tables
}

///|
/// Compute the CRC32-C (Castagnoli) checksum of `data`.
pub fn crc32c(data : BytesView) -> UInt32 {
  let mut crc : UInt32 = 0xffffffff
  let mut offset = 0
  while offset <= data.length() - 8 {
    let word = data[offset].to_uint() |
      (data[offset + 1].to_uint() << 8) |
      (data[offset + 2].to_uint() << 16) |
      (data[offset + 3].to_uint() << 24)
    let head = crc ^ word
    crc = crc32c_slices[7][(head & 0xff).reinterpret_as_int()] ^
      crc32c_slices[6][((head >> 8) & 0xff).reinterpret_as_int()] ^
      crc32c_slices[5][((head >> 16) & 0xff).reinterpret_as_int()] ^
      crc32c_slices[4][(head >> 24).reinterpret_as_int()] ^
      crc32c_slices[3][data[offset + 4].to_int()] ^
      crc32c_slices[2][data[offset + 5].to_int()] ^
      crc32c_slices[1][data[offset + 6].to_int()] ^
      crc32c_slices[0][data[offset + 7].to_int()]
    offset += 8
  }
  for i in offset..<data.length() {
    let idx = ((crc ^ data[i].to_uint()) & 0xff).reinterpret_as_int()
    crc = (crc >> 8) ^ crc32c_table[idx]
  }
  crc ^ 0xffffffff
}
