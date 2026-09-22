// xxHash32, required by the LZ4 frame format for header and content
// checksums.

///|
let xxh32_primes : (UInt, UInt, UInt, UInt, UInt) = (
  2654435761, 2246822519, 3266489917, 668265263, 374761393,
)

///|
fn xxh32_round(acc : UInt, input : UInt) -> UInt {
  let (p1, p2, _, _, _) = xxh32_primes
  let acc = acc + input * p2
  let acc = (acc << 13) | (acc >> 19)
  acc * p1
}

///|
fn read_le32(data : BytesView, at : Int) -> UInt {
  data[at].to_uint() |
  (data[at + 1].to_uint() << 8) |
  (data[at + 2].to_uint() << 16) |
  (data[at + 3].to_uint() << 24)
}

///|
/// Compute xxHash32 of `data` with seed 0.
pub fn xxhash32(data : BytesView) -> UInt {
  let (p1, p2, p3, p4, p5) = xxh32_primes
  let len = data.length()
  let mut offset = 0
  let mut acc : UInt = 0
  if len >= 16 {
    let mut acc1 : UInt = p1 + p2
    let mut acc2 : UInt = p2
    let mut acc3 : UInt = 0
    let mut acc4 : UInt = 0 - p1
    while offset + 16 <= len {
      acc1 = xxh32_round(acc1, read_le32(data, offset))
      acc2 = xxh32_round(acc2, read_le32(data, offset + 4))
      acc3 = xxh32_round(acc3, read_le32(data, offset + 8))
      acc4 = xxh32_round(acc4, read_le32(data, offset + 12))
      offset += 16
    }
    acc = ((acc1 << 1) | (acc1 >> 31)) +
      ((acc2 << 7) | (acc2 >> 25)) +
      ((acc3 << 12) | (acc3 >> 20)) +
      ((acc4 << 18) | (acc4 >> 14))
  } else {
    acc = p5
  }
  acc += len.reinterpret_as_uint()
  while offset + 4 <= len {
    acc += read_le32(data, offset) * p3
    acc = ((acc << 17) | (acc >> 15)) * p4
    offset += 4
  }
  while offset < len {
    acc += data[offset].to_uint() * p5
    acc = ((acc << 11) | (acc >> 21)) * p1
    offset += 1
  }
  acc = acc ^ (acc >> 15)
  acc = acc * p2
  acc = acc ^ (acc >> 13)
  acc = acc * p3
  acc ^ (acc >> 16)
}
