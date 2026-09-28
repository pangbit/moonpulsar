// RFC 1951 structural scan for Java Pulsar's non-final sync-flushed streams.
// This scans tokens without materializing output. Only after proving the final
// empty stored block do we set its BFINAL bit and use flate's bounded decoder.

///|
priv struct FlushBits {
  data : Bytes
  mut pos : Int
}

///|
fn FlushBits::read(self : FlushBits, count : Int) -> Int raise PulsarError {
  guard count <= self.data.length() * 8 - self.pos else {
    raise InvalidFrame("truncated zlib sync flush")
  }
  let mut value = 0
  for i in 0..<count {
    value = value |
      (((self.data[self.pos >> 3].to_int() >> (self.pos & 7)) & 1) << i)
    self.pos += 1
  }
  value
}

///|
priv struct FlushCodes {
  counts : FixedArray[Int]
  symbols : Array[Int]
}

///|
let flush_fixed_literals : FlushCodes = {
  counts: [0, 0, 0, 0, 0, 0, 0, 24, 152, 112, 0, 0, 0, 0, 0, 0],
  symbols: [
    for i in 0..<288 => {
      if i < 24 {
        i + 256
      } else if i < 168 {
        i - 24
      } else if i < 176 {
        i + 112
      } else {
        i - 32
      }
    }
  ],
}

///|
let flush_fixed_distances : FlushCodes = {
  counts: [0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  symbols: [
    for i in 0..<32 => i
  ],
}

///|
fn FlushCodes::new(lengths : Array[Int]) -> FlushCodes raise PulsarError {
  let counts = FixedArray::make(16, 0)
  for n in lengths {
    counts[n] += 1
  }
  let mut available = 1
  for width in 1..<16 {
    available = (available << 1) - counts[width]
    guard available >= 0 else {
      raise InvalidFrame("oversubscribed DEFLATE code")
    }
  }
  let symbols : Array[Int] = []
  for width in 1..<16 {
    for symbol, n in lengths {
      if n == width {
        symbols.push(symbol)
      }
    }
  }
  { counts, symbols, }
}

///|
fn FlushCodes::decode(
  self : FlushCodes,
  bits : FlushBits,
) -> Int raise PulsarError {
  let mut code = 0
  let mut first = 0
  let mut index = 0
  for width in 1..<16 {
    code = (code << 1) | bits.read(1)
    let count = self.counts[width]
    if code >= first && code - first < count {
      return self.symbols[index + code - first]
    }
    index += count
    first = (first + count) << 1
  }
  raise InvalidFrame("invalid DEFLATE code")
}

///|
fn flush_tables(
  bits : FlushBits,
  kind : Int,
) -> (FlushCodes, FlushCodes) raise PulsarError {
  if kind == 1 {
    return (flush_fixed_literals, flush_fixed_distances)
  }
  let literal_count = bits.read(5) + 257
  let distance_count = bits.read(5) + 1
  let code_count = bits.read(4) + 4
  guard literal_count <= 286 else {
    raise InvalidFrame("invalid DEFLATE literal count")
  }
  let order = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15]
  let lengths = Array::make(19, 0)
  for i in 0..<code_count {
    lengths[order[i]] = bits.read(3)
  }
  let codes = FlushCodes::new(lengths)
  let all : Array[Int] = []
  while all.length() < literal_count + distance_count {
    let symbol = codes.decode(bits)
    if symbol < 16 {
      all.push(symbol)
    } else {
      let value = if symbol == 16 {
        guard !all.is_empty() else {
          raise InvalidFrame("DEFLATE repeat without previous length")
        }
        all[all.length() - 1]
      } else {
        0
      }
      let count = match symbol {
        16 => bits.read(2) + 3
        17 => bits.read(3) + 3
        18 => bits.read(7) + 11
        _ => raise InvalidFrame("invalid DEFLATE length symbol")
      }
      guard count <= literal_count + distance_count - all.length() else {
        raise InvalidFrame("DEFLATE code length overflow")
      }
      for _ in 0..<count {
        all.push(value)
      }
    }
  }
  guard all[256] != 0 else { raise InvalidFrame("missing DEFLATE end code") }
  (
    FlushCodes::new(all[:literal_count].to_owned()),
    FlushCodes::new(all[literal_count:].to_owned()),
  )
}

///|
fn zlib_flush_final_bit(payload : Bytes, size : Int) -> Int raise PulsarError {
  guard payload.length() >= 7 && payload.length() <= 0x0fffffff else {
    raise InvalidFrame("invalid zlib sync flush size")
  }
  let cmf = payload[0].to_int()
  let flg = payload[1].to_int()
  guard (cmf & 15) == 8 &&
    cmf >> 4 <= 7 &&
    ((cmf << 8) | flg) % 31 == 0 &&
    (flg & 32) == 0 else {
    raise InvalidFrame("invalid zlib sync flush header")
  }
  let bits : FlushBits = { data: payload, pos: 16, }
  let mut produced = 0
  let bases = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67,
    83, 99, 115, 131, 163, 195, 227, 258,
  ]
  for ;; {
    let start = bits.pos
    guard bits.read(1) == 0 else {
      // A final stream must use the ordinary zlib path and verify its trailer.
      raise InvalidFrame("final DEFLATE block in sync flush fallback")
    }
    let kind = bits.read(2)
    if kind == 0 {
      bits.pos = (bits.pos + 7) & -8
      let length = bits.read(16)
      guard (length ^ bits.read(16)) == 65535 else {
        raise InvalidFrame("invalid stored DEFLATE length")
      }
      guard length <= size - produced &&
        length <= payload.length() - bits.pos / 8 else {
        raise InvalidFrame("invalid stored DEFLATE size")
      }
      bits.pos += length * 8
      produced += length
      if length == 0 && bits.pos == payload.length() * 8 && produced == size {
        return start
      }
    } else {
      guard kind != 3 else { raise InvalidFrame("reserved DEFLATE block") }
      let (literals, distances) = flush_tables(bits, kind)
      for ;; {
        let symbol = literals.decode(bits)
        if symbol == 256 {
          break
        }
        let length = if symbol < 256 {
          1
        } else {
          guard symbol <= 285 else {
            raise InvalidFrame("reserved DEFLATE length")
          }
          let extra = if symbol < 265 || symbol == 285 {
            0
          } else {
            (symbol - 261) / 4
          }
          let length = bases[symbol - 257] + bits.read(extra)
          let code = distances.decode(bits)
          guard code < 30 else {
            raise InvalidFrame("reserved DEFLATE distance")
          }
          let distance = if code < 4 {
            code + 1
          } else {
            let extra = code / 2 - 1
            ((2 + code % 2) << extra) + 1 + bits.read(extra)
          }
          guard distance <= produced && distance <= 1 << ((cmf >> 4) + 8) else {
            raise InvalidFrame("invalid DEFLATE back-reference")
          }
          length
        }
        guard length <= size - produced else {
          raise InvalidFrame("zlib output exceeds declared size")
        }
        produced += length
      }
    }
  }
}

///|
fn zlib_sync_flush(payload : Bytes, size : Int) -> Bytes raise PulsarError {
  guard size > 0 && size <= MAX_CHUNKED_MESSAGE_BYTES else {
    raise InvalidFrame("invalid zlib output size")
  }
  let final_bit = zlib_flush_final_bit(payload, size) - 16
  let raw = @buffer.Buffer()
  let byte_index = final_bit / 8 + 2
  raw.write_bytes(payload[2:byte_index])
  raw.write_byte(payload[byte_index] | (1 << (final_bit & 7)).to_byte())
  raw.write_bytes(payload[byte_index + 1:])
  let output = FixedArray::make(size, b'\x00')
  let length = @flate.inflate_into(raw.to_bytes(), output) catch {
    _ => raise InvalidFrame("invalid zlib sync flush body")
  }
  guard length == size else { raise InvalidFrame("zlib output size mismatch") }
  Bytes::from_array(output)
}
