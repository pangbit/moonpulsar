// Pulsar message compression uses raw Snappy blocks. The public
// snappy_compress/snappy_decompress helpers retain Google's framed stream
// format (3-byte little-endian chunk lengths, masked CRC32-C checksums):
//
//   stream:  chunk 0xFF "sNaPpY" then data chunks
//   data chunk: type(1) + length(3, little-endian, includes checksum)
//               + masked crc32c(4, big-endian) + snappy block
//   chunk 0x01 carries uncompressed data instead

///|
const SNAPPY_FRAMED_CHUNK_MAX : Int = 65536

///|
fn mask_crc(crc : UInt) -> UInt {
  ((crc >> 15) | (crc << 17)) + 0xa282ead8
}

///|
fn write_le24(buf : @buffer.Buffer, v : Int) -> Unit {
  buf.write_byte(v.to_byte())
  buf.write_byte((v >> 8).to_byte())
  buf.write_byte((v >> 16).to_byte())
}

///|

///|
fn read_le24(data : BytesView, at : Int) -> Int {
  data[at].to_int() |
  (data[at + 1].to_int() << 8) |
  (data[at + 2].to_int() << 16)
}

///|
fn snappy_write_varint(out : @buffer.Buffer, value : Int) -> Unit {
  let mut v = value
  while v >= 128 {
    out.write_byte(((v & 0x7f) | 0x80).to_byte())
    v = v >> 7
  }
  out.write_byte(v.to_byte())
}

///|
fn snappy_emit_literal(out : @buffer.Buffer, data : BytesView) -> Unit {
  let len = data.length()
  if len == 0 {
    return
  }
  let n = len - 1
  if n < 60 {
    out.write_byte((n << 2).to_byte())
  } else if n < 256 {
    out.write_byte(60 << 2)
    out.write_byte(n.to_byte())
  } else if n < 65536 {
    out.write_byte(61 << 2)
    out.write_byte(n.to_byte())
    out.write_byte((n >> 8).to_byte())
  } else if n < 16777216 {
    out.write_byte(62 << 2)
    out.write_byte(n.to_byte())
    out.write_byte((n >> 8).to_byte())
    out.write_byte((n >> 16).to_byte())
  } else {
    out.write_byte(63 << 2)
    out.write_byte(n.to_byte())
    out.write_byte((n >> 8).to_byte())
    out.write_byte((n >> 16).to_byte())
    out.write_byte((n >> 24).to_byte())
  }
  out.write_bytes(data)
}

///|
fn snappy_emit_copy(out : @buffer.Buffer, offset : Int, len : Int) -> Unit {
  let mut len = len
  while len > 0 {
    let n = if len > 64 { 64 } else { len }
    // copy with 2-byte offset
    out.write_byte((((n - 1) << 2) | 2).to_byte())
    out.write_byte(offset.to_byte())
    out.write_byte((offset >> 8).to_byte())
    len -= n
  }
}

///|
fn snappy_hash(seq : UInt) -> Int {
  ((seq * 0x1e35a7bd) >> 20).reinterpret_as_int() & 0xfff
}

///|
/// Compress one snappy block (with the varint length preamble).
fn snappy_compress_block(data : BytesView) -> Bytes {
  let out = @buffer.Buffer()
  snappy_write_varint(out, data.length())
  let len = data.length()
  if len == 0 {
    return out.to_bytes()
  }
  let table = FixedArray::make(4096, -1)
  let mut anchor = 0
  let mut pos = 0
  while pos + 4 <= len {
    let seq = read_le32(data, pos)
    let h = snappy_hash(seq)
    let candidate = table[h]
    table[h] = pos
    if candidate >= 0 &&
      pos - candidate <= 65535 &&
      read_le32(data, candidate) == seq {
      let mut match_len = 4
      while pos + match_len < len &&
            data[candidate + match_len] == data[pos + match_len] {
        match_len += 1
      }
      snappy_emit_literal(out, data[anchor:pos])
      snappy_emit_copy(out, pos - candidate, match_len)
      pos += match_len
      anchor = pos
    } else {
      pos += 1
    }
  }
  snappy_emit_literal(out, data[anchor:len])
  out.to_bytes()
}

///|
/// Decompress one snappy block, appending to `out`.
fn snappy_decompress_block(
  data : BytesView,
  out : Array[Byte],
) -> Unit raise PulsarError {
  // preamble: varint uncompressed length
  let mut pos = 0
  let mut shift = 0
  let mut declared = 0
  while true {
    if pos >= data.length() {
      raise InvalidFrame("snappy: truncated length preamble")
    }
    let b = data[pos].to_int()
    pos += 1
    declared = declared | ((b & 0x7f) << shift)
    if (b & 0x80) == 0 {
      break
    }
    shift += 7
    if shift > 28 {
      raise InvalidFrame("snappy: invalid length preamble")
    }
  }
  let start_len = out.length()
  while pos < data.length() {
    let tag = data[pos].to_int()
    pos += 1
    match tag & 3 {
      0 => {
        // literal
        let mut lit_len = tag >> 2
        if lit_len >= 60 {
          let extra = lit_len - 59
          if pos + extra > data.length() {
            raise InvalidFrame("snappy: truncated literal length")
          }
          lit_len = 0
          for i in 0..<extra {
            lit_len = lit_len | (data[pos + i].to_int() << (8 * i))
          }
          pos += extra
          lit_len += 1
        } else {
          lit_len += 1
        }
        if pos + lit_len > data.length() {
          raise InvalidFrame("snappy: truncated literal")
        }
        for i in pos..<(pos + lit_len) {
          out.push(data[i])
        }
        pos += lit_len
      }
      1 => {
        // copy, 1-byte offset suffix
        if pos >= data.length() {
          raise InvalidFrame("snappy: truncated copy1")
        }
        let len = ((tag >> 2) & 7) + 4
        let offset = (tag >> 5 << 8) | data[pos].to_int()
        pos += 1
        snappy_copy_from(out, offset, len)
      }
      2 => {
        // copy, 2-byte offset
        if pos + 2 > data.length() {
          raise InvalidFrame("snappy: truncated copy2")
        }
        let len = (tag >> 2) + 1
        let offset = data[pos].to_int() | (data[pos + 1].to_int() << 8)
        pos += 2
        snappy_copy_from(out, offset, len)
      }
      _ => {
        // copy, 4-byte offset
        if pos + 4 > data.length() {
          raise InvalidFrame("snappy: truncated copy4")
        }
        let len = (tag >> 2) + 1
        let offset = read_le32(data, pos).reinterpret_as_int()
        pos += 4
        snappy_copy_from(out, offset, len)
      }
    }
  }
  if out.length() - start_len != declared {
    raise InvalidFrame("snappy: decompressed length mismatch")
  }
}

///|
fn snappy_copy_from(
  out : Array[Byte],
  offset : Int,
  len : Int,
) -> Unit raise PulsarError {
  if offset <= 0 || offset > out.length() {
    raise InvalidFrame("snappy: invalid copy offset")
  }
  let start = out.length() - offset
  for i in 0..<len {
    out.push(out[start + i])
  }
}

///|
/// Compress `data` into Google's Snappy framed stream format.
pub fn snappy_compress(data : Bytes) -> Bytes {
  let out = @buffer.Buffer()
  // stream identifier chunk
  out.write_byte(0xff)
  write_le24(out, 6)
  out.write_bytes(b"sNaPpY")
  let mut offset = 0
  while offset < data.length() {
    let end = if offset + SNAPPY_FRAMED_CHUNK_MAX > data.length() {
      data.length()
    } else {
      offset + SNAPPY_FRAMED_CHUNK_MAX
    }
    let block = data[offset:end].to_owned()
    let compressed = snappy_compress_block(block)
    let checksum = mask_crc(crc32c(block))
    if compressed.length() >= block.length() {
      // uncompressed chunk
      out.write_byte(0x01)
      write_le24(out, 4 + block.length())
      write_le32(out, checksum)
      out.write_bytes(block)
    } else {
      out.write_byte(0x00)
      write_le24(out, 4 + compressed.length())
      write_le32(out, checksum)
      out.write_bytes(compressed)
    }
    offset = end
  }
  out.to_bytes()
}

///|
/// Decompress Google's Snappy framed stream format.
pub fn snappy_decompress(data : Bytes) -> Bytes raise PulsarError {
  let out : Array[Byte] = []
  let mut pos = 0
  let mut saw_header = false
  while pos < data.length() {
    if pos + 4 > data.length() {
      raise InvalidFrame("snappy stream: truncated chunk header")
    }
    let chunk_type = data[pos].to_int()
    let chunk_len = read_le24(data, pos + 1)
    pos += 4
    if pos + chunk_len > data.length() {
      raise InvalidFrame("snappy stream: truncated chunk")
    }
    let chunk = data[pos:pos + chunk_len]
    pos += chunk_len
    if chunk_type == 0xff {
      if chunk.to_owned() != b"sNaPpY" {
        raise InvalidFrame("snappy stream: bad stream identifier")
      }
      saw_header = true
    } else if chunk_type >= 0x80 && chunk_type <= 0xfe {
      continue // skippable chunk
    } else if chunk_type == 0x00 || chunk_type == 0x01 {
      if chunk_len < 4 {
        raise InvalidFrame("snappy stream: chunk without checksum")
      }
      let want_crc = read_le32(chunk, 0)
      if chunk_type == 0x01 {
        let body = chunk[4:]
        if mask_crc(crc32c(body)) != want_crc {
          raise InvalidFrame("snappy stream: chunk checksum mismatch")
        }
        for b in body {
          out.push(b)
        }
      } else {
        let before = out.length()
        snappy_decompress_block(chunk[4:], out)
        let produced = Bytes::from_array(out[before:])
        if mask_crc(crc32c(produced)) != want_crc {
          raise InvalidFrame("snappy stream: chunk checksum mismatch")
        }
      }
    } else {
      raise InvalidFrame("snappy stream: unknown chunk type")
    }
  }
  if !saw_header {
    raise InvalidFrame("snappy stream: missing stream identifier")
  }
  Bytes::from_array(out)
}
