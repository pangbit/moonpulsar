// LZ4 frame format codec (the variant Pulsar clients exchange, matching
// lz4-java / pierrec-lz4 defaults): 64KB blocks, block independence,
// content checksum, no block checksum, no content size.

///|
const LZ4_FRAME_MAGIC : UInt = 0x184d2204

///|
/// 64KB block maximum (BD code 4).
const LZ4_BLOCK_SIZE : Int = 65536

///|
fn write_le32(buf : @buffer.Buffer, v : UInt) -> Unit {
  buf.write_byte(v.to_byte())
  buf.write_byte((v >> 8).to_byte())
  buf.write_byte((v >> 16).to_byte())
  buf.write_byte((v >> 24).to_byte())
}

///|
fn lz4_hash(seq : UInt) -> Int {
  // multiplicative hash, 12-bit table
  ((seq * 2654435761) >> 20).reinterpret_as_int() & 0xfff
}

///|
fn write_len(out : @buffer.Buffer, len : Int) -> Unit {
  let mut rem = len
  while rem >= 255 {
    out.write_byte(255)
    rem -= 255
  }
  out.write_byte(rem.to_byte())
}

///|
/// Compress one LZ4 block (raw block format, no frame header).
fn lz4_compress_block(data : BytesView) -> Bytes {
  let len = data.length()
  let out = @buffer.Buffer()
  if len == 0 {
    return b""
  }
  let table = FixedArray::make(4096, -1)
  let mut anchor = 0
  let mut pos = 0
  while pos + 4 <= len {
    let seq = read_le32(data, pos)
    let h = lz4_hash(seq)
    let candidate = table[h]
    table[h] = pos
    if candidate >= 0 &&
      pos - candidate <= 65535 &&
      read_le32(data, candidate) == seq {
      // extend the match
      let mut match_len = 4
      while pos + match_len < len &&
            data[candidate + match_len] == data[pos + match_len] {
        match_len += 1
      }
      let lit_len = pos - anchor
      let ml = match_len - 4
      let token = ((if lit_len >= 15 { 15 } else { lit_len }) << 4) |
        (if ml >= 15 { 15 } else { ml })
      out.write_byte(token.to_byte())
      if lit_len >= 15 {
        write_len(out, lit_len - 15)
      }
      for i in anchor..<pos {
        out.write_byte(data[i])
      }
      let offset = pos - candidate
      out.write_byte(offset.to_byte())
      out.write_byte((offset >> 8).to_byte())
      if ml >= 15 {
        write_len(out, ml - 15)
      }
      pos += match_len
      anchor = pos
      // keep the hash table warm near the new position
      if pos + 2 + 4 <= len {
        table[lz4_hash(read_le32(data, pos - 2))] = pos - 2
      }
    } else {
      pos += 1
    }
  }
  // trailing literals
  let lit_len = len - anchor
  out.write_byte(((if lit_len >= 15 { 15 } else { lit_len }) << 4).to_byte())
  if lit_len >= 15 {
    write_len(out, lit_len - 15)
  }
  for i in anchor..<len {
    out.write_byte(data[i])
  }
  out.to_bytes()
}

///|
/// Decompress one raw LZ4 block, appending to `out`; `limit` bounds the
/// total output size.
fn lz4_decompress_block(
  data : BytesView,
  out : Array[Byte],
  limit : Int,
) -> Unit raise PulsarError {
  let len = data.length()
  let mut pos = 0
  while pos < len {
    let token = data[pos].to_int()
    pos += 1
    let mut lit_len = token >> 4
    if lit_len == 15 {
      while true {
        if pos >= len {
          raise InvalidFrame("lz4: truncated literal length")
        }
        let b = data[pos].to_int()
        pos += 1
        lit_len += b
        if b != 255 {
          break
        }
      }
    }
    if pos + lit_len > len {
      raise InvalidFrame("lz4: truncated literals")
    }
    for i in pos..<(pos + lit_len) {
      out.push(data[i])
    }
    pos += lit_len
    if pos >= len {
      break
    }
    if pos + 2 > len {
      raise InvalidFrame("lz4: truncated offset")
    }
    let offset = data[pos].to_int() | (data[pos + 1].to_int() << 8)
    pos += 2
    if offset == 0 || offset > out.length() {
      raise InvalidFrame("lz4: invalid match offset")
    }
    let mut match_len = (token & 0x0f) + 4
    if (token & 0x0f) == 15 {
      while true {
        if pos >= len {
          raise InvalidFrame("lz4: truncated match length")
        }
        let b = data[pos].to_int()
        pos += 1
        match_len += b
        if b != 255 {
          break
        }
      }
    }
    if out.length() + match_len > limit {
      raise InvalidFrame("lz4: decompressed size exceeds limit")
    }
    let start = out.length() - offset
    for i in 0..<match_len {
      out.push(out[start + i])
    }
  }
}

///|
/// Compress `data` as an LZ4 frame.
pub fn lz4_compress(data : Bytes) -> Bytes {
  let out = @buffer.Buffer()
  write_le32(out, LZ4_FRAME_MAGIC)
  // FLG: version 01, block independence, content checksum -> 0b01100100
  let flg : Byte = 0x64
  // BD: max block 64KB -> 0x40
  let bd : Byte = 0x40
  out.write_byte(flg)
  out.write_byte(bd)
  let hc = (xxhash32(b"\x64\x40") >> 8) & 0xff
  out.write_byte(hc.to_byte())
  let mut offset = 0
  while offset < data.length() {
    let end = if offset + LZ4_BLOCK_SIZE > data.length() {
      data.length()
    } else {
      offset + LZ4_BLOCK_SIZE
    }
    let block = data[offset:end]
    let compressed = lz4_compress_block(block)
    if compressed.length() >= block.length() {
      // store uncompressed
      write_le32(out, block.length().reinterpret_as_uint() | 0x80000000)
      out.write_bytes(block.to_owned())
    } else {
      write_le32(out, compressed.length().reinterpret_as_uint())
      out.write_bytes(compressed)
    }
    offset = end
  }
  write_le32(out, 0) // EndMark
  write_le32(out, xxhash32(data))
  out.to_bytes()
}

///|
/// Decompress an LZ4 frame.
pub fn lz4_decompress(data : Bytes) -> Bytes raise PulsarError {
  if data.length() < 7 {
    raise InvalidFrame("lz4 frame too short")
  }
  if read_le32(data, 0) != LZ4_FRAME_MAGIC {
    raise InvalidFrame("bad lz4 frame magic")
  }
  let flg = data[4].to_int()
  let block_checksum = ((flg >> 4) & 1) == 1
  let content_size = ((flg >> 3) & 1) == 1
  let content_checksum = ((flg >> 2) & 1) == 1
  let dict_id = (flg & 1) == 1
  let mut pos = 6 // magic + FLG + BD
  if content_size {
    pos += 8
  }
  if dict_id {
    pos += 4
  }
  pos += 1 // HC (not verified)
  if pos > data.length() {
    raise InvalidFrame("lz4 frame header truncated")
  }
  let out : Array[Byte] = []
  while true {
    if pos + 4 > data.length() {
      raise InvalidFrame("lz4 frame truncated in block table")
    }
    let size = read_le32(data, pos).reinterpret_as_int()
    pos += 4
    if size == 0 {
      break
    }
    let uncompressed = (size & (1 << 31)) != 0
    let block_len = size & 0x7fffffff
    if pos + block_len > data.length() {
      raise InvalidFrame("lz4 frame block truncated")
    }
    let block = data[pos:pos + block_len]
    pos += block_len
    if uncompressed {
      for b in block {
        out.push(b)
      }
    } else {
      lz4_decompress_block(block, out, 64 * 1024 * 1024)
    }
    if block_checksum {
      pos += 4
    }
  }
  if content_checksum {
    if pos + 4 > data.length() {
      raise InvalidFrame("lz4 frame truncated at checksum")
    }
    let want = read_le32(data, pos)
    if xxhash32(Bytes::from_array(out)) != want {
      raise InvalidFrame("lz4 content checksum mismatch")
    }
  }
  Bytes::from_array(out)
}
