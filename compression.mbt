// Compression codecs for message payloads.

///|
/// Compression applied to message payloads.
pub(all) enum CompressionCodec {
  None
  Lz4
  Zlib
  Zstd
  Snappy
} derive(Eq, Debug)

///|
fn CompressionCodec::to_proto(
  self : CompressionCodec,
) -> @proto.CompressionType? {
  match self {
    None => None
    Lz4 => Some(@proto.CompressionType::LZ4)
    Zlib => Some(@proto.CompressionType::ZLIB)
    Zstd => Some(@proto.CompressionType::ZSTD)
    Snappy => Some(@proto.CompressionType::SNAPPY)
  }
}

///|
fn CompressionCodec::from_proto(t : @proto.CompressionType) -> CompressionCodec {
  match t {
    @proto.CompressionType::NONE => None
    @proto.CompressionType::LZ4 => Lz4
    @proto.CompressionType::ZLIB => Zlib
    @proto.CompressionType::ZSTD => Zstd
    @proto.CompressionType::SNAPPY => Snappy
  }
}

///|
/// Compress `payload` with the codec; `None` returns the input unchanged.
pub fn compress(
  codec : CompressionCodec,
  payload : Bytes,
) -> Bytes raise PulsarError {
  match codec {
    None => payload
    Lz4 => lz4_compress_block(payload)
    Zlib => @zlib.zlib_compress(payload)
    Zstd =>
      @zstd.compress(payload) catch {
        _ => raise InvalidFrame("zstd compress failed")
      }
    Snappy => snappy_compress_block(payload)
  }
}

///|
/// Decompress a payload produced by `compress`.
pub fn decompress(
  codec : CompressionCodec,
  payload : Bytes,
  uncompressed_size? : Int,
) -> Bytes raise PulsarError {
  match codec {
    None => payload
    Lz4 =>
      match uncompressed_size {
        Some(size) => lz4_decompress_block_to(payload, size)
        None => raise InvalidFrame("lz4 payload without uncompressed_size")
      }
    Zlib => {
      // A sync flush always ends in LEN=0/NLEN=65535. This is only a fast
      // dispatch hint: the scanner proves the boundary before accepting it.
      if uncompressed_size is Some(size) &&
        size > 0 &&
        size <= MAX_CHUNKED_MESSAGE_BYTES &&
        payload.length() >= 4 &&
        payload[payload.length() - 4:] == b"\x00\x00\xff\xff" {
        try {
          return zlib_sync_flush(payload, size)
        } catch {
          _ => () // It may instead be a complete stream with this checksum.
        }
      }
      @zlib.zlib_decompress(payload) catch {
        _ => raise InvalidFrame("zlib decompress failed")
      }
    }
    Zstd =>
      @zstd.decompress(payload) catch {
        _ => raise InvalidFrame("zstd decompress failed")
      }
    Snappy => {
      let out : Array[Byte] = []
      snappy_decompress_block(payload, out)
      Bytes::from_array(out)
    }
  }
}
