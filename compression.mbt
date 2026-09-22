// Compression codecs for message payloads.

///|
/// Compression applied to message payloads.
pub(all) enum CompressionCodec {
  None
  Lz4
  Zlib
  Zstd
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
  }
}

///|
fn CompressionCodec::from_proto(
  t : @proto.CompressionType,
) -> CompressionCodec raise PulsarError {
  match t {
    @proto.CompressionType::NONE => None
    @proto.CompressionType::LZ4 => Lz4
    @proto.CompressionType::ZLIB => Zlib
    @proto.CompressionType::ZSTD => Zstd
    @proto.CompressionType::SNAPPY =>
      raise InvalidFrame("snappy compression is not supported")
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
    Lz4 => lz4_compress(payload)
    Zlib => @zlib.zlib_compress(payload)
    Zstd =>
      @zstd.compress(payload) catch {
        e => raise InvalidFrame("zstd compress failed")
      }
  }
}

///|
/// Decompress a payload produced by `compress`.
pub fn decompress(
  codec : CompressionCodec,
  payload : Bytes,
) -> Bytes raise PulsarError {
  match codec {
    None => payload
    Lz4 => lz4_decompress(payload)
    Zlib =>
      @zlib.zlib_decompress(payload) catch {
        e => raise InvalidFrame("zlib decompress failed")
      }
    Zstd =>
      @zstd.decompress(payload) catch {
        e => raise InvalidFrame("zstd decompress failed")
      }
  }
}
