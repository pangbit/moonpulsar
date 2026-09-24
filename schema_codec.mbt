// Payload conversion paired with the schema declared to the broker.

///|
/// Converts values to and from Pulsar message payloads. Pass `info()` to
/// producer/consumer creation, then use `encode` and `decode` for messages.
pub struct SchemaCodec[T] {
  priv info : SchemaInfo
  priv encode_value : (T) -> Bytes raise PulsarError
  priv decode_value : (Bytes) -> T raise PulsarError
}

///|
/// Use a custom payload codec with its broker schema declaration.
pub fn[T] SchemaCodec::new(
  info : SchemaInfo,
  encode : (T) -> Bytes raise PulsarError,
  decode : (Bytes) -> T raise PulsarError,
) -> SchemaCodec[T] {
  { info, encode_value: encode, decode_value: decode, }
}

///|
/// UTF-8 string payloads with Pulsar's STRING schema.
pub fn SchemaCodec::string() -> SchemaCodec[String] {
  SchemaCodec::new(SchemaInfo::string(), fn(value) { @utf8.encode(value) }, fn(
    payload,
  ) {
    @utf8.decode(payload) catch {
      _ => raise InvalidFrame("invalid UTF-8 string payload")
    }
  })
}

///|
pub fn SchemaCodec::bytes() -> SchemaCodec[Bytes] {
  SchemaCodec::new(SchemaInfo::bytes(), fn(value) { value }, fn(payload) {
    payload
  })
}

///|
/// INT8 values use `Int` because MoonBit has no signed eight-bit scalar.
pub fn SchemaCodec::int8() -> SchemaCodec[Int] {
  SchemaCodec::new(
    SchemaInfo::int8(),
    fn(value) {
      guard value >= -128 && value <= 127 else {
        raise InvalidFrame("INT8 value out of range")
      }
      Bytes::from_array([value.to_byte()])
    },
    fn(payload) {
      guard payload.length() == 1 else {
        raise InvalidFrame("INT8 payload must be one byte")
      }
      let value = payload[0].to_int()
      if value < 128 {
        value
      } else {
        value - 256
      }
    },
  )
}

///|
pub fn SchemaCodec::int16() -> SchemaCodec[Int] {
  SchemaCodec::new(
    SchemaInfo::int16(),
    fn(value) {
      guard value >= -32768 && value <= 32767 else {
        raise InvalidFrame("INT16 value out of range")
      }
      Bytes::from_array([(value >> 8).to_byte(), value.to_byte()])
    },
    fn(payload) {
      guard payload.length() == 2 else {
        raise InvalidFrame("INT16 payload must be two bytes")
      }
      let value = (payload[0].to_int() << 8) | payload[1].to_int()
      if value < 32768 {
        value
      } else {
        value - 65536
      }
    },
  )
}

///|
pub fn SchemaCodec::int32() -> SchemaCodec[Int] {
  SchemaCodec::new(
    SchemaInfo::int32(),
    fn(value) {
      let buf = @buffer.Buffer()
      write_be32(buf, value.reinterpret_as_uint())
      buf.to_bytes()
    },
    fn(payload) {
      guard payload.length() == 4 else {
        raise InvalidFrame("INT32 payload must be four bytes")
      }
      read_be32(payload, 0).reinterpret_as_int()
    },
  )
}

///|
pub fn SchemaCodec::int64() -> SchemaCodec[Int64] {
  SchemaCodec::new(
    SchemaInfo::int64(),
    fn(value) { value.reinterpret_as_uint64().to_be_bytes() },
    fn(payload) {
      guard payload.length() == 8 else {
        raise InvalidFrame("INT64 payload must be eight bytes")
      }
      let mut value = 0UL
      for byte in payload {
        value = (value << 8) | byte.to_uint64()
      }
      value.reinterpret_as_int64()
    },
  )
}

///|
pub fn SchemaCodec::float() -> SchemaCodec[Float] {
  SchemaCodec::new(SchemaInfo::float(), fn(value) { value.to_be_bytes() }, fn(
    payload,
  ) {
    guard payload.length() == 4 else {
      raise InvalidFrame("FLOAT payload must be four bytes")
    }
    Float::reinterpret_from_uint(read_be32(payload, 0))
  })
}

///|
pub fn SchemaCodec::double() -> SchemaCodec[Double] {
  SchemaCodec::new(
    SchemaInfo::double(),
    fn(value) { value.reinterpret_as_uint64().to_be_bytes() },
    fn(payload) {
      guard payload.length() == 8 else {
        raise InvalidFrame("DOUBLE payload must be eight bytes")
      }
      let mut bits = 0UL
      for byte in payload {
        bits = (bits << 8) | byte.to_uint64()
      }
      bits.reinterpret_as_double()
    },
  )
}

///|
/// JSON payloads for a MoonBit type with JSON conversion support. Supply
/// the broker's JSON schema definition separately from the value encoding.
pub fn[T : ToJson + FromJson] SchemaCodec::json(
  definition : Bytes,
) -> SchemaCodec[T] {
  SchemaCodec::new(
    SchemaInfo::json(definition),
    fn(value) { @utf8.encode(value.to_json().stringify()) },
    fn(payload) {
      let text = @utf8.decode(payload) catch {
        _ => raise InvalidFrame("invalid UTF-8 JSON payload")
      }
      let json = @json.parse(text) catch {
        _ => raise InvalidFrame("invalid JSON payload")
      }
      @json.from_json(json) catch {
        _ => raise InvalidFrame("JSON payload does not match the target type")
      }
    },
  )
}

///|
/// Use a generated MoonBit protobuf message with a Protobuf Native schema.
/// The descriptor set must describe `M`. Decoder errors are reported as
/// `InvalidFrame`.
pub fn[M : @lib.Read + @lib.Write] SchemaCodec::protobuf_native(
  descriptor_set : Bytes,
  root_file : String,
  root_message : String,
) -> SchemaCodec[M] raise PulsarError {
  let info = SchemaInfo::protobuf_native(
    descriptor_set, root_file, root_message,
  )
  SchemaCodec::new(
    info,
    fn(value) {
      let buffer = @buffer.Buffer()
      value.write(buffer) catch {
        _ => raise InvalidFrame("could not encode Protobuf Native payload")
      }
      buffer.to_bytes()
    },
    fn(payload) {
      let reader = @lib.BytesReader::from_bytes(payload)
      M::read_with_limit(@lib.LimitedReader(reader)) catch {
        _ => raise InvalidFrame("invalid Protobuf Native payload")
      }
    },
  )
}

///|
/// Schema declaration for `Client::create_producer` or
/// `Client::create_consumer`.
pub fn[T] SchemaCodec::info(self : SchemaCodec[T]) -> SchemaInfo {
  self.info
}

///|
/// Encode a value into a message payload.
pub fn[T] SchemaCodec::encode(
  self : SchemaCodec[T],
  value : T,
) -> Bytes raise PulsarError {
  (self.encode_value)(value)
}

///|
/// Decode a payload into a typed value.
pub fn[T] SchemaCodec::decode(
  self : SchemaCodec[T],
  payload : Bytes,
) -> T raise PulsarError {
  (self.decode_value)(payload)
}

///|
/// Decode this message's payload with a schema codec. The original message
/// remains available for metadata and acknowledgement.
pub fn[T] Message::decode(
  self : Message,
  codec : SchemaCodec[T],
) -> T raise PulsarError {
  codec.decode(self.payload())
}
