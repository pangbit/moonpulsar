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
