///|
test "legacy Protobuf schema keeps its definition and uses protobuf payloads" {
  let definition = b"{\"type\":\"record\",\"name\":\"KeyValue\",\"namespace\":\"pulsar.proto\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}"
  let codec : SchemaCodec[@proto.KeyValue] = SchemaCodec::protobuf(definition)
  assert_eq(codec.info(), SchemaInfo::protobuf(definition))
  let value = @proto.KeyValue::KeyValue("name", "moonpulsar")
  assert_eq(codec.encode(value), b"\x0a\x04name\x12\x0amoonpulsar")
  assert_eq(codec.decode(codec.encode(value)), value)
  let bad : Result[@proto.KeyValue, Error] = Ok(codec.decode(b"\x0a\x05hi")) catch {
    e => Err(e)
  }
  assert_true(bad is Err(InvalidFrame(_)))
}
