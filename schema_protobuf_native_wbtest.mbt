///|
fn key_value_descriptor_set() -> Bytes {
  @lib.base64_decode(
    "CmwKKHRlc3RkYXRhL3Byb3RvYnVmX25hdGl2ZV9rZXlfdmFsdWUucHJvdG8SDHB1bHNhci5wcm90byIyCghLZXlWYWx1ZRIQCgNrZXkYASACKAlSA2tleRIUCgV2YWx1ZRgCIAIoCVIFdmFsdWU=",
  ) catch {
    _ => b""
  }
}

///|
test "Protobuf Native codec emits interoperable schema metadata and bytes" {
  let descriptor = key_value_descriptor_set()
  let codec : SchemaCodec[@proto.KeyValue] = SchemaCodec::protobuf_native(
    descriptor, "testdata/protobuf_native_key_value.proto", "pulsar.proto.KeyValue",
  )
  let info = codec.info()
  assert_eq(info.name, "ProtoNative")
  assert_eq(info.type_, @proto.Schema_Type::ProtobufNative)
  let metadata = @json.parse(@utf8.decode(info.schema_data))
  guard metadata is Object(fields) else { fail("expected schema JSON object") }
  assert_eq(
    fields.get("fileDescriptorSet"),
    Some(Json::string(@lib.base64_encode(descriptor))),
  )
  assert_eq(
    fields.get("rootFileDescriptorName"),
    Some(Json::string("testdata/protobuf_native_key_value.proto")),
  )
  assert_eq(
    fields.get("rootMessageTypeName"),
    Some(Json::string("pulsar.proto.KeyValue")),
  )
  let value = @proto.KeyValue::KeyValue("name", "moonpulsar")
  assert_eq(codec.encode(value), b"\x0a\x04name\x12\x0amoonpulsar")
  assert_eq(codec.decode(codec.encode(value)), value)
  let bad : Result[@proto.KeyValue, Error] = Ok(codec.decode(b"\x0a\x05hi")) catch {
    e => Err(e)
  }
  assert_true(bad is Err(InvalidFrame(_)))
}

///|
test "Protobuf Native schema rejects invalid descriptors and missing roots" {
  let invalid : Result[SchemaInfo, Error] = Ok(
    SchemaInfo::protobuf_native(b"\xff", "x.proto", "demo.Record"),
  ) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
  let missing_file : Result[SchemaInfo, Error] = Ok(
    SchemaInfo::protobuf_native(
      key_value_descriptor_set(),
      "wrong.proto",
      "pulsar.proto.KeyValue",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(missing_file is Err(InvalidFrame(_)))
  let missing_message : Result[SchemaInfo, Error] = Ok(
    SchemaInfo::protobuf_native(
      key_value_descriptor_set(),
      "testdata/protobuf_native_key_value.proto",
      "pulsar.proto.Other",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(missing_message is Err(InvalidFrame(_)))
}
