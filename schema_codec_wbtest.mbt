///|
test "string schema codec roundtrips Unicode and rejects malformed UTF-8" {
  let codec = SchemaCodec::string()
  assert_eq(codec.info(), SchemaInfo::string())
  assert_eq(codec.decode(codec.encode("你好 Pulsar")), "你好 Pulsar")
  let invalid : Result[String, Error] = Ok(codec.decode(b"\xff")) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
}

///|
test "JSON schema codec roundtrips values and rejects invalid payloads" {
  let codec : SchemaCodec[Map[String, Int]] = SchemaCodec::json(
    b"{\"type\":\"record\",\"name\":\"Count\",\"fields\":[{\"name\":\"count\",\"type\":\"int\"}]}",
  )
  assert_eq(codec.info().type_, @proto.Schema_Type::Json)
  let value : Map[String, Int] = { "count": 7 }
  assert_eq(codec.decode(codec.encode(value)), value)
  let invalid_json : Result[Map[String, Int], Error] = Ok(codec.decode(b"{")) catch {
    e => Err(e)
  }
  assert_true(invalid_json is Err(InvalidFrame(_)))
  let invalid_utf8 : Result[Map[String, Int], Error] = Ok(codec.decode(b"\xff")) catch {
    e => Err(e)
  }
  assert_true(invalid_utf8 is Err(InvalidFrame(_)))
  let wrong_type : Result[Map[String, Int], Error] = Ok(
    codec.decode(b"{\"count\":\"seven\"}"),
  ) catch {
    e => Err(e)
  }
  assert_true(wrong_type is Err(InvalidFrame(_)))
}

///|
test "custom schema codec uses supplied conversion" {
  let codec = SchemaCodec::new(
    SchemaInfo::bytes(),
    fn(value : String) { @utf8.encode("prefix:" + value) },
    fn(payload : Bytes) {
      let text = @utf8.decode(payload) catch {
        _ => raise InvalidFrame("invalid payload")
      }
      match text.strip_prefix("prefix:") {
        Some(value) => value.to_owned()
        None => raise InvalidFrame("missing prefix")
      }
    },
  )
  assert_eq(codec.decode(codec.encode("value")), "value")
  let invalid : Result[String, Error] = Ok(codec.decode(b"value")) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(InvalidFrame(_)))
}

///|
test "primitive schema codecs use Pulsar network byte order" {
  let bytes_codec = SchemaCodec::bytes()
  assert_eq(bytes_codec.decode(bytes_codec.encode(b"raw")), b"raw")
  let int8_codec = SchemaCodec::int8()
  assert_eq(int8_codec.encode(-1), b"\xff")
  assert_eq(int8_codec.decode(b"\x80"), -128)
  let int16_codec = SchemaCodec::int16()
  assert_eq(int16_codec.encode(-2), b"\xff\xfe")
  assert_eq(int16_codec.decode(b"\x80\x00"), -32768)
  let int32_codec = SchemaCodec::int32()
  assert_eq(int32_codec.encode(0x01020304), b"\x01\x02\x03\x04")
  assert_eq(int32_codec.decode(b"\xff\xff\xff\xff"), -1)
  let int64_codec = SchemaCodec::int64()
  assert_eq(int64_codec.encode(-1L), b"\xff\xff\xff\xff\xff\xff\xff\xff")
  assert_eq(int64_codec.decode(b"\x00\x00\x00\x00\x00\x00\x00\x01"), 1L)
  let float_codec = SchemaCodec::float()
  assert_eq(float_codec.encode(1.5F), b"\x3f\xc0\x00\x00")
  assert_eq(float_codec.decode(b"\x3f\xc0\x00\x00"), 1.5F)
  let double_codec = SchemaCodec::double()
  assert_eq(double_codec.encode(1.5), b"\x3f\xf8\x00\x00\x00\x00\x00\x00")
  assert_eq(double_codec.decode(b"\x3f\xf8\x00\x00\x00\x00\x00\x00"), 1.5)
}

///|
test "primitive codecs reject out-of-range values and malformed payloads" {
  let int8_codec = SchemaCodec::int8()
  let too_large : Result[Bytes, Error] = Ok(int8_codec.encode(128)) catch {
    e => Err(e)
  }
  assert_true(too_large is Err(InvalidFrame(_)))
  let int16_codec = SchemaCodec::int16()
  let too_small : Result[Bytes, Error] = Ok(int16_codec.encode(-32769)) catch {
    e => Err(e)
  }
  assert_true(too_small is Err(InvalidFrame(_)))
  let wrong_length : Result[Int, Error] = Ok(
    SchemaCodec::int32().decode(b"\x00"),
  ) catch {
    e => Err(e)
  }
  assert_true(wrong_length is Err(InvalidFrame(_)))
  let wrong_double : Result[Double, Error] = Ok(
    SchemaCodec::double().decode(b"\x00"),
  ) catch {
    e => Err(e)
  }
  assert_true(wrong_double is Err(InvalidFrame(_)))
}

///|
test "integer codecs support Go client little-endian wire format" {
  let int16 = SchemaCodec::int16(little_endian=true)
  assert_eq(int16.encode(0x0102), b"\x02\x01")
  assert_eq(int16.decode(b"\xfe\xff"), -2)
  let int32 = SchemaCodec::int32(little_endian=true)
  assert_eq(int32.encode(0x01020304), b"\x04\x03\x02\x01")
  assert_eq(int32.decode(b"\xff\xff\xff\xff"), -1)
  let int64 = SchemaCodec::int64(little_endian=true)
  assert_eq(
    int64.encode(0x0102030405060708L),
    b"\x08\x07\x06\x05\x04\x03\x02\x01",
  )
  assert_eq(int64.decode(b"\xfe\xff\xff\xff\xff\xff\xff\xff"), -2L)
}
