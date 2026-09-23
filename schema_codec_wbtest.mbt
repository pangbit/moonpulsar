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
