///|
struct AvroTestRecord {
  id : Int
  name : String
} derive(Eq, Debug)

///|
pub extend AvroTestRecord with Eq::{not_equal, equal}

///|
pub extend AvroTestRecord with @moonbitlang/core/debug.Debug::{to_repr}

///|
fn avro_record_codec() -> SchemaCodec[AvroTestRecord] raise PulsarError {
  SchemaCodec::avro(
    b"{\"type\":\"record\",\"name\":\"AvroTestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}",
    fn(value) {
      @avro_codec.Datum::Record([
        ("id", @avro_codec.Datum::Int(value.id)),
        ("name", @avro_codec.Datum::String(value.name)),
      ])
    },
    datum => {
      match datum {
        @avro_codec.Datum::Record(
          [
            ("id", @avro_codec.Datum::Int(id)),
            ("name", @avro_codec.Datum::String(name)),
          ]
        ) => { id, name, }
        _ => raise InvalidFrame("Avro record has unexpected fields")
      }
    },
  )
}

///|
test "typed Avro codec uses schema order and binary datum format" {
  let codec = avro_record_codec()
  let value = AvroTestRecord::{ id: 27, name: "moon", }
  assert_eq(codec.info().type_, @proto.Schema_Type::Avro)
  assert_eq(codec.encode(value), b"\x36\x08moon")
  assert_eq(codec.decode(b"\x36\x08moon"), value)
}

///|
test "Avro codec rejects invalid schema, invalid datum, and truncated payload" {
  let bad_schema : Result[SchemaCodec[@avro_codec.Datum], Error] = Ok(
    SchemaCodec::avro_datum(b"{not-json"),
  ) catch {
    e => Err(e)
  }
  assert_true(bad_schema is Err(InvalidFrame(_)))
  let codec = avro_record_codec()
  let truncated : Result[AvroTestRecord, Error] = Ok(
    codec.decode(b"\x36\x08mo"),
  ) catch {
    e => Err(e)
  }
  assert_true(truncated is Err(InvalidFrame(_)))
  let datum_codec = SchemaCodec::avro_datum(codec.info().schema_data)
  let missing_field : Result[Bytes, Error] = Ok(
    datum_codec.encode(
      @avro_codec.Datum::Record([("id", @avro_codec.Datum::Int(27))]),
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(missing_field is Err(InvalidFrame(_)))
}

///|
test "Avro datum codec handles collections, union, enum, and fixed values" {
  let codec = SchemaCodec::avro_datum(
    b"{\"type\":\"record\",\"name\":\"Complex\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"tags\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"choice\",\"type\":[\"null\",\"int\"]},{\"name\":\"status\",\"type\":{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"NEW\",\"DONE\"]}},{\"name\":\"hash\",\"type\":{\"type\":\"fixed\",\"name\":\"Hash\",\"size\":4}}]}",
  )
  let datum = @avro_codec.Datum::Record([
    (
      "items",
      @avro_codec.Datum::Array([
        @avro_codec.Datum::Long(1L),
        @avro_codec.Datum::Long(-2L),
      ]),
    ),
    (
      "tags",
      @avro_codec.Datum::Map(Map([("a", @avro_codec.Datum::String("x"))])),
    ),
    ("choice", @avro_codec.Datum::Union(1, @avro_codec.Datum::Int(5))),
    ("status", @avro_codec.Datum::Enum(1, "DONE")),
    ("hash", @avro_codec.Datum::Fixed(b"\x01\x02\x03\x04")),
  ])
  assert_eq(codec.decode(codec.encode(datum)), datum)
  let bad_union : Result[Bytes, Error] = Ok(
    codec.encode(
      @avro_codec.Datum::Record([
        ("items", @avro_codec.Datum::Array([@avro_codec.Datum::Long(1L)])),
        ("tags", @avro_codec.Datum::Map(Map([]))),
        ("choice", @avro_codec.Datum::Union(2, @avro_codec.Datum::Int(5))),
        ("status", @avro_codec.Datum::Enum(1, "DONE")),
        ("hash", @avro_codec.Datum::Fixed(b"\x01\x02\x03\x04")),
      ]),
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(bad_union is Err(InvalidFrame(_)))
}
