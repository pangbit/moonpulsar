// Topic schema declaration, mirroring the `Schema` message used in
// PRODUCER and SUBSCRIBE commands.

///|
/// A topic schema as declared to the broker.
pub(all) struct SchemaInfo {
  name : String
  type_ : @proto.Schema_Type
  schema_data : Bytes
  properties : Map[String, String]
} derive(Eq, Debug)

///|
fn SchemaInfo::to_proto(self : SchemaInfo) -> @proto.Schema {
  let schema = @proto.Schema::default()
  schema.name = self.name
  schema.schema_data = self.schema_data
  schema.type_ = self.type_
  schema.properties = key_values_of(self.properties)
  schema
}

///|
fn SchemaInfo::from_proto(schema : @proto.Schema) -> SchemaInfo {
  {
    name: schema.name,
    type_: schema.type_,
    schema_data: schema.schema_data,
    properties: properties_of(schema.properties),
  }
}

///|
/// A schema returned by the broker, with its registry version when present.
pub(all) struct VersionedSchema {
  info : SchemaInfo
  version : Bytes?
} derive(Eq, Debug)

///|
/// Built-in UTF-8 string schema.
pub fn SchemaInfo::string() -> SchemaInfo {
  {
    name: "String",
    type_: @proto.Schema_Type::String,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
/// JSON schema with the given JSON-Schema definition.
pub fn SchemaInfo::json(definition : Bytes) -> SchemaInfo {
  {
    name: "Json",
    type_: @proto.Schema_Type::Json,
    schema_data: definition,
    properties: Map([]),
  }
}

///|
/// Raw bytes schema (no conversion).
pub fn SchemaInfo::bytes() -> SchemaInfo {
  {
    name: "Bytes",
    type_: @proto.Schema_Type::None,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
pub fn SchemaInfo::int8() -> SchemaInfo {
  {
    name: "INT8",
    type_: @proto.Schema_Type::Int8,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
pub fn SchemaInfo::int16() -> SchemaInfo {
  {
    name: "INT16",
    type_: @proto.Schema_Type::Int16,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
pub fn SchemaInfo::int32() -> SchemaInfo {
  {
    name: "INT32",
    type_: @proto.Schema_Type::Int32,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
pub fn SchemaInfo::int64() -> SchemaInfo {
  {
    name: "INT64",
    type_: @proto.Schema_Type::Int64,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
pub fn SchemaInfo::float() -> SchemaInfo {
  {
    name: "FLOAT",
    type_: @proto.Schema_Type::Float,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
pub fn SchemaInfo::double() -> SchemaInfo {
  {
    name: "DOUBLE",
    type_: @proto.Schema_Type::Double,
    schema_data: b"",
    properties: Map([]),
  }
}

///|
/// Avro schema with the given JSON definition.
pub fn SchemaInfo::avro(definition : Bytes) -> SchemaInfo {
  {
    name: "Avro",
    type_: @proto.Schema_Type::Avro,
    schema_data: definition,
    properties: Map([]),
  }
}

///|
/// Protobuf schema with the given descriptor definition.
pub fn SchemaInfo::protobuf(definition : Bytes) -> SchemaInfo {
  {
    name: "Protobuf",
    type_: @proto.Schema_Type::Protobuf,
    schema_data: definition,
    properties: Map([]),
  }
}
