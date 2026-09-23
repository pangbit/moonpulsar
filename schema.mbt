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
