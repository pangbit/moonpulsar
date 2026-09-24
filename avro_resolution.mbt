// Resolve a writer Avro datum into the shape required by a reader schema.

///|
fn[T] avro_incompatible(message : String) -> T raise PulsarError {
  raise SchemaIncompatible(message)
}

///|
fn avro_kind(
  root : @avro_schema.Schema,
  schema : @avro_schema.Schema,
) -> @avro_schema.SchemaKind raise PulsarError {
  match schema.kind() {
    Named(name) =>
      match root.resolve_named(name) {
        Some(kind) => kind
        None => avro_incompatible("unresolved Avro name \{name}")
      }
    kind => kind
  }
}

///|
fn avro_name_matches(
  writer : String,
  reader : String,
  reader_aliases : Array[String],
) -> Bool {
  writer == reader || reader_aliases.contains(writer)
}

///|
fn avro_field(
  values : Array[(String, @avro_codec.Datum)],
  name : String,
) -> @avro_codec.Datum? {
  for _, pair in values {
    let (field_name, value) = pair
    if field_name == name {
      return Some(value)
    }
  }
  None
}

///|
fn avro_default_integer(value : Json) -> Int64 raise PulsarError {
  guard value is Number(number, repr~) else {
    return avro_incompatible("Avro integer default must be a number")
  }
  let text = repr.unwrap_or(number.to_string())
  guard text.length() > 0 else {
    return avro_incompatible("empty Avro integer default")
  }
  let negative = text[0] == '-'
  let start = if negative { 1 } else { 0 }
  guard start < text.length() else {
    return avro_incompatible("invalid Avro integer default")
  }
  let limit = if negative {
    -9223372036854775807L - 1L
  } else {
    -9223372036854775807L
  }
  let mut result = 0L
  for index in start..<text.length() {
    let character = text[index]
    guard character >= '0' && character <= '9' else {
      return avro_incompatible("invalid Avro integer default")
    }
    let digit = Int64::from_int(character.to_int() - '0'.to_int())
    guard result >= limit / 10L &&
      (result != limit / 10L || digit <= -(limit % 10L)) else {
      return avro_incompatible("Avro integer default is out of range")
    }
    result = result * 10L - digit
  }
  if negative {
    result
  } else {
    -result
  }
}

///|
fn avro_default_datum(
  root : @avro_schema.Schema,
  schema : @avro_schema.Schema,
  value : Json,
  depth : Int,
) -> @avro_codec.Datum raise PulsarError {
  guard depth < 64 else {
    return avro_incompatible("Avro default nesting exceeds 64")
  }
  match (avro_kind(root, schema), value) {
    (Primitive(Null), Null) => @avro_codec.Datum::Null
    (Primitive(Boolean), True) => @avro_codec.Datum::Boolean(true)
    (Primitive(Boolean), False) => @avro_codec.Datum::Boolean(false)
    (Primitive(Int), Number(_, repr=_)) => {
      let number = avro_default_integer(value)
      guard number >= -2147483648L && number <= 2147483647L else {
        return avro_incompatible("Avro int default is out of range")
      }
      @avro_codec.Datum::Int(number.to_int())
    }
    (Primitive(Long), Number(_, repr=_)) =>
      @avro_codec.Datum::Long(avro_default_integer(value))
    (Primitive(Float), Number(number, repr=_)) =>
      @avro_codec.Datum::Float(Float::from_double(number))
    (Primitive(Double), Number(number, repr=_)) =>
      @avro_codec.Datum::Double(number)
    (Primitive(String), String(text)) => @avro_codec.Datum::String(text)
    (Primitive(Bytes), String(text)) => {
      let bytes : Array[Byte] = []
      for character in text {
        guard character.to_int() <= 255 else {
          return avro_incompatible("Avro bytes default exceeds one byte")
        }
        bytes.push(character.to_int().to_byte())
      }
      @avro_codec.Datum::Bytes(Bytes::from_array(bytes))
    }
    (Union(branches~), _) => {
      guard branches.length() > 0 else {
        return avro_incompatible("empty Avro default union")
      }
      @avro_codec.Datum::Union(
        0,
        avro_default_datum(root, branches[0], value, depth + 1),
      )
    }
    (Array(items~), Array(elements)) =>
      @avro_codec.Datum::Array(
        elements.map(element => {
          avro_default_datum(root, items, element, depth + 1)
        }),
      )
    (Map(values~), Object(entries)) => {
      let result : Map[String, @avro_codec.Datum] = Map([])
      for key, item in entries {
        result[key] = avro_default_datum(root, values, item, depth + 1)
      }
      @avro_codec.Datum::Map(result)
    }
    (Record(name=_, fields~, ..), Object(entries)) => {
      let result : Array[(String, @avro_codec.Datum)] = []
      for field in fields {
        let field_value = match entries.get(field.name()) {
          Some(item) => item
          None =>
            match field.default_value() {
              Some(item) => item
              None =>
                return avro_incompatible(
                  "record default misses \{field.name()}",
                )
            }
        }
        result.push(
          (
            field.name(),
            avro_default_datum(root, field.schema(), field_value, depth + 1),
          ),
        )
      }
      @avro_codec.Datum::Record(result)
    }
    (Enum(name=_, symbols~, ..), String(symbol)) => {
      for index, candidate in symbols {
        if candidate == symbol {
          return @avro_codec.Datum::Enum(index, symbol)
        }
      }
      avro_incompatible("Avro enum default symbol is absent")
    }
    (Fixed(name=_, size~, ..), String(text)) => {
      let bytes = avro_default_datum(
        root,
        @avro_schema.Schema::new(Primitive(Bytes), "bytes"),
        Json::string(text),
        depth + 1,
      )
      guard bytes is @avro_codec.Datum::Bytes(data) && data.length() == size else {
        return avro_incompatible("Avro fixed default size differs")
      }
      @avro_codec.Datum::Fixed(data)
    }
    _ => avro_incompatible("invalid Avro default for \{schema.kind_name()}")
  }
}

///|
fn avro_default(
  reader_root : @avro_schema.Schema,
  field : @avro_schema.Field,
) -> @avro_codec.Datum raise PulsarError {
  guard field.default_value() is Some(value) else {
    return avro_incompatible(
      "reader field \{field.name()} has no writer field or default",
    )
  }
  avro_default_datum(reader_root, field.schema(), value, 0)
}

///|
fn resolve_avro_datum(
  writer_root : @avro_schema.Schema,
  reader_root : @avro_schema.Schema,
  writer : @avro_schema.Schema,
  reader : @avro_schema.Schema,
  datum : @avro_codec.Datum,
  depth : Int,
) -> @avro_codec.Datum raise PulsarError {
  guard depth < 64 else {
    return avro_incompatible("Avro schema nesting exceeds 64")
  }
  let writer_kind = avro_kind(writer_root, writer)
  let reader_kind = avro_kind(reader_root, reader)
  if writer_kind is Union(branches=writer_branches) {
    guard datum is @avro_codec.Datum::Union(index, value) &&
      index >= 0 &&
      index < writer_branches.length() else {
      return avro_incompatible("invalid writer union branch")
    }
    return resolve_avro_datum(
      writer_root,
      reader_root,
      writer_branches[index],
      reader,
      value,
      depth + 1,
    )
  }
  if reader_kind is Union(branches=reader_branches) {
    for index, branch in reader_branches {
      let candidate : Result[@avro_codec.Datum, PulsarError] = Ok(
        resolve_avro_datum(
          writer_root,
          reader_root,
          writer,
          branch,
          datum,
          depth + 1,
        ),
      ) catch {
        e => Err(e)
      }
      if candidate is Ok(value) {
        return @avro_codec.Datum::Union(index, value)
      }
    }
    return avro_incompatible("no compatible reader union branch")
  }
  match (writer_kind, reader_kind, datum) {
    (Primitive(Null), Primitive(Null), @avro_codec.Datum::Null) =>
      @avro_codec.Datum::Null
    (Primitive(Boolean), Primitive(Boolean), @avro_codec.Datum::Boolean(value)) =>
      @avro_codec.Datum::Boolean(value)
    (Primitive(Int), Primitive(Int), @avro_codec.Datum::Int(value)) =>
      @avro_codec.Datum::Int(value)
    (Primitive(Int), Primitive(Long), @avro_codec.Datum::Int(value)) =>
      @avro_codec.Datum::Long(Int64::from_int(value))
    (Primitive(Int), Primitive(Float), @avro_codec.Datum::Int(value)) =>
      @avro_codec.Datum::Float(Float::from_int(value))
    (Primitive(Int), Primitive(Double), @avro_codec.Datum::Int(value)) =>
      @avro_codec.Datum::Double(value.to_double())
    (Primitive(Long), Primitive(Long), @avro_codec.Datum::Long(value)) =>
      @avro_codec.Datum::Long(value)
    (Primitive(Long), Primitive(Float), @avro_codec.Datum::Long(value)) =>
      @avro_codec.Datum::Float(Float::from_double(value.to_double()))
    (Primitive(Long), Primitive(Double), @avro_codec.Datum::Long(value)) =>
      @avro_codec.Datum::Double(value.to_double())
    (Primitive(Float), Primitive(Float), @avro_codec.Datum::Float(value)) =>
      @avro_codec.Datum::Float(value)
    (Primitive(Float), Primitive(Double), @avro_codec.Datum::Float(value)) =>
      @avro_codec.Datum::Double(value.to_double())
    (Primitive(Double), Primitive(Double), @avro_codec.Datum::Double(value)) =>
      @avro_codec.Datum::Double(value)
    (Primitive(Bytes), Primitive(Bytes), @avro_codec.Datum::Bytes(value)) =>
      @avro_codec.Datum::Bytes(value)
    (Primitive(String), Primitive(String), @avro_codec.Datum::String(value)) =>
      @avro_codec.Datum::String(value)
    (Primitive(String), Primitive(Bytes), @avro_codec.Datum::String(value)) =>
      @avro_codec.Datum::Bytes(@utf8.encode(value))
    (Primitive(Bytes), Primitive(String), @avro_codec.Datum::Bytes(value)) => {
      let text = @utf8.decode(value) catch {
        _ => return avro_incompatible("writer bytes are not valid UTF-8")
      }
      @avro_codec.Datum::String(text)
    }
    (
      Record(name=writer_name, fields=writer_fields, ..),
      Record(name=reader_name, aliases=reader_aliases, fields=reader_fields, ..),
      @avro_codec.Datum::Record(values),
    ) => {
      guard avro_name_matches(writer_name, reader_name, reader_aliases) else {
        return avro_incompatible(
          "record \{writer_name} does not match \{reader_name}",
        )
      }
      let result : Array[(String, @avro_codec.Datum)] = []
      for reader_field in reader_fields {
        let mut writer_field : @avro_schema.Field? = None
        for field in writer_fields {
          if avro_name_matches(
              field.name(),
              reader_field.name(),
              reader_field.aliases(),
            ) {
            writer_field = Some(field)
            break
          }
        }
        let value = match writer_field {
          Some(field) =>
            match avro_field(values, field.name()) {
              Some(value) =>
                resolve_avro_datum(
                  writer_root,
                  reader_root,
                  field.schema(),
                  reader_field.schema(),
                  value,
                  depth + 1,
                )
              None =>
                avro_incompatible("writer field \{field.name()} is missing")
            }
          None => avro_default(reader_root, reader_field)
        }
        result.push((reader_field.name(), value))
      }
      @avro_codec.Datum::Record(result)
    }
    (
      Array(items=writer_items),
      Array(items=reader_items),
      @avro_codec.Datum::Array(items),
    ) =>
      @avro_codec.Datum::Array(
        items.map(item => {
          resolve_avro_datum(
            writer_root,
            reader_root,
            writer_items,
            reader_items,
            item,
            depth + 1,
          )
        }),
      )
    (
      Map(values=writer_values),
      Map(values=reader_values),
      @avro_codec.Datum::Map(values),
    ) => {
      let result : Map[String, @avro_codec.Datum] = Map([])
      for key, item in values {
        result[key] = resolve_avro_datum(
          writer_root,
          reader_root,
          writer_values,
          reader_values,
          item,
          depth + 1,
        )
      }
      @avro_codec.Datum::Map(result)
    }
    (
      Enum(name=writer_name, ..),
      Enum(
        name=reader_name,
        aliases=reader_aliases,
        symbols=reader_symbols,
        default_symbol~,
        ..
      ),
      @avro_codec.Datum::Enum(_, symbol),
    ) => {
      guard avro_name_matches(writer_name, reader_name, reader_aliases) else {
        return avro_incompatible(
          "enum \{writer_name} does not match \{reader_name}",
        )
      }
      let selected = if reader_symbols.contains(symbol) {
        symbol
      } else {
        match default_symbol {
          Some(fallback) => fallback
          None =>
            return avro_incompatible("reader enum has no symbol \{symbol}")
        }
      }
      for index, candidate in reader_symbols {
        if candidate == selected {
          return @avro_codec.Datum::Enum(index, selected)
        }
      }
      avro_incompatible("reader enum default symbol is invalid")
    }
    (
      Fixed(name=writer_name, size=writer_size, ..),
      Fixed(name=reader_name, aliases=reader_aliases, size=reader_size, ..),
      @avro_codec.Datum::Fixed(value),
    ) => {
      guard writer_size == reader_size &&
        avro_name_matches(writer_name, reader_name, reader_aliases) else {
        return avro_incompatible("fixed schema name or size differs")
      }
      @avro_codec.Datum::Fixed(value)
    }
    _ =>
      avro_incompatible(
        "writer \{writer.kind_name()} is not compatible with reader \{reader.kind_name()}",
      )
  }
}

///|
fn avro_version_key(topic : String, version : Bytes) -> String {
  topic +
  "/" +
  version.to_array().map(fn(byte) { byte.to_int().to_string() }).join(".")
}

///|
/// Decode a message using its broker-published writer schema version and a
/// typed reader codec. The parsed writer schema is cached per topic/version.
pub async fn[T] Client::decode_avro(
  self : Client,
  message : Message,
  reader : SchemaCodec[T],
) -> T {
  guard reader.info().type_ == @proto.Schema_Type::Avro else {
    raise SchemaIncompatible("reader codec is not Avro")
  }
  let version = match message.schema_version() {
    Some(version) => version
    None => raise SchemaIncompatible("message has no Avro schema version")
  }
  let key = avro_version_key(message.topic(), version)
  let writer = match self.avro_writers.get(key) {
    Some(schema) => schema
    None => {
      let registered = self.get_schema(message.topic(), version~)
      if registered.version is Some(returned) && returned != version {
        raise UnexpectedResponse("GET_SCHEMA returned a different version")
      }
      guard registered.info.type_ == @proto.Schema_Type::Avro else {
        raise SchemaIncompatible("writer schema is not Avro")
      }
      let text = @utf8.decode(registered.info.schema_data) catch {
        _ => raise SchemaIncompatible("writer schema is not UTF-8")
      }
      let schema = @avro_schema.parse(text) catch {
        _ => raise SchemaIncompatible("writer schema is invalid")
      }
      self.avro_writers[key] = schema
      schema
    }
  }
  let reader_text = @utf8.decode(reader.info().schema_data) catch {
    _ => raise SchemaIncompatible("reader schema is not UTF-8")
  }
  let reader_schema = @avro_schema.parse(reader_text) catch {
    _ => raise SchemaIncompatible("reader schema is invalid")
  }
  let datum = @avro_codec.decode(writer, message.payload()) catch {
    _ => raise InvalidFrame("invalid Avro payload for writer schema")
  }
  let resolved = resolve_avro_datum(
    writer, reader_schema, writer, reader_schema, datum, 0,
  )
  let encoded = @avro_codec.encode(reader_schema, resolved) catch {
    _ => raise SchemaIncompatible("resolved datum does not match reader schema")
  }
  reader.decode(encoded)
}
