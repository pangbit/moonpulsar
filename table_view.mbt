// A live key-value view of a compacted topic. Values are raw payload bytes.

///|
pub struct TableView {
  priv reader : Reader
  priv data : Map[String, Bytes]
  priv listeners : Array[(String, Bytes?) -> Unit]
  priv closed : Ref[Bool]
}

///|
async fn TableView::create(
  group : @async.TaskGroup[Unit],
  reader : Reader,
) -> TableView {
  let view : TableView = {
    reader,
    data: {},
    listeners: [],
    closed: { val: false, },
  }
  while reader.has_message_available() {
    match reader.next() {
      Some(message) => view.apply(message)
      None => break
    }
  }
  group.spawn_bg(
    async fn() {
      while !view.closed.val {
        let message = reader.next() catch { _ => break }
        if message is Some(value) && !view.closed.val {
          view.apply(value)
        }
      }
    },
    no_wait=true,
    allow_failure=true,
  )
  view
}

///|
fn TableView::apply(self : TableView, message : Message) -> Unit {
  guard message.key() is Some(key) else { return }
  let value : Bytes? = if message.payload().length() == 0 {
    self.data.remove(key)
    None
  } else {
    let payload = message.payload()
    self.data[key] = payload
    Some(payload)
  }
  for listener in self.listeners {
    listener(key, value)
  }
}

///|
pub fn TableView::size(self : TableView) -> Int {
  self.data.length()
}

///|
pub fn TableView::is_empty(self : TableView) -> Bool {
  self.data.is_empty()
}

///|
pub fn TableView::contains_key(self : TableView, key : String) -> Bool {
  self.data.contains(key)
}

///|
pub fn TableView::get(self : TableView, key : String) -> Bytes? {
  self.data.get(key)
}

///|
/// Return a separate map so callers cannot mutate the view itself.
pub fn TableView::entries(self : TableView) -> Map[String, Bytes] {
  let copy : Map[String, Bytes] = Map([])
  for key, value in self.data {
    copy[key] = value
  }
  copy
}

///|
pub fn TableView::keys(self : TableView) -> Array[String] {
  let keys = []
  for key, _ in self.data {
    keys.push(key)
  }
  keys
}

///|
pub fn TableView::for_each(
  self : TableView,
  action : (String, Bytes) -> Unit,
) -> Unit {
  for key, value in self.data {
    action(key, value)
  }
}

///|
/// Replay the current entries, then call `action` for future updates.
/// Deletions are delivered as `None`.
pub fn TableView::for_each_and_listen(
  self : TableView,
  action : (String, Bytes?) -> Unit,
) -> Unit {
  for key, value in self.data {
    action(key, Some(value))
  }
  self.listeners.push(action)
}

///|
pub async fn TableView::close(self : TableView) -> Unit {
  if !self.closed.val {
    self.closed.val = true
    self.reader.close()
  }
}

///|
/// A TableView that decodes values with a declared Pulsar schema.
/// Tombstones remain `None`; malformed values raise on access and are passed
/// to listeners as errors so a live update cannot silently replace a value.
pub struct TypedTableView[T] {
  priv raw : TableView
  priv codec : SchemaCodec[T]
}

///|
fn[T] TypedTableView::wrap(
  raw : TableView,
  codec : SchemaCodec[T],
) -> TypedTableView[T] {
  { raw, codec, }
}

///|
pub fn[T] TypedTableView::size(self : TypedTableView[T]) -> Int {
  self.raw.size()
}

///|
pub fn[T] TypedTableView::is_empty(self : TypedTableView[T]) -> Bool {
  self.raw.is_empty()
}

///|
pub fn[T] TypedTableView::contains_key(
  self : TypedTableView[T],
  key : String,
) -> Bool {
  self.raw.contains_key(key)
}

///|
pub fn[T] TypedTableView::get(
  self : TypedTableView[T],
  key : String,
) -> T? raise PulsarError {
  match self.raw.get(key) {
    Some(payload) => Some(self.codec.decode(payload))
    None => None
  }
}

///|
fn[T] TypedTableView::decode_optional(
  self : TypedTableView[T],
  value : Bytes?,
) -> T? raise PulsarError {
  match value {
    Some(payload) => Some(self.codec.decode(payload))
    None => None
  }
}

///|
pub fn[T] TypedTableView::keys(self : TypedTableView[T]) -> Array[String] {
  self.raw.keys()
}

///|
pub fn[T] TypedTableView::entries(
  self : TypedTableView[T],
) -> Map[String, T] raise PulsarError {
  let result : Map[String, T] = {}
  for key, payload in self.raw.entries() {
    result[key] = self.codec.decode(payload)
  }
  result
}

///|
pub fn[T] TypedTableView::for_each(
  self : TypedTableView[T],
  action : (String, T) -> Unit,
) -> Unit raise PulsarError {
  for key, value in self.entries() {
    action(key, value)
  }
}

///|
/// Replay current entries, then report future values and tombstones.
/// Decode failures are delivered as `Err`; the entry remains stored and
/// `get` raises the same error until a valid update or tombstone arrives.
pub fn[T] TypedTableView::for_each_and_listen(
  self : TypedTableView[T],
  action : (String, Result[T?, PulsarError]) -> Unit,
) -> Unit {
  self.raw.for_each_and_listen(fn(key, value) {
    let decoded : Result[T?, PulsarError] = Ok(self.decode_optional(value)) catch {
      e => Err(e)
    }
    action(key, decoded)
  })
}

///|
pub async fn[T] TypedTableView::close(self : TypedTableView[T]) -> Unit {
  self.raw.close()
}
