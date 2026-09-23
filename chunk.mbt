// Reassemble interleaved chunked messages before decompression.

///|
const MAX_PENDING_CHUNKED_MESSAGES : Int = 100

///|
const MAX_CHUNKED_MESSAGE_BYTES : Int = 67108864

///|
const MAX_PENDING_CHUNKED_BYTES : Int = 67108864

///|
const CHUNK_EXPIRY_MS : Int64 = 60000L

///|
priv struct ChunkAssembly {
  expected : Int
  total_size : Int
  next_index : Int
  size : Int
  payload : @buffer.Buffer
  ids : Array[MessageId]
  started_at : Int64
}

///|
struct ChunkResult {
  payload : Bytes?
  ids : Array[MessageId]
  is_chunk : Bool
}

///|
struct ChunkAssembler {
  pending : Map[String, ChunkAssembly]
  mut buffered_bytes : Int
}

///|
fn ChunkAssembler::new() -> ChunkAssembler {
  { pending: {}, buffered_bytes: 0, }
}

///|
fn ChunkAssembler::clear(self : ChunkAssembler) -> Unit {
  self.pending.clear()
  self.buffered_bytes = 0
}

///|
fn ChunkAssembler::discard(self : ChunkAssembler, uuid : String) -> Unit {
  if self.pending.get(uuid) is Some(part) {
    self.buffered_bytes -= part.size
    self.pending.remove(uuid)
  }
}

///|
fn ChunkAssembler::evict_oldest(self : ChunkAssembler) -> Unit {
  let mut oldest_key = ""
  let mut oldest_time = @async.now()
  for key, part in self.pending {
    if oldest_key == "" || part.started_at < oldest_time {
      oldest_key = key
      oldest_time = part.started_at
    }
  }
  if oldest_key != "" {
    self.discard(oldest_key)
  }
}

///|
fn ChunkAssembler::accept(
  self : ChunkAssembler,
  metadata : @proto.MessageMetadata,
  id : MessageId,
  bytes : Bytes,
) -> ChunkResult {
  let count = metadata.num_chunks_from_msg.unwrap_or(0)
  if count <= 1 {
    return { payload: Some(bytes), ids: [id], is_chunk: false, }
  }
  let empty : ChunkResult = { payload: None, ids: [], is_chunk: true, }
  let expired = []
  for key, part in self.pending {
    if @async.now() - part.started_at >= CHUNK_EXPIRY_MS {
      expired.push(key)
    }
  }
  for key in expired {
    self.discard(key)
  }
  guard metadata.uuid is Some(uuid) &&
    metadata.chunk_id is Some(index) &&
    metadata.total_chunk_msg_size is Some(total) else {
    return empty
  }
  if count > 10000 ||
    total <= 0 ||
    total > MAX_CHUNKED_MESSAGE_BYTES ||
    index < 0 ||
    index >= count {
    self.discard(uuid)
    return empty
  }
  if index == 0 {
    self.discard(uuid)
    if self.pending.size() >= MAX_PENDING_CHUNKED_MESSAGES &&
      !self.pending.contains(uuid) {
      self.evict_oldest()
    }
    self.pending[uuid] = {
      expected: count,
      total_size: total,
      next_index: 0,
      size: 0,
      payload: @buffer.Buffer(),
      ids: [],
      started_at: @async.now(),
    }
  }
  guard self.pending.get(uuid) is Some(part) else { return empty }
  if part.expected != count ||
    part.total_size != total ||
    part.next_index != index ||
    part.size + bytes.length() > total {
    self.discard(uuid)
    return empty
  }
  while self.buffered_bytes + bytes.length() > MAX_PENDING_CHUNKED_BYTES &&
        !self.pending.is_empty() {
    self.evict_oldest()
  }
  if !self.pending.contains(uuid) {
    return empty
  }
  part.payload.write_bytes(bytes)
  self.buffered_bytes += bytes.length()
  part.ids.push(id)
  let updated : ChunkAssembly = {
    ..part,
    next_index: index + 1,
    size: part.size + bytes.length(),
  }
  if index + 1 < count {
    self.pending[uuid] = updated
    return empty
  }
  self.pending[uuid] = updated
  self.discard(uuid)
  if updated.size != total {
    return empty
  }
  {
    payload: Some(updated.payload.to_bytes()),
    ids: updated.ids,
    is_chunk: true,
  }
}
