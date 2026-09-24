// Reassemble interleaved chunked messages before decompression.

///|
const MAX_PENDING_CHUNKED_MESSAGES : Int = 100

///|
const MAX_CHUNKED_MESSAGE_BYTES : Int = 67108864

///|
const MAX_PENDING_CHUNKED_BYTES : Int = 67108864

///|
const CHUNK_EXPIRY_MS : Int = 60000

///|
/// Limits for incomplete chunked messages buffered by a reader or consumer.
pub struct ChunkAssemblyPolicy {
  priv expiry_ms : Int
  priv max_pending_messages : Int
  priv auto_ack_incomplete : Bool
}

///|
pub fn ChunkAssemblyPolicy::new(
  expiry_ms? : Int = CHUNK_EXPIRY_MS,
  max_pending_messages? : Int = MAX_PENDING_CHUNKED_MESSAGES,
  auto_ack_incomplete? : Bool = false,
) -> ChunkAssemblyPolicy raise PulsarError {
  guard expiry_ms > 0 && max_pending_messages > 0 else {
    raise InvalidFrame("chunk expiry and pending limit must be positive")
  }
  { expiry_ms, max_pending_messages, auto_ack_incomplete, }
}

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
priv struct ChunkResult {
  payload : Bytes?
  ids : Array[MessageId]
  is_chunk : Bool
}

///|
struct ChunkAssembler {
  pending : Map[String, ChunkAssembly]
  mut buffered_bytes : Int
  mut evicted_ids : Array[MessageId]
  policy : ChunkAssemblyPolicy
}

///|
fn ChunkAssembler::new(policy? : ChunkAssemblyPolicy) -> ChunkAssembler {
  {
    pending: {},
    buffered_bytes: 0,
    evicted_ids: [],
    policy: policy.unwrap_or({
      expiry_ms: CHUNK_EXPIRY_MS,
      max_pending_messages: MAX_PENDING_CHUNKED_MESSAGES,
      auto_ack_incomplete: false,
    }),
  }
}

///|
fn ChunkAssembler::clear(self : ChunkAssembler) -> Unit {
  self.pending.clear()
  self.buffered_bytes = 0
  self.evicted_ids = []
}

///|
fn ChunkAssembler::discard(
  self : ChunkAssembler,
  uuid : String,
  acknowledge? : Bool = true,
) -> Unit {
  if self.pending.get(uuid) is Some(part) {
    self.buffered_bytes -= part.size
    self.pending.remove(uuid)
    if acknowledge && self.policy.auto_ack_incomplete {
      for id in part.ids {
        self.evicted_ids.push(id)
      }
    }
  }
}

///|
fn ChunkAssembler::take_evicted(self : ChunkAssembler) -> Array[MessageId] {
  let ids = self.evicted_ids
  self.evicted_ids = []
  ids
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
fn ChunkAssembler::evict_expired(self : ChunkAssembler) -> Unit {
  let expired = []
  let now = @async.now()
  for key, part in self.pending {
    if now - part.started_at >= self.policy.expiry_ms.to_int64() {
      expired.push(key)
    }
  }
  for key in expired {
    self.discard(key)
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
  self.evict_expired()
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
    // A repeated first chunk can be a redelivery of the same broker entry.
    self.discard(uuid, acknowledge=false)
    if self.pending.length() >= self.policy.max_pending_messages &&
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
  let complete = updated.size == total
  self.discard(uuid, acknowledge=!complete)
  if !complete {
    return empty
  }
  {
    payload: Some(updated.payload.to_bytes()),
    ids: updated.ids,
    is_chunk: true,
  }
}
