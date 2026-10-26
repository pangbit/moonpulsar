// Test-only framing. Payload and receipt frames are never re-encoded.

///|
suberror FixtureError {
  Rejected(String)
} derive(Debug)

///|
const MAX_FRAME : Int = 65536

///|
fn loopback_endpoint(value : String) -> String raise {
  guard value.strip_prefix("127.0.0.1:") is Some(port) else {
    raise FixtureError::Rejected("only literal 127.0.0.1 endpoints are allowed")
  }
  let number = @string.parse_int(port)
  guard number > 0 && number <= 65535 && number.to_string() == port.to_owned() else {
    raise FixtureError::Rejected(
      "endpoint must contain one canonical nonzero decimal port",
    )
  }
  value
}

///|
fn valid_nonce(value : String) -> Bool {
  if value.length() != 32 {
    return false
  }
  for char in value.iter() {
    if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
      return false
    }
  }
  true
}

///|
fn join_bytes(left : Bytes, right : Bytes) -> Bytes {
  Bytes::makei(left.length() + right.length(), i => {
    if i < left.length() {
      left[i]
    } else {
      right[i - left.length()]
    }
  })
}

///|
fn total_size(raw : Bytes) -> Int raise FixtureError {
  guard raw.length() >= 4 else {
    raise FixtureError::Rejected("truncated frame prefix")
  }
  let size = (raw[0].to_uint() << 24) |
    (raw[1].to_uint() << 16) |
    (raw[2].to_uint() << 8) |
    raw[3].to_uint()
  guard size >= 4U && size <= 65536U else {
    raise FixtureError::Rejected("frame outside the 4..65536 byte body bound")
  }
  size.reinterpret_as_int()
}

///|
struct FrameDecoder {
  mut pending : Bytes
}

///|
fn FrameDecoder::new() -> FrameDecoder {
  { pending: b"", }
}

///|
fn FrameDecoder::feed(self : FrameDecoder, chunk : Bytes) -> Array[Bytes] raise {
  guard chunk.length() <= MAX_FRAME + 4 &&
    self.pending.length() + chunk.length() <= 2 * (MAX_FRAME + 4) else {
    raise FixtureError::Rejected("bounded frame accumulator overflow")
  }
  self.pending = join_bytes(self.pending, chunk)
  let frames = []
  while self.pending.length() >= 4 {
    let size = total_size(self.pending)
    if self.pending.length() < size + 4 {
      break
    }
    frames.push(self.pending[:size + 4].to_owned())
    self.pending = self.pending[size + 4:].to_owned()
  }
  frames
}

///|
fn FrameDecoder::finish(self : FrameDecoder) -> Unit raise FixtureError {
  if self.pending.length() != 0 {
    raise FixtureError::Rejected("EOF inside a framed record")
  }
}

///|
fn decode_raw(raw : Bytes) -> @pulsar.IncomingFrame raise {
  guard raw.length() == total_size(raw) + 4 else {
    raise FixtureError::Rejected("raw record must contain exactly one frame")
  }
  @pulsar.decode_frame_body(raw[4:].to_owned())
}

///|
fn lookup_to_frontend(raw : Bytes, frontend : String) -> Bytes raise {
  ignore(loopback_endpoint(frontend))
  let frame = decode_raw(raw)
  if frame.cmd.lookup_topic_response is Some(response) {
    guard frame.cmd.type_ is @proto.BaseCommand_Type::LOOKUP_RESPONSE else {
      raise FixtureError::Rejected("LOOKUP field disagrees with command type")
    }
    if response.response
      is Some(@proto.CommandLookupTopicResponse_LookupType::Failed) {
      return raw
    }
    guard response.broker_service_url is Some(_) else {
      raise FixtureError::Rejected("successful LOOKUP has no plain broker URL")
    }
    // Preserve request id, response kind, authority, errors and TLS URL.
    // Only LOOKUP is re-encoded. SEND/receipt frames use original raw bytes.
    response.broker_service_url = Some("pulsar://" + frontend)
    @pulsar.encode_command_frame(frame.cmd)
  } else {
    raw
  }
}
