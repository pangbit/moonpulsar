// Regression test: a real frame captured from a Pulsar 4.2.4 broker
// (python client producing an LZ4 message). Layout: magic + CRC32-C +
// metadata size + MessageMetadata + raw LZ4 payload; no broker entry
// metadata because we never advertise `supports_broker_entry_metadata`.

///|
test "decode real broker frame" {
  let body = Bytes::from_array(
    [
      0, 0, 0, 23, 8, 9, 74, 19, 8, 3, 18, 15, 8, 126, 16, 0, 24, 255, 255, 255,
      255, 255, 255, 255, 255, 255, 1, 14, 1, 76, 246, 51, 10, 0, 0, 0, 31, 10, 16,
      115, 116, 97, 110, 100, 97, 108, 111, 110, 101, 45, 55, 56, 45, 49, 48, 16,
      0, 24, 226, 228, 244, 221, 140, 52, 64, 1, 72, 20, 240, 5, 111, 102, 102, 105,
      99, 105, 97, 108, 45, 108, 122, 52, 45, 112, 97, 121, 108, 111, 97, 100,
    ].map(fn(i) { i.to_byte() }),
  )
  let frame = decode_frame_body(body)
  guard frame.metadata is Some(m) else { fail("missing metadata") }
  assert_eq(m.producer_name, "standalone-78-10")
  assert_eq(m.compression, Some(@proto.CompressionType::LZ4))
  assert_eq(m.uncompressed_size, Some(20))
  guard frame.payload is Some(p) else { fail("missing payload") }
  assert_eq(decompress(Lz4, p, uncompressed_size=20), b"official-lz4-payload")
}
