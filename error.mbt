// Error types for the Pulsar client.

///|
/// Errors raised by the Pulsar client.
pub(all) suberror PulsarError {
  /// The connection to the broker was closed.
  ConnectionClosed
  /// Failed to establish the TCP connection or protocol handshake.
  ConnectFailed(String)
  /// The broker answered a request with a `CommandError`.
  ServerError(code~ : Int, message~ : String)
  /// The broker answered with an unexpected command.
  UnexpectedResponse(String)
  /// Topic lookup failed.
  LookupFailed(String)
  /// Topic lookup exceeded the maximum number of redirects.
  TooManyRedirects
  /// Operation attempted on a closed producer.
  ProducerClosed
  /// The configured producer pending-message limit has been reached.
  ProducerQueueFull
  /// The client's pending payload-byte limit has been reached.
  ClientMemoryFull
  /// Operation attempted on a closed consumer.
  ConsumerClosed
  /// CRC32-C checksum of an incoming message frame did not match.
  ChecksumMismatch
  /// Malformed frame received from the broker.
  InvalidFrame(String)
  /// The writer's Avro schema cannot be read with the requested reader schema.
  SchemaIncompatible(String)
  /// A request to the broker timed out.
  RequestTimeout
  /// Authentication failed.
  AuthenticationFailed(String)
} derive(Debug)

///|
pub impl Show for PulsarError with fn output(self, logger) {
  let text = match self {
    ConnectionClosed => "connection closed"
    ConnectFailed(addr) => "connect failed: \{addr}"
    ServerError(code~, message~) => "broker error \{code}: \{message}"
    UnexpectedResponse(what) => "unexpected response: \{what}"
    LookupFailed(msg) => "lookup failed: \{msg}"
    TooManyRedirects => "too many lookup redirects"
    ProducerClosed => "producer closed"
    ProducerQueueFull => "producer pending-message queue is full"
    ClientMemoryFull => "client pending payload-byte limit is full"
    ConsumerClosed => "consumer closed"
    ChecksumMismatch => "message checksum mismatch"
    InvalidFrame(msg) => "invalid frame: \{msg}"
    SchemaIncompatible(msg) => "incompatible schema: \{msg}"
    RequestTimeout => "request timeout"
    AuthenticationFailed(msg) => "authentication failed: \{msg}"
  }
  logger.write_string(text)
}
