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
  /// Operation attempted on a closed consumer.
  ConsumerClosed
  /// CRC32-C checksum of an incoming message frame did not match.
  ChecksumMismatch
  /// Malformed frame received from the broker.
  InvalidFrame(String)
  /// A request to the broker timed out.
  RequestTimeout
  /// Authentication failed.
  AuthenticationFailed(String)
} derive(Debug)
