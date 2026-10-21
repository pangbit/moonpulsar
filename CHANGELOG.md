# Changelog

## 0.1.2 — Unreleased

- Reduce the CRC32-C dependency chain with portable slicing-by-eight and independent polynomial regression tests.
- Preserve receive-window capacity across reconnects with buffered messages.
- Wake blocked receivers before waiting for the Broker's close response.
- Reconnect and replay unconfirmed producer frames after a Broker `CLOSE_PRODUCER` notification, including Topic unloads.
- Add a bounded recovery example that records per-message send and delivery events. Unknown sends remain unresolved unless delivery is observed; no end-to-end exactly-once guarantee is implied.
- Release closed-handle callbacks and failed initial-connection resources; prevent closed handles from reconnecting and release terminal chunk buffers.
- End a producer's own memory-admission waits on close and bound blocked sends/replays by their original deadline. Cancellation after a write starts retires that transport; other handles sharing it follow their existing failure or reconnect policy.
- Retire silent connections after bounded keepalive probes, including blocked PING writes; complete incoming commands clear the pending probe.
- Use the module's 0.1.2 identity consistently in CONNECT and AUTH_RESPONSE. Keep internal maintenance plans and raw evidence out of the published archive.

The public API is unchanged. This is a maintenance candidate, not a publication record. See [development and verification](docs/testing.md) for reproducible checks and [candidate verification](docs/verification-0.1.2.md) for source-specific evidence boundaries. SEND timeouts can leave broker acceptance unknown; the existing user-supplied sequence-ID limitation remains documented in the client guide.

## 0.1.1 — 2026-09-29

- Fix residual OpenSSL errors from failed TLS setup affecting other connections on the same event-loop thread. This caused intermittent TLS failures and test timeouts.
- Add a deterministic regression test for missing private-key error isolation.
- Clarify build requirements, feature-specific runtime dependencies, and test/example prerequisites in both READMEs.

The public API is unchanged. These notes describe the release candidate; confirm availability in the Mooncakes registry before installation.

## 0.1.0 — 2026-09-29

Initial version notes, prepared for the scheduled release date above. Confirm actual availability in the Mooncakes registry; these notes alone do not establish that publication succeeded.

- Native MoonBit Pulsar client with connection pooling, lookup, reconnect, producers, consumers and partition discovery.
- Batching, compression, chunking, flow control, ACK/redelivery, retry and dead-letter handling.
- Readers, TableView, transactions, schema codecs and selected Admin REST operations.
- Token, OAuth2, Athenz and TLS authentication; RSA/ECIES message encryption.
- Local mock tests, isolated Broker interoperability checks and recorded performance comparisons.

Targets the native backend. A C compiler and SDK/libc are required; enhanced TLS and ECIES load OpenSSL 3 at runtime. Windows remains unverified. Full Admin REST coverage and end-to-end exactly-once behavior are not promised.

See the [capability matrix](docs/capabilities.md) for verified scope, the [client guide](docs/client-guide.md) for interoperability limits, and [performance documentation](docs/performance.md) for measurement boundaries. Historical reports do not certify every later commit.
