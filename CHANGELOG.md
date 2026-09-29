# Changelog

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
