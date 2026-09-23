# Pulsar client capability and test matrix

Compared with the public APIs of [pulsar-rs](https://github.com/streamnative/pulsar-rs)
and [pulsar-client-go](https://github.com/apache/pulsar-client-go). This is a
functional comparison, not an API-by-API claim of parity. Status reflects the
repository on 2026-09-23.

| Capability | Status | Verification |
| --- | --- | --- |
| TCP handshake, lookup, pooling, keepalive, reconnection | Implemented | Mock broker; prior real-broker restart scenario |
| Client configuration | Partial: connection/operation timeouts, keepalive, listener and lookup properties, reconnect backoff/attempts, per-broker pool sizing and idle eviction; client-wide memory limit open | Mock timeout, LOOKUP wire, invalid retry policy, exhausted producer/consumer reconnect, pool bound/replacement and active-handler-aware idle eviction; two-connection live suite on 4.2.4 and 3.3.9 |
| TLS service URL and custom PEM CA | Implemented, limited live verification | TLS mock validates a private CA, rejects an untrusted certificate, and reconnects with the same CA; runnable TLS example; excluded from the token-authenticated live run |
| Binary-protocol token and basic auth | Implemented | Token: live broker; basic: unit test; Admin REST supports bearer token |
| Auth challenge refresh | Implemented for data refresh | Handshake and post-handshake mock tests; challenge-specific algorithms remain open |
| Async auth provider and rotating token file | Implemented | Local file rotation and broker challenge mock tests; no live token rotation run yet |
| Producer send, async receipt, batching, compression, delayed delivery | Implemented | Mock broker and prior live broker scenarios |
| Producer pending queue and send timeout | Per-producer pending-message limit, fail-fast or blocking admission, and in-flight SEND timeout implemented; byte-based memory cap and batching queue-time timeout open | Mock full/release, blocking, timeout/late receipt, send failure, batch count, close wakeup and invalid options; bounded sequential sends on 4.2.4 and 3.3.9 |
| Producer access modes, metadata, reconnect epoch, and last confirmed sequence | Implemented | Mock wire, broker-history sequence, wait-ready, and reconnect tests; live exclusive conflict and wait-for-exclusive takeover |
| Producer chunking and compressed chunking | Explicit or broker-limit-driven automatic chunking implemented; oversize frames fail before write | Wire-size and failure-path mocks; 6 MiB auto-chunk roundtrip on Pulsar 4.2.4 and 3.3.9 |
| Partition routing and producer expansion | Key hash, round-robin, single-partition, and custom callback routing implemented for initially partitioned topics, including one-partition topics | Mock routing and invalid-index tests, one-to-two and live two-to-three partition expansion; custom routing on 4.2.4 and 3.3.9 |
| Single, partitioned, and explicit multi-topic consumption | Implemented; initially partitioned consumers discover added partitions | Mock one-to-two expansion and bounded slow-consumer forwarding; live two-to-three expansion |
| KeyShared auto-split and sticky hash ranges | Implemented | Range validation and SUBSCRIBE wire mock; sticky policy live broker |
| Pattern subscriptions | Topic additions and removals implemented | Addition: mock and live broker; removal and stale-queue filtering: mock |
| Consumer options, ack/nack, ack timeout, seek, unsubscribe | ACK can optionally await a broker response; ACK grouping, batch-index ACK and zero/auto-sized receive queues remain open | Mock ACK success/failure/protocol-gate and pending-deadline tests; confirmed ACK on 4.2.4 and 3.3.9; ack timeout and ID seek live |
| Exponential negative-ack backoff | Implemented for `Message::nack` | Timing and close-cancellation mock tests; live broker redelivery |
| Chunk reassembly and ACK | Implemented with bounded memory and configurable expiry/pending count; optional ACK of evicted incomplete chunks open | Mock wire and periodic-expiry tests for consumer/reader; live consumer/reader replay |
| Reader, seek, replay, and added-partition discovery | Implemented for initially partitioned topics; custom name/properties/subscription, last-message ID, inclusive start and seek | Mock inclusive/exclusive and empty-latest paths, one-to-two expansion, SUBSCRIBE metadata, last-ID query, bounded slow-reader forwarding, chunk expiry, and disconnect wakeup; live explicit/latest inclusive on 4.2.4 and 3.3.9, two-to-three expansion |
| Transaction coordinator lookup and transaction lifecycle | Implemented | Mock tests and live coordinator/abort; prior live commit/read-back |
| Admin topic and subscription operations | Selected endpoints implemented, including partitioned-topic list/stats, topic properties, and subscription create/skip/expire | Mock HTTP path, body, JSON-shape, and invalid-input tests; live create/expand/query/stats/delete, topic property updates, and subscription cursor/backlog behavior |
| Schema declaration, versions, and typed payload conversion | STRING, JSON, BYTES and integer/float primitive codecs plus custom callbacks; broker schema/version lookup and message version propagation implemented | Local wire-byte, range, malformed-payload, GET_SCHEMA error and version tests; STRING, JSON, INT64 and schema-version broker roundtrips on 4.2.4 and 3.3.9. Official-client primitive interop and Avro/Protobuf typed conversion remain open |
| Dead-letter policy | Implemented for consumer delivery and negative ack; producer created when the consumer is created | Mock publish-before-ACK, send-failure NACK, direct consumer NACK, composite non-blocking receive, and live broker explicit-NACK routing (including a broker that reports redelivery count zero) |
| Retry-letter topic policy | Implemented through explicit `create_retry_consumer` or `retry_topic` on single, multi-topic and pattern constructors | Mock retry then DLQ publish-before-ACK, send-failure NACK, and invalid options; live Shared delayed retry/DLQ and all three ordinary constructors on 4.2.4 and 3.3.9 |
| TableView (raw and typed payload values) | Implemented with schema declaration and decode errors for typed views | Mock typed snapshot, malformed update/snapshot and incompatible schema; raw initial replay/live updates/empty topic/query failure; typed INT32 on 4.2.4 and 3.3.9, raw update/tombstone and added partition live |
| In-place conversion of a non-partitioned topic to a partitioned topic | Broker does not support this on the tested Pulsar 4.2.4 instance | An isolated Admin REST probe returned HTTP 409 (`This topic already exists`); existing clients can discover added partitions of an already partitioned topic |
| OAuth2, Athenz, TLS client-certificate authentication | OAuth2 client-credentials and external Athenz role-token supplier/file providers implemented; Athenz ZTS key/certificate flow and TLS client certificates open | OAuth2 form encoding, fresh token retrieval, rejected/malformed token endpoint, and broker challenge covered by local HTTP and mock broker; local OAuth2 token endpoint to authenticated Pulsar 4.2.4 send/receive passed. Athenz role-token refresh, file rotation and invalid token covered by mock; no Athenz broker run. The current `moonbitlang/async/tls` client API does not expose a client certificate |
| Encryption, interceptors, tracing, metrics | Open | No implementation or coverage |
| Full Admin REST surface | Outside the data-client parity target | Only listed topic/subscription endpoints are implemented |
| Broker version compatibility | Current live regression suite passes on 4.2.4 and 3.3.9 | 4.2.4 test container and isolated 3.3.9 container with transaction coordinator enabled; both exercised `examples/live_capabilities` with Admin REST |

`moon test --target native` runs the in-process mock suite. The
`examples/live_capabilities` program exercises an authenticated Pulsar broker;
set `PULSAR_ADMIN_URL` to include Admin REST checks. The live program is not
part of `moon test` and requires a broker. `moon coverage` measures mock-suite
line execution and therefore does not include the live program's execution.
The mock suite does not yet reach full library line coverage; uncovered paths
include connection failures, malformed broker replies, and several existing
reader/transaction branches.
