# Pulsar client capability and test matrix

Compared with the public APIs of [pulsar-rs](https://github.com/streamnative/pulsar-rs)
and [pulsar-client-go](https://github.com/apache/pulsar-client-go). This is a
functional comparison, not an API-by-API claim of parity. Status reflects the
repository on 2026-09-23.

| Capability | Status | Verification |
| --- | --- | --- |
| TCP handshake, lookup, pooling, keepalive, reconnection | Implemented | Mock broker; prior real-broker restart scenario |
| TLS service URL and custom PEM CA | Implemented, limited live verification | TLS mock validates a private CA, rejects an untrusted certificate, and reconnects with the same CA; runnable TLS example; excluded from the token-authenticated live run |
| Binary-protocol token and basic auth | Implemented | Token: live broker; basic: unit test; Admin REST supports bearer token |
| Auth challenge refresh | Implemented for data refresh | Handshake and post-handshake mock tests; challenge-specific algorithms remain open |
| Producer send, async receipt, batching, compression, delayed delivery | Implemented | Mock broker and prior live broker scenarios |
| Producer access modes, metadata, and reconnect epoch | Implemented | Mock wire, wait-ready, and reconnect tests; live exclusive conflict and wait-for-exclusive takeover |
| Producer chunking and compressed chunking | Implemented with explicit size | Wire-level mock and live Pulsar 4.2.4 |
| Partition routing and producer expansion | Implemented for initially partitioned topics, including one-partition topics | Mock one-to-two and live two-to-three partition expansion |
| Single, partitioned, and explicit multi-topic consumption | Implemented; initially partitioned consumers discover added partitions | Mock one-to-two expansion and bounded slow-consumer forwarding; live two-to-three expansion |
| KeyShared auto-split and sticky hash ranges | Implemented | Range validation and SUBSCRIBE wire mock; sticky policy live broker |
| Pattern subscriptions | Topic additions and removals implemented | Addition: mock and live broker; removal and stale-queue filtering: mock |
| Consumer options, ack/nack, ack timeout, seek, unsubscribe | Implemented | Mock tests; ack timeout and ID seek live |
| Exponential negative-ack backoff | Implemented for `Message::nack` | Timing and close-cancellation mock tests; live broker redelivery |
| Chunk reassembly and ACK | Implemented with bounded memory | Mock wire tests and live consumer/reader replay |
| Reader, seek, replay, and added-partition discovery | Implemented for initially partitioned topics | Mock one-to-two expansion, bounded slow-reader forwarding, and disconnect wakeup; live two-to-three expansion |
| Transaction coordinator lookup and transaction lifecycle | Implemented | Mock tests and live coordinator/abort; prior live commit/read-back |
| Admin topic and subscription operations | Selected endpoints implemented, including partitioned-topic list and stats | Mock HTTP tests and live create/expand/query/stats/delete |
| Schema declaration | Implemented | Existing mock/live schema scenario; typed serialization is open |
| Dead-letter policy | Implemented for consumer delivery and negative ack; producer created when the consumer is created | Mock publish-before-ACK, send-failure NACK, direct consumer NACK, composite non-blocking receive, and live broker explicit-NACK routing (including a broker that reports redelivery count zero) |
| Retry-letter topic policy | Implemented through explicit `create_retry_consumer` and `reconsume_later` for a source topic; standard multi-topic/pattern constructors do not auto-add retry topics | Mock retry then DLQ publish-before-ACK, send-failure NACK; live Shared subscription delayed retry and DLQ escalation |
| TableView (raw payload values) | Implemented | Mock initial replay/live updates/empty topic/query failure; live broker update, tombstone, and added partition |
| In-place conversion of a non-partitioned topic to a partitioned topic | Broker does not support this on the tested Pulsar 4.2.4 instance | An isolated Admin REST probe returned HTTP 409 (`This topic already exists`); existing clients can discover added partitions of an already partitioned topic |
| OAuth2, Athenz, TLS client-certificate authentication | Open | No implementation or coverage; the current `moonbitlang/async/tls` client API does not expose a client certificate |
| Encryption, interceptors, tracing, metrics | Open | No implementation or coverage |
| Full Admin REST surface | Open | Only listed topic/subscription endpoints are implemented |

`moon test --target native` runs the in-process mock suite. The
`examples/live_capabilities` program exercises an authenticated Pulsar broker;
set `PULSAR_ADMIN_URL` to include Admin REST checks. The live program is not
part of `moon test` and requires a broker. `moon coverage` measures mock-suite
line execution and therefore does not include the live program's execution.
The mock suite does not yet reach full library line coverage; uncovered paths
include connection failures, malformed broker replies, and several existing
reader/transaction branches.
