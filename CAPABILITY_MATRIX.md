# Pulsar client capability and test matrix

Compared with the public APIs of [pulsar-rs](https://github.com/streamnative/pulsar-rs)
and [pulsar-client-go](https://github.com/apache/pulsar-client-go). This is a
functional comparison, not an API-by-API claim of parity. Status reflects the
repository on 2026-09-23.

| Capability | Status | Verification |
| --- | --- | --- |
| TCP handshake, lookup, pooling, keepalive, reconnection | Implemented | Mock broker; prior real-broker restart scenario |
| TLS service URL | Implemented, limited verification | URL parsing unit test and runnable TLS example; excluded from this token-authenticated live run |
| Binary-protocol token and basic auth | Implemented | Token: live broker; basic: unit test; Admin REST supports bearer token |
| Auth challenge refresh | Implemented for data refresh | Handshake and post-handshake mock tests; challenge-specific algorithms remain open |
| Producer send, async receipt, batching, compression, delayed delivery | Implemented | Mock broker and prior live broker scenarios |
| Producer chunking and compressed chunking | Implemented with explicit size | Wire-level mock and live Pulsar 4.2.4 |
| Partition routing and producer expansion | Implemented for initially partitioned topics, including one-partition topics | Mock one-to-two and live two-to-three partition expansion |
| Single, partitioned, and explicit multi-topic consumption | Implemented; initially partitioned consumers discover added partitions | Mock one-to-two and live two-to-three partition expansion |
| Pattern subscriptions | Topic additions and removals implemented | Addition: mock and live broker; removal and stale-queue filtering: mock |
| Consumer options, ack/nack, ack timeout, seek, unsubscribe | Implemented | Mock tests; ack timeout and ID seek live |
| Exponential negative-ack backoff | Implemented for `Message::nack` | Timing and close-cancellation mock tests; live broker redelivery |
| Chunk reassembly and ACK | Implemented with bounded memory | Mock wire tests and live consumer/reader replay |
| Reader, seek, replay, and added-partition discovery | Implemented for initially partitioned topics | Mock one-to-two and live two-to-three partition expansion |
| Transaction coordinator lookup and transaction lifecycle | Implemented | Mock tests and live coordinator/abort; prior live commit/read-back |
| Admin topic and subscription operations | Selected endpoints implemented | Mock HTTP tests and live create/expand/query/delete |
| Schema declaration | Implemented | Existing mock/live schema scenario; typed serialization is open |
| Dead-letter/retry policy | Open | No implementation or coverage |
| TableView | Open | No implementation or coverage |
| Conversion of existing non-partitioned handles | Open | No implementation or coverage |
| OAuth2, Athenz, TLS certificate authentication | Open | No implementation or coverage |
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
