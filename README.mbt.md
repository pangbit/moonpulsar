# moonpulsar

English | [简体中文](README.zh-CN.md)

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com). Architecture follows [pulsar-rs](https://github.com/streamnative/pulsar-rs) and [pulsar-client-go](https://github.com/apache/pulsar-client-go). See [CAPABILITY_MATRIX.md](CAPABILITY_MATRIX.md) for verified coverage and remaining differences.

[Source repository](https://github.com/pangbit/moonpulsar).

## Features

- Connection management over TCP with the Pulsar binary protocol (`pulsar://`), handshake, keepalive and request/response correlation
- `ClientOptions` for connection/operation timeouts, keepalive interval, per-broker connection pool size and idle eviction, advertised listener and lookup properties; asynchronous authentication providers and token-file reload on connection or challenge
- Topic lookup with redirect following and connection pooling
- Producer: synchronous and asynchronous send, shared/exclusive/wait-for-exclusive/fencing access modes, producer metadata, batching (count/bytes/delay triggers, manual flush), broker receipts, send error propagation, optional pending-message limit with blocking or fail-fast admission, and in-flight send timeout
- Consumer: Exclusive / Shared / Failover / KeyShared subscriptions (including auto-split and sticky hash-range policies), individual & cumulative ack with optional broker confirmation, negative ack with optional exponential redelivery backoff, ack timeout, seek by message ID or timestamp, unsubscribe, flow control (FLOW permits), batched message unpacking
- Dead-letter policy: route messages after the configured number of unsuccessful deliveries, preserving payload, key, ordering key, properties, and event time; acknowledge the source only after the dead-letter producer receives a broker receipt
- Retry-letter consumers: use `retry_topic` on ordinary subscriptions or the explicit constructor, then `reconsume_later` to publish with a delay and route exhausted retries to the dead-letter topic
- Schema registry lookup by latest or specific version; producers attach the broker-assigned version and messages expose it through `schema_version()`

`DeadLetterPolicy::new(1U, dead_letter_topic)` allows one application attempt;
after a NACK, the next delivery goes to the dead-letter topic. The policy can
be passed to single-topic, multi-topic, and pattern consumer constructors.
- Explicit multi-topic consumers and pattern consumers that discover added topics and close removed sources in a namespace
- Bounded merge queues for composite consumers and readers, so slow applications stop source forwarding
- Automatic reconnection: producers replay unconfirmed messages and consumers re-subscribe after a broker connection breaks; `ClientOptions` can set backoff and a finite retry count
- Binary-protocol authentication: pluggable `Authentication` trait, token and basic auth built in; rotating token files, OAuth2 client credentials, and externally supplied Athenz role tokens through `ClientOptions::new(auth_provider=...)`, refreshed on broker challenge
- TLS via `pulsar+ssl://` service URLs, with system roots or a custom PEM CA through `Client::connect_tls_with_ca`
- Payload compression: LZ4 (frame format), Zlib, Zstd
- Partitioned topics: configurable key hash (default), round-robin, fixed single-partition, or custom callback routing; producers expose the last confirmed sequence ID; consumers aggregate all partitions and both producers and consumers discover added partitions (60-second default polling interval)
- Optional explicit or broker-limit-driven automatic producer chunking and consumer/reader chunk reassembly, including compressed payloads; `ChunkAssemblyPolicy` limits pending assemblies and expires incomplete chunks even when no further messages arrive
- Reader API: non-durable replay from any message id with optional inclusive start/seek, `seek`, `has_message_available`, last-message ID, custom name/properties/subscription, aggregation and added-partition discovery
- TableView: raw-byte or schema-decoded key/value snapshot from a compacted reader, live updates, tombstone deletion, and change listeners; typed listeners receive decode failures as `Err`
- Transactions: coordinator ownership lookup, `new_transaction` / `commit` / `abort`, transactional send and ack
- Admin REST API: topic create/delete and properties, partitioned topic create/expand/delete and metadata, list topics and partitioned topics, create/list/delete subscriptions, skip or expire subscription backlog, topic and partitioned-topic stats
- Schema declaration: producers/consumers declare `SchemaInfo` (String/JSON/Avro/Protobuf/raw) on creation; `SchemaCodec[T]` encodes and decodes STRING, JSON, BYTES and numeric primitive payloads or uses custom callbacks
- Delayed delivery: `deliver_at` / `deliver_after` on `ProducerMessage`
- Timestamp seek for readers

Not yet / known gaps:

- Snappy uses the **google framing** variant (matching the Go/Python clients); Java clients expect xerial framing and cannot decode it — the same incompatibility exists between the official Java and Go clients
- The tested Pulsar 4.2.4 broker rejected creation of partitioned metadata over an existing non-partitioned topic with HTTP 409. Migrate data into a new partitioned topic when this change is needed.
- Retry-letter handling is available through `retry_topic` on single, multi-topic and pattern consumers, or the explicit `create_retry_consumer` constructor. Athenz ZTS key/certificate exchange, TLS client certificates, encryption, and typed Avro/Protobuf serialization are not implemented. OAuth2 currently requests a fresh token for each connection or challenge; a local token endpoint was validated against the authenticated 4.2.4 broker, but no external identity provider was tested. Athenz role-token suppliers were validated with a mock broker only.
- Automatic chunking requires the broker to advertise a maximum message size; optional ACK of expired incomplete chunks is not implemented.
- `send_timeout_ms` starts when a SEND frame is registered; a buffered batch can wait for its flush delay before that timer starts. A timeout does not prove the broker rejected the message, so applications should use stable producer names and sequence IDs when retrying.
- `Connection::max_message_size` exposes the negotiated broker limit; producers cap encoded frames to that limit and can select a chunk size from it automatically.
- `pulsar+ssl://` is implemented via `moonbitlang/async/tls`; custom CA handshake and reconnection are covered by a local TLS mock, while the live broker run used plain TCP with token auth. The current TLS client API does not expose a client certificate for mutual TLS.

## Verified against a real broker

The examples and integration scenarios have been exercised against Pulsar standalone 4.2. The current `examples/live_capabilities` suite also passed against 4.2.4 and an isolated 3.3.9 broker with transactions enabled:

- produce / consume roundtrip, sync + async send, batching (broker-side `batch_index` echoed correctly)
- nack redelivery, shared subscriptions, reader replay
- LZ4 / Zlib / Zstd / Snappy payloads consumed back by official clients (Snappy: google framing)
- partitioned topics (admin-created, key routing, merged consumption)
- transactions: TC channel, transactional produce, commit, read-back
- automatic reconnection across a broker restart (producer replay + consumer re-subscribe)
- token-authenticated multi-topic and pattern consumption, ack timeout, delayed nack redelivery, consumer seek, chunked produce/consume/reader replay, and coordinator lookup
- partition expansion from two to three partitions while the producer, consumer, and reader remain open
- Raw and typed TableView initial replay, live update, tombstone deletion, and added-partition discovery
- token-authenticated Admin REST topic/subscription operations and partition expansion
- Admin subscription creation at Earliest, skip-one/skip-all cursor behavior, and timed backlog expiration
- Admin topic property update, read, and removal
- producer exclusive access rejects a competing producer; wait-for-exclusive becomes ready after the owner closes
- dead-letter routing after explicit NACK, with source ACK after publish and preserved message metadata
- retry-letter delivery through a Shared subscription after a 5-second delay, then transfer to DLQ when the retry limit is exceeded
- STRING and JSON schema declaration with typed payload encode/decode roundtrips

## Requirements

- MoonBit toolchain with the **native** backend (TCP sockets)
- A Pulsar broker for the examples; the test suite uses an in-process mock broker and needs no external services

## Installation

After this module is published to Mooncakes, add it to a native MoonBit module:

```sh
moon add pangbit/moonpulsar
```

Until then, clone the [source repository](https://github.com/pangbit/moonpulsar) to run the examples locally. For an executable using the published `0.1.0` library, declare `"pangbit/moonpulsar@0.1.0"` and `"moonbitlang/async@0.22.1"` in its `moon.mod`, set `preferred_target = "native"`, and use this `moon.pkg`:

```text
import {
  "pangbit/moonpulsar" @pulsar,
  "moonbitlang/async",
}
supported_targets = "+native"
pkgtype(kind: "executable")
```

## Usage

```mbt nocheck
///|
async fn main {
  @async.with_task_group(async fn(group) {
    let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
    let topic = "persistent://public/default/my-topic"
    let consumer = client.create_consumer(topic, "my-subscription")
    let producer = client.create_producer(topic)
    let receipt = producer.send(@pulsar.ProducerMessage::new(b"hello"))
    println("sent entry=\{receipt.message_id.entry_id}")
    let message = consumer.receive()
    message.ack()
    consumer.close()
    producer.close()
    client.close()
  })
}
```

Background tasks (connection reader, keepalive) are tied to the task group passed to `Client::connect`, following `moonbitlang/async` structured concurrency. The consumer is created before sending so a new subscription can receive the message.

The `@pulsar` alias in this snippet is declared in the `moon.pkg` above. See `examples/roundtrip` for a larger executable.

## Examples

The `examples/` workspace module contains runnable programs (each needs a local broker at `pulsar://127.0.0.1:6650`):

| Example | Shows |
| --- | --- |
| `examples/producer` | sync `send` and `send_async` |
| `examples/producer_batching` | batching config and manual `flush` |
| `examples/consumer` | exclusive subscription, receive + ack |
| `examples/consumer_shared` | two workers sharing one subscription |
| `examples/consumer_nack` | negative ack and redelivery |
| `examples/roundtrip` | produce-then-consume verification |
| `examples/auth_token` | token authentication (`PULSAR_TOKEN` env var) |
| `examples/oauth2` | OAuth2 client credentials from a token endpoint |
| `examples/athenz` | Athenz role token file maintained by a sidecar |
| `examples/producer_compression` | Zstd-compressed payloads |
| `examples/reader` | replay a topic from the start with a reader |
| `examples/live_capabilities` | real-broker regression for multi-topic, pattern, ack timeout, seek, chunking, transactions, and optional Admin REST |

Run one with:

```sh
moon run examples/producer --target native
```

The extended integration scenario accepts `PULSAR_URL`, `PULSAR_TOKEN`,
`PULSAR_TEST_PREFIX`, and optional `PULSAR_ADMIN_URL`:

```sh
PULSAR_URL=pulsar://127.0.0.1:6650 PULSAR_TEST_PREFIX=moonpulsar-check \
  moon run examples/live_capabilities --target native
```

A quick local broker via docker:

```sh
docker run -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone
```

## Development

```sh
moon test        # unit + mock-broker tests
moon info        # regenerate .mbti interfaces
moon fmt         # format
```

GitHub Actions runs native checks, debug and release tests, formatting, generated-interface verification, documentation generation, and package creation on pushes and pull requests. The checked-in localhost TLS key and certificate are public, disposable test fixtures; never use them for a real broker. They are excluded from the Mooncakes package, along with tests and examples.

The protocol layer in `proto/` is generated from `proto/PulsarApi.proto` (vendored from apache/pulsar) with `protoc-gen-mbt`:

```sh
protoc --mbt_out=. --mbt_opt=project_name=proto proto/PulsarApi.proto
```

## License

Apache-2.0. The vendored Pulsar protocol definition and generated bindings carry Apache Pulsar attribution in [NOTICE](NOTICE).
