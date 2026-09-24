# moonpulsar

English | [简体中文](README.zh-CN.md)

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com). Architecture follows [pulsar-rs](https://github.com/streamnative/pulsar-rs) and [pulsar-client-go](https://github.com/apache/pulsar-client-go). See [CAPABILITY_MATRIX.md](CAPABILITY_MATRIX.md) for verified coverage and remaining differences.

[Source repository](https://github.com/pangbit/moonpulsar).

## Features

- Connection management over TCP with the Pulsar binary protocol (`pulsar://`), handshake, keepalive and request/response correlation
- `ClientOptions` for connection/operation timeouts, keepalive interval, per-broker connection pool size and idle eviction, a shared pending producer payload-byte budget, advertised listener and lookup properties; asynchronous authentication providers and token-file reload on connection or challenge
- Topic lookup with redirect following and connection pooling
- Producer: synchronous and asynchronous send, shared/exclusive/wait-for-exclusive/fencing access modes, producer metadata, batching (count/bytes/delay triggers, manual flush), broker receipts, send error propagation, optional message-count and client-wide payload-byte limits with blocking or fail-fast admission, and send timeout including buffered batches
- Producer interceptors can transform a message before partition routing and observe either its broker receipt or send failure, including asynchronous sends that the caller never waits for.
- Consumer interceptors observe application delivery and the results of ACK and negative ACK operations, including `Message::ack`, `Message::nack`, and ACK by ID.
- `ClientOptions::new(on_event=...)` exposes structured send, receive, ACK/nack and reconnect events. `ClientMetrics` provides outcome counters; event callbacks can inspect message properties to connect an external tracing sink.
- Consumer: Exclusive / Shared / Failover / KeyShared subscriptions (including auto-split and sticky hash-range policies), individual & cumulative ack with optional broker confirmation, negative ack with optional exponential redelivery backoff, ack timeout, seek by message ID or timestamp, unsubscribe, flow control (FLOW permits), single-topic zero-sized receive queue with on-demand FLOW, optional auto-scaled receive queue, batched message unpacking and optional per-index batch ACK
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
- Schema declaration: producers/consumers declare `SchemaInfo` on creation; `SchemaCodec[T]` encodes and decodes STRING, JSON, BYTES, numeric primitives, Avro, Protobuf Native and legacy Protobuf generated messages, or uses custom callbacks
- Message encryption: `MessageCrypto` loads RSA PEM keys, wraps a fresh AES-256-GCM key with RSA-OAEP-SHA1 for each named recipient, and decrypts in consumers and readers. Pass `message_crypto` and `encryption_key_names` to encrypted producers; encrypted retry/DLQ consumers need both options to re-encrypt forwarded messages.
- Delayed delivery: `deliver_at` / `deliver_after` on `ProducerMessage`
- Timestamp seek for readers

Not yet / known gaps:

- Snappy uses the **google framing** variant (matching the Go/Python clients); Java clients expect xerial framing and cannot decode it — the same incompatibility exists between the official Java and Go clients
- The tested Pulsar 4.2.4 broker rejected creation of partitioned metadata over an existing non-partitioned topic with HTTP 409. Migrate data into a new partitioned topic when this change is needed.
- Retry-letter handling is available through `retry_topic` on single, multi-topic and pattern consumers, or the explicit `create_retry_consumer` constructor. Athenz ZTS key/certificate exchange and TLS client certificates remain open. OAuth2 currently requests a fresh token for each connection or challenge; a local token endpoint was validated against the authenticated 4.2.4 broker, but no external identity provider was tested. Athenz role-token suppliers were validated with a mock broker only.
- `MessageCrypto::load_public_key_file` and `load_private_key_file` can be called again after replacing PEM files; new sends and reads use the replaced key. Authentication failures return `InvalidFrame`; the affected consumer/reader stops delivery. Only RSA-OAEP-SHA1 and AES-256-GCM are supported. The native random source is `/dev/urandom`. [Go encryption interoperability steps](interop/go/README.md) passed on 4.2.4 and 3.3.9; Java and TLS encryption interop remain open.
- `SchemaCodec::avro(definition, to_datum, from_datum)` parses an Avro schema and converts typed values to binary Avro datums; `avro_datum(definition)` exposes the full Avro datum model directly. Add `yugonlian/moon-avro@0.3.0` and import its `codec` package to construct or match datums. Decoding uses the supplied schema; automatic writer/reader schema evolution across message versions remains open.
- `SchemaCodec::protobuf_native(descriptor_set, root_file, root_message)` accepts a binary `FileDescriptorSet` and generated MoonBit Protobuf message type. The root must exist in the descriptor set. `SchemaCodec::protobuf(definition)` uses the legacy PROTOBUF schema with an Avro-style JSON definition and generated MoonBit Protobuf message type. INT16/32/64 codecs default to the Java client's big-endian encoding; pass `little_endian=true` for the Go client's integer encoding. The [Go interop fixture](interop/go/README.md) verifies Avro, both Protobuf schemas, numeric primitives, BYTES and batch payloads in both directions against Pulsar 4.2.4 and 3.3.9.
- Automatic chunking requires the broker to advertise a maximum message size. `ChunkAssemblyPolicy::new(auto_ack_incomplete=true)` makes consumers and readers acknowledge received chunks when an incomplete assembly is evicted or expires; the default leaves them unacknowledged for broker redelivery.
- Batch messages use whole-entry ACK after every index is acknowledged by default. Set `enable_batch_index_ack=true` on a consumer to acknowledge each index; the broker must enable `acknowledgmentAtBatchIndexLevelEnabled`. Partial batch ACK cannot request broker confirmation in the default mode. Transactional batch ACK remains open.
- Set `ack_grouping=AckGroupingOptions::new(max_size=1000, max_time_ms=100)` on a consumer to buffer ACKs by count or time. Grouping is off unless configured; confirmed and transactional ACKs flush the buffer and send immediately. Closing or seeking flushes buffered ACKs, while reconnecting drops them so the broker can redeliver.
- `send_timeout_ms` starts when a buffered batch message enters the producer queue; if it expires before flush, the message is removed. Unbatched messages start their timer when the SEND frame is registered. A timeout after SEND does not prove the broker rejected the message, so applications should use stable producer names and sequence IDs when retrying.
- `ClientOptions::new(max_memory_bytes=...)` limits the payload bytes held by all producers created from one client. Metadata, connections, and other allocations are outside this budget. A producer with `block_if_queue_full=false` reports `ClientMemoryFull` when the budget is full; blocking producers wait for a receipt, failure, timeout, or client close to release it.
- Set `auto_scaled_receiver_queue=true` on a consumer to start with one FLOW permit and double the prefetch size after a full queue is followed by an empty read, up to `receiver_queue_size`. A zero maximum cannot be combined with auto scaling. Composite consumers pass this option to each source; their merge queue still has its own buffering.
- Pass `interceptors=[ProducerInterceptor::new(before_send=..., on_send_result=...)]` to `Client::create_producer`. Hooks run in order on the logical producer; `on_send_result` also receives immediate admission errors such as `ProducerQueueFull`. Callbacks are synchronous and cannot raise errors.
- Pass `interceptors=[ConsumerInterceptor::new(before_consume=..., on_ack=..., on_nack=...)]` to a consumer constructor. Hooks run on application delivery and when its message or ID ACK/nack call completes. `on_ack` reports a rejected confirmed ACK as an error; an unconfirmed ACK callback means the frame was submitted, not broker-confirmed.
- Use `let metrics = ClientMetrics::new()` and `ClientOptions::new(url, on_event=fn(event) { metrics.record(event) })` for a local counter snapshot. `ClientMetrics::snapshot` records completed sends, failures, receives, ACK/nack calls and reconnect outcomes. Callbacks are synchronous; a tracing adapter can read message properties from `ClientEvent::SendCompleted` and `ClientEvent::MessageReceived`. Built-in spans and exporters are still open.
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
- whole-batch and per-index ACK, including persistence of an acknowledged index across consumer recreation, on 4.2.4 and 3.3.9 with broker batch-index ACK enabled
- grouped ACKs with count-triggered flush and close flush on 4.2.4 and 3.3.9
- a buffered batch message expires before flush while the next message is delivered on 4.2.4 and 3.3.9
- two producers sharing a client-wide pending payload-byte budget on 4.2.4 and 3.3.9
- an auto-scaled receive queue sending and acknowledging messages on 4.2.4 and 3.3.9; exact FLOW growth is covered by the mock broker
- producer interceptor payload transformation and receipt callback on 4.2.4 and 3.3.9
- consumer interceptor delivery and ACK callback on 4.2.4 and 3.3.9
- client event and metric counters for send, receive and ACK on 4.2.4 and 3.3.9
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
