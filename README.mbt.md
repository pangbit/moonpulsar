# moonpulsar

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com). Architecture follows [pulsar-rs](https://github.com/streamnative/pulsar-rs) and [pulsar-client-go](https://github.com/apache/pulsar-client-go). See [CAPABILITY_MATRIX.md](CAPABILITY_MATRIX.md) for verified coverage and remaining differences.

## Features

- Connection management over TCP with the Pulsar binary protocol (`pulsar://`), handshake, keepalive and request/response correlation
- Topic lookup with redirect following and connection pooling
- Producer: synchronous and asynchronous send, batching (count/bytes/delay triggers, manual flush), broker receipts, send error propagation
- Consumer: Exclusive / Shared / Failover / KeyShared subscriptions, individual & cumulative ack, negative ack with optional exponential redelivery backoff, ack timeout, seek by message ID or timestamp, unsubscribe, flow control (FLOW permits), batched message unpacking
- Explicit multi-topic consumers and pattern consumers that discover added topics and close removed sources in a namespace
- Automatic reconnection: producers replay unconfirmed messages and consumers re-subscribe after a broker connection breaks
- Binary-protocol authentication: pluggable `Authentication` trait, token and basic auth built in; refresh auth data on broker challenge
- TLS via `pulsar+ssl://` service URLs
- Payload compression: LZ4 (frame format), Zlib, Zstd
- Partitioned topics: keyed messages route by `xxhash32(key) % partitions`, keyless round-robin; consumers aggregate all partitions and both producers and consumers discover added partitions (60-second default polling interval)
- Optional producer chunking and consumer/reader chunk reassembly, including compressed payloads
- Reader API: non-durable replay from any message id, `seek`, `has_message_available`, aggregation and added-partition discovery
- Transactions: coordinator ownership lookup, `new_transaction` / `commit` / `abort`, transactional send and ack
- Admin REST API: topic create/delete, partitioned topic create/expand/delete and metadata, list topics/subscriptions, delete subscriptions, topic stats
- Schema declaration: producers/consumers declare `SchemaInfo` (String/JSON/Avro/Protobuf/raw) on creation
- Delayed delivery: `deliver_at` / `deliver_after` on `ProducerMessage`
- Timestamp seek for readers

Not yet / known gaps:

- Snappy uses the **google framing** variant (matching the Go/Python clients); Java clients expect xerial framing and cannot decode it — the same incompatibility exists between the official Java and Go clients
- Clients created when a topic was non-partitioned do not convert that handle into a partitioned handle.
- Retry/DLQ policies, TableView, broker-side KeyShared policies, OAuth2/Athenz/TLS-certificate authentication, encryption, and schema serialization are not implemented.
- Chunking requires an explicit `chunk_size`; broker maximum-message-size discovery and configurable chunk expiry are not implemented.
- `pulsar+ssl://` is implemented via `moonbitlang/async/tls`; this capability run used plain TCP with token auth.

## Verified against a real broker

The examples and integration scenarios have been exercised against Pulsar standalone 4.2:

- produce / consume roundtrip, sync + async send, batching (broker-side `batch_index` echoed correctly)
- nack redelivery, shared subscriptions, reader replay
- LZ4 / Zlib / Zstd / Snappy payloads consumed back by official clients (Snappy: google framing)
- partitioned topics (admin-created, key routing, merged consumption)
- transactions: TC channel, transactional produce, commit, read-back
- automatic reconnection across a broker restart (producer replay + consumer re-subscribe)
- token-authenticated multi-topic and pattern consumption, ack timeout, delayed nack redelivery, consumer seek, chunked produce/consume/reader replay, and coordinator lookup
- partition expansion from two to three partitions while the producer, consumer, and reader remain open
- token-authenticated Admin REST topic/subscription operations and partition expansion

## Requirements

- MoonBit toolchain with the **native** backend (TCP sockets)
- A Pulsar broker for the examples; the test suite uses an in-process mock broker and needs no external services

## Usage

```mbt nocheck
///|
async fn boot(group : @async.TaskGroup[Unit]) -> Unit {
  let client = @pulsar.Client::connect(group, "pulsar://127.0.0.1:6650")
  let producer = client.create_producer("persistent://public/default/my-topic")
  let receipt = producer.send(@pulsar.ProducerMessage::new(b"hello"))
  println(receipt.message_id)

  let consumer = client.create_consumer(
    "persistent://public/default/my-topic", "my-subscription",
  )
  let message = consumer.receive()
  message.ack()
  client.close()
}
```

Background tasks (connection reader, keepalive) are tied to the task group passed to `Client::connect`, following `moonbitlang/async` structured concurrency.

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

The protocol layer in `proto/` is generated from `proto/PulsarApi.proto` (vendored from apache/pulsar) with `protoc-gen-mbt`:

```sh
protoc --mbt_out=. --mbt_opt=project_name=proto proto/PulsarApi.proto
```

## License

Apache-2.0
