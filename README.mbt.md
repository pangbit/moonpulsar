# moonpulsar

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com), with full producer and consumer support. Architecture follows [pulsar-rs](https://github.com/streamnative/pulsar-rs) and [pulsar-client-go](https://github.com/apache/pulsar-client-go).

## Features

- Connection management over TCP with the Pulsar binary protocol (`pulsar://`), handshake, keepalive and request/response correlation
- Topic lookup with redirect following and connection pooling
- Producer: synchronous and asynchronous send, batching (count/bytes/delay triggers, manual flush), broker receipts, send error propagation
- Consumer: Exclusive / Shared / Failover / KeyShared subscriptions, individual & cumulative ack, negative ack with redelivery, flow control (FLOW permits), batched message unpacking
- Automatic reconnection: producers replay unconfirmed messages and consumers re-subscribe after a broker connection breaks
- Authentication: pluggable `Authentication` trait, token auth built in
- TLS via `pulsar+ssl://` service URLs
- Payload compression: LZ4 (frame format), Zlib, Zstd
- Partitioned topics: keyed messages route by `xxhash32(key) % partitions`, keyless round-robin; consumers aggregate all partitions
- Reader API: non-durable replay from any message id, `seek`, `has_message_available`, aggregated across partitions
- Transactions: coordinator channel, `new_transaction` / `commit` / `abort`, transactional send and ack
- Admin REST API: partitioned topic create/delete, topic delete, list topics, topic stats

Not yet / known gaps:

- Snappy uses the **google framing** variant (matching the Go/Python clients); Java clients expect xerial framing and cannot decode it — the same incompatibility exists between the official Java and Go clients
- Transaction coordinator ownership lookup (connect directly to the owning broker for now)
- `pulsar+ssl://` is implemented via `moonbitlang/async/tls` but has not been exercised against a TLS-enabled broker

## Verified against a real broker

The examples and integration scenarios have been exercised against Pulsar standalone 4.2:

- produce / consume roundtrip, sync + async send, batching (broker-side `batch_index` echoed correctly)
- nack redelivery, shared subscriptions, reader replay
- LZ4 / Zlib / Zstd / Snappy payloads consumed back by official clients (Snappy: google framing)
- partitioned topics (admin-created, key routing, merged consumption)
- transactions: TC channel, transactional produce, commit, read-back
- automatic reconnection across a broker restart (producer replay + consumer re-subscribe)

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

Run one with:

```sh
moon run examples/producer --target native
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
