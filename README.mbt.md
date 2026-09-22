# moonpulsar

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com), with full producer and consumer support. Architecture follows [pulsar-rs](https://github.com/streamnative/pulsar-rs) and [pulsar-client-go](https://github.com/apache/pulsar-client-go).

## Features

- Connection management over TCP with the Pulsar binary protocol (`pulsar://`), handshake, keepalive and request/response correlation
- Topic lookup with redirect following and connection pooling
- Producer: synchronous and asynchronous send, batching (count/bytes/delay triggers, manual flush), broker receipts, send error propagation
- Consumer: Exclusive / Shared / Failover / KeyShared subscriptions, individual & cumulative ack, negative ack with redelivery, flow control (FLOW permits), batched message unpacking
- Automatic reconnection: producers replay unconfirmed messages and consumers re-subscribe after a broker connection breaks
- Authentication: pluggable `Authentication` trait, token auth built in

Not yet: TLS (`pulsar+ssl://`), message compression (LZ4/Zlib/Snappy/Zstd), partitioned-topic aggregation, reader API, transactions, admin API.

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
