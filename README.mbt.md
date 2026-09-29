# moonpulsar

English | [简体中文](README.zh-CN.md)

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com), targeting the native backend. Supports producers and consumers, batching, partitioned topics, readers, transactions, schemas, authentication and message encryption. See the [capability matrix](docs/capabilities.md) for supported behavior and verification limits.

[Source repository](https://github.com/pangbit/moonpulsar) | [Documentation](docs/README.md)

## Requirements

- MoonBit toolchain with the **native** backend (TCP sockets)
- A C compiler and system SDK/libc. OpenSSL and zlib development headers are not required. Zlib, including Java sync-flush compatibility, uses pure MoonBit and needs no system zlib runtime.
- OpenSSL development headers are not required. Client-certificate / TLS-policy options and ECIES message encryption require OpenSSL **3** shared libraries at runtime. On macOS these adapters look in the standard Homebrew `openssl@3` locations; on Linux they use the system library loader. Missing libraries or symbols produce errors when those features are used. RSA message encryption does not load this ECIES dependency. Ordinary TLS keeps the platform requirements of `moonbitlang/async/tls`.
- A Pulsar broker for the examples; the test suite uses an in-process mock broker and needs no external services

## Installation

These instructions target `0.1.0`. Check registry availability before adding the dependency:

```sh
moon view pangbit/moonpulsar --versions --json
```

If `0.1.0` is unavailable, or to run repository examples and tests, use the source workspace:

```sh
git clone https://github.com/pangbit/moonpulsar.git
cd moonpulsar
moon update
moon check --target native
moon test --target native
```

To run a roundtrip, use a disposable local Pulsar 4.2.4 broker. In a separate terminal:

```sh
docker run --rm --name moonpulsar-demo -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:4.2.4 bin/pulsar standalone -a 127.0.0.1
```

After the broker is ready, run from the repository root:

```sh
moon run examples/roundtrip --target native
```

A successful run prints `roundtrip OK`. Stop the disposable container after use with `docker stop moonpulsar-demo`.

Once registry availability is confirmed, a separate executable can declare `"pangbit/moonpulsar@0.1.0"` and `"moonbitlang/async@0.22.1"` in `moon.mod`, set `preferred_target = "native"`, and use this `moon.pkg`:

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

## Documentation

- [Client configuration and limitations](docs/client-guide.md)
- [Runnable examples](https://github.com/pangbit/moonpulsar/blob/main/examples/README.md)
- [Development and verification](docs/testing.md)
- [Performance testing and results](docs/performance.md)
- [Bug reports and usage questions](https://github.com/pangbit/moonpulsar/issues)
- [Security](SECURITY.md) · [Changelog](CHANGELOG.md)

## License

Apache-2.0. The vendored Pulsar protocol definition and generated bindings carry Apache Pulsar attribution in [NOTICE](NOTICE).
