# moonpulsar

English | [简体中文](README.zh-CN.md)

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com), targeting the native backend. Supports producers and consumers, batching, partitioned topics, readers, transactions, schemas, authentication and message encryption. See the [capability matrix](docs/capabilities.md) for supported behavior and verification limits.

[Source repository](https://github.com/pangbit/moonpulsar) | [Documentation](docs/README.md)

## Requirements

### Build environment

- Use the **native** backend; JS and Wasm are not supported by this client. CI runs on **Linux (Ubuntu 24.04)**. macOS has been tested locally; Windows is not verified.
- Install the [supported MoonBit toolchain](#toolchain-compatibility) and a C compiler with the system SDK/libc development files. CI pins and verifies the same toolchain; this is a specific tested combination, not a minimum-version claim.
- Building the library does **not** require OpenSSL or zlib development headers. Run `moon update` to fetch MoonBit dependencies.

### Toolchain compatibility

The supported, verified combination is:

| Component | Version |
| --- | --- |
| `moon` / `moonrun` | `0.1.20260920` (`914d7da`) |
| `moonc` | `v0.10.14+7d59c7ec9` |
| Protobuf runtime dependency | `moonbitlang/protobuf@0.1.3` |

The same Protobuf runtime is known to fail compilation with `moon 0.1.20261020` / `moonc v0.10.16+b1a0e846e` because it declares the reserved type name `Option`. See the [compatibility record and upstream issue](docs/verification-0.1.2.md#supported-toolchain). Other combinations are unverified.

Ordinary producer/consumer applications do not need to declare Protobuf separately. Applications that generate their own Protobuf messages directly use its `Read`/`Write` interfaces. This library maintains its dependency compatibility and will update after an official upstream fix is released and verified. Users do not need to patch upstream code.

The CI pin controls this repository's runners. Applications are compiled with the toolchain selected by their own environment, so installing this library does not switch the application's SDK.

### Runtime dependencies

Requirements depend on the features your application uses:

| Feature | Linux | macOS |
| --- | --- | --- |
| Plain TCP (`pulsar://`), compression, RSA message encryption | No OpenSSL or system zlib runtime needed | Same |
| Ordinary TLS (`pulsar+ssl://`, including a custom CA) | `moonbitlang/async@0.22.1` loads `libssl.so.3`, with fallbacks to `libssl.so.1.1` and `libssl.so` | Uses the system TLS library through `moonbitlang/async@0.22.1` |
| TLS with a client certificate or explicit protocol/cipher settings | OpenSSL **3**: `libssl.so.3` and `libcrypto.so.3` | Homebrew `openssl@3` |
| ECIES message encryption | OpenSSL **3**: `libcrypto.so.3` | Homebrew `openssl@3` |

Linux libraries must be discoverable by the system dynamic loader. The macOS OpenSSL 3 adapters look under `/opt/homebrew/opt/openssl@3/lib` or `/usr/local/opt/openssl@3/lib`. These shared libraries are needed on the **machine running the application**, not just the build machine.

The ECIES caller-error preservation regression has been verified with Linux
OpenSSL **3.0.20** and macOS OpenSSL **3.6.3**. OpenSSL **3.0.13** failed the
independent error-mark contract checks; do not assume that every OpenSSL 3
build preserves an existing caller mark. These are tested combinations, not
a minimum patch-version guarantee, and the loader checks only major version 3.
Distribution backports and other builds require verification. The independent
error-queue boundary in `moonbitlang/async@0.22.1` ordinary TLS remains an upstream
limitation. See [verification scope](docs/verification-0.1.2.md).

### Tests and examples

- `moon test --target native` includes TLS and ECIES tests, so it needs the TLS libraries above, including OpenSSL **3**. It uses in-process mock brokers: no external broker or Docker is required.
- Examples need a reachable Pulsar broker. The quickstart below uses Docker to start Pulsar **4.2.4** locally. Broker CI covers **4.2.4** and **3.3.9**; these are verified versions, not a complete compatibility range.
- Additional ABI and live interoperability checks have separate dependencies; see [Development and verification](docs/testing.md).

## Installation

These instructions target `0.1.2`. Check registry availability before adding the dependency:

```sh
moon view pangbit/moonpulsar --versions --json
```

If `0.1.2` is unavailable, or to run repository examples and tests, use the source workspace:

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

Once registry availability is confirmed, a separate executable can declare `"pangbit/moonpulsar@0.1.2"` and `"moonbitlang/async@0.22.1"` in `moon.mod`, set `preferred_target = "native"`, and use this `moon.pkg`:

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
