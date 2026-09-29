# moonpulsar

English | [简体中文](README.zh-CN.md)

Apache Pulsar binary protocol client for [MoonBit](https://www.moonbitlang.com), targeting the native backend. Supports producers and consumers, batching, partitioned topics, readers, transactions, schemas, authentication and message encryption. See the [capability matrix](docs/capabilities.md) for supported behavior and verification limits.

[Source repository](https://github.com/pangbit/moonpulsar) | [Documentation](docs/README.md)

## Requirements

### Build environment

- Use the **native** backend; JS and Wasm are not supported by this client. CI runs on **Linux (Ubuntu 24.04)**. macOS has been tested locally; Windows is not verified.
- Install the MoonBit toolchain and a C compiler with the system SDK/libc development files. The locally verified toolchain is `moon 0.1.20260920` / `moonc 0.10.14+7d59c7ec9`; this is a tested version, not a declared minimum. CI installs the current toolchain.
- Building the library does **not** require OpenSSL or zlib development headers. Run `moon update` to fetch MoonBit dependencies.

### Runtime dependencies

Requirements depend on the features your application uses:

| Feature | Linux | macOS |
| --- | --- | --- |
| Plain TCP (`pulsar://`), compression, RSA message encryption | No OpenSSL or system zlib runtime needed | Same |
| Ordinary TLS (`pulsar+ssl://`, including a custom CA) | `moonbitlang/async@0.22.1` loads `libssl.so.3`, with fallbacks to `libssl.so.1.1` and `libssl.so` | Uses the system TLS library through `moonbitlang/async@0.22.1` |
| TLS with a client certificate or explicit protocol/cipher settings | OpenSSL **3**: `libssl.so.3` and `libcrypto.so.3` | Homebrew `openssl@3` |
| ECIES message encryption | OpenSSL **3**: `libcrypto.so.3` | Homebrew `openssl@3` |

Linux libraries must be discoverable by the system dynamic loader. The macOS OpenSSL 3 adapters look under `/opt/homebrew/opt/openssl@3/lib` or `/usr/local/opt/openssl@3/lib`. These shared libraries are needed on the **machine running the application**, not just the build machine.

### Tests and examples

- `moon test --target native` includes TLS and ECIES tests, so it needs the TLS libraries above, including OpenSSL **3**. It uses in-process mock brokers: no external broker or Docker is required.
- Examples need a reachable Pulsar broker. The quickstart below uses Docker to start Pulsar **4.2.4** locally. Broker CI covers **4.2.4** and **3.3.9**; these are verified versions, not a complete compatibility range.
- Additional ABI and live interoperability checks have separate dependencies; see [Development and verification](docs/testing.md).

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
