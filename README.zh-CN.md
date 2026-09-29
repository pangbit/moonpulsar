# moonpulsar

[English](README.md) | 简体中文

面向 [MoonBit](https://www.moonbitlang.com) native 后端的 Apache Pulsar 二进制协议客户端，支持生产与消费、批量消息、分区 Topic、Reader、事务、Schema、认证与消息加密。支持范围和验证限制见[能力矩阵](docs/capabilities.md)。

[源码仓库](https://github.com/pangbit/moonpulsar) | [文档索引](docs/README.zh-CN.md)

## 环境要求

- 支持 **native** 后端的 MoonBit 工具链；网络连接依赖 TCP socket
- C 编译器与系统 SDK/libc。不需要 OpenSSL、zlib 开发头文件；Zlib（包括 Java 同步刷新兼容路径）使用纯 MoonBit，不需要系统 zlib 动态库。
- 构建不需要 OpenSSL 开发头文件。客户端证书、TLS 版本/密码套件配置以及 ECIES 消息加密在运行时需要 OpenSSL **3** 动态库；macOS 使用 Homebrew `openssl@3` 标准路径，Linux 使用系统动态库加载器。缺库或缺符号在使用相关功能时明确报错。RSA 消息加密不加载这项 ECIES 依赖。普通 TLS 仍遵循 `moonbitlang/async/tls` 的平台运行库要求。
- 运行示例需要 Pulsar broker；自动化测试使用进程内 mock broker，无需 Docker

## 安装

首个 Mooncakes 版本尚未发布，当前请使用源码工作区：

```sh
git clone https://github.com/pangbit/moonpulsar.git
cd moonpulsar
moon update
moon check --target native
moon test --target native
```

运行收发示例需要一次性本地 Pulsar 4.2.4 Broker，在另一个终端启动：

```sh
docker run --rm --name moonpulsar-demo -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:4.2.4 bin/pulsar standalone -a 127.0.0.1
```

Broker 就绪后，在仓库根目录运行：

```sh
moon run examples/roundtrip --target native
```

成功时输出 `roundtrip OK`。使用完毕后执行 `docker stop moonpulsar-demo` 停止该一次性容器。

待 `0.1.0` 发布后，独立的可执行项目可在 `moon.mod` 中声明 `"pangbit/moonpulsar@0.1.0"` 和 `"moonbitlang/async@0.22.1"`，设置 `preferred_target = "native"`，并使用如下 `moon.pkg`：

```text
import {
  "pangbit/moonpulsar" @pulsar,
  "moonbitlang/async",
}
supported_targets = "+native"
pkgtype(kind: "executable")
```

## 基本用法

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

连接读取和保活等后台任务由传给 `Client::connect` 的任务组管理，遵循 `moonbitlang/async` 的结构化并发规则。这里先创建消费者再发送消息，确保新订阅能够收到这条消息。`@pulsar` 别名在上面的 `moon.pkg` 中声明；更完整的示例见 `examples/roundtrip`。

## 文档

- [客户端配置与限制](docs/client-guide.zh-CN.md)
- [可运行示例](https://github.com/pangbit/moonpulsar/blob/main/examples/README.zh-CN.md)
- [开发与验证](docs/testing.zh-CN.md)
- [性能测试与结果](docs/performance.md)
- [Bug 反馈与使用求助](https://github.com/pangbit/moonpulsar/issues)
- [安全报告](SECURITY.md) · [版本变更](CHANGELOG.md)

## 许可证

Apache-2.0。从 Apache Pulsar 引入的协议定义及其生成的绑定所需署名见 [NOTICE](NOTICE)。
