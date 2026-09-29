# moonpulsar

[English](README.md) | 简体中文

面向 [MoonBit](https://www.moonbitlang.com) native 后端的 Apache Pulsar 二进制协议客户端，支持生产与消费、批量消息、分区 Topic、Reader、事务、Schema、认证与消息加密。支持范围和验证限制见[能力矩阵](docs/capabilities.md)。

[源码仓库](https://github.com/pangbit/moonpulsar) | [文档索引](docs/README.zh-CN.md)

## 环境要求

### 构建环境

- 使用 **native** 后端；本客户端不支持 JS 或 Wasm。CI 覆盖 **Linux（Ubuntu 24.04）**；macOS 已做本地验证，Windows 尚未验证。
- 安装 MoonBit 工具链、C 编译器及系统 SDK/libc 开发文件。本地验证使用 `moon 0.1.20260920` / `moonc 0.10.14+7d59c7ec9`；这是已验证版本，不代表最低版本要求。CI 安装当前工具链。
- 构建库**不需要** OpenSSL 或 zlib 开发头文件。通过 `moon update` 获取 MoonBit 依赖。

### 运行时依赖

根据应用使用的功能准备依赖：

| 功能 | Linux | macOS |
| --- | --- | --- |
| 普通 TCP（`pulsar://`）、压缩、RSA 消息加密 | 不需要 OpenSSL 或系统 zlib 动态库 | 相同 |
| 普通 TLS（`pulsar+ssl://`，包括自定义 CA） | `moonbitlang/async@0.22.1` 加载 `libssl.so.3`，也会尝试 `libssl.so.1.1` 和 `libssl.so` | 通过 `moonbitlang/async@0.22.1` 使用系统 TLS 库 |
| 使用客户端证书，或指定 TLS 版本/密码套件 | OpenSSL **3**：`libssl.so.3` 和 `libcrypto.so.3` | Homebrew `openssl@3` |
| ECIES 消息加密 | OpenSSL **3**：`libcrypto.so.3` | Homebrew `openssl@3` |

Linux 动态库需要能被系统动态库加载器找到。macOS 的 OpenSSL 3 适配层查找 `/opt/homebrew/opt/openssl@3/lib` 或 `/usr/local/opt/openssl@3/lib`。这些动态库需要安装在**运行应用的机器上**，只在构建机器安装不够。

### 测试与示例

- `moon test --target native` 包含 TLS 和 ECIES 测试，因此需要上表中的 TLS 动态库，包括 OpenSSL **3**。测试使用进程内 mock broker，不需要外部 Broker 或 Docker。
- 示例需要可访问的 Pulsar Broker。下方快速开始使用 Docker 在本地启动 Pulsar **4.2.4**。Broker CI 覆盖 **4.2.4** 和 **3.3.9**；这是已验证版本，不代表完整兼容范围。
- 额外的 ABI 与真实 Broker 互操作检查有各自的依赖，见[开发与验证](docs/testing.zh-CN.md)。

## 安装

以下说明面向 `0.1.0`。添加依赖前，先确认注册表中该版本可用：

```sh
moon view pangbit/moonpulsar --versions --json
```

如果 `0.1.0` 尚不可用，或需要运行仓库中的示例与测试，请使用源码工作区：

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

确认注册表版本可用后，独立的可执行项目可在 `moon.mod` 中声明 `"pangbit/moonpulsar@0.1.0"` 和 `"moonbitlang/async@0.22.1"`，设置 `preferred_target = "native"`，并使用如下 `moon.pkg`：

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
