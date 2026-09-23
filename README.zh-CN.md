# moonpulsar

[English](README.md) | 简体中文

面向 [MoonBit](https://www.moonbitlang.com) 的 Apache Pulsar 二进制协议客户端。架构参考 [pulsar-rs](https://github.com/streamnative/pulsar-rs) 和 [pulsar-client-go](https://github.com/apache/pulsar-client-go)。已验证的能力与尚存的差异见[能力矩阵](CAPABILITY_MATRIX.md)。[源码仓库](https://github.com/pangbit/moonpulsar)。

## 功能

- 通过 TCP 使用 Pulsar 二进制协议（`pulsar://`）：握手、保活、请求与响应关联
- `ClientOptions` 可设置连接/操作超时、保活间隔、监听器和查找属性；异步认证提供器可在连接及认证挑战时重读 Token 文件
- Topic 查找、重定向跟随和连接池
- 生产者：同步与异步发送、共享/独占/等待独占/抢占式访问模式、生产者元数据、按条数/字节数/延迟触发的批量发送、手动 flush、broker 回执与发送错误传递
- 消费者：Exclusive、Shared、Failover、KeyShared 订阅（含自动拆分和固定哈希范围策略）；单条/累计 ACK、负面 ACK 与可选的指数退避重投、ACK 超时、按消息 ID 或时间戳 seek、取消订阅、FLOW 许可控制，以及批量消息拆包
- 死信策略：消息超过允许的失败次数后转发到死信 Topic，保留负载、key、排序 key、属性和事件时间；死信生产者收到 broker 回执后才确认源消息
- 显式重试信 Topic 消费者：合并源 Topic 与重试 Topic；`reconsume_later` 延迟重新投递，达到重试上限后转入死信 Topic

`DeadLetterPolicy::new(1U, dead_letter_topic)` 表示允许应用处理一次；发生 NACK 后，下次投递将转入死信 Topic。单 Topic、多 Topic 和模式消费者构造函数都可接收这一策略。

- 多 Topic 消费者，以及能够发现新增 Topic 并关闭已移除源的模式消费者
- 复合消费者与 Reader 的有界合并队列；慢速应用会对源转发形成背压
- 自动重连：连接断开后，生产者重放尚未确认的消息，消费者重新订阅；`ClientOptions` 可设置退避和有限重试次数
- 二进制协议认证：可扩展的 `Authentication` trait，内置 token 与 basic auth；broker 发出认证挑战时刷新认证数据
- `pulsar+ssl://` TLS 连接：系统信任根或通过 `Client::connect_tls_with_ca` 提供自定义 PEM CA
- LZ4（帧格式）、Zlib、Zstd 压缩；Snappy 的兼容性限制见下文
- 分区 Topic：可选 key 哈希（默认）、轮询、固定单分区或自定义回调路由；生产者可读取最后确认的序列号；消费者聚合全部分区，生产者和消费者可发现新增分区（默认每 60 秒轮询）
- 可选的生产者分块和消费者/Reader 分块重组，包括压缩负载；`ChunkAssemblyPolicy` 可限制待组装消息数，并在没有新消息时清理过期分块
- Reader：从指定消息 ID 开始的非持久回放（可选择包含起点）、seek、`has_message_available`、最后消息 ID、自定义名称/属性/订阅、分区聚合和新增分区发现
- TableView：从压缩后的 Reader 获取原始字节或 Schema 解码的键值快照、实时更新、墓碑删除和变更监听器；类型化监听器以 `Err` 报告解码失败
- 事务：协调器归属查找、`new_transaction` / `commit` / `abort`、事务性发送与 ACK
- Admin REST API：创建/删除 Topic、创建/扩容/删除分区 Topic、查询分区元数据、列出 Topic 与订阅、删除订阅、查询 Topic 及分区 Topic 统计信息
- Schema 声明：生产者/消费者创建时声明 `SchemaInfo`（String/JSON/Avro/Protobuf/raw）；`SchemaCodec[T]` 已支持 STRING、JSON、BYTES 和数值原始类型转换
- 延迟投递：`ProducerMessage` 的 `deliver_at` / `deliver_after`
- Reader 按时间戳 seek

## 已知限制

- Snappy 使用 **google framing** 变体，与 Go/Python 客户端一致；Java 客户端使用 xerial framing，不能解码这种格式。官方 Java 与 Go 客户端之间也存在这一差异。
- 测试所用的 Pulsar 4.2.4 broker 在已有非分区 Topic 上创建分区元数据时返回 HTTP 409。需要转换时，应把数据迁移到新的分区 Topic。
- 单 Topic、多 Topic 和模式消费者可通过 `retry_topic` 自动加入重试 Topic，也可使用显式 `create_retry_consumer`。OAuth2、Athenz、TLS 客户端证书认证、加密和 Schema 序列化尚未实现。
- 支持显式 `chunk_size` 或按 broker 协商上限自动分块；自动模式要求 broker 公布最大消息尺寸。尚未实现过期未完成分块的可选 ACK。
- `Connection::max_message_size` 可读取 broker 握手报告的上限；生产者会按此限制编码后的帧，并可自动选择分块大小。
- `pulsar+ssl://` 使用 `moonbitlang/async/tls`。自定义 CA 握手和重连由本地 TLS mock 覆盖；此前的真实 broker 测试使用带 token 认证的明文 TCP。当前 TLS 客户端 API 不提供用于双向 TLS 的客户端证书。

## 真实 broker 验证记录

示例和集成场景曾在 Pulsar standalone 4.2 上运行。当前 `examples/live_capabilities` 套件还在 4.2.4 以及启用事务的独立 3.3.9 broker 上通过，覆盖：

- 生产/消费往返、同步和异步发送、批量发送（broker 正确返回 `batch_index`）
- NACK 重投、Shared 订阅和 Reader 回放
- 官方客户端读取 LZ4 / Zlib / Zstd / Snappy 压缩消息（Snappy 为 google framing）
- 通过 Admin 创建的分区 Topic、按 key 路由和合并消费
- 事务协调器通道、事务发送、提交与回读
- broker 重启后的自动重连（生产者重放和消费者重新订阅）
- token 认证下的多 Topic/模式消费、ACK 超时、延迟 NACK 重投、消费者 seek、分块生产/消费/Reader 回放与协调器查找
- 客户端持续运行时，将分区从两个扩容到三个，生产者、消费者和 Reader 均能发现新增分区
- 原始字节及类型化 TableView 的初始回放、实时更新、墓碑删除和新增分区发现
- token 认证下的 Admin REST Topic/订阅操作和分区扩容
- 独占生产者拒绝竞争者；原生产者关闭后，等待独占的生产者进入就绪状态
- 显式 NACK 后的死信路由、发布成功后确认源消息，以及消息元数据保留
- Shared 订阅下延迟 5 秒的重试信投递，以及超过重试上限后进入死信 Topic

这些是既有验证记录，不代表当前提交已在所有 broker 配置上重新测试。

## 环境要求

- 支持 **native** 后端的 MoonBit 工具链；网络连接依赖 TCP socket
- 运行示例需要 Pulsar broker；自动化测试使用进程内 mock broker，无需 Docker

## 安装

模块发布到 Mooncakes **之后**，在使用 native 后端的 MoonBit 模块中添加依赖：

```sh
moon add pangbit/moonpulsar
```

发布之前，可以克隆[源码仓库](https://github.com/pangbit/moonpulsar)并在本地运行示例。使用已发布的 `0.1.0` 库编写可执行程序时，在 `moon.mod` 中声明 `"pangbit/moonpulsar@0.1.0"` 和 `"moonbitlang/async@0.22.1"`，设置 `preferred_target = "native"`，并使用如下 `moon.pkg`：

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

## 示例

`examples/` 是工作区中的独立模块。每个示例默认连接 `pulsar://127.0.0.1:6650` 的本地 broker：

| 示例 | 内容 |
| --- | --- |
| `examples/producer` | 同步 `send` 和异步 `send_async` |
| `examples/producer_batching` | 批量发送配置和手动 `flush` |
| `examples/consumer` | Exclusive 订阅、接收和 ACK |
| `examples/consumer_shared` | 两个工作者共享一个订阅 |
| `examples/consumer_nack` | 负面 ACK 和重投 |
| `examples/roundtrip` | 生产后消费的往返验证 |
| `examples/auth_token` | token 认证（`PULSAR_TOKEN` 环境变量） |
| `examples/producer_compression` | Zstd 压缩负载 |
| `examples/reader` | 从头回放 Topic |
| `examples/live_capabilities` | 多 Topic、模式订阅、ACK 超时、seek、分块、事务和可选 Admin REST 的真实 broker 回归场景 |

运行示例：

```sh
moon run examples/producer --target native
```

扩展集成场景接受 `PULSAR_URL`、`PULSAR_TOKEN`、`PULSAR_TEST_PREFIX` 和可选的 `PULSAR_ADMIN_URL`：

```sh
PULSAR_URL=pulsar://127.0.0.1:6650 PULSAR_TEST_PREFIX=moonpulsar-check \
  moon run examples/live_capabilities --target native
```

使用 Docker 快速启动本地 broker：

```sh
docker run -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone
```

## 开发与验证

```sh
moon test        # 单元测试和 mock broker 测试
moon info        # 重新生成 .mbti 接口文件
moon fmt         # 格式化
```

GitHub Actions 会在 push 和 pull request 时运行 native 类型检查、debug/release 测试、格式检查、生成接口检查、文档生成和打包。仓库中的 localhost TLS 私钥与证书是公开的一次性测试材料，绝不能用于真实 broker；Mooncakes 包不包含这些材料，也不包含测试和示例。

`proto/` 中的协议层由 `protoc-gen-mbt` 根据从 apache/pulsar 引入的 `proto/PulsarApi.proto` 生成：

```sh
protoc --mbt_out=. --mbt_opt=project_name=proto proto/PulsarApi.proto
```

## 许可证

Apache-2.0。从 Apache Pulsar 引入的协议定义及其生成的绑定所需署名见 [NOTICE](NOTICE)。
