# moonpulsar

[English](README.md) | 简体中文

面向 [MoonBit](https://www.moonbitlang.com) 的 Apache Pulsar 二进制协议客户端。架构参考 [pulsar-rs](https://github.com/streamnative/pulsar-rs) 和 [pulsar-client-go](https://github.com/apache/pulsar-client-go)。已验证的能力与尚存的差异见[能力矩阵](CAPABILITY_MATRIX.md)。[源码仓库](https://github.com/pangbit/moonpulsar)。

## 功能

- 通过 TCP 使用 Pulsar 二进制协议（`pulsar://`）：握手、保活、请求与响应关联
- `ClientOptions` 可设置连接/操作超时、保活间隔、每 broker 连接池大小及空闲回收、跨生产者待发送负载字节预算、监听器和查找属性；异步认证提供器可在连接及认证挑战时重读 Token 文件
- Topic 查找、重定向跟随和连接池
- 生产者：同步与异步发送、共享/独占/等待独占/抢占式访问模式、生产者元数据、按条数/字节数/延迟触发的批量发送、手动 flush、broker 回执与发送错误传递；可限制待发送消息数和客户端共享的负载字节数、选择满队列等待或立即报错，并设置覆盖批次缓冲时间的发送超时
- 生产者拦截器可在分区路由前修改消息，并观察 broker 回执或发送失败；调用方不等待异步回执时也会触发结果回调。
- 消费者拦截器可观察应用收到消息、`receive()` / `try_receive()` 失败，以及 `Message::ack`、`Message::nack` 和按 ID 确认的执行结果。
- `ClientOptions::new(on_event=...)` 提供结构化发送、接收、ACK/nack 和重连事件；`ClientMetrics` 统计结果次数。事件中的消息属性可供外部 tracing 实现使用。
- 消费者：Exclusive、Shared、Failover、KeyShared 订阅（含自动拆分和固定哈希范围策略）；可选 broker 确认的单条/累计/ID 列表 ACK、负面 ACK 与可选的指数退避重投、ACK 超时、按消息 ID 或时间戳 seek、取消订阅、FLOW 许可控制、非分区单主题零接收队列按需请求、可选接收队列自动扩容，以及批量消息拆包和可选的批次索引 ACK
- 死信策略：消息超过允许的失败次数后转发到死信 Topic，保留负载、key、排序 key、属性和事件时间；死信生产者收到 broker 回执后才确认源消息
- 重试信 Topic 消费者：普通订阅可设置 `retry_topic`，也可使用显式构造函数；`reconsume_later` 延迟重新投递，达到上限后转入死信 Topic
- 可按最新或指定版本查询 Schema；生产者在消息中附带 broker 分配的版本，消费者可读取 `schema_version()`

`DeadLetterPolicy::new(1U, dead_letter_topic)` 表示允许应用处理一次；发生 NACK 后，下次投递将转入死信 Topic。单 Topic、多 Topic 和模式消费者构造函数都可接收这一策略。

- 多 Topic 消费者，以及能够发现新增 Topic 并关闭已移除源的模式消费者
- 复合消费者与 Reader 的有界合并队列；慢速应用会对源转发形成背压
- 自动重连：连接断开后，生产者重放尚未确认的消息，消费者重新订阅；`ClientOptions` 可设置退避和有限重试次数
- 二进制协议认证：可扩展的 `Authentication` trait，内置 token 与 basic auth；`ClientOptions::new(auth_provider=...)` 可使用轮换 Token 文件、OAuth2 客户端凭据和外部供应的 Athenz 角色令牌，broker 发出认证挑战时刷新认证数据
- `pulsar+ssl://` TLS 连接：系统信任根或通过 `Client::connect_tls_with_ca` 提供自定义 PEM CA
- LZ4（帧格式）、Zlib、Zstd 压缩；Snappy 的兼容性限制见下文
- 分区 Topic：可选 key 哈希（默认）、轮询、固定单分区或自定义回调路由；生产者可读取最后确认的序列号；消费者聚合全部分区，生产者和消费者可发现新增分区（默认每 60 秒轮询）
- 可选的生产者分块和消费者/Reader 分块重组，包括压缩负载；`ChunkAssemblyPolicy` 可限制待组装消息数，并在没有新消息时清理过期分块
- Reader：从指定消息 ID 开始的非持久回放（可选择包含起点）、seek、`has_message_available`、最后消息 ID、自定义名称/属性/订阅、分区聚合和新增分区发现
- TableView：从压缩后的 Reader 获取原始字节或 Schema 解码的键值快照、实时更新、墓碑删除和变更监听器；类型化监听器以 `Err` 报告解码失败
- 事务：协调器归属查找、`new_transaction` / `commit` / `abort`、事务性发送与 ACK
- Admin REST API：创建/删除 Topic、创建/扩容/删除分区 Topic、查询分区元数据、列出 Topic 与订阅、删除订阅、查询 Topic 及分区 Topic 统计信息
- Schema 声明：生产者/消费者创建时声明 `SchemaInfo`；`SchemaCodec[T]` 支持 STRING、JSON、BYTES、数值原始类型、Avro、Protobuf Native 和传统 Protobuf 的类型化编解码，也可传入自定义回调
- 消息加密：`MessageCrypto` 读取 RSA PEM 密钥，按接收方名称用 RSA-OAEP-SHA1 包裹每条消息的新 AES-256-GCM 密钥；消费者、Reader 和 TableView 解密。加密生产者需传入 `message_crypto` 与 `encryption_key_names`，加密重试/死信消费者也需这两个选项，以便转发时重新加密。
- 延迟投递：`ProducerMessage` 的 `deliver_at` / `deliver_after`
- Reader 按时间戳 seek

## 已知限制

- Snappy 使用 **google framing** 变体，与 Go/Python 客户端一致；Java 客户端使用 xerial framing，不能解码这种格式。官方 Java 与 Go 客户端之间也存在这一差异。
- 测试所用的 Pulsar 4.2.4 broker 在已有非分区 Topic 上创建分区元数据时返回 HTTP 409。需要转换时，应把数据迁移到新的分区 Topic。
- 单 Topic、多 Topic 和模式消费者可通过 `retry_topic` 自动加入重试 Topic，也可使用显式 `create_retry_consumer`。Athenz ZTS 密钥/证书换取角色令牌和 TLS 客户端证书尚未实现。OAuth2 当前在每次连接或认证挑战时重新取令牌；本地令牌端点已通过 4.2.4 认证 broker 的收发验证，尚未用外部身份提供方验证。Athenz 角色令牌供应器目前只有 mock broker 验证。
- 替换 PEM 文件后再次调用 `MessageCrypto::load_public_key_file` 或 `load_private_key_file` 即可轮换密钥；新发送与解密使用新密钥。认证失败返回 `InvalidFrame`，受影响的消费者或 Reader 停止交付。当前仅支持 RSA-OAEP-SHA1 与 AES-256-GCM，native 随机源是 `/dev/urandom`。[Go 加密互通步骤](interop/go/README.md) 已在 4.2.4 和 3.3.9 通过；Java 与 TLS 加密互通仍待验证。
- `MessageCrypto::new(public_key_reader=..., private_key_reader=...)` 可接收按名称读取 PEM 字节的同步回调；每次发送或解密都会重新读取，并优先于手动加入的密钥。返回 `None` 表示密钥不可用。
- `SchemaCodec::avro(definition, to_datum, from_datum)` 提供 Avro 二进制记录的类型化映射；不同版本 Schema 的自动演进仍待实现。`SchemaCodec::protobuf_native(descriptor_set, root_file, root_message)` 使用二进制 `FileDescriptorSet` 和生成的 MoonBit Protobuf 类型；根消息必须存在于描述符中。传统 Protobuf 使用 Avro 风格 JSON Schema。INT16/32/64 默认采用与 Java 客户端一致的大端序，和 Go 客户端互通时设置 `little_endian=true`。[Go 互通测试](interop/go/README.md) 已在 Pulsar 4.2.4 和 3.3.9 上双向验证这些类型与批次负载。
- 支持显式 `chunk_size` 或按 broker 协商上限自动分块；自动模式要求 broker 公布最大消息尺寸。`ChunkAssemblyPolicy::new(auto_ack_incomplete=true)` 让消费者和 Reader 在未完成分块过期或被淘汰时确认已收到的分块；默认不确认，交由 broker 重投。
- 批次消息默认在全部索引确认后发送整批 ACK；消费者设置 `enable_batch_index_ack=true` 后可逐索引确认，broker 需启用 `acknowledgmentAtBatchIndexLevelEnabled`。默认模式下，未完成整批的 ACK 不能请求 broker 确认。事务 ACK 支持两种模式，并等待每次 broker ACK 响应；broker 还需启用事务。
- 固定版本的 Go 客户端消费 MoonBit 发送的加密空载荷时，会返回 16 字节 AES-GCM 标签：其消费路径在 `UncompressedSize` 为零时未替换解密后的缓冲区。MoonBit 能正确解密 Go 发出的加密空载荷；运行 `interop/go/encryption receive-empty` 可复现 Go 侧限制。
- 消费者可设置 `ack_grouping=AckGroupingOptions::new(max_size=1000, max_time_ms=100)`，按数量或时间合并 ACK；默认不启用。要求 broker 确认的 ACK 和事务 ACK 会先冲刷缓存再立即发送；关闭或 seek 前冲刷，重连时丢弃未发出的 ACK 以便 broker 重投。
- `Consumer::ack_ids([id1, id2])` 将普通 ID 合入一个 ACK 帧，批次索引 ID 保持位图处理。`Consumer::last_message_ids()` 与 `Reader::last_message_ids()` 返回按来源 Topic（含分区 Topic）索引的最后 ID。零接收队列只适用于非分区单主题消费者；分区、多 Topic、重试和模式订阅会明确报错。
- `send_timeout_ms` 从批次消息进入生产者缓冲队列时开始计时；在 flush 前过期的消息会从批次中移除。非批次消息仍从 SEND 帧登记时计时。SEND 后超时不代表 broker 一定拒绝消息，重试时应使用稳定的生产者名称和序列号。
- `ClientOptions::new(max_memory_bytes=...)` 限制同一客户端创建的所有生产者占用的待发送负载字节数；消息元数据、连接及其他分配不计入。`block_if_queue_full=false` 时超额返回 `ClientMemoryFull`，否则等待回执、失败、超时或客户端关闭释放额度。
- 消费者设置 `auto_scaled_receiver_queue=true` 后，从 1 个 FLOW 许可开始；队列曾满且下一次读取时已空，预取上限倍增，直至 `receiver_queue_size`。此选项不能与零接收队列同时使用。组合消费者会向每个来源传递此选项，其合并队列仍单独缓冲。
- 创建生产者时传入 `interceptors=[ProducerInterceptor::new(before_send=..., on_send_result=...)]`。钩子按顺序作用于逻辑生产者；`on_send_result` 也会收到 `ProducerQueueFull` 等入队前错误。回调同步执行且不能抛错。
- 创建消费者时传入 `interceptors=[ConsumerInterceptor::new(before_consume=..., on_receive_error=..., on_ack=..., on_nack=...)]`。钩子在应用收取消息、`receive()` / `try_receive()` 失败，以及消息或 ID 的 ACK/nack 调用结束时触发；要求 broker 确认的 ACK 被拒绝时，`on_ack` 收到错误。未要求确认的 ACK 回调只代表帧已提交，不代表 broker 已确认。
- 使用 `let metrics = ClientMetrics::new()` 和 `ClientOptions::new(url, on_event=fn(event) { metrics.record(event) })` 取得本地计数快照，包括发送与失败、接收与失败、ACK/nack 与失败、重连结果。事件回调同步执行；外部 tracing 可从 `ClientEvent::SendCompleted` 与 `ClientEvent::MessageReceived` 读取消息属性。内建 span 与导出器仍未实现。
- `Connection::max_message_size` 可读取 broker 握手报告的上限；生产者会按此限制编码后的帧，并可自动选择分块大小。
- `pulsar+ssl://` 使用 `moonbitlang/async/tls`。本地 TLS mock 覆盖自定义 CA 校验与重连。隔离的 Pulsar 4.2.4 和 3.3.9 broker 已通过 CA 校验下的收发、无关 CA 拒绝、有效 Token 认证与错误 Token 拒绝；4.2.4 还通过了携带 CA 和 Token 的同一生产者、消费者在 broker 重启后继续使用。双向 TLS 尚未验证；当前 TLS 客户端 API 不提供客户端证书。

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
- 4.2.4 与 3.3.9 上的整批及逐索引 ACK；在启用 broker 批次索引 ACK 后，重建消费者只收到未确认的索引
- 4.2.4 与 3.3.9 上的按数量冲刷与关闭前冲刷的 ACK 分组
- 4.2.4 与 3.3.9 上的批次消息 flush 前超时与后续消息成功投递
- 4.2.4 与 3.3.9 上两个生产者共享客户端待发送负载字节预算
- 4.2.4 与 3.3.9 上自动扩容接收队列的收发与确认；精确 FLOW 扩容由 mock broker 验证
- 4.2.4 与 3.3.9 上生产者拦截器修改负载和观察回执
- 4.2.4 与 3.3.9 上消费者拦截器观察交付与 ACK
- 4.2.4 与 3.3.9 上客户端事件与发送、接收、ACK 计数

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
| `examples/oauth2` | 从令牌端点获取 OAuth2 客户端凭据 |
| `examples/athenz` | 读取由 sidecar 维护的 Athenz 角色令牌文件 |
| `examples/tls` | 验证 TLS 收发及可选的 broker 重启检查 |
| `examples/token_rotation` | 同一客户端下验证无效 Token 拒绝和文件 Token 轮换 |
| `examples/producer_compression` | Zstd 压缩负载 |
| `examples/reader` | 从头回放 Topic |
| `examples/encryption_interop` | Go/MoonBit 加密互通、Reader 回放及重试/死信转发 |
| `examples/transaction_participants` | 在事务中注册生产者及订阅参与者 |
| `examples/transactional_batch_ack` | 验证整批或逐索引事务 ACK 的回滚重投和提交（`PULSAR_BATCH_INDEX_ACK=true`） |
| `examples/live_capabilities` | 多 Topic、模式订阅、ACK 超时、seek、分块、事务和可选 Admin REST 的真实 broker 回归场景 |

运行示例：

```sh
moon run examples/producer --target native
```

连接 TLS broker 时设置 `PULSAR_TLS_URL`；示例默认使用系统信任根校验证书，私有
CA 可通过 `PULSAR_TLS_CA_FILE` 指定。需要 Token 认证时设置 `PULSAR_TOKEN`。可通过
`PULSAR_TOPIC`、`PULSAR_SUBSCRIPTION` 隔离测试资源。设置
`PULSAR_TLS_RECONNECT_CHECK=1` 后，在首次收发完成的提示出现时重启 broker；
示例等待 30 秒，再用同一客户端发送和接收。仅在一次性本地 broker 上通过
`PULSAR_TLS_INSECURE=1` 显式关闭证书校验。

`examples/token_rotation` 从 `PULSAR_TOKEN_INITIAL_FILE` 和
`PULSAR_TOKEN_NEXT_FILE` 读取两个有效 Token，并复制到一次性工作文件。它先验证
broker 拒绝轮换后的无效 Token，再让同一客户端用第二个有效 Token 建立新连接并收发。
可设置 `PULSAR_URL`、`PULSAR_TOPIC` 和 `PULSAR_SUBSCRIPTION` 隔离测试资源；
示例不会修改传入的 Token 文件。

扩展集成场景接受 `PULSAR_URL`、`PULSAR_TOKEN` 或 `PULSAR_TOKEN_FILE`、`PULSAR_TEST_PREFIX` 和可选的 `PULSAR_ADMIN_URL`：

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
