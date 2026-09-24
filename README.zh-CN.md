# moonpulsar

[English](README.md) | 简体中文

面向 [MoonBit](https://www.moonbitlang.com) 的 Apache Pulsar 二进制协议客户端。架构参考 [pulsar-rs](https://github.com/streamnative/pulsar-rs) 和 [pulsar-client-go](https://github.com/apache/pulsar-client-go)。已验证的能力与尚存的差异见[能力矩阵](CAPABILITY_MATRIX.md)。[源码仓库](https://github.com/pangbit/moonpulsar)。

## 功能

- 通过 TCP 使用 Pulsar 二进制协议（`pulsar://`）：握手、保活、请求与响应关联
- `ClientOptions` 可设置连接/操作超时、保活间隔、每 broker 连接池大小及空闲回收、生产/接收/分块/入站帧共享字节预算、监听器和查找属性；异步认证提供器可在连接及认证挑战时重读 Token 文件
- Topic 查找、重定向跟随和连接池
- 生产者：同步与异步发送、共享/独占/等待独占/抢占式访问模式、生产者元数据、按条数/字节数/延迟触发的批量发送、手动 flush、broker 回执与发送错误传递；可限制待发送消息数和客户端共享的负载字节数、选择满队列等待或立即报错，并设置覆盖批次缓冲时间的发送超时
- 生产者拦截器可在分区路由前修改消息，并观察 broker 回执或发送失败；调用方不等待异步回执时也会触发结果回调。
- 消费者拦截器可观察应用收到消息、`receive()` / `try_receive()` 失败，以及 `Message::ack`、`Message::nack` 和按 ID 确认的执行结果。
- `ClientOptions::new(on_event=..., on_span=...)` 提供结构化事件，以及发送、接收、ACK 和重连的成对 span；`ClientMetrics` 提供结果计数、耗时直方图和 Prometheus 文本导出。
- 消费者：Exclusive、Shared、Failover、KeyShared 订阅（含自动拆分和固定哈希范围策略）；可选 broker 确认的单条/累计/ID 列表 ACK、负面 ACK 与可选的指数退避重投、ACK 超时、按消息 ID 或时间戳 seek、取消订阅、FLOW 许可控制、非分区单主题零接收队列按需请求、可选接收队列自动扩容，以及批量消息拆包和可选的批次索引 ACK
- 死信策略：消息超过允许的失败次数后转发到死信 Topic，保留负载、key、排序 key、属性和事件时间；死信生产者收到 broker 回执后才确认源消息
- 重试信 Topic 消费者：普通订阅可设置 `retry_topic`，也可使用显式构造函数；`reconsume_later` 延迟重新投递，达到上限后转入死信 Topic
- 可按最新或指定版本查询 Schema；生产者在消息中附带 broker 分配的版本，消费者可读取 `schema_version()`

`DeadLetterPolicy::new(1U, dead_letter_topic)` 表示允许应用处理一次；发生 NACK 后，下次投递将转入死信 Topic。单 Topic、多 Topic 和模式消费者构造函数都可接收这一策略。

- 多 Topic 消费者，以及能够发现新增 Topic 并关闭已移除源的模式消费者。Broker 主动关闭模式订阅来源后，客户端等下一次命名空间发现再决定是否重建，删除中的 Topic 不会被立即重建。
- 复合消费者与 Reader 的有界合并队列；慢速应用会对源转发形成背压
- 自动重连：连接断开后，生产者重放尚未确认的消息，消费者重新订阅；`ClientOptions` 可设置退避和有限重试次数
- `max_memory_bytes` 对生产者消息的载荷、键、属性及编码重放帧、未完成分块、消费者和 Reader 队列中的消息载荷与动态元数据，以及 Broker 入站帧从读取前到分发完成期间的声明字节数记账。生产者预留在确认、错误、超时或关闭后释放；分块预留在完成、淘汰或清理后释放；接收预留在交付应用、seek 或关闭后释放。编码帧须与原消息同时容纳，无法容纳时返回 `ClientMemoryFull`；接收预算超额时，受影响的消费者或 Reader 队列以 `ClientMemoryFull` 关闭，入站帧超额时关闭该 Broker 连接，并向受影响的消费者和 Reader 返回 `ClientMemoryFull`，不重复尝试同一帧；使用足够预算的新客户端可重新读取。TLS/socket 内部缓冲与运行时堆开销不计入。
- 二进制协议认证：可扩展的 `Authentication` trait，内置 token 与 basic auth；`ClientOptions::new(auth_provider=...)` 可使用轮换 Token 文件、OAuth2 客户端凭据、外部供应的 Athenz 角色令牌，或 `athenz_zts_provider(AthenzZtsOptions::new(...))` 用 RSA 服务 NToken 或客户端证书向 ZTS 换取角色令牌；broker 发出认证挑战时刷新认证数据
- `pulsar+ssl://` TLS 连接：系统信任根或通过 `Client::connect_tls_with_ca` 提供自定义 PEM CA。`ClientOptions` 还可设置 `TlsIdentity::new(证书文件, 私钥文件)` 或异步 `tls_identity_provider`、TLS 1.2/1.3 版本范围、OpenSSL TLS 1.2 密码列表及 TLS 1.3 密码套件；native 适配层每次连接重新读取身份，并在 lookup 和重连时保持 CA 与主机名校验。
- LZ4（帧格式）、Zlib、Zstd 压缩；Snappy 的兼容性限制见下文
- 分区 Topic：可选 key 哈希（默认）、轮询、固定单分区或自定义回调路由；生产者可读取最后确认的序列号；消费者聚合全部分区，生产者和消费者可发现新增分区（默认每 60 秒轮询）
- 可选的生产者分块和消费者/Reader 分块重组，包括压缩负载；`ChunkAssemblyPolicy` 可限制待组装消息数，并在没有新消息时清理过期分块
- Reader：从指定消息 ID 开始的非持久回放（可选择包含起点）、seek、`has_message_available`、最后消息 ID、自定义名称/属性/订阅、分区聚合和新增分区发现
- TableView：从压缩后的 Reader 获取原始字节或 Schema 解码的键值快照、实时更新、墓碑删除和变更监听器；类型化监听器以 `Err` 报告解码失败
- 事务：协调器归属查找、`new_transaction` / `commit` / `abort`、事务性发送与 ACK
- Admin REST API：创建/删除 Topic、创建/扩容/删除分区 Topic、查询分区元数据、列出 Topic 与订阅、删除订阅、查询 Topic 及分区 Topic 统计信息
- Schema 声明：生产者/消费者创建时声明 `SchemaInfo`；`SchemaCodec[T]` 支持 STRING、JSON、BYTES、数值原始类型、Avro、Protobuf Native 和传统 Protobuf 的类型化编解码，也可传入自定义回调
- 消息加密：`MessageCrypto` 读取 RSA 或 EC PEM 密钥，按接收方名称用 RSA-OAEP-SHA1 或兼容 Pulsar Java 的 ECIES 包裹每条消息的新 AES-256-GCM 密钥；消费者、Reader 和 TableView 解密。加密生产者需传入 `message_crypto` 与 `encryption_key_names`，加密重试/死信消费者也需这两个选项，以便转发时重新加密。消费者与 Reader 可设置 `decryption_failure_action=Fail | Consume | Discard`。
- 延迟投递：`ProducerMessage` 的 `deliver_at` / `deliver_after`
- Reader 按时间戳 seek

## 已知限制

- Pulsar 消息中的 Snappy 使用原始 block，与固定版本 Go 客户端一致；公开的 `snappy_compress`、`snappy_decompress` 辅助函数仍处理 Google framed stream。Java 客户端使用 xerial framing，不能解码原始 block；官方 Java 与 Go 客户端也存在这一差异。
- 测试所用的 Pulsar 4.2.4 broker 在已有非分区 Topic 上创建分区元数据时返回 HTTP 409。需要转换时，应把数据迁移到新的分区 Topic。
- 单 Topic、多 Topic 和模式消费者可通过 `retry_topic` 自动加入重试 Topic，也可使用显式 `create_retry_consumer`。Athenz ZTS 服务私钥模式签发 RSA NToken；证书模式使用 `AthenzZtsOptions::new(..., certificate_file=..., ca_pem_file=...)`、HTTPS ZTS URL 及证书私钥。两种模式都会请求角色令牌、重试暂时故障，并在过期前刷新；ZTS URL 填写 `/zts/v1` 之前的服务根地址。OAuth2 当前在每次连接或认证挑战时重新取令牌；本地令牌端点已通过 4.2.4 认证 broker 的收发验证，尚未用外部身份提供方验证。证书模式已在 macOS 与 Linux 通过要求客户端证书的本地 ZTS 模拟服务；隔离的 4.2.4 和 3.3.9 Athenz 认证 broker 已通过角色令牌文件与 RSA 服务 NToken 向 ZTS 模拟服务交换令牌后的收发验证；尚未连接外部 Athenz 服务。
- 替换 PEM 文件后再次调用 `MessageCrypto::load_public_key_file` 或 `load_private_key_file` 即可轮换密钥；新发送与解密使用新密钥。解密失败默认采用 `Fail`，返回 `InvalidFrame` 并停止受影响的消费者或 Reader。`Consume` 交付原始密文，`Message::decryption_failed()` 为 `true`，应用自行决定是否 ACK；加密批次无法按此方式交付。RSA-OAEP-SHA1 与 ECIES 包裹 AES-256-GCM 密钥；ECIES 使用 native OpenSSL 3 适配层，已用 P-256、P-384 和 P-521 密钥验证。native 随机源是 `/dev/urandom`。[Go RSA 加密互通](interop/go/README.md)与 [Java EC 加密互通](interop/java/README.md)均在 4.2.4 和 3.3.9 通过；固定版本 Go 客户端默认加密只接受 RSA。RSA 与 P-521 ECIES 加密消息已在隔离的 4.2.4 和 3.3.9 broker 上，经双向 TLS 与官方 Go/Java 客户端双向互通。
- `MessageCrypto::new(public_key_reader=..., private_key_reader=...)` 可接收按名称读取 PEM 字节的同步回调；每次发送或解密都会重新读取，并优先于手动加入的密钥。返回 `None` 表示密钥不可用。
- `SchemaCodec::avro(definition, to_datum, from_datum)` 提供 Avro 二进制记录的类型化映射。`Client::decode_avro(message, reader_codec)` 按消息携带的版本查询并按 Topic/版本缓存 writer schema，解析字段别名与默认值、联合类型、集合、enum 默认符号及允许的数值提升；不兼容时返回 `SchemaIncompatible`。同步 codec 仍按自身声明的 schema 解码。[Java Avro 演进互通](interop/java/README.md) 已在隔离的 4.2.4 和 3.3.9 broker 上双向通过，覆盖 `id` 到 `identifier` 的别名、`active=true` 默认值及 int 到 long 提升。`SchemaCodec::protobuf_native(descriptor_set, root_file, root_message)` 使用二进制 `FileDescriptorSet` 和生成的 MoonBit Protobuf 类型；根消息必须存在于描述符中。传统 Protobuf 使用 Avro 风格 JSON Schema。INT16/32/64 默认采用与 Java 客户端一致的大端序，和 Go 客户端互通时设置 `little_endian=true`。[Go 互通测试](interop/go/README.md) 已在 Pulsar 4.2.4 和 3.3.9 上双向验证这些类型与批次负载。
- 支持显式 `chunk_size` 或按 broker 协商上限自动分块；自动模式要求 broker 公布最大消息尺寸。`ChunkAssemblyPolicy::new(auto_ack_incomplete=true)` 让消费者和 Reader 在未完成分块过期或被淘汰时确认已收到的分块；默认不确认，交由 broker 重投。
- 批次消息默认在全部索引确认后发送整批 ACK；消费者设置 `enable_batch_index_ack=true` 后可逐索引确认，broker 需启用 `acknowledgmentAtBatchIndexLevelEnabled`。默认模式下，未完成整批的 ACK 不能请求 broker 确认。事务 ACK 支持两种模式，并等待每次 broker ACK 响应；broker 还需启用事务。
- 固定版本的 Go 客户端消费 MoonBit 发送的加密空载荷时，会返回 16 字节 AES-GCM 标签：其消费路径在 `UncompressedSize` 为零时未替换解密后的缓冲区。MoonBit 能正确解密 Go 发出的加密空载荷；运行 `interop/go/encryption receive-empty` 可复现 Go 侧限制。
- 消费者可设置 `ack_grouping=AckGroupingOptions::new(max_size=1000, max_time_ms=100)`，按数量或时间合并 ACK；默认不启用。要求 broker 确认的 ACK 和事务 ACK 会先冲刷缓存再立即发送；关闭或 seek 前冲刷，重连时丢弃未发出的 ACK 以便 broker 重投。
- `Consumer::ack_ids([id1, id2])` 将普通 ID 合入一个 ACK 帧，批次索引 ID 保持位图处理。`Consumer::last_message_ids()` 与 `Reader::last_message_ids()` 返回按来源 Topic（含分区 Topic）索引的最后 ID。零接收队列只适用于非分区单主题消费者；分区、多 Topic、重试和模式订阅会明确报错。
- `send_timeout_ms` 从批次消息进入生产者缓冲队列时开始计时；在 flush 前过期的消息会从批次中移除。非批次消息仍从 SEND 帧登记时计时。SEND 后超时不代表 broker 一定拒绝消息，重试时应使用稳定的生产者名称和序列号。
- `ClientOptions::new(max_memory_bytes=...)` 在生产者、未完成分块、接收队列和 Broker 入站帧之间共享保留字节预算。`block_if_queue_full=false` 时生产者超额返回 `ClientMemoryFull`，否则等待回执、失败、超时、消息交付或客户端关闭释放额度。这是协议与消息数据的字节记账，并非进程堆内存的精确上限。
- 消费者设置 `auto_scaled_receiver_queue=true` 后，从 1 个 FLOW 许可开始；队列曾满且下一次读取时已空，预取上限倍增，直至 `receiver_queue_size`。此选项不能与零接收队列同时使用。组合消费者会向每个来源传递此选项，其合并队列仍单独缓冲。
- 创建生产者时传入 `interceptors=[ProducerInterceptor::new(before_send=..., on_send_result=...)]`。钩子按顺序作用于逻辑生产者；`on_send_result` 也会收到 `ProducerQueueFull` 等入队前错误。回调同步执行且不能抛错。
- 创建消费者时传入 `interceptors=[ConsumerInterceptor::new(before_consume=..., on_receive_error=..., on_ack=..., on_nack=...)]`。钩子在应用收取消息、`receive()` / `try_receive()` 失败，以及消息或 ID 的 ACK/nack 调用结束时触发；要求 broker 确认的 ACK 被拒绝时，`on_ack` 收到错误。未要求确认的 ACK 回调只代表帧已提交，不代表 broker 已确认。
- 使用 `let metrics = ClientMetrics::new()` 和 `ClientOptions::new(url, on_event=fn(event) { metrics.record(event) }, on_span=fn(span) { metrics.record_span(span) })` 统计结果次数及耗时；`metrics.prometheus_text()` 导出固定标签的 Prometheus 指标。`ClientSpanEvent::Started` 与 `Finished` 使用同一关联 ID、操作名、Topic 和可选消息 ID，结束事件还包含耗时、结果及错误文本。回调同步执行；`ClientMetrics::snapshot` 保留计数快照。
- `Connection::max_message_size` 可读取 broker 握手报告的上限；生产者会按此限制编码后的帧，并可自动选择分块大小。
- 普通 `pulsar+ssl://` 连接使用 `moonbitlang/async/tls`；证书身份和协议/密码选项启用仓库内 OpenSSL 3 native 适配层。配置客户端证书且没有其他认证提供者时，客户端自动在 Pulsar CONNECT 中声明 `tls` 认证方法。本地 TLS mock 已在 macOS 与 Linux 覆盖 CA 校验、私钥缺失、协议/密码选择及重连时刷新身份。隔离的 Pulsar 4.2.4 和 3.3.9 认证 broker 已从 macOS 与 Linux 通过客户端证书收发；Linux 上无证书或错误 CA 均被拒绝，两种版本重启后原有生产者、消费者均恢复。官方 Go、Java 客户端也在两种版本上与 MoonBit 经双向 TLS 交换加密消息。Windows 尚未验证。

## 真实 broker 验证记录

示例和集成场景曾在 Pulsar standalone 4.2 上运行。当前 `examples/live_capabilities` 套件还在 4.2.4 以及启用事务的独立 3.3.9 broker 上通过，覆盖：

- 生产/消费往返、同步和异步发送、批量发送（broker 正确返回 `batch_index`）
- NACK 重投、Shared 订阅和 Reader 回放
- 官方客户端读取 LZ4 / Zlib / Zstd / 原始 Snappy 压缩消息
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
- 独立 4.2.4 和 3.3.9 容器及专用命名空间中，`examples/memory_budget` 在 1 MiB 共享预算下通过顺序收发、分块接收和 Reader 回放。`scripts/test-chunk-live.sh` 还在两个版本上验证了消费者与 Reader 未完成分块过期 ACK、共享预算耗尽与释放、普通接收队列超额，以及新客户端回放恢复。
- `scripts/test-time-seek-live.sh` 在独立命名空间与隔离的 4.2.4、3.3.9 Broker 上验证消费者和 Reader 按发布时间向前、向后定位及回放。
- `scripts/test-chunk-interop-live.sh` 在隔离的 4.2.4 和 3.3.9 Broker 上，以 256 KiB 随机负载与固定版本官方 Go 客户端双向交换 LZ4、Zlib、Zstd、原始 Snappy 压缩分块，也与官方 Java 客户端双向交换 LZ4、Zlib、Zstd 压缩分块。Java 的 Zlib 同步刷新流由 native 系统 zlib 适配层解码。
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
| `examples/athenz` | Athenz ZTS 服务私钥交换或读取 sidecar 角色令牌文件 |
| `examples/athenz_cert` | 使用客户端证书向 ZTS 换取角色令牌 |
| `examples/auth_challenge` | 应答 Broker 发出的认证挑战 |
| `examples/memory_budget` | 真实 Broker 上的生产、接收、分块和 Reader 共享预算往返测试 |
| `examples/chunk_interop` | 与官方 Go、Java 客户端双向交换压缩分块消息 |
| `examples/tls` | 验证 TLS 收发、可选客户端证书/私钥及 broker 重启检查 |
| `examples/token_rotation` | 同一客户端下验证无效 Token 拒绝和文件 Token 轮换 |
| `examples/token_reconnect` | 文件 Token 与 Broker 签名密钥轮换后，原有生产者和消费者恢复 |
| `examples/producer_compression` | Zstd 压缩负载 |
| `examples/reader` | 从头回放 Topic |
| `examples/encryption_interop` | Go/MoonBit 加密互通、Reader 回放及重试/死信转发 |
| `examples/ec_encryption_interop` | Java/MoonBit P-256、P-384 或 P-521 ECIES 双向加密互通 |
| `examples/avro_evolution_interop` | Java v1/MoonBit v2 和 MoonBit v1/Java v2 的 Avro 演进互通 |
| `examples/transaction_participants` | 在事务中注册生产者及订阅参与者 |
| `examples/transactional_batch_ack` | 验证整批或逐索引事务 ACK 的回滚重投和提交（`PULSAR_BATCH_INDEX_ACK=true`） |
| `examples/time_seek` | 在真实 Broker 上验证消费者与 Reader 按时间 seek |
| `examples/live_capabilities` | 多 Topic、模式订阅、ACK 超时、seek、分块、事务和可选 Admin REST 的真实 broker 回归场景 |
| `examples/pattern_removal` | 删除模式订阅中的非分区和分区 Topic，重建后再次接收 |

运行示例：

```sh
moon run examples/producer --target native
```

连接 TLS broker 时设置 `PULSAR_TLS_URL`；示例默认使用系统信任根校验证书，私有
CA 可通过 `PULSAR_TLS_CA_FILE` 指定。双向 TLS 同时设置
`PULSAR_TLS_CLIENT_CERT_FILE` 与 `PULSAR_TLS_CLIENT_KEY_FILE`；Token 认证仍可设置
`PULSAR_TOKEN`。可通过
`PULSAR_TOPIC`、`PULSAR_SUBSCRIPTION` 隔离测试资源。设置
`PULSAR_TLS_RECONNECT_CHECK=1` 后，在首次收发完成的提示出现时重启 broker；
示例等待 30 秒，再用同一客户端发送和接收。仅在一次性本地 broker 上通过
`PULSAR_TLS_INSECURE=1` 显式关闭证书校验。

`examples/token_rotation` 从 `PULSAR_TOKEN_INITIAL_FILE` 和
`PULSAR_TOKEN_NEXT_FILE` 读取两个有效 Token，并复制到一次性工作文件。它先验证
broker 拒绝轮换后的无效 Token，再让同一客户端用第二个有效 Token 建立新连接并收发。
可设置 `PULSAR_URL`、`PULSAR_TOPIC` 和 `PULSAR_SUBSCRIPTION` 隔离测试资源；
示例不会修改传入的 Token 文件。

`examples/token_reconnect` 还使用 `PULSAR_RECONNECT_READY_FILE` 和
`PULSAR_RECONNECT_DONE_FILE`。首次消息收发并确认后，它将第二个 Token 复制到私有
工作文件并创建 ready 文件。在隔离 Broker 上把签名密钥切换为第二个 Token 对应的密钥，
重启并确认健康后创建 done 文件。示例验证旧 Token 的新连接被拒绝，以及原有生产者
和消费者重连后继续收发与确认。该场景在隔离的 4.2.4 和 3.3.9 Broker 上通过；
测试期间需保留两个输入 Token 文件，示例只修改自己的工作文件。
在独立 Linux 测试机上，以 root 运行 `scripts/test-token-reconnect-live.sh 4.2.4`
或 `scripts/test-token-reconnect-live.sh 3.3.9` 可自动执行该场景；脚本使用 7665、
8086 端口，生成一次性签名密钥和 Token，并在结束时清理容器与密钥。

在独立 Linux 测试机上运行 `scripts/test-chunk-live.sh 4.2.4` 或
`scripts/test-chunk-live.sh 3.3.9`，可用 7665、8086 端口上的一次性 Broker
验证消费者与 Reader 的未完成分块过期 ACK、共享预算耗尽与释放、普通接收队列超额与回放，以及完整分块消息往返；脚本结束时清理容器。

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

Mock 测试覆盖握手超时、Broker 拒绝 CONNECT、认证挑战次数上限，以及非空 Topic
上 Reader 包含起始 ID 的回放，以及事务协调器未启用或 ID 越界。隔离 Linux 测试机可运行
`scripts/test-athenz-broker-live.sh 4.2.4`（或 `3.3.9`），验证签名角色令牌及服务 NToken 换取令牌后对 Athenz 认证 broker 的收发。
`scripts/test-auth-challenge-live.sh 4.2.4`（或 `3.3.9`）在隔离 Broker 上发出真实 `AUTH_CHALLENGE`，验证刷新应答成功及错误应答被拒绝。

GitHub Actions 会在 push 和 pull request 时运行 native 类型检查、debug/release 测试、格式检查、生成接口检查、文档生成和打包。仓库中的 localhost TLS 私钥与证书是公开的一次性测试材料，绝不能用于真实 broker；Mooncakes 包不包含这些材料，也不包含测试和示例。

`proto/` 中的协议层由 `protoc-gen-mbt` 根据从 apache/pulsar 引入的 `proto/PulsarApi.proto` 生成：

```sh
protoc --mbt_out=. --mbt_opt=project_name=proto proto/PulsarApi.proto
```

## 许可证

Apache-2.0。从 Apache Pulsar 引入的协议定义及其生成的绑定所需署名见 [NOTICE](NOTICE)。
