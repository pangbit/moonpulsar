# 客户端配置与限制

[English](client-guide.md) | 简体中文 | [文档索引](README.zh-CN.md)

本文说明配置语义与互通限制；验证范围以[能力矩阵](capabilities.md)为准。

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
- LZ4（帧格式）、Zlib、Zstd 和原始 Snappy 压缩；Snappy 的兼容性限制见下文
- 分区 Topic：可选 key 哈希（默认）、轮询、固定单分区或自定义回调路由；生产者可读取最后确认的序列号；消费者聚合全部分区，生产者和消费者可发现新增分区（默认每 60 秒轮询）
- 可选的生产者分块和消费者/Reader 分块重组，包括压缩负载；`ChunkAssemblyPolicy` 可限制待组装消息数，并在没有新消息时清理过期分块
- Reader：从指定消息 ID 开始的非持久回放（可选择包含起点）、seek、`has_message_available`、最后消息 ID、自定义名称/属性/订阅、分区聚合和新增分区发现
- TableView：从压缩后的 Reader 获取原始字节或 Schema 解码的键值快照、实时更新、墓碑删除和变更监听器；类型化监听器以 `Err` 报告解码失败
- 事务：协调器归属查找、`new_transaction` / `commit` / `abort`、事务性发送与 ACK
- Admin REST API：创建/删除 Topic 及管理属性，创建/扩容/删除分区 Topic 及查询元数据，列出 Topic 与分区 Topic，创建/列出/删除订阅，跳过或过期订阅积压，以及查询 Topic 与分区 Topic 统计信息
- Schema 声明：生产者/消费者创建时声明 `SchemaInfo`；`SchemaCodec[T]` 支持 STRING、JSON、BYTES、数值原始类型、Avro、Protobuf Native 和传统 Protobuf 的类型化编解码，也可传入自定义回调
- 消息加密：`MessageCrypto` 读取 RSA 或 EC PEM 密钥，按接收方名称用 RSA-OAEP-SHA1 或兼容 Pulsar Java 的 ECIES 包裹每条消息的新 AES-256-GCM 密钥；消费者、Reader 和 TableView 解密。加密生产者需传入 `message_crypto` 与 `encryption_key_names`，加密重试/死信消费者也需这两个选项，以便转发时重新加密。消费者与 Reader 可设置 `decryption_failure_action=Fail | Consume | Discard`。
- 延迟投递：`ProducerMessage` 的 `deliver_at` / `deliver_after`
- Reader 按时间戳 seek

## 已知限制

- Pulsar 消息中的 Snappy 使用原始 block，与固定版本 Go 客户端一致；公开的 `snappy_compress`、`snappy_decompress` 辅助函数仍处理 Google framed stream。Java 客户端使用 xerial framing，不能解码原始 block；官方 Java 与 Go 客户端也存在这一差异。
- 测试所用的 Pulsar 4.2.4 broker 在已有非分区 Topic 上创建分区元数据时返回 HTTP 409。需要转换时，应把数据迁移到新的分区 Topic。
- 单 Topic、多 Topic 和模式消费者可通过 `retry_topic` 自动加入重试 Topic，也可使用显式 `create_retry_consumer`。Athenz ZTS 服务私钥模式签发 RSA NToken；证书模式使用 `AthenzZtsOptions::new(..., certificate_file=..., ca_pem_file=...)`、HTTPS ZTS URL 及证书私钥。两种模式都会请求角色令牌、重试暂时故障，并在过期前刷新；ZTS URL 填写 `/zts/v1` 之前的服务根地址。OAuth2 当前在每次连接或认证挑战时重新取令牌。外部 OAuth2 身份提供方及外部 Athenz 服务尚未验证。
- 替换 PEM 文件后再次调用 `MessageCrypto::load_public_key_file` 或 `load_private_key_file` 即可轮换密钥；新发送与解密使用新密钥。解密失败默认采用 `Fail`，返回 `InvalidFrame` 并停止受影响的消费者或 Reader。`Consume` 交付原始密文，`Message::decryption_failed()` 为 `true`，应用自行决定是否 ACK；加密批次无法按此方式交付。`Discard` 跳过消息，消费者发送单条 ACK，Reader 则只跳过而不发送 ACK。RSA-OAEP-SHA1 与 ECIES 包裹 AES-256-GCM 密钥；ECIES 使用 native OpenSSL 3 适配层。native 随机源是 `/dev/urandom`。固定版本 Go 客户端默认加密只接受 RSA。
- `MessageCrypto::new(public_key_reader=..., private_key_reader=...)` 可接收按名称读取 PEM 字节的同步回调；每次发送或解密都会重新读取，并优先于手动加入的密钥。返回 `None` 表示密钥不可用。
- `SchemaCodec::avro(definition, to_datum, from_datum)` 解析 Avro Schema 并提供 Avro 二进制记录的类型化映射；`avro_datum(definition)` 直接暴露完整的 Avro datum 模型。构造或匹配 datum 时，需添加 `yugonlian/moon-avro@0.3.0` 并导入其 `codec` 包。`Client::decode_avro(message, reader_codec)` 按消息携带的版本查询并按 Topic/版本缓存 writer schema，解析字段别名与默认值、联合类型、集合、enum 默认符号及允许的数值提升；不兼容时返回 `SchemaIncompatible`。同步 codec 仍按自身声明的 schema 解码。
- `SchemaCodec::protobuf_native(descriptor_set, root_file, root_message)` 使用二进制 `FileDescriptorSet` 和生成的 MoonBit Protobuf 类型；根消息必须存在于描述符中。传统 Protobuf 使用 Avro 风格 JSON Schema。INT16/32/64 默认采用与 Java 客户端一致的大端序，和 Go 客户端互通时设置 `little_endian=true`。
- 支持显式 `chunk_size` 或按 broker 协商上限自动分块；自动模式要求 broker 公布最大消息尺寸。`ChunkAssemblyPolicy::new(auto_ack_incomplete=true)` 让消费者和 Reader 在未完成分块过期或被淘汰时确认已收到的分块；默认不确认，交由 broker 重投。
- 批次消息默认在全部索引确认后发送整批 ACK；消费者设置 `enable_batch_index_ack=true` 后可逐索引确认，broker 需启用 `acknowledgmentAtBatchIndexLevelEnabled`。默认模式下，未完成整批的 ACK 不能请求 broker 确认。事务 ACK 支持两种模式，并等待每次 broker ACK 响应；broker 还需启用事务。
- 固定版本的 Go 客户端消费 MoonBit 发送的加密空载荷时，会返回 16 字节 AES-GCM 标签：其消费路径在 `UncompressedSize` 为零时未替换解密后的缓冲区。MoonBit 能正确解密 Go 发出的加密空载荷；运行 `interop/go/encryption receive-empty` 可复现 Go 侧限制。
- 消费者可设置 `ack_grouping=AckGroupingOptions::new(max_size=1000, max_time_ms=100)`，按数量或时间合并 ACK；默认不启用。要求 broker 确认的 ACK 和事务 ACK 会先冲刷缓存再立即发送；关闭或 seek 前冲刷，重连时丢弃未发出的 ACK 以便 broker 重投。
- `Consumer::ack_ids([id1, id2])` 将普通 ID 合入一个 ACK 帧，批次索引 ID 保持位图处理。`Consumer::last_message_ids()` 与 `Reader::last_message_ids()` 返回按来源 Topic（含分区 Topic）索引的最后 ID。零接收队列只适用于非分区单主题消费者；分区、多 Topic、重试和模式订阅会明确报错。
- `send_timeout_ms` 在通过消息数量和内存准入后、编码或批次缓冲前开始计时。同一个截止时间覆盖批处理、所有分块以及重连/重放，flush 和重试不会重置它。等待准入的时间不计入，可由调用方取消来限制。到期后移除缓冲消息；帧内没有存活消息时停止重放。每个生产者使用一个截止时间调度器，已完成的消息会立即从索引截止时间堆中移除。SEND 后超时不代表 broker 一定拒绝消息，重试时应使用稳定的生产者名称和序列号。
- `ClientOptions::new(max_memory_bytes=...)` 在生产者、未完成分块、接收队列和 Broker 入站帧之间共享保留字节预算。`block_if_queue_full=false` 时生产者超额返回 `ClientMemoryFull`，否则等待回执、失败、超时、消息交付或客户端关闭释放额度。这是协议与消息数据的字节记账，并非进程堆内存的精确上限。
- 消费者设置 `auto_scaled_receiver_queue=true` 后，从 1 个 FLOW 许可开始；队列曾满且下一次读取时已空，预取上限倍增，直至 `receiver_queue_size`。此选项不能与零接收队列同时使用。组合消费者会向每个来源传递此选项，其合并队列仍单独缓冲。
- 创建生产者时传入 `interceptors=[ProducerInterceptor::new(before_send=..., on_send_result=...)]`。钩子按顺序作用于逻辑生产者；`on_send_result` 也会收到 `ProducerQueueFull` 等入队前错误。回调同步执行且不能抛错。
- 创建消费者时传入 `interceptors=[ConsumerInterceptor::new(before_consume=..., on_receive_error=..., on_ack=..., on_nack=...)]`。钩子在应用收取消息、`receive()` / `try_receive()` 失败，以及消息或 ID 的 ACK/nack 调用结束时触发；要求 broker 确认的 ACK 被拒绝时，`on_ack` 收到错误。未要求确认的 ACK 回调只代表帧已提交，不代表 broker 已确认。
- 使用 `let metrics = ClientMetrics::new()` 和 `ClientOptions::new(url, on_event=fn(event) { metrics.record(event) }, on_span=fn(span) { metrics.record_span(span) })` 统计结果次数及耗时；`metrics.prometheus_text()` 导出固定标签的 Prometheus 指标。`ClientSpanEvent::Started` 与 `Finished` 使用同一关联 ID、操作名、Topic 和可选消息 ID，结束事件还包含耗时、结果及错误文本。回调同步执行；`ClientMetrics::snapshot` 保留计数快照。
- `Connection::max_message_size` 可读取 broker 握手报告的上限；生产者会按此限制编码后的帧，并可自动选择分块大小。
- 普通 `pulsar+ssl://` 连接使用 `moonbitlang/async/tls`；证书身份和协议/密码选项启用仓库内 OpenSSL 3 native 适配层。配置客户端证书且没有其他认证提供者时，客户端自动在 Pulsar CONNECT 中声明 `tls` 认证方法。Windows 尚未验证；已记录的平台与 Broker 验证范围见能力矩阵。
