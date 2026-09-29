# 可运行示例

[English](README.md) | 简体中文 | [文档索引](../docs/README.zh-CN.md)

以下命令均从仓库根目录执行。


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
docker run --rm -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:4.2.4 bin/pulsar standalone -a 127.0.0.1
```
