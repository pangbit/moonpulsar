# 开发与验证

[English](testing.md) | 简体中文 | [文档索引](README.zh-CN.md)

命令从仓库根目录运行。`moon test --target native` 运行本地测试与进程内 mock Broker。可选真实 Broker 测试需要显式环境输入；缺少输入时通过测试，不代表完成真实 Broker 验证。场景见[示例说明](https://github.com/pangbit/moonpulsar/blob/main/examples/README.zh-CN.md)，覆盖边界见[能力矩阵](capabilities.md)。

## 环境与本地检查

需要支持 native 后端的 MoonBit 工具链、C 编译器与系统 SDK/libc。TLS/ECIES 运行库要求见 [README](../README.zh-CN.md)。克隆后运行 `moon update`。Linux 先按下文用有限的 32 MiB 编译栈生成测试程序。发布候选版本执行：

```sh
moon check --target native --deny-warn
moon test --target native
moon test --target native --release
moon info && moon fmt
moon doc
moon package --list
moon package
moon run scripts/check-doc-links.mbtx -- --public --archive _build/publish/pangbit-moonpulsar-0.1.3.zip
git diff --check
```

准备其他版本时使用对应候选 ZIP 文件名。真实 Broker 脚本可能创建、重启或删除隔离容器；先阅读其说明，只在获准使用的测试环境执行。

### Linux 编译器栈

moonc `0.10.18+6c83a7f62` 编译完整 native debug 白盒 IR 时，8 MiB 进程栈可复现
`link-core` 栈溢出；同一编译器、IR 和参数在 32 MiB 下成功生成对象。
这证明该输入对栈限制敏感，不代表所有构建的最低栈要求，也尚未证明是相对旧编译器的回归。

CI 对 debug/release 测试的编译阶段采用同一有限预算：

```sh
prlimit --stack=33554432:33554432 -- moon test --target native --build-only
moon test --target native
prlimit --stack=33554432:33554432 -- moon test --release --target native --build-only
moon test --release --target native
```

`prlimit` 只影响构建子进程及其子进程；后面的测试命令复用产物，并使用正常进程栈，
保留对运行时栈问题的检查。每对命令使用相同输出目录和构建参数，无需修改系统级或持久限制。
本地 Linux 验证中，debug 套件在明确限制为 8 MiB 的栈下报告 **398 项通过**；
其中六项真实 Broker 可选用例因缺少环境输入而直接返回。Release 测试程序也已在有限编译栈下
成功构建，未运行 release 测试。这些结果用于验证 SDK 兼容性；该规避配置不改变协议、
生成绑定、用例选择或断言。

### 连接关闭诊断

设置 `MOONPULSAR_CONNECTION_DIAGNOSTICS=1` 可在读取错误转换为 `ConnectionClosed` 前
输出帧读取/dispatch 错误类别，以及存在时的八位 OpenSSL 错误码或数字 OS errno。
客户端帧读取区分 CONNECT 握手与后台 reader 阶段；诊断省略对端地址和自由格式错误文本。
CI 在 native debug 测试中启用诊断并记录 runner 的 OpenSSL 版本，release 测试保持默认设置。
自定义 CA 重连测试在 `5b6d0bb` 和 `76aa6dd` 的 CI 中失败；本地定向检查通过，
包括使用相同 libssl 构建的检查，根因仍未确定。诊断日志不改变该断言、重试策略或 TLS 校验。

## 维护约定

- 修改聚焦具体问题；行为修复应包含有意义的回归验证。
- 公开 API 提供文档注释与示例，同步双语文档中受影响的行为。
- `proto/top.mbt` 和 `pkg.generated.mbti` 使用工具重新生成，不手工修改。
- 检查公开接口差异、兼容性变化及未验证的平台。
- 提交标题采用 `fix:`、`feat:`、`test:`、`docs:` 等前缀并描述具体变化。
- 保留第三方许可与署名；不引入凭据、生产日志或真实私钥。

可选本地 hook 在提交前运行 `moon check`，不能替代上述完整检查。从仓库根目录启用：

```sh
chmod +x .githooks/pre-commit
git config core.hooksPath .githooks
```

一般 Bug 与使用求助通过 [GitHub Issues](https://github.com/pangbit/moonpulsar/issues)反馈。安全问题先阅读 [SECURITY.md](../SECURITY.md)。

## 测试场景与协议生成

本地压缩、路由、批处理微基准及真实 Broker 收发测量见[性能说明](performance.md)。

Mock 覆盖握手超时、Broker 拒绝 CONNECT、过多认证挑战、非空 Topic 的 Reader 包含起点回放，以及事务协调器禁用或 ID 越界等场景。

在一次性 Linux 测试环境，`scripts/test-athenz-broker-live.sh 4.2.4`（也可用 `3.3.9`）验证隔离 Athenz Broker 上的签名角色令牌与服务 NToken 交换。`scripts/test-auth-challenge-live.sh 4.2.4`（也可用 `3.3.9`）编译一次性 Broker 认证提供者，验证真实二进制 `AUTH_CHALLENGE`、成功刷新与错误响应拒绝。

GitHub Actions 使用 Linux（Ubuntu 24.04），在推送和 PR 时执行 native 检查、debug/release 测试、格式与生成接口检查、文档生成及打包。仓库中的 localhost TLS 密钥与证书为公开的一次性测试夹具，不能用于真实 Broker；夹具、测试和示例均排除在 Mooncakes 包外。

`proto/` 中的协议层由来自 apache/pulsar 的 `proto/PulsarApi.proto` 使用 `protoc-gen-mbt@0.3.0` 和 README 指定的 SDK 生成。本次记录使用 `protoc 29.3`。先生成到独立目录，避免生成器的示例模块元数据覆盖本库配置：

```sh
mkdir -p _build/protocol-gen
moon install moonbitlang/protoc-gen-mbt@0.3.0 --bin _build/protocol-gen/bin
protoc --plugin=protoc-gen-mbt=_build/protocol-gen/bin/protoc-gen-mbt --mbt_out=_build/protocol-gen --mbt_opt=project_name=proto proto/PulsarApi.proto
cp _build/protocol-gen/proto/src/pulsar/proto/top.mbt proto/top.mbt
cp _build/protocol-gen/proto/src/pulsar/proto/moon.pkg proto/moon.pkg
moon fmt proto
moon info
```

检查生成源码与接口差异。生成器会显式导出 trait 方法；不要通过关闭警告或手改生成绑定掩盖兼容错误。普通构建无需重新生成协议。

## 历史真实 Broker 验证

[能力矩阵](capabilities.md)集中维护历史功能验证摘要，包括 mock、真实 Broker 覆盖及已知限制。具体运行证据见其链接报告和[性能报告](performance.md)；历史通过不能推导当前提交已验收。复现入口与环境变量见[示例说明](https://github.com/pangbit/moonpulsar/blob/main/examples/README.zh-CN.md)。

每次新验收应记录源码提交、工具链、平台、Broker 版本与配置、确切命令、结果和跳过的场景，并与报告一同保存。[发布流程](releasing.zh-CN.md)要求验证确切候选提交。

发布链接门槛使用 `--public --archive`，检查确切 ZIP 中 Markdown 文件的工作区版本与包内版本。省略 `--public` 可另行审计全部工作区，包括历史机器相关证据；公开文档检查通过不表示历史归档链接均可移植或有效。

## Producer 重放回归

根包回归覆盖重放期间接纳消息、部分写入失败后的重试，以及注册/重放期间关闭。
内存和 TCP 用例依赖共享白盒辅助代码，整理时需一起保留。复现历史 TCP 检查时，
分别用 `--index 0`、`--index 1`、`--index 2` 选择
`producer_replay_tcp_wbtest.mbt`；完整命令见[英文说明](testing.md)。
独立用例通过不能替代最终候选的常规全量 debug/release 执行。
[relay 夹具](https://github.com/pangbit/moonpulsar/blob/main/examples/pending_replay/README.md)
提供可选的本地文件/TCP 与原生入口检查；Python 控制器不能启动或验证真实 Broker。
生成的验收记录应保存在仓库外。
