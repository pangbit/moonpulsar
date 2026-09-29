# 开发与验证

[English](testing.md) | 简体中文 | [文档索引](README.zh-CN.md)

命令从仓库根目录运行。`moon test --target native` 运行本地测试与进程内 mock Broker。可选真实 Broker 测试需要显式环境输入；缺少输入时通过测试，不代表完成真实 Broker 验证。场景见[示例说明](https://github.com/pangbit/moonpulsar/blob/main/examples/README.zh-CN.md)，覆盖边界见[能力矩阵](capabilities.md)。

## 环境与本地检查

需要支持 native 后端的 MoonBit 工具链、C 编译器与系统 SDK/libc。TLS/ECIES 运行库要求见 [README](../README.zh-CN.md)。克隆后运行 `moon update`。发布候选版本执行：

```sh
moon check --target native --deny-warn
moon test --target native
moon test --target native --release
moon info && moon fmt
moon doc
moon package --list
moon package
moon run scripts/check-doc-links.mbtx -- --archive _build/publish/pangbit-moonpulsar-0.1.1.zip
git diff --check
```

准备其他版本时使用对应候选 ZIP 文件名。真实 Broker 脚本可能创建、重启或删除隔离容器；先阅读其说明，只在获准使用的测试环境执行。

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

`proto/` 中的协议层由来自 apache/pulsar 的 `proto/PulsarApi.proto` 使用 `protoc-gen-mbt` 生成：

```sh
protoc --mbt_out=. --mbt_opt=project_name=proto proto/PulsarApi.proto
```

## 历史真实 Broker 验证

[能力矩阵](capabilities.md)集中维护历史功能验证摘要，包括 mock、真实 Broker 覆盖及已知限制。具体运行证据见其链接报告和[性能报告](performance.md)；历史通过不能推导当前提交已验收。复现入口与环境变量见[示例说明](https://github.com/pangbit/moonpulsar/blob/main/examples/README.zh-CN.md)。

每次新验收应记录源码提交、工具链、平台、Broker 版本与配置、确切命令、结果和跳过的场景，并与报告一同保存。[发布流程](releasing.zh-CN.md)要求验证确切候选提交。
