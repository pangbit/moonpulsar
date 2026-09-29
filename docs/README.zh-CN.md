# 文档索引

[English](README.md) | 简体中文

从根目录的 [English README](../README.md) 或[中文 README](../README.zh-CN.md) 开始安装和运行最小示例。

| 需要了解 | 入口 |
| --- | --- |
| 配置语义、默认行为与互通限制 | [客户端指南](client-guide.zh-CN.md) / [English](client-guide.md) |
| 功能支持范围、mock 与真实 Broker 验证边界 | [能力矩阵](capabilities.md) |
| 示例程序及运行参数 | [示例说明](https://github.com/pangbit/moonpulsar/blob/main/examples/README.zh-CN.md) / [English](https://github.com/pangbit/moonpulsar/blob/main/examples/README.md) |
| 开发检查、协议生成和历史验证记录 | [开发与验证](testing.zh-CN.md) |
| 性能测试方法、测量口径和结果 | [性能测试](performance.md) |
| Bug 反馈与使用求助 | [GitHub Issues](https://github.com/pangbit/moonpulsar/issues) |
| 安全报告 | [安全说明](../SECURITY.md) |
| 版本变化与发布步骤 | [变更说明](../CHANGELOG.md) / [发布流程](releasing.zh-CN.md) |
| 原生依赖调整的方案与验收历史 | [2026-09 归档](https://github.com/pangbit/moonpulsar/blob/main/docs/archive/native-deps-2026-09/README.md) |

互通程序、测试夹具和原生 ABI 的局部说明保留在对应目录：
[Go](https://github.com/pangbit/moonpulsar/blob/main/interop/go/README.md)、[Java](https://github.com/pangbit/moonpulsar/blob/main/interop/java/README.md)、
[TLS 夹具](https://github.com/pangbit/moonpulsar/blob/main/testdata/tls/README.md)、[加密夹具](https://github.com/pangbit/moonpulsar/blob/main/testdata/encryption/README.md)、
[Athenz 夹具](https://github.com/pangbit/moonpulsar/blob/main/testdata/athenz_cert/README.md)、[OpenSSL ABI](../native_support/README.md)。

维护约定：README 保留摘要；配置语义在客户端指南维护，验证范围在能力矩阵维护，具体运行证据保留在所属批次。历史报告的通过结论仅适用于报告记录的版本与环境。仓库中的示例、脚本、归档及性能结果不随库发布；这些资料可从[源码仓库](https://github.com/pangbit/moonpulsar)查阅。
