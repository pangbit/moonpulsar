# 原生依赖调整执行结果

日期：2026-09-28。基线：`8b4eeb6692b25354d68abf1a18af3295c5b40030`。

本批部分完成，不能据此宣告 Mooncakes 发布就绪。OpenSSL 开发头文件依赖已移除并通过本机验收；zlib 开发头文件与原有 native 回退仍保留。没有提交、推送、发布或启动 Docker。

## 已实现

- TLS/ECIES 共用自包含的 `native_support/openssl_abi.h`，移除两个包的 Homebrew include 路径。运行时仍动态加载 OpenSSL 3；明确区分缺库、缺符号和不兼容版本，失败时清理句柄和函数指针。
- RSA PEM 解析按格式及算法标识分流，不再先探测 ECIES。RSA 加解密不触发 ECIES 的 OpenSSL 加载。
- 增强 TLS（方案中的“定制 TLS”）和 ECIES 使用时缺库会报错，不降级。普通 TLS 仍沿用 async/tls 的平台要求。
- 根包生成接口未改变。`native_ecies` 子包新增公开的 `availability_error() -> String?`，由 `moon info` 生成接口文件。
- 增加 ABI 对照、负对照、无 OpenSSL 头文件编译、加载失败 ASan 检查和发布 ZIP 外部消费者检查脚本；更新 README、能力矩阵及 CI 配置。

## 本机验证

环境：macOS arm64、Apple Clang 21、OpenSSL 3.6.3；Moon 0.1.20260920（914d7da）、moonc v0.10.14+7d59c7ec9。脚本显式固定 async 0.22.1 和 flate 0.8.3，未升级项目依赖。

| 验证 | 结果与限制 |
| --- | --- |
| 变更前基线 debug/release | 各 323/323 |
| 变更后完整 debug/release | 各 323/323；不代表真实 Broker 测试已执行 |
| TLS/消息加密定向回归 | 18/18 |
| check、info、fmt、doc | 通过；check 使用 `--deny-warn`，根接口无差异 |
| 官方头文件 ABI 对照 | OpenSSL 3.6.3/Clang 通过；篡改常量的负对照按预期失败 |
| 禁止包含 OpenSSL 头文件 | 产品 C stub 编译通过；宿主仍有系统 SDK 和 zlib 头文件 |
| 加载失败与清理 | TLS/ECIES × 缺库/缺符号/错误版本共 6 个 ASan 场景通过；不是全部功能路径的 ASan 覆盖，未启用 leak sanitizer |
| 未修改的发布 ZIP | 在新建外部消费者工作区 debug/release 构建通过；未运行真实 Broker 收发 |
| ZIP 副本注入缺库 | debug/release 各执行 RSA/ECIES 与 TLS 两个独立场景，共 4 次通过；脚本核验每次实际执行 1 项测试 |

证据目录：`/private/tmp/moonpulsar-native-implementation.AFMgx5`，其中 `debug.log`、`release.log`、`openssl-tests.log`、`abi-pinned/`、`deps-pinned/` 为本次结果。基线日志为 `/private/tmp/moonpulsar-native-baseline-{debug,release}.log`。临时目录可能被系统清理。

外部验收 ZIP：`_build/publish/pangbit-moonpulsar-0.1.0.zip`，SHA-256：

```text
cd3497032ec1858cab8c0700e932d1a81f1e6eecf7e1d345e64e9e6eccbd6c0a
```

## 停止关口与未验证项

P1 未通过：flate 0.8.3 对合法同步刷新边界和截断块头暴露相同的 `NeedMoreInput`、消费量、输出量、完成状态及四字节后缀。当前拟用判据无法区分二者，不能安全替换 native_zlib。详见 [反例与最小上游接口需求](ZLIB_BOUNDARY_FINDING.md)。这不是对所有可能适配方案的不可能性证明；没有提交上游 issue/PR 或修改依赖缓存。

因此 P3 未实施，纯 MoonBit 解码器的完整语料、差分和性能验收尚未开展。P4 只完成 OpenSSL 的本机部分检查，完整无开发包/无运行库/完整运行库环境矩阵未完成。注入缺库证明加载失败行为，不等同于在真正缺库的 Linux 镜像上验证。

`docker info` 无法连接 `/Users/xubochen/.orbstack/run/docker.sock`。本次未启动 Docker；Linux OpenSSL 3.0/GCC ABI、完整环境矩阵以及 P5 真实 Broker 互通未执行。已编写 CI 检查，但没有推送，远端 CI 尚未执行。

下一步先评审并解决 flate 边界状态接口或另选可证明的解码方案，过 P1 后再实施 P3；待容器环境可用后补齐 P4/P5，才能重新评估整体发布条件。

## 复现命令

从仓库根目录执行，ABI 检查需要官方 OpenSSL 3 头文件作为对照；这是维护者验证依赖，不是产品构建依赖。

```sh
moon check --target native --deny-warn
moon test --target native
moon test --target native --release
moon run scripts/probe-zlib-boundary.mbtx
moon run scripts/verify-native-abi.mbtx -- --report-dir /tmp/moonpulsar-abi-review
moon run scripts/verify-native-deps.mbtx -- --openssl-only --report-dir /tmp/moonpulsar-deps-review
moon info
moon fmt --check
moon doc
git diff --check
```

边界探针退出 0 表示成功复现阻塞反例，不表示 Zlib 替换可行。依赖验收脚本要求显式 `--openssl-only`，防止部分通过被误读为完整方案通过。

原有三份性能文档修改及两个未跟踪性能报告目录已保留，没有纳入本批实现范围。
