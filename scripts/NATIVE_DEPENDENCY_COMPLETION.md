# 原生依赖方案最终验收（2026-09-28）

用户授权在 `root@192.168.12.111` 的隔离环境继续执行并保持本地提交记录。
历史基线和中途停止原因保留在 [分批结果](NATIVE_DEPENDENCY_RESULT.md)。
**P0–P5 本地及指定 Linux 验收完成。** 同一候选 ZIP 已通过 Linux A/B/C 和 macOS 外部消费者及缺库检查，双版本真实 Broker 互通通过。云端 CI 尚未触发，未执行发布。

完整回归：macOS 与 Linux 的 `moon test`、`moon test --release` 均为 **326/326**；`moon check --deny-warn`、`moon info`、`moon fmt --check`、`moon doc` 与 diff 检查通过。根 `.mbti` 无变化，README 软链接保留。

最终 ZIP SHA-256：

```text
e151bc957e2e727e8bd0d0ebe606ad3b76cb6c21a64f33d2863ba169d416f6aa
```

Linux 保存于 `reports/matrix-final-r2/candidate.zip`，macOS 保存于 `/private/tmp/moonpulsar-native-final.zip`。两端摘要分别归档在 `linux-matrix/archive-sha256.log` 与 `macos-archive/archive-sha256.log`；状态和实际命令输出均随本报告提交。ZIP 不含 native_zlib、验收脚本或测试夹具，包含共享 ABI 头；保留原有打包策略的历史性能资料，不夹带用户尚未提交的性能修改。

最终原始报告目录：Linux `reports/matrix-final-r2/`、`reports/interop-final-4.2.4/`、`reports/interop-final-3.3.9/`、`reports/zlib-final/`；macOS `/private/tmp/moonpulsar-final-macos/`。最终统一入口退出码为 0。容器清理后只剩运行前已有的 `moonpulsar-test` 和 SafeLine 服务。

## 实现与契约

- P1/P3：增加内部 DEFLATE 结构扫描器，证明同步刷新边界后，仅修改已知末尾空 stored block 的 BFINAL 位，调用已发布 flate 0.8.3 的有界解码 API。移除产品 `native_zlib` 包、C stub 和 import；不修改依赖缓存或升级依赖。完整流继续校验原有 zlib trailer。详见 [边界证明](ZLIB_BOUNDARY_PROOF.md)。
- P2：此前提交 `d2bb674` 已移除 OpenSSL 开发头文件要求。TLS/ECIES 在使用时加载 OpenSSL 3，RSA 不先探测 ECIES。`ccf4214` 修复的 native Zlib 实现仅保留为排除打包的差分参考。
- 基础 native 编译需要 C 编译器及 SDK/libc，无需 OpenSSL/zlib 开发包；普通 TCP、RSA、四种压缩不需要这些运行库。普通 TLS 保留 async/tls 平台要求；增强 TLS/ECIES 仍需要 OpenSSL 3。
- 根公开接口无差异；本次移除未发布的 `native_zlib` 子包。此前 `native_ecies.availability_error` 的新增已在分批报告记录。

## 环境与证据边界

macOS arm64：Apple Clang 21、OpenSSL 3.6.3。Linux x86_64：Ubuntu 24.04、GCC 13.3、OpenSSL 3.0.13。两端 Moon 0.1.20260920（914d7da）、moonc v0.10.14+7d59c7ec9；async 0.22.1、flate 0.8.3。

Linux 工作区为 `/tmp/moonpulsar-native-plan-20260928/source`，报告根目录为同级 `reports/`。只创建和清理本批命名的容器；没有修改或重启既有 Broker、安全服务。原始日志仍留在测试机器；关键证据归档于 [native-deps-results/2026-09-28](native-deps-results/2026-09-28/)。不会归档临时证书私钥。

实际 A 组使用可达的固定 Ubuntu 镜像：
`hub.chinaddos.com/ubuntu@sha256:95fc3d15a7c1c081b6038983ce83b1bf6cfc05c6ce7fb47737223de4ce90f6eb`。
CI 默认使用官方 Ubuntu 24.04 amd64 manifest
`ubuntu@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4`。
本次实测前者；不把它说成已执行后者的云端 CI。

## 验收范围

| 关口 | 实际检查 |
| --- | --- |
| P1 格式与差分 | 1000 个确定性 zlib 输入，sync-flush/完整流共 2000 条；18000 次位变异对照固定 native 解码器；错长度、截断、坏头、坏校验和、空载荷、伪造后缀、多次 flush；常规小向量检查每个截断位置 |
| P2 ABI | Linux 官方 OpenSSL 3.0.13 头文件与 GCC 对照、篡改常量负对照、禁止包含头文件的编译；TLS/ECIES × 缺库/缺符号/错误版本共 6 个 ASan 加载检查。macOS 对照见分批报告 |
| P4 A | 新构建目录，检查包清单及实际编译器头文件搜索；依赖缓存后断网，从未修改发布 ZIP 构建外部消费者 debug/release；真实 mock TCP/RSA/Zlib |
| P4 B | scratch rootfs 不复制 SSL/crypto/zlib；检查 ldd 和六个 dlopen 候选名；debug/release 基础功能成功，增强 TLS/ECIES 缺库明确失败 |
| P4 C | B 的 rootfs 只补 OpenSSL 两个运行库，无开发头文件；每种构建执行 11 项 TLS/基础检查和 8 项消息加密检查 |
| P5 双版本 | Pulsar 4.2.4、3.3.9：mTLS 完整收发，缺客户端证书、错 CA、错主机名拒绝；Java P-256/P-384/P-521 双向 ECIES，Go RSA 双向、压缩、批次、空载荷既有边界 |
| P5 压缩分块 | 两版均 Go LZ4/Zlib/Zstd/Snappy 与 Java LZ4/Zlib/Zstd 双向成功 |

常规测试中环境保护的历史 live tests 不作为真实 Broker 通过证据；P5 的独立脚本实际执行全部强制场景，没有缺环境即成功的跳过路径。ASan 覆盖 C 加载/参考代码，不声称覆盖纯 MoonBit 扫描器的全部执行路径。

## 有界性能与内存

macOS release，同机同编译配置，zlib 1.2.12 默认 level 6。每格耗时为 5 轮中位数，每轮 20 次；RSS 为 5 个新进程中位数，每个进程解码 200 次。旧路径包含原有 flate 尝试和 native 回退，不是单独的 C 调用。完整原始耗时轮次保留于 `zlib-differential.log`。

| 内容 | 旧耗时 µs | 新耗时 µs | 新/旧 | 旧 RSS KiB | 新 RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 KiB 重复 | 6.854 | 1.235 | 0.180 | 2896 | 2656 |
| 1 KiB 伪随机 | 3.183 | 0.679 | 0.213 | 2880 | 2656 |
| 32 KiB 重复 | 105.598 | 15.971 | 0.151 | 2944 | 2816 |
| 32 KiB 伪随机 | 11.113 | 4.625 | 0.416 | 2960 | 3136 |
| 256 KiB 重复 | 500.935 | 58.456 | 0.117 | 3968 | 3200 |
| 256 KiB 伪随机 | 40.869 | 20.635 | 0.505 | 4240 | 3968 |

六组耗时均无回退；RSS 最大增加 5.95%，未触发 20% 工程门槛。RSS 包含运行时和夹具，不是精确分配字节数，也不是 Broker 吞吐 SLA。语料 SHA-256：`c4d78de4728cb2f70949ec3e393d41288875749bce215224dbd5e6eb2a67fbc6`。

Linux/GCC 使用系统 zlib 1.3 重新生成语料并完成同一差分、位变异与 60 次独立进程 RSS 检查。六组耗时新/旧中位数为 0.029、0.015、0.108、0.076、0.110、0.205；RSS 最大增加约 2.1%。原始轮次与中位数分别保存在 `linux-zlib-differential.log`、`linux-zlib-rss-rounds.log` 和 `linux-zlib-metrics.json`。这是共享主机的有界比较，不作跨系统绝对速度比较。

## 发现的问题与修复

Linux 最初全套测试失败：两项 producer 用 150/200 ms 预算和 130 ms 墙钟断言，在该共享主机上不稳定；顺序模式也能复现。改为直接断言 admission 后的新 deadline 和 flush 前后 deadline 相等，同时保留真实 RequestTimeout/回执检查，使用 2 秒超时预算。没有放宽产品行为。

Athenz TLS mock 的 close-notify 清理可能因客户端提前关闭而终止接受重试，首轮出现 Connection refused；只让该清理错误不阻断下一次 accept，HTTP/TLS 主流程断言保留。最初失败日志保留，后续整套复验单独记录。

维护脚本调试期间修正了构建镜像缺 git、内存检查测试程序路径、统一入口与子入口重复创建报告目录、Broker 就绪探针外层超时过短，以及缺客户端证书既可能报 ConnectionClosed 也可能 ConnectFailed 的断言。最终脚本仍先证明 TCP 可达，并实际拒绝错误证书；不以连接不到服务器冒充证书验证。

本地提交记录连续保留：`d2bb674` OpenSSL 去头文件依赖 → `ccf4214` 原生 Zlib 边界修复 → `217edcc` 纯 MoonBit Zlib 替换 → `42c4ff5` 可重复的超时/TLS 重试回归。后续验收脚本、CI 和证据单独提交。

## 复现入口

```sh
moon check --deny-warn
moon test
moon test --release
moon info && moon fmt
moon doc
moon run scripts/verify-native-abi.mbtx -- --report-dir /tmp/moonpulsar-abi
moon run scripts/verify-zlib-boundary.mbtx
MOONPULSAR_ZLIB_REPORT=/tmp/moonpulsar-zlib-gate moon run scripts/verify-zlib-replacement.mbtx
moon run scripts/verify-native-deps.mbtx -- --report-dir /tmp/moonpulsar-native-matrix
moon run scripts/verify-native-interop.mbtx -- --pulsar-version 4.2.4 --report-dir /tmp/moonpulsar-interop-424
moon run scripts/verify-native-interop.mbtx -- --pulsar-version 3.3.9 --report-dir /tmp/moonpulsar-interop-339
bash scripts/test-chunk-interop-live.sh 4.2.4
bash scripts/test-chunk-interop-live.sh 3.3.9
```

每次选择新报告目录；两版 Broker 顺序执行。离线站点显式设置 `MOONPULSAR_NATIVE_BASE` 为可达且固定 digest 的 Ubuntu 基础镜像。macOS 使用 `verify-native-deps.mbtx -- --openssl-only --archive /path/to/same.zip --report-dir /tmp/moonpulsar-macos` 验证 Linux 产出的同一 ZIP。

CI 已加入独立 A/B/C、两平台 ABI/差分、双 Broker 加密与分块互通作业。本地提交不包含用户原有性能文档及报告；未推送，GitHub Actions 尚未触发，未执行 Mooncakes 发布。Windows、完整密码学审计和历史能力矩阵中的其他未支持能力不在本方案完成声明内。
