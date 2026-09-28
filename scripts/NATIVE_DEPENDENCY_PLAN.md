# MoonPulsar 原生依赖调整实施与验收方案

状态：部分实施；OpenSSL 调整通过本机验收，P1 Zlib 关口未通过，完整环境与互通验收未完成。日期：2026-09-28。详见 [执行结果](NATIVE_DEPENDENCY_RESULT.md)。
核查基线：`8b4eeb6692b25354d68abf1a18af3295c5b40030`。

## 1. 目标与范围

保留统一客户端 API，消除基础使用对 OpenSSL、zlib 系统开发包的强制要求。允许自包含的小型 C stub 参与编译；不把“零 C stub”作为目标。

完成后的用户契约：

- 基础 native 构建只需要 MoonBit、C 编译工具和系统 SDK/libc，不要求 OpenSSL、zlib 开发头文件。
- 普通 TCP 收发、纯 MoonBit 压缩和 RSA 功能不依赖 ECIES 所需的 OpenSSL 3 动态库。
- 定制 TLS 和 ECIES 实际使用时才要求 OpenSSL 3；缺失、版本不支持或符号缺失产生明确错误，不能降级为明文或跳过验证。
- 普通 TLS 沿用 `moonbitlang/async/tls`；其平台运行库要求仍然存在，不能宣传为“所有 TLS 都不需要系统库”。
- Java Pulsar 的 Zlib 同步刷新兼容能力保留，且不放宽损坏数据的接受条件。
- 根包公开 API 保持不变。适配子包接口若确需改变，逐项记录生成接口差异；不自动视为私有改动。

本批不做：拆分所有公开 API、增加通用插件系统、切换密码算法、Windows 支持、全面依赖升级、生产部署、提交/推送或 Mooncakes 发布。

原有工作区含性能文档修改和未跟踪报告。实施前重新核对并保留，不自动提交、清理或带入本批补丁。本方案位于已排除打包的 `scripts/` 下。

## 2. 已知事实和未决点

已确认：根包导入三个 native 包；普通 TCP 示例的构建命令会编译三个 stub。TLS/ECIES 包配置硬编码了两处 Homebrew 头文件路径。现有实现通过 `dlopen` 加载动态库。

已确认：`EncryptionPublicKey::from_pem` 和私钥解析会先尝试 ECIES 检测，再解析 RSA。因此缺库验收必须覆盖 RSA，不能只测试 TCP。

已确认：`compression_wbtest.mbt` 已有 Java sync-flush 的 hello 向量、错误长度和截断测试；`tls_wbtest.mbt` 覆盖 CA、身份、版本、密码及重连；`message_crypto_test.mbt` 覆盖 RSA、ECIES、多接收者与篡改。

未决：`flate@0.8.3` 的公开状态是否足以证明已经到达合法的 sync-flush 边界。`NeedMoreInput`、输出长度相等、尾部四字节匹配本身均不能单独作为证明。P1 必须解决该问题，才能承诺移除 `native_zlib`。

## 3. 执行顺序与关口

| 阶段 | 交付 | 通过条件 | 失败处理 |
| --- | --- | --- | --- |
| P0 基线 | SHA、工具链、依赖版本、测试清单和日志 | 当前行为可复现，已有改动已记录 | 先区分环境问题和既有失败，不边修边改变基线 |
| P1 Zlib 可行性 | 有界纯 MoonBit 解码原型、正反语料、边界证明 | 公开 flate API 足以可靠判定合法消息结束 | 保留 native 回退，本方案整体未完成；记录最小上游 API 缺口，不强行删除 |
| P2 OpenSSL | 自包含 ABI 声明、运行时加载与错误、ABI 比对测试 | 无开发头文件构建通过，ABI 与 OpenSSL 3 头文件一致 | 保留原实现；不得用猜测声明通过编译 |
| P3 Zlib 替换 | 根包接入纯 MoonBit 兼容路径，移除 native_zlib | P1 语料与原有压缩测试通过，运行路径无系统 zlib 依赖 | 回退本阶段局部补丁，不影响 P2 已验证工作 |
| P4 环境矩阵 | 无开发包、无运行库、完整环境三组结果 | 发布 ZIP 在外部消费者中通过每组断言 | 不把宿主已有依赖或缓存构建作为通过 |
| P5 互通与交付 | Broker 矩阵、文档、打包检查、证据汇总 | 全部强制门槛通过且无未解释接口变化 | 标记具体失败/未验证项，不宣告可发布 |

P1 和 P2 可分别准备，但 P3 必须等 P1 通过；P4 验证最终合并状态；P5 在 P4 后执行。实际执行默认顺序推进。

## 4. P0：固定可复现基线

记录 `git status --short`、`git rev-parse HEAD`、`moon version --all`、`moon tree` 和原有生成接口。使用当前声明依赖，不能静默升级；记录实际解析版本及来源。所有日志写入本次专属临时目录，交付时附证据摘要。

运行：

```sh
moon check --target native
moon test --target native
moon test --release --target native
moon test --outline compression_wbtest.mbt
moon test --outline tls_wbtest.mbt
moon test --outline message_crypto_test.mbt
```

本次对话上一轮已有 debug/release 各 323/323 的结果，实施时按实际基线重新记录。新增测试后不固定总数，但原有测试不得消失或被静默跳过。

构建类命令顺序执行，避免同一 `_build` 锁竞争。不要使用 `-f 文件名` 来选文件；当前工具链的 `-f` 是测试名称过滤，选文件使用位置参数。

## 5. P1/P3：去除 Zlib 系统依赖

修改范围：`compression.mbt`、`compression_wbtest.mbt`、根 `moon.pkg`；必要时新增内部文件 `zlib_sync_flush.mbt`。通过验收后删除 `native_zlib/` 中本批替代的实现及生成接口，移除相应 import。不手工编辑 `.mbti`。

正常 Zlib 解码继续使用现有 flate 路径。专门兼容逻辑只处理协议允许的 Java 同步刷新流：

1. 校验 RFC 1950 头和已有字典策略；不因进入兼容路径绕过错误头或字典要求。
2. 使用 flate 流式状态机，有界消费输入和输出；输出必须与声明长度一致，并检查现有 `MAX_CHUNKED_MESSAGE_BYTES` 限制。
3. 证明 DEFLATE 状态位于完整的同步刷新边界、输入全部消费、没有等待未完成符号或块。不能只检查 `00 00 ff ff` 后缀，也不能把普通 `NeedMoreInput` 当成成功。
4. 完整 zlib 流仍需通过校验和；校验和失败、截断 trailer、拼接垃圾不能被回退路径误接受。
5. 若现有 flate 无法暴露所需状态，记录最小 API/补丁需求与复现语料，停止移除步骤。对外提交上游 issue/PR 另行授权；不得修改 `.mooncakes` 缓存伪装成已发布依赖。

固定语料至少包括：已有 hello 向量；空载荷当前行为；1、32 KiB、256 KiB 的重复与确定性伪随机内容；多块、多次 flush；标准完整流；声明长度少一/多一；零/负值/超上限；每个位置的截断；无效头；坏校验和；非法 DEFLATE；伪造 flush 后缀；尾随垃圾和拼接流。

正样本保存输入与预期明文/摘要，记录生成器版本和参数。可以用固定版本官方 Java/zlib 工具产生语料，但交付的常规回归不依赖安装这些工具。变异输入不一律期待拒绝：它也可能成为另一条合法流，必须依据格式或参考解码器分类。

对确定性生成的至少 1000 个有效样本做新旧解码差分。对恶意语料按协议断言，不把旧实现可能存在的宽松接受当作标准。任何新增输出限制引起的行为变化都单独列出。

验证：

```sh
moon test --target native compression_wbtest.mbt
moon test --release --target native compression_wbtest.mbt
```

另做变更路径的有界耗时/内存比较：同机同编译配置，1 KiB、32 KiB、256 KiB 重复和伪随机输入，每项 5 轮，记录中位数与离散度。超过 20% 的稳定退化必须解释、优化或明确接受后才能过关；这是本次工程门槛，不是历史 SLA。无需重跑整个跨语言性能项目。

## 6. P2：OpenSSL 编译依赖与运行依赖分离

修改范围：两个 `native_*/native_*.c`、两个 `moon.pkg`，必要的 MoonBit 错误适配和测试。拟新增一个共享的最小声明头，例如 `native_support/openssl_abi.h`，不得复制整套 OpenSSL SDK。

具体步骤：

1. 整理现有全部动态符号、函数签名、宏展开和常量；记录对应 OpenSSL 3 官方头文件版本、路径、来源 SHA/哈希，必要时保留授权声明。
2. 对 SSL、CTX、BIO、EC_KEY 等对象只使用不透明指针，不复制内部结构布局。
3. 用显式函数指针类型替换依赖系统声明的 `__typeof__(&name)`；现有控制宏改为经核实的底层调用，避免从其他版本猜常量。
4. 去掉包配置的 Homebrew include 参数和产品源码中的 OpenSSL 头文件包含。编译不依赖开发包；运行时仍按已支持的平台路径寻找 OpenSSL 3。
5. 明确区分加载失败、版本不支持、缺符号、密钥格式无效、握手失败；只在完整解析所需符号后发布成功状态，失败路径释放本次获取的资源，不保留半初始化函数表。
6. 保持懒加载与现有线程/任务使用约束，不引入全局可变测试开关。TLS 失败沿用现有连接错误类型，ECIES 使用现有错误体系补充诊断；优先保持根 API 不变。
7. RSA 解析在没有 ECIES 动态库时仍成功。不能把“ECIES 不可用”误当 RSA 错误，或把本来支持的 EC 密钥误报成一般 RSA PEM 错误；密钥分类与可用性判断分开。

拟新增维护者专用 ABI 检查 C 文件，仅在装有官方开发头文件的测试环境编译。它使用与产品共享的符号声明表，逐项检查函数指针类型兼容性和常量值；宏包装分别检查底层函数类型与传入常量。检查文件不进入发布包。选择 OpenSSL 3.0 系列和当前受支持的一个较新 3.x 版本，固定具体版本/来源，至少覆盖 GCC/Linux 与 Clang/macOS。

必须有负对照：在临时副本故意改错一个签名或常量，ABI 检查应失败；正式源码不保留故意错误。这样证明测试确实比较了产品声明，而非两份相同手抄定义。

测试至少覆盖：缺库、错误版本、缺符号、连续两次初始化失败、合法密钥、坏 PEM、RSA-only、RSA+EC 混合接收者、P-256/P-384/P-521、加解密与篡改拒绝、TLS 1.2/1.3、CA/主机名校验、身份重载、缺私钥和错误密码套件。

ABI 检查和 loader 故障注入拟由 `scripts/verify-native-abi.mbtx` 编排。缺符号/错误版本使用隔离子进程和测试专用 shim/构建配置，不向产品 API 增加任意库路径注入功能；注入结果不冒充真实缺库环境结果。

已有回归命令：

```sh
moon test --target native tls_wbtest.mbt
moon test --target native message_crypto_test.mbt
moon test --release --target native tls_wbtest.mbt
moon test --release --target native message_crypto_test.mbt
```

## 7. P4：证明基础用户确实不再承担依赖

验证对象必须是 `moon package` 生成的 ZIP，而非源码目录链接。解压到仓库外的临时 workspace，以独立 consumer 模块依赖该库，使用全新的构建目录，不复制宿主 `_build`、Homebrew 或系统开发包。

拟新增 `scripts/verify-native-deps.mbtx`，负责打包/解压、创建消费者夹具、构建、子进程调用、断言和报告。新增自动化统一使用 `.mbtx`；容器配置放 `scripts/native-deps/`。这些都是待实施交付物，当前不存在。

| 环境 | 配置及环境断言 | 必须通过的场景 |
| --- | --- | --- |
| Linux A | 固定 digest 的 Ubuntu 24.04 干净构建镜像；有编译器/libc 开发工具；无 libssl-dev、zlib1g-dev；先缓存依赖再断网构建 | ZIP 消费者 debug/release 构建，普通 TCP 收发，纯 MoonBit 压缩/RSA |
| Linux B | 在 A 构建，运行时用独立最小 rootfs，只复制可执行文件、必要 loader/libc 等依赖及测试夹具；确实没有 libssl/libcrypto/libz | 基础 TCP、RSA、Zlib 兼容语料通过；定制 TLS、ECIES 明确失败；进程不崩溃、不降级 |
| Linux C | A 的构建条件，但运行时提供固定 OpenSSL 3 库；仍无开发头文件 | TLS/ECIES 全功能通过 |
| Linux D | 完整开发环境，固定 OpenSSL 头文件/库 | ABI 比对、完整回归、真实 Broker 互通 |
| macOS | Apple Silicon 上新构建目录；取消 Homebrew include 参数；以禁止包含外部头文件的哨兵/编译依赖跟踪补充检查 | ZIP 构建、系统 SDK 条件下基础功能、装有 OpenSSL 3 时完整 TLS/ECIES |

Linux A 的“无开发包”既检查包清单，也检查编译器实际搜索/包含路径，防止其他前缀留下头文件。编译器探测中包含 openssl/ssl.h 或 zlib.h 应失败，产品构建必须成功。安装工具或复制缓存后重新检查，避免工具依赖带回开发包。

Linux B 中动态链接依赖列表不得含 OpenSSL/zlib，rootfs 不复制这三类库；用显式 dlopen 探针检查候选名均不可加载，再运行功能测试。`ldd`/`otool` 只能证明启动链接依赖，不能独立证明 dlopen 不会成功。不要卸载或移动宿主库。

Linux B 的普通 TCP 验收由自包含消费者夹具启动本地 mock Broker 并完整执行 CONNECT、订阅、SEND/回执、MESSAGE/ACK 和关闭；不能把启动后连接失败视为通过。缺 TLS 库测试也须提供可连接的 TCP 端点，使代码真正走到动态加载，而不是提前返回 connection refused。

macOS 的负向运行库验证优先用隔离故障注入；明确标成模拟缺库。若没有真实缺库 macOS 环境，不把该项写成物理缺库实测。Intel macOS/其他架构未测时不新增支持承诺。

新脚本接口约定（实施后才可执行）：

```sh
moon run scripts/verify-native-abi.mbtx -- --report-dir /private/tmp/moonpulsar-native-audit/abi
moon run scripts/verify-native-deps.mbtx -- --report-dir /private/tmp/moonpulsar-native-audit/deps
```

实施脚本时先用本地 `moon run --help` 核实参数透传，并按实际解析形式同步本节命令。成功返回 0；任何断言失败或强制场景未运行返回非零。报告逐项写明 executed/passed/failed/blocked，不用“skip 后总命令为绿”的方式通过。

报告包含：源码 SHA 及补丁摘要、ZIP SHA256、镜像 digest、架构、MoonBit 版本、依赖版本、系统包清单、头文件与动态库探测结果、测试名/数量、退出码、日志路径。单场景超时有明确失败结果。

## 8. P5：真实互通、CI 和发布产物

原 CI 的 Broker 矩阵仅直接运行 chunk/seek 脚本，不能据此认为已覆盖此次 TLS/ECIES 和 Java 压缩改动。

在隔离 Linux 测试环境复用下列现有命令；它们创建测试 Broker，不能指向生产环境：

```sh
bash scripts/test-chunk-interop-live.sh 4.2.4
bash scripts/test-chunk-interop-live.sh 3.3.9
```

已有脚本用固定端口，两个版本顺序执行，或放到独立 CI runner，不能在同一 host 网络环境并行争抢端口。保存 Go/Java 双向消息摘要、明确成功输出与脚本退出码。

拟新增 `scripts/verify-native-interop.mbtx`，把 `interop/java/README.md`、`interop/go/README.md` 中的现有加密命令封装为可重复场景：每版本一个隔离 Broker、唯一容器名与 topic、固定客户端版本、就绪探针、超时、失败日志和限定资源清理。

每个 Broker 版本强制执行 Java ECIES 的 P-256/P-384/P-521 双向交换；Go RSA 双向交换；mTLS 下双方 CA/主机名校验与客户端证书认证；加密空载荷、压缩、批次的现有边界不退化。保留 Go 空载荷已知行为的专门断言，不把该已知差异当成新修复。无证书、错 CA、错主机名必须拒绝。

新增入口的约定命令（脚本实现后执行）：

```sh
moon run scripts/verify-native-interop.mbtx -- --pulsar-version 4.2.4 --report-dir /private/tmp/moonpulsar-native-audit/interop-4.2.4
moon run scripts/verify-native-interop.mbtx -- --pulsar-version 3.3.9 --report-dir /private/tmp/moonpulsar-native-audit/interop-3.3.9
```

实施时固定构建工具和镜像版本，检查 Docker、Go、MoonBit 可用；Java 使用对应 Pulsar 镜像中的客户端和工具。缺少任一前置条件返回 blocked/非零，不能只打印提示后成功退出。Broker 证书夹具必须匹配实际访问主机名，连接就绪使用完整收发探针；HTTP 健康响应本身不代表 topic 链路准备完成。

脚本不得依赖手工准备环境后静默跳过。设置不超过 30 分钟的单 Broker 作业超时；启动失败、未准备就绪、交换失败均返回非零。清理只处理本次创建的容器/topic；与用户长期资源隔离。

CI 调整：保留现有检查；新增 Linux A/B/C、macOS 编译及 ABI 作业；Pulsar 4.2.4/3.3.9 增加本次互通入口。维护者 ABI 测试可安装开发包，但不得污染验证“不需要开发包”的作业。缓存按平台、编译配置、依赖和源码区分；缺依赖门槛每次使用新 native 构建目录。

最终基础命令依次执行：

```sh
moon check --target native
moon test --target native
moon test --release --target native
moon info
moon fmt
moon fmt --check
git diff -- pkg.generated.mbti
moon doc
moon package --list
git diff --check
```

根接口差异预期为空；适配子包生成接口差异必须逐项审查，不能仅检查根接口。`moon info`/`moon fmt` 如改变实现文件，则重跑受影响测试。确认发布 ZIP 包含共享 ABI 头，且不含测试脚本、容器文件、ABI 检查工具及夹具；使用最终 ZIP 重跑 P4，保存相同摘要的证据。打包排除策略的其他历史问题保持独立记录，不顺手扩大改动。

文档同步 `README.mbt.md`、`README.zh-CN.md`、`CAPABILITY_MATRIX.md` 和 `interop/java/README.md`。写清基础编译要求、普通 TLS 与增强 TLS/ECIES 的运行库差异、系统与版本范围，以及 Zlib 是否真正完成无系统库替换。`README.md` 如为软链接则保留链接关系。

## 9. 完成定义与交付

以下全部满足，才能称为本方案完成：

- 无 OpenSSL/zlib 开发头文件的新环境，从最终发布 ZIP 构建成功。
- 无相关动态库的 Linux 运行环境，基础 TCP、RSA 和兼容 Zlib 成功；可选能力失败明确。
- 全功能环境的 TLS/ECIES、三个 EC 曲线及双版本真实互通通过。
- ABI 比对通过且负对照有效；Zlib 边界判定有解释和正反测试，无任意截断接受。
- 原有测试全部保留执行；公开接口变化符合记录；性能没有未处理的稳定退化。
- CI 与打包验证指向同一候选内容，测试报告没有强制场景 skip。
- 用户原有改动保持原样，未做发布、推送、部署。

交付清单：精确文件 diff、测试与环境报告、失败/未验证项（若有）、最终 ZIP 摘要、文档和生成接口差异。可按 P2、P3、P4/P5 分为三个可审查补丁组，但仅在获得提交授权后创建 commit。

若 P1 不通过：交付明确结论“OpenSSL 改善可独立完成，Zlib 去依赖尚未完成”，保留现有兼容实现，不能将总体标绿；下一步基于记录的 flate 缺口选择上游增强或另行评审独立扩展包方案。

## 10. 依据

- MoonBit 包/stub 构建规则：https://docs.moonbitlang.com/en/latest/toolchain/moon/package.html#native-stub-files
- 官方 async 的 HTTP/TLS 组合：https://github.com/moonbitlang/async/blob/main/src/http/moon.pkg
- 官方 async 的无系统头文件 OpenSSL 适配模式：https://github.com/moonbitlang/async/blob/main/src/tls/openssl.c
- 社区纯 MoonBit 压缩：https://github.com/moonbit-community/flate
- zlib 数据结构、flush 与完整流语义：https://www.zlib.net/manual.html

源码实现以本仓库及当前解析依赖为准；引用上游 main 仅用于设计依据。实施时固定所采用的上游提交和 ABI 声明来源，不能把上游 main 的变化当成当前依赖已经具备的能力。
