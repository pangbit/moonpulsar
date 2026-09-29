# 持续负载测试运行器

`perf-sustained.mbtx` 每次只执行一个测试组：主机预检 → Topic 收发就绪 → 负载与采样 → 证据校验 → 清理。只有所有步骤成功，`status.json` 才写入 `complete`。不再把多小时对照和长稳绑定在一个不可拆分的循环中。

## 先验证工具

```sh
moon run scripts/perf-sustained.mbtx --self-test
moon run scripts/perf-sustained.mbtx --probe-host /absolute/path/to/new-probe-directory
```

自检不连接 Broker，覆盖追加写入、截断/缺号/覆盖时长不足、缺失进程采样和内存门槛。主机探针在 Linux 上运行 10 秒，实际采集内存与交换计数，检查历史记录确实保留。目录必须不存在，避免混入旧证据。

## 一个测试组的配置

在测试机的私有目录准备 JSON，例如：

```json
{
  "case_id": "smoke-moon-001",
  "topic_prefix": "perf-unique-run",
  "phase": "smoke",
  "client": "moon",
  "binary": "/absolute/path/to/perf.exe",
  "broker_pid": "12345",
  "output_dir": "/absolute/path/to/new-evidence-directory",
  "producer_cpu": 0,
  "consumer_cpu": 1,
  "size": 1024,
  "rate": 100,
  "warmup_ms": 1000,
  "duration_ms": 15000
}
```

`broker_pid` 必须是当前 Broker JVM 的宿主机 PID；二进制使用解析符号链接后的绝对路径。两个 CPU 编号应处于进程允许的 affinity 集合内。`client` 为 `moon`、`go` 或 `rust`，均使用已有匹配负载适配器。

连接参数只通过环境提供：`PULSAR_URL`、`PULSAR_ADMIN_URL`、`PULSAR_TOKEN`。不要把真实凭据写入配置、命令参数或报告。运行器默认使用 `public/default` 中的新专用 Topic；已有 Topic 或输出目录会被拒绝。

```sh
moon run scripts/perf-sustained.mbtx --validate-config /private/config.json
moon run scripts/perf-sustained.mbtx --run /private/config.json
moon run scripts/perf-sustained.mbtx --verify /absolute/path/to/evidence-directory
```

正式长测应交由测试机的服务管理器托管，配置整组进程回收和有限运行时限（例如 systemd 的 `KillMode=control-group`、合适的 `RuntimeMaxSec`），并保存标准错误。失败后不要仅凭目录存在就跳过或自动续跑。先确认服务与收发进程退出，读取失败阶段和日志，核对 `created-topic.txt`，只清理该次创建的 Topic；重试必须换 `case_id` 和输出目录。

## 分阶段执行

1. 三客户端先各执行一个低速 `smoke`；该阶段预热至少 1 秒、测量 15–60 秒，只验工具链，不能当作性能结论。
2. 确认测试期间不并行进行大规模编译等资源竞争工作，再执行 `compare`：预热至少 2 分钟、测量至少 15 分钟。1 KiB 和 64 KiB 分别使用经过校准的共同速率，轮换三客户端次序，各三轮。每组核验成功后才进入下一组。
3. 单独执行 `soak`：预热至少 2 分钟、测量至少 2 小时。测试组失败不覆盖其他组，但某场景缺少客户端/轮次时，不能标成完整对照。不要把 30 秒校准的最高通过档当作最大持续容量。

当前固定参数与历史对照一致：32 并发、batch 100 条 / 128 KiB / 1 ms、队列 1000、发送超时 10 秒，无压缩。生产者、消费者各占一个逻辑 CPU；消费者窗口位于持续发送期间。参考目标为发送达到目标 98%、消费达到目标 95%、发送确认 P99 ≤250 ms。未达到时记录 `performance_targets_met=false` 和各项判定，仍保留该组结果并继续后续组；不因此中止，不降低或自动调整负载。`evidence_valid` 只表示证据有效，不代表性能目标达成。

## 资源与证据门槛

- 开始前连续 30 秒要求可用内存至少 4 GiB、磁盘余量至少 20 GiB；该阶段换出页增量不得超过 1024 页。未通过就结束，不修改主机资源配置或保护阈值来强行执行。
- 运行中每秒记录实际经过时间、顺序号、进程 PID/启动计数/CPU ticks/RSS、主机可用内存、交换占用与换入换出累计页数、major faults、Topic 累计收发与积压。原始证据只存私有输出目录。
- 使用 `OpenOrCreate` 加追加模式保存历史。第 3 条及之后每 60 条回读文件，核对实际条数与连续性；不会等整批结束才发现覆盖问题。
- 主机可用内存低于 2 GiB、磁盘余量低于 20 GiB，或积压超过目标速率 10 秒的消息量且持续 30 个采样点，立即中止。先保存触发内存/积压保护的观测，再报错。
- 单组结束要求记录数量至少达到预期秒数的 90%、相邻记录间隔不超过 5 秒、末条覆盖预期运行时长。共同测量区间内每个角色采样覆盖至少 95%，身份不变、CPU 计数不回退；任一不满足均不能标记证据有效。
- `verification.json` 是白名单统计和覆盖检查；`config.json`、原始采样、日志和资源统计可能含主机路径、PID 或 Topic 名，不能直接放进公开性能报告。报告另行移除这些字段，使用实际时间差和 `clock-ticks.txt` 计算 CPU。

该工具仍只有单组整体 P99，没有逐秒 P99；消息没有唯一序号，不支持零丢失/零重复声明。没有自动故障注入，也不自动重启 Broker。旧长测丢失的时间序列无法修复；需要重新测试才能获得趋势证据。

## 工具验证记录（2026-09-28）

本地及 Linux 自检通过，覆盖追加持久化、截断/缺段/时长不足拒绝、角色覆盖和内存保护。Linux 主机探测保留了 10 条连续记录。MoonPulsar 1 KiB、100 msg/s 的 15 秒冒烟测试通过：实际收发均为 100 msg/s，保留 18 条运行采样，最大间隔约 1.01 秒，角色覆盖校验及专用 Topic 清理通过。

以上是早期工具验证。随后于 2026-09-29 完成三客户端冒烟、18 组正式 15 分钟对照和 2 小时长稳，见[完整脱敏报告](../performance-results/2026-09-28-sustained-v3/README.md)。新批采样通过核验；旧批数据缺口仍保留，不用新结果覆盖历史失败证据。
