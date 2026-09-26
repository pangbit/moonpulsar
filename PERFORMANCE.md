# Performance testing

The suite has two independent layers, following the structure of
[pulsar-client-go's perf tools](https://github.com/apache/pulsar-client-go/tree/a83c1519f68dfdbddfa7955a56d91735bd1ca95c/perf)
and its compression/routing benchmarks. These measurements are not a throughput
guarantee or a performance regression threshold.

Real-Broker results: [three-client comparison (2026-09-26)](performance-results/2026-09-26-comparison/README.md).
The [RSS-fix rerun](performance-results/2026-09-26-rss-broker-rerun/README.md)
records the updated three-client measurements and Broker resource limitations.
The [reference adapters](interop/PERFORMANCE_COMPARISON.md) reproduce the matched
workload using the Go and Rust clients. Reports contain sanitized measurements only.

[Producer RSS fix validation](performance-results/2026-09-26-rss-fix/README.md)
uses an isolated mock to compare timeout-state retention before and after the fix;
it is not a replacement for real-Broker throughput measurements.

## Local microbenchmarks

From the repository root:

```sh
moon bench --release --target native -p pangbit/moonpulsar -f performance_wbtest.mbt
```

There are 21 measured cases:

- LZ4, Zlib, Zstd and Snappy compression and decompression at 1 KiB and 64 KiB.
- Key-hash, round-robin and single-partition routing across 200 child handles.
- Batch encoding of 1 and 100 messages, each with a 1 KiB payload.

Inputs and compressed fixtures are prepared outside the timed callbacks. A
roundtrip assertion checks each codec fixture before measurement. Results are
kept through the benchmark library to prevent dead-code elimination. Routing
uses disconnected handles and measures selection only, not network sends.
Compression input repeats byte values 0–250; this is a reproducible compressible
fixture, not an incompressible or production payload distribution. Timings
include output allocation and the benchmark collector's `keep` overhead.

`moon test` does not execute these benchmark bodies. Normal CI builds them;
the manually dispatched **Performance baselines** workflow executes them and
uploads timings, commit and environment information. Shared CI runner noise
means these results must not be treated as a hard pass/fail performance gate.

## Broker producer and consumer

Use an existing disposable Broker and a dedicated topic. The program sends
messages or consumes and ACKs them; it does not start, restart or delete a
Broker, or delete test topics. Keep topic cleanup under your normal test-host
procedure. Producer-only tests can accumulate retained messages.

Producer example (5 seconds warmup, 30 seconds measurement):

```sh
PULSAR_URL=pulsar://127.0.0.1:6650 \
PULSAR_TOPIC=persistent://public/default/moonpulsar-perf-run-001 \
PERF_CONCURRENCY=32 PERF_SIZE=1024 PERF_RATE=10000 \
PERF_BATCH_MESSAGES=100 PERF_BATCH_DELAY_MS=1 \
moon run examples/perf --release --target native > producer.json
```

Consumer example, in a separate terminal while a producer is running, or
against a prefilled topic:

```sh
PULSAR_URL=pulsar://127.0.0.1:6650 \
PULSAR_TOPIC=persistent://public/default/moonpulsar-perf-run-001 \
PULSAR_PERF_MODE=consume PULSAR_SUBSCRIPTION=perf-run-001 \
PERF_QUEUE_SIZE=1000 PERF_WARMUP_MS=5000 PERF_DURATION_MS=30000 \
moon run examples/perf --release --target native > consumer.json
```

Both commands compile before starting their measurement clocks. For overlapping
runs, build first with `moon run examples/perf --release --target native --build-only`.
The consumer uses an exclusive, non-durable subscription starting at Earliest.
Use a unique subscription name per run, and keep producers active for the
consumer's entire window (or provide enough backlog). Producer and consumer
clocks start independently; their windows are not synchronized automatically.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `PULSAR_TOPIC` | required | Dedicated test topic |
| `PULSAR_URL` | `pulsar://127.0.0.1:6650` | Broker URL |
| `PULSAR_TOKEN` | absent | Optional authentication token; never printed |
| `PULSAR_PERF_MODE` | `produce` | `produce` or `consume` |
| `PULSAR_SUBSCRIPTION` | `moonpulsar-perf` | Consumer subscription |
| `PERF_WARMUP_MS` | 5000 | Warmup excluded from reported results |
| `PERF_DURATION_MS` | 30000 | Measurement window; up to one day |
| `PERF_TIMEOUT_MS` | 10000 | Connection/operation/send timeout, and producer drain allowance |
| `PERF_SIZE` | 1024 | Producer payload bytes, 1–1048576 |
| `PERF_CONCURRENCY` | 32 | Producer workers, one outstanding send each; 1–4096 |
| `PERF_RATE` | 0 | Global producer launch schedule in messages/sec; 0 is unthrottled |
| `PERF_BATCH_MESSAGES` | 0 | 0 disables batching; otherwise maximum messages per batch |
| `PERF_BATCH_DELAY_MS` | 1 | Maximum batch delay; batch byte limit is 128 KiB |
| `PERF_QUEUE_SIZE` | 1000 | Producer pending limit or consumer receive queue size |

Producer workers bound concurrency independently of the pending queue; pending
queue saturation blocks. Rate scheduling is shared across workers. Scheduling
uses millisecond sleeps, so short bursts and catch-up after stalls are possible;
`PERF_RATE` is a target schedule, not a strict per-millisecond cap. Batches also
flush by byte limit or timer, so their actual size can be less than configured.
Payload compression is disabled for this initial broker harness.

### Measurement contract

- Successful execution prints one JSON object to stdout. Errors propagate with
  a nonzero exit; zero measured messages also fail instead of producing a false
  successful baseline. Diagnostics go through the runtime/tooling.
- Throughput is successful measured messages and application payload bytes
  divided by the configured measurement duration. Payload MiB/s excludes
  protocol overhead, replication and compression effects.
- Producer latency uses a monotonic microsecond clock immediately before
  `send`, after rate waiting, through the Broker receipt. It includes client
  pending-queue and batching waits. Both start and receipt must lie inside the
  measurement window; warmup-crossing and drain completions are excluded.
  Outstanding sends drain after the window, bounded by the timeout allowance.
- P50/P95/P99/P99.9 are nearest-rank **100 μs bucket upper bounds**, not exact
  samples. Storage is fixed (600002 Int64 counters, approximately 4.8 MB of
  counter data). Values beyond 60 seconds increment `overflow_count`; affected
  percentiles are null. `max` retains the observed maximum without bucketing.
- Consumer counts messages received and successfully locally ACKed within the
  measurement window. ACK submission is not a confirmed Broker ACK receipt.
  Consumer latency is null: receive waiting time is not end-to-end latency.
  Producer-specific configuration fields in consumer JSON are unused.
- These tools do not yet measure end-to-end latency, CPU/RSS, allocation rate
  of broker runs, faults under load, or automatically compare historical runs.

For useful comparisons, record commit, `moon version --all`, CPU/OS, Broker
version/configuration, topic partition count, payload size, batch, concurrency
and queue settings. Use the same release build and environment for repeated
baseline/candidate runs. Extend duration for soak experiments, but collect
CPU/RSS separately; a long run alone does not prove absence of memory leaks.

## Validation boundary

Statistics and configuration tests run with:

```sh
moon test examples/perf --target native
```

They cover percentile boundaries, overflow, warmup/drain exclusion and invalid
numeric settings. A passing build or these tests does not prove real-Broker
performance. Run both Broker modes in the intended environment before using
their results as a baseline.
