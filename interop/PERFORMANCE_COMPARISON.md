# Reference-client performance adapters

These small applications run the same workload as `examples/perf` using the
unmodified reference clients: Apache pulsar-client-go (pinned by `go/go.mod`)
and StreamNative pulsar-rs (pinned by `perf-rust/Cargo.toml` and `Cargo.lock`).
They are workload adapters, not the upstream projects' default benchmark tools.

Build and validate before measuring:

```sh
moon run examples/perf --target native --release --build-only
cd interop/go
go test ./perf
go build -o /tmp/pulsar-go-perf ./perf
```

In `interop/perf-rust` (requires Rust and `protoc`):

```sh
cargo test --release --locked
cargo build --release --locked
```

Use the environment variables documented in [PERFORMANCE.md](../PERFORMANCE.md)
for all three executables. Run a producer and consumer against a fresh dedicated
topic for each pair. Use equal CPU affinity budgets, set `GOMAXPROCS=1`, and keep
the consumer measurement window within sustained production. The Rust adapter
uses Tokio's current-thread runtime. Authentication tokens are read from the
environment; do not put credentials in reports or command transcripts.

Workload alignment:

- Each producer worker has at most one outstanding send receipt. Concurrency,
  payload size, global rate limit, warmup and measurement duration are identical.
- No compression; batch byte limit 128 KiB and delay 1 ms in the reported runs.
  Go uses `SendAsync` followed by waiting for its callback because synchronous
  `Send` forces a flush. Rust locks its producer only while enqueueing, then
  waits for the receipt outside the lock.
- Consumers use Exclusive, non-durable, Earliest subscriptions and a 1000-message
  receive queue. ACK grouping is disabled in Go. Counts include local ACK calls,
  not an independently verified broker-persisted acknowledgment.
- Send latency includes client enqueueing and receipt wait, excludes rate-limit
  pacing, and uses nearest-rank percentiles with 100 μs upper-bound buckets.
  Samples starting before warmup or completing after the window are excluded.
- Queue implementations, transport buffering, allocation and runtime scheduling
  remain client-specific. Equal settings do not imply identical wire batching.

Rotate client order between repeated runs. Export only whitelisted numeric
measurements and public software versions. Keep raw logs, connection metadata,
credentials and machine identifiers outside the report directory.

Results: [2026-09-26 comparison](../performance-results/2026-09-26-comparison/README.md).
