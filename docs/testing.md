# Development and verification

English | [简体中文](testing.zh-CN.md) | [Documentation](README.md)

Run commands from the repository root. `moon test --target native` runs local tests and the in-process mock broker. Optional live tests need explicit environment inputs; their absence is not real-Broker verification. See [examples](https://github.com/pangbit/moonpulsar/blob/main/examples/README.md) for live scenarios and [capabilities](capabilities.md) for coverage.

## Environment and local checks

Use a MoonBit toolchain supporting the native backend, a C compiler and the
system SDK/libc. See the [README](../README.md) for TLS/ECIES runtime requirements.
Run `moon update` after cloning. On Linux, first compile the test executables
with a bounded 32 MiB compiler stack as described below. For a release candidate,
run:

```sh
moon check --target native --deny-warn
moon test --target native
moon test --target native --release
moon info && moon fmt
moon doc
moon package --list
moon package
moon run scripts/check-doc-links.mbtx -- --public --archive _build/publish/pangbit-moonpulsar-0.1.2.zip
git diff --check
```

Use the candidate version's ZIP filename when preparing a different release.
Real-Broker scripts may create, restart or delete isolated containers; read their
instructions and run them only in an authorized test environment.

### Linux compiler stack

With moonc `0.10.18+6c83a7f62`, the full native debug whitebox IR reproduced a
`link-core` stack overflow at an 8 MiB process stack and compiled successfully
at 32 MiB using the same compiler, IR and arguments. This establishes stack
sensitivity for that input, not a minimum stack requirement for every build or
a confirmed regression against an older compiler.

CI applies the same finite compilation budget to debug and release test builds:

```sh
prlimit --stack=33554432:33554432 -- moon test --target native --build-only
moon test --target native
prlimit --stack=33554432:33554432 -- moon test --release --target native --build-only
moon test --release --target native
```

`prlimit` changes only the build subprocess and its children. The following
test command reuses those executables with the normal process stack, preserving
runtime stack checks. Keep the target directory and build flags identical in
each pair. No machine-wide or persistent limit change is needed. Local Linux
validation completed the debug suite with **398 reported passes** at an enforced
8 MiB stack; six opt-in live-Broker bodies returned without their environment
inputs. Release test executables also built with the finite compiler stack;
release tests were not executed. These are SDK compatibility results. This
workaround does not alter the protocol, generated bindings, test selection or
assertions.

## Maintenance

- Keep changes focused; include meaningful regression coverage for behavior fixes.
- Document public APIs and examples; synchronize affected behavior in both languages.
- Regenerate `proto/top.mbt` and `pkg.generated.mbti` rather than editing them by hand.
- Review public-interface diffs, compatibility changes and unverified platforms.
- Use concrete commit subjects with prefixes such as `fix:`, `feat:`, `test:` or `docs:`.
- Preserve third-party licenses and attribution. Do not add credentials, production logs or real private keys.

An optional local hook runs `moon check` before commits; it does not replace the
full checks above. Enable it from the repository root with:

```sh
chmod +x .githooks/pre-commit
git config core.hooksPath .githooks
```

Ordinary bugs and usage questions go to [GitHub Issues](https://github.com/pangbit/moonpulsar/issues).
Read [SECURITY.md](../SECURITY.md) before reporting security issues.

## Test scenarios and generated protocol

See [Performance testing](performance.md) for local compression/routing/batch
benchmarks and explicit real-Broker producer/consumer measurements.

The mock suite includes handshake timeout, broker CONNECT rejection, excessive
authentication challenges, inclusive Reader replay from a nonempty topic,
and disabled or out-of-range transaction coordinator IDs.
On the disposable Linux test host, `scripts/test-athenz-broker-live.sh 4.2.4`
(and `3.3.9`) exercises signed role tokens and service-NToken ZTS exchange
against an isolated Athenz-authenticated broker.
`scripts/test-auth-challenge-live.sh 4.2.4` (and `3.3.9`) compiles a
disposable Broker provider and verifies a real binary `AUTH_CHALLENGE`,
successful refresh and wrong-response rejection.

GitHub Actions runs on Linux (Ubuntu 24.04) and performs native checks, debug and release tests, formatting, generated-interface verification, documentation generation, and package creation on pushes and pull requests. The checked-in localhost TLS key and certificate are public, disposable test fixtures; never use them for a real broker. They are excluded from the Mooncakes package, along with tests and examples.

The protocol layer in `proto/` is generated from `proto/PulsarApi.proto` (vendored from apache/pulsar) with `protoc-gen-mbt@0.3.0` and the SDK selected in the README. The recorded regeneration used `protoc 29.3`. Generate into a separate directory so the generator's example module metadata does not replace the library configuration:

```sh
mkdir -p _build/protocol-gen
moon install moonbitlang/protoc-gen-mbt@0.3.0 --bin _build/protocol-gen/bin
protoc --plugin=protoc-gen-mbt=_build/protocol-gen/bin/protoc-gen-mbt --mbt_out=_build/protocol-gen --mbt_opt=project_name=proto proto/PulsarApi.proto
cp _build/protocol-gen/proto/src/pulsar/proto/top.mbt proto/top.mbt
cp _build/protocol-gen/proto/src/pulsar/proto/moon.pkg proto/moon.pkg
moon fmt proto
moon info
```

Review the generated source and interfaces. The generator emits explicit trait
method extensions; do not suppress warnings or edit the generated bindings to
hide compatibility errors. Protocol generation is not needed for normal builds.

## Recorded real-Broker validation

The [capability matrix](capabilities.md) is the maintained summary of historical
functional verification, including mock versus real-Broker coverage and known
limits. Use its linked reports and the [performance reports](performance.md)
for run-specific evidence; do not infer current-commit acceptance from a prior
pass. Reproduction entry points and environment inputs are listed with the
[examples](https://github.com/pangbit/moonpulsar/blob/main/examples/README.md).

For each new acceptance run, record the source commit, toolchain, platform,
Broker version/configuration, exact command, result and any skipped scenarios
alongside its report. The [release process](releasing.md) requires validation
of the exact candidate commit.

The release link gate uses `--public --archive` to check exactly the Markdown files included in that ZIP, both in the checkout and inside the archive. Omit `--public` for the separate full-checkout audit, which includes historical machine-specific evidence. Public scope does not claim that all historical archive links are portable or valid.

## Producer replay regressions

The root tests cover admission during replay, retry after a partial write and
close during registration/replay. The memory and TCP fixtures share whitebox
helpers; keep those dependencies with the tests. For the TCP cases, select
indices 0, 1 and 2 separately when reproducing the historical bounded run:

```sh
moon test --frozen --target native -j 1 --no-parallelize --index 0 producer_replay_tcp_wbtest.mbt
moon test --frozen --target native -j 1 --no-parallelize --index 1 producer_replay_tcp_wbtest.mbt
moon test --frozen --target native -j 1 --no-parallelize --index 2 producer_replay_tcp_wbtest.mbt
```

Normal full-suite debug/release execution remains required for the final
candidate. Separate index passes do not prove that combined invocation.
The [relay fixture](https://github.com/pangbit/moonpulsar/blob/main/examples/pending_replay/README.md)
has opt-in local file/TCP and native-entry checks. Its Python controller cannot
launch or validate a real Broker. Keep generated evidence outside the checkout.
