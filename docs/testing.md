# Development and verification

English | [简体中文](testing.zh-CN.md) | [Documentation](README.md)

Run commands from the repository root. `moon test --target native` runs local tests and the in-process mock broker. Optional live tests need explicit environment inputs; their absence is not real-Broker verification. See [examples](https://github.com/pangbit/moonpulsar/blob/main/examples/README.md) for live scenarios and [capabilities](capabilities.md) for coverage.

## Environment and local checks

Use a MoonBit toolchain supporting the native backend, a C compiler and the
system SDK/libc. See the [README](../README.md) for TLS/ECIES runtime requirements.
Run `moon update` after cloning. For a release candidate, run:

```sh
moon check --target native --deny-warn
moon test --target native
moon test --target native --release
moon info && moon fmt
moon doc
moon package --list
moon package
moon run scripts/check-doc-links.mbtx -- --archive _build/publish/pangbit-moonpulsar-0.1.0.zip
git diff --check
```

Use the candidate version's ZIP filename when preparing a different release.
Real-Broker scripts may create, restart or delete isolated containers; read their
instructions and run them only in an authorized test environment.

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

The protocol layer in `proto/` is generated from `proto/PulsarApi.proto` (vendored from apache/pulsar) with `protoc-gen-mbt`:

```sh
protoc --mbt_out=. --mbt_opt=project_name=proto proto/PulsarApi.proto
```

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
