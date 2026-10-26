# 0.1.2 candidate verification

This is a development candidate, not a publication record. Each result below
applies to its named source and environment. A package ZIP, its external
consumer and later source changes require their own recorded identities.

The public interfaces, pinned dependencies, Apache-2.0 license and NOTICE are
unchanged from 0.1.1. The native backend is required. See
[runtime requirements](../README.md), [current behavior and limitations](client-guide.md),
and the [release checklist](releasing.md).

## Supported toolchain

For the 0.1.2 maintenance line, the supported, verified combination is `moon` and `moonrun` 0.1.20260920 (`914d7da`), `moonc v0.10.14+7d59c7ec9`, and `moonbitlang/protobuf@0.1.3`. This is not a lower-bound claim for every newer SDK. The library and examples already declare the runtime dependency; minimal version selection can choose a higher version if an application's dependency graph requires one, so that combination needs its own verification.

The public main baseline [`d01d434`](https://github.com/pangbit/moonpulsar/commit/d01d4344a95ef9c60b13da97aaf0cd00745a957a) has the same library tree and exact 74-file ZIP as the separately verified identity/documentation candidate. That ZIP (`d4a2777998268e00ba8862bc52284704adfd900709a0b212d7a4eb689893f2cb`) passed a fresh Linux external-consumer build and run in debug and release with the supported toolchain, including decoded CONNECT and AUTH_RESPONSE identity checks. Its nine intermediate commits also passed their targeted native debug regressions (82 test executions). These results do not certify a complete release build of every workspace example.

That baseline's [CI run on 2026-10-21](https://github.com/pangbit/moonpulsar/actions/runs/39561084202) installed `moon 0.1.20261020` / `moonc v0.10.16+b1a0e846e`. All five jobs were blocked by `protobuf@0.1.3` declaring `Option`, now a reserved type name (Error 4003). Native tests and later broker checks skipped after the failed build are not passing results.

[Upstream issue/fix discussion #143](https://github.com/moonbitlang/protoc-gen-mbt/pull/143) records the incompatibility. This library will track the issue and adopt an official fixed release after its own compatibility validation. It does not patch, fork or vendor the upstream fix, or depend on an unmerged PR.

CI selects the supported SDK using the official installer's `MOONBIT_INSTALL_VERSION=0.10.14+7d59c7ec9`, then checks the actual `moon`, `moonc` and `moonrun` versions. The isolated native dependency matrix copies that installed toolchain. The CI selection does not change a consumer's SDK: source dependencies are compiled by the toolchain selected in the consumer's environment. Ordinary client applications do not need an extra Protobuf declaration; Protobuf-specific schema APIs expose the runtime's `Read`/`Write` traits.

The supported-version CI still runs the existing gates. Its own candidate-specific outcome must be checked before publication; the earlier failed CI and whole-workspace release timeout below remain part of the record.

## Maintenance source verified on 2026-10-21

The following results bind to source
`e107971114f426bc8bc0ca21e9dda81e673d3695`, before the client-identity and public
documentation follow-up in this checkout:

- Linux native debug and release each passed **362 local tests**. Six additional
  opt-in broker tests returned at their environment guards in those local runs.
  Four benchmark nodes are separate and were not executed by `moon test`.
- Dedicated Pulsar 4.2.4 and 3.3.9 brokers each passed all **six actual live
  tests**, covering consumer/reader chunk expiry and chunk/receive-queue budgets.
- Two separately hashed transport probes passed on each broker: an owned TCP
  proxy blackhole and a blocked, certificate-verified native TLS SEND. They
  checked the fault barrier, a three-second keepalive bound, once-only close,
  pending sender/receiver retirement, cleanup and a fresh confirmed roundtrip.
- Each broker passed Java/MoonBit ECIES P-256/P-384/P-521 exchanges, Go/MoonBit
  RSA and compressed/batched encryption, and bidirectional compressed chunks
  with exact 256 KiB random-payload equality. Go covered LZ4, Zlib, Zstd and
  Snappy; Java covered LZ4, Zlib and Zstd. The pinned Go encrypted-empty-payload
  limitation was reproduced, rather than reported as empty-payload support.
- The same runs passed certificate-verified TLS roundtrips, the shared-memory
  budget example and consumer/reader timestamp seek. This dated TLS scope is
  server authentication; earlier mTLS results in the capability matrix retain
  their original source boundaries.

The Linux toolchain was Moon 0.1.20260920, moonc 0.10.14+7d59c7ec9 and Go
1.26.0 with Pulsar Go client `a83c1519f68d`. Java clients came from the two
digest-bound broker images. Source, native binaries, Java bytecode and command
exits were recorded separately from the public package.

Large test compilation needed a process-local 64 MiB stack with this compiler;
runtime tests used 8 MiB. No system limit was changed. The original whole-workspace
release build timed out after 840 seconds while building example test targets.
The later release gate covered every ordinary test node in the three packages
that contain tests; it does not certify that timed-out workspace build.

Earlier failed archive, probe-setup and report-processing attempts are retained
as failures. The first macOS archive included AppleDouble metadata and failed
strict checking before runtime. Corrected transfers exclude that metadata and
verify member lists and SHA-256 manifests. No failed runtime result was turned
into a passing result by changing an assertion or timeout.

## Identity and package follow-up

CONNECT and AUTH_RESPONSE share one private client-version constant. A native
regression compares it with `moon.mod`, and wire-level challenge tests check both
decoded messages. This follow-up changes the advertised client identity from
0.1.1 to 0.1.2; the protocol version remains 21. It needs candidate-specific
checks and an independent consumer of the exact ZIP, rather than inheriting
the preceding source's results.

The published archive excludes internal maintenance plans, raw machine evidence,
test fixtures, interop tools and examples. Public Markdown links are checked
against both the current checkout and exact ZIP; historical checkout documents
remain available for a separate audit.

## ECIES and TLS error isolation

The ECIES adapter rejects non-EC keys before invoking the EC getter. Its
synchronous calls translate failures to false/empty results while removing
their own OpenSSL diagnostics. Existing caller records and marks are retained
within the bounded queue on the verified runtime combinations below. This
does not clear a connection's error state or change the TLS implementation.

The adapter source SHA-256 is
`b6ad4dbfa5a8a70b9e99d3c72de9812db3ea8e920992c12083fce35142705941`.
The final C fixture SHA-256 is
`ce5ff1594cda1d94522f9ac4b98509347f23ea2a4a7be7a4697de235e058a256`.

| Historical environment | Source-bound result |
| --- | --- |
| macOS, OpenSSL 3.6.3 | 68 ECIES checks in each of debug/release, ASan/UBSan; leak detection excluded |
| Ubuntu 24.04, GCC 13.3, isolated OpenSSL 3.0.20 | Revised C fixture: 68/68 in each mode; TLS/ownership/Athenz/message crypto selection: 26/26 in each mode |
| OpenSSL 3.0.13 | Independent mark contract: six of ten cases failed in each mode |

The original GCC release C fixture failed a fortified array-bounds check before
execution. The revised fixture uses a fixed-size C scratch buffer for the same
invalid point and retains the rejection/error-state assertions. Both the initial
failure and the OpenSSL 3.0.13 failures remain failures.

The loader accepts OpenSSL major 3; it does not enforce the tested patch versions.
Error-mark behavior must be checked for the actual runtime, including distribution
backports. No universal minimum patch version is established. Reproduce with
`moon run scripts/verify-ecies-errors.mbtx`; an explicit `--openssl-prefix` selects
the reference runtime. This maintainer script is available in the source repository.

The pinned async ordinary-TLS binding has a separate per-I/O error-queue concern
under the [OpenSSL contract](https://docs.openssl.org/3.0/man3/SSL_get_error/#description).
The controlled ECIES contamination mechanism does not establish the historical
CI reconnect failure's cause. No upstream dependency was patched or upgraded.
These focused results do not certify the reorganized candidate, stock CI runtime,
complete workspace, live Broker matrix or final package consumer.

## Remaining release gates

- Record the final candidate's affected native regressions and exact-ZIP
  external-consumer results, including the advertised identity on a broker.
- Confirm the exact remote candidate's required CI checks and registry version
  availability before a maintainer publishes.
- Review the public source history separately from the package contents.

Synthetic replay tests do not prove broker persistence, deduplication or
exactly-once delivery. A timed-out SEND can have an unknown broker outcome.
User sequence-ID enhancements are a separate feature decision; the present
limitation is described in the client guide.
