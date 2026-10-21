# 0.1.2 candidate verification

This is a development candidate, not a publication record. Each result below
applies to its named source and environment. A package ZIP, its external
consumer and later source changes require their own recorded identities.

The public interfaces, pinned dependencies, Apache-2.0 license and NOTICE are
unchanged from 0.1.1. The native backend is required. See
[runtime requirements](../README.md), [current behavior and limitations](client-guide.md),
and the [release checklist](releasing.md).

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
