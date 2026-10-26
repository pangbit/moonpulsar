# October 2026 maintenance verification

These are historical source-bound results, not acceptance of the reorganized
candidate or a publication record. Preserve failed attempts as failed. Current
checks belong to the exact candidate and package under review.

## Source identities

The original local commit chain has been retained separately. Content hashes
below identify the product inputs without requiring unpublished commit objects
in a fresh clone. No private machine paths or raw process logs are included.

| Input | SHA-256 |
| --- | --- |
| `connection.mbt`, TLS ownership fix | `b1f0ca830a4b788dd83feaa93effc4d87a4eba8f44c3d6345391e7cc7fe07912` |
| `native_ecies/native_ecies.c`, error isolation | `b6ad4dbfa5a8a70b9e99d3c72de9812db3ea8e920992c12083fce35142705941` |
| `producer.mbt`, before replay-admission fixes | `ed521094a91bc2ad9dc9e53025dd077cbf502dbc9356a5fa36ab8d1d572afd6d` |
| `producer.mbt`, replay-admission/close fixes | `839ea7923dcacc045dc08fc0abbf1d210e36435eeaae111e42ff1f6f27558a78` |

The pre-fix producer is also available at public baseline `3d7eacc`.
Matching one product file alone does not bind an entire test environment.

## Recorded scopes

| Date | Selection | Recorded result and limit |
| --- | --- | --- |
| 2026-10-22 | Ubuntu 24.04, GCC 13.3, OpenSSL 3.0.20, TLS/ownership/Athenz/crypto | Debug/release each 26/26; revised ECIES C fixture each 68/68 with ASan/UBSan, without leak detection |
| 2026-10-22–23 | Native dependency matrix on the pre-replay product | 46 actual A/B/C results plus six physical missing-library checks; not the full workspace |
| 2026-10-22 | Pulsar 4.2.4 and 3.3.9 TCP release roundtrips | Two fresh clients per version, ten checked messages each before/after a restart; not persistent-client recovery. The 4.2.4 outer execution status was unavailable |
| 2026-10-23 | Same-client recovery, Pulsar 4.2.4 and 3.3.9 | Original Client/Producer/Consumer survived one owned Broker-child replacement; pre/post payloads and confirmed ACKs passed, reconnect counters advanced once, cleanup passed |
| 2026-10-23 | Four in-memory Producer regression probes | Four passed; producer-only old-code comparison failed the admission case with the intended internal TimeoutError |
| 2026-10-23 | macOS root-package native debug and release | Each runner reported 372 passed; 366 non-live bodies actually executed and six live bodies returned at environment guards |
| 2026-10-23 | Relay state, file following and native entry | 17 Moon state cases, 20 pure controller cases, 20 file-backed follower cases and six entry cases passed in their respective selections; local I/O also passed its persistence/routing/generation/cleanup subscenarios |
| 2026-10-26 | Three local TCP replay-admission cases, selected separately | All three passed; old-producer case 0 failed with the intended internal timeout. No full-root result was recorded for the added test |

The native runs used Moon/moonrun 0.1.20260920 and moonc 0.10.14+7d59c7ec9,
with async 0.22.1 and Protobuf 0.1.3. The full root debug/release results precede
the TCP test addition. The historical TCP fixture SHA-256 is
`dc5aa63a9497b953020f47c7ab8086c93ebf598a2744f6de49e47c837f9340e4`;
the reorganized test removes diagnostic printing/rethrowing and needs validation.
The relay entry driver now has a portable plan CLI, also awaiting validation.

Both real-Broker same-client cases settled the pre-fault SEND before the fault.
They used the pre-replay product and do not validate the later Producer repair,
an unconfirmed SEND at fault time, TLS/mTLS recovery or exactly-once delivery.
The retained exact 74-file pre-replay package had SHA-256
`46898de57bbaae3444462fdca2c641f58346482cc7ca5ab635ed122c3b9a4bf4`.
It is not the reorganized candidate package.

## Retained failures and unknowns

- OpenSSL 3.0.13 failed six of ten independent mark-contract cases in both modes.
  The original GCC optimized ECIES fixture failed compilation; its corrected
  fixed C buffer passed later. No universal minimum OpenSSL patch is established.
- Early replay fixtures had compilation failures and a closed-PipeWrite SIGABRT.
  They are fixture failures, not valid old-product counterexamples. The final
  constructible I/O-error fixture and producer-only comparison have separate results.
- An earlier TCP release restart attempt failed. Earlier TLS success markers
  preceded the restart and cannot establish controlled TLS recovery.
- The historical CI reconnect cause and whole-workspace release timeout remain
  unresolved. Later focused passes do not retroactively repair those outcomes.

## Remaining candidate checks

Use [development checks](../../testing.md) and the
[release checklist](../../releasing.md). Recheck generated interfaces, formatting,
the root and separate native-TLS package, examples, package links and an external
consumer against the final candidate. Run the new CLI and the TCP cases under
their documented local bounds. Six guarded live returns remain NOT_RUN.

Real Broker pending-SEND replay, deployed TLS/mTLS, encrypted interoperability,
chunk/queue/seek Broker gates and exact remote CI remain independent requirements
for their stated release scope. No recovery SLA, deduplication or application
retry idempotency follows from these focused checks.
