# Pending SEND relay regression fixture

This maintainer fixture exercises receipt withholding and replay through a local
TCP relay. It is test-only code in the separate examples module. Its state/codec
tests and local protocol mocks do not require a real Pulsar Broker. A live Broker
restart controller is not included.

The fixture retains one Client, Producer, Consumer, `send_async` result and
`ReceiptFuture.wait()` task. It captures the complete original SEND and matching
receipt, persists and withholds that receipt, then checks the identical replay
on a replacement connection before forwarding a new receipt. Equal old/new
MessageIds and duplicate delivery are allowed; this is not exactly-once delivery.

## Components

| Component | Purpose |
| --- | --- |
| `wire.mbt` | Bounded frame decoding and LOOKUP destination rewriting |
| `gate.mbt` | Connection generations, receipt matching and command ordering |
| `runtime.mbt` | TCP pumps, atomic journal and create-new binary evidence |
| `client_fixture.mbt` | Shared client used by the executable and I/O regression |
| `main.mbt` | Explicitly enabled loopback entry point |
| `relay_wbtest.mbt` | 17 in-memory codec/state cases |
| `runtime_io_wbtest.mbt` | Persistence, routing, generation and cleanup subscenarios |
| `../../scripts/pending_replay_controller.py` | File-follow coordinator; checks externally supplied observations |
| `../../scripts/pending_replay_entry_test.py` | Six opt-in native entry/owner-root cases with local mocks |

The controller does not establish OS ownership or Broker readiness. Its
`initial_identity`, `old_broker_gone` and `replacement_ready` proofs must come
from an independent observer for a live experiment. Synthetic records are only
test inputs. A completed controller trace alone is not Broker acceptance.

## Local checks

Use the repository's supported Moon SDK and frozen dependencies. Run Python
commands from the repository root; they require Python 3.10 or newer on a POSIX
system. Pure controller tests open no sockets or child processes:

```sh
python3 -B scripts/pending_replay_controller_test.py
```

Portable entry-plan checks use temporary files without running the native
fixture or opening sockets:

```sh
python3 -B scripts/pending_replay_entry_plan_test.py
```

The file follower tests create owned temporary files and one Python child per
case. They check ordering, replay-byte identity, deadlines and cleanup:

```sh
python3 -B scripts/pending_replay_follow_test.py
```

From `examples/`, select the Moon files separately:

```sh
moon test --frozen --target native -j 1 --no-parallelize pending_replay/relay_wbtest.mbt
moon test --frozen --target native -j 1 --no-parallelize pending_replay/runtime_io_wbtest.mbt
```

The first file uses only in-memory frames. The second uses owned temporary
directories and ephemeral `127.0.0.1` sockets. The shared native client runs
against protocol mocks through the actual relay; decoy LOOKUP routing must not
bypass it. Both old receipts remain withheld and completion requires the new
receipt, the original future and a checked consumer ACK. The local I/O case has
two sequential subscenarios, not a live Broker restart.

## Native entry checks

Build only this debug executable from `examples/` with a fresh target directory:

```sh
moon build --frozen --target native -j 1 --target-dir "$BUILD_ROOT" pending_replay
```

Set `ARTIFACT` to the resulting
`$BUILD_ROOT/native/debug/build/pangbit/moonpulsar_examples/pending_replay/pending_replay.exe`.
Create an empty evidence directory outside the checkout and set `EVIDENCE_ROOT`
to its absolute path. From the repository root, a portable plan can be inspected
without starting the fixture:

```sh
python3 -B scripts/pending_replay_entry_test.py --artifact "$ARTIFACT" --evidence-root "$EVIDENCE_ROOT" --print-plan
```

Omit `--print-plan` to execute a fresh plan, or save the printed JSON outside
the checkout and pass it with `--plan`. Never reuse a completed plan. Run this
opt-in native test with an outer 60-second process-group bound; the driver has
a 52-second batch budget including cleanup. It launches the selected executable,
local mock sockets and one bounded mock thread. A timeout or forced cleanup is
a failure.

Missing/wrong owner tokens, root/token symlinks and invalid run IDs must reject
without changing the roots or contacting the backend. The positive case reaches
one retained pending future and a persisted withheld receipt, then deliberately
terminates its native child. It does not claim successful replay completion.

## Executable contract

Direct execution requires `MP_PENDING_ENABLE=1`, a fresh 32-character lowercase
hex `MP_PENDING_RUN_ID`, and a literal `127.0.0.1:<nonzero-port>` backend in
`MP_PENDING_BACKEND`. The relay binds an ephemeral loopback port. It has no TLS,
authentication or remote-host inputs. Topics and client names include the run ID.

By default it exclusively creates `/tmp/moonpulsar-pending-<run-id>`. With
`MP_PENDING_PRECREATED_ROOT=1`, an existing regular directory must contain a
regular 32-byte `owner-token` equal to the run ID. Symlink roots/tokens reject.
Receipts stay quarantined after old-connection retirement. `upstream_local` in
the connection journal identifies the relay's upstream endpoint.

The journal is bounded to 512 events, 2 KiB per event and 64 KiB frame bodies;
connection and receipt counts are bounded in `gate.mbt`. The send/whole-client
limits are 180/240 seconds, not an API recovery SLA. Evidence belongs outside
the source checkout and is removed only after ownership and cleanup checks.

## Verification boundary

Historical runs covered 17 Moon state tests, 20 pure controller tests, 20 file
follower tests, local relay I/O and six native entry cases. These results apply
to their historical inputs, not the newly reorganized CLI/documentation.
See the [dated maintenance report](../../docs/archive/maintenance-2026-10/README.md).
The new plan entry and final candidate still require their own validation.

Real Broker pending-SEND replay, deployed TLS/mTLS, chunked replay and admission
of a second SEND during a live Broker replay remain separate acceptance cases.
The root `producer_replay_tcp_wbtest.mbt` covers second admission over local TCP
protocol peers; it does not supply live Broker evidence.

A separate October 29 live whitebox fixture has since verified second-send
admission during actual Writer replay against Pulsar 4.2.4, with both original
futures completed and all three deliveries ACK-confirmed. That fixture kept the
Broker running; it is distinct from this relay executable and its local mocks.
See the [source-bound result](../../docs/verification-0.1.2.md#live-verification-on-2026-10-29).
Retaining an unconfirmed send's original future across Broker restart remains
unverified and is deferred beyond the 0.1.2 release scope.
