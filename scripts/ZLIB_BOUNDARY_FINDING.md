# P1: flate 0.8.3 sync-flush boundary gate

Status: blocked. `native_zlib` has deliberately been retained.

Reproduce from the repository root:

```sh
moon run scripts/probe-zlib-boundary.mbtx
```

The successful exit means that the counterexample assertions passed, not that
the replacement decoder is ready. A raw non-final stored block containing
`hello` is followed by either:

- `00 00 00 ff ff`: a complete, empty stored block at a sync-flush boundary;
- `00 00 ff ff`: an incomplete stored-block header.

With six bytes of output capacity, both inputs expose:

- `NeedMoreInput`;
- `last_consumed() == input.length()`;
- `last_produced() == 5`;
- `is_finished() == false`;
- the same final four bytes `00 00 ff ff`.

The pinned flate decoder explicitly counts an incomplete atomic header accepted
into its private staged input as consumed. Its `Mode`, bit count and staged
input are private. Therefore the direct acceptance predicate proposed for the
replacement is insufficient. This does not prove that every possible adapter
using the public API is impossible; it demonstrates that this implementation
route has not met the plan's boundary-proof gate.

A narrow upstream enhancement to evaluate is a read-only boundary query that
is true only after a fully decoded, non-final empty stored block with byte
alignment, no staged incomplete input, and no pending output/copy. It must be
false after the truncated example, reset/initial state, an error, a final
stream, or an ordinary nonempty stored block whose payload merely ends with
the same marker. Alternatively, expose a constrained decode operation with
this acceptance contract. The exact API needs upstream design review.

Any future integration must test normal zlib headers/checksums separately,
declared lengths and output bounds, full consumption, forged suffixes,
truncation at every position, and Java interoperability. No `.mooncakes`
cache files were modified and no upstream issue/PR was submitted.

## Follow-up: the retained native decoder required the same correction

The wrapped version of the truncated example was reproduced against the
existing public `decompress(Zlib, ..., uncompressed_size=5)` path: it incorrectly
returned `hello`. A regression test failed before the change and passed after it.

The native fallback now uses `inflate(..., Z_BLOCK)` and the documented
`data_type` boundary/unused-bit indicators to locate each block. Acceptance
requires a fully decoded, non-final, empty stored block ending byte-aligned,
exact output size and complete input consumption. A complete final stream
still requires a valid Adler checksum. This follows the
[zlib inflate contract](https://zlib.net/manual.html), not the suffix alone.

`moon run scripts/verify-zlib-boundary.mbtx` validates the product C stub under
ASan/UBSan with the counterexamples and 1000 deterministic zlib-generated
sync-flush/full-stream samples, levels 0–9 and sizes up to 256 KiB. It replaces
only the MoonBit output allocator in the harness; real zlib performs decoding.
LeakSanitizer is disabled. The regular MoonBit tests cover the public wrapper.
This validates the retained native path; P1/P3 remain blocked for pure MoonBit.
