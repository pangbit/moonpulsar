# Pure MoonBit sync-flush acceptance

The counterexample in `ZLIB_BOUNDARY_FINDING.md` rules out accepting a stream
from `NeedMoreInput`, output length and marker bytes alone. The implementation
in `zlib_sync_flush.mbt` instead scans the RFC 1951 grammar, without producing
the decompressed payload. It uses the released flate 0.8.3 API unchanged.

## Why the transformation is safe

1. Validate RFC 1950 CMF/FLG, window size and FCHECK; reject preset dictionaries.
2. Start at the actual first block, then parse every block header. Stored blocks
   require complete LEN/NLEN and payload. Fixed and dynamic Huffman blocks are
   scanned token by token through their end-of-block code; length/distance
   extra bits, repeats and history distances are validated. Every access is
   bounded by physical input. Output counting cannot exceed the declared size.
3. Reject any original final block in this compatibility route. Such streams
   must use ordinary zlib decoding, including Adler-32 verification.
4. Accept only when the last parsed block is an empty, non-final stored block,
   ending exactly at physical EOF with the declared amount of output. Keep the
   bit position at which that block's BFINAL was parsed. Padding bits are not
   confused with block headers, including headers sharing the prior byte.
5. Copy the raw DEFLATE bytes and change only that known BFINAL from zero to
   one. Up to that bit, the grammar and decoded symbols are identical; the bit
   only declares this already validated empty block to be final. No input or
   output is added. There are no earlier final blocks or trailing bytes.
6. Call flate's bounded `inflate_into` with exactly the declared output
   capacity; require its result length to match. flate independently checks
   the Huffman/DEFLATE decoding. Its prefix-oriented convenience API cannot
   mask trailing data here because step 4 already proved the exact endpoint.

The four-byte suffix in `compression.mbt` is only a dispatch optimization.
It cannot authorize acceptance. If structural validation fails, ordinary
zlib decoding still gets a chance to verify a complete stream whose checksum
happens to end in those bytes. A bad checksum cannot pass through the scanner,
since the scanner rejects original final blocks.

## Bounds and unchanged behavior

The scanner retains only the input, bit cursor, output count and small bounded
Huffman tables. It never builds the decoded payload. Compatibility output is
limited to `MAX_CHUNKED_MESSAGE_BYTES`; compatibility compressed input is
limited to 0x0fffffff bytes so bit offsets cannot overflow a signed Int. Every
loop either consumes bits/bytes or advances a bounded code-length table.

The raw copy and bounded decode use O(compressed size + declared size) storage.
The empty sync-flush/zero declared size remains rejected, as before; ordinary
complete empty zlib streams remain accepted. Ordinary complete-stream decoding
and its existing declared-size semantics are unchanged.

`native_zlib` was an unpublished helper package and is removed. Its previous
fixed implementation from `ccf4214` is retained only under
`scripts/native-deps/reference-zlib/`, excluded from the published ZIP and the
normal package graph. Root public interfaces are unchanged.

## Reproducible gate

`moon run scripts/verify-zlib-replacement.mbtx` generates 1000 deterministic
zlib reference inputs, tests both sync-flush and complete forms, and compares
against the old native-backed public path in a temporary ZIP copy. It also
checks 18000 bit mutations against the fixed native boundary validator,
truncations and wrong sizes. Native reference code and headers are used only
by this maintainer gate, never by the product build.

Six fixed performance cases use zlib's default level 6 and cover
1 KiB/32 KiB/256 KiB repetitive and seeded pseudorandom inputs.
Five timing rounds compare the old complete public path
(flate attempt followed by native fallback) against the new public path.
Five fresh-process RSS rounds per case/implementation use 200 decodes each;
RSS includes the runtime and fixture, not just the decoder's allocations.
Reports retain individual rounds rather than reporting only the best result.

Format authority: [RFC 1951](https://www.rfc-editor.org/rfc/rfc1951.html).
