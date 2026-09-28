# OpenSSL 3 ABI declarations

`openssl_abi.h` contains the small set of public opaque types, function
signatures and constants used by MoonPulsar's TLS and ECIES adapters. It does
not contain OpenSSL implementations or private object layouts. Ordinary
builds need no OpenSSL development headers. Enhanced TLS and ECIES still
load OpenSSL 3 shared libraries at runtime.

The declarations were checked against the OpenSSL **3.6.3** Homebrew headers
on macOS arm64 with Apple Clang 21. The maintainer check includes the real
headers and statically compares every function type and constant with the
production table; a deliberately incorrect constant must fail that check.
Run from the repository root:

```sh
moon run scripts/verify-native-abi.mbtx -- --openssl-include /path/to/openssl/include
```

Source specification: the `include/openssl/{ssl,bio,ec,evp,pem,hmac,sha,err,types}.h`
headers of [OpenSSL](https://github.com/openssl/openssl). Recorded input hashes:

| Header | SHA256 |
| --- | --- |
| ssl.h | 5828d7e9a1c4aa6f9f424e86cc5ed26ed4f04e9efefb40692d61fcf7c29965fa |
| ec.h | 2cc0edb6a18c4c1199bfce6799ca56362a6c27d41dfdfee62177a3d53a8c9a65 |
| types.h | de5422d2e621df11c17ab18473ea4f2368ecc64b1e45311c4890cc62b31f81c5 |
| evp.h | 0c2e950834c4cd0ac8a887f039400932e7cb1d6aaf2f80da73698a8ed5c3b318 |
| pem.h | 9098124ff7a21c8f1db5ddead59b1cf092f88c9f44bf88dacd3c8de9c67ade76 |
| bio.h | d927b58ab0993cee60b6520f29b9d632b35fc2601e71836c8e70b7456b6291d9 |
| hmac.h | 9caf0b993c6ae2b3c75b9b585c8539dd358ca5f4edc42447295f87fc504eb1b9 |
| sha.h | 7e006accd6565cc97dee672c097acc5e38482be7cd4fa1e0c316bf466425b895 |
| err.h | 0d673984ff62d55d1fb4534d8136b7325e5264efeeb0b911cc94a4922681e080 |

The checker is also intended for OpenSSL 3.0/GCC; until that job executes,
the local Clang result alone does not establish that platform's validation.
`point_conversion_form_t` must be ABI-compatible with `unsigned int`, as
verified by the checker on each supported compiler. Builds with altered
enum ABI flags are outside this validation.

This adjustment does **not** eliminate the separate zlib development-header
requirement. `native_zlib` remains while the pure MoonBit sync-flush boundary
gate is unresolved. See `scripts/NATIVE_DEPENDENCY_PLAN.md` in the source repo.
