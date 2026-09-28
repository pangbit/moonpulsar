# Test-only reference

These files preserve the fixed native Zlib decoder from commit `ccf4214` for
differential and sanitizer checks. They have no package configuration here.
The maintainer gate copies them into a temporary package; normal builds and
published archives exclude this directory. Product decoding is pure MoonBit.
