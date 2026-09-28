/* Exercise the product stub with real zlib and ASan, replacing only the
 * MoonBit byte allocator so the harness does not alter the installed runtime. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <moonbit.h>
#include <zlib.h>

static int allocated_length;
static moonbit_bytes_t test_make_bytes(int32_t length, int value) {
  allocated_length = length;
  moonbit_bytes_t bytes = malloc(length ? (size_t)length : 1);
  assert(bytes != NULL);
  memset(bytes, value, length);
  return bytes;
}
#define moonbit_make_bytes test_make_bytes
#include "reference-zlib/native_zlib.c"
#undef moonbit_make_bytes

static void check(unsigned char *input, int length, unsigned char *expected,
                  int output_length, int valid) {
  moonbit_bytes_t decoded = moonpulsar_zlib_uncompress_exact(input, length, output_length);
  assert(allocated_length == (valid ? output_length : 0));
  if (valid) assert(memcmp(decoded, expected, output_length) == 0);
  free(decoded);
}

static void record(FILE *file, unsigned char *encoded, int encoded_length,
                   unsigned char *plain, int plain_length) {
  if (!file) return;
  for (int i = 0; i < 4; i++) assert(fputc((encoded_length >> (8 * i)) & 255, file) != EOF);
  for (int i = 0; i < 4; i++) assert(fputc((plain_length >> (8 * i)) & 255, file) != EOF);
  assert(fwrite(encoded, 1, encoded_length, file) == (size_t)encoded_length);
  assert(fwrite(plain, 1, plain_length, file) == (size_t)plain_length);
}

int main(int argc, char **argv) {
  assert(argc == 1 || argc == 2);
  FILE *corpus = argc == 2 ? fopen(argv[1], "wb") : NULL;
  assert(argc == 1 || corpus != NULL);
  unsigned char valid[] = "\x78\x9c\x00\x05\x00\xfa\xffhello\x00\x00\x00\xff\xff";
  unsigned char truncated[] = "\x78\x9c\x00\x05\x00\xfa\xffhello\x00\x00\xff\xff";
  unsigned char forged[] = "\x78\x9c\x00\x05\x00\xfa\xffx\x00\x00\xff\xff";
  check(valid, sizeof(valid) - 1, (unsigned char *)"hello", 5, 1);
  check(truncated, sizeof(truncated) - 1, NULL, 5, 0);
  check(forged, sizeof(forged) - 1, NULL, 5, 0);
  for (int end = 0; end < (int)sizeof(valid) - 1; end++) {
    check(valid, end, NULL, 5, 0);
  }

  unsigned char *plain = malloc(262144);
  unsigned char *encoded = malloc(300000);
  assert(plain && encoded);
  uint32_t random = 0x12345678;
  for (int sample = 0; sample < 1000; sample++) {
    int length = sample % 25 == 0 ? 262144 : sample % 25 == 1 ? 32768 : sample + 1;
    if (sample < 6) length = sample < 2 ? 1024 : sample < 4 ? 32768 : 262144;
    for (int i = 0; i < length; i++) {
      random = random * 1664525u + 1013904223u;
      plain[i] = sample % 2 == 0 ? (unsigned char)(i % 7) : (unsigned char)(random >> 24);
    }
    z_stream compressor = {0};
    assert(deflateInit(&compressor, sample < 6 ? 6 : sample % 10) == Z_OK);
    compressor.next_in = plain;
    compressor.avail_in = length;
    compressor.next_out = encoded;
    compressor.avail_out = 300000;
    assert(deflate(&compressor, Z_SYNC_FLUSH) == Z_OK);
    assert(compressor.avail_in == 0 && compressor.avail_out > 0);
    int flushed_length = (int)compressor.total_out;
    record(corpus, encoded, flushed_length, plain, length);
    check(encoded, flushed_length, plain, length, 1);
    check(encoded, flushed_length, NULL, length - 1, 0);
    check(encoded, flushed_length, NULL, length + 1, 0);
    check(encoded, flushed_length - 1, NULL, length, 0);
    // Continue after a flush and ensure the complete stream also works.
    assert(deflate(&compressor, Z_FINISH) == Z_STREAM_END);
    int complete_length = (int)compressor.total_out;
    record(corpus, encoded, complete_length, plain, length);
    assert(deflateEnd(&compressor) == Z_OK);
    check(encoded, complete_length, plain, length, 1);
    encoded[complete_length - 1] ^= 1;
    check(encoded, complete_length, NULL, length, 0);
  }
  free(encoded);
  free(plain);
  if (corpus) assert(fclose(corpus) == 0);
  printf("PASS zlib %s: boundary regressions and 1000 generated sync-flush/full-stream cases\n", zlibVersion());
  return 0;
}
