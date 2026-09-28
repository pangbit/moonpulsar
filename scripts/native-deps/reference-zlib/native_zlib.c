#include "moonbit.h"
#include <dlfcn.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

typedef int (*inflate_init_fn)(z_streamp, const char *, int);
typedef int (*inflate_fn)(z_streamp, int);
typedef int (*inflate_end_fn)(z_streamp);

MOONBIT_FFI_EXPORT
moonbit_bytes_t moonpulsar_zlib_uncompress_exact(
    moonbit_bytes_t input, int32_t input_length, int32_t output_length) {
  if (input_length < 4 || output_length <= 0) {
    return moonbit_make_bytes(0, 0);
  }
  void *library = NULL;
#ifdef __APPLE__
  library = dlopen("/usr/lib/libz.1.dylib", RTLD_NOW | RTLD_LOCAL);
#else
  library = dlopen("libz.so.1", RTLD_NOW | RTLD_LOCAL);
  if (library == NULL) {
    library = dlopen("libz.so", RTLD_NOW | RTLD_LOCAL);
  }
#endif
  if (library == NULL) {
    return moonbit_make_bytes(0, 0);
  }
  inflate_init_fn initialize = (inflate_init_fn)dlsym(library, "inflateInit_");
  inflate_fn inflate_step = (inflate_fn)dlsym(library, "inflate");
  inflate_end_fn finish = (inflate_end_fn)dlsym(library, "inflateEnd");
  if (initialize == NULL || inflate_step == NULL || finish == NULL) {
    dlclose(library);
    return moonbit_make_bytes(0, 0);
  }
  Bytef *scratch = malloc((size_t)output_length + 1);
  if (scratch == NULL) {
    dlclose(library);
    return moonbit_make_bytes(0, 0);
  }
  z_stream stream;
  memset(&stream, 0, sizeof(stream));
  stream.next_in = (Bytef *)input;
  stream.avail_in = (uInt)input_length;
  stream.next_out = (Bytef *)scratch;
  stream.avail_out = (uInt)output_length + 1;
  int initialized = initialize(&stream, ZLIB_VERSION, sizeof(stream));
  int result = initialized;
  int sync_flushed = 0;
  int have_block_start = 0;
  if (initialized == Z_OK) {
    for (;;) {
      uLong before_in = stream.total_in;
      uLong before_out = stream.total_out;
      int before_type = stream.data_type;
      /* Z_BLOCK first stops after the zlib header, then after each block.
       * At a boundary, data_type's low bits count unused input bits.
       * Remember the next block's actual bit offset, including shared bytes. */
      uint64_t start_bit = (uint64_t)before_in * 8 - (before_type & 63);
      result = inflate_step(&stream, Z_BLOCK);
      sync_flushed = 0;
      if (result == Z_OK && have_block_start && stream.data_type == 128 &&
          stream.total_out == before_out && start_bit + 3 <= (uint64_t)input_length * 8) {
        unsigned header = 0;
        for (unsigned bit = 0; bit < 3; bit++) {
          uint64_t offset = start_bit + bit;
          header |= ((input[offset / 8] >> (offset % 8)) & 1u) << bit;
        }
        /* Exactly an empty, non-final stored block ending byte-aligned.
         * Unlike a marker suffix, this proves LEN/NLEN were fully decoded. */
        sync_flushed = header == 0;
      }
      have_block_start = (stream.data_type & 128) != 0;
      if (result != Z_OK || stream.avail_in == 0 || stream.avail_out == 0 ||
          (stream.total_in == before_in && stream.total_out == before_out &&
           stream.data_type == before_type)) {
        break;
      }
    }
  }
  int complete = result == Z_STREAM_END;
  int valid = (complete || sync_flushed) && stream.avail_in == 0 &&
      stream.total_out == (uLong)output_length;
  if (initialized == Z_OK) {
    finish(&stream);
  }
  dlclose(library);
  if (!valid) {
    free(scratch);
    return moonbit_make_bytes(0, 0);
  }
  moonbit_bytes_t output = moonbit_make_bytes(output_length, 0);
  memcpy(output, scratch, output_length);
  free(scratch);
  return output;
}
