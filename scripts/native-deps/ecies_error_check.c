/* Regression for the owned ECIES adapter, using the real OpenSSL 3 library
 * and MoonBit byte allocator. No TLS, network, or dependency source changes. */
#define OPENSSL_SUPPRESS_DEPRECATED
#include <openssl/err.h>
#include <openssl/evp.h>
#include <moonbit_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef MP_ECIES_SOURCE
#define MP_ECIES_SOURCE "../../native_ecies/native_ecies.c"
#endif
#include MP_ECIES_SOURCE

#define CHECK(condition) do { \
  if (!(condition)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #condition); \
    return 0; \
  } \
} while (0)

struct fixture { unsigned char *bytes; int len; };
static mp_EVP_PKEY_get1_EC_KEY_fn actual_get_ec;
static int ec_getter_calls;

static EC_KEY *count_get_ec(EVP_PKEY *key) {
  ec_getter_calls++;
  return actual_get_ec(key);
}

static struct fixture read_fixture(const char *dir, const char *name) {
  char path[4096];
  if (snprintf(path, sizeof(path), "%s/%s", dir, name) >= (int)sizeof(path)) abort();
  FILE *file = fopen(path, "rb");
  if (!file || fseek(file, 0, SEEK_END) != 0) abort();
  long size = ftell(file);
  if (size <= 0 || size > 65536 || fseek(file, 0, SEEK_SET) != 0) abort();
  unsigned char *bytes = malloc((size_t)size);
  if (!bytes || fread(bytes, 1, (size_t)size, file) != (size_t)size) abort();
  if (fclose(file) != 0) abort();
  return (struct fixture){bytes, (int)size};
}

/* Each seeded case checks both records, data and the caller's existing mark.
 * No assertion or observer clears/pops the queue until after the operation. */
static void seed_caller_errors(void) {
  ERR_raise_data(ERR_LIB_USER, 1001, "%s", "caller-first");
  ERR_raise_data(ERR_LIB_USER, 1002, "%s", "caller-second");
  if (ERR_set_mark() != 1) abort();
}

static int check_queue(int seeded) {
  if (!seeded) {
    CHECK(ERR_peek_error() == 0);
    return 1;
  }
  CHECK(ERR_peek_error() == ERR_PACK(ERR_LIB_USER, 0, 1001));
  CHECK(ERR_peek_last_error() == ERR_PACK(ERR_LIB_USER, 0, 1002));
  ERR_raise(ERR_LIB_USER, 1003);
  CHECK(ERR_pop_to_mark() == 1);
  CHECK(ERR_peek_last_error() == ERR_PACK(ERR_LIB_USER, 0, 1002));
  CHECK(ERR_clear_last_mark() == 0); /* the caller's single mark was consumed */
  for (int reason = 1001; reason <= 1002; reason++) {
    const char *file = NULL, *func = NULL, *data = NULL;
    int line = 0, flags = 0;
    CHECK(ERR_get_error_all(&file, &line, &func, &data, &flags) ==
          ERR_PACK(ERR_LIB_USER, 0, reason));
    CHECK(file && strstr(file, "ecies_error_check.c") && line > 0);
    CHECK(func && strcmp(func, "seed_caller_errors") == 0);
    CHECK(data && strcmp(data, reason == 1001 ? "caller-first" : "caller-second") == 0);
    CHECK((flags & ERR_TXT_STRING) != 0);
  }
  CHECK(ERR_peek_error() == 0);
  return 1;
}

static int validate_keys(struct fixture rsa_public, struct fixture rsa_private,
                         struct fixture ec_public, struct fixture ec_private) {
  const unsigned char malformed[] = "not a PEM key";
  const unsigned char truncated[] =
      "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n";
  for (int seeded = 0; seeded <= 1; seeded++) {
    if (seeded) seed_caller_errors();
    int calls_before = ec_getter_calls;
    CHECK(moonpulsar_ecies_public_valid(rsa_public.bytes, rsa_public.len) == 0);
    CHECK(ec_getter_calls == calls_before); /* reject by type, not failed getter */
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    calls_before = ec_getter_calls;
    CHECK(moonpulsar_ecies_private_valid(rsa_private.bytes, rsa_private.len) == 0);
    CHECK(ec_getter_calls == calls_before);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(moonpulsar_ecies_public_valid(ec_public.bytes, ec_public.len) == 1);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(moonpulsar_ecies_private_valid(ec_private.bytes, ec_private.len) == 1);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(moonpulsar_ecies_public_valid(malformed, sizeof(malformed) - 1) == 0);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(moonpulsar_ecies_public_valid(truncated, sizeof(truncated) - 1) == 0);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(moonpulsar_ecies_private_valid(malformed, sizeof(malformed) - 1) == 0);
    CHECK(check_queue(seeded));
    printf("PASS key predicates and parse failures (%s caller queue)\n",
           seeded ? "preserved marked" : "empty");
  }
  return 1;
}

static int check_empty(moonbit_bytes_t bytes) {
  int empty = Moonbit_array_length(bytes) == 0;
  moonbit_decref(bytes);
  CHECK(empty);
  return 1;
}

static int roundtrip(struct fixture public, struct fixture private,
                     struct fixture rsa_public, const char *curve) {
  unsigned char key[32];
  for (int i = 0; i < 32; i++) key[i] = (unsigned char)i;
  for (int seeded = 0; seeded <= 1; seeded++) {
    if (seeded) seed_caller_errors();
    moonbit_bytes_t wrapped = moonpulsar_ecies_wrap(public.bytes, public.len, key, 32);
    int len = Moonbit_array_length(wrapped);
    CHECK(len > 53);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    moonbit_bytes_t unwrapped = moonpulsar_ecies_unwrap(private.bytes, private.len, wrapped, len);
    CHECK(Moonbit_array_length(unwrapped) == 32 && memcmp(unwrapped, key, 32) == 0);
    moonbit_decref(unwrapped);
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    wrapped[len - 1] ^= 1;
    CHECK(check_empty(moonpulsar_ecies_unwrap(private.bytes, private.len, wrapped, len)));
    wrapped[len - 1] ^= 1;
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    /* Correct uncompressed-point length, coordinates outside the curve. */
    /* Give fortify a known C object extent instead of inferring it from
     * the MoonBit allocation; keep the original blob for later cases. */
    unsigned char invalid_point[1024];
    CHECK(len <= (int)sizeof(invalid_point));
    memcpy(invalid_point, wrapped, (size_t)len);
    memset(invalid_point + 1, 0xff, (size_t)len - 53);
    CHECK(check_empty(moonpulsar_ecies_unwrap(private.bytes, private.len, invalid_point, len)));
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(check_empty(moonpulsar_ecies_unwrap(private.bytes, private.len, wrapped, len - 1)));
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(check_empty(moonpulsar_ecies_wrap(rsa_public.bytes, rsa_public.len, key, 32)));
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(check_empty(moonpulsar_ecies_wrap(public.bytes, public.len, key, 31)));
    CHECK(check_queue(seeded));
    const unsigned char malformed[] = "not a PEM key";
    if (seeded) seed_caller_errors();
    CHECK(check_empty(moonpulsar_ecies_wrap(malformed, sizeof(malformed) - 1, key, 32)));
    CHECK(check_queue(seeded));
    if (seeded) seed_caller_errors();
    CHECK(check_empty(moonpulsar_ecies_unwrap(malformed, sizeof(malformed) - 1, wrapped, len)));
    CHECK(check_queue(seeded));
    moonbit_decref(wrapped);
    printf("PASS %s roundtrip, authentication/point/key rejection (%s caller queue)\n",
           curve, seeded ? "preserved marked" : "empty");
  }
  return 1;
}

int main(int argc, char **argv) {
  if (argc != 2) return 2;
  moonbit_allocator_init();
  if (crypto_status() != 1 || dyn_OpenSSL_version_num != &OpenSSL_version_num) {
    fprintf(stderr, "FAIL test and adapter must use the same OpenSSL 3 library\n");
    return 1;
  }
  printf("OpenSSL %s\n", OpenSSL_version(OPENSSL_VERSION));
  ERR_clear_error();
  actual_get_ec = dyn_EVP_PKEY_get1_EC_KEY;
  dyn_EVP_PKEY_get1_EC_KEY = count_get_ec;
  struct fixture rsa_public = read_fixture(argv[1], "rsa_public.pem");
  struct fixture rsa_private = read_fixture(argv[1], "rsa_private.pem");
  struct fixture public = read_fixture(argv[1], "ec_public.pem");
  struct fixture private = read_fixture(argv[1], "ec_private.pem");
  if (!validate_keys(rsa_public, rsa_private, public, private)) return 1;
  free(public.bytes);
  free(private.bytes);
  const char *curves[] = {"p256", "p384", "p521"};
  for (int i = 0; i < 3; i++) {
    char name[64];
    if (i == 2) {
      public = read_fixture(argv[1], "ec_public.pem");
      private = read_fixture(argv[1], "ec_private.pem");
    } else {
      snprintf(name, sizeof(name), "ec_%s_public.pem", curves[i]);
      public = read_fixture(argv[1], name);
      snprintf(name, sizeof(name), "ec_%s_private.pem", curves[i]);
      private = read_fixture(argv[1], name);
    }
    if (!roundtrip(public, private, rsa_public, curves[i])) return 1;
    free(public.bytes);
    free(private.bytes);
  }
  free(rsa_public.bytes);
  free(rsa_private.bytes);
  puts("PASS 68 ECIES boundary checks; failures remain false/empty");
  return 0;
}
