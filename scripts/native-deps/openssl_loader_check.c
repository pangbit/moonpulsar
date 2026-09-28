/* Isolated loader fault injection. No product configuration knobs or real
 * OpenSSL calls: each process checks one cached loader outcome. */
#include <assert.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>
#include <moonbit.h>

static int scenario;
static int opened;
static int closed;
static int looked_up;
static unsigned long fake_version(void) {
  return scenario == 3 ? 0x40000000UL : 0x30000000UL;
}
static void unused_symbol(void) { abort(); }
static void *test_dlopen(const char *name, int flags) {
  (void)name; (void)flags;
  if (scenario == 1) return NULL;
  opened++;
  return (void *)&scenario;
}
static int test_dlclose(void *handle) {
  assert(handle == (void *)&scenario);
  closed++;
  return 0;
}
static void *test_dlsym(void *handle, const char *name) {
  assert(handle == (void *)&scenario);
  looked_up++;
  if (strcmp(name, "OpenSSL_version_num") == 0) return (void *)fake_version;
  if (scenario == 2) return NULL;
  return (void *)unused_symbol;
}

#define dlopen test_dlopen
#define dlclose test_dlclose
#define dlsym test_dlsym
#ifdef MP_TEST_TLS
#include "../../native_tls/native_tls.c"
#else
#include "../../native_ecies/native_ecies.c"
#endif

int main(int argc, char **argv) {
  assert(argc == 2);
  scenario = atoi(argv[1]);
  assert(scenario >= 1 && scenario <= 3);
#ifdef MP_TEST_TLS
  assert(moonpulsar_tls_load() == -scenario);
#else
  assert(crypto_status() == -scenario);
#endif
  int previous_lookups = looked_up;
  int previous_opens = opened;
#ifdef MP_TEST_TLS
  assert(moonpulsar_tls_load() == -scenario);
#define CHECK_CLEARED(ret, name, args) assert(dyn_##name == NULL);
  MP_TLS_FUNCTIONS(CHECK_CLEARED)
#else
  assert(crypto_status() == -scenario);
#define CHECK_CLEARED(ret, name, args) assert(dyn_##name == NULL);
  MP_CRYPTO_FUNCTIONS(CHECK_CLEARED)
#endif
#undef CHECK_CLEARED
  assert(previous_lookups == looked_up);
  assert(previous_opens == opened);
  assert(closed == opened);
  return 0;
}
