/* Test-only forced include for an extracted package, never a product option. */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
static inline void *moonpulsar_test_missing_openssl(const char *name, int flags) {
  (void)name; (void)flags;
  const char *marker = getenv("MOONPULSAR_TEST_LOADER_MARKER");
  if (marker != NULL) {
    FILE *file = fopen(marker, "a");
    if (file == NULL) abort();
    fputs("attempted\n", file);
    fclose(file);
  }
  return NULL;
}
#define dlopen moonpulsar_test_missing_openssl
