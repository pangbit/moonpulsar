#include <dlfcn.h>
#include <stdio.h>
int main(void) {
  const char *names[] = {"libssl.so", "libssl.so.3", "libcrypto.so", "libcrypto.so.3", "libz.so", "libz.so.1"};
  for (unsigned i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
    void *handle = dlopen(names[i], RTLD_NOW | RTLD_LOCAL);
    if (handle) { fprintf(stderr, "unexpected library: %s\n", names[i]); dlclose(handle); return 1; }
    printf("ABSENT %s\n", names[i]);
  }
  return 0;
}
