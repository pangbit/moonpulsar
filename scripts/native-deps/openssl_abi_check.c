#define OPENSSL_SUPPRESS_DEPRECATED
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include "../../native_support/openssl_abi.h"

#define CHECK_TYPE(ret, name, args) \
  _Static_assert(__builtin_types_compatible_p(mp_##name##_fn, __typeof__(&name)), #name " ABI mismatch");
MP_TLS_FUNCTIONS(CHECK_TYPE)
MP_CRYPTO_FUNCTIONS(CHECK_TYPE)
#undef CHECK_TYPE
#define CHECK_CONSTANT(name, value) \
  _Static_assert(MP_##name == name, #name " value mismatch");
MP_CONSTANTS(CHECK_CONSTANT)
#undef CHECK_CONSTANT
_Static_assert(OPENSSL_VERSION_MAJOR == 3, "OpenSSL 3 headers required");
