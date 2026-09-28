#include <moonbit.h>
#include "../native_support/openssl_abi.h"
#include <dlfcn.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Pulsar Java 4.2.4 uses Bouncy Castle 1.84 ECIES: an uncompressed ephemeral
 * point, ECDH, KDF2-SHA1, a 128-bit HMAC-SHA1 key, XOR, and a 20-byte tag. */
#define DECLARE_CRYPTO(ret, name, args) static mp_##name##_fn dyn_##name;
MP_CRYPTO_FUNCTIONS(DECLARE_CRYPTO)
#undef DECLARE_CRYPTO

static int crypto_status(void) {
  static int status = 0;
  if (status) return status;
#ifdef __APPLE__
  void *lib = dlopen("/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib", RTLD_NOW);
  if (!lib) lib = dlopen("/usr/local/opt/openssl@3/lib/libcrypto.3.dylib", RTLD_NOW);
#else
  void *lib = dlopen("libcrypto.so.3", RTLD_NOW);
#endif
  if (!lib) return status = -1;
#define LOAD_CRYPTO(ret, name, args) do { \
  dyn_##name = (mp_##name##_fn)dlsym(lib, #name); \
  if (!dyn_##name) { status = -2; goto failure; } \
} while (0);
  MP_CRYPTO_FUNCTIONS(LOAD_CRYPTO)
#undef LOAD_CRYPTO
  if ((dyn_OpenSSL_version_num() >> 28) != 3) { status = -3; goto failure; }
  return status = 1;
failure:
#define CLEAR_CRYPTO(ret, name, args) dyn_##name = NULL;
  MP_CRYPTO_FUNCTIONS(CLEAR_CRYPTO)
#undef CLEAR_CRYPTO
  dlclose(lib);
  return status;
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t moonpulsar_ecies_load_error(void) {
  const char *message = mp_openssl_load_error(crypto_status());
  moonbit_bytes_t result = moonbit_make_bytes((int)strlen(message), 0);
  memcpy(result, message, strlen(message));
  return result;
}

#define BIO_new_mem_buf dyn_BIO_new_mem_buf
#define BIO_free dyn_BIO_free
#define PEM_read_bio_PUBKEY dyn_PEM_read_bio_PUBKEY
#define PEM_read_bio_PrivateKey dyn_PEM_read_bio_PrivateKey
#define EVP_PKEY_free dyn_EVP_PKEY_free
#define EVP_PKEY_get1_EC_KEY dyn_EVP_PKEY_get1_EC_KEY
#define EC_KEY_free dyn_EC_KEY_free
#define EC_KEY_get0_group dyn_EC_KEY_get0_group
#define EC_KEY_get0_public_key dyn_EC_KEY_get0_public_key
#define EC_KEY_get0_private_key dyn_EC_KEY_get0_private_key
#define EC_KEY_new dyn_EC_KEY_new
#define EC_KEY_set_group dyn_EC_KEY_set_group
#define EC_KEY_generate_key dyn_EC_KEY_generate_key
#define EC_GROUP_get_degree dyn_EC_GROUP_get_degree
#define EC_POINT_point2oct dyn_EC_POINT_point2oct
#define EC_POINT_new dyn_EC_POINT_new
#define EC_POINT_oct2point dyn_EC_POINT_oct2point
#define EC_POINT_free dyn_EC_POINT_free
#define ECDH_compute_key dyn_ECDH_compute_key
#define SHA1 dyn_SHA1
#define HMAC dyn_HMAC
#define EVP_sha1 dyn_EVP_sha1
#define OPENSSL_cleanse dyn_OPENSSL_cleanse

static EC_KEY *read_ec_key(const unsigned char *pem, int len, int private_key) {
  if (crypto_status() != 1 || len <= 0) return NULL;
  BIO *bio = BIO_new_mem_buf(pem, len);
  if (!bio) return NULL;
  EVP_PKEY *pkey = private_key
      ? PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL)
      : PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
  BIO_free(bio);
  if (!pkey) return NULL;
  EC_KEY *ec = EVP_PKEY_get1_EC_KEY(pkey);
  EVP_PKEY_free(pkey);
  return ec;
}

MOONBIT_FFI_EXPORT
int moonpulsar_ecies_public_valid(const unsigned char *pem, int len) {
  EC_KEY *key = read_ec_key(pem, len, 0);
  if (!key) return 0;
  int valid = EC_KEY_get0_group(key) != NULL && EC_KEY_get0_public_key(key) != NULL;
  EC_KEY_free(key);
  return valid;
}

MOONBIT_FFI_EXPORT
int moonpulsar_ecies_private_valid(const unsigned char *pem, int len) {
  EC_KEY *key = read_ec_key(pem, len, 1);
  if (!key) return 0;
  int valid = EC_KEY_get0_group(key) != NULL && EC_KEY_get0_private_key(key) != NULL;
  EC_KEY_free(key);
  return valid;
}

static moonbit_bytes_t empty_bytes(void) {
  return moonbit_make_bytes(0, 0);
}

/* KDF2-SHA1 over V || Z with the big-endian counter starting at one. */
static int derive_stream(const unsigned char *point, int point_len,
                         const unsigned char *secret, int secret_len,
                         unsigned char stream[48]) {
  if (point_len <= 0 || secret_len <= 0 || point_len > 1024 || secret_len > 1024) return 0;
  unsigned char material[2052];
  memcpy(material, point, point_len);
  memcpy(material + point_len, secret, secret_len);
  int base = point_len + secret_len;
  for (int counter = 1; counter <= 3; counter++) {
    material[base] = 0;
    material[base + 1] = 0;
    material[base + 2] = 0;
    material[base + 3] = (unsigned char)counter;
    unsigned char digest[MP_SHA_DIGEST_LENGTH];
    if (!SHA1(material, (size_t)base + 4, digest)) return 0;
    int offset = (counter - 1) * MP_SHA_DIGEST_LENGTH;
    int copy = offset + MP_SHA_DIGEST_LENGTH <= 48 ? MP_SHA_DIGEST_LENGTH : 48 - offset;
    memcpy(stream + offset, digest, (size_t)copy);
    OPENSSL_cleanse(digest, sizeof(digest));
  }
  OPENSSL_cleanse(material, (size_t)base + 4);
  return 1;
}

static int mac_ciphertext(const unsigned char stream[48],
                          const unsigned char cipher[32], unsigned char tag[20]) {
  unsigned char input[40] = {0};
  memcpy(input, cipher, 32); /* encoding vector length tag: eight zero bytes */
  unsigned int tag_len = 0;
  return HMAC(EVP_sha1(), stream, 16, input, sizeof(input), tag, &tag_len) != NULL
      && tag_len == 20;
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t moonpulsar_ecies_wrap(const unsigned char *pem, int pem_len,
                                      const unsigned char *key, int key_len) {
  if (key_len != 32) return empty_bytes();
  EC_KEY *recipient = read_ec_key(pem, pem_len, 0);
  if (!recipient) return empty_bytes();
  const EC_GROUP *group = EC_KEY_get0_group(recipient);
  EC_KEY *ephemeral = NULL;
  moonbit_bytes_t result = empty_bytes();
  unsigned char secret[1024] = {0};
  unsigned char stream[48] = {0};
  do {
    if (!group) break;
    int field_len = (EC_GROUP_get_degree(group) + 7) / 8;
    if (field_len <= 0 || field_len > (int)sizeof(secret)) break;
    ephemeral = EC_KEY_new();
    if (!ephemeral || EC_KEY_set_group(ephemeral, group) != 1 ||
        EC_KEY_generate_key(ephemeral) != 1) break;
    size_t point_len = EC_POINT_point2oct(group, EC_KEY_get0_public_key(ephemeral),
                                         MP_POINT_CONVERSION_UNCOMPRESSED, NULL, 0, NULL);
    if (!point_len || point_len > 1024) break;
    result = moonbit_make_bytes((int)point_len + 32 + 20, 0);
    if (EC_POINT_point2oct(group, EC_KEY_get0_public_key(ephemeral),
                           MP_POINT_CONVERSION_UNCOMPRESSED, result, point_len, NULL) != point_len) break;
    int secret_len = ECDH_compute_key(secret, (size_t)field_len,
                                      EC_KEY_get0_public_key(recipient), ephemeral, NULL);
    if (secret_len != field_len || !derive_stream(result, (int)point_len, secret, secret_len, stream)) break;
    for (int i = 0; i < 32; i++) result[point_len + i] = key[i] ^ stream[16 + i];
    if (!mac_ciphertext(stream, result + point_len, result + point_len + 32)) break;
    EC_KEY_free(ephemeral);
    EC_KEY_free(recipient);
    OPENSSL_cleanse(secret, sizeof(secret));
    OPENSSL_cleanse(stream, sizeof(stream));
    return result;
  } while (0);
  if (ephemeral) EC_KEY_free(ephemeral);
  EC_KEY_free(recipient);
  OPENSSL_cleanse(secret, sizeof(secret));
  OPENSSL_cleanse(stream, sizeof(stream));
  return empty_bytes();
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t moonpulsar_ecies_unwrap(const unsigned char *pem, int pem_len,
                                        const unsigned char *wrapped, int wrapped_len) {
  EC_KEY *recipient = read_ec_key(pem, pem_len, 1);
  if (!recipient) return empty_bytes();
  const EC_GROUP *group = EC_KEY_get0_group(recipient);
  EC_POINT *ephemeral = NULL;
  unsigned char secret[1024] = {0};
  unsigned char stream[48] = {0};
  moonbit_bytes_t result = empty_bytes();
  do {
    if (!group || wrapped_len < 53) break;
    int field_len = (EC_GROUP_get_degree(group) + 7) / 8;
    if (field_len <= 0 || field_len > (int)sizeof(secret)) break;
    int point_len = wrapped[0] == 4 ? 1 + 2 * field_len
        : (wrapped[0] == 2 || wrapped[0] == 3) ? 1 + field_len : 0;
    if (!point_len || wrapped_len != point_len + 32 + 20) break;
    ephemeral = EC_POINT_new(group);
    if (!ephemeral || EC_POINT_oct2point(group, ephemeral, wrapped, (size_t)point_len, NULL) != 1) break;
    int secret_len = ECDH_compute_key(secret, (size_t)field_len, ephemeral, recipient, NULL);
    if (secret_len != field_len || !derive_stream(wrapped, point_len, secret, secret_len, stream)) break;
    unsigned char expected[20];
    if (!mac_ciphertext(stream, wrapped + point_len, expected)) break;
    unsigned char mismatch = 0;
    for (int i = 0; i < 20; i++) mismatch |= expected[i] ^ wrapped[point_len + 32 + i];
    if (mismatch) break;
    result = moonbit_make_bytes(32, 0);
    for (int i = 0; i < 32; i++) result[i] = wrapped[point_len + i] ^ stream[16 + i];
  } while (0);
  if (ephemeral) EC_POINT_free(ephemeral);
  EC_KEY_free(recipient);
  OPENSSL_cleanse(secret, sizeof(secret));
  OPENSSL_cleanse(stream, sizeof(stream));
  return result;
}
