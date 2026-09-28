#ifndef MOONPULSAR_OPENSSL_ABI_H
#define MOONPULSAR_OPENSSL_ABI_H

/* Minimal OpenSSL 3 ABI, checked against the upstream headers by
 * scripts/native-deps/openssl_abi_check.c. No object layouts are copied.
 * Declaration provenance and supported verification versions: README.md here.
 */
#include <stddef.h>

typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_method_st SSL_METHOD;
typedef struct bio_st BIO;
typedef struct bio_method_st BIO_METHOD;
typedef struct x509_store_ctx_st X509_STORE_CTX;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct evp_md_st EVP_MD;
typedef struct ec_key_st EC_KEY;
typedef struct ec_group_st EC_GROUP;
typedef struct ec_point_st EC_POINT;
typedef struct bignum_st BIGNUM;
typedef struct bignum_ctx BN_CTX;

#define MP_TLS_FUNCTIONS(X) \
  X(unsigned long, OpenSSL_version_num, (void)) \
  X(SSL_CTX *, SSL_CTX_new, (const SSL_METHOD *)) \
  X(void, SSL_CTX_free, (SSL_CTX *)) \
  X(const SSL_METHOD *, TLS_client_method, (void)) \
  X(long, SSL_CTX_ctrl, (SSL_CTX *, int, long, void *)) \
  X(int, SSL_CTX_set_cipher_list, (SSL_CTX *, const char *)) \
  X(int, SSL_CTX_set_ciphersuites, (SSL_CTX *, const char *)) \
  X(void, SSL_CTX_set_verify, (SSL_CTX *, int, int (*)(int, X509_STORE_CTX *))) \
  X(int, SSL_CTX_load_verify_locations, (SSL_CTX *, const char *, const char *)) \
  X(int, SSL_CTX_set_default_verify_paths, (SSL_CTX *)) \
  X(int, SSL_CTX_use_certificate_chain_file, (SSL_CTX *, const char *)) \
  X(int, SSL_CTX_use_PrivateKey_file, (SSL_CTX *, const char *, int)) \
  X(int, SSL_CTX_check_private_key, (const SSL_CTX *)) \
  X(SSL *, SSL_new, (SSL_CTX *)) \
  X(void, SSL_free, (SSL *)) \
  X(int, SSL_set1_host, (SSL *, const char *)) \
  X(long, SSL_ctrl, (SSL *, int, long, void *)) \
  X(void, SSL_set_bio, (SSL *, BIO *, BIO *)) \
  X(void, SSL_set_connect_state, (SSL *)) \
  X(int, SSL_do_handshake, (SSL *)) \
  X(long, SSL_get_verify_result, (const SSL *)) \
  X(int, SSL_get_error, (const SSL *, int)) \
  X(int, SSL_read, (SSL *, void *, int)) \
  X(int, SSL_write, (SSL *, const void *, int)) \
  X(BIO *, BIO_new, (const BIO_METHOD *)) \
  X(const BIO_METHOD *, BIO_s_mem, (void)) \
  X(int, BIO_free, (BIO *)) \
  X(int, BIO_write, (BIO *, const void *, int)) \
  X(int, BIO_read, (BIO *, void *, int)) \
  X(long, BIO_ctrl, (BIO *, int, long, void *)) \
  X(void, ERR_clear_error, (void)) \
  X(void, ERR_error_string_n, (unsigned long, char *, size_t)) \
  X(unsigned long, ERR_get_error, (void))

/* point_conversion_form_t is compatible with unsigned int on the supported
 * GCC/Clang ABIs. The maintainer check verifies this, rather than assuming it.
 */
#define MP_CRYPTO_FUNCTIONS(X) \
  X(unsigned long, OpenSSL_version_num, (void)) \
  X(BIO *, BIO_new_mem_buf, (const void *, int)) \
  X(int, BIO_free, (BIO *)) \
  X(EVP_PKEY *, PEM_read_bio_PUBKEY, (BIO *, EVP_PKEY **, int (*)(char *, int, int, void *), void *)) \
  X(EVP_PKEY *, PEM_read_bio_PrivateKey, (BIO *, EVP_PKEY **, int (*)(char *, int, int, void *), void *)) \
  X(void, EVP_PKEY_free, (EVP_PKEY *)) \
  X(EC_KEY *, EVP_PKEY_get1_EC_KEY, (EVP_PKEY *)) \
  X(void, EC_KEY_free, (EC_KEY *)) \
  X(const EC_GROUP *, EC_KEY_get0_group, (const EC_KEY *)) \
  X(const EC_POINT *, EC_KEY_get0_public_key, (const EC_KEY *)) \
  X(const BIGNUM *, EC_KEY_get0_private_key, (const EC_KEY *)) \
  X(EC_KEY *, EC_KEY_new, (void)) \
  X(int, EC_KEY_set_group, (EC_KEY *, const EC_GROUP *)) \
  X(int, EC_KEY_generate_key, (EC_KEY *)) \
  X(int, EC_GROUP_get_degree, (const EC_GROUP *)) \
  X(size_t, EC_POINT_point2oct, (const EC_GROUP *, const EC_POINT *, unsigned int, unsigned char *, size_t, BN_CTX *)) \
  X(EC_POINT *, EC_POINT_new, (const EC_GROUP *)) \
  X(int, EC_POINT_oct2point, (const EC_GROUP *, EC_POINT *, const unsigned char *, size_t, BN_CTX *)) \
  X(void, EC_POINT_free, (EC_POINT *)) \
  X(int, ECDH_compute_key, (void *, size_t, const EC_POINT *, const EC_KEY *, void *(*)(const void *, size_t, void *, size_t *))) \
  X(unsigned char *, SHA1, (const unsigned char *, size_t, unsigned char *)) \
  X(unsigned char *, HMAC, (const EVP_MD *, const void *, int, const unsigned char *, size_t, unsigned char *, unsigned int *)) \
  X(const EVP_MD *, EVP_sha1, (void)) \
  X(void, OPENSSL_cleanse, (void *, size_t))

#define MP_DECLARE_TYPE(ret, name, args) typedef ret (*mp_##name##_fn) args;
MP_TLS_FUNCTIONS(MP_DECLARE_TYPE)
MP_CRYPTO_FUNCTIONS(MP_DECLARE_TYPE)
#undef MP_DECLARE_TYPE

#define MP_CONSTANTS(X) \
  X(SSL_VERIFY_NONE, 0) X(SSL_VERIFY_PEER, 1) \
  X(SSL_ERROR_WANT_READ, 2) X(SSL_ERROR_WANT_WRITE, 3) \
  X(SSL_ERROR_ZERO_RETURN, 6) X(SSL_FILETYPE_PEM, 1) \
  X(X509_V_OK, 0) X(BIO_CTRL_PENDING, 10) \
  X(BIO_C_SET_BUF_MEM_EOF_RETURN, 130) \
  X(SSL_CTRL_SET_MIN_PROTO_VERSION, 123) \
  X(SSL_CTRL_SET_MAX_PROTO_VERSION, 124) \
  X(SSL_CTRL_SET_TLSEXT_HOSTNAME, 55) \
  X(TLSEXT_NAMETYPE_host_name, 0) \
  X(POINT_CONVERSION_UNCOMPRESSED, 4) X(SHA_DIGEST_LENGTH, 20)
#define MP_DECLARE_CONSTANT(name, value) enum { MP_##name = value };
MP_CONSTANTS(MP_DECLARE_CONSTANT)
#undef MP_DECLARE_CONSTANT

static inline const char *mp_openssl_load_error(int status) {
  switch (status) {
    case -1: return "OpenSSL 3 library unavailable";
    case -2: return "OpenSSL 3 required symbol unavailable";
    case -3: return "unsupported OpenSSL version (requires major 3)";
    default: return "";
  }
}

#endif
