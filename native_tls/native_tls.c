#include <moonbit.h>
#include "../native_support/openssl_abi.h"
#include <limits.h>
#include <string.h>
#include <dlfcn.h>

#define DECLARE_OPENSSL(ret, name, args) static mp_##name##_fn dyn_##name;
MP_TLS_FUNCTIONS(DECLARE_OPENSSL)
#undef DECLARE_OPENSSL

static int moonpulsar_tls_load(void) {
  static int status = 0;
  if (status != 0) return status;
  void *ssl = NULL;
  void *crypto = NULL;
#ifdef __APPLE__
  ssl = dlopen("/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib", RTLD_NOW);
  if (ssl == NULL) ssl = dlopen("/usr/local/opt/openssl@3/lib/libssl.3.dylib", RTLD_NOW);
  crypto = dlopen("/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib", RTLD_NOW);
  if (crypto == NULL) crypto = dlopen("/usr/local/opt/openssl@3/lib/libcrypto.3.dylib", RTLD_NOW);
#else
  ssl = dlopen("libssl.so.3", RTLD_NOW);
  if (ssl == NULL) ssl = dlopen("libssl.so", RTLD_NOW);
  crypto = dlopen("libcrypto.so.3", RTLD_NOW);
  if (crypto == NULL) crypto = dlopen("libcrypto.so", RTLD_NOW);
#endif
  if (ssl == NULL || crypto == NULL) { status = -1; goto failure; }
#define LOAD_OPENSSL(ret, name, args) do { \
  dyn_##name = (mp_##name##_fn)dlsym(ssl, #name); \
  if (dyn_##name == NULL) dyn_##name = (mp_##name##_fn)dlsym(crypto, #name); \
  if (dyn_##name == NULL) { status = -2; goto failure; } \
} while (0);
  MP_TLS_FUNCTIONS(LOAD_OPENSSL)
#undef LOAD_OPENSSL
  if ((dyn_OpenSSL_version_num() >> 28) != 3) { status = -3; goto failure; }
  return status = 1;
failure:
#define CLEAR_OPENSSL(ret, name, args) dyn_##name = NULL;
  MP_TLS_FUNCTIONS(CLEAR_OPENSSL)
#undef CLEAR_OPENSSL
  if (ssl != NULL) dlclose(ssl);
  if (crypto != NULL) dlclose(crypto);
  return status;
}

#define SSL_CTX_new dyn_SSL_CTX_new
#define SSL_CTX_free dyn_SSL_CTX_free
#define TLS_client_method dyn_TLS_client_method
#define SSL_CTX_ctrl dyn_SSL_CTX_ctrl
#define SSL_CTX_set_cipher_list dyn_SSL_CTX_set_cipher_list
#define SSL_CTX_set_ciphersuites dyn_SSL_CTX_set_ciphersuites
#define SSL_CTX_set_verify dyn_SSL_CTX_set_verify
#define SSL_CTX_load_verify_locations dyn_SSL_CTX_load_verify_locations
#define SSL_CTX_set_default_verify_paths dyn_SSL_CTX_set_default_verify_paths
#define SSL_CTX_use_certificate_chain_file dyn_SSL_CTX_use_certificate_chain_file
#define SSL_CTX_use_PrivateKey_file dyn_SSL_CTX_use_PrivateKey_file
#define SSL_CTX_check_private_key dyn_SSL_CTX_check_private_key
#define SSL_new dyn_SSL_new
#define SSL_free dyn_SSL_free
#define SSL_set1_host dyn_SSL_set1_host
#define SSL_ctrl dyn_SSL_ctrl
#define SSL_set_bio dyn_SSL_set_bio
#define SSL_set_connect_state dyn_SSL_set_connect_state
#define SSL_do_handshake dyn_SSL_do_handshake
#define SSL_get_verify_result dyn_SSL_get_verify_result
#define SSL_get_error dyn_SSL_get_error
#define SSL_read dyn_SSL_read
#define SSL_write dyn_SSL_write
#define BIO_new dyn_BIO_new
#define BIO_s_mem dyn_BIO_s_mem
#define BIO_free dyn_BIO_free
#define BIO_write dyn_BIO_write
#define BIO_read dyn_BIO_read
#define BIO_ctrl dyn_BIO_ctrl
#define ERR_clear_error dyn_ERR_clear_error
#define ERR_error_string_n dyn_ERR_error_string_n
#define ERR_get_error dyn_ERR_get_error

typedef struct {
  SSL_CTX *ctx;
  SSL *ssl;
  BIO *incoming;
  BIO *outgoing;
  int last_error;
} moonpulsar_tls_handle;

static void moonpulsar_tls_dispose(void *ptr) {
  moonpulsar_tls_handle *handle = ptr;
  if (handle->ssl != NULL) {
    SSL_free(handle->ssl);
    handle->ssl = NULL;
  }
  if (handle->ctx != NULL) {
    SSL_CTX_free(handle->ctx);
    handle->ctx = NULL;
  }
}

MOONBIT_FFI_EXPORT
moonpulsar_tls_handle *moonpulsar_tls_new(
    const char *host, const char *ca_file, int insecure,
    const char *cert_file, const char *key_file,
    int min_version, int max_version,
    const char *cipher_list, const char *tls13_ciphersuites) {
  if (moonpulsar_tls_load() != 1) return NULL;
  ERR_clear_error();
  moonpulsar_tls_handle *handle = moonbit_make_external_object(
      moonpulsar_tls_dispose, sizeof(moonpulsar_tls_handle));
  memset(handle, 0, sizeof(*handle));
  handle->ctx = SSL_CTX_new(TLS_client_method());
  if (handle->ctx == NULL) goto failure;
  if (min_version != 0 && SSL_CTX_ctrl(handle->ctx, MP_SSL_CTRL_SET_MIN_PROTO_VERSION, min_version, NULL) != 1) goto failure;
  if (max_version != 0 && SSL_CTX_ctrl(handle->ctx, MP_SSL_CTRL_SET_MAX_PROTO_VERSION, max_version, NULL) != 1) goto failure;
  if (cipher_list[0] != '\0' && SSL_CTX_set_cipher_list(handle->ctx, cipher_list) != 1) goto failure;
  if (tls13_ciphersuites[0] != '\0' && SSL_CTX_set_ciphersuites(handle->ctx, tls13_ciphersuites) != 1) goto failure;
  if (insecure) {
    SSL_CTX_set_verify(handle->ctx, MP_SSL_VERIFY_NONE, NULL);
  } else {
    SSL_CTX_set_verify(handle->ctx, MP_SSL_VERIFY_PEER, NULL);
    if (ca_file[0] != '\0') {
      if (SSL_CTX_load_verify_locations(handle->ctx, ca_file, NULL) != 1) goto failure;
    } else if (SSL_CTX_set_default_verify_paths(handle->ctx) != 1) goto failure;
  }
  if (cert_file[0] != '\0') {
    if (SSL_CTX_use_certificate_chain_file(handle->ctx, cert_file) != 1) goto failure;
    if (SSL_CTX_use_PrivateKey_file(handle->ctx, key_file, MP_SSL_FILETYPE_PEM) != 1) goto failure;
    if (SSL_CTX_check_private_key(handle->ctx) != 1) goto failure;
  }
  handle->ssl = SSL_new(handle->ctx);
  if (handle->ssl == NULL) goto failure;
  if (host[0] != '\0') {
    if (!insecure && SSL_set1_host(handle->ssl, host) != 1) goto failure;
    if (SSL_ctrl(handle->ssl, MP_SSL_CTRL_SET_TLSEXT_HOSTNAME, MP_TLSEXT_NAMETYPE_host_name, (void *)host) != 1) goto failure;
  }
  handle->incoming = BIO_new(BIO_s_mem());
  handle->outgoing = BIO_new(BIO_s_mem());
  if (handle->incoming == NULL || handle->outgoing == NULL) goto failure;
  BIO_ctrl(handle->incoming, MP_BIO_C_SET_BUF_MEM_EOF_RETURN, -1, NULL);
  SSL_set_bio(handle->ssl, handle->incoming, handle->outgoing);
  SSL_set_connect_state(handle->ssl);
  return handle;
failure:
  if (handle->incoming != NULL) BIO_free(handle->incoming);
  if (handle->outgoing != NULL) BIO_free(handle->outgoing);
  moonpulsar_tls_dispose(handle);
  return NULL;
}

MOONBIT_FFI_EXPORT
void moonpulsar_tls_close(moonpulsar_tls_handle *handle) {
  if (handle != NULL) moonpulsar_tls_dispose(handle);
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_is_null(moonpulsar_tls_handle *handle) {
  return handle == NULL;
}

static int moonpulsar_tls_status(moonpulsar_tls_handle *handle, int result) {
  int err = SSL_get_error(handle->ssl, result);
  handle->last_error = err;
  if (err == MP_SSL_ERROR_WANT_READ || err == MP_SSL_ERROR_WANT_WRITE) return 0;
  if (err == MP_SSL_ERROR_ZERO_RETURN) return -2;
  return -1;
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_handshake(moonpulsar_tls_handle *handle) {
  ERR_clear_error();
  int result = SSL_do_handshake(handle->ssl);
  if (result == 1) return 1;
  return moonpulsar_tls_status(handle, result);
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_verified(moonpulsar_tls_handle *handle) {
  return SSL_get_verify_result(handle->ssl) == MP_X509_V_OK;
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_last_error(moonpulsar_tls_handle *handle) {
  return handle->last_error;
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_feed(moonpulsar_tls_handle *handle, const unsigned char *data, int len) {
  return BIO_write(handle->incoming, data, len);
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t moonpulsar_tls_drain(moonpulsar_tls_handle *handle) {
  size_t pending = (size_t)BIO_ctrl(handle->outgoing, MP_BIO_CTRL_PENDING, 0, NULL);
  if (pending > INT_MAX) pending = INT_MAX;
  moonbit_bytes_t result = moonbit_make_bytes((int)pending, 0);
  if (pending != 0) BIO_read(handle->outgoing, result, (int)pending);
  return result;
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_read(moonpulsar_tls_handle *handle, unsigned char *buf,
                        int offset, int len) {
  ERR_clear_error();
  int result = SSL_read(handle->ssl, buf + offset, len);
  if (result > 0) return result;
  return moonpulsar_tls_status(handle, result);
}

MOONBIT_FFI_EXPORT
int moonpulsar_tls_write(moonpulsar_tls_handle *handle, const unsigned char *buf,
                         int offset, int len) {
  ERR_clear_error();
  int result = SSL_write(handle->ssl, buf + offset, len);
  if (result > 0) return result;
  return moonpulsar_tls_status(handle, result);
}

MOONBIT_FFI_EXPORT
moonbit_bytes_t moonpulsar_tls_error(void) {
  if (moonpulsar_tls_load() != 1) {
    const char *message = mp_openssl_load_error(moonpulsar_tls_load());
    moonbit_bytes_t unavailable = moonbit_make_bytes((int)strlen(message), 0);
    memcpy(unavailable, message, strlen(message));
    return unavailable;
  }
  unsigned long code = ERR_get_error();
  /* A failed operation can enqueue multiple errors. Do not leak the rest to
   * another TLS connection running on the same event-loop thread. */
  ERR_clear_error();
  char buffer[256];
  ERR_error_string_n(code, buffer, sizeof(buffer));
  size_t len = strlen(buffer);
  moonbit_bytes_t result = moonbit_make_bytes((int)len, 0);
  memcpy(result, buffer, len);
  return result;
}
