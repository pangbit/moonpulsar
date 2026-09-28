// Bouncy Castle ECIES key wrapping for Pulsar message encryption.

///|
extern "C" fn load_error() -> Bytes = "moonpulsar_ecies_load_error"

///|
/// Load the optional OpenSSL 3 runtime and report a dependency failure.
pub fn availability_error() -> String? {
  let message = load_error()
  if message.is_empty() {
    None
  } else {
    Some(@utf8.decode_lossy(message))
  }
}

///|
#borrow(pem)
extern "C" fn public_valid(pem : Bytes, length : Int) -> Bool = "moonpulsar_ecies_public_valid"

///|
#borrow(pem)
extern "C" fn private_valid(pem : Bytes, length : Int) -> Bool = "moonpulsar_ecies_private_valid"

///|
#borrow(pem, key)
extern "C" fn wrap(
  pem : Bytes,
  length : Int,
  key : Bytes,
  key_length : Int,
) -> Bytes = "moonpulsar_ecies_wrap"

///|
#borrow(pem, wrapped)
extern "C" fn unwrap(
  pem : Bytes,
  length : Int,
  wrapped : Bytes,
  wrapped_length : Int,
) -> Bytes = "moonpulsar_ecies_unwrap"

///|
pub fn is_public_key(pem : Bytes) -> Bool {
  public_valid(pem, pem.length())
}

///|
pub fn is_private_key(pem : Bytes) -> Bool {
  private_valid(pem, pem.length())
}

///|
pub fn encrypt_key(pem : Bytes, key : Bytes) -> Bytes? {
  let result = wrap(pem, pem.length(), key, key.length())
  if result.is_empty() {
    None
  } else {
    Some(result)
  }
}

///|
pub fn decrypt_key(pem : Bytes, wrapped : Bytes) -> Bytes? {
  let result = unwrap(pem, pem.length(), wrapped, wrapped.length())
  if result.length() == 32 {
    Some(result)
  } else {
    None
  }
}
