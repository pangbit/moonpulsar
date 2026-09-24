// Pulsar message encryption: RSA-OAEP-SHA1 wraps a fresh 256-bit data key;
// AES-256-GCM encrypts the compressed message payload.

///|
pub struct EncryptionPublicKey {
  priv modulus : Bytes
  priv exponent : Bytes
}

///|
pub struct EncryptionPrivateKey {
  priv modulus : Bytes
  priv private_exponent : Bytes
}

///|
fn der_read(
  data : Bytes,
  offset : Ref[Int],
  tag : Int,
) -> Bytes raise PulsarError {
  guard offset.val < data.length() && data[offset.val].to_int() == tag else {
    raise InvalidFrame("invalid RSA key DER tag")
  }
  offset.val += 1
  guard offset.val < data.length() else {
    raise InvalidFrame("truncated RSA key DER length")
  }
  let first = data[offset.val].to_int()
  offset.val += 1
  let mut length = first
  if first >= 128 {
    let count = first & 0x7f
    guard count > 0 && count <= 4 && offset.val + count <= data.length() else {
      raise InvalidFrame("invalid RSA key DER length")
    }
    length = 0
    for _ in 0..<count {
      length = (length << 8) | data[offset.val].to_int()
      offset.val += 1
    }
    guard length >= 128 else {
      raise InvalidFrame("noncanonical RSA key DER length")
    }
  }
  guard length >= 0 && length <= data.length() - offset.val else {
    raise InvalidFrame("truncated RSA key DER value")
  }
  let result = data[offset.val:offset.val + length].to_owned()
  offset.val += length
  result
}

///|
fn der_sequence(data : Bytes) -> Bytes raise PulsarError {
  let offset = Ref::new(0)
  let sequence = der_read(data, offset, 0x30)
  guard offset.val == data.length() else {
    raise InvalidFrame("trailing RSA key DER data")
  }
  sequence
}

///|
fn der_integer(data : Bytes, offset : Ref[Int]) -> Bytes raise PulsarError {
  let value = der_read(data, offset, 0x02)
  guard value.length() > 0 && value[0].to_int() < 128 else {
    raise InvalidFrame("invalid RSA key integer")
  }
  if value.length() > 1 && value[0] == b'\x00' {
    guard value[1].to_int() >= 128 else {
      raise InvalidFrame("noncanonical RSA key integer")
    }
    value[1:].to_owned()
  } else {
    value
  }
}

///|
fn der_rsa_algorithm(data : Bytes) -> Unit raise PulsarError {
  let offset = Ref::new(0)
  let oid = der_read(data, offset, 0x06)
  guard oid == b"\x2a\x86\x48\x86\xf7\x0d\x01\x01\x01" else {
    raise InvalidFrame("key is not RSA")
  }
  if offset.val < data.length() {
    let null = der_read(data, offset, 0x05)
    guard null.length() == 0 else {
      raise InvalidFrame("invalid RSA algorithm parameters")
    }
  }
  guard offset.val == data.length() else {
    raise InvalidFrame("trailing RSA algorithm parameters")
  }
}

///|
fn pem_body(pem : Bytes, label : String) -> Bytes raise PulsarError {
  let text = @utf8.decode(pem) catch {
    _ => raise InvalidFrame("invalid UTF-8 RSA PEM")
  }
  let begin = "-----BEGIN \{label}-----"
  let end = "-----END \{label}-----"
  guard text.split_once(begin) is Some((_, rest)) &&
    rest.split_once(end) is Some((body, _)) else {
    raise InvalidFrame("RSA PEM block is missing")
  }
  @base64.decode(body, ignore_whitespace=true) catch {
    _ => raise InvalidFrame("invalid RSA PEM base64")
  }
}

///|
fn parse_pkcs1_public(der : Bytes) -> EncryptionPublicKey raise PulsarError {
  let body = der_sequence(der)
  let offset = Ref::new(0)
  let modulus = der_integer(body, offset)
  let exponent = der_integer(body, offset)
  guard offset.val == body.length() &&
    modulus.length() >= 128 &&
    exponent.length() > 0 else {
    raise InvalidFrame("invalid RSA public key")
  }
  { modulus, exponent, }
}

///|
/// Parse an RSA public key in SubjectPublicKeyInfo or PKCS#1 PEM format.
pub fn EncryptionPublicKey::from_pem(
  pem : Bytes,
) -> EncryptionPublicKey raise PulsarError {
  let text = @utf8.decode(pem) catch {
    _ => raise InvalidFrame("invalid UTF-8 RSA PEM")
  }
  if text.contains("BEGIN RSA PUBLIC KEY") {
    return parse_pkcs1_public(pem_body(pem, "RSA PUBLIC KEY"))
  }
  let body = der_sequence(pem_body(pem, "PUBLIC KEY"))
  let offset = Ref::new(0)
  der_rsa_algorithm(der_read(body, offset, 0x30))
  let bits = der_read(body, offset, 0x03)
  guard offset.val == body.length() && bits.length() > 1 && bits[0] == b'\x00' else {
    raise InvalidFrame("invalid RSA public key bit string")
  }
  parse_pkcs1_public(bits[1:].to_owned())
}

///|
fn parse_pkcs1_private(der : Bytes) -> EncryptionPrivateKey raise PulsarError {
  let body = der_sequence(der)
  let offset = Ref::new(0)
  let version = der_integer(body, offset)
  guard version == b"\x00" else {
    raise InvalidFrame("unsupported RSA private key version")
  }
  let modulus = der_integer(body, offset)
  ignore(der_integer(body, offset))
  let private_exponent = der_integer(body, offset)
  for _ in 0..<5 {
    ignore(der_integer(body, offset))
  }
  guard offset.val == body.length() &&
    modulus.length() >= 128 &&
    private_exponent.length() > 0 else {
    raise InvalidFrame("invalid RSA private key")
  }
  { modulus, private_exponent, }
}

///|
/// Parse an unencrypted RSA private key in PKCS#1 or PKCS#8 PEM format.
pub fn EncryptionPrivateKey::from_pem(
  pem : Bytes,
) -> EncryptionPrivateKey raise PulsarError {
  let text = @utf8.decode(pem) catch {
    _ => raise InvalidFrame("invalid UTF-8 RSA PEM")
  }
  if text.contains("BEGIN RSA PRIVATE KEY") {
    return parse_pkcs1_private(pem_body(pem, "RSA PRIVATE KEY"))
  }
  let body = der_sequence(pem_body(pem, "PRIVATE KEY"))
  let offset = Ref::new(0)
  let version = der_integer(body, offset)
  guard version == b"\x00" else {
    raise InvalidFrame("unsupported PKCS#8 key version")
  }
  der_rsa_algorithm(der_read(body, offset, 0x30))
  let private_der = der_read(body, offset, 0x04)
  guard offset.val == body.length() else {
    raise InvalidFrame("trailing PKCS#8 key data")
  }
  parse_pkcs1_private(private_der)
}

///|
pub struct MessageCrypto {
  priv public_keys : Map[String, EncryptionPublicKey]
  priv private_keys : Map[String, EncryptionPrivateKey]
}

///|
pub fn MessageCrypto::new() -> MessageCrypto {
  { public_keys: Map([]), private_keys: Map([]), }
}

///|
/// Replace a producer key. Subsequent sends use the replacement immediately.
pub fn MessageCrypto::add_public_key(
  self : MessageCrypto,
  name : String,
  pem : Bytes,
) -> Unit raise PulsarError {
  guard name != "" else { raise InvalidFrame("encryption key name is empty") }
  self.public_keys[name] = EncryptionPublicKey::from_pem(pem)
}

///|
/// Replace a consumer key. Already received ciphertext retains its key name.
pub fn MessageCrypto::add_private_key(
  self : MessageCrypto,
  name : String,
  pem : Bytes,
) -> Unit raise PulsarError {
  guard name != "" else { raise InvalidFrame("encryption key name is empty") }
  self.private_keys[name] = EncryptionPrivateKey::from_pem(pem)
}

///|
/// Load a PEM public key from a file; call again after rotating the file.
pub async fn MessageCrypto::load_public_key_file(
  self : MessageCrypto,
  name : String,
  path : String,
) -> Unit raise PulsarError {
  let pem = @fs.read_file(path).binary() catch {
    _ => raise InvalidFrame("could not read encryption public key")
  }
  self.add_public_key(name, pem)
}

///|
/// Load a PEM private key from a file; call again after rotating the file.
pub async fn MessageCrypto::load_private_key_file(
  self : MessageCrypto,
  name : String,
  path : String,
) -> Unit raise PulsarError {
  let pem = @fs.read_file(path).binary() catch {
    _ => raise InvalidFrame("could not read encryption private key")
  }
  self.add_private_key(name, pem)
}

///|
async fn encryption_random(length : Int) -> Bytes raise PulsarError {
  let file = @fs.open("/dev/urandom", mode=ReadOnly) catch {
    _ => raise InvalidFrame("could not open system random source")
  }
  defer file.close()
  file.read_exactly(length) catch {
    _ => raise InvalidFrame("could not read system randomness")
  }
}

///|
/// Encrypt a wire payload and fill Pulsar's encryption metadata. The caller
/// must compress first and send the returned bytes without further encoding.
pub async fn MessageCrypto::encrypt(
  self : MessageCrypto,
  metadata : @proto.MessageMetadata,
  payload : Bytes,
  key_names : Array[String],
) -> Bytes raise PulsarError {
  guard !key_names.is_empty() else {
    raise InvalidFrame("encryption requires a recipient key")
  }
  let recipients = []
  for name in key_names {
    match self.public_keys.get(name) {
      Some(key) => recipients.push((name, key))
      None => raise InvalidFrame("encryption public key is unavailable")
    }
  }
  let data_key = encryption_random(32)
  let nonce = encryption_random(12)
  let keys = []
  for (name, key) in recipients {
    let seed = encryption_random(20)
    let wrapped = match
      @crypto.rsa_oaep_encrypt_with_or(
        data_key,
        key.modulus,
        key.exponent,
        seed,
        b"",
        @crypto.rsa_hash_sha1,
      ) {
      Ok(value) => value
      Err(_) => raise InvalidFrame("could not wrap encryption data key")
    }
    keys.push(@proto.EncryptionKeys::EncryptionKeys(name, wrapped, []))
  }
  let result = @crypto.aes_gcm_encrypt(payload, data_key, nonce, b"")
  let output = @buffer.Buffer()
  output.write_bytes(result.ct)
  output.write_bytes(result.tag)
  metadata.encryption_keys = keys
  metadata.encryption_param = Some(nonce)
  metadata.encryption_algo = Some("AES/GCM/NoPadding")
  output.to_bytes()
}

///|
/// Decrypt an encrypted Pulsar payload. Missing keys and failed authentication
/// return an error; callers must not deliver unauthenticated plaintext.
pub fn MessageCrypto::decrypt(
  self : MessageCrypto,
  metadata : @proto.MessageMetadata,
  payload : Bytes,
) -> Bytes raise PulsarError {
  guard metadata.encryption_param is Some(nonce) &&
    nonce.length() == 12 &&
    payload.length() >= 16 &&
    !metadata.encryption_keys.is_empty() else {
    raise InvalidFrame("invalid encrypted message metadata")
  }
  if metadata.encryption_algo is Some(algorithm) &&
    algorithm != "AES/GCM/NoPadding" {
    raise InvalidFrame("unsupported message encryption algorithm")
  }
  for wrapped in metadata.encryption_keys {
    if self.private_keys.get(wrapped.key) is Some(key) {
      match
        @crypto.rsa_oaep_decrypt_with_or(
          wrapped.value,
          key.modulus,
          key.private_exponent,
          b"",
          @crypto.rsa_hash_sha1,
        ) {
        Ok(data_key) if data_key.length() == 32 => {
          let end = payload.length() - 16
          match
            @crypto.aes_gcm_decrypt(
              payload[:end].to_owned(),
              data_key,
              nonce,
              b"",
              payload[end:].to_owned(),
            ) {
            Ok(plain) => return plain
            Err(_) => ()
          }
        }
        _ => ()
      }
    }
  }
  raise InvalidFrame("could not decrypt message")
}
