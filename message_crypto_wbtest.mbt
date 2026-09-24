///|
async test "Pulsar crypto wraps a fresh key and authenticates message payload" {
  let crypto = MessageCrypto::new()
  crypto.load_public_key_file("test-key", "testdata/encryption/rsa_public.pem")
  crypto.load_private_key_file(
    "test-key", "testdata/encryption/rsa_private.pem",
  )
  let metadata = @proto.MessageMetadata::default()
  let encrypted = crypto.encrypt(metadata, b"encrypted moonpulsar", ["test-key"])
  assert_true(encrypted != b"encrypted moonpulsar")
  assert_eq(metadata.encryption_keys.length(), 1)
  assert_eq(metadata.encryption_keys[0].key, "test-key")
  assert_eq(metadata.encryption_param.unwrap().length(), 12)
  assert_eq(crypto.decrypt(metadata, encrypted), b"encrypted moonpulsar")
  let damaged = encrypted.to_array()
  damaged[0] = damaged[0] ^ b'\x01'
  let tampered : Result[Bytes, Error] = Ok(
    crypto.decrypt(metadata, Bytes::from_array(damaged)),
  ) catch {
    e => Err(e)
  }
  assert_true(tampered is Err(InvalidFrame(_)))
  let missing : Result[Bytes, Error] = Ok(
    MessageCrypto::new().decrypt(metadata, encrypted),
  ) catch {
    e => Err(e)
  }
  assert_true(missing is Err(InvalidFrame(_)))
}

///|
test "RSA PEM parser rejects invalid and truncated keys" {
  let public : Result[EncryptionPublicKey, Error] = Ok(
    EncryptionPublicKey::from_pem(
      b"-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(public is Err(InvalidFrame(_)))
  let private : Result[EncryptionPrivateKey, Error] = Ok(
    EncryptionPrivateKey::from_pem(b"not a PEM key"),
  ) catch {
    e => Err(e)
  }
  assert_true(private is Err(InvalidFrame(_)))
}

///|
async test "message keys can rotate and multiple recipients can decrypt" {
  let sender = MessageCrypto::new()
  sender.load_public_key_file("first", "testdata/encryption/rsa_public.pem")
  sender.load_public_key_file(
    "second", "testdata/encryption/rsa_rotated_public.pem",
  )
  let metadata = @proto.MessageMetadata::default()
  let encrypted = sender.encrypt(metadata, b"two recipients", [
    "first", "second",
  ])
  assert_eq(metadata.encryption_keys.length(), 2)
  let receiver = MessageCrypto::new()
  receiver.load_private_key_file(
    "second", "testdata/encryption/rsa_rotated_private.pem",
  )
  assert_eq(receiver.decrypt(metadata, encrypted), b"two recipients")
  metadata.encryption_algo = Some("unknown")
  let algorithm : Result[Bytes, Error] = Ok(
    receiver.decrypt(metadata, encrypted),
  ) catch {
    e => Err(e)
  }
  assert_true(algorithm is Err(InvalidFrame(_)))
  sender.load_public_key_file(
    "first", "testdata/encryption/rsa_rotated_public.pem",
  )
  let rotated_metadata = @proto.MessageMetadata::default()
  let rotated = sender.encrypt(rotated_metadata, b"rotated", ["first"])
  let old_receiver = MessageCrypto::new()
  old_receiver.load_private_key_file(
    "first", "testdata/encryption/rsa_private.pem",
  )
  let old_key : Result[Bytes, Error] = Ok(
    old_receiver.decrypt(rotated_metadata, rotated),
  ) catch {
    e => Err(e)
  }
  assert_true(old_key is Err(InvalidFrame(_)))
  old_receiver.load_private_key_file(
    "first", "testdata/encryption/rsa_rotated_private.pem",
  )
  assert_eq(old_receiver.decrypt(rotated_metadata, rotated), b"rotated")
}
