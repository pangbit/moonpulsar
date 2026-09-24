///|
async test "Pulsar crypto wraps a fresh key and authenticates message payload" {
  let crypto = @moonpulsar.MessageCrypto::new()
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
  assert_true(tampered is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  let missing : Result[Bytes, Error] = Ok(
    @moonpulsar.MessageCrypto::new().decrypt(metadata, encrypted),
  ) catch {
    e => Err(e)
  }
  assert_true(missing is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
}

///|
async test "encrypted empty payload contains an authentication tag" {
  let crypto = @moonpulsar.MessageCrypto::new()
  crypto.load_public_key_file("test-key", "testdata/encryption/rsa_public.pem")
  crypto.load_private_key_file(
    "test-key", "testdata/encryption/rsa_private.pem",
  )
  let metadata = @proto.MessageMetadata::default()
  let encrypted = crypto.encrypt(metadata, b"", ["test-key"])
  assert_eq(encrypted.length(), 16)
  assert_eq(crypto.decrypt(metadata, encrypted), b"")
}

///|
test "RSA PEM parser rejects invalid and truncated keys" {
  let public : Result[@moonpulsar.EncryptionPublicKey, Error] = Ok(
    @moonpulsar.EncryptionPublicKey::from_pem(
      b"-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----",
    ),
  ) catch {
    e => Err(e)
  }
  assert_true(public is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  let private : Result[@moonpulsar.EncryptionPrivateKey, Error] = Ok(
    @moonpulsar.EncryptionPrivateKey::from_pem(b"not a PEM key"),
  ) catch {
    e => Err(e)
  }
  assert_true(private is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
}

///|
async test "message keys can rotate and multiple recipients can decrypt" {
  let sender = @moonpulsar.MessageCrypto::new()
  sender.load_public_key_file("first", "testdata/encryption/rsa_public.pem")
  sender.load_public_key_file(
    "second", "testdata/encryption/rsa_rotated_public.pem",
  )
  let metadata = @proto.MessageMetadata::default()
  let encrypted = sender.encrypt(metadata, b"two recipients", [
    "first", "second",
  ])
  assert_eq(metadata.encryption_keys.length(), 2)
  let receiver = @moonpulsar.MessageCrypto::new()
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
  assert_true(algorithm is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  sender.load_public_key_file(
    "first", "testdata/encryption/rsa_rotated_public.pem",
  )
  let rotated_metadata = @proto.MessageMetadata::default()
  let rotated = sender.encrypt(rotated_metadata, b"rotated", ["first"])
  let old_receiver = @moonpulsar.MessageCrypto::new()
  old_receiver.load_private_key_file(
    "first", "testdata/encryption/rsa_private.pem",
  )
  let old_key : Result[Bytes, Error] = Ok(
    old_receiver.decrypt(rotated_metadata, rotated),
  ) catch {
    e => Err(e)
  }
  assert_true(old_key is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  old_receiver.load_private_key_file(
    "first", "testdata/encryption/rsa_rotated_private.pem",
  )
  assert_eq(old_receiver.decrypt(rotated_metadata, rotated), b"rotated")
}

///|
async test "message key readers use current PEM and reject unavailable keys" {
  let first_public = @fs.read_file("testdata/encryption/rsa_public.pem").binary()
  let first_private = @fs.read_file("testdata/encryption/rsa_private.pem").binary()
  let second_public = @fs.read_file(
    "testdata/encryption/rsa_rotated_public.pem",
  ).binary()
  let second_private = @fs.read_file(
    "testdata/encryption/rsa_rotated_private.pem",
  ).binary()
  let current_public : Ref[Bytes] = { val: first_public, }
  let current_private : Ref[Bytes] = { val: first_private, }
  let sender = @moonpulsar.MessageCrypto::new(public_key_reader=fn(name) {
    if name == "dynamic" {
      Some(current_public.val)
    } else {
      None
    }
  })
  let receiver = @moonpulsar.MessageCrypto::new(private_key_reader=fn(name) {
    if name == "dynamic" {
      Some(current_private.val)
    } else {
      None
    }
  })
  let metadata = @proto.MessageMetadata::default()
  let encrypted = sender.encrypt(metadata, b"first key", ["dynamic"])
  assert_eq(receiver.decrypt(metadata, encrypted), b"first key")
  current_public.val = second_public
  let rotated_metadata = @proto.MessageMetadata::default()
  let rotated = sender.encrypt(rotated_metadata, b"second key", ["dynamic"])
  let old_key : Result[Bytes, Error] = Ok(
    receiver.decrypt(rotated_metadata, rotated),
  ) catch {
    e => Err(e)
  }
  assert_true(old_key is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  current_private.val = second_private
  assert_eq(receiver.decrypt(rotated_metadata, rotated), b"second key")
  let unavailable : Result[Bytes, Error] = Ok(
    sender.encrypt(@proto.MessageMetadata::default(), b"x", ["missing"]),
  ) catch {
    e => Err(e)
  }
  assert_true(unavailable is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
}
