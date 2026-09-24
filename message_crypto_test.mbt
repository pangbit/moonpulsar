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

///|
async test "Pulsar ECIES keys wrap, rotate, and reject tampering" {
  let sender = @moonpulsar.MessageCrypto::new()
  sender.load_public_key_file("ec", "testdata/encryption/ec_public.pem")
  let receiver = @moonpulsar.MessageCrypto::new()
  receiver.load_private_key_file("ec", "testdata/encryption/ec_private.pem")
  let metadata = @proto.MessageMetadata::default()
  let encrypted = sender.encrypt(metadata, b"ec payload", ["ec"])
  assert_true(metadata.encryption_keys[0].value.length() > 32)
  assert_eq(receiver.decrypt(metadata, encrypted), b"ec payload")
  let damaged_key = metadata.encryption_keys[0].value.to_array()
  damaged_key[damaged_key.length() - 1] = damaged_key[damaged_key.length() - 1] ^
    b'\x01'
  metadata.encryption_keys[0].value = Bytes::from_array(damaged_key)
  let rejected : Result[Bytes, Error] = Ok(
    receiver.decrypt(metadata, encrypted),
  ) catch {
    e => Err(e)
  }
  assert_true(rejected is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  sender.load_public_key_file("ec", "testdata/encryption/ec_rotated_public.pem")
  let rotated_metadata = @proto.MessageMetadata::default()
  let rotated = sender.encrypt(rotated_metadata, b"new EC key", ["ec"])
  let stale : Result[Bytes, Error] = Ok(
    receiver.decrypt(rotated_metadata, rotated),
  ) catch {
    e => Err(e)
  }
  assert_true(stale is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  receiver.load_private_key_file(
    "ec", "testdata/encryption/ec_rotated_private.pem",
  )
  assert_eq(receiver.decrypt(rotated_metadata, rotated), b"new EC key")
}

///|
async test "Pulsar ECIES supports P-256 and P-384 keys" {
  for curve in ["p256", "p384"] {
    let sender = @moonpulsar.MessageCrypto::new()
    sender.load_public_key_file(
      "ec",
      "testdata/encryption/ec_\{curve}_public.pem",
    )
    let receiver = @moonpulsar.MessageCrypto::new()
    receiver.load_private_key_file(
      "ec",
      "testdata/encryption/ec_\{curve}_private.pem",
    )
    let metadata = @proto.MessageMetadata::default()
    let encrypted = sender.encrypt(metadata, b"ec curve payload", ["ec"])
    assert_eq(receiver.decrypt(metadata, encrypted), b"ec curve payload")
    let wrong = @moonpulsar.MessageCrypto::new()
    wrong.load_private_key_file("ec", "testdata/encryption/ec_private.pem")
    let rejected : Result[Bytes, Error] = Ok(wrong.decrypt(metadata, encrypted)) catch {
      e => Err(e)
    }
    assert_true(rejected is Err(@moonpulsar.PulsarError::InvalidFrame(_)))
  }
}

///|
async test "Pulsar encryption can mix RSA and EC recipients" {
  let sender = @moonpulsar.MessageCrypto::new()
  sender.load_public_key_file("rsa", "testdata/encryption/rsa_public.pem")
  sender.load_public_key_file("ec", "testdata/encryption/ec_public.pem")
  let metadata = @proto.MessageMetadata::default()
  let encrypted = sender.encrypt(metadata, b"mixed recipients", ["rsa", "ec"])
  let rsa_receiver = @moonpulsar.MessageCrypto::new()
  rsa_receiver.load_private_key_file(
    "rsa", "testdata/encryption/rsa_private.pem",
  )
  let ec_receiver = @moonpulsar.MessageCrypto::new()
  ec_receiver.load_private_key_file("ec", "testdata/encryption/ec_private.pem")
  assert_eq(rsa_receiver.decrypt(metadata, encrypted), b"mixed recipients")
  assert_eq(ec_receiver.decrypt(metadata, encrypted), b"mixed recipients")
}
