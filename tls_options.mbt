///|
/// Protocol versions available in the native TLS adapter.
pub(all) enum TlsVersion {
  TLS12
  TLS13
}

///|
fn TlsVersion::native_value(self : TlsVersion) -> Int {
  match self {
    TLS12 => 0x0303
    TLS13 => 0x0304
  }
}

///|
/// PEM files used as a TLS client identity on a connection. Files are read
/// again at each new TLS handshake, including lookup redirects and reconnects.
pub(all) struct TlsIdentity {
  certificate_file : String
  private_key_file : String
}

///|
pub fn TlsIdentity::new(
  certificate_file : String,
  private_key_file : String,
) -> TlsIdentity raise PulsarError {
  guard certificate_file != "" && private_key_file != "" else {
    raise InvalidFrame(
      "TLS identity requires certificate and private key files",
    )
  }
  { certificate_file, private_key_file, }
}
