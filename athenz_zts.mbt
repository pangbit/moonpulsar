// Athenz ZTS role-token provider using a signed RSA service NToken.

///|
/// Settings for exchanging a service identity for an Athenz role token.
/// `zts_url` is the ZTS server base URL, without `/zts/v1`.
pub(all) struct AthenzZtsOptions {
  zts_url : String
  provider_domain : String
  tenant_domain : String
  tenant_service : String
  private_key_file : String
  certificate_file : String?
  ca_pem_file : String?
  key_id : String
  role : String
  principal_header : String
  min_expiry_secs : Int
  max_expiry_secs : Int
  refresh_before_ms : Int
  max_retries : Int
  retry_delay_ms : Int
}

///|
pub fn AthenzZtsOptions::new(
  zts_url : String,
  provider_domain : String,
  tenant_domain : String,
  tenant_service : String,
  private_key_file : String,
  certificate_file? : String,
  ca_pem_file? : String,
  key_id? : String = "0",
  role? : String = "",
  principal_header? : String = "Athenz-Principal-Auth",
  min_expiry_secs? : Int = 7200,
  max_expiry_secs? : Int = 86400,
  refresh_before_ms? : Int = 600000,
  max_retries? : Int = 2,
  retry_delay_ms? : Int = 100,
) -> AthenzZtsOptions raise PulsarError {
  guard zts_url != "" &&
    provider_domain != "" &&
    (
      certificate_file is Some(_) ||
      (tenant_domain != "" && tenant_service != "")
    ) &&
    private_key_file != "" &&
    key_id != "" &&
    principal_header != "" else {
    raise InvalidFrame("Athenz ZTS identity and URL are required")
  }
  guard min_expiry_secs > 0 &&
    max_expiry_secs >= min_expiry_secs &&
    refresh_before_ms >= 0 &&
    max_retries >= 0 &&
    retry_delay_ms >= 0 else {
    raise InvalidFrame("invalid Athenz ZTS expiry or retry settings")
  }
  if certificate_file is Some(cert) {
    guard cert != "" && zts_url.has_prefix("https://") else {
      raise InvalidFrame(
        "Athenz certificate identity requires an HTTPS ZTS URL",
      )
    }
  }
  if ca_pem_file is Some("") {
    raise InvalidFrame("Athenz ZTS CA file must not be empty")
  }
  if ca_pem_file is Some(_) && certificate_file is None {
    raise InvalidFrame("Athenz ZTS CA file requires certificate identity")
  }
  {
    zts_url: zts_url.trim_end(chars="/").to_owned(),
    provider_domain,
    tenant_domain,
    tenant_service,
    private_key_file,
    certificate_file,
    ca_pem_file,
    key_id,
    role,
    principal_header,
    min_expiry_secs,
    max_expiry_secs,
    refresh_before_ms,
    max_retries,
    retry_delay_ms,
  }
}

///|
fn athenz_ybase64(bytes : Bytes) -> String {
  @crypto.base64_encode(bytes)
  .replace_all(old="+", new=".")
  .replace_all(old="/", new="_")
  .replace_all(old="=", new="-")
}

///|
fn athenz_hex(bytes : Bytes) -> String {
  let digits = b"0123456789abcdef"
  let buffer = @buffer.Buffer()
  for byte in bytes {
    let value = byte.to_int()
    buffer.write_byte(digits[value >> 4])
    buffer.write_byte(digits[value & 15])
  }
  @utf8.decode(buffer.to_bytes()) catch {
    _ => ""
  }
}

///|
async fn athenz_service_token(options : AthenzZtsOptions) -> String {
  let pem = @fs.read_file(options.private_key_file).binary() catch {
    _ => raise AuthenticationFailed("could not read Athenz private key")
  }
  let key = EncryptionPrivateKey::from_pem(pem) catch {
    _ => raise AuthenticationFailed("invalid Athenz RSA private key")
  }
  guard key.ec_pem is None else {
    raise AuthenticationFailed(
      "Athenz service NToken requires an RSA private key",
    )
  }
  let now = @async.now() / 1000L
  let salt = athenz_hex(encryption_random(8))
  let unsigned = "v=S1;d=\{options.tenant_domain};n=\{options.tenant_service};k=\{options.key_id};a=\{salt};t=\{now};e=\{now + 3600L}"
  let signed = @crypto.rsa_pkcs1_v15_sign_with(
    @utf8.encode(unsigned),
    key.modulus,
    key.private_exponent,
    @crypto.rsa_hash_sha256,
  )
  unsigned + ";s=" + athenz_ybase64(signed)
}

///|
priv struct AthenzCachedRoleToken {
  token : String
  expiry_ms : Int64
}

///|
async fn athenz_request_role_token(
  options : AthenzZtsOptions,
) -> AthenzCachedRoleToken {
  let principal : String? = if options.certificate_file is Some(_) {
    None
  } else {
    Some(athenz_service_token(options))
  }
  let mut url = options.zts_url +
    "/zts/v1/domain/" +
    oauth2_form_component(options.provider_domain) +
    "/token?minExpiryTime=\{options.min_expiry_secs}&maxExpiryTime=\{options.max_expiry_secs}"
  if options.role != "" {
    url += "&role=" + oauth2_form_component(options.role)
  }
  let (status, body) = match options.certificate_file {
    Some(cert) =>
      athenz_certificate_get(
        url,
        cert,
        options.private_key_file,
        options.ca_pem_file,
      )
    None => {
      let headers = Map([])
      headers[@http.CaseInsensitiveString(options.principal_header)] = principal.unwrap()
      let (response, body) = @http.request(
        url,
        @http.RequestMethod::Get,
        headers,
        b"",
      )
      (response.code, body.binary())
    }
  }
  guard status >= 200 && status < 300 else {
    raise ServerError(
      code=status,
      message="Athenz ZTS rejected role-token request",
    )
  }
  let json = @json.parse(@utf8.decode_lossy(body)) catch {
    _ => raise AuthenticationFailed("Athenz ZTS response is not JSON")
  }
  guard json is Object(fields) &&
    fields.get("token") is Some(String(token)) &&
    token != "" &&
    fields.get("expiryTime") is Some(Number(expiry, repr=_)) &&
    !expiry.is_nan() &&
    !expiry.is_inf() else {
    raise AuthenticationFailed("Athenz ZTS response has no valid role token")
  }
  let expiry_ms = expiry.to_int64() * 1000L
  guard expiry_ms > @async.now() else {
    raise AuthenticationFailed("Athenz ZTS role token already expired")
  }
  { token, expiry_ms, }
}

///|
/// Sign a service NToken or present a client certificate to ZTS, then cache
/// the role token until shortly before expiry. Failed refreshes retry and may
/// use a still valid cached token. Pass the result as
/// `ClientOptions::new(auth_provider=...)`.
pub fn athenz_zts_provider(options : AthenzZtsOptions) -> async () -> AuthData {
  let cached : Ref[AthenzCachedRoleToken?] = { val: None, }
  async fn() {
    let now = @async.now()
    if cached.val is Some(current) &&
      now + Int64::from_int(options.refresh_before_ms) < current.expiry_ms {
      return { method_name: "athenz", data: @utf8.encode(current.token), }
    }
    let mut last_error : Error = AuthenticationFailed("Athenz ZTS unavailable")
    for attempt in 0..<=options.max_retries {
      let result : Result[AthenzCachedRoleToken, Error] = Ok(
        athenz_request_role_token(options),
      ) catch {
        error => Err(error)
      }
      match result {
        Ok(token) => {
          cached.val = Some(token)
          return { method_name: "athenz", data: @utf8.encode(token.token), }
        }
        Err(error) => {
          last_error = error
          if error is ServerError(code~, ..) && code < 500 {
            break
          }
          if attempt < options.max_retries {
            @async.sleep(options.retry_delay_ms)
          }
        }
      }
    }
    if cached.val is Some(current) && now < current.expiry_ms {
      { method_name: "athenz", data: @utf8.encode(current.token), }
    } else {
      raise last_error
    }
  }
}
