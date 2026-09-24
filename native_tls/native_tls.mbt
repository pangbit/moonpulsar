// Native OpenSSL transport for client certificate authentication and TLS policy.
// OpenSSL uses memory BIOs; the socket remains under the async event loop.

///|
pub suberror TlsError {
  TlsError(String)
}

///|
pub impl Show for TlsError with fn output(self, logger) {
  match self {
    TlsError(message) => logger.write_string(message)
  }
}

///|
pub extend TlsError with Show::{to_string, output}

///|
type Handle

///|
#borrow(host, ca_file, cert_file, key_file, cipher_list, tls13_ciphersuites)
extern "C" fn tls_new(
  host : Bytes,
  ca_file : Bytes,
  insecure : Bool,
  cert_file : Bytes,
  key_file : Bytes,
  min_version : Int,
  max_version : Int,
  cipher_list : Bytes,
  tls13_ciphersuites : Bytes,
) -> Handle = "moonpulsar_tls_new"

///|
#borrow(handle)
extern "C" fn tls_is_null(handle : Handle) -> Bool = "moonpulsar_tls_is_null"

///|
#borrow(handle)
extern "C" fn tls_close(handle : Handle) -> Unit = "moonpulsar_tls_close"

///|
#borrow(handle)
extern "C" fn tls_handshake(handle : Handle) -> Int = "moonpulsar_tls_handshake"

///|
#borrow(handle)
extern "C" fn tls_verified(handle : Handle) -> Bool = "moonpulsar_tls_verified"

///|
#borrow(handle)
extern "C" fn tls_last_error(handle : Handle) -> Int = "moonpulsar_tls_last_error"

///|
#borrow(handle, data)
extern "C" fn tls_feed(handle : Handle, data : Bytes, length : Int) -> Int = "moonpulsar_tls_feed"

///|
#borrow(handle)
extern "C" fn tls_drain(handle : Handle) -> Bytes = "moonpulsar_tls_drain"

///|
#borrow(handle, buf)
extern "C" fn tls_read(
  handle : Handle,
  buf : FixedArray[Byte],
  offset : Int,
  length : Int,
) -> Int = "moonpulsar_tls_read"

///|
#borrow(handle, buf)
extern "C" fn tls_write(
  handle : Handle,
  buf : Bytes,
  offset : Int,
  length : Int,
) -> Int = "moonpulsar_tls_write"

///|
extern "C" fn tls_error() -> Bytes = "moonpulsar_tls_error"

///|
#warnings("-alert_internal")
pub struct NativeTls {
  priv handle : Handle
  priv tcp : @socket.Tcp
  priv read_buffer : @io.ReaderBuffer
  priv mut closed : Bool
}

///|
fn native_tls_error() -> TlsError {
  TlsError(@utf8.decode_lossy(tls_error()))
}

///|
/// Handshake using server hostname verification, optional custom CA, client
/// certificate and key, protocol bounds, and OpenSSL cipher policy.
#warnings("-alert_internal")
pub async fn NativeTls::connect(
  tcp : @socket.Tcp,
  host : String,
  ca_file? : String = "",
  insecure? : Bool = false,
  cert_file? : String = "",
  key_file? : String = "",
  min_version? : Int = 0,
  max_version? : Int = 0,
  cipher_list? : String = "",
  tls13_ciphersuites? : String = "",
) -> NativeTls {
  let handle = tls_new(
    @utf8.encode(host),
    @utf8.encode(ca_file),
    insecure,
    @utf8.encode(cert_file),
    @utf8.encode(key_file),
    min_version,
    max_version,
    @utf8.encode(cipher_list),
    @utf8.encode(tls13_ciphersuites),
  )
  if tls_is_null(handle) {
    tcp.close()
    raise native_tls_error()
  }
  let self : NativeTls = {
    handle,
    tcp,
    read_buffer: @io.ReaderBuffer::new(),
    closed: false,
  }
  errdefer self.close()
  for ;; {
    let result = tls_handshake(handle)
    self.flush_output()
    if result == 1 {
      if !insecure && !tls_verified(handle) {
        raise TlsError("server certificate verification failed")
      }
      return self
    }
    if result < 0 {
      raise native_tls_error()
    }
    if tls_last_error(handle) == 2 {
      self.feed_input()
    }
  }
}

///|
async fn NativeTls::flush_output(self : NativeTls) -> Unit {
  let bytes = tls_drain(self.handle)
  if !bytes.is_empty() {
    self.tcp.write(bytes)
  }
}

///|
async fn NativeTls::feed_input(self : NativeTls) -> Unit {
  let buf = FixedArray::make(16384, b'\x00')
  let count = self.tcp.read(buf)
  if count == 0 {
    raise TlsError("TLS peer closed during handshake or read")
  }
  let bytes = buf.unsafe_reinterpret_as_bytes()[0:count].to_owned()
  if tls_feed(self.handle, bytes, count) != count {
    raise native_tls_error()
  }
}

///|
pub impl @io.Reader for NativeTls with fn _direct_read(
  self,
  buf,
  offset~,
  max_len~,
) {
  for ;; {
    let result = tls_read(self.handle, buf, offset, max_len)
    self.flush_output()
    if result > 0 {
      return result
    }
    if result == -2 {
      return 0
    }
    if result < 0 {
      raise native_tls_error()
    }
    if tls_last_error(self.handle) == 2 {
      self.feed_input()
    }
  }
}

///|
pub impl @io.Reader for NativeTls with fn _get_internal_buffer(self) {
  self.read_buffer
}

///|
pub impl @io.Writer for NativeTls with fn write_once(self, buf, offset~, len~) {
  for ;; {
    let result = tls_write(self.handle, buf, offset, len)
    self.flush_output()
    if result > 0 {
      return result
    }
    if result < 0 {
      raise native_tls_error()
    }
    if tls_last_error(self.handle) == 2 {
      self.feed_input()
    }
  }
}

///|
pub extend NativeTls with @io.Reader::{
  read,
  read_some,
  _direct_read,
  drop,
  read_all,
  read_until,
  read_exactly,
  _get_internal_buffer,
}

///|
pub extend NativeTls with @io.Writer::{write_once, write, write_reader}

///|
pub fn NativeTls::close(self : NativeTls) -> Unit {
  if !self.closed {
    self.closed = true
    tls_close(self.handle)
    self.tcp.close()
  }
}
