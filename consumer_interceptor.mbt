// Consumer hooks run when an application receives a message and when it
// acknowledges or negatively acknowledges that message.

///|
pub struct ConsumerInterceptor {
  priv before_consume : ((Message) -> Unit)?
  priv on_receive_error : ((Error) -> Unit)?
  priv on_ack : ((MessageId, Result[Unit, Error]) -> Unit)?
  priv on_nack : ((MessageId, Result[Unit, Error]) -> Unit)?
}

///|
pub fn ConsumerInterceptor::new(
  before_consume? : (Message) -> Unit,
  on_receive_error? : (Error) -> Unit,
  on_ack? : (MessageId, Result[Unit, Error]) -> Unit,
  on_nack? : (MessageId, Result[Unit, Error]) -> Unit,
) -> ConsumerInterceptor {
  { before_consume, on_receive_error, on_ack, on_nack, }
}

///|
fn ConsumerInterceptor::receive_failed(
  self : ConsumerInterceptor,
  error : Error,
) -> Unit {
  if self.on_receive_error is Some(callback) {
    callback(error)
  }
}

///|
fn ConsumerInterceptor::delivered(
  self : ConsumerInterceptor,
  message : Message,
) -> Unit {
  if self.before_consume is Some(callback) {
    callback(message)
  }
}

///|
fn ConsumerInterceptor::acked(
  self : ConsumerInterceptor,
  id : MessageId,
  result : Result[Unit, Error],
) -> Unit {
  if self.on_ack is Some(callback) {
    callback(id, result)
  }
}

///|
fn ConsumerInterceptor::nacked(
  self : ConsumerInterceptor,
  id : MessageId,
  result : Result[Unit, Error],
) -> Unit {
  if self.on_nack is Some(callback) {
    callback(id, result)
  }
}
