// Producer hooks run on the logical producer before partition routing and
// when its broker receipt (or send failure) becomes available.

///|
pub struct ProducerInterceptor {
  priv before_send : ((ProducerMessage) -> ProducerMessage)?
  priv on_send_result : ((ProducerMessage, Result[SendReceipt, Error]) -> Unit)?
}

///|
pub fn ProducerInterceptor::new(
  before_send? : (ProducerMessage) -> ProducerMessage,
  on_send_result? : (ProducerMessage, Result[SendReceipt, Error]) -> Unit,
) -> ProducerInterceptor {
  { before_send, on_send_result, }
}
