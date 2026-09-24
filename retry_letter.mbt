// Explicit delayed reconsumption through a retry-letter topic.

///|
priv struct RetryLetterState {
  topic : String
  producer : Producer
}

///|
fn Consumer::set_retry_letter(
  self : Consumer,
  topic : String,
  producer : Producer,
) -> Unit {
  self.retry_letter = Some({ topic, producer, })
}

///|
/// Publish a message for delayed reconsumption. Its source is acknowledged
/// only after the retry or dead-letter producer receives a broker receipt.
/// Once the retry count exceeds the dead-letter delivery limit, the message
/// goes directly to the dead-letter topic.
pub async fn Consumer::reconsume_later(
  self : Consumer,
  message : Message,
  delay_ms? : Int = 0,
  custom_properties? : Map[String, String] = {},
) -> Unit {
  guard delay_ms >= 0 else {
    raise InvalidFrame("retry delay must not be negative")
  }
  guard self.retry_letter is Some(retry) && self.dead_letter is Some(dlq) else {
    raise InvalidFrame("consumer has no retry-letter policy")
  }
  guard !self.is_closed() &&
    self.topics().contains(base_topic_name(message.topic())) else {
    raise InvalidFrame("message does not belong to this retry consumer")
  }
  let properties : Map[String, String] = Map([])
  for key, value in message.properties() {
    properties[key] = value
  }
  for key, value in custom_properties {
    properties[key] = value
  }
  let previous = match properties.get("RECONSUMETIMES") {
    Some(value) => @string.parse_int(value) catch { _ => 0 }
    None => 0
  }
  let attempts = if previous < 0 { 1 } else { previous + 1 }
  if properties.get("REAL_TOPIC") is None {
    properties["REAL_TOPIC"] = base_topic_name(message.topic())
    let id = message.id()
    properties["ORIGIN_MESSAGE_ID"] = "\{id.ledger_id}:\{id.entry_id}:\{id.batch_index}:\{id.partition}"
  }
  properties["RECONSUMETIMES"] = "\{attempts}"
  properties["DELAY_TIME"] = "\{delay_ms}"
  properties["RETRY_TOPIC"] = retry.topic
  let to_dead_letter = attempts.reinterpret_as_uint() >
    dlq.policy.max_deliveries
  let outgoing = ProducerMessage::new(
    message.payload(),
    key?=message.key(),
    ordering_key?=message.ordering_key(),
    properties~,
    event_time?=message.event_time(),
    deliver_after?=if to_dead_letter {
      None
    } else {
      Some(delay_ms.to_uint64())
    },
  )
  let producer = if to_dead_letter { dlq.producer } else { retry.producer }
  {
    errdefer (message.nack() catch { _ => () })
    ignore(producer.send(outgoing))
  }
  message.ack()
}
