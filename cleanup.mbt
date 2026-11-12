///|
// Failed construction must preserve the original error and still release local
// ownership if the broker never acknowledges CLOSE. Bound each leaf separately
// so one stalled partition cannot prevent cleanup of the remaining partitions.
async fn cleanup_after_failure(cleanup : async () -> Unit) -> Unit nocancel {
  @async.protect_from_cancel(() => {
    @async.with_timeout(1000, cleanup) catch {
      _ => ()
    }
  })
}

///|
async fn Producer::cleanup_after_failure(self : Producer) -> Unit nocancel {
  self.closed.val = true
  match self.inner {
    Single(producer) => cleanup_after_failure(() => producer.close())
    Partitioned(partitions, _, _) =>
      for producer in partitions {
        producer.cleanup_after_failure()
      }
  }
}

///|
async fn Consumer::cleanup_after_failure(self : Consumer) -> Unit nocancel {
  match self.inner {
    Single(consumer) => cleanup_after_failure(() => consumer.close())
    Partitioned(state) => {
      state.closed.val = true
      state.clear_buffered_messages()
      state.queue.close(error=ConsumerClosed, clear=true)
      for consumer in state.consumers {
        consumer.cleanup_after_failure()
      }
    }
  }
  if self.dead_letter is Some(dlq) {
    dlq.producer.cleanup_after_failure()
  }
  if self.retry_letter is Some(retry) {
    retry.producer.cleanup_after_failure()
  }
}

///|
async fn SingleReader::cleanup_after_failure(
  self : SingleReader,
) -> Unit nocancel {
  cleanup_after_failure(() => self.close())
}

///|
async fn Reader::cleanup_after_failure(self : Reader) -> Unit nocancel {
  match self.inner {
    One(reader) => reader.cleanup_after_failure()
    Many(state) => {
      state.closed.val = true
      state.clear_buffered_events()
      state.queue.close(error=ConsumerClosed, clear=true)
      for reader in state.readers {
        reader.cleanup_after_failure()
      }
    }
  }
}

///|
async fn TableView::cleanup_after_failure(self : TableView) -> Unit nocancel {
  self.closed.val = true
  self.reader.cleanup_after_failure()
}
