// Reconnect / retry helpers built on `moonbitlang/async`'s retry.

///|
/// Retry policy for broker operations such as reconnect and lookup.
pub(all) struct RetryPolicy {
  /// maximum number of attempts; negative means retry forever
  max_retries : Int
  /// backoff of the first retry, in milliseconds
  initial_backoff_ms : Int
  /// upper bound of the backoff, in milliseconds
  max_backoff_ms : Int
} derive(Debug)

///|
/// Default policy: unlimited retries, 100ms initial backoff doubling up to
/// 30 seconds.
pub fn RetryPolicy::default() -> RetryPolicy {
  { max_retries: -1, initial_backoff_ms: 100, max_backoff_ms: 30000 }
}

///|
/// Run `f` with the retry policy, retrying any failure.
pub async fn[T] with_retry(
  policy : RetryPolicy,
  f : async () -> T raise,
) -> T raise {
  let method = @async.RetryMethod::ExponentialDelay(
    initial=policy.initial_backoff_ms,
    factor=2.0,
    maximum=policy.max_backoff_ms,
  )
  if policy.max_retries < 0 {
    @async.retry(method, f)
  } else {
    @async.retry(method, max_retry=policy.max_retries, f)
  }
}
