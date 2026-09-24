// Preserve the methods that trait implementations currently expose on public types.

///|
pub extend TopicName with Eq::{not_equal, equal}

///|
pub extend TopicName with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend TokenAuth with Authentication::{method_name, auth_data}

///|
pub extend BasicAuth with Authentication::{method_name, auth_data}

///|
pub extend CompressionCodec with Eq::{not_equal, equal}

///|
pub extend CompressionCodec with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend AuthData with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SubscriptionType with Eq::{not_equal, equal}

///|
pub extend SubscriptionType with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SubscriptionInitialPosition with Eq::{not_equal, equal}

///|
pub extend SubscriptionInitialPosition with @moonbitlang/core/debug.Debug::{
  to_repr,
}

///|
pub extend PulsarError with Show::{to_string, output}

///|
pub extend PulsarError with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend IncomingFrame with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend NamespaceTopicMode with Eq::{not_equal, equal}

///|
pub extend NamespaceTopicMode with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend MessageId with Eq::{not_equal, equal}

///|
pub extend MessageId with Compare::{op_lt, op_le, op_ge, compare, op_gt}

///|
pub extend MessageId with Hash::{hash, hash_combine}

///|
pub extend MessageId with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SendReceipt with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ProducerAccessMode with Eq::{not_equal, equal}

///|
pub extend ProducerAccessMode with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend BatchingConfig with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend RetryPolicy with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend SchemaInfo with Eq::{not_equal, equal}

///|
pub extend SchemaInfo with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend VersionedSchema with Eq::{not_equal, equal}

///|
pub extend VersionedSchema with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend ClientMetricsSnapshot with Eq::{not_equal, equal}

///|
pub extend ClientMetricsSnapshot with @moonbitlang/core/debug.Debug::{to_repr}
