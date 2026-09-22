///|
pub(all) enum CompressionType {
  NONE
  LZ4
  ZLIB
  ZSTD
  SNAPPY
} derive(Eq, Debug)

///|
pub fn CompressionType::to_enum(self : CompressionType) -> @lib.Enum {
  match self {
    CompressionType::NONE => 0
    CompressionType::LZ4 => 1
    CompressionType::ZLIB => 2
    CompressionType::ZSTD => 3
    CompressionType::SNAPPY => 4
  }
}

///|
pub fn CompressionType::from_enum(i : @lib.Enum) -> CompressionType {
  match i.0 {
    0 => CompressionType::NONE
    1 => CompressionType::LZ4
    2 => CompressionType::ZLIB
    3 => CompressionType::ZSTD
    4 => CompressionType::SNAPPY
    _ => Default::default()
  }
}

///|
pub impl Default for CompressionType with fn default() -> CompressionType {
  CompressionType::NONE
}

///|
pub impl @lib.Sized for CompressionType with fn size_of(self : CompressionType) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CompressionType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CompressionType raise {
  match json {
    String("NONE") => CompressionType::NONE
    String("LZ4") => CompressionType::LZ4
    String("ZLIB") => CompressionType::ZLIB
    String("ZSTD") => CompressionType::ZSTD
    String("SNAPPY") => CompressionType::SNAPPY
    Number(0, ..) => CompressionType::NONE
    Number(1, ..) => CompressionType::LZ4
    Number(2, ..) => CompressionType::ZLIB
    Number(3, ..) => CompressionType::ZSTD
    Number(4, ..) => CompressionType::SNAPPY
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CompressionType with fn to_json(self : CompressionType) -> Json {
  match self {
    CompressionType::NONE => "NONE"
    CompressionType::LZ4 => "LZ4"
    CompressionType::ZLIB => "ZLIB"
    CompressionType::ZSTD => "ZSTD"
    CompressionType::SNAPPY => "SNAPPY"
  }
}

///|
pub(all) enum ProducerAccessMode {
  Shared
  Exclusive
  WaitForExclusive
  ExclusiveWithFencing
} derive(Eq, Debug)

///|
pub fn ProducerAccessMode::to_enum(self : ProducerAccessMode) -> @lib.Enum {
  match self {
    ProducerAccessMode::Shared => 0
    ProducerAccessMode::Exclusive => 1
    ProducerAccessMode::WaitForExclusive => 2
    ProducerAccessMode::ExclusiveWithFencing => 3
  }
}

///|
pub fn ProducerAccessMode::from_enum(i : @lib.Enum) -> ProducerAccessMode {
  match i.0 {
    0 => ProducerAccessMode::Shared
    1 => ProducerAccessMode::Exclusive
    2 => ProducerAccessMode::WaitForExclusive
    3 => ProducerAccessMode::ExclusiveWithFencing
    _ => Default::default()
  }
}

///|
pub impl Default for ProducerAccessMode with fn default() -> ProducerAccessMode {
  ProducerAccessMode::Shared
}

///|
pub impl @lib.Sized for ProducerAccessMode with fn size_of(
  self : ProducerAccessMode,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for ProducerAccessMode with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ProducerAccessMode raise {
  match json {
    String("Shared") => ProducerAccessMode::Shared
    String("Exclusive") => ProducerAccessMode::Exclusive
    String("WaitForExclusive") => ProducerAccessMode::WaitForExclusive
    String("ExclusiveWithFencing") => ProducerAccessMode::ExclusiveWithFencing
    Number(0, ..) => ProducerAccessMode::Shared
    Number(1, ..) => ProducerAccessMode::Exclusive
    Number(2, ..) => ProducerAccessMode::WaitForExclusive
    Number(3, ..) => ProducerAccessMode::ExclusiveWithFencing
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for ProducerAccessMode with fn to_json(
  self : ProducerAccessMode,
) -> Json {
  match self {
    ProducerAccessMode::Shared => "Shared"
    ProducerAccessMode::Exclusive => "Exclusive"
    ProducerAccessMode::WaitForExclusive => "WaitForExclusive"
    ProducerAccessMode::ExclusiveWithFencing => "ExclusiveWithFencing"
  }
}

///|
pub(all) enum ServerError {
  UnknownError
  MetadataError
  PersistenceError
  AuthenticationError
  AuthorizationError
  ConsumerBusy
  ServiceNotReady
  ProducerBlockedQuotaExceededError
  ProducerBlockedQuotaExceededException
  ChecksumError
  UnsupportedVersionError
  TopicNotFound
  SubscriptionNotFound
  ConsumerNotFound
  TooManyRequests
  TopicTerminatedError
  ProducerBusy
  InvalidTopicName
  IncompatibleSchema
  ConsumerAssignError
  TransactionCoordinatorNotFound
  InvalidTxnStatus
  NotAllowedError
  TransactionConflict
  TransactionNotFound
  ProducerFenced
} derive(Eq, Debug)

///|
pub fn ServerError::to_enum(self : ServerError) -> @lib.Enum {
  match self {
    ServerError::UnknownError => 0
    ServerError::MetadataError => 1
    ServerError::PersistenceError => 2
    ServerError::AuthenticationError => 3
    ServerError::AuthorizationError => 4
    ServerError::ConsumerBusy => 5
    ServerError::ServiceNotReady => 6
    ServerError::ProducerBlockedQuotaExceededError => 7
    ServerError::ProducerBlockedQuotaExceededException => 8
    ServerError::ChecksumError => 9
    ServerError::UnsupportedVersionError => 10
    ServerError::TopicNotFound => 11
    ServerError::SubscriptionNotFound => 12
    ServerError::ConsumerNotFound => 13
    ServerError::TooManyRequests => 14
    ServerError::TopicTerminatedError => 15
    ServerError::ProducerBusy => 16
    ServerError::InvalidTopicName => 17
    ServerError::IncompatibleSchema => 18
    ServerError::ConsumerAssignError => 19
    ServerError::TransactionCoordinatorNotFound => 20
    ServerError::InvalidTxnStatus => 21
    ServerError::NotAllowedError => 22
    ServerError::TransactionConflict => 23
    ServerError::TransactionNotFound => 24
    ServerError::ProducerFenced => 25
  }
}

///|
pub fn ServerError::from_enum(i : @lib.Enum) -> ServerError {
  match i.0 {
    0 => ServerError::UnknownError
    1 => ServerError::MetadataError
    2 => ServerError::PersistenceError
    3 => ServerError::AuthenticationError
    4 => ServerError::AuthorizationError
    5 => ServerError::ConsumerBusy
    6 => ServerError::ServiceNotReady
    7 => ServerError::ProducerBlockedQuotaExceededError
    8 => ServerError::ProducerBlockedQuotaExceededException
    9 => ServerError::ChecksumError
    10 => ServerError::UnsupportedVersionError
    11 => ServerError::TopicNotFound
    12 => ServerError::SubscriptionNotFound
    13 => ServerError::ConsumerNotFound
    14 => ServerError::TooManyRequests
    15 => ServerError::TopicTerminatedError
    16 => ServerError::ProducerBusy
    17 => ServerError::InvalidTopicName
    18 => ServerError::IncompatibleSchema
    19 => ServerError::ConsumerAssignError
    20 => ServerError::TransactionCoordinatorNotFound
    21 => ServerError::InvalidTxnStatus
    22 => ServerError::NotAllowedError
    23 => ServerError::TransactionConflict
    24 => ServerError::TransactionNotFound
    25 => ServerError::ProducerFenced
    _ => Default::default()
  }
}

///|
pub impl Default for ServerError with fn default() -> ServerError {
  ServerError::UnknownError
}

///|
pub impl @lib.Sized for ServerError with fn size_of(self : ServerError) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for ServerError with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ServerError raise {
  match json {
    String("UnknownError") => ServerError::UnknownError
    String("MetadataError") => ServerError::MetadataError
    String("PersistenceError") => ServerError::PersistenceError
    String("AuthenticationError") => ServerError::AuthenticationError
    String("AuthorizationError") => ServerError::AuthorizationError
    String("ConsumerBusy") => ServerError::ConsumerBusy
    String("ServiceNotReady") => ServerError::ServiceNotReady
    String("ProducerBlockedQuotaExceededError") =>
      ServerError::ProducerBlockedQuotaExceededError
    String("ProducerBlockedQuotaExceededException") =>
      ServerError::ProducerBlockedQuotaExceededException
    String("ChecksumError") => ServerError::ChecksumError
    String("UnsupportedVersionError") => ServerError::UnsupportedVersionError
    String("TopicNotFound") => ServerError::TopicNotFound
    String("SubscriptionNotFound") => ServerError::SubscriptionNotFound
    String("ConsumerNotFound") => ServerError::ConsumerNotFound
    String("TooManyRequests") => ServerError::TooManyRequests
    String("TopicTerminatedError") => ServerError::TopicTerminatedError
    String("ProducerBusy") => ServerError::ProducerBusy
    String("InvalidTopicName") => ServerError::InvalidTopicName
    String("IncompatibleSchema") => ServerError::IncompatibleSchema
    String("ConsumerAssignError") => ServerError::ConsumerAssignError
    String("TransactionCoordinatorNotFound") =>
      ServerError::TransactionCoordinatorNotFound
    String("InvalidTxnStatus") => ServerError::InvalidTxnStatus
    String("NotAllowedError") => ServerError::NotAllowedError
    String("TransactionConflict") => ServerError::TransactionConflict
    String("TransactionNotFound") => ServerError::TransactionNotFound
    String("ProducerFenced") => ServerError::ProducerFenced
    Number(0, ..) => ServerError::UnknownError
    Number(1, ..) => ServerError::MetadataError
    Number(2, ..) => ServerError::PersistenceError
    Number(3, ..) => ServerError::AuthenticationError
    Number(4, ..) => ServerError::AuthorizationError
    Number(5, ..) => ServerError::ConsumerBusy
    Number(6, ..) => ServerError::ServiceNotReady
    Number(7, ..) => ServerError::ProducerBlockedQuotaExceededError
    Number(8, ..) => ServerError::ProducerBlockedQuotaExceededException
    Number(9, ..) => ServerError::ChecksumError
    Number(10, ..) => ServerError::UnsupportedVersionError
    Number(11, ..) => ServerError::TopicNotFound
    Number(12, ..) => ServerError::SubscriptionNotFound
    Number(13, ..) => ServerError::ConsumerNotFound
    Number(14, ..) => ServerError::TooManyRequests
    Number(15, ..) => ServerError::TopicTerminatedError
    Number(16, ..) => ServerError::ProducerBusy
    Number(17, ..) => ServerError::InvalidTopicName
    Number(18, ..) => ServerError::IncompatibleSchema
    Number(19, ..) => ServerError::ConsumerAssignError
    Number(20, ..) => ServerError::TransactionCoordinatorNotFound
    Number(21, ..) => ServerError::InvalidTxnStatus
    Number(22, ..) => ServerError::NotAllowedError
    Number(23, ..) => ServerError::TransactionConflict
    Number(24, ..) => ServerError::TransactionNotFound
    Number(25, ..) => ServerError::ProducerFenced
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for ServerError with fn to_json(self : ServerError) -> Json {
  match self {
    ServerError::UnknownError => "UnknownError"
    ServerError::MetadataError => "MetadataError"
    ServerError::PersistenceError => "PersistenceError"
    ServerError::AuthenticationError => "AuthenticationError"
    ServerError::AuthorizationError => "AuthorizationError"
    ServerError::ConsumerBusy => "ConsumerBusy"
    ServerError::ServiceNotReady => "ServiceNotReady"
    ServerError::ProducerBlockedQuotaExceededError =>
      "ProducerBlockedQuotaExceededError"
    ServerError::ProducerBlockedQuotaExceededException =>
      "ProducerBlockedQuotaExceededException"
    ServerError::ChecksumError => "ChecksumError"
    ServerError::UnsupportedVersionError => "UnsupportedVersionError"
    ServerError::TopicNotFound => "TopicNotFound"
    ServerError::SubscriptionNotFound => "SubscriptionNotFound"
    ServerError::ConsumerNotFound => "ConsumerNotFound"
    ServerError::TooManyRequests => "TooManyRequests"
    ServerError::TopicTerminatedError => "TopicTerminatedError"
    ServerError::ProducerBusy => "ProducerBusy"
    ServerError::InvalidTopicName => "InvalidTopicName"
    ServerError::IncompatibleSchema => "IncompatibleSchema"
    ServerError::ConsumerAssignError => "ConsumerAssignError"
    ServerError::TransactionCoordinatorNotFound =>
      "TransactionCoordinatorNotFound"
    ServerError::InvalidTxnStatus => "InvalidTxnStatus"
    ServerError::NotAllowedError => "NotAllowedError"
    ServerError::TransactionConflict => "TransactionConflict"
    ServerError::TransactionNotFound => "TransactionNotFound"
    ServerError::ProducerFenced => "ProducerFenced"
  }
}

///|
pub(all) enum AuthMethod {
  AuthMethodNone
  AuthMethodYcaV1
  AuthMethodAthens
} derive(Eq, Debug)

///|
pub fn AuthMethod::to_enum(self : AuthMethod) -> @lib.Enum {
  match self {
    AuthMethod::AuthMethodNone => 0
    AuthMethod::AuthMethodYcaV1 => 1
    AuthMethod::AuthMethodAthens => 2
  }
}

///|
pub fn AuthMethod::from_enum(i : @lib.Enum) -> AuthMethod {
  match i.0 {
    0 => AuthMethod::AuthMethodNone
    1 => AuthMethod::AuthMethodYcaV1
    2 => AuthMethod::AuthMethodAthens
    _ => Default::default()
  }
}

///|
pub impl Default for AuthMethod with fn default() -> AuthMethod {
  AuthMethod::AuthMethodNone
}

///|
pub impl @lib.Sized for AuthMethod with fn size_of(self : AuthMethod) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for AuthMethod with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> AuthMethod raise {
  match json {
    String("AuthMethodNone") => AuthMethod::AuthMethodNone
    String("AuthMethodYcaV1") => AuthMethod::AuthMethodYcaV1
    String("AuthMethodAthens") => AuthMethod::AuthMethodAthens
    Number(0, ..) => AuthMethod::AuthMethodNone
    Number(1, ..) => AuthMethod::AuthMethodYcaV1
    Number(2, ..) => AuthMethod::AuthMethodAthens
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for AuthMethod with fn to_json(self : AuthMethod) -> Json {
  match self {
    AuthMethod::AuthMethodNone => "AuthMethodNone"
    AuthMethod::AuthMethodYcaV1 => "AuthMethodYcaV1"
    AuthMethod::AuthMethodAthens => "AuthMethodAthens"
  }
}

///|
pub(all) enum ProtocolVersion {
  V0
  V1
  V2
  V3
  V4
  V5
  V6
  V7
  V8
  V9
  V10
  V11
  V12
  V13
  V14
  V15
  V16
  V17
  V18
  V19
  V20
  V21
} derive(Eq, Debug)

///|
pub fn ProtocolVersion::to_enum(self : ProtocolVersion) -> @lib.Enum {
  match self {
    ProtocolVersion::V0 => 0
    ProtocolVersion::V1 => 1
    ProtocolVersion::V2 => 2
    ProtocolVersion::V3 => 3
    ProtocolVersion::V4 => 4
    ProtocolVersion::V5 => 5
    ProtocolVersion::V6 => 6
    ProtocolVersion::V7 => 7
    ProtocolVersion::V8 => 8
    ProtocolVersion::V9 => 9
    ProtocolVersion::V10 => 10
    ProtocolVersion::V11 => 11
    ProtocolVersion::V12 => 12
    ProtocolVersion::V13 => 13
    ProtocolVersion::V14 => 14
    ProtocolVersion::V15 => 15
    ProtocolVersion::V16 => 16
    ProtocolVersion::V17 => 17
    ProtocolVersion::V18 => 18
    ProtocolVersion::V19 => 19
    ProtocolVersion::V20 => 20
    ProtocolVersion::V21 => 21
  }
}

///|
pub fn ProtocolVersion::from_enum(i : @lib.Enum) -> ProtocolVersion {
  match i.0 {
    0 => ProtocolVersion::V0
    1 => ProtocolVersion::V1
    2 => ProtocolVersion::V2
    3 => ProtocolVersion::V3
    4 => ProtocolVersion::V4
    5 => ProtocolVersion::V5
    6 => ProtocolVersion::V6
    7 => ProtocolVersion::V7
    8 => ProtocolVersion::V8
    9 => ProtocolVersion::V9
    10 => ProtocolVersion::V10
    11 => ProtocolVersion::V11
    12 => ProtocolVersion::V12
    13 => ProtocolVersion::V13
    14 => ProtocolVersion::V14
    15 => ProtocolVersion::V15
    16 => ProtocolVersion::V16
    17 => ProtocolVersion::V17
    18 => ProtocolVersion::V18
    19 => ProtocolVersion::V19
    20 => ProtocolVersion::V20
    21 => ProtocolVersion::V21
    _ => Default::default()
  }
}

///|
pub impl Default for ProtocolVersion with fn default() -> ProtocolVersion {
  ProtocolVersion::V0
}

///|
pub impl @lib.Sized for ProtocolVersion with fn size_of(self : ProtocolVersion) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for ProtocolVersion with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ProtocolVersion raise {
  match json {
    String("V0") => ProtocolVersion::V0
    String("V1") => ProtocolVersion::V1
    String("V2") => ProtocolVersion::V2
    String("V3") => ProtocolVersion::V3
    String("V4") => ProtocolVersion::V4
    String("V5") => ProtocolVersion::V5
    String("V6") => ProtocolVersion::V6
    String("V7") => ProtocolVersion::V7
    String("V8") => ProtocolVersion::V8
    String("V9") => ProtocolVersion::V9
    String("V10") => ProtocolVersion::V10
    String("V11") => ProtocolVersion::V11
    String("V12") => ProtocolVersion::V12
    String("V13") => ProtocolVersion::V13
    String("V14") => ProtocolVersion::V14
    String("V15") => ProtocolVersion::V15
    String("V16") => ProtocolVersion::V16
    String("V17") => ProtocolVersion::V17
    String("V18") => ProtocolVersion::V18
    String("V19") => ProtocolVersion::V19
    String("V20") => ProtocolVersion::V20
    String("V21") => ProtocolVersion::V21
    Number(0, ..) => ProtocolVersion::V0
    Number(1, ..) => ProtocolVersion::V1
    Number(2, ..) => ProtocolVersion::V2
    Number(3, ..) => ProtocolVersion::V3
    Number(4, ..) => ProtocolVersion::V4
    Number(5, ..) => ProtocolVersion::V5
    Number(6, ..) => ProtocolVersion::V6
    Number(7, ..) => ProtocolVersion::V7
    Number(8, ..) => ProtocolVersion::V8
    Number(9, ..) => ProtocolVersion::V9
    Number(10, ..) => ProtocolVersion::V10
    Number(11, ..) => ProtocolVersion::V11
    Number(12, ..) => ProtocolVersion::V12
    Number(13, ..) => ProtocolVersion::V13
    Number(14, ..) => ProtocolVersion::V14
    Number(15, ..) => ProtocolVersion::V15
    Number(16, ..) => ProtocolVersion::V16
    Number(17, ..) => ProtocolVersion::V17
    Number(18, ..) => ProtocolVersion::V18
    Number(19, ..) => ProtocolVersion::V19
    Number(20, ..) => ProtocolVersion::V20
    Number(21, ..) => ProtocolVersion::V21
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for ProtocolVersion with fn to_json(self : ProtocolVersion) -> Json {
  match self {
    ProtocolVersion::V0 => "V0"
    ProtocolVersion::V1 => "V1"
    ProtocolVersion::V2 => "V2"
    ProtocolVersion::V3 => "V3"
    ProtocolVersion::V4 => "V4"
    ProtocolVersion::V5 => "V5"
    ProtocolVersion::V6 => "V6"
    ProtocolVersion::V7 => "V7"
    ProtocolVersion::V8 => "V8"
    ProtocolVersion::V9 => "V9"
    ProtocolVersion::V10 => "V10"
    ProtocolVersion::V11 => "V11"
    ProtocolVersion::V12 => "V12"
    ProtocolVersion::V13 => "V13"
    ProtocolVersion::V14 => "V14"
    ProtocolVersion::V15 => "V15"
    ProtocolVersion::V16 => "V16"
    ProtocolVersion::V17 => "V17"
    ProtocolVersion::V18 => "V18"
    ProtocolVersion::V19 => "V19"
    ProtocolVersion::V20 => "V20"
    ProtocolVersion::V21 => "V21"
  }
}

///|
pub(all) enum KeySharedMode {
  AUTO_SPLIT
  STICKY
} derive(Eq, Debug)

///|
pub fn KeySharedMode::to_enum(self : KeySharedMode) -> @lib.Enum {
  match self {
    KeySharedMode::AUTO_SPLIT => 0
    KeySharedMode::STICKY => 1
  }
}

///|
pub fn KeySharedMode::from_enum(i : @lib.Enum) -> KeySharedMode {
  match i.0 {
    0 => KeySharedMode::AUTO_SPLIT
    1 => KeySharedMode::STICKY
    _ => Default::default()
  }
}

///|
pub impl Default for KeySharedMode with fn default() -> KeySharedMode {
  KeySharedMode::AUTO_SPLIT
}

///|
pub impl @lib.Sized for KeySharedMode with fn size_of(self : KeySharedMode) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for KeySharedMode with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> KeySharedMode raise {
  match json {
    String("AUTO_SPLIT") => KeySharedMode::AUTO_SPLIT
    String("STICKY") => KeySharedMode::STICKY
    Number(0, ..) => KeySharedMode::AUTO_SPLIT
    Number(1, ..) => KeySharedMode::STICKY
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for KeySharedMode with fn to_json(self : KeySharedMode) -> Json {
  match self {
    KeySharedMode::AUTO_SPLIT => "AUTO_SPLIT"
    KeySharedMode::STICKY => "STICKY"
  }
}

///|
pub(all) enum SegmentState {
  ACTIVE
  SEALED
} derive(Eq, Debug)

///|
pub fn SegmentState::to_enum(self : SegmentState) -> @lib.Enum {
  match self {
    SegmentState::ACTIVE => 0
    SegmentState::SEALED => 1
  }
}

///|
pub fn SegmentState::from_enum(i : @lib.Enum) -> SegmentState {
  match i.0 {
    0 => SegmentState::ACTIVE
    1 => SegmentState::SEALED
    _ => Default::default()
  }
}

///|
pub impl Default for SegmentState with fn default() -> SegmentState {
  SegmentState::ACTIVE
}

///|
pub impl @lib.Sized for SegmentState with fn size_of(self : SegmentState) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for SegmentState with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> SegmentState raise {
  match json {
    String("ACTIVE") => SegmentState::ACTIVE
    String("SEALED") => SegmentState::SEALED
    Number(0, ..) => SegmentState::ACTIVE
    Number(1, ..) => SegmentState::SEALED
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for SegmentState with fn to_json(self : SegmentState) -> Json {
  match self {
    SegmentState::ACTIVE => "ACTIVE"
    SegmentState::SEALED => "SEALED"
  }
}

///|
pub(all) enum ScalableConsumerType {
  STREAM
  CHECKPOINT
} derive(Eq, Debug)

///|
pub fn ScalableConsumerType::to_enum(self : ScalableConsumerType) -> @lib.Enum {
  match self {
    ScalableConsumerType::STREAM => 0
    ScalableConsumerType::CHECKPOINT => 1
  }
}

///|
pub fn ScalableConsumerType::from_enum(i : @lib.Enum) -> ScalableConsumerType {
  match i.0 {
    0 => ScalableConsumerType::STREAM
    1 => ScalableConsumerType::CHECKPOINT
    _ => Default::default()
  }
}

///|
pub impl Default for ScalableConsumerType with fn default() -> ScalableConsumerType {
  ScalableConsumerType::STREAM
}

///|
pub impl @lib.Sized for ScalableConsumerType with fn size_of(
  self : ScalableConsumerType,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for ScalableConsumerType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ScalableConsumerType raise {
  match json {
    String("STREAM") => ScalableConsumerType::STREAM
    String("CHECKPOINT") => ScalableConsumerType::CHECKPOINT
    Number(0, ..) => ScalableConsumerType::STREAM
    Number(1, ..) => ScalableConsumerType::CHECKPOINT
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for ScalableConsumerType with fn to_json(
  self : ScalableConsumerType,
) -> Json {
  match self {
    ScalableConsumerType::STREAM => "STREAM"
    ScalableConsumerType::CHECKPOINT => "CHECKPOINT"
  }
}

///|
pub(all) enum TxnAction {
  COMMIT
  ABORT
} derive(Eq, Debug)

///|
pub fn TxnAction::to_enum(self : TxnAction) -> @lib.Enum {
  match self {
    TxnAction::COMMIT => 0
    TxnAction::ABORT => 1
  }
}

///|
pub fn TxnAction::from_enum(i : @lib.Enum) -> TxnAction {
  match i.0 {
    0 => TxnAction::COMMIT
    1 => TxnAction::ABORT
    _ => Default::default()
  }
}

///|
pub impl Default for TxnAction with fn default() -> TxnAction {
  TxnAction::COMMIT
}

///|
pub impl @lib.Sized for TxnAction with fn size_of(self : TxnAction) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for TxnAction with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> TxnAction raise {
  match json {
    String("COMMIT") => TxnAction::COMMIT
    String("ABORT") => TxnAction::ABORT
    Number(0, ..) => TxnAction::COMMIT
    Number(1, ..) => TxnAction::ABORT
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for TxnAction with fn to_json(self : TxnAction) -> Json {
  match self {
    TxnAction::COMMIT => "COMMIT"
    TxnAction::ABORT => "ABORT"
  }
}

///|
pub(all) enum Schema_Type {
  None
  String
  Json
  Protobuf
  Avro
  Bool
  Int8
  Int16
  Int32
  Int64
  Float
  Double
  Date
  Time
  Timestamp
  KeyValue
  Instant
  LocalDate
  LocalTime
  LocalDateTime
  ProtobufNative
  AutoConsume
  External
} derive(Eq, Debug)

///|
pub fn Schema_Type::to_enum(self : Schema_Type) -> @lib.Enum {
  match self {
    Schema_Type::None => 0
    Schema_Type::String => 1
    Schema_Type::Json => 2
    Schema_Type::Protobuf => 3
    Schema_Type::Avro => 4
    Schema_Type::Bool => 5
    Schema_Type::Int8 => 6
    Schema_Type::Int16 => 7
    Schema_Type::Int32 => 8
    Schema_Type::Int64 => 9
    Schema_Type::Float => 10
    Schema_Type::Double => 11
    Schema_Type::Date => 12
    Schema_Type::Time => 13
    Schema_Type::Timestamp => 14
    Schema_Type::KeyValue => 15
    Schema_Type::Instant => 16
    Schema_Type::LocalDate => 17
    Schema_Type::LocalTime => 18
    Schema_Type::LocalDateTime => 19
    Schema_Type::ProtobufNative => 20
    Schema_Type::AutoConsume => 21
    Schema_Type::External => 22
  }
}

///|
pub fn Schema_Type::from_enum(i : @lib.Enum) -> Schema_Type {
  match i.0 {
    0 => Schema_Type::None
    1 => Schema_Type::String
    2 => Schema_Type::Json
    3 => Schema_Type::Protobuf
    4 => Schema_Type::Avro
    5 => Schema_Type::Bool
    6 => Schema_Type::Int8
    7 => Schema_Type::Int16
    8 => Schema_Type::Int32
    9 => Schema_Type::Int64
    10 => Schema_Type::Float
    11 => Schema_Type::Double
    12 => Schema_Type::Date
    13 => Schema_Type::Time
    14 => Schema_Type::Timestamp
    15 => Schema_Type::KeyValue
    16 => Schema_Type::Instant
    17 => Schema_Type::LocalDate
    18 => Schema_Type::LocalTime
    19 => Schema_Type::LocalDateTime
    20 => Schema_Type::ProtobufNative
    21 => Schema_Type::AutoConsume
    22 => Schema_Type::External
    _ => Default::default()
  }
}

///|
pub impl Default for Schema_Type with fn default() -> Schema_Type {
  Schema_Type::None
}

///|
pub impl @lib.Sized for Schema_Type with fn size_of(self : Schema_Type) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for Schema_Type with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> Schema_Type raise {
  match json {
    String("None") => Schema_Type::None
    String("String") => Schema_Type::String
    String("Json") => Schema_Type::Json
    String("Protobuf") => Schema_Type::Protobuf
    String("Avro") => Schema_Type::Avro
    String("Bool") => Schema_Type::Bool
    String("Int8") => Schema_Type::Int8
    String("Int16") => Schema_Type::Int16
    String("Int32") => Schema_Type::Int32
    String("Int64") => Schema_Type::Int64
    String("Float") => Schema_Type::Float
    String("Double") => Schema_Type::Double
    String("Date") => Schema_Type::Date
    String("Time") => Schema_Type::Time
    String("Timestamp") => Schema_Type::Timestamp
    String("KeyValue") => Schema_Type::KeyValue
    String("Instant") => Schema_Type::Instant
    String("LocalDate") => Schema_Type::LocalDate
    String("LocalTime") => Schema_Type::LocalTime
    String("LocalDateTime") => Schema_Type::LocalDateTime
    String("ProtobufNative") => Schema_Type::ProtobufNative
    String("AutoConsume") => Schema_Type::AutoConsume
    String("External") => Schema_Type::External
    Number(0, ..) => Schema_Type::None
    Number(1, ..) => Schema_Type::String
    Number(2, ..) => Schema_Type::Json
    Number(3, ..) => Schema_Type::Protobuf
    Number(4, ..) => Schema_Type::Avro
    Number(5, ..) => Schema_Type::Bool
    Number(6, ..) => Schema_Type::Int8
    Number(7, ..) => Schema_Type::Int16
    Number(8, ..) => Schema_Type::Int32
    Number(9, ..) => Schema_Type::Int64
    Number(10, ..) => Schema_Type::Float
    Number(11, ..) => Schema_Type::Double
    Number(12, ..) => Schema_Type::Date
    Number(13, ..) => Schema_Type::Time
    Number(14, ..) => Schema_Type::Timestamp
    Number(15, ..) => Schema_Type::KeyValue
    Number(16, ..) => Schema_Type::Instant
    Number(17, ..) => Schema_Type::LocalDate
    Number(18, ..) => Schema_Type::LocalTime
    Number(19, ..) => Schema_Type::LocalDateTime
    Number(20, ..) => Schema_Type::ProtobufNative
    Number(21, ..) => Schema_Type::AutoConsume
    Number(22, ..) => Schema_Type::External
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for Schema_Type with fn to_json(self : Schema_Type) -> Json {
  match self {
    Schema_Type::None => "None"
    Schema_Type::String => "String"
    Schema_Type::Json => "Json"
    Schema_Type::Protobuf => "Protobuf"
    Schema_Type::Avro => "Avro"
    Schema_Type::Bool => "Bool"
    Schema_Type::Int8 => "Int8"
    Schema_Type::Int16 => "Int16"
    Schema_Type::Int32 => "Int32"
    Schema_Type::Int64 => "Int64"
    Schema_Type::Float => "Float"
    Schema_Type::Double => "Double"
    Schema_Type::Date => "Date"
    Schema_Type::Time => "Time"
    Schema_Type::Timestamp => "Timestamp"
    Schema_Type::KeyValue => "KeyValue"
    Schema_Type::Instant => "Instant"
    Schema_Type::LocalDate => "LocalDate"
    Schema_Type::LocalTime => "LocalTime"
    Schema_Type::LocalDateTime => "LocalDateTime"
    Schema_Type::ProtobufNative => "ProtobufNative"
    Schema_Type::AutoConsume => "AutoConsume"
    Schema_Type::External => "External"
  }
}

///|
pub(all) struct Schema {
  mut name : String
  mut schema_data : Bytes
  mut type_ : Schema_Type
  mut properties : Array[KeyValue]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for Schema with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.name))
  size += @lib.size_of_length_delimited_field(
    3U,
    @lib.size_of(self.schema_data),
  )
  size += @lib.size_of_field(4U, @lib.size_of(self.type_))
  for s in self.properties {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for Schema with fn default() -> Schema {
  Schema::{
    name: Default::default(),
    schema_data: Default::default(),
    type_: Default::default(),
    properties: [],
  }
}

///|
#alias(new, deprecated)
pub fn Schema::Schema(
  name : String,
  schema_data : Bytes,
  type_ : Schema_Type,
  properties : Array[KeyValue],
) -> Schema {
  Schema::{ name, schema_data, type_, properties, }
}

///|
pub impl @lib.Read for Schema with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> Schema raise {
  let msg : Schema = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.name = reader |> @lib.read_string()
      (3, _) => msg.schema_data = reader |> @lib.read_bytes()
      (4, _) => msg.type_ = reader |> @lib.read_enum() |> Schema_Type::from_enum
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.read_message() : KeyValue))
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for Schema with fn write(
  self : Schema,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.name)
  writer |> @lib.write_varint(26UL)
  writer |> @lib.write_bytes(self.schema_data)
  writer |> @lib.write_varint(32UL)
  writer |> @lib.write_enum(self.type_.to_enum())
  for item in self.properties {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for Schema with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.name != Default::default() {
    json["name"] = ToJson::to_json(self.name)
  }
  if self.schema_data != Default::default() {
    json["schemaData"] = @lib.base64_encode(self.schema_data).to_json()
  }
  if self.type_ != Default::default() {
    json["type"] = ToJson::to_json(self.type_)
  }
  if self.properties != Default::default() {
    json["properties"] = ToJson::to_json(self.properties)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for Schema with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> Schema raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for Schema"))
  }
  let message : Schema = Default::default()
  for key, value in obj {
    match (key, value) {
      ("name", value) => message.name = @json.from_json(value, path~)
      ("schemaData", String(value)) =>
        message.schema_data = @lib.base64_decode(value) catch {
          _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
        }
      ("type", value) => message.type_ = @json.from_json(value, path~)
      ("properties", Array(value)) =>
        message.properties = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for Schema with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> Schema raise {
  let msg : Schema = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.name = reader |> @lib.async_read_string()
      (3, _) => msg.schema_data = reader |> @lib.async_read_bytes()
      (4, _) =>
        msg.type_ = reader |> @lib.async_read_enum() |> Schema_Type::from_enum
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.async_read_message() : KeyValue))
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for Schema with fn write(
  self : Schema,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.name)
  writer |> @lib.async_write_varint(26UL)
  writer |> @lib.async_write_bytes(self.schema_data)
  writer |> @lib.async_write_varint(32UL)
  writer |> @lib.async_write_enum(self.type_.to_enum())
  for item in self.properties {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) struct MessageIdData {
  mut ledger_id : UInt64
  mut entry_id : UInt64
  mut partition : Int?
  mut batch_index : Int?
  mut ack_set : Array[Int64]
  mut batch_size : Int?
  mut first_chunk_message_id : MessageIdData?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for MessageIdData with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.ledger_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.entry_id))
  if self.partition is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.batch_index is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  for s in self.ack_set {
    size += @lib.size_of_field(5U, @lib.size_of(s))
  }
  if self.batch_size is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  if self.first_chunk_message_id is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for MessageIdData with fn default() -> MessageIdData {
  MessageIdData::{
    ledger_id: Default::default(),
    entry_id: Default::default(),
    partition: Some(-1),
    batch_index: Some(-1),
    ack_set: [],
    batch_size: None,
    first_chunk_message_id: None,
  }
}

///|
#alias(new, deprecated)
pub fn MessageIdData::MessageIdData(
  ledger_id : UInt64,
  entry_id : UInt64,
  partition? : Int,
  batch_index? : Int,
  ack_set : Array[Int64],
  batch_size? : Int,
  first_chunk_message_id? : MessageIdData,
) -> MessageIdData {
  MessageIdData::{
    ledger_id,
    entry_id,
    partition,
    batch_index,
    ack_set,
    batch_size,
    first_chunk_message_id,
  }
}

///|
pub impl @lib.Read for MessageIdData with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> MessageIdData raise {
  let msg : MessageIdData = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.ledger_id = reader |> @lib.read_uint64()
      (2, _) => msg.entry_id = reader |> @lib.read_uint64()
      (3, _) => msg.partition = reader |> @lib.read_int32() |> Some
      (4, _) => msg.batch_index = reader |> @lib.read_int32() |> Some
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack_set.push_iter(
          (reader |> @lib.read_packed(@lib.read_int64, None)).iter(),
        )
      (5, @lib.WIRE_TYPE_VARINT) =>
        msg.ack_set.push(reader |> @lib.read_int64())
      (6, _) => msg.batch_size = reader |> @lib.read_int32() |> Some
      (7, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.first_chunk_message_id = (
            reader |> @lib.read_message() : MessageIdData)
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for MessageIdData with fn write(
  self : MessageIdData,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.ledger_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.entry_id)
  if self.partition is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_int32(v)
  }
  if self.batch_index is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_int32(v)
  }
  for item in self.ack_set {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_int64(item)
  }
  if self.batch_size is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_int32(v)
  }
  if self.first_chunk_message_id is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for MessageIdData with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.ledger_id != Default::default() {
    json["ledgerId"] = ToJson::to_json(self.ledger_id)
  }
  if self.entry_id != Default::default() {
    json["entryId"] = ToJson::to_json(self.entry_id)
  }
  match self.partition {
    Some(v) if v != -1 => json["partition"] = ToJson::to_json(v)
    _ => ()
  }
  match self.batch_index {
    Some(v) if v != -1 => json["batchIndex"] = ToJson::to_json(v)
    _ => ()
  }
  if self.ack_set != Default::default() {
    json["ackSet"] = ToJson::to_json(self.ack_set)
  }
  match self.batch_size {
    Some(v) => json["batchSize"] = ToJson::to_json(v)
    _ => ()
  }
  match self.first_chunk_message_id {
    Some(v) => json["firstChunkMessageId"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for MessageIdData with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> MessageIdData raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for MessageIdData"))
  }
  let message : MessageIdData = Default::default()
  for key, value in obj {
    match (key, value) {
      ("ledgerId", value) => message.ledger_id = @json.from_json(value, path~)
      ("entryId", value) => message.entry_id = @json.from_json(value, path~)
      ("partition", value) =>
        message.partition = Some(@json.from_json(value, path~))
      ("batchIndex", value) =>
        message.batch_index = Some(@json.from_json(value, path~))
      ("ackSet", Array(value)) =>
        message.ack_set = value.map(v => @json.from_json(v, path~))
      ("batchSize", value) =>
        message.batch_size = Some(@json.from_json(value, path~))
      ("firstChunkMessageId", value) =>
        message.first_chunk_message_id = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for MessageIdData with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> MessageIdData raise {
  let msg : MessageIdData = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.ledger_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.entry_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.partition = reader |> @lib.async_read_int32() |> Some
      (4, _) => msg.batch_index = reader |> @lib.async_read_int32() |> Some
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack_set.push_iter(
          (reader |> @lib.async_read_packed(@lib.async_read_int64, None)).iter(),
        )
      (5, @lib.WIRE_TYPE_VARINT) =>
        msg.ack_set.push(reader |> @lib.async_read_int64())
      (6, _) => msg.batch_size = reader |> @lib.async_read_int32() |> Some
      (7, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.first_chunk_message_id = (
            reader |> @lib.async_read_message() : MessageIdData)
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for MessageIdData with fn write(
  self : MessageIdData,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.ledger_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.entry_id)
  if self.partition is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.batch_index is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_int32(v)
  }
  for item in self.ack_set {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_int64(item)
  }
  if self.batch_size is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.first_chunk_message_id is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_message(v)
  }
}

///|
pub(all) struct KeyValue {
  mut key : String
  mut value : String
} derive(Eq, Debug)

///|
pub impl @lib.Sized for KeyValue with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.key))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.value))
  size
}

///|
pub impl Default for KeyValue with fn default() -> KeyValue {
  KeyValue::{ key: Default::default(), value: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn KeyValue::KeyValue(key : String, value : String) -> KeyValue {
  KeyValue::{ key, value, }
}

///|
pub impl @lib.Read for KeyValue with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> KeyValue raise {
  let msg : KeyValue = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.key = reader |> @lib.read_string()
      (2, _) => msg.value = reader |> @lib.read_string()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for KeyValue with fn write(
  self : KeyValue,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.key)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.value)
}

///|
pub impl ToJson for KeyValue with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.key != Default::default() {
    json["key"] = ToJson::to_json(self.key)
  }
  if self.value != Default::default() {
    json["value"] = ToJson::to_json(self.value)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for KeyValue with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> KeyValue raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for KeyValue"))
  }
  let message : KeyValue = Default::default()
  for key, value in obj {
    match (key, value) {
      ("key", value) => message.key = @json.from_json(value, path~)
      ("value", value) => message.value = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for KeyValue with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> KeyValue raise {
  let msg : KeyValue = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.key = reader |> @lib.async_read_string()
      (2, _) => msg.value = reader |> @lib.async_read_string()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for KeyValue with fn write(
  self : KeyValue,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.key)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.value)
}

///|
pub(all) struct KeyLongValue {
  mut key : String
  mut value : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for KeyLongValue with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.key))
  size += @lib.size_of_field(2U, @lib.size_of(self.value))
  size
}

///|
pub impl Default for KeyLongValue with fn default() -> KeyLongValue {
  KeyLongValue::{ key: Default::default(), value: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn KeyLongValue::KeyLongValue(key : String, value : UInt64) -> KeyLongValue {
  KeyLongValue::{ key, value, }
}

///|
pub impl @lib.Read for KeyLongValue with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> KeyLongValue raise {
  let msg : KeyLongValue = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.key = reader |> @lib.read_string()
      (2, _) => msg.value = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for KeyLongValue with fn write(
  self : KeyLongValue,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.key)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.value)
}

///|
pub impl ToJson for KeyLongValue with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.key != Default::default() {
    json["key"] = ToJson::to_json(self.key)
  }
  if self.value != Default::default() {
    json["value"] = ToJson::to_json(self.value)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for KeyLongValue with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> KeyLongValue raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for KeyLongValue"))
  }
  let message : KeyLongValue = Default::default()
  for key, value in obj {
    match (key, value) {
      ("key", value) => message.key = @json.from_json(value, path~)
      ("value", value) => message.value = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for KeyLongValue with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> KeyLongValue raise {
  let msg : KeyLongValue = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.key = reader |> @lib.async_read_string()
      (2, _) => msg.value = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for KeyLongValue with fn write(
  self : KeyLongValue,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.key)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.value)
}

///|
pub(all) struct IntRange {
  mut start : Int
  mut end : Int
} derive(Eq, Debug)

///|
pub impl @lib.Sized for IntRange with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.start))
  size += @lib.size_of_field(2U, @lib.size_of(self.end))
  size
}

///|
pub impl Default for IntRange with fn default() -> IntRange {
  IntRange::{ start: Default::default(), end: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn IntRange::IntRange(start : Int, end : Int) -> IntRange {
  IntRange::{ start, end, }
}

///|
pub impl @lib.Read for IntRange with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> IntRange raise {
  let msg : IntRange = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.start = reader |> @lib.read_int32()
      (2, _) => msg.end = reader |> @lib.read_int32()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for IntRange with fn write(
  self : IntRange,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_int32(self.start)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_int32(self.end)
}

///|
pub impl ToJson for IntRange with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.start != Default::default() {
    json["start"] = ToJson::to_json(self.start)
  }
  if self.end != Default::default() {
    json["end"] = ToJson::to_json(self.end)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for IntRange with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> IntRange raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for IntRange"))
  }
  let message : IntRange = Default::default()
  for key, value in obj {
    match (key, value) {
      ("start", value) => message.start = @json.from_json(value, path~)
      ("end", value) => message.end = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for IntRange with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> IntRange raise {
  let msg : IntRange = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.start = reader |> @lib.async_read_int32()
      (2, _) => msg.end = reader |> @lib.async_read_int32()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for IntRange with fn write(
  self : IntRange,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_int32(self.start)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_int32(self.end)
}

///|
pub(all) struct EncryptionKeys {
  mut key : String
  mut value : Bytes
  mut metadata : Array[KeyValue]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for EncryptionKeys with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.key))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.value))
  for s in self.metadata {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for EncryptionKeys with fn default() -> EncryptionKeys {
  EncryptionKeys::{
    key: Default::default(),
    value: Default::default(),
    metadata: [],
  }
}

///|
#alias(new, deprecated)
pub fn EncryptionKeys::EncryptionKeys(
  key : String,
  value : Bytes,
  metadata : Array[KeyValue],
) -> EncryptionKeys {
  EncryptionKeys::{ key, value, metadata, }
}

///|
pub impl @lib.Read for EncryptionKeys with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> EncryptionKeys raise {
  let msg : EncryptionKeys = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.key = reader |> @lib.read_string()
      (2, _) => msg.value = reader |> @lib.read_bytes()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.metadata.push((reader |> @lib.read_message() : KeyValue))
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for EncryptionKeys with fn write(
  self : EncryptionKeys,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.key)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_bytes(self.value)
  for item in self.metadata {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for EncryptionKeys with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.key != Default::default() {
    json["key"] = ToJson::to_json(self.key)
  }
  if self.value != Default::default() {
    json["value"] = @lib.base64_encode(self.value).to_json()
  }
  if self.metadata != Default::default() {
    json["metadata"] = ToJson::to_json(self.metadata)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for EncryptionKeys with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> EncryptionKeys raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for EncryptionKeys"))
  }
  let message : EncryptionKeys = Default::default()
  for key, value in obj {
    match (key, value) {
      ("key", value) => message.key = @json.from_json(value, path~)
      ("value", String(value)) =>
        message.value = @lib.base64_decode(value) catch {
          _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
        }
      ("metadata", Array(value)) =>
        message.metadata = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for EncryptionKeys with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> EncryptionKeys raise {
  let msg : EncryptionKeys = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.key = reader |> @lib.async_read_string()
      (2, _) => msg.value = reader |> @lib.async_read_bytes()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.metadata.push((reader |> @lib.async_read_message() : KeyValue))
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for EncryptionKeys with fn write(
  self : EncryptionKeys,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.key)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_bytes(self.value)
  for item in self.metadata {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) struct MessageMetadata {
  mut producer_name : String
  mut sequence_id : UInt64
  mut publish_time : UInt64
  mut properties : Array[KeyValue]
  mut replicated_from : String?
  mut partition_key : String?
  mut replicate_to : Array[String]
  mut compression : CompressionType?
  mut uncompressed_size : UInt?
  mut num_messages_in_batch : Int?
  mut event_time : UInt64?
  mut encryption_keys : Array[EncryptionKeys]
  mut encryption_algo : String?
  mut encryption_param : Bytes?
  mut schema_version : Bytes?
  mut partition_key_b64_encoded : Bool?
  mut ordering_key : Bytes?
  mut deliver_at_time : Int64?
  mut marker_type : Int?
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut highest_sequence_id : UInt64?
  mut null_value : Bool?
  mut uuid : String?
  mut num_chunks_from_msg : Int?
  mut total_chunk_msg_size : Int?
  mut chunk_id : Int?
  mut null_partition_key : Bool?
  mut compacted_batch_indexes : Array[Int]
  mut schema_id : Bytes?
  mut entry_hash_min : UInt?
  mut entry_hash_max : UInt?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for MessageMetadata with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(
    1U,
    @lib.size_of(self.producer_name),
  )
  size += @lib.size_of_field(2U, @lib.size_of(self.sequence_id))
  size += @lib.size_of_field(3U, @lib.size_of(self.publish_time))
  for s in self.properties {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(s))
  }
  if self.replicated_from is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  if self.partition_key is Some(v) {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(v))
  }
  for s in self.replicate_to {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(s))
  }
  if self.compression is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.uncompressed_size is Some(v) {
    size += @lib.size_of_field(9U, @lib.size_of(v))
  }
  if self.num_messages_in_batch is Some(v) {
    size += @lib.size_of_field(11U, @lib.size_of(v))
  }
  if self.event_time is Some(v) {
    size += @lib.size_of_field(12U, @lib.size_of(v))
  }
  for s in self.encryption_keys {
    size += @lib.size_of_length_delimited_field(13U, @lib.size_of(s))
  }
  if self.encryption_algo is Some(v) {
    size += @lib.size_of_length_delimited_field(14U, @lib.size_of(v))
  }
  if self.encryption_param is Some(v) {
    size += @lib.size_of_length_delimited_field(15U, @lib.size_of(v))
  }
  if self.schema_version is Some(v) {
    size += @lib.size_of_length_delimited_field(16U, @lib.size_of(v))
  }
  if self.partition_key_b64_encoded is Some(v) {
    size += @lib.size_of_field(17U, @lib.size_of(v))
  }
  if self.ordering_key is Some(v) {
    size += @lib.size_of_length_delimited_field(18U, @lib.size_of(v))
  }
  if self.deliver_at_time is Some(v) {
    size += @lib.size_of_field(19U, @lib.size_of(v))
  }
  if self.marker_type is Some(v) {
    size += @lib.size_of_field(20U, @lib.size_of(v))
  }
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(22U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(23U, @lib.size_of(v))
  }
  if self.highest_sequence_id is Some(v) {
    size += @lib.size_of_field(24U, @lib.size_of(v))
  }
  if self.null_value is Some(v) {
    size += @lib.size_of_field(25U, @lib.size_of(v))
  }
  if self.uuid is Some(v) {
    size += @lib.size_of_length_delimited_field(26U, @lib.size_of(v))
  }
  if self.num_chunks_from_msg is Some(v) {
    size += @lib.size_of_field(27U, @lib.size_of(v))
  }
  if self.total_chunk_msg_size is Some(v) {
    size += @lib.size_of_field(28U, @lib.size_of(v))
  }
  if self.chunk_id is Some(v) {
    size += @lib.size_of_field(29U, @lib.size_of(v))
  }
  if self.null_partition_key is Some(v) {
    size += @lib.size_of_field(30U, @lib.size_of(v))
  }
  for s in self.compacted_batch_indexes {
    size += @lib.size_of_field(31U, @lib.size_of(s))
  }
  if self.schema_id is Some(v) {
    size += @lib.size_of_length_delimited_field(32U, @lib.size_of(v))
  }
  if self.entry_hash_min is Some(v) {
    size += @lib.size_of_field(33U, @lib.size_of(v))
  }
  if self.entry_hash_max is Some(v) {
    size += @lib.size_of_field(34U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for MessageMetadata with fn default() -> MessageMetadata {
  MessageMetadata::{
    producer_name: Default::default(),
    sequence_id: Default::default(),
    publish_time: Default::default(),
    properties: [],
    replicated_from: None,
    partition_key: None,
    replicate_to: [],
    compression: Some(CompressionType::NONE),
    uncompressed_size: Some(0),
    num_messages_in_batch: Some(1),
    event_time: Some(0),
    encryption_keys: [],
    encryption_algo: None,
    encryption_param: None,
    schema_version: None,
    partition_key_b64_encoded: Some(false),
    ordering_key: None,
    deliver_at_time: None,
    marker_type: None,
    txnid_least_bits: None,
    txnid_most_bits: None,
    highest_sequence_id: Some(0),
    null_value: Some(false),
    uuid: None,
    num_chunks_from_msg: None,
    total_chunk_msg_size: None,
    chunk_id: None,
    null_partition_key: Some(false),
    compacted_batch_indexes: [],
    schema_id: None,
    entry_hash_min: None,
    entry_hash_max: None,
  }
}

///|
#alias(new, deprecated)
pub fn MessageMetadata::MessageMetadata(
  producer_name : String,
  sequence_id : UInt64,
  publish_time : UInt64,
  properties : Array[KeyValue],
  replicated_from? : String,
  partition_key? : String,
  replicate_to : Array[String],
  compression? : CompressionType,
  uncompressed_size? : UInt,
  num_messages_in_batch? : Int,
  event_time? : UInt64,
  encryption_keys : Array[EncryptionKeys],
  encryption_algo? : String,
  encryption_param? : Bytes,
  schema_version? : Bytes,
  partition_key_b64_encoded? : Bool,
  ordering_key? : Bytes,
  deliver_at_time? : Int64,
  marker_type? : Int,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  highest_sequence_id? : UInt64,
  null_value? : Bool,
  uuid? : String,
  num_chunks_from_msg? : Int,
  total_chunk_msg_size? : Int,
  chunk_id? : Int,
  null_partition_key? : Bool,
  compacted_batch_indexes : Array[Int],
  schema_id? : Bytes,
  entry_hash_min? : UInt,
  entry_hash_max? : UInt,
) -> MessageMetadata {
  MessageMetadata::{
    producer_name,
    sequence_id,
    publish_time,
    properties,
    replicated_from,
    partition_key,
    replicate_to,
    compression,
    uncompressed_size,
    num_messages_in_batch,
    event_time,
    encryption_keys,
    encryption_algo,
    encryption_param,
    schema_version,
    partition_key_b64_encoded,
    ordering_key,
    deliver_at_time,
    marker_type,
    txnid_least_bits,
    txnid_most_bits,
    highest_sequence_id,
    null_value,
    uuid,
    num_chunks_from_msg,
    total_chunk_msg_size,
    chunk_id,
    null_partition_key,
    compacted_batch_indexes,
    schema_id,
    entry_hash_min,
    entry_hash_max,
  }
}

///|
pub impl @lib.Read for MessageMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> MessageMetadata raise {
  let msg : MessageMetadata = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_name = reader |> @lib.read_string()
      (2, _) => msg.sequence_id = reader |> @lib.read_uint64()
      (3, _) => msg.publish_time = reader |> @lib.read_uint64()
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.read_message() : KeyValue))
      (5, _) => msg.replicated_from = reader |> @lib.read_string() |> Some
      (6, _) => msg.partition_key = reader |> @lib.read_string() |> Some
      (7, _) => msg.replicate_to.push(reader |> @lib.read_string())
      (8, _) =>
        msg.compression = reader
          |> @lib.read_enum()
          |> CompressionType::from_enum
          |> Some
      (9, _) => msg.uncompressed_size = reader |> @lib.read_uint32() |> Some
      (11, _) => msg.num_messages_in_batch = reader |> @lib.read_int32() |> Some
      (12, _) => msg.event_time = reader |> @lib.read_uint64() |> Some
      (13, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.encryption_keys.push(
          (reader |> @lib.read_message() : EncryptionKeys),
        )
      (14, _) => msg.encryption_algo = reader |> @lib.read_string() |> Some
      (15, _) => msg.encryption_param = reader |> @lib.read_bytes() |> Some
      (16, _) => msg.schema_version = reader |> @lib.read_bytes() |> Some
      (17, _) =>
        msg.partition_key_b64_encoded = reader |> @lib.read_bool() |> Some
      (18, _) => msg.ordering_key = reader |> @lib.read_bytes() |> Some
      (19, _) => msg.deliver_at_time = reader |> @lib.read_int64() |> Some
      (20, _) => msg.marker_type = reader |> @lib.read_int32() |> Some
      (22, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (23, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (24, _) => msg.highest_sequence_id = reader |> @lib.read_uint64() |> Some
      (25, _) => msg.null_value = reader |> @lib.read_bool() |> Some
      (26, _) => msg.uuid = reader |> @lib.read_string() |> Some
      (27, _) => msg.num_chunks_from_msg = reader |> @lib.read_int32() |> Some
      (28, _) => msg.total_chunk_msg_size = reader |> @lib.read_int32() |> Some
      (29, _) => msg.chunk_id = reader |> @lib.read_int32() |> Some
      (30, _) => msg.null_partition_key = reader |> @lib.read_bool() |> Some
      (31, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.compacted_batch_indexes.push_iter(
          (reader |> @lib.read_packed(@lib.read_int32, None)).iter(),
        )
      (31, @lib.WIRE_TYPE_VARINT) =>
        msg.compacted_batch_indexes.push(reader |> @lib.read_int32())
      (32, _) => msg.schema_id = reader |> @lib.read_bytes() |> Some
      (33, _) => msg.entry_hash_min = reader |> @lib.read_uint32() |> Some
      (34, _) => msg.entry_hash_max = reader |> @lib.read_uint32() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for MessageMetadata with fn write(
  self : MessageMetadata,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.producer_name)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.sequence_id)
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_uint64(self.publish_time)
  for item in self.properties {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(item)
  }
  if self.replicated_from is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  if self.partition_key is Some(v) {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_string(v)
  }
  for item in self.replicate_to {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_string(item)
  }
  if self.compression is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.uncompressed_size is Some(v) {
    writer |> @lib.write_varint(72UL)
    writer |> @lib.write_uint32(v)
  }
  if self.num_messages_in_batch is Some(v) {
    writer |> @lib.write_varint(88UL)
    writer |> @lib.write_int32(v)
  }
  if self.event_time is Some(v) {
    writer |> @lib.write_varint(96UL)
    writer |> @lib.write_uint64(v)
  }
  for item in self.encryption_keys {
    writer |> @lib.write_varint(106UL)
    writer |> @lib.write_message(item)
  }
  if self.encryption_algo is Some(v) {
    writer |> @lib.write_varint(114UL)
    writer |> @lib.write_string(v)
  }
  if self.encryption_param is Some(v) {
    writer |> @lib.write_varint(122UL)
    writer |> @lib.write_bytes(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.write_varint(130UL)
    writer |> @lib.write_bytes(v)
  }
  if self.partition_key_b64_encoded is Some(v) {
    writer |> @lib.write_varint(136UL)
    writer |> @lib.write_bool(v)
  }
  if self.ordering_key is Some(v) {
    writer |> @lib.write_varint(146UL)
    writer |> @lib.write_bytes(v)
  }
  if self.deliver_at_time is Some(v) {
    writer |> @lib.write_varint(152UL)
    writer |> @lib.write_int64(v)
  }
  if self.marker_type is Some(v) {
    writer |> @lib.write_varint(160UL)
    writer |> @lib.write_int32(v)
  }
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(176UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(184UL)
    writer |> @lib.write_uint64(v)
  }
  if self.highest_sequence_id is Some(v) {
    writer |> @lib.write_varint(192UL)
    writer |> @lib.write_uint64(v)
  }
  if self.null_value is Some(v) {
    writer |> @lib.write_varint(200UL)
    writer |> @lib.write_bool(v)
  }
  if self.uuid is Some(v) {
    writer |> @lib.write_varint(210UL)
    writer |> @lib.write_string(v)
  }
  if self.num_chunks_from_msg is Some(v) {
    writer |> @lib.write_varint(216UL)
    writer |> @lib.write_int32(v)
  }
  if self.total_chunk_msg_size is Some(v) {
    writer |> @lib.write_varint(224UL)
    writer |> @lib.write_int32(v)
  }
  if self.chunk_id is Some(v) {
    writer |> @lib.write_varint(232UL)
    writer |> @lib.write_int32(v)
  }
  if self.null_partition_key is Some(v) {
    writer |> @lib.write_varint(240UL)
    writer |> @lib.write_bool(v)
  }
  for item in self.compacted_batch_indexes {
    writer |> @lib.write_varint(248UL)
    writer |> @lib.write_int32(item)
  }
  if self.schema_id is Some(v) {
    writer |> @lib.write_varint(258UL)
    writer |> @lib.write_bytes(v)
  }
  if self.entry_hash_min is Some(v) {
    writer |> @lib.write_varint(264UL)
    writer |> @lib.write_uint32(v)
  }
  if self.entry_hash_max is Some(v) {
    writer |> @lib.write_varint(272UL)
    writer |> @lib.write_uint32(v)
  }
}

///|
pub impl ToJson for MessageMetadata with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.producer_name != Default::default() {
    json["producerName"] = ToJson::to_json(self.producer_name)
  }
  if self.sequence_id != Default::default() {
    json["sequenceId"] = ToJson::to_json(self.sequence_id)
  }
  if self.publish_time != Default::default() {
    json["publishTime"] = ToJson::to_json(self.publish_time)
  }
  if self.properties != Default::default() {
    json["properties"] = ToJson::to_json(self.properties)
  }
  match self.replicated_from {
    Some(v) => json["replicatedFrom"] = ToJson::to_json(v)
    _ => ()
  }
  match self.partition_key {
    Some(v) => json["partitionKey"] = ToJson::to_json(v)
    _ => ()
  }
  if self.replicate_to != Default::default() {
    json["replicateTo"] = ToJson::to_json(self.replicate_to)
  }
  match self.compression {
    Some(v) if v != CompressionType::NONE =>
      json["compression"] = ToJson::to_json(v)
    _ => ()
  }
  match self.uncompressed_size {
    Some(v) if v != 0 => json["uncompressedSize"] = ToJson::to_json(v)
    _ => ()
  }
  match self.num_messages_in_batch {
    Some(v) if v != 1 => json["numMessagesInBatch"] = ToJson::to_json(v)
    _ => ()
  }
  match self.event_time {
    Some(v) if v != 0 => json["eventTime"] = ToJson::to_json(v)
    _ => ()
  }
  if self.encryption_keys != Default::default() {
    json["encryptionKeys"] = ToJson::to_json(self.encryption_keys)
  }
  match self.encryption_algo {
    Some(v) => json["encryptionAlgo"] = ToJson::to_json(v)
    _ => ()
  }
  match self.encryption_param {
    Some(v) => json["encryptionParam"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.schema_version {
    Some(v) => json["schemaVersion"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.partition_key_b64_encoded {
    Some(v) if v != false => json["partitionKeyB64Encoded"] = ToJson::to_json(v)
    _ => ()
  }
  match self.ordering_key {
    Some(v) => json["orderingKey"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.deliver_at_time {
    Some(v) => json["deliverAtTime"] = ToJson::to_json(v)
    _ => ()
  }
  match self.marker_type {
    Some(v) => json["markerType"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_least_bits {
    Some(v) => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.highest_sequence_id {
    Some(v) if v != 0 => json["highestSequenceId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.null_value {
    Some(v) if v != false => json["nullValue"] = ToJson::to_json(v)
    _ => ()
  }
  match self.uuid {
    Some(v) => json["uuid"] = ToJson::to_json(v)
    _ => ()
  }
  match self.num_chunks_from_msg {
    Some(v) => json["numChunksFromMsg"] = ToJson::to_json(v)
    _ => ()
  }
  match self.total_chunk_msg_size {
    Some(v) => json["totalChunkMsgSize"] = ToJson::to_json(v)
    _ => ()
  }
  match self.chunk_id {
    Some(v) => json["chunkId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.null_partition_key {
    Some(v) if v != false => json["nullPartitionKey"] = ToJson::to_json(v)
    _ => ()
  }
  if self.compacted_batch_indexes != Default::default() {
    json["compactedBatchIndexes"] = ToJson::to_json(
      self.compacted_batch_indexes,
    )
  }
  match self.schema_id {
    Some(v) => json["schemaId"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.entry_hash_min {
    Some(v) => json["entryHashMin"] = ToJson::to_json(v)
    _ => ()
  }
  match self.entry_hash_max {
    Some(v) => json["entryHashMax"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for MessageMetadata with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> MessageMetadata raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for MessageMetadata"),
    )
  }
  let message : MessageMetadata = Default::default()
  for key, value in obj {
    match (key, value) {
      ("producerName", value) =>
        message.producer_name = @json.from_json(value, path~)
      ("sequenceId", value) =>
        message.sequence_id = @json.from_json(value, path~)
      ("publishTime", value) =>
        message.publish_time = @json.from_json(value, path~)
      ("properties", Array(value)) =>
        message.properties = value.map(v => @json.from_json(v, path~))
      ("replicatedFrom", value) =>
        message.replicated_from = Some(@json.from_json(value, path~))
      ("partitionKey", value) =>
        message.partition_key = Some(@json.from_json(value, path~))
      ("replicateTo", Array(value)) =>
        message.replicate_to = value.map(v => @json.from_json(v, path~))
      ("compression", value) =>
        message.compression = Some(@json.from_json(value, path~))
      ("uncompressedSize", value) =>
        message.uncompressed_size = Some(@json.from_json(value, path~))
      ("numMessagesInBatch", value) =>
        message.num_messages_in_batch = Some(@json.from_json(value, path~))
      ("eventTime", value) =>
        message.event_time = Some(@json.from_json(value, path~))
      ("encryptionKeys", Array(value)) =>
        message.encryption_keys = value.map(v => @json.from_json(v, path~))
      ("encryptionAlgo", value) =>
        message.encryption_algo = Some(@json.from_json(value, path~))
      ("encryptionParam", String(value)) =>
        message.encryption_param = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("schemaVersion", String(value)) =>
        message.schema_version = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("partitionKeyB64Encoded", value) =>
        message.partition_key_b64_encoded = Some(@json.from_json(value, path~))
      ("orderingKey", String(value)) =>
        message.ordering_key = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("deliverAtTime", value) =>
        message.deliver_at_time = Some(@json.from_json(value, path~))
      ("markerType", value) =>
        message.marker_type = Some(@json.from_json(value, path~))
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("highestSequenceId", value) =>
        message.highest_sequence_id = Some(@json.from_json(value, path~))
      ("nullValue", value) =>
        message.null_value = Some(@json.from_json(value, path~))
      ("uuid", value) => message.uuid = Some(@json.from_json(value, path~))
      ("numChunksFromMsg", value) =>
        message.num_chunks_from_msg = Some(@json.from_json(value, path~))
      ("totalChunkMsgSize", value) =>
        message.total_chunk_msg_size = Some(@json.from_json(value, path~))
      ("chunkId", value) =>
        message.chunk_id = Some(@json.from_json(value, path~))
      ("nullPartitionKey", value) =>
        message.null_partition_key = Some(@json.from_json(value, path~))
      ("compactedBatchIndexes", Array(value)) =>
        message.compacted_batch_indexes = value.map(v => {
          @json.from_json(v, path~)
        })
      ("schemaId", String(value)) =>
        message.schema_id = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("entryHashMin", value) =>
        message.entry_hash_min = Some(@json.from_json(value, path~))
      ("entryHashMax", value) =>
        message.entry_hash_max = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for MessageMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> MessageMetadata raise {
  let msg : MessageMetadata = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_name = reader |> @lib.async_read_string()
      (2, _) => msg.sequence_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.publish_time = reader |> @lib.async_read_uint64()
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.async_read_message() : KeyValue))
      (5, _) => msg.replicated_from = reader |> @lib.async_read_string() |> Some
      (6, _) => msg.partition_key = reader |> @lib.async_read_string() |> Some
      (7, _) => msg.replicate_to.push(reader |> @lib.async_read_string())
      (8, _) =>
        msg.compression = reader
          |> @lib.async_read_enum()
          |> CompressionType::from_enum
          |> Some
      (9, _) =>
        msg.uncompressed_size = reader |> @lib.async_read_uint32() |> Some
      (11, _) =>
        msg.num_messages_in_batch = reader |> @lib.async_read_int32() |> Some
      (12, _) => msg.event_time = reader |> @lib.async_read_uint64() |> Some
      (13, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.encryption_keys.push(
          (reader |> @lib.async_read_message() : EncryptionKeys),
        )
      (14, _) =>
        msg.encryption_algo = reader |> @lib.async_read_string() |> Some
      (15, _) =>
        msg.encryption_param = reader |> @lib.async_read_bytes() |> Some
      (16, _) => msg.schema_version = reader |> @lib.async_read_bytes() |> Some
      (17, _) =>
        msg.partition_key_b64_encoded = reader |> @lib.async_read_bool() |> Some
      (18, _) => msg.ordering_key = reader |> @lib.async_read_bytes() |> Some
      (19, _) => msg.deliver_at_time = reader |> @lib.async_read_int64() |> Some
      (20, _) => msg.marker_type = reader |> @lib.async_read_int32() |> Some
      (22, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (23, _) =>
        msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (24, _) =>
        msg.highest_sequence_id = reader |> @lib.async_read_uint64() |> Some
      (25, _) => msg.null_value = reader |> @lib.async_read_bool() |> Some
      (26, _) => msg.uuid = reader |> @lib.async_read_string() |> Some
      (27, _) =>
        msg.num_chunks_from_msg = reader |> @lib.async_read_int32() |> Some
      (28, _) =>
        msg.total_chunk_msg_size = reader |> @lib.async_read_int32() |> Some
      (29, _) => msg.chunk_id = reader |> @lib.async_read_int32() |> Some
      (30, _) =>
        msg.null_partition_key = reader |> @lib.async_read_bool() |> Some
      (31, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.compacted_batch_indexes.push_iter(
          (reader |> @lib.async_read_packed(@lib.async_read_int32, None)).iter(),
        )
      (31, @lib.WIRE_TYPE_VARINT) =>
        msg.compacted_batch_indexes.push(reader |> @lib.async_read_int32())
      (32, _) => msg.schema_id = reader |> @lib.async_read_bytes() |> Some
      (33, _) => msg.entry_hash_min = reader |> @lib.async_read_uint32() |> Some
      (34, _) => msg.entry_hash_max = reader |> @lib.async_read_uint32() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for MessageMetadata with fn write(
  self : MessageMetadata,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.producer_name)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.sequence_id)
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_uint64(self.publish_time)
  for item in self.properties {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(item)
  }
  if self.replicated_from is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  if self.partition_key is Some(v) {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_string(v)
  }
  for item in self.replicate_to {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_string(item)
  }
  if self.compression is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.uncompressed_size is Some(v) {
    writer |> @lib.async_write_varint(72UL)
    writer |> @lib.async_write_uint32(v)
  }
  if self.num_messages_in_batch is Some(v) {
    writer |> @lib.async_write_varint(88UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.event_time is Some(v) {
    writer |> @lib.async_write_varint(96UL)
    writer |> @lib.async_write_uint64(v)
  }
  for item in self.encryption_keys {
    writer |> @lib.async_write_varint(106UL)
    writer |> @lib.async_write_message(item)
  }
  if self.encryption_algo is Some(v) {
    writer |> @lib.async_write_varint(114UL)
    writer |> @lib.async_write_string(v)
  }
  if self.encryption_param is Some(v) {
    writer |> @lib.async_write_varint(122UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.async_write_varint(130UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.partition_key_b64_encoded is Some(v) {
    writer |> @lib.async_write_varint(136UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.ordering_key is Some(v) {
    writer |> @lib.async_write_varint(146UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.deliver_at_time is Some(v) {
    writer |> @lib.async_write_varint(152UL)
    writer |> @lib.async_write_int64(v)
  }
  if self.marker_type is Some(v) {
    writer |> @lib.async_write_varint(160UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(176UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(184UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.highest_sequence_id is Some(v) {
    writer |> @lib.async_write_varint(192UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.null_value is Some(v) {
    writer |> @lib.async_write_varint(200UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.uuid is Some(v) {
    writer |> @lib.async_write_varint(210UL)
    writer |> @lib.async_write_string(v)
  }
  if self.num_chunks_from_msg is Some(v) {
    writer |> @lib.async_write_varint(216UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.total_chunk_msg_size is Some(v) {
    writer |> @lib.async_write_varint(224UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.chunk_id is Some(v) {
    writer |> @lib.async_write_varint(232UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.null_partition_key is Some(v) {
    writer |> @lib.async_write_varint(240UL)
    writer |> @lib.async_write_bool(v)
  }
  for item in self.compacted_batch_indexes {
    writer |> @lib.async_write_varint(248UL)
    writer |> @lib.async_write_int32(item)
  }
  if self.schema_id is Some(v) {
    writer |> @lib.async_write_varint(258UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.entry_hash_min is Some(v) {
    writer |> @lib.async_write_varint(264UL)
    writer |> @lib.async_write_uint32(v)
  }
  if self.entry_hash_max is Some(v) {
    writer |> @lib.async_write_varint(272UL)
    writer |> @lib.async_write_uint32(v)
  }
}

///|
pub(all) struct SingleMessageMetadata {
  mut properties : Array[KeyValue]
  mut partition_key : String?
  mut payload_size : Int
  mut compacted_out : Bool?
  mut event_time : UInt64?
  mut partition_key_b64_encoded : Bool?
  mut ordering_key : Bytes?
  mut sequence_id : UInt64?
  mut null_value : Bool?
  mut null_partition_key : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for SingleMessageMetadata with fn size_of(self) {
  let mut size = 0U
  for s in self.properties {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(s))
  }
  if self.partition_key is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  size += @lib.size_of_field(3U, @lib.size_of(self.payload_size))
  if self.compacted_out is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.event_time is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.partition_key_b64_encoded is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  if self.ordering_key is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  if self.sequence_id is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.null_value is Some(v) {
    size += @lib.size_of_field(9U, @lib.size_of(v))
  }
  if self.null_partition_key is Some(v) {
    size += @lib.size_of_field(10U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for SingleMessageMetadata with fn default() -> SingleMessageMetadata {
  SingleMessageMetadata::{
    properties: [],
    partition_key: None,
    payload_size: Default::default(),
    compacted_out: Some(false),
    event_time: Some(0),
    partition_key_b64_encoded: Some(false),
    ordering_key: None,
    sequence_id: None,
    null_value: Some(false),
    null_partition_key: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn SingleMessageMetadata::SingleMessageMetadata(
  properties : Array[KeyValue],
  partition_key? : String,
  payload_size : Int,
  compacted_out? : Bool,
  event_time? : UInt64,
  partition_key_b64_encoded? : Bool,
  ordering_key? : Bytes,
  sequence_id? : UInt64,
  null_value? : Bool,
  null_partition_key? : Bool,
) -> SingleMessageMetadata {
  SingleMessageMetadata::{
    properties,
    partition_key,
    payload_size,
    compacted_out,
    event_time,
    partition_key_b64_encoded,
    ordering_key,
    sequence_id,
    null_value,
    null_partition_key,
  }
}

///|
pub impl @lib.Read for SingleMessageMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> SingleMessageMetadata raise {
  let msg : SingleMessageMetadata = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.read_message() : KeyValue))
      (2, _) => msg.partition_key = reader |> @lib.read_string() |> Some
      (3, _) => msg.payload_size = reader |> @lib.read_int32()
      (4, _) => msg.compacted_out = reader |> @lib.read_bool() |> Some
      (5, _) => msg.event_time = reader |> @lib.read_uint64() |> Some
      (6, _) =>
        msg.partition_key_b64_encoded = reader |> @lib.read_bool() |> Some
      (7, _) => msg.ordering_key = reader |> @lib.read_bytes() |> Some
      (8, _) => msg.sequence_id = reader |> @lib.read_uint64() |> Some
      (9, _) => msg.null_value = reader |> @lib.read_bool() |> Some
      (10, _) => msg.null_partition_key = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for SingleMessageMetadata with fn write(
  self : SingleMessageMetadata,
  writer : &@lib.Writer,
) -> Unit raise {
  for item in self.properties {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_message(item)
  }
  if self.partition_key is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_string(v)
  }
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_int32(self.payload_size)
  if self.compacted_out is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_bool(v)
  }
  if self.event_time is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_uint64(v)
  }
  if self.partition_key_b64_encoded is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_bool(v)
  }
  if self.ordering_key is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_bytes(v)
  }
  if self.sequence_id is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_uint64(v)
  }
  if self.null_value is Some(v) {
    writer |> @lib.write_varint(72UL)
    writer |> @lib.write_bool(v)
  }
  if self.null_partition_key is Some(v) {
    writer |> @lib.write_varint(80UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for SingleMessageMetadata with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.properties != Default::default() {
    json["properties"] = ToJson::to_json(self.properties)
  }
  match self.partition_key {
    Some(v) => json["partitionKey"] = ToJson::to_json(v)
    _ => ()
  }
  if self.payload_size != Default::default() {
    json["payloadSize"] = ToJson::to_json(self.payload_size)
  }
  match self.compacted_out {
    Some(v) if v != false => json["compactedOut"] = ToJson::to_json(v)
    _ => ()
  }
  match self.event_time {
    Some(v) if v != 0 => json["eventTime"] = ToJson::to_json(v)
    _ => ()
  }
  match self.partition_key_b64_encoded {
    Some(v) if v != false => json["partitionKeyB64Encoded"] = ToJson::to_json(v)
    _ => ()
  }
  match self.ordering_key {
    Some(v) => json["orderingKey"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.sequence_id {
    Some(v) => json["sequenceId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.null_value {
    Some(v) if v != false => json["nullValue"] = ToJson::to_json(v)
    _ => ()
  }
  match self.null_partition_key {
    Some(v) if v != false => json["nullPartitionKey"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for SingleMessageMetadata with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> SingleMessageMetadata raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for SingleMessageMetadata"),
    )
  }
  let message : SingleMessageMetadata = Default::default()
  for key, value in obj {
    match (key, value) {
      ("properties", Array(value)) =>
        message.properties = value.map(v => @json.from_json(v, path~))
      ("partitionKey", value) =>
        message.partition_key = Some(@json.from_json(value, path~))
      ("payloadSize", value) =>
        message.payload_size = @json.from_json(value, path~)
      ("compactedOut", value) =>
        message.compacted_out = Some(@json.from_json(value, path~))
      ("eventTime", value) =>
        message.event_time = Some(@json.from_json(value, path~))
      ("partitionKeyB64Encoded", value) =>
        message.partition_key_b64_encoded = Some(@json.from_json(value, path~))
      ("orderingKey", String(value)) =>
        message.ordering_key = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("sequenceId", value) =>
        message.sequence_id = Some(@json.from_json(value, path~))
      ("nullValue", value) =>
        message.null_value = Some(@json.from_json(value, path~))
      ("nullPartitionKey", value) =>
        message.null_partition_key = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for SingleMessageMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> SingleMessageMetadata raise {
  let msg : SingleMessageMetadata = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.async_read_message() : KeyValue))
      (2, _) => msg.partition_key = reader |> @lib.async_read_string() |> Some
      (3, _) => msg.payload_size = reader |> @lib.async_read_int32()
      (4, _) => msg.compacted_out = reader |> @lib.async_read_bool() |> Some
      (5, _) => msg.event_time = reader |> @lib.async_read_uint64() |> Some
      (6, _) =>
        msg.partition_key_b64_encoded = reader |> @lib.async_read_bool() |> Some
      (7, _) => msg.ordering_key = reader |> @lib.async_read_bytes() |> Some
      (8, _) => msg.sequence_id = reader |> @lib.async_read_uint64() |> Some
      (9, _) => msg.null_value = reader |> @lib.async_read_bool() |> Some
      (10, _) =>
        msg.null_partition_key = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for SingleMessageMetadata with fn write(
  self : SingleMessageMetadata,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  for item in self.properties {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_message(item)
  }
  if self.partition_key is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_string(v)
  }
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_int32(self.payload_size)
  if self.compacted_out is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.event_time is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.partition_key_b64_encoded is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.ordering_key is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.sequence_id is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.null_value is Some(v) {
    writer |> @lib.async_write_varint(72UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.null_partition_key is Some(v) {
    writer |> @lib.async_write_varint(80UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct BrokerEntryMetadata {
  mut broker_timestamp : UInt64?
  mut index : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for BrokerEntryMetadata with fn size_of(self) {
  let mut size = 0U
  if self.broker_timestamp is Some(v) {
    size += @lib.size_of_field(1U, @lib.size_of(v))
  }
  if self.index is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for BrokerEntryMetadata with fn default() -> BrokerEntryMetadata {
  BrokerEntryMetadata::{ broker_timestamp: None, index: None, }
}

///|
#alias(new, deprecated)
pub fn BrokerEntryMetadata::BrokerEntryMetadata(
  broker_timestamp? : UInt64,
  index? : UInt64,
) -> BrokerEntryMetadata {
  BrokerEntryMetadata::{ broker_timestamp, index, }
}

///|
pub impl @lib.Read for BrokerEntryMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> BrokerEntryMetadata raise {
  let msg : BrokerEntryMetadata = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.broker_timestamp = reader |> @lib.read_uint64() |> Some
      (2, _) => msg.index = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for BrokerEntryMetadata with fn write(
  self : BrokerEntryMetadata,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.broker_timestamp is Some(v) {
    writer |> @lib.write_varint(8UL)
    writer |> @lib.write_uint64(v)
  }
  if self.index is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for BrokerEntryMetadata with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  match self.broker_timestamp {
    Some(v) => json["brokerTimestamp"] = ToJson::to_json(v)
    _ => ()
  }
  match self.index {
    Some(v) => json["index"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for BrokerEntryMetadata with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> BrokerEntryMetadata raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for BrokerEntryMetadata"),
    )
  }
  let message : BrokerEntryMetadata = Default::default()
  for key, value in obj {
    match (key, value) {
      ("brokerTimestamp", value) =>
        message.broker_timestamp = Some(@json.from_json(value, path~))
      ("index", value) => message.index = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for BrokerEntryMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> BrokerEntryMetadata raise {
  let msg : BrokerEntryMetadata = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.broker_timestamp = reader |> @lib.async_read_uint64() |> Some
      (2, _) => msg.index = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for BrokerEntryMetadata with fn write(
  self : BrokerEntryMetadata,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.broker_timestamp is Some(v) {
    writer |> @lib.async_write_varint(8UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.index is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandConnect {
  mut client_version : String
  mut auth_method : AuthMethod?
  mut auth_method_name : String?
  mut auth_data : Bytes?
  mut protocol_version : Int?
  mut proxy_to_broker_url : String?
  mut original_principal : String?
  mut original_auth_data : String?
  mut original_auth_method : String?
  mut feature_flags : FeatureFlags?
  mut proxy_version : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandConnect with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(
    1U,
    @lib.size_of(self.client_version),
  )
  if self.auth_method is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.auth_method_name is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  if self.auth_data is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.protocol_version is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.proxy_to_broker_url is Some(v) {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(v))
  }
  if self.original_principal is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  if self.original_auth_data is Some(v) {
    size += @lib.size_of_length_delimited_field(8U, @lib.size_of(v))
  }
  if self.original_auth_method is Some(v) {
    size += @lib.size_of_length_delimited_field(9U, @lib.size_of(v))
  }
  if self.feature_flags is Some(v) {
    size += @lib.size_of_length_delimited_field(10U, @lib.size_of(v))
  }
  if self.proxy_version is Some(v) {
    size += @lib.size_of_length_delimited_field(11U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandConnect with fn default() -> CommandConnect {
  CommandConnect::{
    client_version: Default::default(),
    auth_method: None,
    auth_method_name: None,
    auth_data: None,
    protocol_version: Some(0),
    proxy_to_broker_url: None,
    original_principal: None,
    original_auth_data: None,
    original_auth_method: None,
    feature_flags: None,
    proxy_version: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandConnect::CommandConnect(
  client_version : String,
  auth_method? : AuthMethod,
  auth_method_name? : String,
  auth_data? : Bytes,
  protocol_version? : Int,
  proxy_to_broker_url? : String,
  original_principal? : String,
  original_auth_data? : String,
  original_auth_method? : String,
  feature_flags? : FeatureFlags,
  proxy_version? : String,
) -> CommandConnect {
  CommandConnect::{
    client_version,
    auth_method,
    auth_method_name,
    auth_data,
    protocol_version,
    proxy_to_broker_url,
    original_principal,
    original_auth_data,
    original_auth_method,
    feature_flags,
    proxy_version,
  }
}

///|
pub impl @lib.Read for CommandConnect with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandConnect raise {
  let msg : CommandConnect = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.client_version = reader |> @lib.read_string()
      (2, _) =>
        msg.auth_method = reader
          |> @lib.read_enum()
          |> AuthMethod::from_enum
          |> Some
      (5, _) => msg.auth_method_name = reader |> @lib.read_string() |> Some
      (3, _) => msg.auth_data = reader |> @lib.read_bytes() |> Some
      (4, _) => msg.protocol_version = reader |> @lib.read_int32() |> Some
      (6, _) => msg.proxy_to_broker_url = reader |> @lib.read_string() |> Some
      (7, _) => msg.original_principal = reader |> @lib.read_string() |> Some
      (8, _) => msg.original_auth_data = reader |> @lib.read_string() |> Some
      (9, _) => msg.original_auth_method = reader |> @lib.read_string() |> Some
      (10, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.feature_flags = (reader |> @lib.read_message() : FeatureFlags)
          |> Some
      (11, _) => msg.proxy_version = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandConnect with fn write(
  self : CommandConnect,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.client_version)
  if self.auth_method is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.auth_method_name is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  if self.auth_data is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_bytes(v)
  }
  if self.protocol_version is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_int32(v)
  }
  if self.proxy_to_broker_url is Some(v) {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_string(v)
  }
  if self.original_principal is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_string(v)
  }
  if self.original_auth_data is Some(v) {
    writer |> @lib.write_varint(66UL)
    writer |> @lib.write_string(v)
  }
  if self.original_auth_method is Some(v) {
    writer |> @lib.write_varint(74UL)
    writer |> @lib.write_string(v)
  }
  if self.feature_flags is Some(v) {
    writer |> @lib.write_varint(82UL)
    writer |> @lib.write_message(v)
  }
  if self.proxy_version is Some(v) {
    writer |> @lib.write_varint(90UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandConnect with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.client_version != Default::default() {
    json["clientVersion"] = ToJson::to_json(self.client_version)
  }
  match self.auth_method {
    Some(v) => json["authMethod"] = ToJson::to_json(v)
    _ => ()
  }
  match self.auth_method_name {
    Some(v) => json["authMethodName"] = ToJson::to_json(v)
    _ => ()
  }
  match self.auth_data {
    Some(v) => json["authData"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.protocol_version {
    Some(v) if v != 0 => json["protocolVersion"] = ToJson::to_json(v)
    _ => ()
  }
  match self.proxy_to_broker_url {
    Some(v) => json["proxyToBrokerUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_principal {
    Some(v) => json["originalPrincipal"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_auth_data {
    Some(v) => json["originalAuthData"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_auth_method {
    Some(v) => json["originalAuthMethod"] = ToJson::to_json(v)
    _ => ()
  }
  match self.feature_flags {
    Some(v) => json["featureFlags"] = ToJson::to_json(v)
    _ => ()
  }
  match self.proxy_version {
    Some(v) => json["proxyVersion"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandConnect with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandConnect raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandConnect"))
  }
  let message : CommandConnect = Default::default()
  for key, value in obj {
    match (key, value) {
      ("clientVersion", value) =>
        message.client_version = @json.from_json(value, path~)
      ("authMethod", value) =>
        message.auth_method = Some(@json.from_json(value, path~))
      ("authMethodName", value) =>
        message.auth_method_name = Some(@json.from_json(value, path~))
      ("authData", String(value)) =>
        message.auth_data = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("protocolVersion", value) =>
        message.protocol_version = Some(@json.from_json(value, path~))
      ("proxyToBrokerUrl", value) =>
        message.proxy_to_broker_url = Some(@json.from_json(value, path~))
      ("originalPrincipal", value) =>
        message.original_principal = Some(@json.from_json(value, path~))
      ("originalAuthData", value) =>
        message.original_auth_data = Some(@json.from_json(value, path~))
      ("originalAuthMethod", value) =>
        message.original_auth_method = Some(@json.from_json(value, path~))
      ("featureFlags", value) =>
        message.feature_flags = Some(@json.from_json(value, path~))
      ("proxyVersion", value) =>
        message.proxy_version = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandConnect with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandConnect raise {
  let msg : CommandConnect = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.client_version = reader |> @lib.async_read_string()
      (2, _) =>
        msg.auth_method = reader
          |> @lib.async_read_enum()
          |> AuthMethod::from_enum
          |> Some
      (5, _) =>
        msg.auth_method_name = reader |> @lib.async_read_string() |> Some
      (3, _) => msg.auth_data = reader |> @lib.async_read_bytes() |> Some
      (4, _) => msg.protocol_version = reader |> @lib.async_read_int32() |> Some
      (6, _) =>
        msg.proxy_to_broker_url = reader |> @lib.async_read_string() |> Some
      (7, _) =>
        msg.original_principal = reader |> @lib.async_read_string() |> Some
      (8, _) =>
        msg.original_auth_data = reader |> @lib.async_read_string() |> Some
      (9, _) =>
        msg.original_auth_method = reader |> @lib.async_read_string() |> Some
      (10, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.feature_flags = (reader |> @lib.async_read_message() : FeatureFlags)
          |> Some
      (11, _) => msg.proxy_version = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandConnect with fn write(
  self : CommandConnect,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.client_version)
  if self.auth_method is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.auth_method_name is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  if self.auth_data is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.protocol_version is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.proxy_to_broker_url is Some(v) {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_principal is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_auth_data is Some(v) {
    writer |> @lib.async_write_varint(66UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_auth_method is Some(v) {
    writer |> @lib.async_write_varint(74UL)
    writer |> @lib.async_write_string(v)
  }
  if self.feature_flags is Some(v) {
    writer |> @lib.async_write_varint(82UL)
    writer |> @lib.async_write_message(v)
  }
  if self.proxy_version is Some(v) {
    writer |> @lib.async_write_varint(90UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct FeatureFlags {
  mut supports_auth_refresh : Bool?
  mut supports_broker_entry_metadata : Bool?
  mut supports_partial_producer : Bool?
  mut supports_topic_watchers : Bool?
  mut supports_get_partitioned_metadata_without_auto_creation : Bool?
  mut supports_repl_dedup_by_lid_and_eid : Bool?
  mut supports_topic_watcher_reconcile : Bool?
  mut supports_scalable_topics : Bool?
  mut supports_tc_metadata_discovery : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for FeatureFlags with fn size_of(self) {
  let mut size = 0U
  if self.supports_auth_refresh is Some(v) {
    size += @lib.size_of_field(1U, @lib.size_of(v))
  }
  if self.supports_broker_entry_metadata is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.supports_partial_producer is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.supports_topic_watchers is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.supports_get_partitioned_metadata_without_auto_creation is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.supports_repl_dedup_by_lid_and_eid is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  if self.supports_topic_watcher_reconcile is Some(v) {
    size += @lib.size_of_field(7U, @lib.size_of(v))
  }
  if self.supports_scalable_topics is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.supports_tc_metadata_discovery is Some(v) {
    size += @lib.size_of_field(9U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for FeatureFlags with fn default() -> FeatureFlags {
  FeatureFlags::{
    supports_auth_refresh: Some(false),
    supports_broker_entry_metadata: Some(false),
    supports_partial_producer: Some(false),
    supports_topic_watchers: Some(false),
    supports_get_partitioned_metadata_without_auto_creation: Some(false),
    supports_repl_dedup_by_lid_and_eid: Some(false),
    supports_topic_watcher_reconcile: Some(false),
    supports_scalable_topics: Some(false),
    supports_tc_metadata_discovery: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn FeatureFlags::FeatureFlags(
  supports_auth_refresh? : Bool,
  supports_broker_entry_metadata? : Bool,
  supports_partial_producer? : Bool,
  supports_topic_watchers? : Bool,
  supports_get_partitioned_metadata_without_auto_creation? : Bool,
  supports_repl_dedup_by_lid_and_eid? : Bool,
  supports_topic_watcher_reconcile? : Bool,
  supports_scalable_topics? : Bool,
  supports_tc_metadata_discovery? : Bool,
) -> FeatureFlags {
  FeatureFlags::{
    supports_auth_refresh,
    supports_broker_entry_metadata,
    supports_partial_producer,
    supports_topic_watchers,
    supports_get_partitioned_metadata_without_auto_creation,
    supports_repl_dedup_by_lid_and_eid,
    supports_topic_watcher_reconcile,
    supports_scalable_topics,
    supports_tc_metadata_discovery,
  }
}

///|
pub impl @lib.Read for FeatureFlags with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> FeatureFlags raise {
  let msg : FeatureFlags = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.supports_auth_refresh = reader |> @lib.read_bool() |> Some
      (2, _) =>
        msg.supports_broker_entry_metadata = reader |> @lib.read_bool() |> Some
      (3, _) =>
        msg.supports_partial_producer = reader |> @lib.read_bool() |> Some
      (4, _) => msg.supports_topic_watchers = reader |> @lib.read_bool() |> Some
      (5, _) =>
        msg.supports_get_partitioned_metadata_without_auto_creation = reader
          |> @lib.read_bool()
          |> Some
      (6, _) =>
        msg.supports_repl_dedup_by_lid_and_eid = reader
          |> @lib.read_bool()
          |> Some
      (7, _) =>
        msg.supports_topic_watcher_reconcile = reader
          |> @lib.read_bool()
          |> Some
      (8, _) =>
        msg.supports_scalable_topics = reader |> @lib.read_bool() |> Some
      (9, _) =>
        msg.supports_tc_metadata_discovery = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for FeatureFlags with fn write(
  self : FeatureFlags,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.supports_auth_refresh is Some(v) {
    writer |> @lib.write_varint(8UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_broker_entry_metadata is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_partial_producer is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_topic_watchers is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_get_partitioned_metadata_without_auto_creation is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_repl_dedup_by_lid_and_eid is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_topic_watcher_reconcile is Some(v) {
    writer |> @lib.write_varint(56UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_scalable_topics is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_bool(v)
  }
  if self.supports_tc_metadata_discovery is Some(v) {
    writer |> @lib.write_varint(72UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for FeatureFlags with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  match self.supports_auth_refresh {
    Some(v) if v != false => json["supportsAuthRefresh"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_broker_entry_metadata {
    Some(v) if v != false =>
      json["supportsBrokerEntryMetadata"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_partial_producer {
    Some(v) if v != false =>
      json["supportsPartialProducer"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_topic_watchers {
    Some(v) if v != false => json["supportsTopicWatchers"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_get_partitioned_metadata_without_auto_creation {
    Some(v) if v != false =>
      json["supportsGetPartitionedMetadataWithoutAutoCreation"] = ToJson::to_json(
        v,
      )
    _ => ()
  }
  match self.supports_repl_dedup_by_lid_and_eid {
    Some(v) if v != false =>
      json["supportsReplDedupByLidAndEid"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_topic_watcher_reconcile {
    Some(v) if v != false =>
      json["supportsTopicWatcherReconcile"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_scalable_topics {
    Some(v) if v != false => json["supportsScalableTopics"] = ToJson::to_json(v)
    _ => ()
  }
  match self.supports_tc_metadata_discovery {
    Some(v) if v != false =>
      json["supportsTcMetadataDiscovery"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for FeatureFlags with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> FeatureFlags raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for FeatureFlags"))
  }
  let message : FeatureFlags = Default::default()
  for key, value in obj {
    match (key, value) {
      ("supportsAuthRefresh", value) =>
        message.supports_auth_refresh = Some(@json.from_json(value, path~))
      ("supportsBrokerEntryMetadata", value) =>
        message.supports_broker_entry_metadata = Some(
          @json.from_json(value, path~),
        )
      ("supportsPartialProducer", value) =>
        message.supports_partial_producer = Some(@json.from_json(value, path~))
      ("supportsTopicWatchers", value) =>
        message.supports_topic_watchers = Some(@json.from_json(value, path~))
      ("supportsGetPartitionedMetadataWithoutAutoCreation", value) =>
        message.supports_get_partitioned_metadata_without_auto_creation = Some(
          @json.from_json(value, path~),
        )
      ("supportsReplDedupByLidAndEid", value) =>
        message.supports_repl_dedup_by_lid_and_eid = Some(
          @json.from_json(value, path~),
        )
      ("supportsTopicWatcherReconcile", value) =>
        message.supports_topic_watcher_reconcile = Some(
          @json.from_json(value, path~),
        )
      ("supportsScalableTopics", value) =>
        message.supports_scalable_topics = Some(@json.from_json(value, path~))
      ("supportsTcMetadataDiscovery", value) =>
        message.supports_tc_metadata_discovery = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for FeatureFlags with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> FeatureFlags raise {
  let msg : FeatureFlags = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.supports_auth_refresh = reader |> @lib.async_read_bool() |> Some
      (2, _) =>
        msg.supports_broker_entry_metadata = reader
          |> @lib.async_read_bool()
          |> Some
      (3, _) =>
        msg.supports_partial_producer = reader |> @lib.async_read_bool() |> Some
      (4, _) =>
        msg.supports_topic_watchers = reader |> @lib.async_read_bool() |> Some
      (5, _) =>
        msg.supports_get_partitioned_metadata_without_auto_creation = reader
          |> @lib.async_read_bool()
          |> Some
      (6, _) =>
        msg.supports_repl_dedup_by_lid_and_eid = reader
          |> @lib.async_read_bool()
          |> Some
      (7, _) =>
        msg.supports_topic_watcher_reconcile = reader
          |> @lib.async_read_bool()
          |> Some
      (8, _) =>
        msg.supports_scalable_topics = reader |> @lib.async_read_bool() |> Some
      (9, _) =>
        msg.supports_tc_metadata_discovery = reader
          |> @lib.async_read_bool()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for FeatureFlags with fn write(
  self : FeatureFlags,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.supports_auth_refresh is Some(v) {
    writer |> @lib.async_write_varint(8UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_broker_entry_metadata is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_partial_producer is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_topic_watchers is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_get_partitioned_metadata_without_auto_creation is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_repl_dedup_by_lid_and_eid is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_topic_watcher_reconcile is Some(v) {
    writer |> @lib.async_write_varint(56UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_scalable_topics is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.supports_tc_metadata_discovery is Some(v) {
    writer |> @lib.async_write_varint(72UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandConnected {
  mut server_version : String
  mut protocol_version : Int?
  mut max_message_size : Int?
  mut feature_flags : FeatureFlags?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandConnected with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(
    1U,
    @lib.size_of(self.server_version),
  )
  if self.protocol_version is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.max_message_size is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.feature_flags is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandConnected with fn default() -> CommandConnected {
  CommandConnected::{
    server_version: Default::default(),
    protocol_version: Some(0),
    max_message_size: None,
    feature_flags: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandConnected::CommandConnected(
  server_version : String,
  protocol_version? : Int,
  max_message_size? : Int,
  feature_flags? : FeatureFlags,
) -> CommandConnected {
  CommandConnected::{
    server_version,
    protocol_version,
    max_message_size,
    feature_flags,
  }
}

///|
pub impl @lib.Read for CommandConnected with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandConnected raise {
  let msg : CommandConnected = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.server_version = reader |> @lib.read_string()
      (2, _) => msg.protocol_version = reader |> @lib.read_int32() |> Some
      (3, _) => msg.max_message_size = reader |> @lib.read_int32() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.feature_flags = (reader |> @lib.read_message() : FeatureFlags)
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandConnected with fn write(
  self : CommandConnected,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.server_version)
  if self.protocol_version is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_int32(v)
  }
  if self.max_message_size is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_int32(v)
  }
  if self.feature_flags is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for CommandConnected with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.server_version != Default::default() {
    json["serverVersion"] = ToJson::to_json(self.server_version)
  }
  match self.protocol_version {
    Some(v) if v != 0 => json["protocolVersion"] = ToJson::to_json(v)
    _ => ()
  }
  match self.max_message_size {
    Some(v) => json["maxMessageSize"] = ToJson::to_json(v)
    _ => ()
  }
  match self.feature_flags {
    Some(v) => json["featureFlags"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandConnected with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandConnected raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandConnected"),
    )
  }
  let message : CommandConnected = Default::default()
  for key, value in obj {
    match (key, value) {
      ("serverVersion", value) =>
        message.server_version = @json.from_json(value, path~)
      ("protocolVersion", value) =>
        message.protocol_version = Some(@json.from_json(value, path~))
      ("maxMessageSize", value) =>
        message.max_message_size = Some(@json.from_json(value, path~))
      ("featureFlags", value) =>
        message.feature_flags = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandConnected with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandConnected raise {
  let msg : CommandConnected = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.server_version = reader |> @lib.async_read_string()
      (2, _) => msg.protocol_version = reader |> @lib.async_read_int32() |> Some
      (3, _) => msg.max_message_size = reader |> @lib.async_read_int32() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.feature_flags = (reader |> @lib.async_read_message() : FeatureFlags)
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandConnected with fn write(
  self : CommandConnected,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.server_version)
  if self.protocol_version is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.max_message_size is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.feature_flags is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(v)
  }
}

///|
pub(all) struct CommandAuthResponse {
  mut client_version : String?
  mut response : AuthData?
  mut protocol_version : Int?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAuthResponse with fn size_of(self) {
  let mut size = 0U
  if self.client_version is Some(v) {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(v))
  }
  if self.response is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.protocol_version is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAuthResponse with fn default() -> CommandAuthResponse {
  CommandAuthResponse::{
    client_version: None,
    response: None,
    protocol_version: Some(0),
  }
}

///|
#alias(new, deprecated)
pub fn CommandAuthResponse::CommandAuthResponse(
  client_version? : String,
  response? : AuthData,
  protocol_version? : Int,
) -> CommandAuthResponse {
  CommandAuthResponse::{ client_version, response, protocol_version, }
}

///|
pub impl @lib.Read for CommandAuthResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAuthResponse raise {
  let msg : CommandAuthResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.client_version = reader |> @lib.read_string() |> Some
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.response = (reader |> @lib.read_message() : AuthData) |> Some
      (3, _) => msg.protocol_version = reader |> @lib.read_int32() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAuthResponse with fn write(
  self : CommandAuthResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.client_version is Some(v) {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_string(v)
  }
  if self.response is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(v)
  }
  if self.protocol_version is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_int32(v)
  }
}

///|
pub impl ToJson for CommandAuthResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  match self.client_version {
    Some(v) => json["clientVersion"] = ToJson::to_json(v)
    _ => ()
  }
  match self.response {
    Some(v) => json["response"] = ToJson::to_json(v)
    _ => ()
  }
  match self.protocol_version {
    Some(v) if v != 0 => json["protocolVersion"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAuthResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAuthResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAuthResponse"),
    )
  }
  let message : CommandAuthResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("clientVersion", value) =>
        message.client_version = Some(@json.from_json(value, path~))
      ("response", value) =>
        message.response = Some(@json.from_json(value, path~))
      ("protocolVersion", value) =>
        message.protocol_version = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAuthResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAuthResponse raise {
  let msg : CommandAuthResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.client_version = reader |> @lib.async_read_string() |> Some
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.response = (reader |> @lib.async_read_message() : AuthData) |> Some
      (3, _) => msg.protocol_version = reader |> @lib.async_read_int32() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAuthResponse with fn write(
  self : CommandAuthResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.client_version is Some(v) {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_string(v)
  }
  if self.response is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(v)
  }
  if self.protocol_version is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_int32(v)
  }
}

///|
pub(all) struct CommandAuthChallenge {
  mut server_version : String?
  mut challenge : AuthData?
  mut protocol_version : Int?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAuthChallenge with fn size_of(self) {
  let mut size = 0U
  if self.server_version is Some(v) {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(v))
  }
  if self.challenge is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.protocol_version is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAuthChallenge with fn default() -> CommandAuthChallenge {
  CommandAuthChallenge::{
    server_version: None,
    challenge: None,
    protocol_version: Some(0),
  }
}

///|
#alias(new, deprecated)
pub fn CommandAuthChallenge::CommandAuthChallenge(
  server_version? : String,
  challenge? : AuthData,
  protocol_version? : Int,
) -> CommandAuthChallenge {
  CommandAuthChallenge::{ server_version, challenge, protocol_version, }
}

///|
pub impl @lib.Read for CommandAuthChallenge with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAuthChallenge raise {
  let msg : CommandAuthChallenge = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.server_version = reader |> @lib.read_string() |> Some
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.challenge = (reader |> @lib.read_message() : AuthData) |> Some
      (3, _) => msg.protocol_version = reader |> @lib.read_int32() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAuthChallenge with fn write(
  self : CommandAuthChallenge,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.server_version is Some(v) {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_string(v)
  }
  if self.challenge is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(v)
  }
  if self.protocol_version is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_int32(v)
  }
}

///|
pub impl ToJson for CommandAuthChallenge with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  match self.server_version {
    Some(v) => json["serverVersion"] = ToJson::to_json(v)
    _ => ()
  }
  match self.challenge {
    Some(v) => json["challenge"] = ToJson::to_json(v)
    _ => ()
  }
  match self.protocol_version {
    Some(v) if v != 0 => json["protocolVersion"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAuthChallenge with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAuthChallenge raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAuthChallenge"),
    )
  }
  let message : CommandAuthChallenge = Default::default()
  for key, value in obj {
    match (key, value) {
      ("serverVersion", value) =>
        message.server_version = Some(@json.from_json(value, path~))
      ("challenge", value) =>
        message.challenge = Some(@json.from_json(value, path~))
      ("protocolVersion", value) =>
        message.protocol_version = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAuthChallenge with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAuthChallenge raise {
  let msg : CommandAuthChallenge = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.server_version = reader |> @lib.async_read_string() |> Some
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.challenge = (reader |> @lib.async_read_message() : AuthData) |> Some
      (3, _) => msg.protocol_version = reader |> @lib.async_read_int32() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAuthChallenge with fn write(
  self : CommandAuthChallenge,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.server_version is Some(v) {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_string(v)
  }
  if self.challenge is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(v)
  }
  if self.protocol_version is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_int32(v)
  }
}

///|
pub(all) struct AuthData {
  mut auth_method_name : String?
  mut auth_data : Bytes?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for AuthData with fn size_of(self) {
  let mut size = 0U
  if self.auth_method_name is Some(v) {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(v))
  }
  if self.auth_data is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for AuthData with fn default() -> AuthData {
  AuthData::{ auth_method_name: None, auth_data: None, }
}

///|
#alias(new, deprecated)
pub fn AuthData::AuthData(
  auth_method_name? : String,
  auth_data? : Bytes,
) -> AuthData {
  AuthData::{ auth_method_name, auth_data, }
}

///|
pub impl @lib.Read for AuthData with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> AuthData raise {
  let msg : AuthData = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.auth_method_name = reader |> @lib.read_string() |> Some
      (2, _) => msg.auth_data = reader |> @lib.read_bytes() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for AuthData with fn write(
  self : AuthData,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.auth_method_name is Some(v) {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_string(v)
  }
  if self.auth_data is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_bytes(v)
  }
}

///|
pub impl ToJson for AuthData with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  match self.auth_method_name {
    Some(v) => json["authMethodName"] = ToJson::to_json(v)
    _ => ()
  }
  match self.auth_data {
    Some(v) => json["authData"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for AuthData with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> AuthData raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for AuthData"))
  }
  let message : AuthData = Default::default()
  for key, value in obj {
    match (key, value) {
      ("authMethodName", value) =>
        message.auth_method_name = Some(@json.from_json(value, path~))
      ("authData", String(value)) =>
        message.auth_data = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for AuthData with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> AuthData raise {
  let msg : AuthData = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.auth_method_name = reader |> @lib.async_read_string() |> Some
      (2, _) => msg.auth_data = reader |> @lib.async_read_bytes() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for AuthData with fn write(
  self : AuthData,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.auth_method_name is Some(v) {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_string(v)
  }
  if self.auth_data is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_bytes(v)
  }
}

///|
pub(all) struct KeySharedMeta {
  mut key_shared_mode : KeySharedMode
  mut hash_ranges : Array[IntRange]
  mut allow_out_of_order_delivery : Bool?
  mut entry_bucket_dispatch : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for KeySharedMeta with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.key_shared_mode))
  for s in self.hash_ranges {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  if self.allow_out_of_order_delivery is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.entry_bucket_dispatch is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for KeySharedMeta with fn default() -> KeySharedMeta {
  KeySharedMeta::{
    key_shared_mode: Default::default(),
    hash_ranges: [],
    allow_out_of_order_delivery: Some(false),
    entry_bucket_dispatch: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn KeySharedMeta::KeySharedMeta(
  key_shared_mode : KeySharedMode,
  hash_ranges : Array[IntRange],
  allow_out_of_order_delivery? : Bool,
  entry_bucket_dispatch? : Bool,
) -> KeySharedMeta {
  KeySharedMeta::{
    key_shared_mode,
    hash_ranges,
    allow_out_of_order_delivery,
    entry_bucket_dispatch,
  }
}

///|
pub impl @lib.Read for KeySharedMeta with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> KeySharedMeta raise {
  let msg : KeySharedMeta = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.key_shared_mode = reader
          |> @lib.read_enum()
          |> KeySharedMode::from_enum
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.hash_ranges.push((reader |> @lib.read_message() : IntRange))
      (4, _) =>
        msg.allow_out_of_order_delivery = reader |> @lib.read_bool() |> Some
      (5, _) => msg.entry_bucket_dispatch = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for KeySharedMeta with fn write(
  self : KeySharedMeta,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_enum(self.key_shared_mode.to_enum())
  for item in self.hash_ranges {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(item)
  }
  if self.allow_out_of_order_delivery is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_bool(v)
  }
  if self.entry_bucket_dispatch is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for KeySharedMeta with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.key_shared_mode != Default::default() {
    json["keySharedMode"] = ToJson::to_json(self.key_shared_mode)
  }
  if self.hash_ranges != Default::default() {
    json["hashRanges"] = ToJson::to_json(self.hash_ranges)
  }
  match self.allow_out_of_order_delivery {
    Some(v) if v != false =>
      json["allowOutOfOrderDelivery"] = ToJson::to_json(v)
    _ => ()
  }
  match self.entry_bucket_dispatch {
    Some(v) if v != false => json["entryBucketDispatch"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for KeySharedMeta with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> KeySharedMeta raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for KeySharedMeta"))
  }
  let message : KeySharedMeta = Default::default()
  for key, value in obj {
    match (key, value) {
      ("keySharedMode", value) =>
        message.key_shared_mode = @json.from_json(value, path~)
      ("hashRanges", Array(value)) =>
        message.hash_ranges = value.map(v => @json.from_json(v, path~))
      ("allowOutOfOrderDelivery", value) =>
        message.allow_out_of_order_delivery = Some(
          @json.from_json(value, path~),
        )
      ("entryBucketDispatch", value) =>
        message.entry_bucket_dispatch = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for KeySharedMeta with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> KeySharedMeta raise {
  let msg : KeySharedMeta = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.key_shared_mode = reader
          |> @lib.async_read_enum()
          |> KeySharedMode::from_enum
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.hash_ranges.push((reader |> @lib.async_read_message() : IntRange))
      (4, _) =>
        msg.allow_out_of_order_delivery = reader
          |> @lib.async_read_bool()
          |> Some
      (5, _) =>
        msg.entry_bucket_dispatch = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for KeySharedMeta with fn write(
  self : KeySharedMeta,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_enum(self.key_shared_mode.to_enum())
  for item in self.hash_ranges {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(item)
  }
  if self.allow_out_of_order_delivery is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.entry_bucket_dispatch is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) enum CommandSubscribe_SubType {
  Exclusive
  Shared
  Failover
  Key_Shared
} derive(Eq, Debug)

///|
pub fn CommandSubscribe_SubType::to_enum(
  self : CommandSubscribe_SubType,
) -> @lib.Enum {
  match self {
    CommandSubscribe_SubType::Exclusive => 0
    CommandSubscribe_SubType::Shared => 1
    CommandSubscribe_SubType::Failover => 2
    CommandSubscribe_SubType::Key_Shared => 3
  }
}

///|
pub fn CommandSubscribe_SubType::from_enum(
  i : @lib.Enum,
) -> CommandSubscribe_SubType {
  match i.0 {
    0 => CommandSubscribe_SubType::Exclusive
    1 => CommandSubscribe_SubType::Shared
    2 => CommandSubscribe_SubType::Failover
    3 => CommandSubscribe_SubType::Key_Shared
    _ => Default::default()
  }
}

///|
pub impl Default for CommandSubscribe_SubType with fn default() -> CommandSubscribe_SubType {
  CommandSubscribe_SubType::Exclusive
}

///|
pub impl @lib.Sized for CommandSubscribe_SubType with fn size_of(
  self : CommandSubscribe_SubType,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandSubscribe_SubType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSubscribe_SubType raise {
  match json {
    String("Exclusive") => CommandSubscribe_SubType::Exclusive
    String("Shared") => CommandSubscribe_SubType::Shared
    String("Failover") => CommandSubscribe_SubType::Failover
    String("Key_Shared") => CommandSubscribe_SubType::Key_Shared
    Number(0, ..) => CommandSubscribe_SubType::Exclusive
    Number(1, ..) => CommandSubscribe_SubType::Shared
    Number(2, ..) => CommandSubscribe_SubType::Failover
    Number(3, ..) => CommandSubscribe_SubType::Key_Shared
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandSubscribe_SubType with fn to_json(
  self : CommandSubscribe_SubType,
) -> Json {
  match self {
    CommandSubscribe_SubType::Exclusive => "Exclusive"
    CommandSubscribe_SubType::Shared => "Shared"
    CommandSubscribe_SubType::Failover => "Failover"
    CommandSubscribe_SubType::Key_Shared => "Key_Shared"
  }
}

///|
pub(all) enum CommandSubscribe_InitialPosition {
  Latest
  Earliest
} derive(Eq, Debug)

///|
pub fn CommandSubscribe_InitialPosition::to_enum(
  self : CommandSubscribe_InitialPosition,
) -> @lib.Enum {
  match self {
    CommandSubscribe_InitialPosition::Latest => 0
    CommandSubscribe_InitialPosition::Earliest => 1
  }
}

///|
pub fn CommandSubscribe_InitialPosition::from_enum(
  i : @lib.Enum,
) -> CommandSubscribe_InitialPosition {
  match i.0 {
    0 => CommandSubscribe_InitialPosition::Latest
    1 => CommandSubscribe_InitialPosition::Earliest
    _ => Default::default()
  }
}

///|
pub impl Default for CommandSubscribe_InitialPosition with fn default() -> CommandSubscribe_InitialPosition {
  CommandSubscribe_InitialPosition::Latest
}

///|
pub impl @lib.Sized for CommandSubscribe_InitialPosition with fn size_of(
  self : CommandSubscribe_InitialPosition,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandSubscribe_InitialPosition with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSubscribe_InitialPosition raise {
  match json {
    String("Latest") => CommandSubscribe_InitialPosition::Latest
    String("Earliest") => CommandSubscribe_InitialPosition::Earliest
    Number(0, ..) => CommandSubscribe_InitialPosition::Latest
    Number(1, ..) => CommandSubscribe_InitialPosition::Earliest
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandSubscribe_InitialPosition with fn to_json(
  self : CommandSubscribe_InitialPosition,
) -> Json {
  match self {
    CommandSubscribe_InitialPosition::Latest => "Latest"
    CommandSubscribe_InitialPosition::Earliest => "Earliest"
  }
}

///|
pub(all) struct CommandSubscribe {
  mut topic : String
  mut subscription : String
  mut sub_type : CommandSubscribe_SubType
  mut consumer_id : UInt64
  mut request_id : UInt64
  mut consumer_name : String?
  mut priority_level : Int?
  mut durable : Bool?
  mut start_message_id : MessageIdData?
  mut metadata : Array[KeyValue]
  mut read_compacted : Bool?
  mut schema : Schema?
  mut initial_position : CommandSubscribe_InitialPosition?
  mut replicate_subscription_state : Bool?
  mut force_topic_creation : Bool?
  mut start_message_rollback_duration_sec : UInt64?
  mut key_shared_meta : KeySharedMeta?
  mut subscription_properties : Array[KeyValue]
  mut consumer_epoch : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandSubscribe with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.topic))
  size += @lib.size_of_length_delimited_field(
    2U,
    @lib.size_of(self.subscription),
  )
  size += @lib.size_of_field(3U, @lib.size_of(self.sub_type))
  size += @lib.size_of_field(4U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(5U, @lib.size_of(self.request_id))
  if self.consumer_name is Some(v) {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(v))
  }
  if self.priority_level is Some(v) {
    size += @lib.size_of_field(7U, @lib.size_of(v))
  }
  if self.durable is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.start_message_id is Some(v) {
    size += @lib.size_of_length_delimited_field(9U, @lib.size_of(v))
  }
  for s in self.metadata {
    size += @lib.size_of_length_delimited_field(10U, @lib.size_of(s))
  }
  if self.read_compacted is Some(v) {
    size += @lib.size_of_field(11U, @lib.size_of(v))
  }
  if self.schema is Some(v) {
    size += @lib.size_of_length_delimited_field(12U, @lib.size_of(v))
  }
  if self.initial_position is Some(v) {
    size += @lib.size_of_field(13U, @lib.size_of(v))
  }
  if self.replicate_subscription_state is Some(v) {
    size += @lib.size_of_field(14U, @lib.size_of(v))
  }
  if self.force_topic_creation is Some(v) {
    size += @lib.size_of_field(15U, @lib.size_of(v))
  }
  if self.start_message_rollback_duration_sec is Some(v) {
    size += @lib.size_of_field(16U, @lib.size_of(v))
  }
  if self.key_shared_meta is Some(v) {
    size += @lib.size_of_length_delimited_field(17U, @lib.size_of(v))
  }
  for s in self.subscription_properties {
    size += @lib.size_of_length_delimited_field(18U, @lib.size_of(s))
  }
  if self.consumer_epoch is Some(v) {
    size += @lib.size_of_field(19U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandSubscribe with fn default() -> CommandSubscribe {
  CommandSubscribe::{
    topic: Default::default(),
    subscription: Default::default(),
    sub_type: Default::default(),
    consumer_id: Default::default(),
    request_id: Default::default(),
    consumer_name: None,
    priority_level: None,
    durable: Some(true),
    start_message_id: None,
    metadata: [],
    read_compacted: None,
    schema: None,
    initial_position: Some(CommandSubscribe_InitialPosition::Latest),
    replicate_subscription_state: None,
    force_topic_creation: Some(true),
    start_message_rollback_duration_sec: Some(0),
    key_shared_meta: None,
    subscription_properties: [],
    consumer_epoch: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandSubscribe::CommandSubscribe(
  topic : String,
  subscription : String,
  sub_type : CommandSubscribe_SubType,
  consumer_id : UInt64,
  request_id : UInt64,
  consumer_name? : String,
  priority_level? : Int,
  durable? : Bool,
  start_message_id? : MessageIdData,
  metadata : Array[KeyValue],
  read_compacted? : Bool,
  schema? : Schema,
  initial_position? : CommandSubscribe_InitialPosition,
  replicate_subscription_state? : Bool,
  force_topic_creation? : Bool,
  start_message_rollback_duration_sec? : UInt64,
  key_shared_meta? : KeySharedMeta,
  subscription_properties : Array[KeyValue],
  consumer_epoch? : UInt64,
) -> CommandSubscribe {
  CommandSubscribe::{
    topic,
    subscription,
    sub_type,
    consumer_id,
    request_id,
    consumer_name,
    priority_level,
    durable,
    start_message_id,
    metadata,
    read_compacted,
    schema,
    initial_position,
    replicate_subscription_state,
    force_topic_creation,
    start_message_rollback_duration_sec,
    key_shared_meta,
    subscription_properties,
    consumer_epoch,
  }
}

///|
pub impl @lib.Read for CommandSubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandSubscribe raise {
  let msg : CommandSubscribe = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.read_string()
      (2, _) => msg.subscription = reader |> @lib.read_string()
      (3, _) =>
        msg.sub_type = reader
          |> @lib.read_enum()
          |> CommandSubscribe_SubType::from_enum
      (4, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (5, _) => msg.request_id = reader |> @lib.read_uint64()
      (6, _) => msg.consumer_name = reader |> @lib.read_string() |> Some
      (7, _) => msg.priority_level = reader |> @lib.read_int32() |> Some
      (8, _) => msg.durable = reader |> @lib.read_bool() |> Some
      (9, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.start_message_id = (reader |> @lib.read_message() : MessageIdData)
          |> Some
      (10, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.metadata.push((reader |> @lib.read_message() : KeyValue))
      (11, _) => msg.read_compacted = reader |> @lib.read_bool() |> Some
      (12, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.read_message() : Schema) |> Some
      (13, _) =>
        msg.initial_position = reader
          |> @lib.read_enum()
          |> CommandSubscribe_InitialPosition::from_enum
          |> Some
      (14, _) =>
        msg.replicate_subscription_state = reader |> @lib.read_bool() |> Some
      (15, _) => msg.force_topic_creation = reader |> @lib.read_bool() |> Some
      (16, _) =>
        msg.start_message_rollback_duration_sec = reader
          |> @lib.read_uint64()
          |> Some
      (17, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.key_shared_meta = (reader |> @lib.read_message() : KeySharedMeta)
          |> Some
      (18, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscription_properties.push(
          (reader |> @lib.read_message() : KeyValue),
        )
      (19, _) => msg.consumer_epoch = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandSubscribe with fn write(
  self : CommandSubscribe,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.subscription)
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_enum(self.sub_type.to_enum())
  writer |> @lib.write_varint(32UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(40UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.consumer_name is Some(v) {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_string(v)
  }
  if self.priority_level is Some(v) {
    writer |> @lib.write_varint(56UL)
    writer |> @lib.write_int32(v)
  }
  if self.durable is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_bool(v)
  }
  if self.start_message_id is Some(v) {
    writer |> @lib.write_varint(74UL)
    writer |> @lib.write_message(v)
  }
  for item in self.metadata {
    writer |> @lib.write_varint(82UL)
    writer |> @lib.write_message(item)
  }
  if self.read_compacted is Some(v) {
    writer |> @lib.write_varint(88UL)
    writer |> @lib.write_bool(v)
  }
  if self.schema is Some(v) {
    writer |> @lib.write_varint(98UL)
    writer |> @lib.write_message(v)
  }
  if self.initial_position is Some(v) {
    writer |> @lib.write_varint(104UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.replicate_subscription_state is Some(v) {
    writer |> @lib.write_varint(112UL)
    writer |> @lib.write_bool(v)
  }
  if self.force_topic_creation is Some(v) {
    writer |> @lib.write_varint(120UL)
    writer |> @lib.write_bool(v)
  }
  if self.start_message_rollback_duration_sec is Some(v) {
    writer |> @lib.write_varint(128UL)
    writer |> @lib.write_uint64(v)
  }
  if self.key_shared_meta is Some(v) {
    writer |> @lib.write_varint(138UL)
    writer |> @lib.write_message(v)
  }
  for item in self.subscription_properties {
    writer |> @lib.write_varint(146UL)
    writer |> @lib.write_message(item)
  }
  if self.consumer_epoch is Some(v) {
    writer |> @lib.write_varint(152UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandSubscribe with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.subscription != Default::default() {
    json["subscription"] = ToJson::to_json(self.subscription)
  }
  if self.sub_type != Default::default() {
    json["subType"] = ToJson::to_json(self.sub_type)
  }
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.consumer_name {
    Some(v) => json["consumerName"] = ToJson::to_json(v)
    _ => ()
  }
  match self.priority_level {
    Some(v) => json["priorityLevel"] = ToJson::to_json(v)
    _ => ()
  }
  match self.durable {
    Some(v) if v != true => json["durable"] = ToJson::to_json(v)
    _ => ()
  }
  match self.start_message_id {
    Some(v) => json["startMessageId"] = ToJson::to_json(v)
    _ => ()
  }
  if self.metadata != Default::default() {
    json["metadata"] = ToJson::to_json(self.metadata)
  }
  match self.read_compacted {
    Some(v) => json["readCompacted"] = ToJson::to_json(v)
    _ => ()
  }
  match self.schema {
    Some(v) => json["schema"] = ToJson::to_json(v)
    _ => ()
  }
  match self.initial_position {
    Some(v) if v != CommandSubscribe_InitialPosition::Latest =>
      json["initialPosition"] = ToJson::to_json(v)
    _ => ()
  }
  match self.replicate_subscription_state {
    Some(v) => json["replicateSubscriptionState"] = ToJson::to_json(v)
    _ => ()
  }
  match self.force_topic_creation {
    Some(v) if v != true => json["forceTopicCreation"] = ToJson::to_json(v)
    _ => ()
  }
  match self.start_message_rollback_duration_sec {
    Some(v) if v != 0 =>
      json["startMessageRollbackDurationSec"] = ToJson::to_json(v)
    _ => ()
  }
  match self.key_shared_meta {
    Some(v) => json["keySharedMeta"] = ToJson::to_json(v)
    _ => ()
  }
  if self.subscription_properties != Default::default() {
    json["subscriptionProperties"] = ToJson::to_json(
      self.subscription_properties,
    )
  }
  match self.consumer_epoch {
    Some(v) => json["consumerEpoch"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandSubscribe with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSubscribe raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandSubscribe"),
    )
  }
  let message : CommandSubscribe = Default::default()
  for key, value in obj {
    match (key, value) {
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("subscription", value) =>
        message.subscription = @json.from_json(value, path~)
      ("subType", value) => message.sub_type = @json.from_json(value, path~)
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("consumerName", value) =>
        message.consumer_name = Some(@json.from_json(value, path~))
      ("priorityLevel", value) =>
        message.priority_level = Some(@json.from_json(value, path~))
      ("durable", value) =>
        message.durable = Some(@json.from_json(value, path~))
      ("startMessageId", value) =>
        message.start_message_id = Some(@json.from_json(value, path~))
      ("metadata", Array(value)) =>
        message.metadata = value.map(v => @json.from_json(v, path~))
      ("readCompacted", value) =>
        message.read_compacted = Some(@json.from_json(value, path~))
      ("schema", value) => message.schema = Some(@json.from_json(value, path~))
      ("initialPosition", value) =>
        message.initial_position = Some(@json.from_json(value, path~))
      ("replicateSubscriptionState", value) =>
        message.replicate_subscription_state = Some(
          @json.from_json(value, path~),
        )
      ("forceTopicCreation", value) =>
        message.force_topic_creation = Some(@json.from_json(value, path~))
      ("startMessageRollbackDurationSec", value) =>
        message.start_message_rollback_duration_sec = Some(
          @json.from_json(value, path~),
        )
      ("keySharedMeta", value) =>
        message.key_shared_meta = Some(@json.from_json(value, path~))
      ("subscriptionProperties", Array(value)) =>
        message.subscription_properties = value.map(v => {
          @json.from_json(v, path~)
        })
      ("consumerEpoch", value) =>
        message.consumer_epoch = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandSubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandSubscribe raise {
  let msg : CommandSubscribe = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.async_read_string()
      (2, _) => msg.subscription = reader |> @lib.async_read_string()
      (3, _) =>
        msg.sub_type = reader
          |> @lib.async_read_enum()
          |> CommandSubscribe_SubType::from_enum
      (4, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (5, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (6, _) => msg.consumer_name = reader |> @lib.async_read_string() |> Some
      (7, _) => msg.priority_level = reader |> @lib.async_read_int32() |> Some
      (8, _) => msg.durable = reader |> @lib.async_read_bool() |> Some
      (9, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.start_message_id = (
            reader |> @lib.async_read_message() : MessageIdData)
          |> Some
      (10, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.metadata.push((reader |> @lib.async_read_message() : KeyValue))
      (11, _) => msg.read_compacted = reader |> @lib.async_read_bool() |> Some
      (12, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.async_read_message() : Schema) |> Some
      (13, _) =>
        msg.initial_position = reader
          |> @lib.async_read_enum()
          |> CommandSubscribe_InitialPosition::from_enum
          |> Some
      (14, _) =>
        msg.replicate_subscription_state = reader
          |> @lib.async_read_bool()
          |> Some
      (15, _) =>
        msg.force_topic_creation = reader |> @lib.async_read_bool() |> Some
      (16, _) =>
        msg.start_message_rollback_duration_sec = reader
          |> @lib.async_read_uint64()
          |> Some
      (17, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.key_shared_meta = (
            reader |> @lib.async_read_message() : KeySharedMeta)
          |> Some
      (18, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscription_properties.push(
          (reader |> @lib.async_read_message() : KeyValue),
        )
      (19, _) => msg.consumer_epoch = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandSubscribe with fn write(
  self : CommandSubscribe,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.subscription)
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_enum(self.sub_type.to_enum())
  writer |> @lib.async_write_varint(32UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(40UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.consumer_name is Some(v) {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_string(v)
  }
  if self.priority_level is Some(v) {
    writer |> @lib.async_write_varint(56UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.durable is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.start_message_id is Some(v) {
    writer |> @lib.async_write_varint(74UL)
    writer |> @lib.async_write_message(v)
  }
  for item in self.metadata {
    writer |> @lib.async_write_varint(82UL)
    writer |> @lib.async_write_message(item)
  }
  if self.read_compacted is Some(v) {
    writer |> @lib.async_write_varint(88UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.schema is Some(v) {
    writer |> @lib.async_write_varint(98UL)
    writer |> @lib.async_write_message(v)
  }
  if self.initial_position is Some(v) {
    writer |> @lib.async_write_varint(104UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.replicate_subscription_state is Some(v) {
    writer |> @lib.async_write_varint(112UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.force_topic_creation is Some(v) {
    writer |> @lib.async_write_varint(120UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.start_message_rollback_duration_sec is Some(v) {
    writer |> @lib.async_write_varint(128UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.key_shared_meta is Some(v) {
    writer |> @lib.async_write_varint(138UL)
    writer |> @lib.async_write_message(v)
  }
  for item in self.subscription_properties {
    writer |> @lib.async_write_varint(146UL)
    writer |> @lib.async_write_message(item)
  }
  if self.consumer_epoch is Some(v) {
    writer |> @lib.async_write_varint(152UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandPartitionedTopicMetadata {
  mut topic : String
  mut request_id : UInt64
  mut original_principal : String?
  mut original_auth_data : String?
  mut original_auth_method : String?
  mut metadata_auto_creation_enabled : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandPartitionedTopicMetadata with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.topic))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.original_principal is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.original_auth_data is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.original_auth_method is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  if self.metadata_auto_creation_enabled is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandPartitionedTopicMetadata with fn default() -> CommandPartitionedTopicMetadata {
  CommandPartitionedTopicMetadata::{
    topic: Default::default(),
    request_id: Default::default(),
    original_principal: None,
    original_auth_data: None,
    original_auth_method: None,
    metadata_auto_creation_enabled: Some(true),
  }
}

///|
#alias(new, deprecated)
pub fn CommandPartitionedTopicMetadata::CommandPartitionedTopicMetadata(
  topic : String,
  request_id : UInt64,
  original_principal? : String,
  original_auth_data? : String,
  original_auth_method? : String,
  metadata_auto_creation_enabled? : Bool,
) -> CommandPartitionedTopicMetadata {
  CommandPartitionedTopicMetadata::{
    topic,
    request_id,
    original_principal,
    original_auth_data,
    original_auth_method,
    metadata_auto_creation_enabled,
  }
}

///|
pub impl @lib.Read for CommandPartitionedTopicMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandPartitionedTopicMetadata raise {
  let msg : CommandPartitionedTopicMetadata = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.read_string()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, _) => msg.original_principal = reader |> @lib.read_string() |> Some
      (4, _) => msg.original_auth_data = reader |> @lib.read_string() |> Some
      (5, _) => msg.original_auth_method = reader |> @lib.read_string() |> Some
      (6, _) =>
        msg.metadata_auto_creation_enabled = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandPartitionedTopicMetadata with fn write(
  self : CommandPartitionedTopicMetadata,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.original_principal is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.original_auth_data is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.original_auth_method is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  if self.metadata_auto_creation_enabled is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandPartitionedTopicMetadata with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.original_principal {
    Some(v) => json["originalPrincipal"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_auth_data {
    Some(v) => json["originalAuthData"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_auth_method {
    Some(v) => json["originalAuthMethod"] = ToJson::to_json(v)
    _ => ()
  }
  match self.metadata_auto_creation_enabled {
    Some(v) if v != true =>
      json["metadataAutoCreationEnabled"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandPartitionedTopicMetadata with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandPartitionedTopicMetadata raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandPartitionedTopicMetadata"),
    )
  }
  let message : CommandPartitionedTopicMetadata = Default::default()
  for key, value in obj {
    match (key, value) {
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("originalPrincipal", value) =>
        message.original_principal = Some(@json.from_json(value, path~))
      ("originalAuthData", value) =>
        message.original_auth_data = Some(@json.from_json(value, path~))
      ("originalAuthMethod", value) =>
        message.original_auth_method = Some(@json.from_json(value, path~))
      ("metadataAutoCreationEnabled", value) =>
        message.metadata_auto_creation_enabled = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandPartitionedTopicMetadata with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandPartitionedTopicMetadata raise {
  let msg : CommandPartitionedTopicMetadata = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.async_read_string()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, _) =>
        msg.original_principal = reader |> @lib.async_read_string() |> Some
      (4, _) =>
        msg.original_auth_data = reader |> @lib.async_read_string() |> Some
      (5, _) =>
        msg.original_auth_method = reader |> @lib.async_read_string() |> Some
      (6, _) =>
        msg.metadata_auto_creation_enabled = reader
          |> @lib.async_read_bool()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandPartitionedTopicMetadata with fn write(
  self : CommandPartitionedTopicMetadata,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.original_principal is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_auth_data is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_auth_method is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  if self.metadata_auto_creation_enabled is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) enum CommandPartitionedTopicMetadataResponse_LookupType {
  Success
  Failed
} derive(Eq, Debug)

///|
pub fn CommandPartitionedTopicMetadataResponse_LookupType::to_enum(
  self : CommandPartitionedTopicMetadataResponse_LookupType,
) -> @lib.Enum {
  match self {
    CommandPartitionedTopicMetadataResponse_LookupType::Success => 0
    CommandPartitionedTopicMetadataResponse_LookupType::Failed => 1
  }
}

///|
pub fn CommandPartitionedTopicMetadataResponse_LookupType::from_enum(
  i : @lib.Enum,
) -> CommandPartitionedTopicMetadataResponse_LookupType {
  match i.0 {
    0 => CommandPartitionedTopicMetadataResponse_LookupType::Success
    1 => CommandPartitionedTopicMetadataResponse_LookupType::Failed
    _ => Default::default()
  }
}

///|
pub impl Default for CommandPartitionedTopicMetadataResponse_LookupType with fn default() -> CommandPartitionedTopicMetadataResponse_LookupType {
  CommandPartitionedTopicMetadataResponse_LookupType::Success
}

///|
pub impl @lib.Sized for CommandPartitionedTopicMetadataResponse_LookupType with fn size_of(
  self : CommandPartitionedTopicMetadataResponse_LookupType,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandPartitionedTopicMetadataResponse_LookupType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandPartitionedTopicMetadataResponse_LookupType raise {
  match json {
    String("Success") =>
      CommandPartitionedTopicMetadataResponse_LookupType::Success
    String("Failed") =>
      CommandPartitionedTopicMetadataResponse_LookupType::Failed
    Number(0, ..) => CommandPartitionedTopicMetadataResponse_LookupType::Success
    Number(1, ..) => CommandPartitionedTopicMetadataResponse_LookupType::Failed
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandPartitionedTopicMetadataResponse_LookupType with fn to_json(
  self : CommandPartitionedTopicMetadataResponse_LookupType,
) -> Json {
  match self {
    CommandPartitionedTopicMetadataResponse_LookupType::Success => "Success"
    CommandPartitionedTopicMetadataResponse_LookupType::Failed => "Failed"
  }
}

///|
pub(all) struct CommandPartitionedTopicMetadataResponse {
  mut partitions : UInt?
  mut request_id : UInt64
  mut response : CommandPartitionedTopicMetadataResponse_LookupType?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandPartitionedTopicMetadataResponse with fn size_of(
  self,
) {
  let mut size = 0U
  if self.partitions is Some(v) {
    size += @lib.size_of_field(1U, @lib.size_of(v))
  }
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.response is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandPartitionedTopicMetadataResponse with fn default() -> CommandPartitionedTopicMetadataResponse {
  CommandPartitionedTopicMetadataResponse::{
    partitions: None,
    request_id: Default::default(),
    response: None,
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandPartitionedTopicMetadataResponse::CommandPartitionedTopicMetadataResponse(
  partitions? : UInt,
  request_id : UInt64,
  response? : CommandPartitionedTopicMetadataResponse_LookupType,
  error? : ServerError,
  message? : String,
) -> CommandPartitionedTopicMetadataResponse {
  CommandPartitionedTopicMetadataResponse::{
    partitions,
    request_id,
    response,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandPartitionedTopicMetadataResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandPartitionedTopicMetadataResponse raise {
  let msg : CommandPartitionedTopicMetadataResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.partitions = reader |> @lib.read_uint32() |> Some
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, _) =>
        msg.response = reader
          |> @lib.read_enum()
          |> CommandPartitionedTopicMetadataResponse_LookupType::from_enum
          |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandPartitionedTopicMetadataResponse with fn write(
  self : CommandPartitionedTopicMetadataResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.partitions is Some(v) {
    writer |> @lib.write_varint(8UL)
    writer |> @lib.write_uint32(v)
  }
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.response is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandPartitionedTopicMetadataResponse with fn to_json(
  self,
) {
  let json : Map[String, Json] = Map([])
  match self.partitions {
    Some(v) => json["partitions"] = ToJson::to_json(v)
    _ => ()
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.response {
    Some(v) => json["response"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandPartitionedTopicMetadataResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandPartitionedTopicMetadataResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandPartitionedTopicMetadataResponse"),
    )
  }
  let message : CommandPartitionedTopicMetadataResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("partitions", value) =>
        message.partitions = Some(@json.from_json(value, path~))
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("response", value) =>
        message.response = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandPartitionedTopicMetadataResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandPartitionedTopicMetadataResponse raise {
  let msg : CommandPartitionedTopicMetadataResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.partitions = reader |> @lib.async_read_uint32() |> Some
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, _) =>
        msg.response = reader
          |> @lib.async_read_enum()
          |> CommandPartitionedTopicMetadataResponse_LookupType::from_enum
          |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandPartitionedTopicMetadataResponse with fn write(
  self : CommandPartitionedTopicMetadataResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.partitions is Some(v) {
    writer |> @lib.async_write_varint(8UL)
    writer |> @lib.async_write_uint32(v)
  }
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.response is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandLookupTopic {
  mut topic : String
  mut request_id : UInt64
  mut authoritative : Bool?
  mut original_principal : String?
  mut original_auth_data : String?
  mut original_auth_method : String?
  mut advertised_listener_name : String?
  mut properties : Array[KeyValue]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandLookupTopic with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.topic))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.authoritative is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.original_principal is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.original_auth_data is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  if self.original_auth_method is Some(v) {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(v))
  }
  if self.advertised_listener_name is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  for s in self.properties {
    size += @lib.size_of_length_delimited_field(8U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for CommandLookupTopic with fn default() -> CommandLookupTopic {
  CommandLookupTopic::{
    topic: Default::default(),
    request_id: Default::default(),
    authoritative: Some(false),
    original_principal: None,
    original_auth_data: None,
    original_auth_method: None,
    advertised_listener_name: None,
    properties: [],
  }
}

///|
#alias(new, deprecated)
pub fn CommandLookupTopic::CommandLookupTopic(
  topic : String,
  request_id : UInt64,
  authoritative? : Bool,
  original_principal? : String,
  original_auth_data? : String,
  original_auth_method? : String,
  advertised_listener_name? : String,
  properties : Array[KeyValue],
) -> CommandLookupTopic {
  CommandLookupTopic::{
    topic,
    request_id,
    authoritative,
    original_principal,
    original_auth_data,
    original_auth_method,
    advertised_listener_name,
    properties,
  }
}

///|
pub impl @lib.Read for CommandLookupTopic with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandLookupTopic raise {
  let msg : CommandLookupTopic = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.read_string()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, _) => msg.authoritative = reader |> @lib.read_bool() |> Some
      (4, _) => msg.original_principal = reader |> @lib.read_string() |> Some
      (5, _) => msg.original_auth_data = reader |> @lib.read_string() |> Some
      (6, _) => msg.original_auth_method = reader |> @lib.read_string() |> Some
      (7, _) =>
        msg.advertised_listener_name = reader |> @lib.read_string() |> Some
      (8, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.read_message() : KeyValue))
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandLookupTopic with fn write(
  self : CommandLookupTopic,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.authoritative is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_bool(v)
  }
  if self.original_principal is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.original_auth_data is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  if self.original_auth_method is Some(v) {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_string(v)
  }
  if self.advertised_listener_name is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_string(v)
  }
  for item in self.properties {
    writer |> @lib.write_varint(66UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for CommandLookupTopic with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.authoritative {
    Some(v) if v != false => json["authoritative"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_principal {
    Some(v) => json["originalPrincipal"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_auth_data {
    Some(v) => json["originalAuthData"] = ToJson::to_json(v)
    _ => ()
  }
  match self.original_auth_method {
    Some(v) => json["originalAuthMethod"] = ToJson::to_json(v)
    _ => ()
  }
  match self.advertised_listener_name {
    Some(v) => json["advertisedListenerName"] = ToJson::to_json(v)
    _ => ()
  }
  if self.properties != Default::default() {
    json["properties"] = ToJson::to_json(self.properties)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandLookupTopic with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandLookupTopic raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandLookupTopic"),
    )
  }
  let message : CommandLookupTopic = Default::default()
  for key, value in obj {
    match (key, value) {
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("authoritative", value) =>
        message.authoritative = Some(@json.from_json(value, path~))
      ("originalPrincipal", value) =>
        message.original_principal = Some(@json.from_json(value, path~))
      ("originalAuthData", value) =>
        message.original_auth_data = Some(@json.from_json(value, path~))
      ("originalAuthMethod", value) =>
        message.original_auth_method = Some(@json.from_json(value, path~))
      ("advertisedListenerName", value) =>
        message.advertised_listener_name = Some(@json.from_json(value, path~))
      ("properties", Array(value)) =>
        message.properties = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandLookupTopic with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandLookupTopic raise {
  let msg : CommandLookupTopic = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.async_read_string()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.authoritative = reader |> @lib.async_read_bool() |> Some
      (4, _) =>
        msg.original_principal = reader |> @lib.async_read_string() |> Some
      (5, _) =>
        msg.original_auth_data = reader |> @lib.async_read_string() |> Some
      (6, _) =>
        msg.original_auth_method = reader |> @lib.async_read_string() |> Some
      (7, _) =>
        msg.advertised_listener_name = reader
          |> @lib.async_read_string()
          |> Some
      (8, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.async_read_message() : KeyValue))
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandLookupTopic with fn write(
  self : CommandLookupTopic,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.authoritative is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.original_principal is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_auth_data is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  if self.original_auth_method is Some(v) {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_string(v)
  }
  if self.advertised_listener_name is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_string(v)
  }
  for item in self.properties {
    writer |> @lib.async_write_varint(66UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) enum CommandLookupTopicResponse_LookupType {
  Redirect
  Connect
  Failed
} derive(Eq, Debug)

///|
pub fn CommandLookupTopicResponse_LookupType::to_enum(
  self : CommandLookupTopicResponse_LookupType,
) -> @lib.Enum {
  match self {
    CommandLookupTopicResponse_LookupType::Redirect => 0
    CommandLookupTopicResponse_LookupType::Connect => 1
    CommandLookupTopicResponse_LookupType::Failed => 2
  }
}

///|
pub fn CommandLookupTopicResponse_LookupType::from_enum(
  i : @lib.Enum,
) -> CommandLookupTopicResponse_LookupType {
  match i.0 {
    0 => CommandLookupTopicResponse_LookupType::Redirect
    1 => CommandLookupTopicResponse_LookupType::Connect
    2 => CommandLookupTopicResponse_LookupType::Failed
    _ => Default::default()
  }
}

///|
pub impl Default for CommandLookupTopicResponse_LookupType with fn default() -> CommandLookupTopicResponse_LookupType {
  CommandLookupTopicResponse_LookupType::Redirect
}

///|
pub impl @lib.Sized for CommandLookupTopicResponse_LookupType with fn size_of(
  self : CommandLookupTopicResponse_LookupType,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandLookupTopicResponse_LookupType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandLookupTopicResponse_LookupType raise {
  match json {
    String("Redirect") => CommandLookupTopicResponse_LookupType::Redirect
    String("Connect") => CommandLookupTopicResponse_LookupType::Connect
    String("Failed") => CommandLookupTopicResponse_LookupType::Failed
    Number(0, ..) => CommandLookupTopicResponse_LookupType::Redirect
    Number(1, ..) => CommandLookupTopicResponse_LookupType::Connect
    Number(2, ..) => CommandLookupTopicResponse_LookupType::Failed
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandLookupTopicResponse_LookupType with fn to_json(
  self : CommandLookupTopicResponse_LookupType,
) -> Json {
  match self {
    CommandLookupTopicResponse_LookupType::Redirect => "Redirect"
    CommandLookupTopicResponse_LookupType::Connect => "Connect"
    CommandLookupTopicResponse_LookupType::Failed => "Failed"
  }
}

///|
pub(all) struct CommandLookupTopicResponse {
  mut broker_service_url : String?
  mut broker_service_url_tls : String?
  mut response : CommandLookupTopicResponse_LookupType?
  mut request_id : UInt64
  mut authoritative : Bool?
  mut error : ServerError?
  mut message : String?
  mut proxy_through_service_url : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandLookupTopicResponse with fn size_of(self) {
  let mut size = 0U
  if self.broker_service_url is Some(v) {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(v))
  }
  if self.broker_service_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.response is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size += @lib.size_of_field(4U, @lib.size_of(self.request_id))
  if self.authoritative is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  if self.proxy_through_service_url is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandLookupTopicResponse with fn default() -> CommandLookupTopicResponse {
  CommandLookupTopicResponse::{
    broker_service_url: None,
    broker_service_url_tls: None,
    response: None,
    request_id: Default::default(),
    authoritative: Some(false),
    error: None,
    message: None,
    proxy_through_service_url: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandLookupTopicResponse::CommandLookupTopicResponse(
  broker_service_url? : String,
  broker_service_url_tls? : String,
  response? : CommandLookupTopicResponse_LookupType,
  request_id : UInt64,
  authoritative? : Bool,
  error? : ServerError,
  message? : String,
  proxy_through_service_url? : Bool,
) -> CommandLookupTopicResponse {
  CommandLookupTopicResponse::{
    broker_service_url,
    broker_service_url_tls,
    response,
    request_id,
    authoritative,
    error,
    message,
    proxy_through_service_url,
  }
}

///|
pub impl @lib.Read for CommandLookupTopicResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandLookupTopicResponse raise {
  let msg : CommandLookupTopicResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.broker_service_url = reader |> @lib.read_string() |> Some
      (2, _) =>
        msg.broker_service_url_tls = reader |> @lib.read_string() |> Some
      (3, _) =>
        msg.response = reader
          |> @lib.read_enum()
          |> CommandLookupTopicResponse_LookupType::from_enum
          |> Some
      (4, _) => msg.request_id = reader |> @lib.read_uint64()
      (5, _) => msg.authoritative = reader |> @lib.read_bool() |> Some
      (6, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (7, _) => msg.message = reader |> @lib.read_string() |> Some
      (8, _) =>
        msg.proxy_through_service_url = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandLookupTopicResponse with fn write(
  self : CommandLookupTopicResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  if self.broker_service_url is Some(v) {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_string(v)
  }
  if self.broker_service_url_tls is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_string(v)
  }
  if self.response is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  writer |> @lib.write_varint(32UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.authoritative is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_string(v)
  }
  if self.proxy_through_service_url is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandLookupTopicResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  match self.broker_service_url {
    Some(v) => json["brokerServiceUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.broker_service_url_tls {
    Some(v) => json["brokerServiceUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  match self.response {
    Some(v) => json["response"] = ToJson::to_json(v)
    _ => ()
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.authoritative {
    Some(v) if v != false => json["authoritative"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  match self.proxy_through_service_url {
    Some(v) if v != false => json["proxyThroughServiceUrl"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandLookupTopicResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandLookupTopicResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandLookupTopicResponse"),
    )
  }
  let message : CommandLookupTopicResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("brokerServiceUrl", value) =>
        message.broker_service_url = Some(@json.from_json(value, path~))
      ("brokerServiceUrlTls", value) =>
        message.broker_service_url_tls = Some(@json.from_json(value, path~))
      ("response", value) =>
        message.response = Some(@json.from_json(value, path~))
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("authoritative", value) =>
        message.authoritative = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      ("proxyThroughServiceUrl", value) =>
        message.proxy_through_service_url = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandLookupTopicResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandLookupTopicResponse raise {
  let msg : CommandLookupTopicResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.broker_service_url = reader |> @lib.async_read_string() |> Some
      (2, _) =>
        msg.broker_service_url_tls = reader |> @lib.async_read_string() |> Some
      (3, _) =>
        msg.response = reader
          |> @lib.async_read_enum()
          |> CommandLookupTopicResponse_LookupType::from_enum
          |> Some
      (4, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (5, _) => msg.authoritative = reader |> @lib.async_read_bool() |> Some
      (6, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (7, _) => msg.message = reader |> @lib.async_read_string() |> Some
      (8, _) =>
        msg.proxy_through_service_url = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandLookupTopicResponse with fn write(
  self : CommandLookupTopicResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  if self.broker_service_url is Some(v) {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_string(v)
  }
  if self.broker_service_url_tls is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_string(v)
  }
  if self.response is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  writer |> @lib.async_write_varint(32UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.authoritative is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_string(v)
  }
  if self.proxy_through_service_url is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandProducer {
  mut topic : String
  mut producer_id : UInt64
  mut request_id : UInt64
  mut producer_name : String?
  mut encrypted : Bool?
  mut metadata : Array[KeyValue]
  mut schema : Schema?
  mut epoch : UInt64?
  mut user_provided_producer_name : Bool?
  mut producer_access_mode : ProducerAccessMode?
  mut topic_epoch : UInt64?
  mut txn_enabled : Bool?
  mut initial_subscription_name : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandProducer with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.topic))
  size += @lib.size_of_field(2U, @lib.size_of(self.producer_id))
  size += @lib.size_of_field(3U, @lib.size_of(self.request_id))
  if self.producer_name is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.encrypted is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  for s in self.metadata {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(s))
  }
  if self.schema is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  if self.epoch is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.user_provided_producer_name is Some(v) {
    size += @lib.size_of_field(9U, @lib.size_of(v))
  }
  if self.producer_access_mode is Some(v) {
    size += @lib.size_of_field(10U, @lib.size_of(v))
  }
  if self.topic_epoch is Some(v) {
    size += @lib.size_of_field(11U, @lib.size_of(v))
  }
  if self.txn_enabled is Some(v) {
    size += @lib.size_of_field(12U, @lib.size_of(v))
  }
  if self.initial_subscription_name is Some(v) {
    size += @lib.size_of_length_delimited_field(13U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandProducer with fn default() -> CommandProducer {
  CommandProducer::{
    topic: Default::default(),
    producer_id: Default::default(),
    request_id: Default::default(),
    producer_name: None,
    encrypted: Some(false),
    metadata: [],
    schema: None,
    epoch: Some(0),
    user_provided_producer_name: Some(true),
    producer_access_mode: Some(ProducerAccessMode::Shared),
    topic_epoch: None,
    txn_enabled: Some(false),
    initial_subscription_name: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandProducer::CommandProducer(
  topic : String,
  producer_id : UInt64,
  request_id : UInt64,
  producer_name? : String,
  encrypted? : Bool,
  metadata : Array[KeyValue],
  schema? : Schema,
  epoch? : UInt64,
  user_provided_producer_name? : Bool,
  producer_access_mode? : ProducerAccessMode,
  topic_epoch? : UInt64,
  txn_enabled? : Bool,
  initial_subscription_name? : String,
) -> CommandProducer {
  CommandProducer::{
    topic,
    producer_id,
    request_id,
    producer_name,
    encrypted,
    metadata,
    schema,
    epoch,
    user_provided_producer_name,
    producer_access_mode,
    topic_epoch,
    txn_enabled,
    initial_subscription_name,
  }
}

///|
pub impl @lib.Read for CommandProducer with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandProducer raise {
  let msg : CommandProducer = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.read_string()
      (2, _) => msg.producer_id = reader |> @lib.read_uint64()
      (3, _) => msg.request_id = reader |> @lib.read_uint64()
      (4, _) => msg.producer_name = reader |> @lib.read_string() |> Some
      (5, _) => msg.encrypted = reader |> @lib.read_bool() |> Some
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.metadata.push((reader |> @lib.read_message() : KeyValue))
      (7, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.read_message() : Schema) |> Some
      (8, _) => msg.epoch = reader |> @lib.read_uint64() |> Some
      (9, _) =>
        msg.user_provided_producer_name = reader |> @lib.read_bool() |> Some
      (10, _) =>
        msg.producer_access_mode = reader
          |> @lib.read_enum()
          |> ProducerAccessMode::from_enum
          |> Some
      (11, _) => msg.topic_epoch = reader |> @lib.read_uint64() |> Some
      (12, _) => msg.txn_enabled = reader |> @lib.read_bool() |> Some
      (13, _) =>
        msg.initial_subscription_name = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandProducer with fn write(
  self : CommandProducer,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.producer_id)
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.producer_name is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.encrypted is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
  for item in self.metadata {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_message(item)
  }
  if self.schema is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_message(v)
  }
  if self.epoch is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_uint64(v)
  }
  if self.user_provided_producer_name is Some(v) {
    writer |> @lib.write_varint(72UL)
    writer |> @lib.write_bool(v)
  }
  if self.producer_access_mode is Some(v) {
    writer |> @lib.write_varint(80UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.topic_epoch is Some(v) {
    writer |> @lib.write_varint(88UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txn_enabled is Some(v) {
    writer |> @lib.write_varint(96UL)
    writer |> @lib.write_bool(v)
  }
  if self.initial_subscription_name is Some(v) {
    writer |> @lib.write_varint(106UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandProducer with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.producer_id != Default::default() {
    json["producerId"] = ToJson::to_json(self.producer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.producer_name {
    Some(v) => json["producerName"] = ToJson::to_json(v)
    _ => ()
  }
  match self.encrypted {
    Some(v) if v != false => json["encrypted"] = ToJson::to_json(v)
    _ => ()
  }
  if self.metadata != Default::default() {
    json["metadata"] = ToJson::to_json(self.metadata)
  }
  match self.schema {
    Some(v) => json["schema"] = ToJson::to_json(v)
    _ => ()
  }
  match self.epoch {
    Some(v) if v != 0 => json["epoch"] = ToJson::to_json(v)
    _ => ()
  }
  match self.user_provided_producer_name {
    Some(v) if v != true =>
      json["userProvidedProducerName"] = ToJson::to_json(v)
    _ => ()
  }
  match self.producer_access_mode {
    Some(v) if v != ProducerAccessMode::Shared =>
      json["producerAccessMode"] = ToJson::to_json(v)
    _ => ()
  }
  match self.topic_epoch {
    Some(v) => json["topicEpoch"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txn_enabled {
    Some(v) if v != false => json["txnEnabled"] = ToJson::to_json(v)
    _ => ()
  }
  match self.initial_subscription_name {
    Some(v) => json["initialSubscriptionName"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandProducer with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandProducer raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandProducer"),
    )
  }
  let message : CommandProducer = Default::default()
  for key, value in obj {
    match (key, value) {
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("producerId", value) =>
        message.producer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("producerName", value) =>
        message.producer_name = Some(@json.from_json(value, path~))
      ("encrypted", value) =>
        message.encrypted = Some(@json.from_json(value, path~))
      ("metadata", Array(value)) =>
        message.metadata = value.map(v => @json.from_json(v, path~))
      ("schema", value) => message.schema = Some(@json.from_json(value, path~))
      ("epoch", value) => message.epoch = Some(@json.from_json(value, path~))
      ("userProvidedProducerName", value) =>
        message.user_provided_producer_name = Some(
          @json.from_json(value, path~),
        )
      ("producerAccessMode", value) =>
        message.producer_access_mode = Some(@json.from_json(value, path~))
      ("topicEpoch", value) =>
        message.topic_epoch = Some(@json.from_json(value, path~))
      ("txnEnabled", value) =>
        message.txn_enabled = Some(@json.from_json(value, path~))
      ("initialSubscriptionName", value) =>
        message.initial_subscription_name = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandProducer with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandProducer raise {
  let msg : CommandProducer = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.async_read_string()
      (2, _) => msg.producer_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (4, _) => msg.producer_name = reader |> @lib.async_read_string() |> Some
      (5, _) => msg.encrypted = reader |> @lib.async_read_bool() |> Some
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.metadata.push((reader |> @lib.async_read_message() : KeyValue))
      (7, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.async_read_message() : Schema) |> Some
      (8, _) => msg.epoch = reader |> @lib.async_read_uint64() |> Some
      (9, _) =>
        msg.user_provided_producer_name = reader
          |> @lib.async_read_bool()
          |> Some
      (10, _) =>
        msg.producer_access_mode = reader
          |> @lib.async_read_enum()
          |> ProducerAccessMode::from_enum
          |> Some
      (11, _) => msg.topic_epoch = reader |> @lib.async_read_uint64() |> Some
      (12, _) => msg.txn_enabled = reader |> @lib.async_read_bool() |> Some
      (13, _) =>
        msg.initial_subscription_name = reader
          |> @lib.async_read_string()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandProducer with fn write(
  self : CommandProducer,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.producer_id)
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.producer_name is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.encrypted is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
  for item in self.metadata {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_message(item)
  }
  if self.schema is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_message(v)
  }
  if self.epoch is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.user_provided_producer_name is Some(v) {
    writer |> @lib.async_write_varint(72UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.producer_access_mode is Some(v) {
    writer |> @lib.async_write_varint(80UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.topic_epoch is Some(v) {
    writer |> @lib.async_write_varint(88UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txn_enabled is Some(v) {
    writer |> @lib.async_write_varint(96UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.initial_subscription_name is Some(v) {
    writer |> @lib.async_write_varint(106UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandSend {
  mut producer_id : UInt64
  mut sequence_id : UInt64
  mut num_messages : Int?
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut highest_sequence_id : UInt64?
  mut is_chunk : Bool?
  mut marker : Bool?
  mut message_id : MessageIdData?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandSend with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.producer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.sequence_id))
  if self.num_messages is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.highest_sequence_id is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  if self.is_chunk is Some(v) {
    size += @lib.size_of_field(7U, @lib.size_of(v))
  }
  if self.marker is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.message_id is Some(v) {
    size += @lib.size_of_length_delimited_field(9U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandSend with fn default() -> CommandSend {
  CommandSend::{
    producer_id: Default::default(),
    sequence_id: Default::default(),
    num_messages: Some(1),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    highest_sequence_id: Some(0),
    is_chunk: Some(false),
    marker: Some(false),
    message_id: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandSend::CommandSend(
  producer_id : UInt64,
  sequence_id : UInt64,
  num_messages? : Int,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  highest_sequence_id? : UInt64,
  is_chunk? : Bool,
  marker? : Bool,
  message_id? : MessageIdData,
) -> CommandSend {
  CommandSend::{
    producer_id,
    sequence_id,
    num_messages,
    txnid_least_bits,
    txnid_most_bits,
    highest_sequence_id,
    is_chunk,
    marker,
    message_id,
  }
}

///|
pub impl @lib.Read for CommandSend with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandSend raise {
  let msg : CommandSend = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.read_uint64()
      (2, _) => msg.sequence_id = reader |> @lib.read_uint64()
      (3, _) => msg.num_messages = reader |> @lib.read_int32() |> Some
      (4, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (5, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (6, _) => msg.highest_sequence_id = reader |> @lib.read_uint64() |> Some
      (7, _) => msg.is_chunk = reader |> @lib.read_bool() |> Some
      (8, _) => msg.marker = reader |> @lib.read_bool() |> Some
      (9, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.read_message() : MessageIdData) |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandSend with fn write(
  self : CommandSend,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.producer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.sequence_id)
  if self.num_messages is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_int32(v)
  }
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_uint64(v)
  }
  if self.highest_sequence_id is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_uint64(v)
  }
  if self.is_chunk is Some(v) {
    writer |> @lib.write_varint(56UL)
    writer |> @lib.write_bool(v)
  }
  if self.marker is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_bool(v)
  }
  if self.message_id is Some(v) {
    writer |> @lib.write_varint(74UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for CommandSend with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.producer_id != Default::default() {
    json["producerId"] = ToJson::to_json(self.producer_id)
  }
  if self.sequence_id != Default::default() {
    json["sequenceId"] = ToJson::to_json(self.sequence_id)
  }
  match self.num_messages {
    Some(v) if v != 1 => json["numMessages"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.highest_sequence_id {
    Some(v) if v != 0 => json["highestSequenceId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.is_chunk {
    Some(v) if v != false => json["isChunk"] = ToJson::to_json(v)
    _ => ()
  }
  match self.marker {
    Some(v) if v != false => json["marker"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message_id {
    Some(v) => json["messageId"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandSend with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSend raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandSend"))
  }
  let message : CommandSend = Default::default()
  for key, value in obj {
    match (key, value) {
      ("producerId", value) =>
        message.producer_id = @json.from_json(value, path~)
      ("sequenceId", value) =>
        message.sequence_id = @json.from_json(value, path~)
      ("numMessages", value) =>
        message.num_messages = Some(@json.from_json(value, path~))
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("highestSequenceId", value) =>
        message.highest_sequence_id = Some(@json.from_json(value, path~))
      ("isChunk", value) =>
        message.is_chunk = Some(@json.from_json(value, path~))
      ("marker", value) => message.marker = Some(@json.from_json(value, path~))
      ("messageId", value) =>
        message.message_id = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandSend with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandSend raise {
  let msg : CommandSend = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.sequence_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.num_messages = reader |> @lib.async_read_int32() |> Some
      (4, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (5, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (6, _) =>
        msg.highest_sequence_id = reader |> @lib.async_read_uint64() |> Some
      (7, _) => msg.is_chunk = reader |> @lib.async_read_bool() |> Some
      (8, _) => msg.marker = reader |> @lib.async_read_bool() |> Some
      (9, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.async_read_message() : MessageIdData)
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandSend with fn write(
  self : CommandSend,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.producer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.sequence_id)
  if self.num_messages is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_int32(v)
  }
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.highest_sequence_id is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.is_chunk is Some(v) {
    writer |> @lib.async_write_varint(56UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.marker is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.message_id is Some(v) {
    writer |> @lib.async_write_varint(74UL)
    writer |> @lib.async_write_message(v)
  }
}

///|
pub(all) struct CommandSendReceipt {
  mut producer_id : UInt64
  mut sequence_id : UInt64
  mut message_id : MessageIdData?
  mut highest_sequence_id : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandSendReceipt with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.producer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.sequence_id))
  if self.message_id is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.highest_sequence_id is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandSendReceipt with fn default() -> CommandSendReceipt {
  CommandSendReceipt::{
    producer_id: Default::default(),
    sequence_id: Default::default(),
    message_id: None,
    highest_sequence_id: Some(0),
  }
}

///|
#alias(new, deprecated)
pub fn CommandSendReceipt::CommandSendReceipt(
  producer_id : UInt64,
  sequence_id : UInt64,
  message_id? : MessageIdData,
  highest_sequence_id? : UInt64,
) -> CommandSendReceipt {
  CommandSendReceipt::{
    producer_id,
    sequence_id,
    message_id,
    highest_sequence_id,
  }
}

///|
pub impl @lib.Read for CommandSendReceipt with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandSendReceipt raise {
  let msg : CommandSendReceipt = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.read_uint64()
      (2, _) => msg.sequence_id = reader |> @lib.read_uint64()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.read_message() : MessageIdData) |> Some
      (4, _) => msg.highest_sequence_id = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandSendReceipt with fn write(
  self : CommandSendReceipt,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.producer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.sequence_id)
  if self.message_id is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(v)
  }
  if self.highest_sequence_id is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandSendReceipt with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.producer_id != Default::default() {
    json["producerId"] = ToJson::to_json(self.producer_id)
  }
  if self.sequence_id != Default::default() {
    json["sequenceId"] = ToJson::to_json(self.sequence_id)
  }
  match self.message_id {
    Some(v) => json["messageId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.highest_sequence_id {
    Some(v) if v != 0 => json["highestSequenceId"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandSendReceipt with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSendReceipt raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandSendReceipt"),
    )
  }
  let message : CommandSendReceipt = Default::default()
  for key, value in obj {
    match (key, value) {
      ("producerId", value) =>
        message.producer_id = @json.from_json(value, path~)
      ("sequenceId", value) =>
        message.sequence_id = @json.from_json(value, path~)
      ("messageId", value) =>
        message.message_id = Some(@json.from_json(value, path~))
      ("highestSequenceId", value) =>
        message.highest_sequence_id = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandSendReceipt with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandSendReceipt raise {
  let msg : CommandSendReceipt = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.sequence_id = reader |> @lib.async_read_uint64()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.async_read_message() : MessageIdData)
          |> Some
      (4, _) =>
        msg.highest_sequence_id = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandSendReceipt with fn write(
  self : CommandSendReceipt,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.producer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.sequence_id)
  if self.message_id is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(v)
  }
  if self.highest_sequence_id is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandSendError {
  mut producer_id : UInt64
  mut sequence_id : UInt64
  mut error : ServerError
  mut message : String
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandSendError with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.producer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.sequence_id))
  size += @lib.size_of_field(3U, @lib.size_of(self.error))
  size += @lib.size_of_length_delimited_field(4U, @lib.size_of(self.message))
  size
}

///|
pub impl Default for CommandSendError with fn default() -> CommandSendError {
  CommandSendError::{
    producer_id: Default::default(),
    sequence_id: Default::default(),
    error: Default::default(),
    message: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandSendError::CommandSendError(
  producer_id : UInt64,
  sequence_id : UInt64,
  error : ServerError,
  message : String,
) -> CommandSendError {
  CommandSendError::{ producer_id, sequence_id, error, message, }
}

///|
pub impl @lib.Read for CommandSendError with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandSendError raise {
  let msg : CommandSendError = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.read_uint64()
      (2, _) => msg.sequence_id = reader |> @lib.read_uint64()
      (3, _) => msg.error = reader |> @lib.read_enum() |> ServerError::from_enum
      (4, _) => msg.message = reader |> @lib.read_string()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandSendError with fn write(
  self : CommandSendError,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.producer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.sequence_id)
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_enum(self.error.to_enum())
  writer |> @lib.write_varint(34UL)
  writer |> @lib.write_string(self.message)
}

///|
pub impl ToJson for CommandSendError with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.producer_id != Default::default() {
    json["producerId"] = ToJson::to_json(self.producer_id)
  }
  if self.sequence_id != Default::default() {
    json["sequenceId"] = ToJson::to_json(self.sequence_id)
  }
  if self.error != Default::default() {
    json["error"] = ToJson::to_json(self.error)
  }
  if self.message != Default::default() {
    json["message"] = ToJson::to_json(self.message)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandSendError with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSendError raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandSendError"),
    )
  }
  let message : CommandSendError = Default::default()
  for key, value in obj {
    match (key, value) {
      ("producerId", value) =>
        message.producer_id = @json.from_json(value, path~)
      ("sequenceId", value) =>
        message.sequence_id = @json.from_json(value, path~)
      ("error", value) => message.error = @json.from_json(value, path~)
      ("message", value) => message.message = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandSendError with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandSendError raise {
  let msg : CommandSendError = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.sequence_id = reader |> @lib.async_read_uint64()
      (3, _) =>
        msg.error = reader |> @lib.async_read_enum() |> ServerError::from_enum
      (4, _) => msg.message = reader |> @lib.async_read_string()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandSendError with fn write(
  self : CommandSendError,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.producer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.sequence_id)
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_enum(self.error.to_enum())
  writer |> @lib.async_write_varint(34UL)
  writer |> @lib.async_write_string(self.message)
}

///|
pub(all) struct CommandMessage {
  mut consumer_id : UInt64
  mut message_id : MessageIdData
  mut redelivery_count : UInt?
  mut ack_set : Array[Int64]
  mut consumer_epoch : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandMessage with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.message_id))
  if self.redelivery_count is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  for s in self.ack_set {
    size += @lib.size_of_field(4U, @lib.size_of(s))
  }
  if self.consumer_epoch is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandMessage with fn default() -> CommandMessage {
  CommandMessage::{
    consumer_id: Default::default(),
    message_id: Default::default(),
    redelivery_count: Some(0),
    ack_set: [],
    consumer_epoch: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandMessage::CommandMessage(
  consumer_id : UInt64,
  message_id : MessageIdData,
  redelivery_count? : UInt,
  ack_set : Array[Int64],
  consumer_epoch? : UInt64,
) -> CommandMessage {
  CommandMessage::{
    consumer_id,
    message_id,
    redelivery_count,
    ack_set,
    consumer_epoch,
  }
}

///|
pub impl @lib.Read for CommandMessage with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandMessage raise {
  let msg : CommandMessage = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.read_message() : MessageIdData)
      (3, _) => msg.redelivery_count = reader |> @lib.read_uint32() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack_set.push_iter(
          (reader |> @lib.read_packed(@lib.read_int64, None)).iter(),
        )
      (4, @lib.WIRE_TYPE_VARINT) =>
        msg.ack_set.push(reader |> @lib.read_int64())
      (5, _) => msg.consumer_epoch = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandMessage with fn write(
  self : CommandMessage,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_message(self.message_id)
  if self.redelivery_count is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint32(v)
  }
  for item in self.ack_set {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_int64(item)
  }
  if self.consumer_epoch is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandMessage with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.message_id != Default::default() {
    json["messageId"] = ToJson::to_json(self.message_id)
  }
  match self.redelivery_count {
    Some(v) if v != 0 => json["redeliveryCount"] = ToJson::to_json(v)
    _ => ()
  }
  if self.ack_set != Default::default() {
    json["ackSet"] = ToJson::to_json(self.ack_set)
  }
  match self.consumer_epoch {
    Some(v) => json["consumerEpoch"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandMessage with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandMessage raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandMessage"))
  }
  let message : CommandMessage = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("messageId", value) => message.message_id = @json.from_json(value, path~)
      ("redeliveryCount", value) =>
        message.redelivery_count = Some(@json.from_json(value, path~))
      ("ackSet", Array(value)) =>
        message.ack_set = value.map(v => @json.from_json(v, path~))
      ("consumerEpoch", value) =>
        message.consumer_epoch = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandMessage with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandMessage raise {
  let msg : CommandMessage = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.async_read_message() : MessageIdData)
      (3, _) =>
        msg.redelivery_count = reader |> @lib.async_read_uint32() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack_set.push_iter(
          (reader |> @lib.async_read_packed(@lib.async_read_int64, None)).iter(),
        )
      (4, @lib.WIRE_TYPE_VARINT) =>
        msg.ack_set.push(reader |> @lib.async_read_int64())
      (5, _) => msg.consumer_epoch = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandMessage with fn write(
  self : CommandMessage,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_message(self.message_id)
  if self.redelivery_count is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint32(v)
  }
  for item in self.ack_set {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_int64(item)
  }
  if self.consumer_epoch is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) enum CommandAck_AckType {
  Individual
  Cumulative
} derive(Eq, Debug)

///|
pub fn CommandAck_AckType::to_enum(self : CommandAck_AckType) -> @lib.Enum {
  match self {
    CommandAck_AckType::Individual => 0
    CommandAck_AckType::Cumulative => 1
  }
}

///|
pub fn CommandAck_AckType::from_enum(i : @lib.Enum) -> CommandAck_AckType {
  match i.0 {
    0 => CommandAck_AckType::Individual
    1 => CommandAck_AckType::Cumulative
    _ => Default::default()
  }
}

///|
pub impl Default for CommandAck_AckType with fn default() -> CommandAck_AckType {
  CommandAck_AckType::Individual
}

///|
pub impl @lib.Sized for CommandAck_AckType with fn size_of(
  self : CommandAck_AckType,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandAck_AckType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAck_AckType raise {
  match json {
    String("Individual") => CommandAck_AckType::Individual
    String("Cumulative") => CommandAck_AckType::Cumulative
    Number(0, ..) => CommandAck_AckType::Individual
    Number(1, ..) => CommandAck_AckType::Cumulative
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandAck_AckType with fn to_json(
  self : CommandAck_AckType,
) -> Json {
  match self {
    CommandAck_AckType::Individual => "Individual"
    CommandAck_AckType::Cumulative => "Cumulative"
  }
}

///|
pub(all) enum CommandAck_ValidationError {
  UncompressedSizeCorruption
  DecompressionError
  ChecksumMismatch
  BatchDeSerializeError
  DecryptionError
} derive(Eq, Debug)

///|
pub fn CommandAck_ValidationError::to_enum(
  self : CommandAck_ValidationError,
) -> @lib.Enum {
  match self {
    CommandAck_ValidationError::UncompressedSizeCorruption => 0
    CommandAck_ValidationError::DecompressionError => 1
    CommandAck_ValidationError::ChecksumMismatch => 2
    CommandAck_ValidationError::BatchDeSerializeError => 3
    CommandAck_ValidationError::DecryptionError => 4
  }
}

///|
pub fn CommandAck_ValidationError::from_enum(
  i : @lib.Enum,
) -> CommandAck_ValidationError {
  match i.0 {
    0 => CommandAck_ValidationError::UncompressedSizeCorruption
    1 => CommandAck_ValidationError::DecompressionError
    2 => CommandAck_ValidationError::ChecksumMismatch
    3 => CommandAck_ValidationError::BatchDeSerializeError
    4 => CommandAck_ValidationError::DecryptionError
    _ => Default::default()
  }
}

///|
pub impl Default for CommandAck_ValidationError with fn default() -> CommandAck_ValidationError {
  CommandAck_ValidationError::UncompressedSizeCorruption
}

///|
pub impl @lib.Sized for CommandAck_ValidationError with fn size_of(
  self : CommandAck_ValidationError,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandAck_ValidationError with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAck_ValidationError raise {
  match json {
    String("UncompressedSizeCorruption") =>
      CommandAck_ValidationError::UncompressedSizeCorruption
    String("DecompressionError") =>
      CommandAck_ValidationError::DecompressionError
    String("ChecksumMismatch") => CommandAck_ValidationError::ChecksumMismatch
    String("BatchDeSerializeError") =>
      CommandAck_ValidationError::BatchDeSerializeError
    String("DecryptionError") => CommandAck_ValidationError::DecryptionError
    Number(0, ..) => CommandAck_ValidationError::UncompressedSizeCorruption
    Number(1, ..) => CommandAck_ValidationError::DecompressionError
    Number(2, ..) => CommandAck_ValidationError::ChecksumMismatch
    Number(3, ..) => CommandAck_ValidationError::BatchDeSerializeError
    Number(4, ..) => CommandAck_ValidationError::DecryptionError
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandAck_ValidationError with fn to_json(
  self : CommandAck_ValidationError,
) -> Json {
  match self {
    CommandAck_ValidationError::UncompressedSizeCorruption =>
      "UncompressedSizeCorruption"
    CommandAck_ValidationError::DecompressionError => "DecompressionError"
    CommandAck_ValidationError::ChecksumMismatch => "ChecksumMismatch"
    CommandAck_ValidationError::BatchDeSerializeError => "BatchDeSerializeError"
    CommandAck_ValidationError::DecryptionError => "DecryptionError"
  }
}

///|
pub(all) struct CommandAck {
  mut consumer_id : UInt64
  mut ack_type : CommandAck_AckType
  mut message_id : Array[MessageIdData]
  mut validation_error : CommandAck_ValidationError?
  mut properties : Array[KeyLongValue]
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut request_id : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAck with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.ack_type))
  for s in self.message_id {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  if self.validation_error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  for s in self.properties {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(s))
  }
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(7U, @lib.size_of(v))
  }
  if self.request_id is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAck with fn default() -> CommandAck {
  CommandAck::{
    consumer_id: Default::default(),
    ack_type: Default::default(),
    message_id: [],
    validation_error: None,
    properties: [],
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    request_id: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandAck::CommandAck(
  consumer_id : UInt64,
  ack_type : CommandAck_AckType,
  message_id : Array[MessageIdData],
  validation_error? : CommandAck_ValidationError,
  properties : Array[KeyLongValue],
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  request_id? : UInt64,
) -> CommandAck {
  CommandAck::{
    consumer_id,
    ack_type,
    message_id,
    validation_error,
    properties,
    txnid_least_bits,
    txnid_most_bits,
    request_id,
  }
}

///|
pub impl @lib.Read for CommandAck with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAck raise {
  let msg : CommandAck = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.ack_type = reader
          |> @lib.read_enum()
          |> CommandAck_AckType::from_enum
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id.push((reader |> @lib.read_message() : MessageIdData))
      (4, _) =>
        msg.validation_error = reader
          |> @lib.read_enum()
          |> CommandAck_ValidationError::from_enum
          |> Some
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.read_message() : KeyLongValue))
      (6, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (7, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (8, _) => msg.request_id = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAck with fn write(
  self : CommandAck,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_enum(self.ack_type.to_enum())
  for item in self.message_id {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(item)
  }
  if self.validation_error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  for item in self.properties {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_message(item)
  }
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(56UL)
    writer |> @lib.write_uint64(v)
  }
  if self.request_id is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandAck with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.ack_type != Default::default() {
    json["ackType"] = ToJson::to_json(self.ack_type)
  }
  if self.message_id != Default::default() {
    json["messageId"] = ToJson::to_json(self.message_id)
  }
  match self.validation_error {
    Some(v) => json["validationError"] = ToJson::to_json(v)
    _ => ()
  }
  if self.properties != Default::default() {
    json["properties"] = ToJson::to_json(self.properties)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.request_id {
    Some(v) => json["requestId"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAck with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAck raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandAck"))
  }
  let message : CommandAck = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("ackType", value) => message.ack_type = @json.from_json(value, path~)
      ("messageId", Array(value)) =>
        message.message_id = value.map(v => @json.from_json(v, path~))
      ("validationError", value) =>
        message.validation_error = Some(@json.from_json(value, path~))
      ("properties", Array(value)) =>
        message.properties = value.map(v => @json.from_json(v, path~))
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("requestId", value) =>
        message.request_id = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAck with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAck raise {
  let msg : CommandAck = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.ack_type = reader
          |> @lib.async_read_enum()
          |> CommandAck_AckType::from_enum
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id.push(
          (reader |> @lib.async_read_message() : MessageIdData),
        )
      (4, _) =>
        msg.validation_error = reader
          |> @lib.async_read_enum()
          |> CommandAck_ValidationError::from_enum
          |> Some
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push(
          (reader |> @lib.async_read_message() : KeyLongValue),
        )
      (6, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (7, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (8, _) => msg.request_id = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAck with fn write(
  self : CommandAck,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_enum(self.ack_type.to_enum())
  for item in self.message_id {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(item)
  }
  if self.validation_error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  for item in self.properties {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_message(item)
  }
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(56UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.request_id is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandAckResponse {
  mut consumer_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
  mut request_id : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAckResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  if self.request_id is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAckResponse with fn default() -> CommandAckResponse {
  CommandAckResponse::{
    consumer_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
    request_id: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandAckResponse::CommandAckResponse(
  consumer_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
  request_id? : UInt64,
) -> CommandAckResponse {
  CommandAckResponse::{
    consumer_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
    request_id,
  }
}

///|
pub impl @lib.Read for CommandAckResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAckResponse raise {
  let msg : CommandAckResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      (6, _) => msg.request_id = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAckResponse with fn write(
  self : CommandAckResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  if self.request_id is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandAckResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  match self.request_id {
    Some(v) => json["requestId"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAckResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAckResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAckResponse"),
    )
  }
  let message : CommandAckResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      ("requestId", value) =>
        message.request_id = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAckResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAckResponse raise {
  let msg : CommandAckResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      (6, _) => msg.request_id = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAckResponse with fn write(
  self : CommandAckResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  if self.request_id is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandActiveConsumerChange {
  mut consumer_id : UInt64
  mut is_active : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandActiveConsumerChange with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  if self.is_active is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandActiveConsumerChange with fn default() -> CommandActiveConsumerChange {
  CommandActiveConsumerChange::{
    consumer_id: Default::default(),
    is_active: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandActiveConsumerChange::CommandActiveConsumerChange(
  consumer_id : UInt64,
  is_active? : Bool,
) -> CommandActiveConsumerChange {
  CommandActiveConsumerChange::{ consumer_id, is_active, }
}

///|
pub impl @lib.Read for CommandActiveConsumerChange with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandActiveConsumerChange raise {
  let msg : CommandActiveConsumerChange = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.is_active = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandActiveConsumerChange with fn write(
  self : CommandActiveConsumerChange,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  if self.is_active is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandActiveConsumerChange with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  match self.is_active {
    Some(v) if v != false => json["isActive"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandActiveConsumerChange with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandActiveConsumerChange raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandActiveConsumerChange"),
    )
  }
  let message : CommandActiveConsumerChange = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("isActive", value) =>
        message.is_active = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandActiveConsumerChange with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandActiveConsumerChange raise {
  let msg : CommandActiveConsumerChange = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.is_active = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandActiveConsumerChange with fn write(
  self : CommandActiveConsumerChange,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  if self.is_active is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandFlow {
  mut consumer_id : UInt64
  mut message_permits : UInt
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandFlow with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.message_permits))
  size
}

///|
pub impl Default for CommandFlow with fn default() -> CommandFlow {
  CommandFlow::{
    consumer_id: Default::default(),
    message_permits: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandFlow::CommandFlow(
  consumer_id : UInt64,
  message_permits : UInt,
) -> CommandFlow {
  CommandFlow::{ consumer_id, message_permits, }
}

///|
pub impl @lib.Read for CommandFlow with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandFlow raise {
  let msg : CommandFlow = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.message_permits = reader |> @lib.read_uint32()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandFlow with fn write(
  self : CommandFlow,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint32(self.message_permits)
}

///|
pub impl ToJson for CommandFlow with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.message_permits != Default::default() {
    json["messagePermits"] = ToJson::to_json(self.message_permits)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandFlow with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandFlow raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandFlow"))
  }
  let message : CommandFlow = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("messagePermits", value) =>
        message.message_permits = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandFlow with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandFlow raise {
  let msg : CommandFlow = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.message_permits = reader |> @lib.async_read_uint32()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandFlow with fn write(
  self : CommandFlow,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint32(self.message_permits)
}

///|
pub(all) struct CommandUnsubscribe {
  mut consumer_id : UInt64
  mut request_id : UInt64
  mut force : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandUnsubscribe with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.force is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandUnsubscribe with fn default() -> CommandUnsubscribe {
  CommandUnsubscribe::{
    consumer_id: Default::default(),
    request_id: Default::default(),
    force: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandUnsubscribe::CommandUnsubscribe(
  consumer_id : UInt64,
  request_id : UInt64,
  force? : Bool,
) -> CommandUnsubscribe {
  CommandUnsubscribe::{ consumer_id, request_id, force, }
}

///|
pub impl @lib.Read for CommandUnsubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandUnsubscribe raise {
  let msg : CommandUnsubscribe = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, _) => msg.force = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandUnsubscribe with fn write(
  self : CommandUnsubscribe,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.force is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandUnsubscribe with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.force {
    Some(v) if v != false => json["force"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandUnsubscribe with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandUnsubscribe raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandUnsubscribe"),
    )
  }
  let message : CommandUnsubscribe = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("force", value) => message.force = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandUnsubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandUnsubscribe raise {
  let msg : CommandUnsubscribe = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.force = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandUnsubscribe with fn write(
  self : CommandUnsubscribe,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.force is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandSeek {
  mut consumer_id : UInt64
  mut request_id : UInt64
  mut message_id : MessageIdData?
  mut message_publish_time : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandSeek with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.message_id is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.message_publish_time is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandSeek with fn default() -> CommandSeek {
  CommandSeek::{
    consumer_id: Default::default(),
    request_id: Default::default(),
    message_id: None,
    message_publish_time: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandSeek::CommandSeek(
  consumer_id : UInt64,
  request_id : UInt64,
  message_id? : MessageIdData,
  message_publish_time? : UInt64,
) -> CommandSeek {
  CommandSeek::{ consumer_id, request_id, message_id, message_publish_time, }
}

///|
pub impl @lib.Read for CommandSeek with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandSeek raise {
  let msg : CommandSeek = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.read_message() : MessageIdData) |> Some
      (4, _) => msg.message_publish_time = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandSeek with fn write(
  self : CommandSeek,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.message_id is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(v)
  }
  if self.message_publish_time is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandSeek with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.message_id {
    Some(v) => json["messageId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message_publish_time {
    Some(v) => json["messagePublishTime"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandSeek with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSeek raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandSeek"))
  }
  let message : CommandSeek = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("messageId", value) =>
        message.message_id = Some(@json.from_json(value, path~))
      ("messagePublishTime", value) =>
        message.message_publish_time = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandSeek with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandSeek raise {
  let msg : CommandSeek = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_id = (reader |> @lib.async_read_message() : MessageIdData)
          |> Some
      (4, _) =>
        msg.message_publish_time = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandSeek with fn write(
  self : CommandSeek,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.message_id is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(v)
  }
  if self.message_publish_time is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandReachedEndOfTopic {
  mut consumer_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandReachedEndOfTopic with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size
}

///|
pub impl Default for CommandReachedEndOfTopic with fn default() -> CommandReachedEndOfTopic {
  CommandReachedEndOfTopic::{ consumer_id: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn CommandReachedEndOfTopic::CommandReachedEndOfTopic(
  consumer_id : UInt64,
) -> CommandReachedEndOfTopic {
  CommandReachedEndOfTopic::{ consumer_id, }
}

///|
pub impl @lib.Read for CommandReachedEndOfTopic with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandReachedEndOfTopic raise {
  let msg : CommandReachedEndOfTopic = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandReachedEndOfTopic with fn write(
  self : CommandReachedEndOfTopic,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
}

///|
pub impl ToJson for CommandReachedEndOfTopic with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandReachedEndOfTopic with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandReachedEndOfTopic raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandReachedEndOfTopic"),
    )
  }
  let message : CommandReachedEndOfTopic = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandReachedEndOfTopic with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandReachedEndOfTopic raise {
  let msg : CommandReachedEndOfTopic = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandReachedEndOfTopic with fn write(
  self : CommandReachedEndOfTopic,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
}

///|
pub(all) enum CommandTopicMigrated_ResourceType {
  Producer
  Consumer
} derive(Eq, Debug)

///|
pub fn CommandTopicMigrated_ResourceType::to_enum(
  self : CommandTopicMigrated_ResourceType,
) -> @lib.Enum {
  match self {
    CommandTopicMigrated_ResourceType::Producer => 0
    CommandTopicMigrated_ResourceType::Consumer => 1
  }
}

///|
pub fn CommandTopicMigrated_ResourceType::from_enum(
  i : @lib.Enum,
) -> CommandTopicMigrated_ResourceType {
  match i.0 {
    0 => CommandTopicMigrated_ResourceType::Producer
    1 => CommandTopicMigrated_ResourceType::Consumer
    _ => Default::default()
  }
}

///|
pub impl Default for CommandTopicMigrated_ResourceType with fn default() -> CommandTopicMigrated_ResourceType {
  CommandTopicMigrated_ResourceType::Producer
}

///|
pub impl @lib.Sized for CommandTopicMigrated_ResourceType with fn size_of(
  self : CommandTopicMigrated_ResourceType,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandTopicMigrated_ResourceType with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandTopicMigrated_ResourceType raise {
  match json {
    String("Producer") => CommandTopicMigrated_ResourceType::Producer
    String("Consumer") => CommandTopicMigrated_ResourceType::Consumer
    Number(0, ..) => CommandTopicMigrated_ResourceType::Producer
    Number(1, ..) => CommandTopicMigrated_ResourceType::Consumer
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandTopicMigrated_ResourceType with fn to_json(
  self : CommandTopicMigrated_ResourceType,
) -> Json {
  match self {
    CommandTopicMigrated_ResourceType::Producer => "Producer"
    CommandTopicMigrated_ResourceType::Consumer => "Consumer"
  }
}

///|
pub(all) struct CommandTopicMigrated {
  mut resource_id : UInt64
  mut resource_type : CommandTopicMigrated_ResourceType
  mut broker_service_url : String?
  mut broker_service_url_tls : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandTopicMigrated with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.resource_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.resource_type))
  if self.broker_service_url is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.broker_service_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandTopicMigrated with fn default() -> CommandTopicMigrated {
  CommandTopicMigrated::{
    resource_id: Default::default(),
    resource_type: Default::default(),
    broker_service_url: None,
    broker_service_url_tls: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandTopicMigrated::CommandTopicMigrated(
  resource_id : UInt64,
  resource_type : CommandTopicMigrated_ResourceType,
  broker_service_url? : String,
  broker_service_url_tls? : String,
) -> CommandTopicMigrated {
  CommandTopicMigrated::{
    resource_id,
    resource_type,
    broker_service_url,
    broker_service_url_tls,
  }
}

///|
pub impl @lib.Read for CommandTopicMigrated with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandTopicMigrated raise {
  let msg : CommandTopicMigrated = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.resource_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.resource_type = reader
          |> @lib.read_enum()
          |> CommandTopicMigrated_ResourceType::from_enum
      (3, _) => msg.broker_service_url = reader |> @lib.read_string() |> Some
      (4, _) =>
        msg.broker_service_url_tls = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandTopicMigrated with fn write(
  self : CommandTopicMigrated,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.resource_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_enum(self.resource_type.to_enum())
  if self.broker_service_url is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.broker_service_url_tls is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandTopicMigrated with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.resource_id != Default::default() {
    json["resourceId"] = ToJson::to_json(self.resource_id)
  }
  if self.resource_type != Default::default() {
    json["resourceType"] = ToJson::to_json(self.resource_type)
  }
  match self.broker_service_url {
    Some(v) => json["brokerServiceUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.broker_service_url_tls {
    Some(v) => json["brokerServiceUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandTopicMigrated with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandTopicMigrated raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandTopicMigrated"),
    )
  }
  let message : CommandTopicMigrated = Default::default()
  for key, value in obj {
    match (key, value) {
      ("resourceId", value) =>
        message.resource_id = @json.from_json(value, path~)
      ("resourceType", value) =>
        message.resource_type = @json.from_json(value, path~)
      ("brokerServiceUrl", value) =>
        message.broker_service_url = Some(@json.from_json(value, path~))
      ("brokerServiceUrlTls", value) =>
        message.broker_service_url_tls = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandTopicMigrated with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandTopicMigrated raise {
  let msg : CommandTopicMigrated = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.resource_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.resource_type = reader
          |> @lib.async_read_enum()
          |> CommandTopicMigrated_ResourceType::from_enum
      (3, _) =>
        msg.broker_service_url = reader |> @lib.async_read_string() |> Some
      (4, _) =>
        msg.broker_service_url_tls = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandTopicMigrated with fn write(
  self : CommandTopicMigrated,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.resource_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_enum(self.resource_type.to_enum())
  if self.broker_service_url is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.broker_service_url_tls is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandCloseProducer {
  mut producer_id : UInt64
  mut request_id : UInt64
  mut assigned_broker_service_url : String?
  mut assigned_broker_service_url_tls : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandCloseProducer with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.producer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.assigned_broker_service_url is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.assigned_broker_service_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandCloseProducer with fn default() -> CommandCloseProducer {
  CommandCloseProducer::{
    producer_id: Default::default(),
    request_id: Default::default(),
    assigned_broker_service_url: None,
    assigned_broker_service_url_tls: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandCloseProducer::CommandCloseProducer(
  producer_id : UInt64,
  request_id : UInt64,
  assigned_broker_service_url? : String,
  assigned_broker_service_url_tls? : String,
) -> CommandCloseProducer {
  CommandCloseProducer::{
    producer_id,
    request_id,
    assigned_broker_service_url,
    assigned_broker_service_url_tls,
  }
}

///|
pub impl @lib.Read for CommandCloseProducer with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandCloseProducer raise {
  let msg : CommandCloseProducer = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.read_uint64()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, _) =>
        msg.assigned_broker_service_url = reader |> @lib.read_string() |> Some
      (4, _) =>
        msg.assigned_broker_service_url_tls = reader
          |> @lib.read_string()
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandCloseProducer with fn write(
  self : CommandCloseProducer,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.producer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.assigned_broker_service_url is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.assigned_broker_service_url_tls is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandCloseProducer with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.producer_id != Default::default() {
    json["producerId"] = ToJson::to_json(self.producer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.assigned_broker_service_url {
    Some(v) => json["assignedBrokerServiceUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.assigned_broker_service_url_tls {
    Some(v) => json["assignedBrokerServiceUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandCloseProducer with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandCloseProducer raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandCloseProducer"),
    )
  }
  let message : CommandCloseProducer = Default::default()
  for key, value in obj {
    match (key, value) {
      ("producerId", value) =>
        message.producer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("assignedBrokerServiceUrl", value) =>
        message.assigned_broker_service_url = Some(
          @json.from_json(value, path~),
        )
      ("assignedBrokerServiceUrlTls", value) =>
        message.assigned_broker_service_url_tls = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandCloseProducer with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandCloseProducer raise {
  let msg : CommandCloseProducer = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.producer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, _) =>
        msg.assigned_broker_service_url = reader
          |> @lib.async_read_string()
          |> Some
      (4, _) =>
        msg.assigned_broker_service_url_tls = reader
          |> @lib.async_read_string()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandCloseProducer with fn write(
  self : CommandCloseProducer,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.producer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.assigned_broker_service_url is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.assigned_broker_service_url_tls is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandCloseConsumer {
  mut consumer_id : UInt64
  mut request_id : UInt64
  mut assigned_broker_service_url : String?
  mut assigned_broker_service_url_tls : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandCloseConsumer with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.assigned_broker_service_url is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.assigned_broker_service_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandCloseConsumer with fn default() -> CommandCloseConsumer {
  CommandCloseConsumer::{
    consumer_id: Default::default(),
    request_id: Default::default(),
    assigned_broker_service_url: None,
    assigned_broker_service_url_tls: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandCloseConsumer::CommandCloseConsumer(
  consumer_id : UInt64,
  request_id : UInt64,
  assigned_broker_service_url? : String,
  assigned_broker_service_url_tls? : String,
) -> CommandCloseConsumer {
  CommandCloseConsumer::{
    consumer_id,
    request_id,
    assigned_broker_service_url,
    assigned_broker_service_url_tls,
  }
}

///|
pub impl @lib.Read for CommandCloseConsumer with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandCloseConsumer raise {
  let msg : CommandCloseConsumer = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, _) =>
        msg.assigned_broker_service_url = reader |> @lib.read_string() |> Some
      (4, _) =>
        msg.assigned_broker_service_url_tls = reader
          |> @lib.read_string()
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandCloseConsumer with fn write(
  self : CommandCloseConsumer,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.assigned_broker_service_url is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.assigned_broker_service_url_tls is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandCloseConsumer with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.assigned_broker_service_url {
    Some(v) => json["assignedBrokerServiceUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.assigned_broker_service_url_tls {
    Some(v) => json["assignedBrokerServiceUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandCloseConsumer with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandCloseConsumer raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandCloseConsumer"),
    )
  }
  let message : CommandCloseConsumer = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("assignedBrokerServiceUrl", value) =>
        message.assigned_broker_service_url = Some(
          @json.from_json(value, path~),
        )
      ("assignedBrokerServiceUrlTls", value) =>
        message.assigned_broker_service_url_tls = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandCloseConsumer with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandCloseConsumer raise {
  let msg : CommandCloseConsumer = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, _) =>
        msg.assigned_broker_service_url = reader
          |> @lib.async_read_string()
          |> Some
      (4, _) =>
        msg.assigned_broker_service_url_tls = reader
          |> @lib.async_read_string()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandCloseConsumer with fn write(
  self : CommandCloseConsumer,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.assigned_broker_service_url is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.assigned_broker_service_url_tls is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandRedeliverUnacknowledgedMessages {
  mut consumer_id : UInt64
  mut message_ids : Array[MessageIdData]
  mut consumer_epoch : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandRedeliverUnacknowledgedMessages with fn size_of(
  self,
) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  for s in self.message_ids {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  if self.consumer_epoch is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandRedeliverUnacknowledgedMessages with fn default() -> CommandRedeliverUnacknowledgedMessages {
  CommandRedeliverUnacknowledgedMessages::{
    consumer_id: Default::default(),
    message_ids: [],
    consumer_epoch: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandRedeliverUnacknowledgedMessages::CommandRedeliverUnacknowledgedMessages(
  consumer_id : UInt64,
  message_ids : Array[MessageIdData],
  consumer_epoch? : UInt64,
) -> CommandRedeliverUnacknowledgedMessages {
  CommandRedeliverUnacknowledgedMessages::{
    consumer_id,
    message_ids,
    consumer_epoch,
  }
}

///|
pub impl @lib.Read for CommandRedeliverUnacknowledgedMessages with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandRedeliverUnacknowledgedMessages raise {
  let msg : CommandRedeliverUnacknowledgedMessages = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_ids.push((reader |> @lib.read_message() : MessageIdData))
      (3, _) => msg.consumer_epoch = reader |> @lib.read_uint64() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandRedeliverUnacknowledgedMessages with fn write(
  self : CommandRedeliverUnacknowledgedMessages,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  for item in self.message_ids {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(item)
  }
  if self.consumer_epoch is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandRedeliverUnacknowledgedMessages with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.message_ids != Default::default() {
    json["messageIds"] = ToJson::to_json(self.message_ids)
  }
  match self.consumer_epoch {
    Some(v) => json["consumerEpoch"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandRedeliverUnacknowledgedMessages with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandRedeliverUnacknowledgedMessages raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandRedeliverUnacknowledgedMessages"),
    )
  }
  let message : CommandRedeliverUnacknowledgedMessages = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("messageIds", Array(value)) =>
        message.message_ids = value.map(v => @json.from_json(v, path~))
      ("consumerEpoch", value) =>
        message.consumer_epoch = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandRedeliverUnacknowledgedMessages with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandRedeliverUnacknowledgedMessages raise {
  let msg : CommandRedeliverUnacknowledgedMessages = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message_ids.push(
          (reader |> @lib.async_read_message() : MessageIdData),
        )
      (3, _) => msg.consumer_epoch = reader |> @lib.async_read_uint64() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandRedeliverUnacknowledgedMessages with fn write(
  self : CommandRedeliverUnacknowledgedMessages,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  for item in self.message_ids {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(item)
  }
  if self.consumer_epoch is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandSuccess {
  mut request_id : UInt64
  mut schema : Schema?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandSuccess with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.schema is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandSuccess with fn default() -> CommandSuccess {
  CommandSuccess::{ request_id: Default::default(), schema: None, }
}

///|
#alias(new, deprecated)
pub fn CommandSuccess::CommandSuccess(
  request_id : UInt64,
  schema? : Schema,
) -> CommandSuccess {
  CommandSuccess::{ request_id, schema, }
}

///|
pub impl @lib.Read for CommandSuccess with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandSuccess raise {
  let msg : CommandSuccess = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.read_message() : Schema) |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandSuccess with fn write(
  self : CommandSuccess,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.schema is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for CommandSuccess with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.schema {
    Some(v) => json["schema"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandSuccess with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandSuccess raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandSuccess"))
  }
  let message : CommandSuccess = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("schema", value) => message.schema = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandSuccess with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandSuccess raise {
  let msg : CommandSuccess = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.async_read_message() : Schema) |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandSuccess with fn write(
  self : CommandSuccess,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.schema is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(v)
  }
}

///|
pub(all) struct CommandProducerSuccess {
  mut request_id : UInt64
  mut producer_name : String
  mut last_sequence_id : Int64?
  mut schema_version : Bytes?
  mut topic_epoch : UInt64?
  mut producer_ready : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandProducerSuccess with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_length_delimited_field(
    2U,
    @lib.size_of(self.producer_name),
  )
  if self.last_sequence_id is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.schema_version is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.topic_epoch is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.producer_ready is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandProducerSuccess with fn default() -> CommandProducerSuccess {
  CommandProducerSuccess::{
    request_id: Default::default(),
    producer_name: Default::default(),
    last_sequence_id: Some(-1),
    schema_version: None,
    topic_epoch: None,
    producer_ready: Some(true),
  }
}

///|
#alias(new, deprecated)
pub fn CommandProducerSuccess::CommandProducerSuccess(
  request_id : UInt64,
  producer_name : String,
  last_sequence_id? : Int64,
  schema_version? : Bytes,
  topic_epoch? : UInt64,
  producer_ready? : Bool,
) -> CommandProducerSuccess {
  CommandProducerSuccess::{
    request_id,
    producer_name,
    last_sequence_id,
    schema_version,
    topic_epoch,
    producer_ready,
  }
}

///|
pub impl @lib.Read for CommandProducerSuccess with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandProducerSuccess raise {
  let msg : CommandProducerSuccess = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.producer_name = reader |> @lib.read_string()
      (3, _) => msg.last_sequence_id = reader |> @lib.read_int64() |> Some
      (4, _) => msg.schema_version = reader |> @lib.read_bytes() |> Some
      (5, _) => msg.topic_epoch = reader |> @lib.read_uint64() |> Some
      (6, _) => msg.producer_ready = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandProducerSuccess with fn write(
  self : CommandProducerSuccess,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.producer_name)
  if self.last_sequence_id is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_int64(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_bytes(v)
  }
  if self.topic_epoch is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_uint64(v)
  }
  if self.producer_ready is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandProducerSuccess with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.producer_name != Default::default() {
    json["producerName"] = ToJson::to_json(self.producer_name)
  }
  match self.last_sequence_id {
    Some(v) if v != -1 => json["lastSequenceId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.schema_version {
    Some(v) => json["schemaVersion"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  match self.topic_epoch {
    Some(v) => json["topicEpoch"] = ToJson::to_json(v)
    _ => ()
  }
  match self.producer_ready {
    Some(v) if v != true => json["producerReady"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandProducerSuccess with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandProducerSuccess raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandProducerSuccess"),
    )
  }
  let message : CommandProducerSuccess = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("producerName", value) =>
        message.producer_name = @json.from_json(value, path~)
      ("lastSequenceId", value) =>
        message.last_sequence_id = Some(@json.from_json(value, path~))
      ("schemaVersion", String(value)) =>
        message.schema_version = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      ("topicEpoch", value) =>
        message.topic_epoch = Some(@json.from_json(value, path~))
      ("producerReady", value) =>
        message.producer_ready = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandProducerSuccess with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandProducerSuccess raise {
  let msg : CommandProducerSuccess = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.producer_name = reader |> @lib.async_read_string()
      (3, _) => msg.last_sequence_id = reader |> @lib.async_read_int64() |> Some
      (4, _) => msg.schema_version = reader |> @lib.async_read_bytes() |> Some
      (5, _) => msg.topic_epoch = reader |> @lib.async_read_uint64() |> Some
      (6, _) => msg.producer_ready = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandProducerSuccess with fn write(
  self : CommandProducerSuccess,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.producer_name)
  if self.last_sequence_id is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_int64(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_bytes(v)
  }
  if self.topic_epoch is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.producer_ready is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandError {
  mut request_id : UInt64
  mut error : ServerError
  mut message : String
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandError with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.error))
  size += @lib.size_of_length_delimited_field(3U, @lib.size_of(self.message))
  size
}

///|
pub impl Default for CommandError with fn default() -> CommandError {
  CommandError::{
    request_id: Default::default(),
    error: Default::default(),
    message: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandError::CommandError(
  request_id : UInt64,
  error : ServerError,
  message : String,
) -> CommandError {
  CommandError::{ request_id, error, message, }
}

///|
pub impl @lib.Read for CommandError with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandError raise {
  let msg : CommandError = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.error = reader |> @lib.read_enum() |> ServerError::from_enum
      (3, _) => msg.message = reader |> @lib.read_string()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandError with fn write(
  self : CommandError,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_enum(self.error.to_enum())
  writer |> @lib.write_varint(26UL)
  writer |> @lib.write_string(self.message)
}

///|
pub impl ToJson for CommandError with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.error != Default::default() {
    json["error"] = ToJson::to_json(self.error)
  }
  if self.message != Default::default() {
    json["message"] = ToJson::to_json(self.message)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandError with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandError raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandError"))
  }
  let message : CommandError = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("error", value) => message.error = @json.from_json(value, path~)
      ("message", value) => message.message = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandError with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandError raise {
  let msg : CommandError = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.error = reader |> @lib.async_read_enum() |> ServerError::from_enum
      (3, _) => msg.message = reader |> @lib.async_read_string()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandError with fn write(
  self : CommandError,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_enum(self.error.to_enum())
  writer |> @lib.async_write_varint(26UL)
  writer |> @lib.async_write_string(self.message)
}

///|
pub(all) struct CommandPing {} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandPing with fn size_of(_) {
  0
}

///|
pub impl Default for CommandPing with fn default() -> CommandPing {
  CommandPing::{ }
}

///|
#alias(new, deprecated)
pub fn CommandPing::CommandPing() -> CommandPing {
  CommandPing::{ }
}

///|
pub impl @lib.Read for CommandPing with fn read_with_limit(_) -> CommandPing noraise {
  Default::default()
}

///|
pub impl @lib.Write for CommandPing with fn write(_, _) -> Unit noraise {

}

///|
pub impl ToJson for CommandPing with fn to_json(_) {
  {}
}

///|
pub impl @json.FromJson for CommandPing with fn from_json(_, _) -> CommandPing noraise {
  Default::default()
}

///|
pub impl @lib.AsyncRead for CommandPing with fn read_with_limit(_) -> CommandPing noraise {
  Default::default()
}

///|
pub impl @lib.AsyncWrite for CommandPing with fn write(_, _) -> Unit noraise {

}

///|
pub(all) struct CommandPong {} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandPong with fn size_of(_) {
  0
}

///|
pub impl Default for CommandPong with fn default() -> CommandPong {
  CommandPong::{ }
}

///|
#alias(new, deprecated)
pub fn CommandPong::CommandPong() -> CommandPong {
  CommandPong::{ }
}

///|
pub impl @lib.Read for CommandPong with fn read_with_limit(_) -> CommandPong noraise {
  Default::default()
}

///|
pub impl @lib.Write for CommandPong with fn write(_, _) -> Unit noraise {

}

///|
pub impl ToJson for CommandPong with fn to_json(_) {
  {}
}

///|
pub impl @json.FromJson for CommandPong with fn from_json(_, _) -> CommandPong noraise {
  Default::default()
}

///|
pub impl @lib.AsyncRead for CommandPong with fn read_with_limit(_) -> CommandPong noraise {
  Default::default()
}

///|
pub impl @lib.AsyncWrite for CommandPong with fn write(_, _) -> Unit noraise {

}

///|
pub(all) struct CommandConsumerStats {
  mut request_id : UInt64
  mut consumer_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandConsumerStats with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(4U, @lib.size_of(self.consumer_id))
  size
}

///|
pub impl Default for CommandConsumerStats with fn default() -> CommandConsumerStats {
  CommandConsumerStats::{
    request_id: Default::default(),
    consumer_id: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandConsumerStats::CommandConsumerStats(
  request_id : UInt64,
  consumer_id : UInt64,
) -> CommandConsumerStats {
  CommandConsumerStats::{ request_id, consumer_id, }
}

///|
pub impl @lib.Read for CommandConsumerStats with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandConsumerStats raise {
  let msg : CommandConsumerStats = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (4, _) => msg.consumer_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandConsumerStats with fn write(
  self : CommandConsumerStats,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(32UL)
  writer |> @lib.write_uint64(self.consumer_id)
}

///|
pub impl ToJson for CommandConsumerStats with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandConsumerStats with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandConsumerStats raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandConsumerStats"),
    )
  }
  let message : CommandConsumerStats = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandConsumerStats with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandConsumerStats raise {
  let msg : CommandConsumerStats = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (4, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandConsumerStats with fn write(
  self : CommandConsumerStats,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(32UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
}

///|
pub(all) struct CommandConsumerStatsResponse {
  mut request_id : UInt64
  mut error_code : ServerError?
  mut error_message : String?
  mut msg_rate_out : Double?
  mut msg_throughput_out : Double?
  mut msg_rate_redeliver : Double?
  mut consumer_name : String?
  mut available_permits : UInt64?
  mut unacked_messages : UInt64?
  mut blocked_consumer_on_unacked_msgs : Bool?
  mut address : String?
  mut connected_since : String?
  mut type_ : String?
  mut msg_rate_expired : Double?
  mut msg_backlog : UInt64?
  mut message_ack_rate : Double?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandConsumerStatsResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.error_code is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.error_message is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.msg_rate_out is Some(_) {
    size += @lib.size_of_field(4U, 8U)
  }
  if self.msg_throughput_out is Some(_) {
    size += @lib.size_of_field(5U, 8U)
  }
  if self.msg_rate_redeliver is Some(_) {
    size += @lib.size_of_field(6U, 8U)
  }
  if self.consumer_name is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  if self.available_permits is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  if self.unacked_messages is Some(v) {
    size += @lib.size_of_field(9U, @lib.size_of(v))
  }
  if self.blocked_consumer_on_unacked_msgs is Some(v) {
    size += @lib.size_of_field(10U, @lib.size_of(v))
  }
  if self.address is Some(v) {
    size += @lib.size_of_length_delimited_field(11U, @lib.size_of(v))
  }
  if self.connected_since is Some(v) {
    size += @lib.size_of_length_delimited_field(12U, @lib.size_of(v))
  }
  if self.type_ is Some(v) {
    size += @lib.size_of_length_delimited_field(13U, @lib.size_of(v))
  }
  if self.msg_rate_expired is Some(_) {
    size += @lib.size_of_field(14U, 8U)
  }
  if self.msg_backlog is Some(v) {
    size += @lib.size_of_field(15U, @lib.size_of(v))
  }
  if self.message_ack_rate is Some(_) {
    size += @lib.size_of_field(16U, 8U)
  }
  size
}

///|
pub impl Default for CommandConsumerStatsResponse with fn default() -> CommandConsumerStatsResponse {
  CommandConsumerStatsResponse::{
    request_id: Default::default(),
    error_code: None,
    error_message: None,
    msg_rate_out: None,
    msg_throughput_out: None,
    msg_rate_redeliver: None,
    consumer_name: None,
    available_permits: None,
    unacked_messages: None,
    blocked_consumer_on_unacked_msgs: None,
    address: None,
    connected_since: None,
    type_: None,
    msg_rate_expired: None,
    msg_backlog: None,
    message_ack_rate: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandConsumerStatsResponse::CommandConsumerStatsResponse(
  request_id : UInt64,
  error_code? : ServerError,
  error_message? : String,
  msg_rate_out? : Double,
  msg_throughput_out? : Double,
  msg_rate_redeliver? : Double,
  consumer_name? : String,
  available_permits? : UInt64,
  unacked_messages? : UInt64,
  blocked_consumer_on_unacked_msgs? : Bool,
  address? : String,
  connected_since? : String,
  type_? : String,
  msg_rate_expired? : Double,
  msg_backlog? : UInt64,
  message_ack_rate? : Double,
) -> CommandConsumerStatsResponse {
  CommandConsumerStatsResponse::{
    request_id,
    error_code,
    error_message,
    msg_rate_out,
    msg_throughput_out,
    msg_rate_redeliver,
    consumer_name,
    available_permits,
    unacked_messages,
    blocked_consumer_on_unacked_msgs,
    address,
    connected_since,
    type_,
    msg_rate_expired,
    msg_backlog,
    message_ack_rate,
  }
}

///|
pub impl @lib.Read for CommandConsumerStatsResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandConsumerStatsResponse raise {
  let msg : CommandConsumerStatsResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.error_code = reader
          |> @lib.read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.error_message = reader |> @lib.read_string() |> Some
      (4, _) => msg.msg_rate_out = reader |> @lib.read_double() |> Some
      (5, _) => msg.msg_throughput_out = reader |> @lib.read_double() |> Some
      (6, _) => msg.msg_rate_redeliver = reader |> @lib.read_double() |> Some
      (7, _) => msg.consumer_name = reader |> @lib.read_string() |> Some
      (8, _) => msg.available_permits = reader |> @lib.read_uint64() |> Some
      (9, _) => msg.unacked_messages = reader |> @lib.read_uint64() |> Some
      (10, _) =>
        msg.blocked_consumer_on_unacked_msgs = reader
          |> @lib.read_bool()
          |> Some
      (11, _) => msg.address = reader |> @lib.read_string() |> Some
      (12, _) => msg.connected_since = reader |> @lib.read_string() |> Some
      (13, _) => msg.type_ = reader |> @lib.read_string() |> Some
      (14, _) => msg.msg_rate_expired = reader |> @lib.read_double() |> Some
      (15, _) => msg.msg_backlog = reader |> @lib.read_uint64() |> Some
      (16, _) => msg.message_ack_rate = reader |> @lib.read_double() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandConsumerStatsResponse with fn write(
  self : CommandConsumerStatsResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.error_code is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.error_message is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.msg_rate_out is Some(v) {
    writer |> @lib.write_varint(33UL)
    writer |> @lib.write_double(v)
  }
  if self.msg_throughput_out is Some(v) {
    writer |> @lib.write_varint(41UL)
    writer |> @lib.write_double(v)
  }
  if self.msg_rate_redeliver is Some(v) {
    writer |> @lib.write_varint(49UL)
    writer |> @lib.write_double(v)
  }
  if self.consumer_name is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_string(v)
  }
  if self.available_permits is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_uint64(v)
  }
  if self.unacked_messages is Some(v) {
    writer |> @lib.write_varint(72UL)
    writer |> @lib.write_uint64(v)
  }
  if self.blocked_consumer_on_unacked_msgs is Some(v) {
    writer |> @lib.write_varint(80UL)
    writer |> @lib.write_bool(v)
  }
  if self.address is Some(v) {
    writer |> @lib.write_varint(90UL)
    writer |> @lib.write_string(v)
  }
  if self.connected_since is Some(v) {
    writer |> @lib.write_varint(98UL)
    writer |> @lib.write_string(v)
  }
  if self.type_ is Some(v) {
    writer |> @lib.write_varint(106UL)
    writer |> @lib.write_string(v)
  }
  if self.msg_rate_expired is Some(v) {
    writer |> @lib.write_varint(113UL)
    writer |> @lib.write_double(v)
  }
  if self.msg_backlog is Some(v) {
    writer |> @lib.write_varint(120UL)
    writer |> @lib.write_uint64(v)
  }
  if self.message_ack_rate is Some(v) {
    writer |> @lib.write_varint(129UL)
    writer |> @lib.write_double(v)
  }
}

///|
pub impl ToJson for CommandConsumerStatsResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.error_code {
    Some(v) => json["errorCode"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error_message {
    Some(v) => json["errorMessage"] = ToJson::to_json(v)
    _ => ()
  }
  match self.msg_rate_out {
    Some(v) => json["msgRateOut"] = ToJson::to_json(v)
    _ => ()
  }
  match self.msg_throughput_out {
    Some(v) => json["msgThroughputOut"] = ToJson::to_json(v)
    _ => ()
  }
  match self.msg_rate_redeliver {
    Some(v) => json["msgRateRedeliver"] = ToJson::to_json(v)
    _ => ()
  }
  match self.consumer_name {
    Some(v) => json["consumerName"] = ToJson::to_json(v)
    _ => ()
  }
  match self.available_permits {
    Some(v) => json["availablePermits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.unacked_messages {
    Some(v) => json["unackedMessages"] = ToJson::to_json(v)
    _ => ()
  }
  match self.blocked_consumer_on_unacked_msgs {
    Some(v) => json["blockedConsumerOnUnackedMsgs"] = ToJson::to_json(v)
    _ => ()
  }
  match self.address {
    Some(v) => json["address"] = ToJson::to_json(v)
    _ => ()
  }
  match self.connected_since {
    Some(v) => json["connectedSince"] = ToJson::to_json(v)
    _ => ()
  }
  match self.type_ {
    Some(v) => json["type"] = ToJson::to_json(v)
    _ => ()
  }
  match self.msg_rate_expired {
    Some(v) => json["msgRateExpired"] = ToJson::to_json(v)
    _ => ()
  }
  match self.msg_backlog {
    Some(v) => json["msgBacklog"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message_ack_rate {
    Some(v) => json["messageAckRate"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandConsumerStatsResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandConsumerStatsResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandConsumerStatsResponse"),
    )
  }
  let message : CommandConsumerStatsResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("errorCode", value) =>
        message.error_code = Some(@json.from_json(value, path~))
      ("errorMessage", value) =>
        message.error_message = Some(@json.from_json(value, path~))
      ("msgRateOut", value) =>
        message.msg_rate_out = Some(@json.from_json(value, path~))
      ("msgThroughputOut", value) =>
        message.msg_throughput_out = Some(@json.from_json(value, path~))
      ("msgRateRedeliver", value) =>
        message.msg_rate_redeliver = Some(@json.from_json(value, path~))
      ("consumerName", value) =>
        message.consumer_name = Some(@json.from_json(value, path~))
      ("availablePermits", value) =>
        message.available_permits = Some(@json.from_json(value, path~))
      ("unackedMessages", value) =>
        message.unacked_messages = Some(@json.from_json(value, path~))
      ("blockedConsumerOnUnackedMsgs", value) =>
        message.blocked_consumer_on_unacked_msgs = Some(
          @json.from_json(value, path~),
        )
      ("address", value) =>
        message.address = Some(@json.from_json(value, path~))
      ("connectedSince", value) =>
        message.connected_since = Some(@json.from_json(value, path~))
      ("type", value) => message.type_ = Some(@json.from_json(value, path~))
      ("msgRateExpired", value) =>
        message.msg_rate_expired = Some(@json.from_json(value, path~))
      ("msgBacklog", value) =>
        message.msg_backlog = Some(@json.from_json(value, path~))
      ("messageAckRate", value) =>
        message.message_ack_rate = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandConsumerStatsResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandConsumerStatsResponse raise {
  let msg : CommandConsumerStatsResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.error_code = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.error_message = reader |> @lib.async_read_string() |> Some
      (4, _) => msg.msg_rate_out = reader |> @lib.async_read_double() |> Some
      (5, _) =>
        msg.msg_throughput_out = reader |> @lib.async_read_double() |> Some
      (6, _) =>
        msg.msg_rate_redeliver = reader |> @lib.async_read_double() |> Some
      (7, _) => msg.consumer_name = reader |> @lib.async_read_string() |> Some
      (8, _) =>
        msg.available_permits = reader |> @lib.async_read_uint64() |> Some
      (9, _) =>
        msg.unacked_messages = reader |> @lib.async_read_uint64() |> Some
      (10, _) =>
        msg.blocked_consumer_on_unacked_msgs = reader
          |> @lib.async_read_bool()
          |> Some
      (11, _) => msg.address = reader |> @lib.async_read_string() |> Some
      (12, _) =>
        msg.connected_since = reader |> @lib.async_read_string() |> Some
      (13, _) => msg.type_ = reader |> @lib.async_read_string() |> Some
      (14, _) =>
        msg.msg_rate_expired = reader |> @lib.async_read_double() |> Some
      (15, _) => msg.msg_backlog = reader |> @lib.async_read_uint64() |> Some
      (16, _) =>
        msg.message_ack_rate = reader |> @lib.async_read_double() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandConsumerStatsResponse with fn write(
  self : CommandConsumerStatsResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.error_code is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.error_message is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.msg_rate_out is Some(v) {
    writer |> @lib.async_write_varint(33UL)
    writer |> @lib.async_write_double(v)
  }
  if self.msg_throughput_out is Some(v) {
    writer |> @lib.async_write_varint(41UL)
    writer |> @lib.async_write_double(v)
  }
  if self.msg_rate_redeliver is Some(v) {
    writer |> @lib.async_write_varint(49UL)
    writer |> @lib.async_write_double(v)
  }
  if self.consumer_name is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_string(v)
  }
  if self.available_permits is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.unacked_messages is Some(v) {
    writer |> @lib.async_write_varint(72UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.blocked_consumer_on_unacked_msgs is Some(v) {
    writer |> @lib.async_write_varint(80UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.address is Some(v) {
    writer |> @lib.async_write_varint(90UL)
    writer |> @lib.async_write_string(v)
  }
  if self.connected_since is Some(v) {
    writer |> @lib.async_write_varint(98UL)
    writer |> @lib.async_write_string(v)
  }
  if self.type_ is Some(v) {
    writer |> @lib.async_write_varint(106UL)
    writer |> @lib.async_write_string(v)
  }
  if self.msg_rate_expired is Some(v) {
    writer |> @lib.async_write_varint(113UL)
    writer |> @lib.async_write_double(v)
  }
  if self.msg_backlog is Some(v) {
    writer |> @lib.async_write_varint(120UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.message_ack_rate is Some(v) {
    writer |> @lib.async_write_varint(129UL)
    writer |> @lib.async_write_double(v)
  }
}

///|
pub(all) struct CommandGetLastMessageId {
  mut consumer_id : UInt64
  mut request_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetLastMessageId with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  size
}

///|
pub impl Default for CommandGetLastMessageId with fn default() -> CommandGetLastMessageId {
  CommandGetLastMessageId::{
    consumer_id: Default::default(),
    request_id: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetLastMessageId::CommandGetLastMessageId(
  consumer_id : UInt64,
  request_id : UInt64,
) -> CommandGetLastMessageId {
  CommandGetLastMessageId::{ consumer_id, request_id, }
}

///|
pub impl @lib.Read for CommandGetLastMessageId with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetLastMessageId raise {
  let msg : CommandGetLastMessageId = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetLastMessageId with fn write(
  self : CommandGetLastMessageId,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
}

///|
pub impl ToJson for CommandGetLastMessageId with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetLastMessageId with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetLastMessageId raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetLastMessageId"),
    )
  }
  let message : CommandGetLastMessageId = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetLastMessageId with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetLastMessageId raise {
  let msg : CommandGetLastMessageId = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetLastMessageId with fn write(
  self : CommandGetLastMessageId,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
}

///|
pub(all) struct CommandGetLastMessageIdResponse {
  mut last_message_id : MessageIdData
  mut request_id : UInt64
  mut consumer_mark_delete_position : MessageIdData?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetLastMessageIdResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(
    1U,
    @lib.size_of(self.last_message_id),
  )
  size += @lib.size_of_field(2U, @lib.size_of(self.request_id))
  if self.consumer_mark_delete_position is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandGetLastMessageIdResponse with fn default() -> CommandGetLastMessageIdResponse {
  CommandGetLastMessageIdResponse::{
    last_message_id: Default::default(),
    request_id: Default::default(),
    consumer_mark_delete_position: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetLastMessageIdResponse::CommandGetLastMessageIdResponse(
  last_message_id : MessageIdData,
  request_id : UInt64,
  consumer_mark_delete_position? : MessageIdData,
) -> CommandGetLastMessageIdResponse {
  CommandGetLastMessageIdResponse::{
    last_message_id,
    request_id,
    consumer_mark_delete_position,
  }
}

///|
pub impl @lib.Read for CommandGetLastMessageIdResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetLastMessageIdResponse raise {
  let msg : CommandGetLastMessageIdResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.last_message_id = (reader |> @lib.read_message() : MessageIdData)
      (2, _) => msg.request_id = reader |> @lib.read_uint64()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.consumer_mark_delete_position = (
            reader |> @lib.read_message() : MessageIdData)
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetLastMessageIdResponse with fn write(
  self : CommandGetLastMessageIdResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_message(self.last_message_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.consumer_mark_delete_position is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for CommandGetLastMessageIdResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.last_message_id != Default::default() {
    json["lastMessageId"] = ToJson::to_json(self.last_message_id)
  }
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.consumer_mark_delete_position {
    Some(v) => json["consumerMarkDeletePosition"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetLastMessageIdResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetLastMessageIdResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetLastMessageIdResponse"),
    )
  }
  let message : CommandGetLastMessageIdResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("lastMessageId", value) =>
        message.last_message_id = @json.from_json(value, path~)
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("consumerMarkDeletePosition", value) =>
        message.consumer_mark_delete_position = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetLastMessageIdResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetLastMessageIdResponse raise {
  let msg : CommandGetLastMessageIdResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.last_message_id = (
          reader |> @lib.async_read_message() : MessageIdData)
      (2, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.consumer_mark_delete_position = (
            reader |> @lib.async_read_message() : MessageIdData)
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetLastMessageIdResponse with fn write(
  self : CommandGetLastMessageIdResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_message(self.last_message_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.consumer_mark_delete_position is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(v)
  }
}

///|
pub(all) enum CommandGetTopicsOfNamespace_Mode {
  PERSISTENT
  NON_PERSISTENT
  ALL
} derive(Eq, Debug)

///|
pub fn CommandGetTopicsOfNamespace_Mode::to_enum(
  self : CommandGetTopicsOfNamespace_Mode,
) -> @lib.Enum {
  match self {
    CommandGetTopicsOfNamespace_Mode::PERSISTENT => 0
    CommandGetTopicsOfNamespace_Mode::NON_PERSISTENT => 1
    CommandGetTopicsOfNamespace_Mode::ALL => 2
  }
}

///|
pub fn CommandGetTopicsOfNamespace_Mode::from_enum(
  i : @lib.Enum,
) -> CommandGetTopicsOfNamespace_Mode {
  match i.0 {
    0 => CommandGetTopicsOfNamespace_Mode::PERSISTENT
    1 => CommandGetTopicsOfNamespace_Mode::NON_PERSISTENT
    2 => CommandGetTopicsOfNamespace_Mode::ALL
    _ => Default::default()
  }
}

///|
pub impl Default for CommandGetTopicsOfNamespace_Mode with fn default() -> CommandGetTopicsOfNamespace_Mode {
  CommandGetTopicsOfNamespace_Mode::PERSISTENT
}

///|
pub impl @lib.Sized for CommandGetTopicsOfNamespace_Mode with fn size_of(
  self : CommandGetTopicsOfNamespace_Mode,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for CommandGetTopicsOfNamespace_Mode with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetTopicsOfNamespace_Mode raise {
  match json {
    String("PERSISTENT") => CommandGetTopicsOfNamespace_Mode::PERSISTENT
    String("NON_PERSISTENT") => CommandGetTopicsOfNamespace_Mode::NON_PERSISTENT
    String("ALL") => CommandGetTopicsOfNamespace_Mode::ALL
    Number(0, ..) => CommandGetTopicsOfNamespace_Mode::PERSISTENT
    Number(1, ..) => CommandGetTopicsOfNamespace_Mode::NON_PERSISTENT
    Number(2, ..) => CommandGetTopicsOfNamespace_Mode::ALL
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for CommandGetTopicsOfNamespace_Mode with fn to_json(
  self : CommandGetTopicsOfNamespace_Mode,
) -> Json {
  match self {
    CommandGetTopicsOfNamespace_Mode::PERSISTENT => "PERSISTENT"
    CommandGetTopicsOfNamespace_Mode::NON_PERSISTENT => "NON_PERSISTENT"
    CommandGetTopicsOfNamespace_Mode::ALL => "ALL"
  }
}

///|
pub(all) struct CommandGetTopicsOfNamespace {
  mut request_id : UInt64
  mut namespace_ : String
  mut mode : CommandGetTopicsOfNamespace_Mode?
  mut topics_pattern : String?
  mut topics_hash : String?
  mut properties : Array[KeyValue]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetTopicsOfNamespace with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.namespace_))
  if self.mode is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.topics_pattern is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.topics_hash is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  for s in self.properties {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for CommandGetTopicsOfNamespace with fn default() -> CommandGetTopicsOfNamespace {
  CommandGetTopicsOfNamespace::{
    request_id: Default::default(),
    namespace_: Default::default(),
    mode: Some(CommandGetTopicsOfNamespace_Mode::PERSISTENT),
    topics_pattern: None,
    topics_hash: None,
    properties: [],
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetTopicsOfNamespace::CommandGetTopicsOfNamespace(
  request_id : UInt64,
  namespace_ : String,
  mode? : CommandGetTopicsOfNamespace_Mode,
  topics_pattern? : String,
  topics_hash? : String,
  properties : Array[KeyValue],
) -> CommandGetTopicsOfNamespace {
  CommandGetTopicsOfNamespace::{
    request_id,
    namespace_,
    mode,
    topics_pattern,
    topics_hash,
    properties,
  }
}

///|
pub impl @lib.Read for CommandGetTopicsOfNamespace with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetTopicsOfNamespace raise {
  let msg : CommandGetTopicsOfNamespace = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.namespace_ = reader |> @lib.read_string()
      (3, _) =>
        msg.mode = reader
          |> @lib.read_enum()
          |> CommandGetTopicsOfNamespace_Mode::from_enum
          |> Some
      (4, _) => msg.topics_pattern = reader |> @lib.read_string() |> Some
      (5, _) => msg.topics_hash = reader |> @lib.read_string() |> Some
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.read_message() : KeyValue))
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetTopicsOfNamespace with fn write(
  self : CommandGetTopicsOfNamespace,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.namespace_)
  if self.mode is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.topics_pattern is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.topics_hash is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  for item in self.properties {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for CommandGetTopicsOfNamespace with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.namespace_ != Default::default() {
    json["namespace"] = ToJson::to_json(self.namespace_)
  }
  match self.mode {
    Some(v) if v != CommandGetTopicsOfNamespace_Mode::PERSISTENT =>
      json["mode"] = ToJson::to_json(v)
    _ => ()
  }
  match self.topics_pattern {
    Some(v) => json["topicsPattern"] = ToJson::to_json(v)
    _ => ()
  }
  match self.topics_hash {
    Some(v) => json["topicsHash"] = ToJson::to_json(v)
    _ => ()
  }
  if self.properties != Default::default() {
    json["properties"] = ToJson::to_json(self.properties)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetTopicsOfNamespace with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetTopicsOfNamespace raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetTopicsOfNamespace"),
    )
  }
  let message : CommandGetTopicsOfNamespace = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("namespace", value) => message.namespace_ = @json.from_json(value, path~)
      ("mode", value) => message.mode = Some(@json.from_json(value, path~))
      ("topicsPattern", value) =>
        message.topics_pattern = Some(@json.from_json(value, path~))
      ("topicsHash", value) =>
        message.topics_hash = Some(@json.from_json(value, path~))
      ("properties", Array(value)) =>
        message.properties = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetTopicsOfNamespace with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetTopicsOfNamespace raise {
  let msg : CommandGetTopicsOfNamespace = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.namespace_ = reader |> @lib.async_read_string()
      (3, _) =>
        msg.mode = reader
          |> @lib.async_read_enum()
          |> CommandGetTopicsOfNamespace_Mode::from_enum
          |> Some
      (4, _) => msg.topics_pattern = reader |> @lib.async_read_string() |> Some
      (5, _) => msg.topics_hash = reader |> @lib.async_read_string() |> Some
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.properties.push((reader |> @lib.async_read_message() : KeyValue))
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetTopicsOfNamespace with fn write(
  self : CommandGetTopicsOfNamespace,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.namespace_)
  if self.mode is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.topics_pattern is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.topics_hash is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  for item in self.properties {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) struct CommandGetTopicsOfNamespaceResponse {
  mut request_id : UInt64
  mut topics : Array[String]
  mut filtered : Bool?
  mut topics_hash : String?
  mut changed : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetTopicsOfNamespaceResponse with fn size_of(
  self,
) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  for s in self.topics {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  if self.filtered is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.topics_hash is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.changed is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandGetTopicsOfNamespaceResponse with fn default() -> CommandGetTopicsOfNamespaceResponse {
  CommandGetTopicsOfNamespaceResponse::{
    request_id: Default::default(),
    topics: [],
    filtered: Some(false),
    topics_hash: None,
    changed: Some(true),
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetTopicsOfNamespaceResponse::CommandGetTopicsOfNamespaceResponse(
  request_id : UInt64,
  topics : Array[String],
  filtered? : Bool,
  topics_hash? : String,
  changed? : Bool,
) -> CommandGetTopicsOfNamespaceResponse {
  CommandGetTopicsOfNamespaceResponse::{
    request_id,
    topics,
    filtered,
    topics_hash,
    changed,
  }
}

///|
pub impl @lib.Read for CommandGetTopicsOfNamespaceResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetTopicsOfNamespaceResponse raise {
  let msg : CommandGetTopicsOfNamespaceResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.topics.push(reader |> @lib.read_string())
      (3, _) => msg.filtered = reader |> @lib.read_bool() |> Some
      (4, _) => msg.topics_hash = reader |> @lib.read_string() |> Some
      (5, _) => msg.changed = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetTopicsOfNamespaceResponse with fn write(
  self : CommandGetTopicsOfNamespaceResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  for item in self.topics {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_string(item)
  }
  if self.filtered is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_bool(v)
  }
  if self.topics_hash is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.changed is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandGetTopicsOfNamespaceResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.topics != Default::default() {
    json["topics"] = ToJson::to_json(self.topics)
  }
  match self.filtered {
    Some(v) if v != false => json["filtered"] = ToJson::to_json(v)
    _ => ()
  }
  match self.topics_hash {
    Some(v) => json["topicsHash"] = ToJson::to_json(v)
    _ => ()
  }
  match self.changed {
    Some(v) if v != true => json["changed"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetTopicsOfNamespaceResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetTopicsOfNamespaceResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetTopicsOfNamespaceResponse"),
    )
  }
  let message : CommandGetTopicsOfNamespaceResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("topics", Array(value)) =>
        message.topics = value.map(v => @json.from_json(v, path~))
      ("filtered", value) =>
        message.filtered = Some(@json.from_json(value, path~))
      ("topicsHash", value) =>
        message.topics_hash = Some(@json.from_json(value, path~))
      ("changed", value) =>
        message.changed = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetTopicsOfNamespaceResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetTopicsOfNamespaceResponse raise {
  let msg : CommandGetTopicsOfNamespaceResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.topics.push(reader |> @lib.async_read_string())
      (3, _) => msg.filtered = reader |> @lib.async_read_bool() |> Some
      (4, _) => msg.topics_hash = reader |> @lib.async_read_string() |> Some
      (5, _) => msg.changed = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetTopicsOfNamespaceResponse with fn write(
  self : CommandGetTopicsOfNamespaceResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  for item in self.topics {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_string(item)
  }
  if self.filtered is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_bool(v)
  }
  if self.topics_hash is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.changed is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandWatchTopicList {
  mut request_id : UInt64
  mut watcher_id : UInt64
  mut namespace_ : String
  mut topics_pattern : String
  mut topics_hash : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTopicList with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.watcher_id))
  size += @lib.size_of_length_delimited_field(3U, @lib.size_of(self.namespace_))
  size += @lib.size_of_length_delimited_field(
    4U,
    @lib.size_of(self.topics_pattern),
  )
  if self.topics_hash is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandWatchTopicList with fn default() -> CommandWatchTopicList {
  CommandWatchTopicList::{
    request_id: Default::default(),
    watcher_id: Default::default(),
    namespace_: Default::default(),
    topics_pattern: Default::default(),
    topics_hash: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTopicList::CommandWatchTopicList(
  request_id : UInt64,
  watcher_id : UInt64,
  namespace_ : String,
  topics_pattern : String,
  topics_hash? : String,
) -> CommandWatchTopicList {
  CommandWatchTopicList::{
    request_id,
    watcher_id,
    namespace_,
    topics_pattern,
    topics_hash,
  }
}

///|
pub impl @lib.Read for CommandWatchTopicList with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTopicList raise {
  let msg : CommandWatchTopicList = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.watcher_id = reader |> @lib.read_uint64()
      (3, _) => msg.namespace_ = reader |> @lib.read_string()
      (4, _) => msg.topics_pattern = reader |> @lib.read_string()
      (5, _) => msg.topics_hash = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTopicList with fn write(
  self : CommandWatchTopicList,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.watcher_id)
  writer |> @lib.write_varint(26UL)
  writer |> @lib.write_string(self.namespace_)
  writer |> @lib.write_varint(34UL)
  writer |> @lib.write_string(self.topics_pattern)
  if self.topics_hash is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandWatchTopicList with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.watcher_id != Default::default() {
    json["watcherId"] = ToJson::to_json(self.watcher_id)
  }
  if self.namespace_ != Default::default() {
    json["namespace"] = ToJson::to_json(self.namespace_)
  }
  if self.topics_pattern != Default::default() {
    json["topicsPattern"] = ToJson::to_json(self.topics_pattern)
  }
  match self.topics_hash {
    Some(v) => json["topicsHash"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTopicList with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTopicList raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTopicList"),
    )
  }
  let message : CommandWatchTopicList = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("watcherId", value) => message.watcher_id = @json.from_json(value, path~)
      ("namespace", value) => message.namespace_ = @json.from_json(value, path~)
      ("topicsPattern", value) =>
        message.topics_pattern = @json.from_json(value, path~)
      ("topicsHash", value) =>
        message.topics_hash = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTopicList with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTopicList raise {
  let msg : CommandWatchTopicList = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.watcher_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.namespace_ = reader |> @lib.async_read_string()
      (4, _) => msg.topics_pattern = reader |> @lib.async_read_string()
      (5, _) => msg.topics_hash = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTopicList with fn write(
  self : CommandWatchTopicList,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.watcher_id)
  writer |> @lib.async_write_varint(26UL)
  writer |> @lib.async_write_string(self.namespace_)
  writer |> @lib.async_write_varint(34UL)
  writer |> @lib.async_write_string(self.topics_pattern)
  if self.topics_hash is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandWatchTopicListSuccess {
  mut request_id : UInt64
  mut watcher_id : UInt64
  mut topic : Array[String]
  mut topics_hash : String
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTopicListSuccess with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.watcher_id))
  for s in self.topic {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  size += @lib.size_of_length_delimited_field(
    4U,
    @lib.size_of(self.topics_hash),
  )
  size
}

///|
pub impl Default for CommandWatchTopicListSuccess with fn default() -> CommandWatchTopicListSuccess {
  CommandWatchTopicListSuccess::{
    request_id: Default::default(),
    watcher_id: Default::default(),
    topic: [],
    topics_hash: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTopicListSuccess::CommandWatchTopicListSuccess(
  request_id : UInt64,
  watcher_id : UInt64,
  topic : Array[String],
  topics_hash : String,
) -> CommandWatchTopicListSuccess {
  CommandWatchTopicListSuccess::{ request_id, watcher_id, topic, topics_hash, }
}

///|
pub impl @lib.Read for CommandWatchTopicListSuccess with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTopicListSuccess raise {
  let msg : CommandWatchTopicListSuccess = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.watcher_id = reader |> @lib.read_uint64()
      (3, _) => msg.topic.push(reader |> @lib.read_string())
      (4, _) => msg.topics_hash = reader |> @lib.read_string()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTopicListSuccess with fn write(
  self : CommandWatchTopicListSuccess,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.watcher_id)
  for item in self.topic {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(item)
  }
  writer |> @lib.write_varint(34UL)
  writer |> @lib.write_string(self.topics_hash)
}

///|
pub impl ToJson for CommandWatchTopicListSuccess with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.watcher_id != Default::default() {
    json["watcherId"] = ToJson::to_json(self.watcher_id)
  }
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.topics_hash != Default::default() {
    json["topicsHash"] = ToJson::to_json(self.topics_hash)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTopicListSuccess with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTopicListSuccess raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTopicListSuccess"),
    )
  }
  let message : CommandWatchTopicListSuccess = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("watcherId", value) => message.watcher_id = @json.from_json(value, path~)
      ("topic", Array(value)) =>
        message.topic = value.map(v => @json.from_json(v, path~))
      ("topicsHash", value) =>
        message.topics_hash = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTopicListSuccess with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTopicListSuccess raise {
  let msg : CommandWatchTopicListSuccess = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.watcher_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.topic.push(reader |> @lib.async_read_string())
      (4, _) => msg.topics_hash = reader |> @lib.async_read_string()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTopicListSuccess with fn write(
  self : CommandWatchTopicListSuccess,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.watcher_id)
  for item in self.topic {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(item)
  }
  writer |> @lib.async_write_varint(34UL)
  writer |> @lib.async_write_string(self.topics_hash)
}

///|
pub(all) struct CommandWatchTopicUpdate {
  mut watcher_id : UInt64
  mut new_topics : Array[String]
  mut deleted_topics : Array[String]
  mut topics_hash : String
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTopicUpdate with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watcher_id))
  for s in self.new_topics {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  for s in self.deleted_topics {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  size += @lib.size_of_length_delimited_field(
    4U,
    @lib.size_of(self.topics_hash),
  )
  size
}

///|
pub impl Default for CommandWatchTopicUpdate with fn default() -> CommandWatchTopicUpdate {
  CommandWatchTopicUpdate::{
    watcher_id: Default::default(),
    new_topics: [],
    deleted_topics: [],
    topics_hash: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTopicUpdate::CommandWatchTopicUpdate(
  watcher_id : UInt64,
  new_topics : Array[String],
  deleted_topics : Array[String],
  topics_hash : String,
) -> CommandWatchTopicUpdate {
  CommandWatchTopicUpdate::{
    watcher_id,
    new_topics,
    deleted_topics,
    topics_hash,
  }
}

///|
pub impl @lib.Read for CommandWatchTopicUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTopicUpdate raise {
  let msg : CommandWatchTopicUpdate = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watcher_id = reader |> @lib.read_uint64()
      (2, _) => msg.new_topics.push(reader |> @lib.read_string())
      (3, _) => msg.deleted_topics.push(reader |> @lib.read_string())
      (4, _) => msg.topics_hash = reader |> @lib.read_string()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTopicUpdate with fn write(
  self : CommandWatchTopicUpdate,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watcher_id)
  for item in self.new_topics {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_string(item)
  }
  for item in self.deleted_topics {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(item)
  }
  writer |> @lib.write_varint(34UL)
  writer |> @lib.write_string(self.topics_hash)
}

///|
pub impl ToJson for CommandWatchTopicUpdate with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watcher_id != Default::default() {
    json["watcherId"] = ToJson::to_json(self.watcher_id)
  }
  if self.new_topics != Default::default() {
    json["newTopics"] = ToJson::to_json(self.new_topics)
  }
  if self.deleted_topics != Default::default() {
    json["deletedTopics"] = ToJson::to_json(self.deleted_topics)
  }
  if self.topics_hash != Default::default() {
    json["topicsHash"] = ToJson::to_json(self.topics_hash)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTopicUpdate with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTopicUpdate raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTopicUpdate"),
    )
  }
  let message : CommandWatchTopicUpdate = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watcherId", value) => message.watcher_id = @json.from_json(value, path~)
      ("newTopics", Array(value)) =>
        message.new_topics = value.map(v => @json.from_json(v, path~))
      ("deletedTopics", Array(value)) =>
        message.deleted_topics = value.map(v => @json.from_json(v, path~))
      ("topicsHash", value) =>
        message.topics_hash = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTopicUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTopicUpdate raise {
  let msg : CommandWatchTopicUpdate = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watcher_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.new_topics.push(reader |> @lib.async_read_string())
      (3, _) => msg.deleted_topics.push(reader |> @lib.async_read_string())
      (4, _) => msg.topics_hash = reader |> @lib.async_read_string()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTopicUpdate with fn write(
  self : CommandWatchTopicUpdate,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watcher_id)
  for item in self.new_topics {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_string(item)
  }
  for item in self.deleted_topics {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(item)
  }
  writer |> @lib.async_write_varint(34UL)
  writer |> @lib.async_write_string(self.topics_hash)
}

///|
pub(all) struct CommandWatchTopicListClose {
  mut request_id : UInt64
  mut watcher_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTopicListClose with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.watcher_id))
  size
}

///|
pub impl Default for CommandWatchTopicListClose with fn default() -> CommandWatchTopicListClose {
  CommandWatchTopicListClose::{
    request_id: Default::default(),
    watcher_id: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTopicListClose::CommandWatchTopicListClose(
  request_id : UInt64,
  watcher_id : UInt64,
) -> CommandWatchTopicListClose {
  CommandWatchTopicListClose::{ request_id, watcher_id, }
}

///|
pub impl @lib.Read for CommandWatchTopicListClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTopicListClose raise {
  let msg : CommandWatchTopicListClose = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.watcher_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTopicListClose with fn write(
  self : CommandWatchTopicListClose,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.watcher_id)
}

///|
pub impl ToJson for CommandWatchTopicListClose with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.watcher_id != Default::default() {
    json["watcherId"] = ToJson::to_json(self.watcher_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTopicListClose with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTopicListClose raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTopicListClose"),
    )
  }
  let message : CommandWatchTopicListClose = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("watcherId", value) => message.watcher_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTopicListClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTopicListClose raise {
  let msg : CommandWatchTopicListClose = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.watcher_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTopicListClose with fn write(
  self : CommandWatchTopicListClose,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.watcher_id)
}

///|
pub(all) struct SegmentInfoProto {
  mut segment_id : UInt64
  mut hash_start : UInt
  mut hash_end : UInt
  mut state : SegmentState
  mut parent_ids : Array[UInt64]
  mut child_ids : Array[UInt64]
  mut created_at_epoch : UInt64
  mut sealed_at_epoch : UInt64?
  mut created_at_ms : UInt64
  mut sealed_at_ms : UInt64?
  mut legacy_topic_name : String?
  mut entry_bucket_splits : Array[UInt]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for SegmentInfoProto with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.segment_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.hash_start))
  size += @lib.size_of_field(3U, @lib.size_of(self.hash_end))
  size += @lib.size_of_field(4U, @lib.size_of(self.state))
  for s in self.parent_ids {
    size += @lib.size_of_field(5U, @lib.size_of(s))
  }
  for s in self.child_ids {
    size += @lib.size_of_field(6U, @lib.size_of(s))
  }
  size += @lib.size_of_field(7U, @lib.size_of(self.created_at_epoch))
  if self.sealed_at_epoch is Some(v) {
    size += @lib.size_of_field(8U, @lib.size_of(v))
  }
  size += @lib.size_of_field(9U, @lib.size_of(self.created_at_ms))
  if self.sealed_at_ms is Some(v) {
    size += @lib.size_of_field(10U, @lib.size_of(v))
  }
  if self.legacy_topic_name is Some(v) {
    size += @lib.size_of_length_delimited_field(11U, @lib.size_of(v))
  }
  for s in self.entry_bucket_splits {
    size += @lib.size_of_field(12U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for SegmentInfoProto with fn default() -> SegmentInfoProto {
  SegmentInfoProto::{
    segment_id: Default::default(),
    hash_start: Default::default(),
    hash_end: Default::default(),
    state: Default::default(),
    parent_ids: [],
    child_ids: [],
    created_at_epoch: Default::default(),
    sealed_at_epoch: None,
    created_at_ms: Default::default(),
    sealed_at_ms: None,
    legacy_topic_name: None,
    entry_bucket_splits: [],
  }
}

///|
#alias(new, deprecated)
pub fn SegmentInfoProto::SegmentInfoProto(
  segment_id : UInt64,
  hash_start : UInt,
  hash_end : UInt,
  state : SegmentState,
  parent_ids : Array[UInt64],
  child_ids : Array[UInt64],
  created_at_epoch : UInt64,
  sealed_at_epoch? : UInt64,
  created_at_ms : UInt64,
  sealed_at_ms? : UInt64,
  legacy_topic_name? : String,
  entry_bucket_splits : Array[UInt],
) -> SegmentInfoProto {
  SegmentInfoProto::{
    segment_id,
    hash_start,
    hash_end,
    state,
    parent_ids,
    child_ids,
    created_at_epoch,
    sealed_at_epoch,
    created_at_ms,
    sealed_at_ms,
    legacy_topic_name,
    entry_bucket_splits,
  }
}

///|
pub impl @lib.Read for SegmentInfoProto with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> SegmentInfoProto raise {
  let msg : SegmentInfoProto = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.segment_id = reader |> @lib.read_uint64()
      (2, _) => msg.hash_start = reader |> @lib.read_uint32()
      (3, _) => msg.hash_end = reader |> @lib.read_uint32()
      (4, _) =>
        msg.state = reader |> @lib.read_enum() |> SegmentState::from_enum
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.parent_ids.push_iter(
          (reader |> @lib.read_packed(@lib.read_uint64, None)).iter(),
        )
      (5, @lib.WIRE_TYPE_VARINT) =>
        msg.parent_ids.push(reader |> @lib.read_uint64())
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.child_ids.push_iter(
          (reader |> @lib.read_packed(@lib.read_uint64, None)).iter(),
        )
      (6, @lib.WIRE_TYPE_VARINT) =>
        msg.child_ids.push(reader |> @lib.read_uint64())
      (7, _) => msg.created_at_epoch = reader |> @lib.read_uint64()
      (8, _) => msg.sealed_at_epoch = reader |> @lib.read_uint64() |> Some
      (9, _) => msg.created_at_ms = reader |> @lib.read_uint64()
      (10, _) => msg.sealed_at_ms = reader |> @lib.read_uint64() |> Some
      (11, _) => msg.legacy_topic_name = reader |> @lib.read_string() |> Some
      (12, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.entry_bucket_splits.push_iter(
          (reader |> @lib.read_packed(@lib.read_uint32, None)).iter(),
        )
      (12, @lib.WIRE_TYPE_VARINT) =>
        msg.entry_bucket_splits.push(reader |> @lib.read_uint32())
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for SegmentInfoProto with fn write(
  self : SegmentInfoProto,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.segment_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint32(self.hash_start)
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_uint32(self.hash_end)
  writer |> @lib.write_varint(32UL)
  writer |> @lib.write_enum(self.state.to_enum())
  for item in self.parent_ids {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_uint64(item)
  }
  for item in self.child_ids {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_uint64(item)
  }
  writer |> @lib.write_varint(56UL)
  writer |> @lib.write_uint64(self.created_at_epoch)
  if self.sealed_at_epoch is Some(v) {
    writer |> @lib.write_varint(64UL)
    writer |> @lib.write_uint64(v)
  }
  writer |> @lib.write_varint(72UL)
  writer |> @lib.write_uint64(self.created_at_ms)
  if self.sealed_at_ms is Some(v) {
    writer |> @lib.write_varint(80UL)
    writer |> @lib.write_uint64(v)
  }
  if self.legacy_topic_name is Some(v) {
    writer |> @lib.write_varint(90UL)
    writer |> @lib.write_string(v)
  }
  for item in self.entry_bucket_splits {
    writer |> @lib.write_varint(96UL)
    writer |> @lib.write_uint32(item)
  }
}

///|
pub impl ToJson for SegmentInfoProto with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.segment_id != Default::default() {
    json["segmentId"] = ToJson::to_json(self.segment_id)
  }
  if self.hash_start != Default::default() {
    json["hashStart"] = ToJson::to_json(self.hash_start)
  }
  if self.hash_end != Default::default() {
    json["hashEnd"] = ToJson::to_json(self.hash_end)
  }
  if self.state != Default::default() {
    json["state"] = ToJson::to_json(self.state)
  }
  if self.parent_ids != Default::default() {
    json["parentIds"] = ToJson::to_json(self.parent_ids)
  }
  if self.child_ids != Default::default() {
    json["childIds"] = ToJson::to_json(self.child_ids)
  }
  if self.created_at_epoch != Default::default() {
    json["createdAtEpoch"] = ToJson::to_json(self.created_at_epoch)
  }
  match self.sealed_at_epoch {
    Some(v) => json["sealedAtEpoch"] = ToJson::to_json(v)
    _ => ()
  }
  if self.created_at_ms != Default::default() {
    json["createdAtMs"] = ToJson::to_json(self.created_at_ms)
  }
  match self.sealed_at_ms {
    Some(v) => json["sealedAtMs"] = ToJson::to_json(v)
    _ => ()
  }
  match self.legacy_topic_name {
    Some(v) => json["legacyTopicName"] = ToJson::to_json(v)
    _ => ()
  }
  if self.entry_bucket_splits != Default::default() {
    json["entryBucketSplits"] = ToJson::to_json(self.entry_bucket_splits)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for SegmentInfoProto with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> SegmentInfoProto raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for SegmentInfoProto"),
    )
  }
  let message : SegmentInfoProto = Default::default()
  for key, value in obj {
    match (key, value) {
      ("segmentId", value) => message.segment_id = @json.from_json(value, path~)
      ("hashStart", value) => message.hash_start = @json.from_json(value, path~)
      ("hashEnd", value) => message.hash_end = @json.from_json(value, path~)
      ("state", value) => message.state = @json.from_json(value, path~)
      ("parentIds", Array(value)) =>
        message.parent_ids = value.map(v => @json.from_json(v, path~))
      ("childIds", Array(value)) =>
        message.child_ids = value.map(v => @json.from_json(v, path~))
      ("createdAtEpoch", value) =>
        message.created_at_epoch = @json.from_json(value, path~)
      ("sealedAtEpoch", value) =>
        message.sealed_at_epoch = Some(@json.from_json(value, path~))
      ("createdAtMs", value) =>
        message.created_at_ms = @json.from_json(value, path~)
      ("sealedAtMs", value) =>
        message.sealed_at_ms = Some(@json.from_json(value, path~))
      ("legacyTopicName", value) =>
        message.legacy_topic_name = Some(@json.from_json(value, path~))
      ("entryBucketSplits", Array(value)) =>
        message.entry_bucket_splits = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for SegmentInfoProto with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> SegmentInfoProto raise {
  let msg : SegmentInfoProto = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.segment_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.hash_start = reader |> @lib.async_read_uint32()
      (3, _) => msg.hash_end = reader |> @lib.async_read_uint32()
      (4, _) =>
        msg.state = reader |> @lib.async_read_enum() |> SegmentState::from_enum
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.parent_ids.push_iter(
          (reader |> @lib.async_read_packed(@lib.async_read_uint64, None)).iter(),
        )
      (5, @lib.WIRE_TYPE_VARINT) =>
        msg.parent_ids.push(reader |> @lib.async_read_uint64())
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.child_ids.push_iter(
          (reader |> @lib.async_read_packed(@lib.async_read_uint64, None)).iter(),
        )
      (6, @lib.WIRE_TYPE_VARINT) =>
        msg.child_ids.push(reader |> @lib.async_read_uint64())
      (7, _) => msg.created_at_epoch = reader |> @lib.async_read_uint64()
      (8, _) => msg.sealed_at_epoch = reader |> @lib.async_read_uint64() |> Some
      (9, _) => msg.created_at_ms = reader |> @lib.async_read_uint64()
      (10, _) => msg.sealed_at_ms = reader |> @lib.async_read_uint64() |> Some
      (11, _) =>
        msg.legacy_topic_name = reader |> @lib.async_read_string() |> Some
      (12, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.entry_bucket_splits.push_iter(
          (reader |> @lib.async_read_packed(@lib.async_read_uint32, None)).iter(),
        )
      (12, @lib.WIRE_TYPE_VARINT) =>
        msg.entry_bucket_splits.push(reader |> @lib.async_read_uint32())
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for SegmentInfoProto with fn write(
  self : SegmentInfoProto,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.segment_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint32(self.hash_start)
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_uint32(self.hash_end)
  writer |> @lib.async_write_varint(32UL)
  writer |> @lib.async_write_enum(self.state.to_enum())
  for item in self.parent_ids {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_uint64(item)
  }
  for item in self.child_ids {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_uint64(item)
  }
  writer |> @lib.async_write_varint(56UL)
  writer |> @lib.async_write_uint64(self.created_at_epoch)
  if self.sealed_at_epoch is Some(v) {
    writer |> @lib.async_write_varint(64UL)
    writer |> @lib.async_write_uint64(v)
  }
  writer |> @lib.async_write_varint(72UL)
  writer |> @lib.async_write_uint64(self.created_at_ms)
  if self.sealed_at_ms is Some(v) {
    writer |> @lib.async_write_varint(80UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.legacy_topic_name is Some(v) {
    writer |> @lib.async_write_varint(90UL)
    writer |> @lib.async_write_string(v)
  }
  for item in self.entry_bucket_splits {
    writer |> @lib.async_write_varint(96UL)
    writer |> @lib.async_write_uint32(item)
  }
}

///|
pub(all) struct SegmentBrokerAddress {
  mut segment_id : UInt64
  mut broker_url : String
  mut broker_url_tls : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for SegmentBrokerAddress with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.segment_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.broker_url))
  if self.broker_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for SegmentBrokerAddress with fn default() -> SegmentBrokerAddress {
  SegmentBrokerAddress::{
    segment_id: Default::default(),
    broker_url: Default::default(),
    broker_url_tls: None,
  }
}

///|
#alias(new, deprecated)
pub fn SegmentBrokerAddress::SegmentBrokerAddress(
  segment_id : UInt64,
  broker_url : String,
  broker_url_tls? : String,
) -> SegmentBrokerAddress {
  SegmentBrokerAddress::{ segment_id, broker_url, broker_url_tls, }
}

///|
pub impl @lib.Read for SegmentBrokerAddress with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> SegmentBrokerAddress raise {
  let msg : SegmentBrokerAddress = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.segment_id = reader |> @lib.read_uint64()
      (2, _) => msg.broker_url = reader |> @lib.read_string()
      (3, _) => msg.broker_url_tls = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for SegmentBrokerAddress with fn write(
  self : SegmentBrokerAddress,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.segment_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.broker_url)
  if self.broker_url_tls is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for SegmentBrokerAddress with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.segment_id != Default::default() {
    json["segmentId"] = ToJson::to_json(self.segment_id)
  }
  if self.broker_url != Default::default() {
    json["brokerUrl"] = ToJson::to_json(self.broker_url)
  }
  match self.broker_url_tls {
    Some(v) => json["brokerUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for SegmentBrokerAddress with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> SegmentBrokerAddress raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for SegmentBrokerAddress"),
    )
  }
  let message : SegmentBrokerAddress = Default::default()
  for key, value in obj {
    match (key, value) {
      ("segmentId", value) => message.segment_id = @json.from_json(value, path~)
      ("brokerUrl", value) => message.broker_url = @json.from_json(value, path~)
      ("brokerUrlTls", value) =>
        message.broker_url_tls = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for SegmentBrokerAddress with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> SegmentBrokerAddress raise {
  let msg : SegmentBrokerAddress = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.segment_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.broker_url = reader |> @lib.async_read_string()
      (3, _) => msg.broker_url_tls = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for SegmentBrokerAddress with fn write(
  self : SegmentBrokerAddress,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.segment_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.broker_url)
  if self.broker_url_tls is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct ScalableTopicDAG {
  mut epoch : UInt64
  mut segments : Array[SegmentInfoProto]
  mut segment_brokers : Array[SegmentBrokerAddress]
  mut controller_broker_url : String?
  mut controller_broker_url_tls : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for ScalableTopicDAG with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.epoch))
  for s in self.segments {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  for s in self.segment_brokers {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  if self.controller_broker_url is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.controller_broker_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for ScalableTopicDAG with fn default() -> ScalableTopicDAG {
  ScalableTopicDAG::{
    epoch: Default::default(),
    segments: [],
    segment_brokers: [],
    controller_broker_url: None,
    controller_broker_url_tls: None,
  }
}

///|
#alias(new, deprecated)
pub fn ScalableTopicDAG::ScalableTopicDAG(
  epoch : UInt64,
  segments : Array[SegmentInfoProto],
  segment_brokers : Array[SegmentBrokerAddress],
  controller_broker_url? : String,
  controller_broker_url_tls? : String,
) -> ScalableTopicDAG {
  ScalableTopicDAG::{
    epoch,
    segments,
    segment_brokers,
    controller_broker_url,
    controller_broker_url_tls,
  }
}

///|
pub impl @lib.Read for ScalableTopicDAG with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> ScalableTopicDAG raise {
  let msg : ScalableTopicDAG = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.epoch = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.segments.push((reader |> @lib.read_message() : SegmentInfoProto))
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.segment_brokers.push(
          (reader |> @lib.read_message() : SegmentBrokerAddress),
        )
      (4, _) => msg.controller_broker_url = reader |> @lib.read_string() |> Some
      (5, _) =>
        msg.controller_broker_url_tls = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for ScalableTopicDAG with fn write(
  self : ScalableTopicDAG,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.epoch)
  for item in self.segments {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(item)
  }
  for item in self.segment_brokers {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(item)
  }
  if self.controller_broker_url is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.controller_broker_url_tls is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for ScalableTopicDAG with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.epoch != Default::default() {
    json["epoch"] = ToJson::to_json(self.epoch)
  }
  if self.segments != Default::default() {
    json["segments"] = ToJson::to_json(self.segments)
  }
  if self.segment_brokers != Default::default() {
    json["segmentBrokers"] = ToJson::to_json(self.segment_brokers)
  }
  match self.controller_broker_url {
    Some(v) => json["controllerBrokerUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.controller_broker_url_tls {
    Some(v) => json["controllerBrokerUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for ScalableTopicDAG with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ScalableTopicDAG raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for ScalableTopicDAG"),
    )
  }
  let message : ScalableTopicDAG = Default::default()
  for key, value in obj {
    match (key, value) {
      ("epoch", value) => message.epoch = @json.from_json(value, path~)
      ("segments", Array(value)) =>
        message.segments = value.map(v => @json.from_json(v, path~))
      ("segmentBrokers", Array(value)) =>
        message.segment_brokers = value.map(v => @json.from_json(v, path~))
      ("controllerBrokerUrl", value) =>
        message.controller_broker_url = Some(@json.from_json(value, path~))
      ("controllerBrokerUrlTls", value) =>
        message.controller_broker_url_tls = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for ScalableTopicDAG with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> ScalableTopicDAG raise {
  let msg : ScalableTopicDAG = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.epoch = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.segments.push(
          (reader |> @lib.async_read_message() : SegmentInfoProto),
        )
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.segment_brokers.push(
          (reader |> @lib.async_read_message() : SegmentBrokerAddress),
        )
      (4, _) =>
        msg.controller_broker_url = reader |> @lib.async_read_string() |> Some
      (5, _) =>
        msg.controller_broker_url_tls = reader
          |> @lib.async_read_string()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for ScalableTopicDAG with fn write(
  self : ScalableTopicDAG,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.epoch)
  for item in self.segments {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(item)
  }
  for item in self.segment_brokers {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(item)
  }
  if self.controller_broker_url is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.controller_broker_url_tls is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandScalableTopicLookup {
  mut session_id : UInt64
  mut topic : String
  mut create_if_missing : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicLookup with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.session_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.topic))
  if self.create_if_missing is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandScalableTopicLookup with fn default() -> CommandScalableTopicLookup {
  CommandScalableTopicLookup::{
    session_id: Default::default(),
    topic: Default::default(),
    create_if_missing: Some(true),
  }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicLookup::CommandScalableTopicLookup(
  session_id : UInt64,
  topic : String,
  create_if_missing? : Bool,
) -> CommandScalableTopicLookup {
  CommandScalableTopicLookup::{ session_id, topic, create_if_missing, }
}

///|
pub impl @lib.Read for CommandScalableTopicLookup with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicLookup raise {
  let msg : CommandScalableTopicLookup = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.session_id = reader |> @lib.read_uint64()
      (2, _) => msg.topic = reader |> @lib.read_string()
      (3, _) => msg.create_if_missing = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicLookup with fn write(
  self : CommandScalableTopicLookup,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.session_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.topic)
  if self.create_if_missing is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandScalableTopicLookup with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.session_id != Default::default() {
    json["sessionId"] = ToJson::to_json(self.session_id)
  }
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  match self.create_if_missing {
    Some(v) if v != true => json["createIfMissing"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicLookup with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicLookup raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicLookup"),
    )
  }
  let message : CommandScalableTopicLookup = Default::default()
  for key, value in obj {
    match (key, value) {
      ("sessionId", value) => message.session_id = @json.from_json(value, path~)
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("createIfMissing", value) =>
        message.create_if_missing = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicLookup with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicLookup raise {
  let msg : CommandScalableTopicLookup = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.session_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.topic = reader |> @lib.async_read_string()
      (3, _) => msg.create_if_missing = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicLookup with fn write(
  self : CommandScalableTopicLookup,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.session_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.topic)
  if self.create_if_missing is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandScalableTopicUpdate {
  mut session_id : UInt64
  mut dag : ScalableTopicDAG?
  mut error : ServerError?
  mut message : String?
  mut resolved_topic_name : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicUpdate with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.session_id))
  if self.dag is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.resolved_topic_name is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandScalableTopicUpdate with fn default() -> CommandScalableTopicUpdate {
  CommandScalableTopicUpdate::{
    session_id: Default::default(),
    dag: None,
    error: None,
    message: None,
    resolved_topic_name: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicUpdate::CommandScalableTopicUpdate(
  session_id : UInt64,
  dag? : ScalableTopicDAG,
  error? : ServerError,
  message? : String,
  resolved_topic_name? : String,
) -> CommandScalableTopicUpdate {
  CommandScalableTopicUpdate::{
    session_id,
    dag,
    error,
    message,
    resolved_topic_name,
  }
}

///|
pub impl @lib.Read for CommandScalableTopicUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicUpdate raise {
  let msg : CommandScalableTopicUpdate = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.session_id = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.dag = (reader |> @lib.read_message() : ScalableTopicDAG) |> Some
      (3, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (4, _) => msg.message = reader |> @lib.read_string() |> Some
      (5, _) => msg.resolved_topic_name = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicUpdate with fn write(
  self : CommandScalableTopicUpdate,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.session_id)
  if self.dag is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.resolved_topic_name is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandScalableTopicUpdate with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.session_id != Default::default() {
    json["sessionId"] = ToJson::to_json(self.session_id)
  }
  match self.dag {
    Some(v) => json["dag"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  match self.resolved_topic_name {
    Some(v) => json["resolvedTopicName"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicUpdate with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicUpdate raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicUpdate"),
    )
  }
  let message : CommandScalableTopicUpdate = Default::default()
  for key, value in obj {
    match (key, value) {
      ("sessionId", value) => message.session_id = @json.from_json(value, path~)
      ("dag", value) => message.dag = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      ("resolvedTopicName", value) =>
        message.resolved_topic_name = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicUpdate raise {
  let msg : CommandScalableTopicUpdate = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.session_id = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.dag = (reader |> @lib.async_read_message() : ScalableTopicDAG)
          |> Some
      (3, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (4, _) => msg.message = reader |> @lib.async_read_string() |> Some
      (5, _) =>
        msg.resolved_topic_name = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicUpdate with fn write(
  self : CommandScalableTopicUpdate,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.session_id)
  if self.dag is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.resolved_topic_name is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandScalableTopicClose {
  mut session_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicClose with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.session_id))
  size
}

///|
pub impl Default for CommandScalableTopicClose with fn default() -> CommandScalableTopicClose {
  CommandScalableTopicClose::{ session_id: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicClose::CommandScalableTopicClose(
  session_id : UInt64,
) -> CommandScalableTopicClose {
  CommandScalableTopicClose::{ session_id, }
}

///|
pub impl @lib.Read for CommandScalableTopicClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicClose raise {
  let msg : CommandScalableTopicClose = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.session_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicClose with fn write(
  self : CommandScalableTopicClose,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.session_id)
}

///|
pub impl ToJson for CommandScalableTopicClose with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.session_id != Default::default() {
    json["sessionId"] = ToJson::to_json(self.session_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicClose with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicClose raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicClose"),
    )
  }
  let message : CommandScalableTopicClose = Default::default()
  for key, value in obj {
    match (key, value) {
      ("sessionId", value) => message.session_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicClose raise {
  let msg : CommandScalableTopicClose = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.session_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicClose with fn write(
  self : CommandScalableTopicClose,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.session_id)
}

///|
pub(all) struct ScalableAssignedSegment {
  mut segment_id : UInt64
  mut hash_start : UInt
  mut hash_end : UInt
  mut segment_topic : String
  mut bucket_ranges : Array[IntRange]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for ScalableAssignedSegment with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.segment_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.hash_start))
  size += @lib.size_of_field(3U, @lib.size_of(self.hash_end))
  size += @lib.size_of_length_delimited_field(
    4U,
    @lib.size_of(self.segment_topic),
  )
  for s in self.bucket_ranges {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for ScalableAssignedSegment with fn default() -> ScalableAssignedSegment {
  ScalableAssignedSegment::{
    segment_id: Default::default(),
    hash_start: Default::default(),
    hash_end: Default::default(),
    segment_topic: Default::default(),
    bucket_ranges: [],
  }
}

///|
#alias(new, deprecated)
pub fn ScalableAssignedSegment::ScalableAssignedSegment(
  segment_id : UInt64,
  hash_start : UInt,
  hash_end : UInt,
  segment_topic : String,
  bucket_ranges : Array[IntRange],
) -> ScalableAssignedSegment {
  ScalableAssignedSegment::{
    segment_id,
    hash_start,
    hash_end,
    segment_topic,
    bucket_ranges,
  }
}

///|
pub impl @lib.Read for ScalableAssignedSegment with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> ScalableAssignedSegment raise {
  let msg : ScalableAssignedSegment = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.segment_id = reader |> @lib.read_uint64()
      (2, _) => msg.hash_start = reader |> @lib.read_uint32()
      (3, _) => msg.hash_end = reader |> @lib.read_uint32()
      (4, _) => msg.segment_topic = reader |> @lib.read_string()
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.bucket_ranges.push((reader |> @lib.read_message() : IntRange))
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for ScalableAssignedSegment with fn write(
  self : ScalableAssignedSegment,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.segment_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint32(self.hash_start)
  writer |> @lib.write_varint(24UL)
  writer |> @lib.write_uint32(self.hash_end)
  writer |> @lib.write_varint(34UL)
  writer |> @lib.write_string(self.segment_topic)
  for item in self.bucket_ranges {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for ScalableAssignedSegment with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.segment_id != Default::default() {
    json["segmentId"] = ToJson::to_json(self.segment_id)
  }
  if self.hash_start != Default::default() {
    json["hashStart"] = ToJson::to_json(self.hash_start)
  }
  if self.hash_end != Default::default() {
    json["hashEnd"] = ToJson::to_json(self.hash_end)
  }
  if self.segment_topic != Default::default() {
    json["segmentTopic"] = ToJson::to_json(self.segment_topic)
  }
  if self.bucket_ranges != Default::default() {
    json["bucketRanges"] = ToJson::to_json(self.bucket_ranges)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for ScalableAssignedSegment with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ScalableAssignedSegment raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for ScalableAssignedSegment"),
    )
  }
  let message : ScalableAssignedSegment = Default::default()
  for key, value in obj {
    match (key, value) {
      ("segmentId", value) => message.segment_id = @json.from_json(value, path~)
      ("hashStart", value) => message.hash_start = @json.from_json(value, path~)
      ("hashEnd", value) => message.hash_end = @json.from_json(value, path~)
      ("segmentTopic", value) =>
        message.segment_topic = @json.from_json(value, path~)
      ("bucketRanges", Array(value)) =>
        message.bucket_ranges = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for ScalableAssignedSegment with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> ScalableAssignedSegment raise {
  let msg : ScalableAssignedSegment = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.segment_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.hash_start = reader |> @lib.async_read_uint32()
      (3, _) => msg.hash_end = reader |> @lib.async_read_uint32()
      (4, _) => msg.segment_topic = reader |> @lib.async_read_string()
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.bucket_ranges.push((reader |> @lib.async_read_message() : IntRange))
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for ScalableAssignedSegment with fn write(
  self : ScalableAssignedSegment,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.segment_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint32(self.hash_start)
  writer |> @lib.async_write_varint(24UL)
  writer |> @lib.async_write_uint32(self.hash_end)
  writer |> @lib.async_write_varint(34UL)
  writer |> @lib.async_write_string(self.segment_topic)
  for item in self.bucket_ranges {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) struct ScalableConsumerAssignment {
  mut layout_epoch : UInt64
  mut segments : Array[ScalableAssignedSegment]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for ScalableConsumerAssignment with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.layout_epoch))
  for s in self.segments {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for ScalableConsumerAssignment with fn default() -> ScalableConsumerAssignment {
  ScalableConsumerAssignment::{
    layout_epoch: Default::default(),
    segments: [],
  }
}

///|
#alias(new, deprecated)
pub fn ScalableConsumerAssignment::ScalableConsumerAssignment(
  layout_epoch : UInt64,
  segments : Array[ScalableAssignedSegment],
) -> ScalableConsumerAssignment {
  ScalableConsumerAssignment::{ layout_epoch, segments, }
}

///|
pub impl @lib.Read for ScalableConsumerAssignment with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> ScalableConsumerAssignment raise {
  let msg : ScalableConsumerAssignment = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.layout_epoch = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.segments.push(
          (reader |> @lib.read_message() : ScalableAssignedSegment),
        )
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for ScalableConsumerAssignment with fn write(
  self : ScalableConsumerAssignment,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.layout_epoch)
  for item in self.segments {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for ScalableConsumerAssignment with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.layout_epoch != Default::default() {
    json["layoutEpoch"] = ToJson::to_json(self.layout_epoch)
  }
  if self.segments != Default::default() {
    json["segments"] = ToJson::to_json(self.segments)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for ScalableConsumerAssignment with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ScalableConsumerAssignment raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for ScalableConsumerAssignment"),
    )
  }
  let message : ScalableConsumerAssignment = Default::default()
  for key, value in obj {
    match (key, value) {
      ("layoutEpoch", value) =>
        message.layout_epoch = @json.from_json(value, path~)
      ("segments", Array(value)) =>
        message.segments = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for ScalableConsumerAssignment with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> ScalableConsumerAssignment raise {
  let msg : ScalableConsumerAssignment = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.layout_epoch = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.segments.push(
          (reader |> @lib.async_read_message() : ScalableAssignedSegment),
        )
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for ScalableConsumerAssignment with fn write(
  self : ScalableConsumerAssignment,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.layout_epoch)
  for item in self.segments {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) struct CommandScalableTopicSubscribe {
  mut request_id : UInt64
  mut topic : String
  mut subscription : String
  mut consumer_name : String
  mut consumer_id : UInt64
  mut consumer_type : ScalableConsumerType
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicSubscribe with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.topic))
  size += @lib.size_of_length_delimited_field(
    3U,
    @lib.size_of(self.subscription),
  )
  size += @lib.size_of_length_delimited_field(
    4U,
    @lib.size_of(self.consumer_name),
  )
  size += @lib.size_of_field(5U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_field(6U, @lib.size_of(self.consumer_type))
  size
}

///|
pub impl Default for CommandScalableTopicSubscribe with fn default() -> CommandScalableTopicSubscribe {
  CommandScalableTopicSubscribe::{
    request_id: Default::default(),
    topic: Default::default(),
    subscription: Default::default(),
    consumer_name: Default::default(),
    consumer_id: Default::default(),
    consumer_type: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicSubscribe::CommandScalableTopicSubscribe(
  request_id : UInt64,
  topic : String,
  subscription : String,
  consumer_name : String,
  consumer_id : UInt64,
  consumer_type : ScalableConsumerType,
) -> CommandScalableTopicSubscribe {
  CommandScalableTopicSubscribe::{
    request_id,
    topic,
    subscription,
    consumer_name,
    consumer_id,
    consumer_type,
  }
}

///|
pub impl @lib.Read for CommandScalableTopicSubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicSubscribe raise {
  let msg : CommandScalableTopicSubscribe = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.topic = reader |> @lib.read_string()
      (3, _) => msg.subscription = reader |> @lib.read_string()
      (4, _) => msg.consumer_name = reader |> @lib.read_string()
      (5, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (6, _) =>
        msg.consumer_type = reader
          |> @lib.read_enum()
          |> ScalableConsumerType::from_enum
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicSubscribe with fn write(
  self : CommandScalableTopicSubscribe,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(26UL)
  writer |> @lib.write_string(self.subscription)
  writer |> @lib.write_varint(34UL)
  writer |> @lib.write_string(self.consumer_name)
  writer |> @lib.write_varint(40UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(48UL)
  writer |> @lib.write_enum(self.consumer_type.to_enum())
}

///|
pub impl ToJson for CommandScalableTopicSubscribe with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.subscription != Default::default() {
    json["subscription"] = ToJson::to_json(self.subscription)
  }
  if self.consumer_name != Default::default() {
    json["consumerName"] = ToJson::to_json(self.consumer_name)
  }
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.consumer_type != Default::default() {
    json["consumerType"] = ToJson::to_json(self.consumer_type)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicSubscribe with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicSubscribe raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicSubscribe"),
    )
  }
  let message : CommandScalableTopicSubscribe = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("subscription", value) =>
        message.subscription = @json.from_json(value, path~)
      ("consumerName", value) =>
        message.consumer_name = @json.from_json(value, path~)
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("consumerType", value) =>
        message.consumer_type = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicSubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicSubscribe raise {
  let msg : CommandScalableTopicSubscribe = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.topic = reader |> @lib.async_read_string()
      (3, _) => msg.subscription = reader |> @lib.async_read_string()
      (4, _) => msg.consumer_name = reader |> @lib.async_read_string()
      (5, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (6, _) =>
        msg.consumer_type = reader
          |> @lib.async_read_enum()
          |> ScalableConsumerType::from_enum
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicSubscribe with fn write(
  self : CommandScalableTopicSubscribe,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(26UL)
  writer |> @lib.async_write_string(self.subscription)
  writer |> @lib.async_write_varint(34UL)
  writer |> @lib.async_write_string(self.consumer_name)
  writer |> @lib.async_write_varint(40UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(48UL)
  writer |> @lib.async_write_enum(self.consumer_type.to_enum())
}

///|
pub(all) struct CommandScalableTopicSubscribeResponse {
  mut request_id : UInt64
  mut error : ServerError?
  mut message : String?
  mut assignment : ScalableConsumerAssignment?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicSubscribeResponse with fn size_of(
  self,
) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.error is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.assignment is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandScalableTopicSubscribeResponse with fn default() -> CommandScalableTopicSubscribeResponse {
  CommandScalableTopicSubscribeResponse::{
    request_id: Default::default(),
    error: None,
    message: None,
    assignment: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicSubscribeResponse::CommandScalableTopicSubscribeResponse(
  request_id : UInt64,
  error? : ServerError,
  message? : String,
  assignment? : ScalableConsumerAssignment,
) -> CommandScalableTopicSubscribeResponse {
  CommandScalableTopicSubscribeResponse::{
    request_id,
    error,
    message,
    assignment,
  }
}

///|
pub impl @lib.Read for CommandScalableTopicSubscribeResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicSubscribeResponse raise {
  let msg : CommandScalableTopicSubscribeResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (3, _) => msg.message = reader |> @lib.read_string() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.assignment = (
            reader |> @lib.read_message() : ScalableConsumerAssignment)
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicSubscribeResponse with fn write(
  self : CommandScalableTopicSubscribeResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.error is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.assignment is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for CommandScalableTopicSubscribeResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  match self.assignment {
    Some(v) => json["assignment"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicSubscribeResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicSubscribeResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicSubscribeResponse"),
    )
  }
  let message : CommandScalableTopicSubscribeResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      ("assignment", value) =>
        message.assignment = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicSubscribeResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicSubscribeResponse raise {
  let msg : CommandScalableTopicSubscribeResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.message = reader |> @lib.async_read_string() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.assignment = (
            reader |> @lib.async_read_message() : ScalableConsumerAssignment)
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicSubscribeResponse with fn write(
  self : CommandScalableTopicSubscribeResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.assignment is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(v)
  }
}

///|
pub(all) struct CommandScalableTopicAssignmentUpdate {
  mut consumer_id : UInt64
  mut assignment : ScalableConsumerAssignment
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicAssignmentUpdate with fn size_of(
  self,
) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.consumer_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.assignment))
  size
}

///|
pub impl Default for CommandScalableTopicAssignmentUpdate with fn default() -> CommandScalableTopicAssignmentUpdate {
  CommandScalableTopicAssignmentUpdate::{
    consumer_id: Default::default(),
    assignment: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicAssignmentUpdate::CommandScalableTopicAssignmentUpdate(
  consumer_id : UInt64,
  assignment : ScalableConsumerAssignment,
) -> CommandScalableTopicAssignmentUpdate {
  CommandScalableTopicAssignmentUpdate::{ consumer_id, assignment, }
}

///|
pub impl @lib.Read for CommandScalableTopicAssignmentUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicAssignmentUpdate raise {
  let msg : CommandScalableTopicAssignmentUpdate = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.assignment = (
          reader |> @lib.read_message() : ScalableConsumerAssignment)
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicAssignmentUpdate with fn write(
  self : CommandScalableTopicAssignmentUpdate,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.consumer_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_message(self.assignment)
}

///|
pub impl ToJson for CommandScalableTopicAssignmentUpdate with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  if self.assignment != Default::default() {
    json["assignment"] = ToJson::to_json(self.assignment)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicAssignmentUpdate with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicAssignmentUpdate raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicAssignmentUpdate"),
    )
  }
  let message : CommandScalableTopicAssignmentUpdate = Default::default()
  for key, value in obj {
    match (key, value) {
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      ("assignment", value) =>
        message.assignment = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicAssignmentUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicAssignmentUpdate raise {
  let msg : CommandScalableTopicAssignmentUpdate = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.assignment = (
          reader |> @lib.async_read_message() : ScalableConsumerAssignment)
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicAssignmentUpdate with fn write(
  self : CommandScalableTopicAssignmentUpdate,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_message(self.assignment)
}

///|
pub(all) struct CommandScalableTopicUnsubscribe {
  mut request_id : UInt64
  mut consumer_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandScalableTopicUnsubscribe with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.consumer_id))
  size
}

///|
pub impl Default for CommandScalableTopicUnsubscribe with fn default() -> CommandScalableTopicUnsubscribe {
  CommandScalableTopicUnsubscribe::{
    request_id: Default::default(),
    consumer_id: Default::default(),
  }
}

///|
#alias(new, deprecated)
pub fn CommandScalableTopicUnsubscribe::CommandScalableTopicUnsubscribe(
  request_id : UInt64,
  consumer_id : UInt64,
) -> CommandScalableTopicUnsubscribe {
  CommandScalableTopicUnsubscribe::{ request_id, consumer_id, }
}

///|
pub impl @lib.Read for CommandScalableTopicUnsubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandScalableTopicUnsubscribe raise {
  let msg : CommandScalableTopicUnsubscribe = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.consumer_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandScalableTopicUnsubscribe with fn write(
  self : CommandScalableTopicUnsubscribe,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.consumer_id)
}

///|
pub impl ToJson for CommandScalableTopicUnsubscribe with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.consumer_id != Default::default() {
    json["consumerId"] = ToJson::to_json(self.consumer_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandScalableTopicUnsubscribe with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandScalableTopicUnsubscribe raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandScalableTopicUnsubscribe"),
    )
  }
  let message : CommandScalableTopicUnsubscribe = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("consumerId", value) =>
        message.consumer_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandScalableTopicUnsubscribe with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandScalableTopicUnsubscribe raise {
  let msg : CommandScalableTopicUnsubscribe = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.consumer_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandScalableTopicUnsubscribe with fn write(
  self : CommandScalableTopicUnsubscribe,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.consumer_id)
}

///|
pub(all) struct CommandWatchScalableTopics {
  mut watch_id : UInt64
  mut namespace_ : String
  mut property_filters : Array[KeyValue]
  mut current_hash : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchScalableTopics with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watch_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.namespace_))
  for s in self.property_filters {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(s))
  }
  if self.current_hash is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandWatchScalableTopics with fn default() -> CommandWatchScalableTopics {
  CommandWatchScalableTopics::{
    watch_id: Default::default(),
    namespace_: Default::default(),
    property_filters: [],
    current_hash: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchScalableTopics::CommandWatchScalableTopics(
  watch_id : UInt64,
  namespace_ : String,
  property_filters : Array[KeyValue],
  current_hash? : String,
) -> CommandWatchScalableTopics {
  CommandWatchScalableTopics::{
    watch_id,
    namespace_,
    property_filters,
    current_hash,
  }
}

///|
pub impl @lib.Read for CommandWatchScalableTopics with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchScalableTopics raise {
  let msg : CommandWatchScalableTopics = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.read_uint64()
      (2, _) => msg.namespace_ = reader |> @lib.read_string()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.property_filters.push((reader |> @lib.read_message() : KeyValue))
      (4, _) => msg.current_hash = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchScalableTopics with fn write(
  self : CommandWatchScalableTopics,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watch_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.namespace_)
  for item in self.property_filters {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(item)
  }
  if self.current_hash is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandWatchScalableTopics with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watch_id != Default::default() {
    json["watchId"] = ToJson::to_json(self.watch_id)
  }
  if self.namespace_ != Default::default() {
    json["namespace"] = ToJson::to_json(self.namespace_)
  }
  if self.property_filters != Default::default() {
    json["propertyFilters"] = ToJson::to_json(self.property_filters)
  }
  match self.current_hash {
    Some(v) => json["currentHash"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchScalableTopics with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchScalableTopics raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchScalableTopics"),
    )
  }
  let message : CommandWatchScalableTopics = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watchId", value) => message.watch_id = @json.from_json(value, path~)
      ("namespace", value) => message.namespace_ = @json.from_json(value, path~)
      ("propertyFilters", Array(value)) =>
        message.property_filters = value.map(v => @json.from_json(v, path~))
      ("currentHash", value) =>
        message.current_hash = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchScalableTopics with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchScalableTopics raise {
  let msg : CommandWatchScalableTopics = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.namespace_ = reader |> @lib.async_read_string()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.property_filters.push(
          (reader |> @lib.async_read_message() : KeyValue),
        )
      (4, _) => msg.current_hash = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchScalableTopics with fn write(
  self : CommandWatchScalableTopics,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watch_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.namespace_)
  for item in self.property_filters {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(item)
  }
  if self.current_hash is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct ScalableTopicsSnapshot {
  mut topics : Array[String]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for ScalableTopicsSnapshot with fn size_of(self) {
  let mut size = 0U
  for s in self.topics {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for ScalableTopicsSnapshot with fn default() -> ScalableTopicsSnapshot {
  ScalableTopicsSnapshot::{ topics: [], }
}

///|
#alias(new, deprecated)
pub fn ScalableTopicsSnapshot::ScalableTopicsSnapshot(
  topics : Array[String],
) -> ScalableTopicsSnapshot {
  ScalableTopicsSnapshot::{ topics, }
}

///|
pub impl @lib.Read for ScalableTopicsSnapshot with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> ScalableTopicsSnapshot raise {
  let msg : ScalableTopicsSnapshot = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topics.push(reader |> @lib.read_string())
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for ScalableTopicsSnapshot with fn write(
  self : ScalableTopicsSnapshot,
  writer : &@lib.Writer,
) -> Unit raise {
  for item in self.topics {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_string(item)
  }
}

///|
pub impl ToJson for ScalableTopicsSnapshot with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.topics != Default::default() {
    json["topics"] = ToJson::to_json(self.topics)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for ScalableTopicsSnapshot with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ScalableTopicsSnapshot raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for ScalableTopicsSnapshot"),
    )
  }
  let message : ScalableTopicsSnapshot = Default::default()
  for key, value in obj {
    match (key, value) {
      ("topics", Array(value)) =>
        message.topics = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for ScalableTopicsSnapshot with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> ScalableTopicsSnapshot raise {
  let msg : ScalableTopicsSnapshot = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topics.push(reader |> @lib.async_read_string())
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for ScalableTopicsSnapshot with fn write(
  self : ScalableTopicsSnapshot,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  for item in self.topics {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_string(item)
  }
}

///|
pub(all) struct ScalableTopicsDiff {
  mut added : Array[String]
  mut removed : Array[String]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for ScalableTopicsDiff with fn size_of(self) {
  let mut size = 0U
  for s in self.added {
    size += @lib.size_of_length_delimited_field(1U, @lib.size_of(s))
  }
  for s in self.removed {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for ScalableTopicsDiff with fn default() -> ScalableTopicsDiff {
  ScalableTopicsDiff::{ added: [], removed: [], }
}

///|
#alias(new, deprecated)
pub fn ScalableTopicsDiff::ScalableTopicsDiff(
  added : Array[String],
  removed : Array[String],
) -> ScalableTopicsDiff {
  ScalableTopicsDiff::{ added, removed, }
}

///|
pub impl @lib.Read for ScalableTopicsDiff with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> ScalableTopicsDiff raise {
  let msg : ScalableTopicsDiff = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.added.push(reader |> @lib.read_string())
      (2, _) => msg.removed.push(reader |> @lib.read_string())
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for ScalableTopicsDiff with fn write(
  self : ScalableTopicsDiff,
  writer : &@lib.Writer,
) -> Unit raise {
  for item in self.added {
    writer |> @lib.write_varint(10UL)
    writer |> @lib.write_string(item)
  }
  for item in self.removed {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_string(item)
  }
}

///|
pub impl ToJson for ScalableTopicsDiff with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.added != Default::default() {
    json["added"] = ToJson::to_json(self.added)
  }
  if self.removed != Default::default() {
    json["removed"] = ToJson::to_json(self.removed)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for ScalableTopicsDiff with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> ScalableTopicsDiff raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for ScalableTopicsDiff"),
    )
  }
  let message : ScalableTopicsDiff = Default::default()
  for key, value in obj {
    match (key, value) {
      ("added", Array(value)) =>
        message.added = value.map(v => @json.from_json(v, path~))
      ("removed", Array(value)) =>
        message.removed = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for ScalableTopicsDiff with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> ScalableTopicsDiff raise {
  let msg : ScalableTopicsDiff = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.added.push(reader |> @lib.async_read_string())
      (2, _) => msg.removed.push(reader |> @lib.async_read_string())
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for ScalableTopicsDiff with fn write(
  self : ScalableTopicsDiff,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  for item in self.added {
    writer |> @lib.async_write_varint(10UL)
    writer |> @lib.async_write_string(item)
  }
  for item in self.removed {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_string(item)
  }
}

///|
pub(all) struct CommandWatchScalableTopicsUpdate {
  mut watch_id : UInt64
  mut error : ServerError?
  mut message : String?
  mut event : CommandWatchScalableTopicsUpdate_Event
} derive(Eq, Debug)

///|
pub(all) enum CommandWatchScalableTopicsUpdate_Event {
  Snapshot(ScalableTopicsSnapshot)
  Diff(ScalableTopicsDiff)
  NotSet
} derive(Eq, Debug)

///|
pub impl Default for CommandWatchScalableTopicsUpdate_Event with fn default() -> CommandWatchScalableTopicsUpdate_Event {
  NotSet
}

///|
pub impl @json.FromJson for CommandWatchScalableTopicsUpdate_Event with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchScalableTopicsUpdate_Event raise {
  try
    CommandWatchScalableTopicsUpdate_Event::Snapshot(
      json |> @json.from_json(path~),
    )
  catch {
    _ => ()
  } noraise {
    v => return v
  }
  try
    CommandWatchScalableTopicsUpdate_Event::Diff(json |> @json.from_json(path~))
  catch {
    _ => ()
  } noraise {
    v => return v
  }
  CommandWatchScalableTopicsUpdate_Event::NotSet
}

///|
pub impl ToJson for CommandWatchScalableTopicsUpdate_Event with fn to_json(
  self : CommandWatchScalableTopicsUpdate_Event,
) -> Json {
  match self {
    CommandWatchScalableTopicsUpdate_Event::Snapshot(v) => ToJson::to_json(v)
    CommandWatchScalableTopicsUpdate_Event::Diff(v) => ToJson::to_json(v)
    CommandWatchScalableTopicsUpdate_Event::NotSet => Json::null()
  }
}

///|
pub impl @lib.Sized for CommandWatchScalableTopicsUpdate with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watch_id))
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  match self.event {
    Snapshot(v) =>
      size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
    Diff(v) => size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
    NotSet => ()
  }
  size
}

///|
pub impl Default for CommandWatchScalableTopicsUpdate with fn default() -> CommandWatchScalableTopicsUpdate {
  CommandWatchScalableTopicsUpdate::{
    watch_id: Default::default(),
    error: None,
    message: None,
    event: CommandWatchScalableTopicsUpdate_Event::NotSet,
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchScalableTopicsUpdate::CommandWatchScalableTopicsUpdate(
  watch_id : UInt64,
  error? : ServerError,
  message? : String,
  event? : CommandWatchScalableTopicsUpdate_Event = CommandWatchScalableTopicsUpdate_Event::NotSet,
) -> CommandWatchScalableTopicsUpdate {
  CommandWatchScalableTopicsUpdate::{ watch_id, error, message, event, }
}

///|
pub impl @lib.Read for CommandWatchScalableTopicsUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchScalableTopicsUpdate raise {
  let msg : CommandWatchScalableTopicsUpdate = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.read_uint64()
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.event = (reader |> @lib.read_message() : ScalableTopicsSnapshot)
          |> CommandWatchScalableTopicsUpdate_Event::Snapshot
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.event = (reader |> @lib.read_message() : ScalableTopicsDiff)
          |> CommandWatchScalableTopicsUpdate_Event::Diff
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchScalableTopicsUpdate with fn write(
  self : CommandWatchScalableTopicsUpdate,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watch_id)
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
  match self.event {
    CommandWatchScalableTopicsUpdate_Event::Snapshot(v) => {
      writer |> @lib.write_varint(18UL)
      writer |> @lib.write_message(v)
    }
    CommandWatchScalableTopicsUpdate_Event::Diff(v) => {
      writer |> @lib.write_varint(26UL)
      writer |> @lib.write_message(v)
    }
    CommandWatchScalableTopicsUpdate_Event::NotSet => ()
  }
}

///|
pub impl ToJson for CommandWatchScalableTopicsUpdate with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watch_id != Default::default() {
    json["watchId"] = ToJson::to_json(self.watch_id)
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  match self.event {
    NotSet => ()
    Snapshot(v) => json["snapshot"] = ToJson::to_json(v)
    Diff(v) => json["diff"] = ToJson::to_json(v)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchScalableTopicsUpdate with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchScalableTopicsUpdate raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchScalableTopicsUpdate"),
    )
  }
  let message : CommandWatchScalableTopicsUpdate = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watchId", value) => message.watch_id = @json.from_json(value, path~)
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      ("snapshot", value) => message.event = @json.from_json(value, path~)
      ("diff", value) => message.event = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchScalableTopicsUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchScalableTopicsUpdate raise {
  let msg : CommandWatchScalableTopicsUpdate = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.async_read_uint64()
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.event = (
            reader |> @lib.async_read_message() : ScalableTopicsSnapshot)
          |> CommandWatchScalableTopicsUpdate_Event::Snapshot
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.event = (reader |> @lib.async_read_message() : ScalableTopicsDiff)
          |> CommandWatchScalableTopicsUpdate_Event::Diff
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchScalableTopicsUpdate with fn write(
  self : CommandWatchScalableTopicsUpdate,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watch_id)
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
  match self.event {
    CommandWatchScalableTopicsUpdate_Event::Snapshot(v) => {
      writer |> @lib.async_write_varint(18UL)
      writer |> @lib.async_write_message(v)
    }
    CommandWatchScalableTopicsUpdate_Event::Diff(v) => {
      writer |> @lib.async_write_varint(26UL)
      writer |> @lib.async_write_message(v)
    }
    CommandWatchScalableTopicsUpdate_Event::NotSet => ()
  }
}

///|
pub(all) struct CommandWatchScalableTopicsClose {
  mut watch_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchScalableTopicsClose with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watch_id))
  size
}

///|
pub impl Default for CommandWatchScalableTopicsClose with fn default() -> CommandWatchScalableTopicsClose {
  CommandWatchScalableTopicsClose::{ watch_id: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn CommandWatchScalableTopicsClose::CommandWatchScalableTopicsClose(
  watch_id : UInt64,
) -> CommandWatchScalableTopicsClose {
  CommandWatchScalableTopicsClose::{ watch_id, }
}

///|
pub impl @lib.Read for CommandWatchScalableTopicsClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchScalableTopicsClose raise {
  let msg : CommandWatchScalableTopicsClose = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchScalableTopicsClose with fn write(
  self : CommandWatchScalableTopicsClose,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watch_id)
}

///|
pub impl ToJson for CommandWatchScalableTopicsClose with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watch_id != Default::default() {
    json["watchId"] = ToJson::to_json(self.watch_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchScalableTopicsClose with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchScalableTopicsClose raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchScalableTopicsClose"),
    )
  }
  let message : CommandWatchScalableTopicsClose = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watchId", value) => message.watch_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchScalableTopicsClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchScalableTopicsClose raise {
  let msg : CommandWatchScalableTopicsClose = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchScalableTopicsClose with fn write(
  self : CommandWatchScalableTopicsClose,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watch_id)
}

///|
pub(all) struct CommandWatchTcAssignments {
  mut watch_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTcAssignments with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watch_id))
  size
}

///|
pub impl Default for CommandWatchTcAssignments with fn default() -> CommandWatchTcAssignments {
  CommandWatchTcAssignments::{ watch_id: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTcAssignments::CommandWatchTcAssignments(
  watch_id : UInt64,
) -> CommandWatchTcAssignments {
  CommandWatchTcAssignments::{ watch_id, }
}

///|
pub impl @lib.Read for CommandWatchTcAssignments with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTcAssignments raise {
  let msg : CommandWatchTcAssignments = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTcAssignments with fn write(
  self : CommandWatchTcAssignments,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watch_id)
}

///|
pub impl ToJson for CommandWatchTcAssignments with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watch_id != Default::default() {
    json["watchId"] = ToJson::to_json(self.watch_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTcAssignments with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTcAssignments raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTcAssignments"),
    )
  }
  let message : CommandWatchTcAssignments = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watchId", value) => message.watch_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTcAssignments with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTcAssignments raise {
  let msg : CommandWatchTcAssignments = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTcAssignments with fn write(
  self : CommandWatchTcAssignments,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watch_id)
}

///|
pub(all) struct TcAssignment {
  mut tc_id : UInt64
  mut broker_service_url : String?
  mut broker_service_url_tls : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for TcAssignment with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.tc_id))
  if self.broker_service_url is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.broker_service_url_tls is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for TcAssignment with fn default() -> TcAssignment {
  TcAssignment::{
    tc_id: Default::default(),
    broker_service_url: None,
    broker_service_url_tls: None,
  }
}

///|
#alias(new, deprecated)
pub fn TcAssignment::TcAssignment(
  tc_id : UInt64,
  broker_service_url? : String,
  broker_service_url_tls? : String,
) -> TcAssignment {
  TcAssignment::{ tc_id, broker_service_url, broker_service_url_tls, }
}

///|
pub impl @lib.Read for TcAssignment with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> TcAssignment raise {
  let msg : TcAssignment = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.tc_id = reader |> @lib.read_uint64()
      (2, _) => msg.broker_service_url = reader |> @lib.read_string() |> Some
      (3, _) =>
        msg.broker_service_url_tls = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for TcAssignment with fn write(
  self : TcAssignment,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.tc_id)
  if self.broker_service_url is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_string(v)
  }
  if self.broker_service_url_tls is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for TcAssignment with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.tc_id != Default::default() {
    json["tcId"] = ToJson::to_json(self.tc_id)
  }
  match self.broker_service_url {
    Some(v) => json["brokerServiceUrl"] = ToJson::to_json(v)
    _ => ()
  }
  match self.broker_service_url_tls {
    Some(v) => json["brokerServiceUrlTls"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for TcAssignment with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> TcAssignment raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for TcAssignment"))
  }
  let message : TcAssignment = Default::default()
  for key, value in obj {
    match (key, value) {
      ("tcId", value) => message.tc_id = @json.from_json(value, path~)
      ("brokerServiceUrl", value) =>
        message.broker_service_url = Some(@json.from_json(value, path~))
      ("brokerServiceUrlTls", value) =>
        message.broker_service_url_tls = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for TcAssignment with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> TcAssignment raise {
  let msg : TcAssignment = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.tc_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.broker_service_url = reader |> @lib.async_read_string() |> Some
      (3, _) =>
        msg.broker_service_url_tls = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for TcAssignment with fn write(
  self : TcAssignment,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.tc_id)
  if self.broker_service_url is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_string(v)
  }
  if self.broker_service_url_tls is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct TcAssignmentsSnapshot {
  mut parallelism : UInt
  mut assignments : Array[TcAssignment]
} derive(Eq, Debug)

///|
pub impl @lib.Sized for TcAssignmentsSnapshot with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.parallelism))
  for s in self.assignments {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(s))
  }
  size
}

///|
pub impl Default for TcAssignmentsSnapshot with fn default() -> TcAssignmentsSnapshot {
  TcAssignmentsSnapshot::{ parallelism: Default::default(), assignments: [], }
}

///|
#alias(new, deprecated)
pub fn TcAssignmentsSnapshot::TcAssignmentsSnapshot(
  parallelism : UInt,
  assignments : Array[TcAssignment],
) -> TcAssignmentsSnapshot {
  TcAssignmentsSnapshot::{ parallelism, assignments, }
}

///|
pub impl @lib.Read for TcAssignmentsSnapshot with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> TcAssignmentsSnapshot raise {
  let msg : TcAssignmentsSnapshot = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.parallelism = reader |> @lib.read_uint32()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.assignments.push((reader |> @lib.read_message() : TcAssignment))
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for TcAssignmentsSnapshot with fn write(
  self : TcAssignmentsSnapshot,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint32(self.parallelism)
  for item in self.assignments {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(item)
  }
}

///|
pub impl ToJson for TcAssignmentsSnapshot with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.parallelism != Default::default() {
    json["parallelism"] = ToJson::to_json(self.parallelism)
  }
  if self.assignments != Default::default() {
    json["assignments"] = ToJson::to_json(self.assignments)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for TcAssignmentsSnapshot with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> TcAssignmentsSnapshot raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for TcAssignmentsSnapshot"),
    )
  }
  let message : TcAssignmentsSnapshot = Default::default()
  for key, value in obj {
    match (key, value) {
      ("parallelism", value) =>
        message.parallelism = @json.from_json(value, path~)
      ("assignments", Array(value)) =>
        message.assignments = value.map(v => @json.from_json(v, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for TcAssignmentsSnapshot with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> TcAssignmentsSnapshot raise {
  let msg : TcAssignmentsSnapshot = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.parallelism = reader |> @lib.async_read_uint32()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.assignments.push(
          (reader |> @lib.async_read_message() : TcAssignment),
        )
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for TcAssignmentsSnapshot with fn write(
  self : TcAssignmentsSnapshot,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint32(self.parallelism)
  for item in self.assignments {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(item)
  }
}

///|
pub(all) struct CommandWatchTcAssignmentsUpdate {
  mut watch_id : UInt64
  mut snapshot : TcAssignmentsSnapshot?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTcAssignmentsUpdate with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watch_id))
  if self.snapshot is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandWatchTcAssignmentsUpdate with fn default() -> CommandWatchTcAssignmentsUpdate {
  CommandWatchTcAssignmentsUpdate::{
    watch_id: Default::default(),
    snapshot: None,
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTcAssignmentsUpdate::CommandWatchTcAssignmentsUpdate(
  watch_id : UInt64,
  snapshot? : TcAssignmentsSnapshot,
  error? : ServerError,
  message? : String,
) -> CommandWatchTcAssignmentsUpdate {
  CommandWatchTcAssignmentsUpdate::{ watch_id, snapshot, error, message, }
}

///|
pub impl @lib.Read for CommandWatchTcAssignmentsUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTcAssignmentsUpdate raise {
  let msg : CommandWatchTcAssignmentsUpdate = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.snapshot = (reader |> @lib.read_message() : TcAssignmentsSnapshot)
          |> Some
      (3, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (4, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTcAssignmentsUpdate with fn write(
  self : CommandWatchTcAssignmentsUpdate,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watch_id)
  if self.snapshot is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandWatchTcAssignmentsUpdate with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watch_id != Default::default() {
    json["watchId"] = ToJson::to_json(self.watch_id)
  }
  match self.snapshot {
    Some(v) => json["snapshot"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTcAssignmentsUpdate with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTcAssignmentsUpdate raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTcAssignmentsUpdate"),
    )
  }
  let message : CommandWatchTcAssignmentsUpdate = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watchId", value) => message.watch_id = @json.from_json(value, path~)
      ("snapshot", value) =>
        message.snapshot = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTcAssignmentsUpdate with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTcAssignmentsUpdate raise {
  let msg : CommandWatchTcAssignmentsUpdate = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.async_read_uint64()
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.snapshot = (
            reader |> @lib.async_read_message() : TcAssignmentsSnapshot)
          |> Some
      (3, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (4, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTcAssignmentsUpdate with fn write(
  self : CommandWatchTcAssignmentsUpdate,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watch_id)
  if self.snapshot is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandWatchTcAssignmentsClose {
  mut watch_id : UInt64
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandWatchTcAssignmentsClose with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.watch_id))
  size
}

///|
pub impl Default for CommandWatchTcAssignmentsClose with fn default() -> CommandWatchTcAssignmentsClose {
  CommandWatchTcAssignmentsClose::{ watch_id: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn CommandWatchTcAssignmentsClose::CommandWatchTcAssignmentsClose(
  watch_id : UInt64,
) -> CommandWatchTcAssignmentsClose {
  CommandWatchTcAssignmentsClose::{ watch_id, }
}

///|
pub impl @lib.Read for CommandWatchTcAssignmentsClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandWatchTcAssignmentsClose raise {
  let msg : CommandWatchTcAssignmentsClose = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.read_uint64()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandWatchTcAssignmentsClose with fn write(
  self : CommandWatchTcAssignmentsClose,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.watch_id)
}

///|
pub impl ToJson for CommandWatchTcAssignmentsClose with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.watch_id != Default::default() {
    json["watchId"] = ToJson::to_json(self.watch_id)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandWatchTcAssignmentsClose with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandWatchTcAssignmentsClose raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandWatchTcAssignmentsClose"),
    )
  }
  let message : CommandWatchTcAssignmentsClose = Default::default()
  for key, value in obj {
    match (key, value) {
      ("watchId", value) => message.watch_id = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandWatchTcAssignmentsClose with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandWatchTcAssignmentsClose raise {
  let msg : CommandWatchTcAssignmentsClose = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.watch_id = reader |> @lib.async_read_uint64()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandWatchTcAssignmentsClose with fn write(
  self : CommandWatchTcAssignmentsClose,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.watch_id)
}

///|
pub(all) struct CommandGetSchema {
  mut request_id : UInt64
  mut topic : String
  mut schema_version : Bytes?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetSchema with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.topic))
  if self.schema_version is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandGetSchema with fn default() -> CommandGetSchema {
  CommandGetSchema::{
    request_id: Default::default(),
    topic: Default::default(),
    schema_version: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetSchema::CommandGetSchema(
  request_id : UInt64,
  topic : String,
  schema_version? : Bytes,
) -> CommandGetSchema {
  CommandGetSchema::{ request_id, topic, schema_version, }
}

///|
pub impl @lib.Read for CommandGetSchema with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetSchema raise {
  let msg : CommandGetSchema = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.topic = reader |> @lib.read_string()
      (3, _) => msg.schema_version = reader |> @lib.read_bytes() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetSchema with fn write(
  self : CommandGetSchema,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.topic)
  if self.schema_version is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_bytes(v)
  }
}

///|
pub impl ToJson for CommandGetSchema with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  match self.schema_version {
    Some(v) => json["schemaVersion"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetSchema with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetSchema raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetSchema"),
    )
  }
  let message : CommandGetSchema = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("schemaVersion", String(value)) =>
        message.schema_version = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetSchema with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetSchema raise {
  let msg : CommandGetSchema = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.topic = reader |> @lib.async_read_string()
      (3, _) => msg.schema_version = reader |> @lib.async_read_bytes() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetSchema with fn write(
  self : CommandGetSchema,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.topic)
  if self.schema_version is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_bytes(v)
  }
}

///|
pub(all) struct CommandGetSchemaResponse {
  mut request_id : UInt64
  mut error_code : ServerError?
  mut error_message : String?
  mut schema : Schema?
  mut schema_version : Bytes?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetSchemaResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.error_code is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.error_message is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.schema is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.schema_version is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandGetSchemaResponse with fn default() -> CommandGetSchemaResponse {
  CommandGetSchemaResponse::{
    request_id: Default::default(),
    error_code: None,
    error_message: None,
    schema: None,
    schema_version: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetSchemaResponse::CommandGetSchemaResponse(
  request_id : UInt64,
  error_code? : ServerError,
  error_message? : String,
  schema? : Schema,
  schema_version? : Bytes,
) -> CommandGetSchemaResponse {
  CommandGetSchemaResponse::{
    request_id,
    error_code,
    error_message,
    schema,
    schema_version,
  }
}

///|
pub impl @lib.Read for CommandGetSchemaResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetSchemaResponse raise {
  let msg : CommandGetSchemaResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.error_code = reader
          |> @lib.read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.error_message = reader |> @lib.read_string() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.read_message() : Schema) |> Some
      (5, _) => msg.schema_version = reader |> @lib.read_bytes() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetSchemaResponse with fn write(
  self : CommandGetSchemaResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.error_code is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.error_message is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.schema is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_bytes(v)
  }
}

///|
pub impl ToJson for CommandGetSchemaResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.error_code {
    Some(v) => json["errorCode"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error_message {
    Some(v) => json["errorMessage"] = ToJson::to_json(v)
    _ => ()
  }
  match self.schema {
    Some(v) => json["schema"] = ToJson::to_json(v)
    _ => ()
  }
  match self.schema_version {
    Some(v) => json["schemaVersion"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetSchemaResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetSchemaResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetSchemaResponse"),
    )
  }
  let message : CommandGetSchemaResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("errorCode", value) =>
        message.error_code = Some(@json.from_json(value, path~))
      ("errorMessage", value) =>
        message.error_message = Some(@json.from_json(value, path~))
      ("schema", value) => message.schema = Some(@json.from_json(value, path~))
      ("schemaVersion", String(value)) =>
        message.schema_version = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetSchemaResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetSchemaResponse raise {
  let msg : CommandGetSchemaResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.error_code = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.error_message = reader |> @lib.async_read_string() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.async_read_message() : Schema) |> Some
      (5, _) => msg.schema_version = reader |> @lib.async_read_bytes() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetSchemaResponse with fn write(
  self : CommandGetSchemaResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.error_code is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.error_message is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.schema is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_bytes(v)
  }
}

///|
pub(all) struct CommandGetOrCreateSchema {
  mut request_id : UInt64
  mut topic : String
  mut schema : Schema
  mut producer_name : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetOrCreateSchema with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_length_delimited_field(2U, @lib.size_of(self.topic))
  size += @lib.size_of_length_delimited_field(3U, @lib.size_of(self.schema))
  if self.producer_name is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandGetOrCreateSchema with fn default() -> CommandGetOrCreateSchema {
  CommandGetOrCreateSchema::{
    request_id: Default::default(),
    topic: Default::default(),
    schema: Default::default(),
    producer_name: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetOrCreateSchema::CommandGetOrCreateSchema(
  request_id : UInt64,
  topic : String,
  schema : Schema,
  producer_name? : String,
) -> CommandGetOrCreateSchema {
  CommandGetOrCreateSchema::{ request_id, topic, schema, producer_name, }
}

///|
pub impl @lib.Read for CommandGetOrCreateSchema with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetOrCreateSchema raise {
  let msg : CommandGetOrCreateSchema = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.topic = reader |> @lib.read_string()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.read_message() : Schema)
      (4, _) => msg.producer_name = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetOrCreateSchema with fn write(
  self : CommandGetOrCreateSchema,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(26UL)
  writer |> @lib.write_message(self.schema)
  if self.producer_name is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandGetOrCreateSchema with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.schema != Default::default() {
    json["schema"] = ToJson::to_json(self.schema)
  }
  match self.producer_name {
    Some(v) => json["producerName"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetOrCreateSchema with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetOrCreateSchema raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetOrCreateSchema"),
    )
  }
  let message : CommandGetOrCreateSchema = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("schema", value) => message.schema = @json.from_json(value, path~)
      ("producerName", value) =>
        message.producer_name = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetOrCreateSchema with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetOrCreateSchema raise {
  let msg : CommandGetOrCreateSchema = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.topic = reader |> @lib.async_read_string()
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.schema = (reader |> @lib.async_read_message() : Schema)
      (4, _) => msg.producer_name = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetOrCreateSchema with fn write(
  self : CommandGetOrCreateSchema,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(26UL)
  writer |> @lib.async_write_message(self.schema)
  if self.producer_name is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandGetOrCreateSchemaResponse {
  mut request_id : UInt64
  mut error_code : ServerError?
  mut error_message : String?
  mut schema_version : Bytes?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandGetOrCreateSchemaResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.error_code is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.error_message is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.schema_version is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandGetOrCreateSchemaResponse with fn default() -> CommandGetOrCreateSchemaResponse {
  CommandGetOrCreateSchemaResponse::{
    request_id: Default::default(),
    error_code: None,
    error_message: None,
    schema_version: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandGetOrCreateSchemaResponse::CommandGetOrCreateSchemaResponse(
  request_id : UInt64,
  error_code? : ServerError,
  error_message? : String,
  schema_version? : Bytes,
) -> CommandGetOrCreateSchemaResponse {
  CommandGetOrCreateSchemaResponse::{
    request_id,
    error_code,
    error_message,
    schema_version,
  }
}

///|
pub impl @lib.Read for CommandGetOrCreateSchemaResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandGetOrCreateSchemaResponse raise {
  let msg : CommandGetOrCreateSchemaResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.error_code = reader
          |> @lib.read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.error_message = reader |> @lib.read_string() |> Some
      (4, _) => msg.schema_version = reader |> @lib.read_bytes() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandGetOrCreateSchemaResponse with fn write(
  self : CommandGetOrCreateSchemaResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.error_code is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.error_message is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_bytes(v)
  }
}

///|
pub impl ToJson for CommandGetOrCreateSchemaResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.error_code {
    Some(v) => json["errorCode"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error_message {
    Some(v) => json["errorMessage"] = ToJson::to_json(v)
    _ => ()
  }
  match self.schema_version {
    Some(v) => json["schemaVersion"] = @lib.base64_encode(v).to_json()
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandGetOrCreateSchemaResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandGetOrCreateSchemaResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandGetOrCreateSchemaResponse"),
    )
  }
  let message : CommandGetOrCreateSchemaResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("errorCode", value) =>
        message.error_code = Some(@json.from_json(value, path~))
      ("errorMessage", value) =>
        message.error_message = Some(@json.from_json(value, path~))
      ("schemaVersion", String(value)) =>
        message.schema_version = Some(
          @lib.base64_decode(value) catch {
            _ => raise @json.JsonDecodeError((path, "Invalid base64 encoding"))
          },
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandGetOrCreateSchemaResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandGetOrCreateSchemaResponse raise {
  let msg : CommandGetOrCreateSchemaResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.error_code = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.error_message = reader |> @lib.async_read_string() |> Some
      (4, _) => msg.schema_version = reader |> @lib.async_read_bytes() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandGetOrCreateSchemaResponse with fn write(
  self : CommandGetOrCreateSchemaResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.error_code is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.error_message is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
  if self.schema_version is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_bytes(v)
  }
}

///|
pub(all) struct CommandTcClientConnectRequest {
  mut request_id : UInt64
  mut tc_id : UInt64
  mut scalable : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandTcClientConnectRequest with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  size += @lib.size_of_field(2U, @lib.size_of(self.tc_id))
  if self.scalable is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandTcClientConnectRequest with fn default() -> CommandTcClientConnectRequest {
  CommandTcClientConnectRequest::{
    request_id: Default::default(),
    tc_id: 0,
    scalable: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandTcClientConnectRequest::CommandTcClientConnectRequest(
  request_id : UInt64,
  tc_id : UInt64,
  scalable? : Bool,
) -> CommandTcClientConnectRequest {
  CommandTcClientConnectRequest::{ request_id, tc_id, scalable, }
}

///|
pub impl @lib.Read for CommandTcClientConnectRequest with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandTcClientConnectRequest raise {
  let msg : CommandTcClientConnectRequest = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.tc_id = reader |> @lib.read_uint64()
      (3, _) => msg.scalable = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandTcClientConnectRequest with fn write(
  self : CommandTcClientConnectRequest,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  writer |> @lib.write_varint(16UL)
  writer |> @lib.write_uint64(self.tc_id)
  if self.scalable is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandTcClientConnectRequest with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  if self.tc_id != 0 {
    json["tcId"] = ToJson::to_json(self.tc_id)
  }
  match self.scalable {
    Some(v) if v != false => json["scalable"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandTcClientConnectRequest with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandTcClientConnectRequest raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandTcClientConnectRequest"),
    )
  }
  let message : CommandTcClientConnectRequest = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("tcId", value) => message.tc_id = @json.from_json(value, path~)
      ("scalable", value) =>
        message.scalable = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandTcClientConnectRequest with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandTcClientConnectRequest raise {
  let msg : CommandTcClientConnectRequest = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.tc_id = reader |> @lib.async_read_uint64()
      (3, _) => msg.scalable = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandTcClientConnectRequest with fn write(
  self : CommandTcClientConnectRequest,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  writer |> @lib.async_write_varint(16UL)
  writer |> @lib.async_write_uint64(self.tc_id)
  if self.scalable is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandTcClientConnectResponse {
  mut request_id : UInt64
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandTcClientConnectResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.error is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandTcClientConnectResponse with fn default() -> CommandTcClientConnectResponse {
  CommandTcClientConnectResponse::{
    request_id: Default::default(),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandTcClientConnectResponse::CommandTcClientConnectResponse(
  request_id : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandTcClientConnectResponse {
  CommandTcClientConnectResponse::{ request_id, error, message, }
}

///|
pub impl @lib.Read for CommandTcClientConnectResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandTcClientConnectResponse raise {
  let msg : CommandTcClientConnectResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (3, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandTcClientConnectResponse with fn write(
  self : CommandTcClientConnectResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.error is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandTcClientConnectResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandTcClientConnectResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandTcClientConnectResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandTcClientConnectResponse"),
    )
  }
  let message : CommandTcClientConnectResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandTcClientConnectResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandTcClientConnectResponse raise {
  let msg : CommandTcClientConnectResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (3, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandTcClientConnectResponse with fn write(
  self : CommandTcClientConnectResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandNewTxn {
  mut request_id : UInt64
  mut txn_ttl_millis : UInt64?
  mut tc_id : UInt64?
  mut scalable : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandNewTxn with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txn_ttl_millis is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.tc_id is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.scalable is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandNewTxn with fn default() -> CommandNewTxn {
  CommandNewTxn::{
    request_id: Default::default(),
    txn_ttl_millis: Some(0),
    tc_id: Some(0),
    scalable: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandNewTxn::CommandNewTxn(
  request_id : UInt64,
  txn_ttl_millis? : UInt64,
  tc_id? : UInt64,
  scalable? : Bool,
) -> CommandNewTxn {
  CommandNewTxn::{ request_id, txn_ttl_millis, tc_id, scalable, }
}

///|
pub impl @lib.Read for CommandNewTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandNewTxn raise {
  let msg : CommandNewTxn = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txn_ttl_millis = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.tc_id = reader |> @lib.read_uint64() |> Some
      (4, _) => msg.scalable = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandNewTxn with fn write(
  self : CommandNewTxn,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txn_ttl_millis is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.tc_id is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.scalable is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandNewTxn with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txn_ttl_millis {
    Some(v) if v != 0 => json["txnTtlMillis"] = ToJson::to_json(v)
    _ => ()
  }
  match self.tc_id {
    Some(v) if v != 0 => json["tcId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable {
    Some(v) if v != false => json["scalable"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandNewTxn with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandNewTxn raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandNewTxn"))
  }
  let message : CommandNewTxn = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnTtlMillis", value) =>
        message.txn_ttl_millis = Some(@json.from_json(value, path~))
      ("tcId", value) => message.tc_id = Some(@json.from_json(value, path~))
      ("scalable", value) =>
        message.scalable = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandNewTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandNewTxn raise {
  let msg : CommandNewTxn = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) => msg.txn_ttl_millis = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.tc_id = reader |> @lib.async_read_uint64() |> Some
      (4, _) => msg.scalable = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandNewTxn with fn write(
  self : CommandNewTxn,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txn_ttl_millis is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.tc_id is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.scalable is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandNewTxnResponse {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandNewTxnResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandNewTxnResponse with fn default() -> CommandNewTxnResponse {
  CommandNewTxnResponse::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandNewTxnResponse::CommandNewTxnResponse(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandNewTxnResponse {
  CommandNewTxnResponse::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandNewTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandNewTxnResponse raise {
  let msg : CommandNewTxnResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandNewTxnResponse with fn write(
  self : CommandNewTxnResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandNewTxnResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandNewTxnResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandNewTxnResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandNewTxnResponse"),
    )
  }
  let message : CommandNewTxnResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandNewTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandNewTxnResponse raise {
  let msg : CommandNewTxnResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandNewTxnResponse with fn write(
  self : CommandNewTxnResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandAddPartitionToTxn {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut partitions : Array[String]
  mut scalable : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAddPartitionToTxn with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  for s in self.partitions {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(s))
  }
  if self.scalable is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAddPartitionToTxn with fn default() -> CommandAddPartitionToTxn {
  CommandAddPartitionToTxn::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    partitions: [],
    scalable: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandAddPartitionToTxn::CommandAddPartitionToTxn(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  partitions : Array[String],
  scalable? : Bool,
) -> CommandAddPartitionToTxn {
  CommandAddPartitionToTxn::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    partitions,
    scalable,
  }
}

///|
pub impl @lib.Read for CommandAddPartitionToTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAddPartitionToTxn raise {
  let msg : CommandAddPartitionToTxn = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) => msg.partitions.push(reader |> @lib.read_string())
      (5, _) => msg.scalable = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAddPartitionToTxn with fn write(
  self : CommandAddPartitionToTxn,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  for item in self.partitions {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(item)
  }
  if self.scalable is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandAddPartitionToTxn with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  if self.partitions != Default::default() {
    json["partitions"] = ToJson::to_json(self.partitions)
  }
  match self.scalable {
    Some(v) if v != false => json["scalable"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAddPartitionToTxn with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAddPartitionToTxn raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAddPartitionToTxn"),
    )
  }
  let message : CommandAddPartitionToTxn = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("partitions", Array(value)) =>
        message.partitions = value.map(v => @json.from_json(v, path~))
      ("scalable", value) =>
        message.scalable = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAddPartitionToTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAddPartitionToTxn raise {
  let msg : CommandAddPartitionToTxn = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) => msg.partitions.push(reader |> @lib.async_read_string())
      (5, _) => msg.scalable = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAddPartitionToTxn with fn write(
  self : CommandAddPartitionToTxn,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  for item in self.partitions {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(item)
  }
  if self.scalable is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandAddPartitionToTxnResponse {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAddPartitionToTxnResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAddPartitionToTxnResponse with fn default() -> CommandAddPartitionToTxnResponse {
  CommandAddPartitionToTxnResponse::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandAddPartitionToTxnResponse::CommandAddPartitionToTxnResponse(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandAddPartitionToTxnResponse {
  CommandAddPartitionToTxnResponse::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandAddPartitionToTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAddPartitionToTxnResponse raise {
  let msg : CommandAddPartitionToTxnResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAddPartitionToTxnResponse with fn write(
  self : CommandAddPartitionToTxnResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandAddPartitionToTxnResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAddPartitionToTxnResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAddPartitionToTxnResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAddPartitionToTxnResponse"),
    )
  }
  let message : CommandAddPartitionToTxnResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAddPartitionToTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAddPartitionToTxnResponse raise {
  let msg : CommandAddPartitionToTxnResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAddPartitionToTxnResponse with fn write(
  self : CommandAddPartitionToTxnResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct Subscription {
  mut topic : String
  mut subscription : String
} derive(Eq, Debug)

///|
pub impl @lib.Sized for Subscription with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_length_delimited_field(1U, @lib.size_of(self.topic))
  size += @lib.size_of_length_delimited_field(
    2U,
    @lib.size_of(self.subscription),
  )
  size
}

///|
pub impl Default for Subscription with fn default() -> Subscription {
  Subscription::{ topic: Default::default(), subscription: Default::default(), }
}

///|
#alias(new, deprecated)
pub fn Subscription::Subscription(
  topic : String,
  subscription : String,
) -> Subscription {
  Subscription::{ topic, subscription, }
}

///|
pub impl @lib.Read for Subscription with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> Subscription raise {
  let msg : Subscription = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.read_string()
      (2, _) => msg.subscription = reader |> @lib.read_string()
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for Subscription with fn write(
  self : Subscription,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(10UL)
  writer |> @lib.write_string(self.topic)
  writer |> @lib.write_varint(18UL)
  writer |> @lib.write_string(self.subscription)
}

///|
pub impl ToJson for Subscription with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.topic != Default::default() {
    json["topic"] = ToJson::to_json(self.topic)
  }
  if self.subscription != Default::default() {
    json["subscription"] = ToJson::to_json(self.subscription)
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for Subscription with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> Subscription raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for Subscription"))
  }
  let message : Subscription = Default::default()
  for key, value in obj {
    match (key, value) {
      ("topic", value) => message.topic = @json.from_json(value, path~)
      ("subscription", value) =>
        message.subscription = @json.from_json(value, path~)
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for Subscription with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> Subscription raise {
  let msg : Subscription = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.topic = reader |> @lib.async_read_string()
      (2, _) => msg.subscription = reader |> @lib.async_read_string()
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for Subscription with fn write(
  self : Subscription,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(10UL)
  writer |> @lib.async_write_string(self.topic)
  writer |> @lib.async_write_varint(18UL)
  writer |> @lib.async_write_string(self.subscription)
}

///|
pub(all) struct CommandAddSubscriptionToTxn {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut subscription : Array[Subscription]
  mut scalable : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAddSubscriptionToTxn with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  for s in self.subscription {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(s))
  }
  if self.scalable is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAddSubscriptionToTxn with fn default() -> CommandAddSubscriptionToTxn {
  CommandAddSubscriptionToTxn::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    subscription: [],
    scalable: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandAddSubscriptionToTxn::CommandAddSubscriptionToTxn(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  subscription : Array[Subscription],
  scalable? : Bool,
) -> CommandAddSubscriptionToTxn {
  CommandAddSubscriptionToTxn::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    subscription,
    scalable,
  }
}

///|
pub impl @lib.Read for CommandAddSubscriptionToTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAddSubscriptionToTxn raise {
  let msg : CommandAddSubscriptionToTxn = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscription.push((reader |> @lib.read_message() : Subscription))
      (5, _) => msg.scalable = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAddSubscriptionToTxn with fn write(
  self : CommandAddSubscriptionToTxn,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  for item in self.subscription {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(item)
  }
  if self.scalable is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandAddSubscriptionToTxn with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  if self.subscription != Default::default() {
    json["subscription"] = ToJson::to_json(self.subscription)
  }
  match self.scalable {
    Some(v) if v != false => json["scalable"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAddSubscriptionToTxn with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAddSubscriptionToTxn raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAddSubscriptionToTxn"),
    )
  }
  let message : CommandAddSubscriptionToTxn = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("subscription", Array(value)) =>
        message.subscription = value.map(v => @json.from_json(v, path~))
      ("scalable", value) =>
        message.scalable = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAddSubscriptionToTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAddSubscriptionToTxn raise {
  let msg : CommandAddSubscriptionToTxn = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscription.push(
          (reader |> @lib.async_read_message() : Subscription),
        )
      (5, _) => msg.scalable = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAddSubscriptionToTxn with fn write(
  self : CommandAddSubscriptionToTxn,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  for item in self.subscription {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(item)
  }
  if self.scalable is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandAddSubscriptionToTxnResponse {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandAddSubscriptionToTxnResponse with fn size_of(
  self,
) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandAddSubscriptionToTxnResponse with fn default() -> CommandAddSubscriptionToTxnResponse {
  CommandAddSubscriptionToTxnResponse::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandAddSubscriptionToTxnResponse::CommandAddSubscriptionToTxnResponse(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandAddSubscriptionToTxnResponse {
  CommandAddSubscriptionToTxnResponse::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandAddSubscriptionToTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandAddSubscriptionToTxnResponse raise {
  let msg : CommandAddSubscriptionToTxnResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandAddSubscriptionToTxnResponse with fn write(
  self : CommandAddSubscriptionToTxnResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandAddSubscriptionToTxnResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandAddSubscriptionToTxnResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandAddSubscriptionToTxnResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandAddSubscriptionToTxnResponse"),
    )
  }
  let message : CommandAddSubscriptionToTxnResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandAddSubscriptionToTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandAddSubscriptionToTxnResponse raise {
  let msg : CommandAddSubscriptionToTxnResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandAddSubscriptionToTxnResponse with fn write(
  self : CommandAddSubscriptionToTxnResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandEndTxn {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut txn_action : TxnAction?
  mut scalable : Bool?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandEndTxn with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.txn_action is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.scalable is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandEndTxn with fn default() -> CommandEndTxn {
  CommandEndTxn::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    txn_action: None,
    scalable: Some(false),
  }
}

///|
#alias(new, deprecated)
pub fn CommandEndTxn::CommandEndTxn(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  txn_action? : TxnAction,
  scalable? : Bool,
) -> CommandEndTxn {
  CommandEndTxn::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    txn_action,
    scalable,
  }
}

///|
pub impl @lib.Read for CommandEndTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandEndTxn raise {
  let msg : CommandEndTxn = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.txn_action = reader
          |> @lib.read_enum()
          |> TxnAction::from_enum
          |> Some
      (5, _) => msg.scalable = reader |> @lib.read_bool() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandEndTxn with fn write(
  self : CommandEndTxn,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txn_action is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.scalable is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_bool(v)
  }
}

///|
pub impl ToJson for CommandEndTxn with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txn_action {
    Some(v) => json["txnAction"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable {
    Some(v) if v != false => json["scalable"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandEndTxn with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandEndTxn raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for CommandEndTxn"))
  }
  let message : CommandEndTxn = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("txnAction", value) =>
        message.txn_action = Some(@json.from_json(value, path~))
      ("scalable", value) =>
        message.scalable = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandEndTxn with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandEndTxn raise {
  let msg : CommandEndTxn = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.txn_action = reader
          |> @lib.async_read_enum()
          |> TxnAction::from_enum
          |> Some
      (5, _) => msg.scalable = reader |> @lib.async_read_bool() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandEndTxn with fn write(
  self : CommandEndTxn,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txn_action is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.scalable is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_bool(v)
  }
}

///|
pub(all) struct CommandEndTxnResponse {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandEndTxnResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandEndTxnResponse with fn default() -> CommandEndTxnResponse {
  CommandEndTxnResponse::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandEndTxnResponse::CommandEndTxnResponse(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandEndTxnResponse {
  CommandEndTxnResponse::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandEndTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandEndTxnResponse raise {
  let msg : CommandEndTxnResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandEndTxnResponse with fn write(
  self : CommandEndTxnResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandEndTxnResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandEndTxnResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandEndTxnResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandEndTxnResponse"),
    )
  }
  let message : CommandEndTxnResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandEndTxnResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandEndTxnResponse raise {
  let msg : CommandEndTxnResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandEndTxnResponse with fn write(
  self : CommandEndTxnResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandEndTxnOnPartition {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut topic : String?
  mut txn_action : TxnAction?
  mut txnid_least_bits_of_low_watermark : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandEndTxnOnPartition with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.topic is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.txn_action is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.txnid_least_bits_of_low_watermark is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandEndTxnOnPartition with fn default() -> CommandEndTxnOnPartition {
  CommandEndTxnOnPartition::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    topic: None,
    txn_action: None,
    txnid_least_bits_of_low_watermark: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandEndTxnOnPartition::CommandEndTxnOnPartition(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  topic? : String,
  txn_action? : TxnAction,
  txnid_least_bits_of_low_watermark? : UInt64,
) -> CommandEndTxnOnPartition {
  CommandEndTxnOnPartition::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    topic,
    txn_action,
    txnid_least_bits_of_low_watermark,
  }
}

///|
pub impl @lib.Read for CommandEndTxnOnPartition with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandEndTxnOnPartition raise {
  let msg : CommandEndTxnOnPartition = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) => msg.topic = reader |> @lib.read_string() |> Some
      (5, _) =>
        msg.txn_action = reader
          |> @lib.read_enum()
          |> TxnAction::from_enum
          |> Some
      (6, _) =>
        msg.txnid_least_bits_of_low_watermark = reader
          |> @lib.read_uint64()
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandEndTxnOnPartition with fn write(
  self : CommandEndTxnOnPartition,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.topic is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_string(v)
  }
  if self.txn_action is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.txnid_least_bits_of_low_watermark is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandEndTxnOnPartition with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.topic {
    Some(v) => json["topic"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txn_action {
    Some(v) => json["txnAction"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_least_bits_of_low_watermark {
    Some(v) => json["txnidLeastBitsOfLowWatermark"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandEndTxnOnPartition with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandEndTxnOnPartition raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandEndTxnOnPartition"),
    )
  }
  let message : CommandEndTxnOnPartition = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("topic", value) => message.topic = Some(@json.from_json(value, path~))
      ("txnAction", value) =>
        message.txn_action = Some(@json.from_json(value, path~))
      ("txnidLeastBitsOfLowWatermark", value) =>
        message.txnid_least_bits_of_low_watermark = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandEndTxnOnPartition with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandEndTxnOnPartition raise {
  let msg : CommandEndTxnOnPartition = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) => msg.topic = reader |> @lib.async_read_string() |> Some
      (5, _) =>
        msg.txn_action = reader
          |> @lib.async_read_enum()
          |> TxnAction::from_enum
          |> Some
      (6, _) =>
        msg.txnid_least_bits_of_low_watermark = reader
          |> @lib.async_read_uint64()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandEndTxnOnPartition with fn write(
  self : CommandEndTxnOnPartition,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.topic is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_string(v)
  }
  if self.txn_action is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.txnid_least_bits_of_low_watermark is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandEndTxnOnPartitionResponse {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandEndTxnOnPartitionResponse with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandEndTxnOnPartitionResponse with fn default() -> CommandEndTxnOnPartitionResponse {
  CommandEndTxnOnPartitionResponse::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandEndTxnOnPartitionResponse::CommandEndTxnOnPartitionResponse(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandEndTxnOnPartitionResponse {
  CommandEndTxnOnPartitionResponse::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandEndTxnOnPartitionResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandEndTxnOnPartitionResponse raise {
  let msg : CommandEndTxnOnPartitionResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandEndTxnOnPartitionResponse with fn write(
  self : CommandEndTxnOnPartitionResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandEndTxnOnPartitionResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandEndTxnOnPartitionResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandEndTxnOnPartitionResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandEndTxnOnPartitionResponse"),
    )
  }
  let message : CommandEndTxnOnPartitionResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandEndTxnOnPartitionResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandEndTxnOnPartitionResponse raise {
  let msg : CommandEndTxnOnPartitionResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandEndTxnOnPartitionResponse with fn write(
  self : CommandEndTxnOnPartitionResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) struct CommandEndTxnOnSubscription {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut subscription : Subscription?
  mut txn_action : TxnAction?
  mut txnid_least_bits_of_low_watermark : UInt64?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandEndTxnOnSubscription with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.subscription is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.txn_action is Some(v) {
    size += @lib.size_of_field(5U, @lib.size_of(v))
  }
  if self.txnid_least_bits_of_low_watermark is Some(v) {
    size += @lib.size_of_field(6U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandEndTxnOnSubscription with fn default() -> CommandEndTxnOnSubscription {
  CommandEndTxnOnSubscription::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    subscription: None,
    txn_action: None,
    txnid_least_bits_of_low_watermark: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandEndTxnOnSubscription::CommandEndTxnOnSubscription(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  subscription? : Subscription,
  txn_action? : TxnAction,
  txnid_least_bits_of_low_watermark? : UInt64,
) -> CommandEndTxnOnSubscription {
  CommandEndTxnOnSubscription::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    subscription,
    txn_action,
    txnid_least_bits_of_low_watermark,
  }
}

///|
pub impl @lib.Read for CommandEndTxnOnSubscription with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandEndTxnOnSubscription raise {
  let msg : CommandEndTxnOnSubscription = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscription = (reader |> @lib.read_message() : Subscription)
          |> Some
      (5, _) =>
        msg.txn_action = reader
          |> @lib.read_enum()
          |> TxnAction::from_enum
          |> Some
      (6, _) =>
        msg.txnid_least_bits_of_low_watermark = reader
          |> @lib.read_uint64()
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandEndTxnOnSubscription with fn write(
  self : CommandEndTxnOnSubscription,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.subscription is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(v)
  }
  if self.txn_action is Some(v) {
    writer |> @lib.write_varint(40UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.txnid_least_bits_of_low_watermark is Some(v) {
    writer |> @lib.write_varint(48UL)
    writer |> @lib.write_uint64(v)
  }
}

///|
pub impl ToJson for CommandEndTxnOnSubscription with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.subscription {
    Some(v) => json["subscription"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txn_action {
    Some(v) => json["txnAction"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_least_bits_of_low_watermark {
    Some(v) => json["txnidLeastBitsOfLowWatermark"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandEndTxnOnSubscription with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandEndTxnOnSubscription raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandEndTxnOnSubscription"),
    )
  }
  let message : CommandEndTxnOnSubscription = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("subscription", value) =>
        message.subscription = Some(@json.from_json(value, path~))
      ("txnAction", value) =>
        message.txn_action = Some(@json.from_json(value, path~))
      ("txnidLeastBitsOfLowWatermark", value) =>
        message.txnid_least_bits_of_low_watermark = Some(
          @json.from_json(value, path~),
        )
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandEndTxnOnSubscription with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandEndTxnOnSubscription raise {
  let msg : CommandEndTxnOnSubscription = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscription = (reader |> @lib.async_read_message() : Subscription)
          |> Some
      (5, _) =>
        msg.txn_action = reader
          |> @lib.async_read_enum()
          |> TxnAction::from_enum
          |> Some
      (6, _) =>
        msg.txnid_least_bits_of_low_watermark = reader
          |> @lib.async_read_uint64()
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandEndTxnOnSubscription with fn write(
  self : CommandEndTxnOnSubscription,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.subscription is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(v)
  }
  if self.txn_action is Some(v) {
    writer |> @lib.async_write_varint(40UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.txnid_least_bits_of_low_watermark is Some(v) {
    writer |> @lib.async_write_varint(48UL)
    writer |> @lib.async_write_uint64(v)
  }
}

///|
pub(all) struct CommandEndTxnOnSubscriptionResponse {
  mut request_id : UInt64
  mut txnid_least_bits : UInt64?
  mut txnid_most_bits : UInt64?
  mut error : ServerError?
  mut message : String?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for CommandEndTxnOnSubscriptionResponse with fn size_of(
  self,
) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.request_id))
  if self.txnid_least_bits is Some(v) {
    size += @lib.size_of_field(2U, @lib.size_of(v))
  }
  if self.txnid_most_bits is Some(v) {
    size += @lib.size_of_field(3U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_field(4U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for CommandEndTxnOnSubscriptionResponse with fn default() -> CommandEndTxnOnSubscriptionResponse {
  CommandEndTxnOnSubscriptionResponse::{
    request_id: Default::default(),
    txnid_least_bits: Some(0),
    txnid_most_bits: Some(0),
    error: None,
    message: None,
  }
}

///|
#alias(new, deprecated)
pub fn CommandEndTxnOnSubscriptionResponse::CommandEndTxnOnSubscriptionResponse(
  request_id : UInt64,
  txnid_least_bits? : UInt64,
  txnid_most_bits? : UInt64,
  error? : ServerError,
  message? : String,
) -> CommandEndTxnOnSubscriptionResponse {
  CommandEndTxnOnSubscriptionResponse::{
    request_id,
    txnid_least_bits,
    txnid_most_bits,
    error,
    message,
  }
}

///|
pub impl @lib.Read for CommandEndTxnOnSubscriptionResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> CommandEndTxnOnSubscriptionResponse raise {
  let msg : CommandEndTxnOnSubscriptionResponse = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.read_uint64()
      (2, _) => msg.txnid_least_bits = reader |> @lib.read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.read_uint64() |> Some
      (4, _) =>
        msg.error = reader |> @lib.read_enum() |> ServerError::from_enum |> Some
      (5, _) => msg.message = reader |> @lib.read_string() |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for CommandEndTxnOnSubscriptionResponse with fn write(
  self : CommandEndTxnOnSubscriptionResponse,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.write_varint(16UL)
    writer |> @lib.write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.write_varint(24UL)
    writer |> @lib.write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(32UL)
    writer |> @lib.write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_string(v)
  }
}

///|
pub impl ToJson for CommandEndTxnOnSubscriptionResponse with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.request_id != Default::default() {
    json["requestId"] = ToJson::to_json(self.request_id)
  }
  match self.txnid_least_bits {
    Some(v) if v != 0 => json["txnidLeastBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.txnid_most_bits {
    Some(v) if v != 0 => json["txnidMostBits"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for CommandEndTxnOnSubscriptionResponse with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> CommandEndTxnOnSubscriptionResponse raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError(
      (path, "Expected an object for CommandEndTxnOnSubscriptionResponse"),
    )
  }
  let message : CommandEndTxnOnSubscriptionResponse = Default::default()
  for key, value in obj {
    match (key, value) {
      ("requestId", value) => message.request_id = @json.from_json(value, path~)
      ("txnidLeastBits", value) =>
        message.txnid_least_bits = Some(@json.from_json(value, path~))
      ("txnidMostBits", value) =>
        message.txnid_most_bits = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for CommandEndTxnOnSubscriptionResponse with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> CommandEndTxnOnSubscriptionResponse raise {
  let msg : CommandEndTxnOnSubscriptionResponse = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) => msg.request_id = reader |> @lib.async_read_uint64()
      (2, _) =>
        msg.txnid_least_bits = reader |> @lib.async_read_uint64() |> Some
      (3, _) => msg.txnid_most_bits = reader |> @lib.async_read_uint64() |> Some
      (4, _) =>
        msg.error = reader
          |> @lib.async_read_enum()
          |> ServerError::from_enum
          |> Some
      (5, _) => msg.message = reader |> @lib.async_read_string() |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for CommandEndTxnOnSubscriptionResponse with fn write(
  self : CommandEndTxnOnSubscriptionResponse,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_uint64(self.request_id)
  if self.txnid_least_bits is Some(v) {
    writer |> @lib.async_write_varint(16UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.txnid_most_bits is Some(v) {
    writer |> @lib.async_write_varint(24UL)
    writer |> @lib.async_write_uint64(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(32UL)
    writer |> @lib.async_write_enum(v.to_enum())
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_string(v)
  }
}

///|
pub(all) enum BaseCommand_Type {
  CONNECT
  CONNECTED
  SUBSCRIBE
  PRODUCER
  SEND
  SEND_RECEIPT
  SEND_ERROR
  MESSAGE
  ACK
  FLOW
  UNSUBSCRIBE
  SUCCESS
  ERROR
  CLOSE_PRODUCER
  CLOSE_CONSUMER
  PRODUCER_SUCCESS
  PING
  PONG
  REDELIVER_UNACKNOWLEDGED_MESSAGES
  PARTITIONED_METADATA
  PARTITIONED_METADATA_RESPONSE
  LOOKUP
  LOOKUP_RESPONSE
  CONSUMER_STATS
  CONSUMER_STATS_RESPONSE
  REACHED_END_OF_TOPIC
  SEEK
  GET_LAST_MESSAGE_ID
  GET_LAST_MESSAGE_ID_RESPONSE
  ACTIVE_CONSUMER_CHANGE
  GET_TOPICS_OF_NAMESPACE
  GET_TOPICS_OF_NAMESPACE_RESPONSE
  GET_SCHEMA
  GET_SCHEMA_RESPONSE
  AUTH_CHALLENGE
  AUTH_RESPONSE
  ACK_RESPONSE
  GET_OR_CREATE_SCHEMA
  GET_OR_CREATE_SCHEMA_RESPONSE
  NEW_TXN
  NEW_TXN_RESPONSE
  ADD_PARTITION_TO_TXN
  ADD_PARTITION_TO_TXN_RESPONSE
  ADD_SUBSCRIPTION_TO_TXN
  ADD_SUBSCRIPTION_TO_TXN_RESPONSE
  END_TXN
  END_TXN_RESPONSE
  END_TXN_ON_PARTITION
  END_TXN_ON_PARTITION_RESPONSE
  END_TXN_ON_SUBSCRIPTION
  END_TXN_ON_SUBSCRIPTION_RESPONSE
  TC_CLIENT_CONNECT_REQUEST
  TC_CLIENT_CONNECT_RESPONSE
  WATCH_TOPIC_LIST
  WATCH_TOPIC_LIST_SUCCESS
  WATCH_TOPIC_UPDATE
  WATCH_TOPIC_LIST_CLOSE
  TOPIC_MIGRATED
  SCALABLE_TOPIC_LOOKUP
  SCALABLE_TOPIC_UPDATE
  SCALABLE_TOPIC_CLOSE
  SCALABLE_TOPIC_SUBSCRIBE
  SCALABLE_TOPIC_SUBSCRIBE_RESPONSE
  SCALABLE_TOPIC_ASSIGNMENT_UPDATE
  WATCH_SCALABLE_TOPICS
  WATCH_SCALABLE_TOPICS_UPDATE
  WATCH_SCALABLE_TOPICS_CLOSE
  WATCH_TC_ASSIGNMENTS
  WATCH_TC_ASSIGNMENTS_UPDATE
  WATCH_TC_ASSIGNMENTS_CLOSE
  SCALABLE_TOPIC_UNSUBSCRIBE
} derive(Eq, Debug)

///|
pub fn BaseCommand_Type::to_enum(self : BaseCommand_Type) -> @lib.Enum {
  match self {
    BaseCommand_Type::CONNECT => 2
    BaseCommand_Type::CONNECTED => 3
    BaseCommand_Type::SUBSCRIBE => 4
    BaseCommand_Type::PRODUCER => 5
    BaseCommand_Type::SEND => 6
    BaseCommand_Type::SEND_RECEIPT => 7
    BaseCommand_Type::SEND_ERROR => 8
    BaseCommand_Type::MESSAGE => 9
    BaseCommand_Type::ACK => 10
    BaseCommand_Type::FLOW => 11
    BaseCommand_Type::UNSUBSCRIBE => 12
    BaseCommand_Type::SUCCESS => 13
    BaseCommand_Type::ERROR => 14
    BaseCommand_Type::CLOSE_PRODUCER => 15
    BaseCommand_Type::CLOSE_CONSUMER => 16
    BaseCommand_Type::PRODUCER_SUCCESS => 17
    BaseCommand_Type::PING => 18
    BaseCommand_Type::PONG => 19
    BaseCommand_Type::REDELIVER_UNACKNOWLEDGED_MESSAGES => 20
    BaseCommand_Type::PARTITIONED_METADATA => 21
    BaseCommand_Type::PARTITIONED_METADATA_RESPONSE => 22
    BaseCommand_Type::LOOKUP => 23
    BaseCommand_Type::LOOKUP_RESPONSE => 24
    BaseCommand_Type::CONSUMER_STATS => 25
    BaseCommand_Type::CONSUMER_STATS_RESPONSE => 26
    BaseCommand_Type::REACHED_END_OF_TOPIC => 27
    BaseCommand_Type::SEEK => 28
    BaseCommand_Type::GET_LAST_MESSAGE_ID => 29
    BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE => 30
    BaseCommand_Type::ACTIVE_CONSUMER_CHANGE => 31
    BaseCommand_Type::GET_TOPICS_OF_NAMESPACE => 32
    BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE => 33
    BaseCommand_Type::GET_SCHEMA => 34
    BaseCommand_Type::GET_SCHEMA_RESPONSE => 35
    BaseCommand_Type::AUTH_CHALLENGE => 36
    BaseCommand_Type::AUTH_RESPONSE => 37
    BaseCommand_Type::ACK_RESPONSE => 38
    BaseCommand_Type::GET_OR_CREATE_SCHEMA => 39
    BaseCommand_Type::GET_OR_CREATE_SCHEMA_RESPONSE => 40
    BaseCommand_Type::NEW_TXN => 50
    BaseCommand_Type::NEW_TXN_RESPONSE => 51
    BaseCommand_Type::ADD_PARTITION_TO_TXN => 52
    BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE => 53
    BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN => 54
    BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE => 55
    BaseCommand_Type::END_TXN => 56
    BaseCommand_Type::END_TXN_RESPONSE => 57
    BaseCommand_Type::END_TXN_ON_PARTITION => 58
    BaseCommand_Type::END_TXN_ON_PARTITION_RESPONSE => 59
    BaseCommand_Type::END_TXN_ON_SUBSCRIPTION => 60
    BaseCommand_Type::END_TXN_ON_SUBSCRIPTION_RESPONSE => 61
    BaseCommand_Type::TC_CLIENT_CONNECT_REQUEST => 62
    BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE => 63
    BaseCommand_Type::WATCH_TOPIC_LIST => 64
    BaseCommand_Type::WATCH_TOPIC_LIST_SUCCESS => 65
    BaseCommand_Type::WATCH_TOPIC_UPDATE => 66
    BaseCommand_Type::WATCH_TOPIC_LIST_CLOSE => 67
    BaseCommand_Type::TOPIC_MIGRATED => 68
    BaseCommand_Type::SCALABLE_TOPIC_LOOKUP => 70
    BaseCommand_Type::SCALABLE_TOPIC_UPDATE => 71
    BaseCommand_Type::SCALABLE_TOPIC_CLOSE => 72
    BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE => 73
    BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE => 74
    BaseCommand_Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE => 75
    BaseCommand_Type::WATCH_SCALABLE_TOPICS => 76
    BaseCommand_Type::WATCH_SCALABLE_TOPICS_UPDATE => 77
    BaseCommand_Type::WATCH_SCALABLE_TOPICS_CLOSE => 78
    BaseCommand_Type::WATCH_TC_ASSIGNMENTS => 79
    BaseCommand_Type::WATCH_TC_ASSIGNMENTS_UPDATE => 80
    BaseCommand_Type::WATCH_TC_ASSIGNMENTS_CLOSE => 81
    BaseCommand_Type::SCALABLE_TOPIC_UNSUBSCRIBE => 82
  }
}

///|
pub fn BaseCommand_Type::from_enum(i : @lib.Enum) -> BaseCommand_Type {
  match i.0 {
    2 => BaseCommand_Type::CONNECT
    3 => BaseCommand_Type::CONNECTED
    4 => BaseCommand_Type::SUBSCRIBE
    5 => BaseCommand_Type::PRODUCER
    6 => BaseCommand_Type::SEND
    7 => BaseCommand_Type::SEND_RECEIPT
    8 => BaseCommand_Type::SEND_ERROR
    9 => BaseCommand_Type::MESSAGE
    10 => BaseCommand_Type::ACK
    11 => BaseCommand_Type::FLOW
    12 => BaseCommand_Type::UNSUBSCRIBE
    13 => BaseCommand_Type::SUCCESS
    14 => BaseCommand_Type::ERROR
    15 => BaseCommand_Type::CLOSE_PRODUCER
    16 => BaseCommand_Type::CLOSE_CONSUMER
    17 => BaseCommand_Type::PRODUCER_SUCCESS
    18 => BaseCommand_Type::PING
    19 => BaseCommand_Type::PONG
    20 => BaseCommand_Type::REDELIVER_UNACKNOWLEDGED_MESSAGES
    21 => BaseCommand_Type::PARTITIONED_METADATA
    22 => BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
    23 => BaseCommand_Type::LOOKUP
    24 => BaseCommand_Type::LOOKUP_RESPONSE
    25 => BaseCommand_Type::CONSUMER_STATS
    26 => BaseCommand_Type::CONSUMER_STATS_RESPONSE
    27 => BaseCommand_Type::REACHED_END_OF_TOPIC
    28 => BaseCommand_Type::SEEK
    29 => BaseCommand_Type::GET_LAST_MESSAGE_ID
    30 => BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
    31 => BaseCommand_Type::ACTIVE_CONSUMER_CHANGE
    32 => BaseCommand_Type::GET_TOPICS_OF_NAMESPACE
    33 => BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
    34 => BaseCommand_Type::GET_SCHEMA
    35 => BaseCommand_Type::GET_SCHEMA_RESPONSE
    36 => BaseCommand_Type::AUTH_CHALLENGE
    37 => BaseCommand_Type::AUTH_RESPONSE
    38 => BaseCommand_Type::ACK_RESPONSE
    39 => BaseCommand_Type::GET_OR_CREATE_SCHEMA
    40 => BaseCommand_Type::GET_OR_CREATE_SCHEMA_RESPONSE
    50 => BaseCommand_Type::NEW_TXN
    51 => BaseCommand_Type::NEW_TXN_RESPONSE
    52 => BaseCommand_Type::ADD_PARTITION_TO_TXN
    53 => BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE
    54 => BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN
    55 => BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE
    56 => BaseCommand_Type::END_TXN
    57 => BaseCommand_Type::END_TXN_RESPONSE
    58 => BaseCommand_Type::END_TXN_ON_PARTITION
    59 => BaseCommand_Type::END_TXN_ON_PARTITION_RESPONSE
    60 => BaseCommand_Type::END_TXN_ON_SUBSCRIPTION
    61 => BaseCommand_Type::END_TXN_ON_SUBSCRIPTION_RESPONSE
    62 => BaseCommand_Type::TC_CLIENT_CONNECT_REQUEST
    63 => BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
    64 => BaseCommand_Type::WATCH_TOPIC_LIST
    65 => BaseCommand_Type::WATCH_TOPIC_LIST_SUCCESS
    66 => BaseCommand_Type::WATCH_TOPIC_UPDATE
    67 => BaseCommand_Type::WATCH_TOPIC_LIST_CLOSE
    68 => BaseCommand_Type::TOPIC_MIGRATED
    70 => BaseCommand_Type::SCALABLE_TOPIC_LOOKUP
    71 => BaseCommand_Type::SCALABLE_TOPIC_UPDATE
    72 => BaseCommand_Type::SCALABLE_TOPIC_CLOSE
    73 => BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE
    74 => BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE
    75 => BaseCommand_Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE
    76 => BaseCommand_Type::WATCH_SCALABLE_TOPICS
    77 => BaseCommand_Type::WATCH_SCALABLE_TOPICS_UPDATE
    78 => BaseCommand_Type::WATCH_SCALABLE_TOPICS_CLOSE
    79 => BaseCommand_Type::WATCH_TC_ASSIGNMENTS
    80 => BaseCommand_Type::WATCH_TC_ASSIGNMENTS_UPDATE
    81 => BaseCommand_Type::WATCH_TC_ASSIGNMENTS_CLOSE
    82 => BaseCommand_Type::SCALABLE_TOPIC_UNSUBSCRIBE
    _ => Default::default()
  }
}

///|
pub impl Default for BaseCommand_Type with fn default() -> BaseCommand_Type {
  BaseCommand_Type::CONNECT
}

///|
pub impl @lib.Sized for BaseCommand_Type with fn size_of(
  self : BaseCommand_Type,
) {
  @lib.Sized::size_of(self.to_enum())
}

///|
pub impl @json.FromJson for BaseCommand_Type with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> BaseCommand_Type raise {
  match json {
    String("CONNECT") => BaseCommand_Type::CONNECT
    String("CONNECTED") => BaseCommand_Type::CONNECTED
    String("SUBSCRIBE") => BaseCommand_Type::SUBSCRIBE
    String("PRODUCER") => BaseCommand_Type::PRODUCER
    String("SEND") => BaseCommand_Type::SEND
    String("SEND_RECEIPT") => BaseCommand_Type::SEND_RECEIPT
    String("SEND_ERROR") => BaseCommand_Type::SEND_ERROR
    String("MESSAGE") => BaseCommand_Type::MESSAGE
    String("ACK") => BaseCommand_Type::ACK
    String("FLOW") => BaseCommand_Type::FLOW
    String("UNSUBSCRIBE") => BaseCommand_Type::UNSUBSCRIBE
    String("SUCCESS") => BaseCommand_Type::SUCCESS
    String("ERROR") => BaseCommand_Type::ERROR
    String("CLOSE_PRODUCER") => BaseCommand_Type::CLOSE_PRODUCER
    String("CLOSE_CONSUMER") => BaseCommand_Type::CLOSE_CONSUMER
    String("PRODUCER_SUCCESS") => BaseCommand_Type::PRODUCER_SUCCESS
    String("PING") => BaseCommand_Type::PING
    String("PONG") => BaseCommand_Type::PONG
    String("REDELIVER_UNACKNOWLEDGED_MESSAGES") =>
      BaseCommand_Type::REDELIVER_UNACKNOWLEDGED_MESSAGES
    String("PARTITIONED_METADATA") => BaseCommand_Type::PARTITIONED_METADATA
    String("PARTITIONED_METADATA_RESPONSE") =>
      BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
    String("LOOKUP") => BaseCommand_Type::LOOKUP
    String("LOOKUP_RESPONSE") => BaseCommand_Type::LOOKUP_RESPONSE
    String("CONSUMER_STATS") => BaseCommand_Type::CONSUMER_STATS
    String("CONSUMER_STATS_RESPONSE") =>
      BaseCommand_Type::CONSUMER_STATS_RESPONSE
    String("REACHED_END_OF_TOPIC") => BaseCommand_Type::REACHED_END_OF_TOPIC
    String("SEEK") => BaseCommand_Type::SEEK
    String("GET_LAST_MESSAGE_ID") => BaseCommand_Type::GET_LAST_MESSAGE_ID
    String("GET_LAST_MESSAGE_ID_RESPONSE") =>
      BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
    String("ACTIVE_CONSUMER_CHANGE") => BaseCommand_Type::ACTIVE_CONSUMER_CHANGE
    String("GET_TOPICS_OF_NAMESPACE") =>
      BaseCommand_Type::GET_TOPICS_OF_NAMESPACE
    String("GET_TOPICS_OF_NAMESPACE_RESPONSE") =>
      BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
    String("GET_SCHEMA") => BaseCommand_Type::GET_SCHEMA
    String("GET_SCHEMA_RESPONSE") => BaseCommand_Type::GET_SCHEMA_RESPONSE
    String("AUTH_CHALLENGE") => BaseCommand_Type::AUTH_CHALLENGE
    String("AUTH_RESPONSE") => BaseCommand_Type::AUTH_RESPONSE
    String("ACK_RESPONSE") => BaseCommand_Type::ACK_RESPONSE
    String("GET_OR_CREATE_SCHEMA") => BaseCommand_Type::GET_OR_CREATE_SCHEMA
    String("GET_OR_CREATE_SCHEMA_RESPONSE") =>
      BaseCommand_Type::GET_OR_CREATE_SCHEMA_RESPONSE
    String("NEW_TXN") => BaseCommand_Type::NEW_TXN
    String("NEW_TXN_RESPONSE") => BaseCommand_Type::NEW_TXN_RESPONSE
    String("ADD_PARTITION_TO_TXN") => BaseCommand_Type::ADD_PARTITION_TO_TXN
    String("ADD_PARTITION_TO_TXN_RESPONSE") =>
      BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE
    String("ADD_SUBSCRIPTION_TO_TXN") =>
      BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN
    String("ADD_SUBSCRIPTION_TO_TXN_RESPONSE") =>
      BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE
    String("END_TXN") => BaseCommand_Type::END_TXN
    String("END_TXN_RESPONSE") => BaseCommand_Type::END_TXN_RESPONSE
    String("END_TXN_ON_PARTITION") => BaseCommand_Type::END_TXN_ON_PARTITION
    String("END_TXN_ON_PARTITION_RESPONSE") =>
      BaseCommand_Type::END_TXN_ON_PARTITION_RESPONSE
    String("END_TXN_ON_SUBSCRIPTION") =>
      BaseCommand_Type::END_TXN_ON_SUBSCRIPTION
    String("END_TXN_ON_SUBSCRIPTION_RESPONSE") =>
      BaseCommand_Type::END_TXN_ON_SUBSCRIPTION_RESPONSE
    String("TC_CLIENT_CONNECT_REQUEST") =>
      BaseCommand_Type::TC_CLIENT_CONNECT_REQUEST
    String("TC_CLIENT_CONNECT_RESPONSE") =>
      BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
    String("WATCH_TOPIC_LIST") => BaseCommand_Type::WATCH_TOPIC_LIST
    String("WATCH_TOPIC_LIST_SUCCESS") =>
      BaseCommand_Type::WATCH_TOPIC_LIST_SUCCESS
    String("WATCH_TOPIC_UPDATE") => BaseCommand_Type::WATCH_TOPIC_UPDATE
    String("WATCH_TOPIC_LIST_CLOSE") => BaseCommand_Type::WATCH_TOPIC_LIST_CLOSE
    String("TOPIC_MIGRATED") => BaseCommand_Type::TOPIC_MIGRATED
    String("SCALABLE_TOPIC_LOOKUP") => BaseCommand_Type::SCALABLE_TOPIC_LOOKUP
    String("SCALABLE_TOPIC_UPDATE") => BaseCommand_Type::SCALABLE_TOPIC_UPDATE
    String("SCALABLE_TOPIC_CLOSE") => BaseCommand_Type::SCALABLE_TOPIC_CLOSE
    String("SCALABLE_TOPIC_SUBSCRIBE") =>
      BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE
    String("SCALABLE_TOPIC_SUBSCRIBE_RESPONSE") =>
      BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE
    String("SCALABLE_TOPIC_ASSIGNMENT_UPDATE") =>
      BaseCommand_Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE
    String("WATCH_SCALABLE_TOPICS") => BaseCommand_Type::WATCH_SCALABLE_TOPICS
    String("WATCH_SCALABLE_TOPICS_UPDATE") =>
      BaseCommand_Type::WATCH_SCALABLE_TOPICS_UPDATE
    String("WATCH_SCALABLE_TOPICS_CLOSE") =>
      BaseCommand_Type::WATCH_SCALABLE_TOPICS_CLOSE
    String("WATCH_TC_ASSIGNMENTS") => BaseCommand_Type::WATCH_TC_ASSIGNMENTS
    String("WATCH_TC_ASSIGNMENTS_UPDATE") =>
      BaseCommand_Type::WATCH_TC_ASSIGNMENTS_UPDATE
    String("WATCH_TC_ASSIGNMENTS_CLOSE") =>
      BaseCommand_Type::WATCH_TC_ASSIGNMENTS_CLOSE
    String("SCALABLE_TOPIC_UNSUBSCRIBE") =>
      BaseCommand_Type::SCALABLE_TOPIC_UNSUBSCRIBE
    Number(2, ..) => BaseCommand_Type::CONNECT
    Number(3, ..) => BaseCommand_Type::CONNECTED
    Number(4, ..) => BaseCommand_Type::SUBSCRIBE
    Number(5, ..) => BaseCommand_Type::PRODUCER
    Number(6, ..) => BaseCommand_Type::SEND
    Number(7, ..) => BaseCommand_Type::SEND_RECEIPT
    Number(8, ..) => BaseCommand_Type::SEND_ERROR
    Number(9, ..) => BaseCommand_Type::MESSAGE
    Number(10, ..) => BaseCommand_Type::ACK
    Number(11, ..) => BaseCommand_Type::FLOW
    Number(12, ..) => BaseCommand_Type::UNSUBSCRIBE
    Number(13, ..) => BaseCommand_Type::SUCCESS
    Number(14, ..) => BaseCommand_Type::ERROR
    Number(15, ..) => BaseCommand_Type::CLOSE_PRODUCER
    Number(16, ..) => BaseCommand_Type::CLOSE_CONSUMER
    Number(17, ..) => BaseCommand_Type::PRODUCER_SUCCESS
    Number(18, ..) => BaseCommand_Type::PING
    Number(19, ..) => BaseCommand_Type::PONG
    Number(20, ..) => BaseCommand_Type::REDELIVER_UNACKNOWLEDGED_MESSAGES
    Number(21, ..) => BaseCommand_Type::PARTITIONED_METADATA
    Number(22, ..) => BaseCommand_Type::PARTITIONED_METADATA_RESPONSE
    Number(23, ..) => BaseCommand_Type::LOOKUP
    Number(24, ..) => BaseCommand_Type::LOOKUP_RESPONSE
    Number(25, ..) => BaseCommand_Type::CONSUMER_STATS
    Number(26, ..) => BaseCommand_Type::CONSUMER_STATS_RESPONSE
    Number(27, ..) => BaseCommand_Type::REACHED_END_OF_TOPIC
    Number(28, ..) => BaseCommand_Type::SEEK
    Number(29, ..) => BaseCommand_Type::GET_LAST_MESSAGE_ID
    Number(30, ..) => BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE
    Number(31, ..) => BaseCommand_Type::ACTIVE_CONSUMER_CHANGE
    Number(32, ..) => BaseCommand_Type::GET_TOPICS_OF_NAMESPACE
    Number(33, ..) => BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE
    Number(34, ..) => BaseCommand_Type::GET_SCHEMA
    Number(35, ..) => BaseCommand_Type::GET_SCHEMA_RESPONSE
    Number(36, ..) => BaseCommand_Type::AUTH_CHALLENGE
    Number(37, ..) => BaseCommand_Type::AUTH_RESPONSE
    Number(38, ..) => BaseCommand_Type::ACK_RESPONSE
    Number(39, ..) => BaseCommand_Type::GET_OR_CREATE_SCHEMA
    Number(40, ..) => BaseCommand_Type::GET_OR_CREATE_SCHEMA_RESPONSE
    Number(50, ..) => BaseCommand_Type::NEW_TXN
    Number(51, ..) => BaseCommand_Type::NEW_TXN_RESPONSE
    Number(52, ..) => BaseCommand_Type::ADD_PARTITION_TO_TXN
    Number(53, ..) => BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE
    Number(54, ..) => BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN
    Number(55, ..) => BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE
    Number(56, ..) => BaseCommand_Type::END_TXN
    Number(57, ..) => BaseCommand_Type::END_TXN_RESPONSE
    Number(58, ..) => BaseCommand_Type::END_TXN_ON_PARTITION
    Number(59, ..) => BaseCommand_Type::END_TXN_ON_PARTITION_RESPONSE
    Number(60, ..) => BaseCommand_Type::END_TXN_ON_SUBSCRIPTION
    Number(61, ..) => BaseCommand_Type::END_TXN_ON_SUBSCRIPTION_RESPONSE
    Number(62, ..) => BaseCommand_Type::TC_CLIENT_CONNECT_REQUEST
    Number(63, ..) => BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE
    Number(64, ..) => BaseCommand_Type::WATCH_TOPIC_LIST
    Number(65, ..) => BaseCommand_Type::WATCH_TOPIC_LIST_SUCCESS
    Number(66, ..) => BaseCommand_Type::WATCH_TOPIC_UPDATE
    Number(67, ..) => BaseCommand_Type::WATCH_TOPIC_LIST_CLOSE
    Number(68, ..) => BaseCommand_Type::TOPIC_MIGRATED
    Number(70, ..) => BaseCommand_Type::SCALABLE_TOPIC_LOOKUP
    Number(71, ..) => BaseCommand_Type::SCALABLE_TOPIC_UPDATE
    Number(72, ..) => BaseCommand_Type::SCALABLE_TOPIC_CLOSE
    Number(73, ..) => BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE
    Number(74, ..) => BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE
    Number(75, ..) => BaseCommand_Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE
    Number(76, ..) => BaseCommand_Type::WATCH_SCALABLE_TOPICS
    Number(77, ..) => BaseCommand_Type::WATCH_SCALABLE_TOPICS_UPDATE
    Number(78, ..) => BaseCommand_Type::WATCH_SCALABLE_TOPICS_CLOSE
    Number(79, ..) => BaseCommand_Type::WATCH_TC_ASSIGNMENTS
    Number(80, ..) => BaseCommand_Type::WATCH_TC_ASSIGNMENTS_UPDATE
    Number(81, ..) => BaseCommand_Type::WATCH_TC_ASSIGNMENTS_CLOSE
    Number(82, ..) => BaseCommand_Type::SCALABLE_TOPIC_UNSUBSCRIBE
    _ =>
      raise @json.JsonDecodeError(
        (path, "Expected a number or string for enum"),
      )
  }
}

///|
pub impl ToJson for BaseCommand_Type with fn to_json(self : BaseCommand_Type) -> Json {
  match self {
    BaseCommand_Type::CONNECT => "CONNECT"
    BaseCommand_Type::CONNECTED => "CONNECTED"
    BaseCommand_Type::SUBSCRIBE => "SUBSCRIBE"
    BaseCommand_Type::PRODUCER => "PRODUCER"
    BaseCommand_Type::SEND => "SEND"
    BaseCommand_Type::SEND_RECEIPT => "SEND_RECEIPT"
    BaseCommand_Type::SEND_ERROR => "SEND_ERROR"
    BaseCommand_Type::MESSAGE => "MESSAGE"
    BaseCommand_Type::ACK => "ACK"
    BaseCommand_Type::FLOW => "FLOW"
    BaseCommand_Type::UNSUBSCRIBE => "UNSUBSCRIBE"
    BaseCommand_Type::SUCCESS => "SUCCESS"
    BaseCommand_Type::ERROR => "ERROR"
    BaseCommand_Type::CLOSE_PRODUCER => "CLOSE_PRODUCER"
    BaseCommand_Type::CLOSE_CONSUMER => "CLOSE_CONSUMER"
    BaseCommand_Type::PRODUCER_SUCCESS => "PRODUCER_SUCCESS"
    BaseCommand_Type::PING => "PING"
    BaseCommand_Type::PONG => "PONG"
    BaseCommand_Type::REDELIVER_UNACKNOWLEDGED_MESSAGES =>
      "REDELIVER_UNACKNOWLEDGED_MESSAGES"
    BaseCommand_Type::PARTITIONED_METADATA => "PARTITIONED_METADATA"
    BaseCommand_Type::PARTITIONED_METADATA_RESPONSE =>
      "PARTITIONED_METADATA_RESPONSE"
    BaseCommand_Type::LOOKUP => "LOOKUP"
    BaseCommand_Type::LOOKUP_RESPONSE => "LOOKUP_RESPONSE"
    BaseCommand_Type::CONSUMER_STATS => "CONSUMER_STATS"
    BaseCommand_Type::CONSUMER_STATS_RESPONSE => "CONSUMER_STATS_RESPONSE"
    BaseCommand_Type::REACHED_END_OF_TOPIC => "REACHED_END_OF_TOPIC"
    BaseCommand_Type::SEEK => "SEEK"
    BaseCommand_Type::GET_LAST_MESSAGE_ID => "GET_LAST_MESSAGE_ID"
    BaseCommand_Type::GET_LAST_MESSAGE_ID_RESPONSE =>
      "GET_LAST_MESSAGE_ID_RESPONSE"
    BaseCommand_Type::ACTIVE_CONSUMER_CHANGE => "ACTIVE_CONSUMER_CHANGE"
    BaseCommand_Type::GET_TOPICS_OF_NAMESPACE => "GET_TOPICS_OF_NAMESPACE"
    BaseCommand_Type::GET_TOPICS_OF_NAMESPACE_RESPONSE =>
      "GET_TOPICS_OF_NAMESPACE_RESPONSE"
    BaseCommand_Type::GET_SCHEMA => "GET_SCHEMA"
    BaseCommand_Type::GET_SCHEMA_RESPONSE => "GET_SCHEMA_RESPONSE"
    BaseCommand_Type::AUTH_CHALLENGE => "AUTH_CHALLENGE"
    BaseCommand_Type::AUTH_RESPONSE => "AUTH_RESPONSE"
    BaseCommand_Type::ACK_RESPONSE => "ACK_RESPONSE"
    BaseCommand_Type::GET_OR_CREATE_SCHEMA => "GET_OR_CREATE_SCHEMA"
    BaseCommand_Type::GET_OR_CREATE_SCHEMA_RESPONSE =>
      "GET_OR_CREATE_SCHEMA_RESPONSE"
    BaseCommand_Type::NEW_TXN => "NEW_TXN"
    BaseCommand_Type::NEW_TXN_RESPONSE => "NEW_TXN_RESPONSE"
    BaseCommand_Type::ADD_PARTITION_TO_TXN => "ADD_PARTITION_TO_TXN"
    BaseCommand_Type::ADD_PARTITION_TO_TXN_RESPONSE =>
      "ADD_PARTITION_TO_TXN_RESPONSE"
    BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN => "ADD_SUBSCRIPTION_TO_TXN"
    BaseCommand_Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE =>
      "ADD_SUBSCRIPTION_TO_TXN_RESPONSE"
    BaseCommand_Type::END_TXN => "END_TXN"
    BaseCommand_Type::END_TXN_RESPONSE => "END_TXN_RESPONSE"
    BaseCommand_Type::END_TXN_ON_PARTITION => "END_TXN_ON_PARTITION"
    BaseCommand_Type::END_TXN_ON_PARTITION_RESPONSE =>
      "END_TXN_ON_PARTITION_RESPONSE"
    BaseCommand_Type::END_TXN_ON_SUBSCRIPTION => "END_TXN_ON_SUBSCRIPTION"
    BaseCommand_Type::END_TXN_ON_SUBSCRIPTION_RESPONSE =>
      "END_TXN_ON_SUBSCRIPTION_RESPONSE"
    BaseCommand_Type::TC_CLIENT_CONNECT_REQUEST => "TC_CLIENT_CONNECT_REQUEST"
    BaseCommand_Type::TC_CLIENT_CONNECT_RESPONSE => "TC_CLIENT_CONNECT_RESPONSE"
    BaseCommand_Type::WATCH_TOPIC_LIST => "WATCH_TOPIC_LIST"
    BaseCommand_Type::WATCH_TOPIC_LIST_SUCCESS => "WATCH_TOPIC_LIST_SUCCESS"
    BaseCommand_Type::WATCH_TOPIC_UPDATE => "WATCH_TOPIC_UPDATE"
    BaseCommand_Type::WATCH_TOPIC_LIST_CLOSE => "WATCH_TOPIC_LIST_CLOSE"
    BaseCommand_Type::TOPIC_MIGRATED => "TOPIC_MIGRATED"
    BaseCommand_Type::SCALABLE_TOPIC_LOOKUP => "SCALABLE_TOPIC_LOOKUP"
    BaseCommand_Type::SCALABLE_TOPIC_UPDATE => "SCALABLE_TOPIC_UPDATE"
    BaseCommand_Type::SCALABLE_TOPIC_CLOSE => "SCALABLE_TOPIC_CLOSE"
    BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE => "SCALABLE_TOPIC_SUBSCRIBE"
    BaseCommand_Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE =>
      "SCALABLE_TOPIC_SUBSCRIBE_RESPONSE"
    BaseCommand_Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE =>
      "SCALABLE_TOPIC_ASSIGNMENT_UPDATE"
    BaseCommand_Type::WATCH_SCALABLE_TOPICS => "WATCH_SCALABLE_TOPICS"
    BaseCommand_Type::WATCH_SCALABLE_TOPICS_UPDATE =>
      "WATCH_SCALABLE_TOPICS_UPDATE"
    BaseCommand_Type::WATCH_SCALABLE_TOPICS_CLOSE =>
      "WATCH_SCALABLE_TOPICS_CLOSE"
    BaseCommand_Type::WATCH_TC_ASSIGNMENTS => "WATCH_TC_ASSIGNMENTS"
    BaseCommand_Type::WATCH_TC_ASSIGNMENTS_UPDATE =>
      "WATCH_TC_ASSIGNMENTS_UPDATE"
    BaseCommand_Type::WATCH_TC_ASSIGNMENTS_CLOSE => "WATCH_TC_ASSIGNMENTS_CLOSE"
    BaseCommand_Type::SCALABLE_TOPIC_UNSUBSCRIBE => "SCALABLE_TOPIC_UNSUBSCRIBE"
  }
}

///|
pub(all) struct BaseCommand {
  mut type_ : BaseCommand_Type
  mut connect : CommandConnect?
  mut connected : CommandConnected?
  mut subscribe : CommandSubscribe?
  mut producer : CommandProducer?
  mut send : CommandSend?
  mut send_receipt : CommandSendReceipt?
  mut send_error : CommandSendError?
  mut message : CommandMessage?
  mut ack : CommandAck?
  mut flow : CommandFlow?
  mut unsubscribe : CommandUnsubscribe?
  mut success : CommandSuccess?
  mut error : CommandError?
  mut close_producer : CommandCloseProducer?
  mut close_consumer : CommandCloseConsumer?
  mut producer_success : CommandProducerSuccess?
  mut ping : CommandPing?
  mut pong : CommandPong?
  mut redeliver_unacknowledged_messages : CommandRedeliverUnacknowledgedMessages?
  mut partition_metadata : CommandPartitionedTopicMetadata?
  mut partition_metadata_response : CommandPartitionedTopicMetadataResponse?
  mut lookup_topic : CommandLookupTopic?
  mut lookup_topic_response : CommandLookupTopicResponse?
  mut consumer_stats : CommandConsumerStats?
  mut consumer_stats_response : CommandConsumerStatsResponse?
  mut reached_end_of_topic : CommandReachedEndOfTopic?
  mut seek : CommandSeek?
  mut get_last_message_id : CommandGetLastMessageId?
  mut get_last_message_id_response : CommandGetLastMessageIdResponse?
  mut active_consumer_change : CommandActiveConsumerChange?
  mut get_topics_of_namespace : CommandGetTopicsOfNamespace?
  mut get_topics_of_namespace_response : CommandGetTopicsOfNamespaceResponse?
  mut get_schema : CommandGetSchema?
  mut get_schema_response : CommandGetSchemaResponse?
  mut auth_challenge : CommandAuthChallenge?
  mut auth_response : CommandAuthResponse?
  mut ack_response : CommandAckResponse?
  mut get_or_create_schema : CommandGetOrCreateSchema?
  mut get_or_create_schema_response : CommandGetOrCreateSchemaResponse?
  mut new_txn : CommandNewTxn?
  mut new_txn_response : CommandNewTxnResponse?
  mut add_partition_to_txn : CommandAddPartitionToTxn?
  mut add_partition_to_txn_response : CommandAddPartitionToTxnResponse?
  mut add_subscription_to_txn : CommandAddSubscriptionToTxn?
  mut add_subscription_to_txn_response : CommandAddSubscriptionToTxnResponse?
  mut end_txn : CommandEndTxn?
  mut end_txn_response : CommandEndTxnResponse?
  mut end_txn_on_partition : CommandEndTxnOnPartition?
  mut end_txn_on_partition_response : CommandEndTxnOnPartitionResponse?
  mut end_txn_on_subscription : CommandEndTxnOnSubscription?
  mut end_txn_on_subscription_response : CommandEndTxnOnSubscriptionResponse?
  mut tc_client_connect_request : CommandTcClientConnectRequest?
  mut tc_client_connect_response : CommandTcClientConnectResponse?
  mut watch_topic_list : CommandWatchTopicList?
  mut watch_topic_list_success : CommandWatchTopicListSuccess?
  mut watch_topic_update : CommandWatchTopicUpdate?
  mut watch_topic_list_close : CommandWatchTopicListClose?
  mut topic_migrated : CommandTopicMigrated?
  mut scalable_topic_lookup : CommandScalableTopicLookup?
  mut scalable_topic_update : CommandScalableTopicUpdate?
  mut scalable_topic_close : CommandScalableTopicClose?
  mut scalable_topic_subscribe : CommandScalableTopicSubscribe?
  mut scalable_topic_subscribe_response : CommandScalableTopicSubscribeResponse?
  mut scalable_topic_assignment_update : CommandScalableTopicAssignmentUpdate?
  mut watch_scalable_topics : CommandWatchScalableTopics?
  mut watch_scalable_topics_update : CommandWatchScalableTopicsUpdate?
  mut watch_scalable_topics_close : CommandWatchScalableTopicsClose?
  mut watch_tc_assignments : CommandWatchTcAssignments?
  mut watch_tc_assignments_update : CommandWatchTcAssignmentsUpdate?
  mut watch_tc_assignments_close : CommandWatchTcAssignmentsClose?
  mut scalable_topic_unsubscribe : CommandScalableTopicUnsubscribe?
} derive(Eq, Debug)

///|
pub impl @lib.Sized for BaseCommand with fn size_of(self) {
  let mut size = 0U
  size += @lib.size_of_field(1U, @lib.size_of(self.type_))
  if self.connect is Some(v) {
    size += @lib.size_of_length_delimited_field(2U, @lib.size_of(v))
  }
  if self.connected is Some(v) {
    size += @lib.size_of_length_delimited_field(3U, @lib.size_of(v))
  }
  if self.subscribe is Some(v) {
    size += @lib.size_of_length_delimited_field(4U, @lib.size_of(v))
  }
  if self.producer is Some(v) {
    size += @lib.size_of_length_delimited_field(5U, @lib.size_of(v))
  }
  if self.send is Some(v) {
    size += @lib.size_of_length_delimited_field(6U, @lib.size_of(v))
  }
  if self.send_receipt is Some(v) {
    size += @lib.size_of_length_delimited_field(7U, @lib.size_of(v))
  }
  if self.send_error is Some(v) {
    size += @lib.size_of_length_delimited_field(8U, @lib.size_of(v))
  }
  if self.message is Some(v) {
    size += @lib.size_of_length_delimited_field(9U, @lib.size_of(v))
  }
  if self.ack is Some(v) {
    size += @lib.size_of_length_delimited_field(10U, @lib.size_of(v))
  }
  if self.flow is Some(v) {
    size += @lib.size_of_length_delimited_field(11U, @lib.size_of(v))
  }
  if self.unsubscribe is Some(v) {
    size += @lib.size_of_length_delimited_field(12U, @lib.size_of(v))
  }
  if self.success is Some(v) {
    size += @lib.size_of_length_delimited_field(13U, @lib.size_of(v))
  }
  if self.error is Some(v) {
    size += @lib.size_of_length_delimited_field(14U, @lib.size_of(v))
  }
  if self.close_producer is Some(v) {
    size += @lib.size_of_length_delimited_field(15U, @lib.size_of(v))
  }
  if self.close_consumer is Some(v) {
    size += @lib.size_of_length_delimited_field(16U, @lib.size_of(v))
  }
  if self.producer_success is Some(v) {
    size += @lib.size_of_length_delimited_field(17U, @lib.size_of(v))
  }
  if self.ping is Some(v) {
    size += @lib.size_of_length_delimited_field(18U, @lib.size_of(v))
  }
  if self.pong is Some(v) {
    size += @lib.size_of_length_delimited_field(19U, @lib.size_of(v))
  }
  if self.redeliver_unacknowledged_messages is Some(v) {
    size += @lib.size_of_length_delimited_field(20U, @lib.size_of(v))
  }
  if self.partition_metadata is Some(v) {
    size += @lib.size_of_length_delimited_field(21U, @lib.size_of(v))
  }
  if self.partition_metadata_response is Some(v) {
    size += @lib.size_of_length_delimited_field(22U, @lib.size_of(v))
  }
  if self.lookup_topic is Some(v) {
    size += @lib.size_of_length_delimited_field(23U, @lib.size_of(v))
  }
  if self.lookup_topic_response is Some(v) {
    size += @lib.size_of_length_delimited_field(24U, @lib.size_of(v))
  }
  if self.consumer_stats is Some(v) {
    size += @lib.size_of_length_delimited_field(25U, @lib.size_of(v))
  }
  if self.consumer_stats_response is Some(v) {
    size += @lib.size_of_length_delimited_field(26U, @lib.size_of(v))
  }
  if self.reached_end_of_topic is Some(v) {
    size += @lib.size_of_length_delimited_field(27U, @lib.size_of(v))
  }
  if self.seek is Some(v) {
    size += @lib.size_of_length_delimited_field(28U, @lib.size_of(v))
  }
  if self.get_last_message_id is Some(v) {
    size += @lib.size_of_length_delimited_field(29U, @lib.size_of(v))
  }
  if self.get_last_message_id_response is Some(v) {
    size += @lib.size_of_length_delimited_field(30U, @lib.size_of(v))
  }
  if self.active_consumer_change is Some(v) {
    size += @lib.size_of_length_delimited_field(31U, @lib.size_of(v))
  }
  if self.get_topics_of_namespace is Some(v) {
    size += @lib.size_of_length_delimited_field(32U, @lib.size_of(v))
  }
  if self.get_topics_of_namespace_response is Some(v) {
    size += @lib.size_of_length_delimited_field(33U, @lib.size_of(v))
  }
  if self.get_schema is Some(v) {
    size += @lib.size_of_length_delimited_field(34U, @lib.size_of(v))
  }
  if self.get_schema_response is Some(v) {
    size += @lib.size_of_length_delimited_field(35U, @lib.size_of(v))
  }
  if self.auth_challenge is Some(v) {
    size += @lib.size_of_length_delimited_field(36U, @lib.size_of(v))
  }
  if self.auth_response is Some(v) {
    size += @lib.size_of_length_delimited_field(37U, @lib.size_of(v))
  }
  if self.ack_response is Some(v) {
    size += @lib.size_of_length_delimited_field(38U, @lib.size_of(v))
  }
  if self.get_or_create_schema is Some(v) {
    size += @lib.size_of_length_delimited_field(39U, @lib.size_of(v))
  }
  if self.get_or_create_schema_response is Some(v) {
    size += @lib.size_of_length_delimited_field(40U, @lib.size_of(v))
  }
  if self.new_txn is Some(v) {
    size += @lib.size_of_length_delimited_field(50U, @lib.size_of(v))
  }
  if self.new_txn_response is Some(v) {
    size += @lib.size_of_length_delimited_field(51U, @lib.size_of(v))
  }
  if self.add_partition_to_txn is Some(v) {
    size += @lib.size_of_length_delimited_field(52U, @lib.size_of(v))
  }
  if self.add_partition_to_txn_response is Some(v) {
    size += @lib.size_of_length_delimited_field(53U, @lib.size_of(v))
  }
  if self.add_subscription_to_txn is Some(v) {
    size += @lib.size_of_length_delimited_field(54U, @lib.size_of(v))
  }
  if self.add_subscription_to_txn_response is Some(v) {
    size += @lib.size_of_length_delimited_field(55U, @lib.size_of(v))
  }
  if self.end_txn is Some(v) {
    size += @lib.size_of_length_delimited_field(56U, @lib.size_of(v))
  }
  if self.end_txn_response is Some(v) {
    size += @lib.size_of_length_delimited_field(57U, @lib.size_of(v))
  }
  if self.end_txn_on_partition is Some(v) {
    size += @lib.size_of_length_delimited_field(58U, @lib.size_of(v))
  }
  if self.end_txn_on_partition_response is Some(v) {
    size += @lib.size_of_length_delimited_field(59U, @lib.size_of(v))
  }
  if self.end_txn_on_subscription is Some(v) {
    size += @lib.size_of_length_delimited_field(60U, @lib.size_of(v))
  }
  if self.end_txn_on_subscription_response is Some(v) {
    size += @lib.size_of_length_delimited_field(61U, @lib.size_of(v))
  }
  if self.tc_client_connect_request is Some(v) {
    size += @lib.size_of_length_delimited_field(62U, @lib.size_of(v))
  }
  if self.tc_client_connect_response is Some(v) {
    size += @lib.size_of_length_delimited_field(63U, @lib.size_of(v))
  }
  if self.watch_topic_list is Some(v) {
    size += @lib.size_of_length_delimited_field(64U, @lib.size_of(v))
  }
  if self.watch_topic_list_success is Some(v) {
    size += @lib.size_of_length_delimited_field(65U, @lib.size_of(v))
  }
  if self.watch_topic_update is Some(v) {
    size += @lib.size_of_length_delimited_field(66U, @lib.size_of(v))
  }
  if self.watch_topic_list_close is Some(v) {
    size += @lib.size_of_length_delimited_field(67U, @lib.size_of(v))
  }
  if self.topic_migrated is Some(v) {
    size += @lib.size_of_length_delimited_field(68U, @lib.size_of(v))
  }
  if self.scalable_topic_lookup is Some(v) {
    size += @lib.size_of_length_delimited_field(70U, @lib.size_of(v))
  }
  if self.scalable_topic_update is Some(v) {
    size += @lib.size_of_length_delimited_field(71U, @lib.size_of(v))
  }
  if self.scalable_topic_close is Some(v) {
    size += @lib.size_of_length_delimited_field(72U, @lib.size_of(v))
  }
  if self.scalable_topic_subscribe is Some(v) {
    size += @lib.size_of_length_delimited_field(73U, @lib.size_of(v))
  }
  if self.scalable_topic_subscribe_response is Some(v) {
    size += @lib.size_of_length_delimited_field(74U, @lib.size_of(v))
  }
  if self.scalable_topic_assignment_update is Some(v) {
    size += @lib.size_of_length_delimited_field(75U, @lib.size_of(v))
  }
  if self.watch_scalable_topics is Some(v) {
    size += @lib.size_of_length_delimited_field(76U, @lib.size_of(v))
  }
  if self.watch_scalable_topics_update is Some(v) {
    size += @lib.size_of_length_delimited_field(77U, @lib.size_of(v))
  }
  if self.watch_scalable_topics_close is Some(v) {
    size += @lib.size_of_length_delimited_field(78U, @lib.size_of(v))
  }
  if self.watch_tc_assignments is Some(v) {
    size += @lib.size_of_length_delimited_field(79U, @lib.size_of(v))
  }
  if self.watch_tc_assignments_update is Some(v) {
    size += @lib.size_of_length_delimited_field(80U, @lib.size_of(v))
  }
  if self.watch_tc_assignments_close is Some(v) {
    size += @lib.size_of_length_delimited_field(81U, @lib.size_of(v))
  }
  if self.scalable_topic_unsubscribe is Some(v) {
    size += @lib.size_of_length_delimited_field(82U, @lib.size_of(v))
  }
  size
}

///|
pub impl Default for BaseCommand with fn default() -> BaseCommand {
  BaseCommand::{
    type_: Default::default(),
    connect: None,
    connected: None,
    subscribe: None,
    producer: None,
    send: None,
    send_receipt: None,
    send_error: None,
    message: None,
    ack: None,
    flow: None,
    unsubscribe: None,
    success: None,
    error: None,
    close_producer: None,
    close_consumer: None,
    producer_success: None,
    ping: None,
    pong: None,
    redeliver_unacknowledged_messages: None,
    partition_metadata: None,
    partition_metadata_response: None,
    lookup_topic: None,
    lookup_topic_response: None,
    consumer_stats: None,
    consumer_stats_response: None,
    reached_end_of_topic: None,
    seek: None,
    get_last_message_id: None,
    get_last_message_id_response: None,
    active_consumer_change: None,
    get_topics_of_namespace: None,
    get_topics_of_namespace_response: None,
    get_schema: None,
    get_schema_response: None,
    auth_challenge: None,
    auth_response: None,
    ack_response: None,
    get_or_create_schema: None,
    get_or_create_schema_response: None,
    new_txn: None,
    new_txn_response: None,
    add_partition_to_txn: None,
    add_partition_to_txn_response: None,
    add_subscription_to_txn: None,
    add_subscription_to_txn_response: None,
    end_txn: None,
    end_txn_response: None,
    end_txn_on_partition: None,
    end_txn_on_partition_response: None,
    end_txn_on_subscription: None,
    end_txn_on_subscription_response: None,
    tc_client_connect_request: None,
    tc_client_connect_response: None,
    watch_topic_list: None,
    watch_topic_list_success: None,
    watch_topic_update: None,
    watch_topic_list_close: None,
    topic_migrated: None,
    scalable_topic_lookup: None,
    scalable_topic_update: None,
    scalable_topic_close: None,
    scalable_topic_subscribe: None,
    scalable_topic_subscribe_response: None,
    scalable_topic_assignment_update: None,
    watch_scalable_topics: None,
    watch_scalable_topics_update: None,
    watch_scalable_topics_close: None,
    watch_tc_assignments: None,
    watch_tc_assignments_update: None,
    watch_tc_assignments_close: None,
    scalable_topic_unsubscribe: None,
  }
}

///|
#alias(new, deprecated)
pub fn BaseCommand::BaseCommand(
  type_ : BaseCommand_Type,
  connect? : CommandConnect,
  connected? : CommandConnected,
  subscribe? : CommandSubscribe,
  producer? : CommandProducer,
  send? : CommandSend,
  send_receipt? : CommandSendReceipt,
  send_error? : CommandSendError,
  message? : CommandMessage,
  ack? : CommandAck,
  flow? : CommandFlow,
  unsubscribe? : CommandUnsubscribe,
  success? : CommandSuccess,
  error? : CommandError,
  close_producer? : CommandCloseProducer,
  close_consumer? : CommandCloseConsumer,
  producer_success? : CommandProducerSuccess,
  ping? : CommandPing,
  pong? : CommandPong,
  redeliver_unacknowledged_messages? : CommandRedeliverUnacknowledgedMessages,
  partition_metadata? : CommandPartitionedTopicMetadata,
  partition_metadata_response? : CommandPartitionedTopicMetadataResponse,
  lookup_topic? : CommandLookupTopic,
  lookup_topic_response? : CommandLookupTopicResponse,
  consumer_stats? : CommandConsumerStats,
  consumer_stats_response? : CommandConsumerStatsResponse,
  reached_end_of_topic? : CommandReachedEndOfTopic,
  seek? : CommandSeek,
  get_last_message_id? : CommandGetLastMessageId,
  get_last_message_id_response? : CommandGetLastMessageIdResponse,
  active_consumer_change? : CommandActiveConsumerChange,
  get_topics_of_namespace? : CommandGetTopicsOfNamespace,
  get_topics_of_namespace_response? : CommandGetTopicsOfNamespaceResponse,
  get_schema? : CommandGetSchema,
  get_schema_response? : CommandGetSchemaResponse,
  auth_challenge? : CommandAuthChallenge,
  auth_response? : CommandAuthResponse,
  ack_response? : CommandAckResponse,
  get_or_create_schema? : CommandGetOrCreateSchema,
  get_or_create_schema_response? : CommandGetOrCreateSchemaResponse,
  new_txn? : CommandNewTxn,
  new_txn_response? : CommandNewTxnResponse,
  add_partition_to_txn? : CommandAddPartitionToTxn,
  add_partition_to_txn_response? : CommandAddPartitionToTxnResponse,
  add_subscription_to_txn? : CommandAddSubscriptionToTxn,
  add_subscription_to_txn_response? : CommandAddSubscriptionToTxnResponse,
  end_txn? : CommandEndTxn,
  end_txn_response? : CommandEndTxnResponse,
  end_txn_on_partition? : CommandEndTxnOnPartition,
  end_txn_on_partition_response? : CommandEndTxnOnPartitionResponse,
  end_txn_on_subscription? : CommandEndTxnOnSubscription,
  end_txn_on_subscription_response? : CommandEndTxnOnSubscriptionResponse,
  tc_client_connect_request? : CommandTcClientConnectRequest,
  tc_client_connect_response? : CommandTcClientConnectResponse,
  watch_topic_list? : CommandWatchTopicList,
  watch_topic_list_success? : CommandWatchTopicListSuccess,
  watch_topic_update? : CommandWatchTopicUpdate,
  watch_topic_list_close? : CommandWatchTopicListClose,
  topic_migrated? : CommandTopicMigrated,
  scalable_topic_lookup? : CommandScalableTopicLookup,
  scalable_topic_update? : CommandScalableTopicUpdate,
  scalable_topic_close? : CommandScalableTopicClose,
  scalable_topic_subscribe? : CommandScalableTopicSubscribe,
  scalable_topic_subscribe_response? : CommandScalableTopicSubscribeResponse,
  scalable_topic_assignment_update? : CommandScalableTopicAssignmentUpdate,
  watch_scalable_topics? : CommandWatchScalableTopics,
  watch_scalable_topics_update? : CommandWatchScalableTopicsUpdate,
  watch_scalable_topics_close? : CommandWatchScalableTopicsClose,
  watch_tc_assignments? : CommandWatchTcAssignments,
  watch_tc_assignments_update? : CommandWatchTcAssignmentsUpdate,
  watch_tc_assignments_close? : CommandWatchTcAssignmentsClose,
  scalable_topic_unsubscribe? : CommandScalableTopicUnsubscribe,
) -> BaseCommand {
  BaseCommand::{
    type_,
    connect,
    connected,
    subscribe,
    producer,
    send,
    send_receipt,
    send_error,
    message,
    ack,
    flow,
    unsubscribe,
    success,
    error,
    close_producer,
    close_consumer,
    producer_success,
    ping,
    pong,
    redeliver_unacknowledged_messages,
    partition_metadata,
    partition_metadata_response,
    lookup_topic,
    lookup_topic_response,
    consumer_stats,
    consumer_stats_response,
    reached_end_of_topic,
    seek,
    get_last_message_id,
    get_last_message_id_response,
    active_consumer_change,
    get_topics_of_namespace,
    get_topics_of_namespace_response,
    get_schema,
    get_schema_response,
    auth_challenge,
    auth_response,
    ack_response,
    get_or_create_schema,
    get_or_create_schema_response,
    new_txn,
    new_txn_response,
    add_partition_to_txn,
    add_partition_to_txn_response,
    add_subscription_to_txn,
    add_subscription_to_txn_response,
    end_txn,
    end_txn_response,
    end_txn_on_partition,
    end_txn_on_partition_response,
    end_txn_on_subscription,
    end_txn_on_subscription_response,
    tc_client_connect_request,
    tc_client_connect_response,
    watch_topic_list,
    watch_topic_list_success,
    watch_topic_update,
    watch_topic_list_close,
    topic_migrated,
    scalable_topic_lookup,
    scalable_topic_update,
    scalable_topic_close,
    scalable_topic_subscribe,
    scalable_topic_subscribe_response,
    scalable_topic_assignment_update,
    watch_scalable_topics,
    watch_scalable_topics_update,
    watch_scalable_topics_close,
    watch_tc_assignments,
    watch_tc_assignments_update,
    watch_tc_assignments_close,
    scalable_topic_unsubscribe,
  }
}

///|
pub impl @lib.Read for BaseCommand with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.Reader],
) -> BaseCommand raise {
  let msg : BaseCommand = Default::default()
  while (reader |> @lib.read_next_message_field()) is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.type_ = reader |> @lib.read_enum() |> BaseCommand_Type::from_enum
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.connect = (reader |> @lib.read_message() : CommandConnect) |> Some
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.connected = (reader |> @lib.read_message() : CommandConnected)
          |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscribe = (reader |> @lib.read_message() : CommandSubscribe)
          |> Some
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.producer = (reader |> @lib.read_message() : CommandProducer) |> Some
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.send = (reader |> @lib.read_message() : CommandSend) |> Some
      (7, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.send_receipt = (reader |> @lib.read_message() : CommandSendReceipt)
          |> Some
      (8, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.send_error = (reader |> @lib.read_message() : CommandSendError)
          |> Some
      (9, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message = (reader |> @lib.read_message() : CommandMessage) |> Some
      (10, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack = (reader |> @lib.read_message() : CommandAck) |> Some
      (11, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.flow = (reader |> @lib.read_message() : CommandFlow) |> Some
      (12, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.unsubscribe = (reader |> @lib.read_message() : CommandUnsubscribe)
          |> Some
      (13, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.success = (reader |> @lib.read_message() : CommandSuccess) |> Some
      (14, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.error = (reader |> @lib.read_message() : CommandError) |> Some
      (15, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.close_producer = (
            reader |> @lib.read_message() : CommandCloseProducer)
          |> Some
      (16, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.close_consumer = (
            reader |> @lib.read_message() : CommandCloseConsumer)
          |> Some
      (17, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.producer_success = (
            reader |> @lib.read_message() : CommandProducerSuccess)
          |> Some
      (18, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ping = (reader |> @lib.read_message() : CommandPing) |> Some
      (19, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.pong = (reader |> @lib.read_message() : CommandPong) |> Some
      (20, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.redeliver_unacknowledged_messages = (
            reader |> @lib.read_message() :
            CommandRedeliverUnacknowledgedMessages)
          |> Some
      (21, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.partition_metadata = (
            reader |> @lib.read_message() : CommandPartitionedTopicMetadata)
          |> Some
      (22, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.partition_metadata_response = (
            reader |> @lib.read_message() :
            CommandPartitionedTopicMetadataResponse)
          |> Some
      (23, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.lookup_topic = (reader |> @lib.read_message() : CommandLookupTopic)
          |> Some
      (24, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.lookup_topic_response = (
            reader |> @lib.read_message() : CommandLookupTopicResponse)
          |> Some
      (25, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.consumer_stats = (
            reader |> @lib.read_message() : CommandConsumerStats)
          |> Some
      (26, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.consumer_stats_response = (
            reader |> @lib.read_message() : CommandConsumerStatsResponse)
          |> Some
      (27, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.reached_end_of_topic = (
            reader |> @lib.read_message() : CommandReachedEndOfTopic)
          |> Some
      (28, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.seek = (reader |> @lib.read_message() : CommandSeek) |> Some
      (29, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_last_message_id = (
            reader |> @lib.read_message() : CommandGetLastMessageId)
          |> Some
      (30, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_last_message_id_response = (
            reader |> @lib.read_message() : CommandGetLastMessageIdResponse)
          |> Some
      (31, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.active_consumer_change = (
            reader |> @lib.read_message() : CommandActiveConsumerChange)
          |> Some
      (32, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_topics_of_namespace = (
            reader |> @lib.read_message() : CommandGetTopicsOfNamespace)
          |> Some
      (33, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_topics_of_namespace_response = (
            reader |> @lib.read_message() : CommandGetTopicsOfNamespaceResponse)
          |> Some
      (34, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_schema = (reader |> @lib.read_message() : CommandGetSchema)
          |> Some
      (35, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_schema_response = (
            reader |> @lib.read_message() : CommandGetSchemaResponse)
          |> Some
      (36, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.auth_challenge = (
            reader |> @lib.read_message() : CommandAuthChallenge)
          |> Some
      (37, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.auth_response = (reader |> @lib.read_message() : CommandAuthResponse)
          |> Some
      (38, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack_response = (reader |> @lib.read_message() : CommandAckResponse)
          |> Some
      (39, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_or_create_schema = (
            reader |> @lib.read_message() : CommandGetOrCreateSchema)
          |> Some
      (40, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_or_create_schema_response = (
            reader |> @lib.read_message() : CommandGetOrCreateSchemaResponse)
          |> Some
      (50, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.new_txn = (reader |> @lib.read_message() : CommandNewTxn) |> Some
      (51, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.new_txn_response = (
            reader |> @lib.read_message() : CommandNewTxnResponse)
          |> Some
      (52, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_partition_to_txn = (
            reader |> @lib.read_message() : CommandAddPartitionToTxn)
          |> Some
      (53, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_partition_to_txn_response = (
            reader |> @lib.read_message() : CommandAddPartitionToTxnResponse)
          |> Some
      (54, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_subscription_to_txn = (
            reader |> @lib.read_message() : CommandAddSubscriptionToTxn)
          |> Some
      (55, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_subscription_to_txn_response = (
            reader |> @lib.read_message() : CommandAddSubscriptionToTxnResponse)
          |> Some
      (56, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn = (reader |> @lib.read_message() : CommandEndTxn) |> Some
      (57, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_response = (
            reader |> @lib.read_message() : CommandEndTxnResponse)
          |> Some
      (58, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_partition = (
            reader |> @lib.read_message() : CommandEndTxnOnPartition)
          |> Some
      (59, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_partition_response = (
            reader |> @lib.read_message() : CommandEndTxnOnPartitionResponse)
          |> Some
      (60, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_subscription = (
            reader |> @lib.read_message() : CommandEndTxnOnSubscription)
          |> Some
      (61, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_subscription_response = (
            reader |> @lib.read_message() : CommandEndTxnOnSubscriptionResponse)
          |> Some
      (62, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.tc_client_connect_request = (
            reader |> @lib.read_message() : CommandTcClientConnectRequest)
          |> Some
      (63, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.tc_client_connect_response = (
            reader |> @lib.read_message() : CommandTcClientConnectResponse)
          |> Some
      (64, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_list = (
            reader |> @lib.read_message() : CommandWatchTopicList)
          |> Some
      (65, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_list_success = (
            reader |> @lib.read_message() : CommandWatchTopicListSuccess)
          |> Some
      (66, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_update = (
            reader |> @lib.read_message() : CommandWatchTopicUpdate)
          |> Some
      (67, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_list_close = (
            reader |> @lib.read_message() : CommandWatchTopicListClose)
          |> Some
      (68, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.topic_migrated = (
            reader |> @lib.read_message() : CommandTopicMigrated)
          |> Some
      (70, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_lookup = (
            reader |> @lib.read_message() : CommandScalableTopicLookup)
          |> Some
      (71, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_update = (
            reader |> @lib.read_message() : CommandScalableTopicUpdate)
          |> Some
      (72, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_close = (
            reader |> @lib.read_message() : CommandScalableTopicClose)
          |> Some
      (73, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_subscribe = (
            reader |> @lib.read_message() : CommandScalableTopicSubscribe)
          |> Some
      (74, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_subscribe_response = (
            reader |> @lib.read_message() :
            CommandScalableTopicSubscribeResponse)
          |> Some
      (75, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_assignment_update = (
            reader |> @lib.read_message() : CommandScalableTopicAssignmentUpdate)
          |> Some
      (76, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_scalable_topics = (
            reader |> @lib.read_message() : CommandWatchScalableTopics)
          |> Some
      (77, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_scalable_topics_update = (
            reader |> @lib.read_message() : CommandWatchScalableTopicsUpdate)
          |> Some
      (78, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_scalable_topics_close = (
            reader |> @lib.read_message() : CommandWatchScalableTopicsClose)
          |> Some
      (79, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_tc_assignments = (
            reader |> @lib.read_message() : CommandWatchTcAssignments)
          |> Some
      (80, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_tc_assignments_update = (
            reader |> @lib.read_message() : CommandWatchTcAssignmentsUpdate)
          |> Some
      (81, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_tc_assignments_close = (
            reader |> @lib.read_message() : CommandWatchTcAssignmentsClose)
          |> Some
      (82, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_unsubscribe = (
            reader |> @lib.read_message() : CommandScalableTopicUnsubscribe)
          |> Some
      _ => reader |> @lib.skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.Write for BaseCommand with fn write(
  self : BaseCommand,
  writer : &@lib.Writer,
) -> Unit raise {
  writer |> @lib.write_varint(8UL)
  writer |> @lib.write_enum(self.type_.to_enum())
  if self.connect is Some(v) {
    writer |> @lib.write_varint(18UL)
    writer |> @lib.write_message(v)
  }
  if self.connected is Some(v) {
    writer |> @lib.write_varint(26UL)
    writer |> @lib.write_message(v)
  }
  if self.subscribe is Some(v) {
    writer |> @lib.write_varint(34UL)
    writer |> @lib.write_message(v)
  }
  if self.producer is Some(v) {
    writer |> @lib.write_varint(42UL)
    writer |> @lib.write_message(v)
  }
  if self.send is Some(v) {
    writer |> @lib.write_varint(50UL)
    writer |> @lib.write_message(v)
  }
  if self.send_receipt is Some(v) {
    writer |> @lib.write_varint(58UL)
    writer |> @lib.write_message(v)
  }
  if self.send_error is Some(v) {
    writer |> @lib.write_varint(66UL)
    writer |> @lib.write_message(v)
  }
  if self.message is Some(v) {
    writer |> @lib.write_varint(74UL)
    writer |> @lib.write_message(v)
  }
  if self.ack is Some(v) {
    writer |> @lib.write_varint(82UL)
    writer |> @lib.write_message(v)
  }
  if self.flow is Some(v) {
    writer |> @lib.write_varint(90UL)
    writer |> @lib.write_message(v)
  }
  if self.unsubscribe is Some(v) {
    writer |> @lib.write_varint(98UL)
    writer |> @lib.write_message(v)
  }
  if self.success is Some(v) {
    writer |> @lib.write_varint(106UL)
    writer |> @lib.write_message(v)
  }
  if self.error is Some(v) {
    writer |> @lib.write_varint(114UL)
    writer |> @lib.write_message(v)
  }
  if self.close_producer is Some(v) {
    writer |> @lib.write_varint(122UL)
    writer |> @lib.write_message(v)
  }
  if self.close_consumer is Some(v) {
    writer |> @lib.write_varint(130UL)
    writer |> @lib.write_message(v)
  }
  if self.producer_success is Some(v) {
    writer |> @lib.write_varint(138UL)
    writer |> @lib.write_message(v)
  }
  if self.ping is Some(v) {
    writer |> @lib.write_varint(146UL)
    writer |> @lib.write_message(v)
  }
  if self.pong is Some(v) {
    writer |> @lib.write_varint(154UL)
    writer |> @lib.write_message(v)
  }
  if self.redeliver_unacknowledged_messages is Some(v) {
    writer |> @lib.write_varint(162UL)
    writer |> @lib.write_message(v)
  }
  if self.partition_metadata is Some(v) {
    writer |> @lib.write_varint(170UL)
    writer |> @lib.write_message(v)
  }
  if self.partition_metadata_response is Some(v) {
    writer |> @lib.write_varint(178UL)
    writer |> @lib.write_message(v)
  }
  if self.lookup_topic is Some(v) {
    writer |> @lib.write_varint(186UL)
    writer |> @lib.write_message(v)
  }
  if self.lookup_topic_response is Some(v) {
    writer |> @lib.write_varint(194UL)
    writer |> @lib.write_message(v)
  }
  if self.consumer_stats is Some(v) {
    writer |> @lib.write_varint(202UL)
    writer |> @lib.write_message(v)
  }
  if self.consumer_stats_response is Some(v) {
    writer |> @lib.write_varint(210UL)
    writer |> @lib.write_message(v)
  }
  if self.reached_end_of_topic is Some(v) {
    writer |> @lib.write_varint(218UL)
    writer |> @lib.write_message(v)
  }
  if self.seek is Some(v) {
    writer |> @lib.write_varint(226UL)
    writer |> @lib.write_message(v)
  }
  if self.get_last_message_id is Some(v) {
    writer |> @lib.write_varint(234UL)
    writer |> @lib.write_message(v)
  }
  if self.get_last_message_id_response is Some(v) {
    writer |> @lib.write_varint(242UL)
    writer |> @lib.write_message(v)
  }
  if self.active_consumer_change is Some(v) {
    writer |> @lib.write_varint(250UL)
    writer |> @lib.write_message(v)
  }
  if self.get_topics_of_namespace is Some(v) {
    writer |> @lib.write_varint(258UL)
    writer |> @lib.write_message(v)
  }
  if self.get_topics_of_namespace_response is Some(v) {
    writer |> @lib.write_varint(266UL)
    writer |> @lib.write_message(v)
  }
  if self.get_schema is Some(v) {
    writer |> @lib.write_varint(274UL)
    writer |> @lib.write_message(v)
  }
  if self.get_schema_response is Some(v) {
    writer |> @lib.write_varint(282UL)
    writer |> @lib.write_message(v)
  }
  if self.auth_challenge is Some(v) {
    writer |> @lib.write_varint(290UL)
    writer |> @lib.write_message(v)
  }
  if self.auth_response is Some(v) {
    writer |> @lib.write_varint(298UL)
    writer |> @lib.write_message(v)
  }
  if self.ack_response is Some(v) {
    writer |> @lib.write_varint(306UL)
    writer |> @lib.write_message(v)
  }
  if self.get_or_create_schema is Some(v) {
    writer |> @lib.write_varint(314UL)
    writer |> @lib.write_message(v)
  }
  if self.get_or_create_schema_response is Some(v) {
    writer |> @lib.write_varint(322UL)
    writer |> @lib.write_message(v)
  }
  if self.new_txn is Some(v) {
    writer |> @lib.write_varint(402UL)
    writer |> @lib.write_message(v)
  }
  if self.new_txn_response is Some(v) {
    writer |> @lib.write_varint(410UL)
    writer |> @lib.write_message(v)
  }
  if self.add_partition_to_txn is Some(v) {
    writer |> @lib.write_varint(418UL)
    writer |> @lib.write_message(v)
  }
  if self.add_partition_to_txn_response is Some(v) {
    writer |> @lib.write_varint(426UL)
    writer |> @lib.write_message(v)
  }
  if self.add_subscription_to_txn is Some(v) {
    writer |> @lib.write_varint(434UL)
    writer |> @lib.write_message(v)
  }
  if self.add_subscription_to_txn_response is Some(v) {
    writer |> @lib.write_varint(442UL)
    writer |> @lib.write_message(v)
  }
  if self.end_txn is Some(v) {
    writer |> @lib.write_varint(450UL)
    writer |> @lib.write_message(v)
  }
  if self.end_txn_response is Some(v) {
    writer |> @lib.write_varint(458UL)
    writer |> @lib.write_message(v)
  }
  if self.end_txn_on_partition is Some(v) {
    writer |> @lib.write_varint(466UL)
    writer |> @lib.write_message(v)
  }
  if self.end_txn_on_partition_response is Some(v) {
    writer |> @lib.write_varint(474UL)
    writer |> @lib.write_message(v)
  }
  if self.end_txn_on_subscription is Some(v) {
    writer |> @lib.write_varint(482UL)
    writer |> @lib.write_message(v)
  }
  if self.end_txn_on_subscription_response is Some(v) {
    writer |> @lib.write_varint(490UL)
    writer |> @lib.write_message(v)
  }
  if self.tc_client_connect_request is Some(v) {
    writer |> @lib.write_varint(498UL)
    writer |> @lib.write_message(v)
  }
  if self.tc_client_connect_response is Some(v) {
    writer |> @lib.write_varint(506UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_topic_list is Some(v) {
    writer |> @lib.write_varint(514UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_topic_list_success is Some(v) {
    writer |> @lib.write_varint(522UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_topic_update is Some(v) {
    writer |> @lib.write_varint(530UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_topic_list_close is Some(v) {
    writer |> @lib.write_varint(538UL)
    writer |> @lib.write_message(v)
  }
  if self.topic_migrated is Some(v) {
    writer |> @lib.write_varint(546UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_lookup is Some(v) {
    writer |> @lib.write_varint(562UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_update is Some(v) {
    writer |> @lib.write_varint(570UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_close is Some(v) {
    writer |> @lib.write_varint(578UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_subscribe is Some(v) {
    writer |> @lib.write_varint(586UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_subscribe_response is Some(v) {
    writer |> @lib.write_varint(594UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_assignment_update is Some(v) {
    writer |> @lib.write_varint(602UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_scalable_topics is Some(v) {
    writer |> @lib.write_varint(610UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_scalable_topics_update is Some(v) {
    writer |> @lib.write_varint(618UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_scalable_topics_close is Some(v) {
    writer |> @lib.write_varint(626UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_tc_assignments is Some(v) {
    writer |> @lib.write_varint(634UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_tc_assignments_update is Some(v) {
    writer |> @lib.write_varint(642UL)
    writer |> @lib.write_message(v)
  }
  if self.watch_tc_assignments_close is Some(v) {
    writer |> @lib.write_varint(650UL)
    writer |> @lib.write_message(v)
  }
  if self.scalable_topic_unsubscribe is Some(v) {
    writer |> @lib.write_varint(658UL)
    writer |> @lib.write_message(v)
  }
}

///|
pub impl ToJson for BaseCommand with fn to_json(self) {
  let json : Map[String, Json] = Map([])
  if self.type_ != Default::default() {
    json["type"] = ToJson::to_json(self.type_)
  }
  match self.connect {
    Some(v) => json["connect"] = ToJson::to_json(v)
    _ => ()
  }
  match self.connected {
    Some(v) => json["connected"] = ToJson::to_json(v)
    _ => ()
  }
  match self.subscribe {
    Some(v) => json["subscribe"] = ToJson::to_json(v)
    _ => ()
  }
  match self.producer {
    Some(v) => json["producer"] = ToJson::to_json(v)
    _ => ()
  }
  match self.send {
    Some(v) => json["send"] = ToJson::to_json(v)
    _ => ()
  }
  match self.send_receipt {
    Some(v) => json["sendReceipt"] = ToJson::to_json(v)
    _ => ()
  }
  match self.send_error {
    Some(v) => json["sendError"] = ToJson::to_json(v)
    _ => ()
  }
  match self.message {
    Some(v) => json["message"] = ToJson::to_json(v)
    _ => ()
  }
  match self.ack {
    Some(v) => json["ack"] = ToJson::to_json(v)
    _ => ()
  }
  match self.flow {
    Some(v) => json["flow"] = ToJson::to_json(v)
    _ => ()
  }
  match self.unsubscribe {
    Some(v) => json["unsubscribe"] = ToJson::to_json(v)
    _ => ()
  }
  match self.success {
    Some(v) => json["success"] = ToJson::to_json(v)
    _ => ()
  }
  match self.error {
    Some(v) => json["error"] = ToJson::to_json(v)
    _ => ()
  }
  match self.close_producer {
    Some(v) => json["closeProducer"] = ToJson::to_json(v)
    _ => ()
  }
  match self.close_consumer {
    Some(v) => json["closeConsumer"] = ToJson::to_json(v)
    _ => ()
  }
  match self.producer_success {
    Some(v) => json["producerSuccess"] = ToJson::to_json(v)
    _ => ()
  }
  match self.ping {
    Some(v) => json["ping"] = ToJson::to_json(v)
    _ => ()
  }
  match self.pong {
    Some(v) => json["pong"] = ToJson::to_json(v)
    _ => ()
  }
  match self.redeliver_unacknowledged_messages {
    Some(v) => json["redeliverUnacknowledgedMessages"] = ToJson::to_json(v)
    _ => ()
  }
  match self.partition_metadata {
    Some(v) => json["partitionMetadata"] = ToJson::to_json(v)
    _ => ()
  }
  match self.partition_metadata_response {
    Some(v) => json["partitionMetadataResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.lookup_topic {
    Some(v) => json["lookupTopic"] = ToJson::to_json(v)
    _ => ()
  }
  match self.lookup_topic_response {
    Some(v) => json["lookupTopicResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.consumer_stats {
    Some(v) => json["consumerStats"] = ToJson::to_json(v)
    _ => ()
  }
  match self.consumer_stats_response {
    Some(v) => json["consumerStatsResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.reached_end_of_topic {
    Some(v) => json["reachedEndOfTopic"] = ToJson::to_json(v)
    _ => ()
  }
  match self.seek {
    Some(v) => json["seek"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_last_message_id {
    Some(v) => json["getLastMessageId"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_last_message_id_response {
    Some(v) => json["getLastMessageIdResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.active_consumer_change {
    Some(v) => json["activeConsumerChange"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_topics_of_namespace {
    Some(v) => json["getTopicsOfNamespace"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_topics_of_namespace_response {
    Some(v) => json["getTopicsOfNamespaceResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_schema {
    Some(v) => json["getSchema"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_schema_response {
    Some(v) => json["getSchemaResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.auth_challenge {
    Some(v) => json["authChallenge"] = ToJson::to_json(v)
    _ => ()
  }
  match self.auth_response {
    Some(v) => json["authResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.ack_response {
    Some(v) => json["ackResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_or_create_schema {
    Some(v) => json["getOrCreateSchema"] = ToJson::to_json(v)
    _ => ()
  }
  match self.get_or_create_schema_response {
    Some(v) => json["getOrCreateSchemaResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.new_txn {
    Some(v) => json["newTxn"] = ToJson::to_json(v)
    _ => ()
  }
  match self.new_txn_response {
    Some(v) => json["newTxnResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.add_partition_to_txn {
    Some(v) => json["addPartitionToTxn"] = ToJson::to_json(v)
    _ => ()
  }
  match self.add_partition_to_txn_response {
    Some(v) => json["addPartitionToTxnResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.add_subscription_to_txn {
    Some(v) => json["addSubscriptionToTxn"] = ToJson::to_json(v)
    _ => ()
  }
  match self.add_subscription_to_txn_response {
    Some(v) => json["addSubscriptionToTxnResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.end_txn {
    Some(v) => json["endTxn"] = ToJson::to_json(v)
    _ => ()
  }
  match self.end_txn_response {
    Some(v) => json["endTxnResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.end_txn_on_partition {
    Some(v) => json["endTxnOnPartition"] = ToJson::to_json(v)
    _ => ()
  }
  match self.end_txn_on_partition_response {
    Some(v) => json["endTxnOnPartitionResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.end_txn_on_subscription {
    Some(v) => json["endTxnOnSubscription"] = ToJson::to_json(v)
    _ => ()
  }
  match self.end_txn_on_subscription_response {
    Some(v) => json["endTxnOnSubscriptionResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.tc_client_connect_request {
    Some(v) => json["tcClientConnectRequest"] = ToJson::to_json(v)
    _ => ()
  }
  match self.tc_client_connect_response {
    Some(v) => json["tcClientConnectResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_topic_list {
    Some(v) => json["watchTopicList"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_topic_list_success {
    Some(v) => json["watchTopicListSuccess"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_topic_update {
    Some(v) => json["watchTopicUpdate"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_topic_list_close {
    Some(v) => json["watchTopicListClose"] = ToJson::to_json(v)
    _ => ()
  }
  match self.topic_migrated {
    Some(v) => json["topicMigrated"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_lookup {
    Some(v) => json["scalableTopicLookup"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_update {
    Some(v) => json["scalableTopicUpdate"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_close {
    Some(v) => json["scalableTopicClose"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_subscribe {
    Some(v) => json["scalableTopicSubscribe"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_subscribe_response {
    Some(v) => json["scalableTopicSubscribeResponse"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_assignment_update {
    Some(v) => json["scalableTopicAssignmentUpdate"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_scalable_topics {
    Some(v) => json["watchScalableTopics"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_scalable_topics_update {
    Some(v) => json["watchScalableTopicsUpdate"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_scalable_topics_close {
    Some(v) => json["watchScalableTopicsClose"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_tc_assignments {
    Some(v) => json["watchTcAssignments"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_tc_assignments_update {
    Some(v) => json["watchTcAssignmentsUpdate"] = ToJson::to_json(v)
    _ => ()
  }
  match self.watch_tc_assignments_close {
    Some(v) => json["watchTcAssignmentsClose"] = ToJson::to_json(v)
    _ => ()
  }
  match self.scalable_topic_unsubscribe {
    Some(v) => json["scalableTopicUnsubscribe"] = ToJson::to_json(v)
    _ => ()
  }
  Json::object(json)
}

///|
pub impl @json.FromJson for BaseCommand with fn from_json(
  json : Json,
  path : @json.JsonPath,
) -> BaseCommand raise {
  guard json is Object(obj) else {
    raise @json.JsonDecodeError((path, "Expected an object for BaseCommand"))
  }
  let message : BaseCommand = Default::default()
  for key, value in obj {
    match (key, value) {
      ("type", value) => message.type_ = @json.from_json(value, path~)
      ("connect", value) =>
        message.connect = Some(@json.from_json(value, path~))
      ("connected", value) =>
        message.connected = Some(@json.from_json(value, path~))
      ("subscribe", value) =>
        message.subscribe = Some(@json.from_json(value, path~))
      ("producer", value) =>
        message.producer = Some(@json.from_json(value, path~))
      ("send", value) => message.send = Some(@json.from_json(value, path~))
      ("sendReceipt", value) =>
        message.send_receipt = Some(@json.from_json(value, path~))
      ("sendError", value) =>
        message.send_error = Some(@json.from_json(value, path~))
      ("message", value) =>
        message.message = Some(@json.from_json(value, path~))
      ("ack", value) => message.ack = Some(@json.from_json(value, path~))
      ("flow", value) => message.flow = Some(@json.from_json(value, path~))
      ("unsubscribe", value) =>
        message.unsubscribe = Some(@json.from_json(value, path~))
      ("success", value) =>
        message.success = Some(@json.from_json(value, path~))
      ("error", value) => message.error = Some(@json.from_json(value, path~))
      ("closeProducer", value) =>
        message.close_producer = Some(@json.from_json(value, path~))
      ("closeConsumer", value) =>
        message.close_consumer = Some(@json.from_json(value, path~))
      ("producerSuccess", value) =>
        message.producer_success = Some(@json.from_json(value, path~))
      ("ping", value) => message.ping = Some(@json.from_json(value, path~))
      ("pong", value) => message.pong = Some(@json.from_json(value, path~))
      ("redeliverUnacknowledgedMessages", value) =>
        message.redeliver_unacknowledged_messages = Some(
          @json.from_json(value, path~),
        )
      ("partitionMetadata", value) =>
        message.partition_metadata = Some(@json.from_json(value, path~))
      ("partitionMetadataResponse", value) =>
        message.partition_metadata_response = Some(
          @json.from_json(value, path~),
        )
      ("lookupTopic", value) =>
        message.lookup_topic = Some(@json.from_json(value, path~))
      ("lookupTopicResponse", value) =>
        message.lookup_topic_response = Some(@json.from_json(value, path~))
      ("consumerStats", value) =>
        message.consumer_stats = Some(@json.from_json(value, path~))
      ("consumerStatsResponse", value) =>
        message.consumer_stats_response = Some(@json.from_json(value, path~))
      ("reachedEndOfTopic", value) =>
        message.reached_end_of_topic = Some(@json.from_json(value, path~))
      ("seek", value) => message.seek = Some(@json.from_json(value, path~))
      ("getLastMessageId", value) =>
        message.get_last_message_id = Some(@json.from_json(value, path~))
      ("getLastMessageIdResponse", value) =>
        message.get_last_message_id_response = Some(
          @json.from_json(value, path~),
        )
      ("activeConsumerChange", value) =>
        message.active_consumer_change = Some(@json.from_json(value, path~))
      ("getTopicsOfNamespace", value) =>
        message.get_topics_of_namespace = Some(@json.from_json(value, path~))
      ("getTopicsOfNamespaceResponse", value) =>
        message.get_topics_of_namespace_response = Some(
          @json.from_json(value, path~),
        )
      ("getSchema", value) =>
        message.get_schema = Some(@json.from_json(value, path~))
      ("getSchemaResponse", value) =>
        message.get_schema_response = Some(@json.from_json(value, path~))
      ("authChallenge", value) =>
        message.auth_challenge = Some(@json.from_json(value, path~))
      ("authResponse", value) =>
        message.auth_response = Some(@json.from_json(value, path~))
      ("ackResponse", value) =>
        message.ack_response = Some(@json.from_json(value, path~))
      ("getOrCreateSchema", value) =>
        message.get_or_create_schema = Some(@json.from_json(value, path~))
      ("getOrCreateSchemaResponse", value) =>
        message.get_or_create_schema_response = Some(
          @json.from_json(value, path~),
        )
      ("newTxn", value) => message.new_txn = Some(@json.from_json(value, path~))
      ("newTxnResponse", value) =>
        message.new_txn_response = Some(@json.from_json(value, path~))
      ("addPartitionToTxn", value) =>
        message.add_partition_to_txn = Some(@json.from_json(value, path~))
      ("addPartitionToTxnResponse", value) =>
        message.add_partition_to_txn_response = Some(
          @json.from_json(value, path~),
        )
      ("addSubscriptionToTxn", value) =>
        message.add_subscription_to_txn = Some(@json.from_json(value, path~))
      ("addSubscriptionToTxnResponse", value) =>
        message.add_subscription_to_txn_response = Some(
          @json.from_json(value, path~),
        )
      ("endTxn", value) => message.end_txn = Some(@json.from_json(value, path~))
      ("endTxnResponse", value) =>
        message.end_txn_response = Some(@json.from_json(value, path~))
      ("endTxnOnPartition", value) =>
        message.end_txn_on_partition = Some(@json.from_json(value, path~))
      ("endTxnOnPartitionResponse", value) =>
        message.end_txn_on_partition_response = Some(
          @json.from_json(value, path~),
        )
      ("endTxnOnSubscription", value) =>
        message.end_txn_on_subscription = Some(@json.from_json(value, path~))
      ("endTxnOnSubscriptionResponse", value) =>
        message.end_txn_on_subscription_response = Some(
          @json.from_json(value, path~),
        )
      ("tcClientConnectRequest", value) =>
        message.tc_client_connect_request = Some(@json.from_json(value, path~))
      ("tcClientConnectResponse", value) =>
        message.tc_client_connect_response = Some(@json.from_json(value, path~))
      ("watchTopicList", value) =>
        message.watch_topic_list = Some(@json.from_json(value, path~))
      ("watchTopicListSuccess", value) =>
        message.watch_topic_list_success = Some(@json.from_json(value, path~))
      ("watchTopicUpdate", value) =>
        message.watch_topic_update = Some(@json.from_json(value, path~))
      ("watchTopicListClose", value) =>
        message.watch_topic_list_close = Some(@json.from_json(value, path~))
      ("topicMigrated", value) =>
        message.topic_migrated = Some(@json.from_json(value, path~))
      ("scalableTopicLookup", value) =>
        message.scalable_topic_lookup = Some(@json.from_json(value, path~))
      ("scalableTopicUpdate", value) =>
        message.scalable_topic_update = Some(@json.from_json(value, path~))
      ("scalableTopicClose", value) =>
        message.scalable_topic_close = Some(@json.from_json(value, path~))
      ("scalableTopicSubscribe", value) =>
        message.scalable_topic_subscribe = Some(@json.from_json(value, path~))
      ("scalableTopicSubscribeResponse", value) =>
        message.scalable_topic_subscribe_response = Some(
          @json.from_json(value, path~),
        )
      ("scalableTopicAssignmentUpdate", value) =>
        message.scalable_topic_assignment_update = Some(
          @json.from_json(value, path~),
        )
      ("watchScalableTopics", value) =>
        message.watch_scalable_topics = Some(@json.from_json(value, path~))
      ("watchScalableTopicsUpdate", value) =>
        message.watch_scalable_topics_update = Some(
          @json.from_json(value, path~),
        )
      ("watchScalableTopicsClose", value) =>
        message.watch_scalable_topics_close = Some(
          @json.from_json(value, path~),
        )
      ("watchTcAssignments", value) =>
        message.watch_tc_assignments = Some(@json.from_json(value, path~))
      ("watchTcAssignmentsUpdate", value) =>
        message.watch_tc_assignments_update = Some(
          @json.from_json(value, path~),
        )
      ("watchTcAssignmentsClose", value) =>
        message.watch_tc_assignments_close = Some(@json.from_json(value, path~))
      ("scalableTopicUnsubscribe", value) =>
        message.scalable_topic_unsubscribe = Some(@json.from_json(value, path~))
      (key, _) => raise @json.JsonDecodeError((path, "Unknown field \{key}"))
    }
  }
  message
}

///|
pub impl @lib.AsyncRead for BaseCommand with fn read_with_limit(
  reader : @lib.LimitedReader[&@lib.AsyncReader],
) -> BaseCommand raise {
  let msg : BaseCommand = Default::default()
  while (reader |> @lib.async_read_next_message_field())
        is Some((field_number, wire)) {
    match (field_number, wire) {
      (1, _) =>
        msg.type_ = reader
          |> @lib.async_read_enum()
          |> BaseCommand_Type::from_enum
      (2, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.connect = (reader |> @lib.async_read_message() : CommandConnect)
          |> Some
      (3, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.connected = (reader |> @lib.async_read_message() : CommandConnected)
          |> Some
      (4, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.subscribe = (reader |> @lib.async_read_message() : CommandSubscribe)
          |> Some
      (5, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.producer = (reader |> @lib.async_read_message() : CommandProducer)
          |> Some
      (6, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.send = (reader |> @lib.async_read_message() : CommandSend) |> Some
      (7, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.send_receipt = (
            reader |> @lib.async_read_message() : CommandSendReceipt)
          |> Some
      (8, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.send_error = (reader |> @lib.async_read_message() : CommandSendError)
          |> Some
      (9, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.message = (reader |> @lib.async_read_message() : CommandMessage)
          |> Some
      (10, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack = (reader |> @lib.async_read_message() : CommandAck) |> Some
      (11, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.flow = (reader |> @lib.async_read_message() : CommandFlow) |> Some
      (12, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.unsubscribe = (
            reader |> @lib.async_read_message() : CommandUnsubscribe)
          |> Some
      (13, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.success = (reader |> @lib.async_read_message() : CommandSuccess)
          |> Some
      (14, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.error = (reader |> @lib.async_read_message() : CommandError) |> Some
      (15, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.close_producer = (
            reader |> @lib.async_read_message() : CommandCloseProducer)
          |> Some
      (16, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.close_consumer = (
            reader |> @lib.async_read_message() : CommandCloseConsumer)
          |> Some
      (17, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.producer_success = (
            reader |> @lib.async_read_message() : CommandProducerSuccess)
          |> Some
      (18, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ping = (reader |> @lib.async_read_message() : CommandPing) |> Some
      (19, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.pong = (reader |> @lib.async_read_message() : CommandPong) |> Some
      (20, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.redeliver_unacknowledged_messages = (
            reader |> @lib.async_read_message() :
            CommandRedeliverUnacknowledgedMessages)
          |> Some
      (21, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.partition_metadata = (
            reader |> @lib.async_read_message() :
            CommandPartitionedTopicMetadata)
          |> Some
      (22, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.partition_metadata_response = (
            reader |> @lib.async_read_message() :
            CommandPartitionedTopicMetadataResponse)
          |> Some
      (23, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.lookup_topic = (
            reader |> @lib.async_read_message() : CommandLookupTopic)
          |> Some
      (24, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.lookup_topic_response = (
            reader |> @lib.async_read_message() : CommandLookupTopicResponse)
          |> Some
      (25, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.consumer_stats = (
            reader |> @lib.async_read_message() : CommandConsumerStats)
          |> Some
      (26, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.consumer_stats_response = (
            reader |> @lib.async_read_message() : CommandConsumerStatsResponse)
          |> Some
      (27, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.reached_end_of_topic = (
            reader |> @lib.async_read_message() : CommandReachedEndOfTopic)
          |> Some
      (28, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.seek = (reader |> @lib.async_read_message() : CommandSeek) |> Some
      (29, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_last_message_id = (
            reader |> @lib.async_read_message() : CommandGetLastMessageId)
          |> Some
      (30, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_last_message_id_response = (
            reader |> @lib.async_read_message() :
            CommandGetLastMessageIdResponse)
          |> Some
      (31, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.active_consumer_change = (
            reader |> @lib.async_read_message() : CommandActiveConsumerChange)
          |> Some
      (32, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_topics_of_namespace = (
            reader |> @lib.async_read_message() : CommandGetTopicsOfNamespace)
          |> Some
      (33, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_topics_of_namespace_response = (
            reader |> @lib.async_read_message() :
            CommandGetTopicsOfNamespaceResponse)
          |> Some
      (34, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_schema = (reader |> @lib.async_read_message() : CommandGetSchema)
          |> Some
      (35, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_schema_response = (
            reader |> @lib.async_read_message() : CommandGetSchemaResponse)
          |> Some
      (36, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.auth_challenge = (
            reader |> @lib.async_read_message() : CommandAuthChallenge)
          |> Some
      (37, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.auth_response = (
            reader |> @lib.async_read_message() : CommandAuthResponse)
          |> Some
      (38, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.ack_response = (
            reader |> @lib.async_read_message() : CommandAckResponse)
          |> Some
      (39, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_or_create_schema = (
            reader |> @lib.async_read_message() : CommandGetOrCreateSchema)
          |> Some
      (40, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.get_or_create_schema_response = (
            reader |> @lib.async_read_message() :
            CommandGetOrCreateSchemaResponse)
          |> Some
      (50, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.new_txn = (reader |> @lib.async_read_message() : CommandNewTxn)
          |> Some
      (51, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.new_txn_response = (
            reader |> @lib.async_read_message() : CommandNewTxnResponse)
          |> Some
      (52, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_partition_to_txn = (
            reader |> @lib.async_read_message() : CommandAddPartitionToTxn)
          |> Some
      (53, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_partition_to_txn_response = (
            reader |> @lib.async_read_message() :
            CommandAddPartitionToTxnResponse)
          |> Some
      (54, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_subscription_to_txn = (
            reader |> @lib.async_read_message() : CommandAddSubscriptionToTxn)
          |> Some
      (55, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.add_subscription_to_txn_response = (
            reader |> @lib.async_read_message() :
            CommandAddSubscriptionToTxnResponse)
          |> Some
      (56, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn = (reader |> @lib.async_read_message() : CommandEndTxn)
          |> Some
      (57, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_response = (
            reader |> @lib.async_read_message() : CommandEndTxnResponse)
          |> Some
      (58, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_partition = (
            reader |> @lib.async_read_message() : CommandEndTxnOnPartition)
          |> Some
      (59, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_partition_response = (
            reader |> @lib.async_read_message() :
            CommandEndTxnOnPartitionResponse)
          |> Some
      (60, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_subscription = (
            reader |> @lib.async_read_message() : CommandEndTxnOnSubscription)
          |> Some
      (61, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.end_txn_on_subscription_response = (
            reader |> @lib.async_read_message() :
            CommandEndTxnOnSubscriptionResponse)
          |> Some
      (62, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.tc_client_connect_request = (
            reader |> @lib.async_read_message() : CommandTcClientConnectRequest)
          |> Some
      (63, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.tc_client_connect_response = (
            reader |> @lib.async_read_message() : CommandTcClientConnectResponse)
          |> Some
      (64, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_list = (
            reader |> @lib.async_read_message() : CommandWatchTopicList)
          |> Some
      (65, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_list_success = (
            reader |> @lib.async_read_message() : CommandWatchTopicListSuccess)
          |> Some
      (66, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_update = (
            reader |> @lib.async_read_message() : CommandWatchTopicUpdate)
          |> Some
      (67, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_topic_list_close = (
            reader |> @lib.async_read_message() : CommandWatchTopicListClose)
          |> Some
      (68, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.topic_migrated = (
            reader |> @lib.async_read_message() : CommandTopicMigrated)
          |> Some
      (70, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_lookup = (
            reader |> @lib.async_read_message() : CommandScalableTopicLookup)
          |> Some
      (71, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_update = (
            reader |> @lib.async_read_message() : CommandScalableTopicUpdate)
          |> Some
      (72, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_close = (
            reader |> @lib.async_read_message() : CommandScalableTopicClose)
          |> Some
      (73, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_subscribe = (
            reader |> @lib.async_read_message() : CommandScalableTopicSubscribe)
          |> Some
      (74, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_subscribe_response = (
            reader |> @lib.async_read_message() :
            CommandScalableTopicSubscribeResponse)
          |> Some
      (75, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_assignment_update = (
            reader |> @lib.async_read_message() :
            CommandScalableTopicAssignmentUpdate)
          |> Some
      (76, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_scalable_topics = (
            reader |> @lib.async_read_message() : CommandWatchScalableTopics)
          |> Some
      (77, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_scalable_topics_update = (
            reader |> @lib.async_read_message() :
            CommandWatchScalableTopicsUpdate)
          |> Some
      (78, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_scalable_topics_close = (
            reader |> @lib.async_read_message() :
            CommandWatchScalableTopicsClose)
          |> Some
      (79, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_tc_assignments = (
            reader |> @lib.async_read_message() : CommandWatchTcAssignments)
          |> Some
      (80, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_tc_assignments_update = (
            reader |> @lib.async_read_message() :
            CommandWatchTcAssignmentsUpdate)
          |> Some
      (81, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.watch_tc_assignments_close = (
            reader |> @lib.async_read_message() : CommandWatchTcAssignmentsClose)
          |> Some
      (82, @lib.WIRE_TYPE_LENGTH_DELIMITED) =>
        msg.scalable_topic_unsubscribe = (
            reader |> @lib.async_read_message() :
            CommandScalableTopicUnsubscribe)
          |> Some
      _ => reader |> @lib.async_skip_message_field_by_number(field_number, wire)
    }
  }
  msg
}

///|
pub impl @lib.AsyncWrite for BaseCommand with fn write(
  self : BaseCommand,
  writer : &@lib.AsyncWriter,
) -> Unit raise {
  writer |> @lib.async_write_varint(8UL)
  writer |> @lib.async_write_enum(self.type_.to_enum())
  if self.connect is Some(v) {
    writer |> @lib.async_write_varint(18UL)
    writer |> @lib.async_write_message(v)
  }
  if self.connected is Some(v) {
    writer |> @lib.async_write_varint(26UL)
    writer |> @lib.async_write_message(v)
  }
  if self.subscribe is Some(v) {
    writer |> @lib.async_write_varint(34UL)
    writer |> @lib.async_write_message(v)
  }
  if self.producer is Some(v) {
    writer |> @lib.async_write_varint(42UL)
    writer |> @lib.async_write_message(v)
  }
  if self.send is Some(v) {
    writer |> @lib.async_write_varint(50UL)
    writer |> @lib.async_write_message(v)
  }
  if self.send_receipt is Some(v) {
    writer |> @lib.async_write_varint(58UL)
    writer |> @lib.async_write_message(v)
  }
  if self.send_error is Some(v) {
    writer |> @lib.async_write_varint(66UL)
    writer |> @lib.async_write_message(v)
  }
  if self.message is Some(v) {
    writer |> @lib.async_write_varint(74UL)
    writer |> @lib.async_write_message(v)
  }
  if self.ack is Some(v) {
    writer |> @lib.async_write_varint(82UL)
    writer |> @lib.async_write_message(v)
  }
  if self.flow is Some(v) {
    writer |> @lib.async_write_varint(90UL)
    writer |> @lib.async_write_message(v)
  }
  if self.unsubscribe is Some(v) {
    writer |> @lib.async_write_varint(98UL)
    writer |> @lib.async_write_message(v)
  }
  if self.success is Some(v) {
    writer |> @lib.async_write_varint(106UL)
    writer |> @lib.async_write_message(v)
  }
  if self.error is Some(v) {
    writer |> @lib.async_write_varint(114UL)
    writer |> @lib.async_write_message(v)
  }
  if self.close_producer is Some(v) {
    writer |> @lib.async_write_varint(122UL)
    writer |> @lib.async_write_message(v)
  }
  if self.close_consumer is Some(v) {
    writer |> @lib.async_write_varint(130UL)
    writer |> @lib.async_write_message(v)
  }
  if self.producer_success is Some(v) {
    writer |> @lib.async_write_varint(138UL)
    writer |> @lib.async_write_message(v)
  }
  if self.ping is Some(v) {
    writer |> @lib.async_write_varint(146UL)
    writer |> @lib.async_write_message(v)
  }
  if self.pong is Some(v) {
    writer |> @lib.async_write_varint(154UL)
    writer |> @lib.async_write_message(v)
  }
  if self.redeliver_unacknowledged_messages is Some(v) {
    writer |> @lib.async_write_varint(162UL)
    writer |> @lib.async_write_message(v)
  }
  if self.partition_metadata is Some(v) {
    writer |> @lib.async_write_varint(170UL)
    writer |> @lib.async_write_message(v)
  }
  if self.partition_metadata_response is Some(v) {
    writer |> @lib.async_write_varint(178UL)
    writer |> @lib.async_write_message(v)
  }
  if self.lookup_topic is Some(v) {
    writer |> @lib.async_write_varint(186UL)
    writer |> @lib.async_write_message(v)
  }
  if self.lookup_topic_response is Some(v) {
    writer |> @lib.async_write_varint(194UL)
    writer |> @lib.async_write_message(v)
  }
  if self.consumer_stats is Some(v) {
    writer |> @lib.async_write_varint(202UL)
    writer |> @lib.async_write_message(v)
  }
  if self.consumer_stats_response is Some(v) {
    writer |> @lib.async_write_varint(210UL)
    writer |> @lib.async_write_message(v)
  }
  if self.reached_end_of_topic is Some(v) {
    writer |> @lib.async_write_varint(218UL)
    writer |> @lib.async_write_message(v)
  }
  if self.seek is Some(v) {
    writer |> @lib.async_write_varint(226UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_last_message_id is Some(v) {
    writer |> @lib.async_write_varint(234UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_last_message_id_response is Some(v) {
    writer |> @lib.async_write_varint(242UL)
    writer |> @lib.async_write_message(v)
  }
  if self.active_consumer_change is Some(v) {
    writer |> @lib.async_write_varint(250UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_topics_of_namespace is Some(v) {
    writer |> @lib.async_write_varint(258UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_topics_of_namespace_response is Some(v) {
    writer |> @lib.async_write_varint(266UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_schema is Some(v) {
    writer |> @lib.async_write_varint(274UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_schema_response is Some(v) {
    writer |> @lib.async_write_varint(282UL)
    writer |> @lib.async_write_message(v)
  }
  if self.auth_challenge is Some(v) {
    writer |> @lib.async_write_varint(290UL)
    writer |> @lib.async_write_message(v)
  }
  if self.auth_response is Some(v) {
    writer |> @lib.async_write_varint(298UL)
    writer |> @lib.async_write_message(v)
  }
  if self.ack_response is Some(v) {
    writer |> @lib.async_write_varint(306UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_or_create_schema is Some(v) {
    writer |> @lib.async_write_varint(314UL)
    writer |> @lib.async_write_message(v)
  }
  if self.get_or_create_schema_response is Some(v) {
    writer |> @lib.async_write_varint(322UL)
    writer |> @lib.async_write_message(v)
  }
  if self.new_txn is Some(v) {
    writer |> @lib.async_write_varint(402UL)
    writer |> @lib.async_write_message(v)
  }
  if self.new_txn_response is Some(v) {
    writer |> @lib.async_write_varint(410UL)
    writer |> @lib.async_write_message(v)
  }
  if self.add_partition_to_txn is Some(v) {
    writer |> @lib.async_write_varint(418UL)
    writer |> @lib.async_write_message(v)
  }
  if self.add_partition_to_txn_response is Some(v) {
    writer |> @lib.async_write_varint(426UL)
    writer |> @lib.async_write_message(v)
  }
  if self.add_subscription_to_txn is Some(v) {
    writer |> @lib.async_write_varint(434UL)
    writer |> @lib.async_write_message(v)
  }
  if self.add_subscription_to_txn_response is Some(v) {
    writer |> @lib.async_write_varint(442UL)
    writer |> @lib.async_write_message(v)
  }
  if self.end_txn is Some(v) {
    writer |> @lib.async_write_varint(450UL)
    writer |> @lib.async_write_message(v)
  }
  if self.end_txn_response is Some(v) {
    writer |> @lib.async_write_varint(458UL)
    writer |> @lib.async_write_message(v)
  }
  if self.end_txn_on_partition is Some(v) {
    writer |> @lib.async_write_varint(466UL)
    writer |> @lib.async_write_message(v)
  }
  if self.end_txn_on_partition_response is Some(v) {
    writer |> @lib.async_write_varint(474UL)
    writer |> @lib.async_write_message(v)
  }
  if self.end_txn_on_subscription is Some(v) {
    writer |> @lib.async_write_varint(482UL)
    writer |> @lib.async_write_message(v)
  }
  if self.end_txn_on_subscription_response is Some(v) {
    writer |> @lib.async_write_varint(490UL)
    writer |> @lib.async_write_message(v)
  }
  if self.tc_client_connect_request is Some(v) {
    writer |> @lib.async_write_varint(498UL)
    writer |> @lib.async_write_message(v)
  }
  if self.tc_client_connect_response is Some(v) {
    writer |> @lib.async_write_varint(506UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_topic_list is Some(v) {
    writer |> @lib.async_write_varint(514UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_topic_list_success is Some(v) {
    writer |> @lib.async_write_varint(522UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_topic_update is Some(v) {
    writer |> @lib.async_write_varint(530UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_topic_list_close is Some(v) {
    writer |> @lib.async_write_varint(538UL)
    writer |> @lib.async_write_message(v)
  }
  if self.topic_migrated is Some(v) {
    writer |> @lib.async_write_varint(546UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_lookup is Some(v) {
    writer |> @lib.async_write_varint(562UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_update is Some(v) {
    writer |> @lib.async_write_varint(570UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_close is Some(v) {
    writer |> @lib.async_write_varint(578UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_subscribe is Some(v) {
    writer |> @lib.async_write_varint(586UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_subscribe_response is Some(v) {
    writer |> @lib.async_write_varint(594UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_assignment_update is Some(v) {
    writer |> @lib.async_write_varint(602UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_scalable_topics is Some(v) {
    writer |> @lib.async_write_varint(610UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_scalable_topics_update is Some(v) {
    writer |> @lib.async_write_varint(618UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_scalable_topics_close is Some(v) {
    writer |> @lib.async_write_varint(626UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_tc_assignments is Some(v) {
    writer |> @lib.async_write_varint(634UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_tc_assignments_update is Some(v) {
    writer |> @lib.async_write_varint(642UL)
    writer |> @lib.async_write_message(v)
  }
  if self.watch_tc_assignments_close is Some(v) {
    writer |> @lib.async_write_varint(650UL)
    writer |> @lib.async_write_message(v)
  }
  if self.scalable_topic_unsubscribe is Some(v) {
    writer |> @lib.async_write_varint(658UL)
    writer |> @lib.async_write_message(v)
  }
}
