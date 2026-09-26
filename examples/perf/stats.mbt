///|
// Fixed memory: 100us buckets up to 60 seconds, plus an overflow bucket.
struct Stats {
  buckets : FixedArray[Int64]
  mut count : Int64
  mut bytes : Int64
  mut max_us : Double
}

///|
fn Stats::new() -> Stats {
  { buckets: FixedArray::make(600002, 0L), count: 0L, bytes: 0L, max_us: 0.0, }
}

///|
fn Stats::record(self : Stats, bytes : Int, latency_us : Double) -> Unit {
  let index = if latency_us > 60000000.0 {
    600001
  } else {
    (latency_us / 100.0).ceil().to_int()
  }
  self.buckets[index] += 1L
  self.count += 1L
  self.bytes += bytes.to_int64()
  self.max_us = self.max_us.max(latency_us)
}

///|
// Nearest-rank percentile, returning the bucket upper bound in microseconds.
// Null denotes an empty sample or a percentile beyond the histogram range.
fn Stats::percentile(self : Stats, percentile : Double) -> Json {
  if self.count == 0L {
    return null
  }
  let rank = (self.count.to_double() * percentile).ceil().to_int64()
  let mut cumulative = 0L
  for i in 0..<600001 {
    cumulative += self.buckets[i]
    if cumulative >= rank {
      return (i * 100).to_json()
    }
  }
  null
}

///|
fn in_window(
  start_us : Double,
  end_us : Double,
  warmup_us : Double,
  stop_us : Double,
) -> Bool {
  start_us >= warmup_us && end_us < stop_us
}
