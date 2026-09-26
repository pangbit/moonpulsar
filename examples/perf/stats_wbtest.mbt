///|
test "histogram nearest rank, upper bounds and empty sample" {
  let stats = Stats::new()
  assert_eq(stats.percentile(0.99), (null : Json))
  for value in [0.0, 1.0, 100.0, 101.0] {
    stats.record(1024, value)
  }
  assert_eq(stats.count, 4L)
  assert_eq(stats.bytes, 4096L)
  assert_eq(stats.percentile(0.5), (100 : Json))
  assert_eq(stats.percentile(0.95), (200 : Json))
  assert_eq(stats.max_us, 101.0)
}

///|
test "histogram overflow is explicit, not a capped false percentile" {
  let stats = Stats::new()
  stats.record(1, 60000000.0)
  stats.record(1, 60000001.0)
  assert_eq(stats.percentile(0.5), (60000000 : Json))
  assert_eq(stats.percentile(0.99), (null : Json))
  assert_eq(stats.buckets[600001], 1L)
}

///|
test "warmup and drain are excluded from send measurements" {
  assert_false(in_window(999.0, 1100.0, 1000.0, 2000.0))
  assert_true(in_window(1000.0, 1999.0, 1000.0, 2000.0))
  assert_false(in_window(1900.0, 2000.0, 1000.0, 2000.0))
}

///|
test "configuration bounds reject invalid durations and sizes" {
  assert_eq(bounded_int("warmup", "0", 0, 100), 0)
  let invalid : Result[Int, Error] = Ok(bounded_int("duration", "0", 1, 100)) catch {
    e => Err(e)
  }
  assert_true(invalid is Err(_))
  let oversized : Result[Int, Error] = Ok(bounded_int("size", "101", 1, 100)) catch {
    e => Err(e)
  }
  assert_true(oversized is Err(_))
}
