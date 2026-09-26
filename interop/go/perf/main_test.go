package main

import "testing"

func TestHistogram(t *testing.T) {
	s := new(stats)
	if s.percentile(.99) != nil {
		t.Fatal("empty histogram")
	}
	for _, v := range []float64{0, 1, 100, 101} {
		s.record(1024, v)
	}
	if s.percentile(.5) != 100 || s.percentile(.99) != 200 || s.bytes != 4096 {
		t.Fatal("nearest-rank/upper-bound mismatch")
	}
	s = new(stats)
	s.record(1, 60000000)
	s.record(1, 60000001)
	if s.percentile(.5) != 60000000 || s.percentile(.99) != nil || s.buckets[600001] != 1 {
		t.Fatal("overflow mismatch")
	}
}
