package main

import "testing"

func TestPercentile(t *testing.T) {
	s := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	if got := percentile(s, 50); got != 50 {
		t.Errorf("p50 = %d, want 50", got)
	}
	if got := percentile(s, 99); got != 100 {
		t.Errorf("p99 = %d, want 100", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("p50(nil) = %d, want 0", got)
	}
}

func TestMaxInt64(t *testing.T) {
	if got := maxInt64([]int64{3, 9, 1, 7}); got != 9 {
		t.Errorf("max = %d, want 9", got)
	}
	if got := maxInt64(nil); got != 0 {
		t.Errorf("max(nil) = %d, want 0", got)
	}
}

func TestHumanizeBytes(t *testing.T) {
	cases := map[int64]string{
		512:               "512B",
		1024:              "1.0KB",
		1536:              "1.5KB",
		1024 * 1024:       "1.0MB",
		412 * 1024 * 1024: "412.0MB",
	}
	for in, want := range cases {
		if got := humanizeBytes(in); got != want {
			t.Errorf("humanizeBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestBounded(t *testing.T) {
	if !bounded([]int64{100, 110, 90, 105, 100}) {
		t.Error("flat lag should be bounded")
	}
	if bounded([]int64{10, 100, 500, 2000, 10000}) {
		t.Error("growing lag should not be bounded")
	}
	if !bounded([]int64{42}) {
		t.Error("single sample should be bounded")
	}
}
