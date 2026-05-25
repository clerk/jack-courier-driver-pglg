package pglg

import (
	"testing"
	"time"
)

func TestConfigSetDefaults(t *testing.T) {
	cfg := Config{
		ConnString:      "postgres://localhost/test",
		SlotName:        "test_slot",
		PublicationName: "test_pub",
	}
	cfg.setDefaults()

	if cfg.Schema != "public" {
		t.Errorf("expected Schema=public, got %s", cfg.Schema)
	}
	if cfg.TablePrefix != "outbox" {
		t.Errorf("expected TablePrefix=outbox, got %s", cfg.TablePrefix)
	}
	if cfg.MaxBatchSize != 100 {
		t.Errorf("expected MaxBatchSize=100, got %d", cfg.MaxBatchSize)
	}
	if cfg.BatchTimeout != 1*time.Second {
		t.Errorf("expected BatchTimeout=1s, got %s", cfg.BatchTimeout)
	}
	if cfg.StandbyInterval != 10*time.Second {
		t.Errorf("expected StandbyInterval=10s, got %s", cfg.StandbyInterval)
	}
	if cfg.PartitionInterval != 1*time.Hour {
		t.Errorf("expected PartitionInterval=1h, got %s", cfg.PartitionInterval)
	}
	if cfg.PartitionLookAhead != 12*time.Hour {
		t.Errorf("expected PartitionLookAhead=12h, got %s", cfg.PartitionLookAhead)
	}
	if cfg.PartitionRetention != 3*time.Hour {
		t.Errorf("expected PartitionRetention=3h, got %s", cfg.PartitionRetention)
	}
	if cfg.PartitionMaintInterval != 5*time.Minute {
		t.Errorf("expected PartitionMaintInterval=5m, got %s", cfg.PartitionMaintInterval)
	}
	if cfg.ReconnectInitialDelay != 1*time.Second {
		t.Errorf("expected ReconnectInitialDelay=1s, got %s", cfg.ReconnectInitialDelay)
	}
	if cfg.ReconnectMaxDelay != 30*time.Second {
		t.Errorf("expected ReconnectMaxDelay=30s, got %s", cfg.ReconnectMaxDelay)
	}
	if cfg.Logger == nil {
		t.Error("expected non-nil Logger")
	}
	if cfg.Statsd == nil {
		t.Error("expected non-nil Statsd")
	}
}

func TestConfigSetDefaults_PreservesExisting(t *testing.T) {
	cfg := Config{
		ConnString:      "postgres://localhost/test",
		SlotName:        "s",
		PublicationName: "p",
		Schema:          "myschema",
		TablePrefix:     "myprefix",
		MaxBatchSize:    500,
		BatchTimeout:    5 * time.Second,
	}
	cfg.setDefaults()

	if cfg.Schema != "myschema" {
		t.Errorf("expected Schema=myschema, got %s", cfg.Schema)
	}
	if cfg.TablePrefix != "myprefix" {
		t.Errorf("expected TablePrefix=myprefix, got %s", cfg.TablePrefix)
	}
	if cfg.MaxBatchSize != 500 {
		t.Errorf("expected MaxBatchSize=500, got %d", cfg.MaxBatchSize)
	}
	if cfg.BatchTimeout != 5*time.Second {
		t.Errorf("expected BatchTimeout=5s, got %s", cfg.BatchTimeout)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid", Config{ConnString: "postgres://localhost/test", SlotName: "s", PublicationName: "p"}, false},
		{"missing ConnString", Config{SlotName: "s", PublicationName: "p"}, true},
		{"missing SlotName", Config{ConnString: "x", PublicationName: "p"}, true},
		{"missing PublicationName", Config{ConnString: "x", SlotName: "s"}, true},
		{
			"valid with replicas",
			Config{
				ConnString:      "x",
				SlotName:        "s",
				PublicationName: "p",
				Replicas: []ReplicaConfig{
					{Name: "r1", ConnString: "c1", SubscriptionName: "sub1"},
					{Name: "r2", ConnString: "c2", SubscriptionName: "sub2"},
				},
			},
			false,
		},
		{
			"replica missing Name",
			Config{
				ConnString:      "x",
				SlotName:        "s",
				PublicationName: "p",
				Replicas:        []ReplicaConfig{{ConnString: "c", SubscriptionName: "sub"}},
			},
			true,
		},
		{
			"replica missing ConnString",
			Config{
				ConnString:      "x",
				SlotName:        "s",
				PublicationName: "p",
				Replicas:        []ReplicaConfig{{Name: "r", SubscriptionName: "sub"}},
			},
			true,
		},
		{
			"replica missing SubscriptionName",
			Config{
				ConnString:      "x",
				SlotName:        "s",
				PublicationName: "p",
				Replicas:        []ReplicaConfig{{Name: "r", ConnString: "c"}},
			},
			true,
		},
		{
			"duplicate replica names",
			Config{
				ConnString:      "x",
				SlotName:        "s",
				PublicationName: "p",
				Replicas: []ReplicaConfig{
					{Name: "r1", ConnString: "c1", SubscriptionName: "sub1"},
					{Name: "r1", ConnString: "c2", SubscriptionName: "sub2"},
				},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigTableNames(t *testing.T) {
	cfg := Config{Schema: "myschema", TablePrefix: "billing"}
	cfg.setDefaults()

	if got := cfg.jobsTable(); got != "myschema.billing_jobs" {
		t.Errorf("jobsTable() = %s, want myschema.billing_jobs", got)
	}
	if got := cfg.cursorTable(); got != "myschema.billing_cursor" {
		t.Errorf("cursorTable() = %s, want myschema.billing_cursor", got)
	}
	if got := cfg.partitionMetaTable(); got != "myschema.billing_partition_meta" {
		t.Errorf("partitionMetaTable() = %s, want myschema.billing_partition_meta", got)
	}
}

func TestBackoff(t *testing.T) {
	bo := &backoff{
		initial:    100 * time.Millisecond,
		max:        1 * time.Second,
		multiplier: 2.0,
	}

	// First attempt: ~100ms (with jitter 75-100ms)
	d1 := bo.next()
	if d1 < 75*time.Millisecond || d1 > 100*time.Millisecond {
		t.Errorf("first delay %s outside expected range [75ms, 100ms]", d1)
	}

	// Second attempt: ~200ms
	d2 := bo.next()
	if d2 < 150*time.Millisecond || d2 > 200*time.Millisecond {
		t.Errorf("second delay %s outside expected range [150ms, 200ms]", d2)
	}

	// Third attempt: ~400ms
	d3 := bo.next()
	if d3 < 300*time.Millisecond || d3 > 400*time.Millisecond {
		t.Errorf("third delay %s outside expected range [300ms, 400ms]", d3)
	}

	// Eventually caps at max
	for i := 0; i < 20; i++ {
		bo.next()
	}
	d := bo.next()
	if d > 1*time.Second {
		t.Errorf("delay %s exceeds max 1s", d)
	}

	// Reset brings it back
	bo.reset()
	d = bo.next()
	if d < 75*time.Millisecond || d > 100*time.Millisecond {
		t.Errorf("after reset, delay %s outside expected range [75ms, 100ms]", d)
	}
}

func TestPartitionName(t *testing.T) {
	d := &Driver{cfg: Config{TablePrefix: "billing"}}
	lower := time.Date(2026, 2, 16, 14, 0, 0, 0, time.UTC)

	got := d.partitionName(lower)
	want := "billing_jobs_20260216_1400"
	if got != want {
		t.Errorf("partitionName() = %s, want %s", got, want)
	}
}

func TestParsePartitionLower(t *testing.T) {
	d := &Driver{cfg: Config{TablePrefix: "job_table_prefix"}}

	tests := []struct {
		name   string
		input  string
		want   time.Time
		wantOK bool
	}{
		{"valid", "job_table_prefix_jobs_20260216_1400", time.Date(2026, 2, 16, 14, 0, 0, 0, time.UTC), true},
		{"valid midnight", "job_table_prefix_jobs_20260101_0000", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), true},
		{"wrong prefix", "other_jobs_20260216_1400", time.Time{}, false},
		{"no jobs segment", "job_table_prefix_20260216_1400", time.Time{}, false},
		{"malformed timestamp", "job_table_prefix_jobs_2026-02-16", time.Time{}, false},
		{"extra suffix", "job_table_prefix_jobs_20260216_1400_extra", time.Time{}, false},
		{"empty", "", time.Time{}, false},
		{"only prefix", "job_table_prefix_jobs_", time.Time{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := d.parsePartitionLower(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("parsePartitionLower(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("parsePartitionLower(%q) = %v, want %v", tt.input, got, tt.want)
			}
			if got.Location() != time.UTC {
				t.Errorf("parsePartitionLower(%q) location = %v, want UTC", tt.input, got.Location())
			}
		})
	}
}

func TestSelectExpiredPartitions(t *testing.T) {
	d := &Driver{cfg: Config{TablePrefix: "job_table_prefix", PartitionInterval: time.Hour}}

	makeSet := func(names ...string) map[string]bool {
		m := make(map[string]bool, len(names))
		for _, n := range names {
			m[n] = true
		}
		return m
	}

	t.Run("empty input", func(t *testing.T) {
		got := d.selectExpiredPartitions(makeSet(), time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC))
		if len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("non-matching names skipped", func(t *testing.T) {
		got := d.selectExpiredPartitions(
			makeSet("unrelated_table", "other_jobs_20260101_0000", "job_table_prefix_jobs_bad"),
			time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		)
		if len(got) != 0 {
			t.Errorf("expected empty for non-matching names, got %v", got)
		}
	})

	t.Run("upper exactly at cutoff is included", func(t *testing.T) {
		got := d.selectExpiredPartitions(
			makeSet("job_table_prefix_jobs_20260525_0900"),
			time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC),
		)
		if len(got) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(got))
		}
		if got[0].name != "job_table_prefix_jobs_20260525_0900" {
			t.Errorf("unexpected name: %s", got[0].name)
		}
	})

	t.Run("upper just past cutoff is excluded", func(t *testing.T) {
		got := d.selectExpiredPartitions(
			makeSet("job_table_prefix_jobs_20260525_0900"),
			time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC).Add(-time.Nanosecond),
		)
		if len(got) != 0 {
			t.Errorf("expected exclusion at upper > cutoff, got %v", got)
		}
	})

	t.Run("output sorted oldest-first", func(t *testing.T) {
		got := d.selectExpiredPartitions(
			makeSet(
				"job_table_prefix_jobs_20260525_0900",
				"job_table_prefix_jobs_20260525_0700",
				"job_table_prefix_jobs_20260525_0800",
			),
			time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
		)
		if len(got) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(got))
		}
		want := []string{
			"job_table_prefix_jobs_20260525_0700",
			"job_table_prefix_jobs_20260525_0800",
			"job_table_prefix_jobs_20260525_0900",
		}
		for i, w := range want {
			if got[i].name != w {
				t.Errorf("entry %d: got %s, want %s", i, got[i].name, w)
			}
		}
	})

	t.Run("mix of matching and non-matching", func(t *testing.T) {
		got := d.selectExpiredPartitions(
			makeSet(
				"job_table_prefix_jobs_20260525_0900",
				"unrelated_table",
				"job_table_prefix_jobs_20260525_1100",
				"random_table_jobs_20260525_0900",
			),
			time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
		)
		if len(got) != 2 {
			t.Fatalf("expected 2 matching entries, got %d", len(got))
		}
	})
}

func TestSelectExpiredPartitions_IntervalAffectsUpper(t *testing.T) {
	for _, tc := range []struct {
		name        string
		interval    time.Duration
		partition   string
		cutoff      time.Time
		shouldDrop  bool
		expectUpper time.Time
	}{
		{
			name:        "1h interval, upper at cutoff",
			interval:    time.Hour,
			partition:   "job_table_prefix_jobs_20260525_0900",
			cutoff:      time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC),
			shouldDrop:  true,
			expectUpper: time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC),
		},
		{
			name:        "30m interval, upper at cutoff",
			interval:    30 * time.Minute,
			partition:   "job_table_prefix_jobs_20260525_0900",
			cutoff:      time.Date(2026, 5, 25, 9, 30, 0, 0, time.UTC),
			shouldDrop:  true,
			expectUpper: time.Date(2026, 5, 25, 9, 30, 0, 0, time.UTC),
		},
		{
			name:       "24h interval, upper after cutoff",
			interval:   24 * time.Hour,
			partition:  "job_table_prefix_jobs_20260525_0900",
			cutoff:     time.Date(2026, 5, 26, 8, 0, 0, 0, time.UTC),
			shouldDrop: false,
		},
		{
			name:        "24h interval, upper at cutoff",
			interval:    24 * time.Hour,
			partition:   "job_table_prefix_jobs_20260525_0900",
			cutoff:      time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC),
			shouldDrop:  true,
			expectUpper: time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &Driver{cfg: Config{TablePrefix: "job_table_prefix", PartitionInterval: tc.interval}}
			got := d.selectExpiredPartitions(map[string]bool{tc.partition: true}, tc.cutoff)
			if tc.shouldDrop {
				if len(got) != 1 {
					t.Fatalf("expected 1 entry, got %d", len(got))
				}
				if !got[0].upper.Equal(tc.expectUpper) {
					t.Errorf("upper = %v, want %v", got[0].upper, tc.expectUpper)
				}
			} else {
				if len(got) != 0 {
					t.Errorf("expected exclusion, got %v", got)
				}
			}
		})
	}
}

func TestParsePartitionLower_RoundTrip(t *testing.T) {
	d := &Driver{cfg: Config{TablePrefix: "job_table_prefix"}}

	for _, lower := range []time.Time{
		time.Date(2026, 2, 16, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		name := d.partitionName(lower)
		got, ok := d.parsePartitionLower(name)
		if !ok {
			t.Errorf("round-trip failed: parsePartitionLower(%q) ok = false", name)
			continue
		}
		if !got.Equal(lower) {
			t.Errorf("round-trip mismatch for %v: name = %q, parsed = %v", lower, name, got)
		}
	}
}
