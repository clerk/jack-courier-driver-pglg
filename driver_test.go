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
