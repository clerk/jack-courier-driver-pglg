package pglg

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/stretchr/testify/assert"
)

type incrCall struct {
	name string
	tags []string
}

type gaugeCall struct {
	name  string
	value float64
	tags  []string
}

type recordingStatsd struct {
	statsd.NoOpClient
	mu     sync.Mutex
	incrs  []incrCall
	gauges []gaugeCall
}

func (r *recordingStatsd) Incr(name string, tags []string, _ float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.incrs = append(r.incrs, incrCall{name: name, tags: append([]string(nil), tags...)})
	return nil
}

func (r *recordingStatsd) Gauge(name string, value float64, tags []string, _ float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges = append(r.gauges, gaugeCall{name: name, value: value, tags: append([]string(nil), tags...)})
	return nil
}

func (r *recordingStatsd) calls() []incrCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]incrCall(nil), r.incrs...)
}

func (r *recordingStatsd) gaugeCalls() []gaugeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]gaugeCall(nil), r.gauges...)
}

func newTestDriver(rs *recordingStatsd) *Driver {
	cfg := Config{
		ConnString:      "postgres://localhost/test",
		SlotName:        "test_slot",
		PublicationName: "test_pub",
	}
	cfg.setDefaults()
	cfg.Statsd = rs
	return &Driver{
		cfg:       cfg,
		maintWake: make(chan struct{}, 1),
	}
}

func TestAcquireLeadership_FromStandby(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)

	d.acquireLeadership()

	assert.Equal(t, "leader", d.Role())
	assert.Equal(t, []incrCall{{name: "jack.courier.leader.acquired", tags: nil}}, rs.calls())

	select {
	case <-d.maintWake:
	default:
		t.Fatal("expected maintWake to be signalled")
	}
}

func TestAcquireLeadership_AlreadyLeaderIsNoOp(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.isLeader.Store(true)

	d.acquireLeadership()

	assert.Equal(t, "leader", d.Role())
	assert.Empty(t, rs.calls(), "no metric should fire when already leader")

	select {
	case <-d.maintWake:
		t.Fatal("maintWake should not be signalled when already leader")
	default:
	}
}

func TestAcquireLeadership_ResetsStandbyLogged(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.standbyLogged = true

	d.acquireLeadership()

	assert.False(t, d.standbyLogged, "acquiring leadership should reset standbyLogged")
}

func TestAcquireLeadership_NoOpDoesNotResetStandbyLogged(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.isLeader.Store(true)
	d.standbyLogged = true

	d.acquireLeadership()

	assert.True(t, d.standbyLogged, "no-op acquire (already leader) should not touch standbyLogged")
}

func TestAcquireLeadership_PendingWakeIsDropped(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.maintWake <- struct{}{}

	d.acquireLeadership()

	assert.Equal(t, "leader", d.Role())

	select {
	case <-d.maintWake:
	default:
		t.Fatal("expected maintWake to still hold a signal")
	}

	select {
	case <-d.maintWake:
		t.Fatal("expected only one pending signal in maintWake")
	default:
	}
}

func TestRelinquishLeadership_FromLeader(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.isLeader.Store(true)

	d.relinquishLeadership("slot_busy")

	assert.Equal(t, "standby", d.Role())
	assert.Equal(t, []incrCall{{
		name: "jack.courier.leader.relinquished",
		tags: []string{"reason:slot_busy"},
	}}, rs.calls())
}

func TestRelinquishLeadership_FromStandbyIsNoOp(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)

	d.relinquishLeadership("shutdown")

	assert.Equal(t, "standby", d.Role())
	assert.Empty(t, rs.calls())
}

func TestRelinquishLeadership_DoubleCallEmitsOnce(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.isLeader.Store(true)

	d.relinquishLeadership("shutdown")
	d.relinquishLeadership("shutdown")

	assert.Len(t, rs.calls(), 1, "double relinquish should emit metric only once")
}

func TestLeaderHeartbeat(t *testing.T) {
	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.cfg.LeaderHeartbeatInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		d.leaderHeartbeat(ctx)
		close(done)
	}()

	assert.Eventually(t, func() bool {
		for _, g := range rs.gaugeCalls() {
			if g.name == "jack.courier.leader.is_leader" && g.value == 0 {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond, "expected at least one standby (value=0) emission")

	d.isLeader.Store(true)

	assert.Eventually(t, func() bool {
		for _, g := range rs.gaugeCalls() {
			if g.name == "jack.courier.leader.is_leader" && g.value == 1 {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond, "expected a leader (value=1) emission after isLeader flipped")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("leaderHeartbeat did not exit after context cancel")
	}
}
