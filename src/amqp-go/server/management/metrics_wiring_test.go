package management

import (
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// mgmtWireCollector is the single "amqp"-namespace metrics collector in this
// test binary. NewCollector registers its fixed families on the default
// registry, so it MUST be created exactly once (`-count=N` would otherwise
// panic on duplicate registration). The amqp_management_* families are added by
// RegisterManagement (idempotent).
var (
	mgmtWireOnce      sync.Once
	mgmtWireCollector *metrics.Collector
)

func mgmtWireMetrics() *metrics.Collector {
	mgmtWireOnce.Do(func() {
		mgmtWireCollector = metrics.NewCollector("amqp")
		mgmtWireCollector.RegisterManagement()
	})
	return mgmtWireCollector
}

// mgmtLabelMatch reports whether every wanted label pair is present.
func mgmtLabelMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(pairs))
	for _, p := range pairs {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// mgmtScrapeGauge sums a gauge family on the default registry for a label set.
func mgmtScrapeGauge(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range f.GetMetric() {
			if mgmtLabelMatch(m.GetLabel(), labels) {
				total += m.GetGauge().GetValue()
			}
		}
		return total
	}
	return 0
}

// mgmtScrapeHistogramCount returns the sample count of a histogram family on
// the default registry for a label set.
func mgmtScrapeHistogramCount(t *testing.T, name string, labels map[string]string) uint64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range f.GetMetric() {
			if mgmtLabelMatch(m.GetLabel(), labels) {
				total += m.GetHistogram().GetSampleCount()
			}
		}
		return total
	}
	return 0
}

// TestManagementJobsGauge_TracksQueueLifecycle (MEDIUM-2): the job pool feeds
// amqp_management_jobs{state}. With a single worker busy on a blocking job, a
// second submit is deterministically queued.
func TestManagementJobsGauge_TracksQueueLifecycle(t *testing.T) {
	c := mgmtWireMetrics()
	pool := newJobPool(8, time.Minute, 1)
	pool.setSink(c)
	t.Cleanup(pool.close)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// A failing assertion must not leave the single worker blocked in j1.fn:
	// pool.close (registered first) waits for the workers, so this cleanup
	// (registered last, runs first) must release them.
	t.Cleanup(unblock)

	if _, err := pool.submit("purge", "alice", nil, func() (any, error) {
		close(started)
		<-release
		return nil, nil
	}); err != nil {
		t.Fatalf("submit j1: %v", err)
	}
	<-started // the single worker is now running j1 and blocked in its fn

	j2, err := pool.submit("purge", "alice", nil, func() (any, error) {
		<-release
		return nil, nil
	})
	if err != nil {
		t.Fatalf("submit j2: %v", err)
	}

	// j1 is running, j2 is queued behind the only worker.
	if got := mgmtScrapeGauge(t, "amqp_management_jobs", map[string]string{"state": "queued"}); got != 1 {
		t.Fatalf("amqp_management_jobs{state=queued} = %v, want 1", got)
	}
	if got := mgmtScrapeGauge(t, "amqp_management_jobs", map[string]string{"state": "running"}); got != 1 {
		t.Fatalf("amqp_management_jobs{state=running} = %v, want 1", got)
	}

	unblock()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if st, _ := pool.stateOf(j2.id, "alice"); st == jobDone || st == jobFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("j2 did not reach a terminal state within 3s")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := mgmtScrapeGauge(t, "amqp_management_jobs", map[string]string{"state": "queued"}); got != 0 {
		t.Fatalf("amqp_management_jobs{state=queued} = %v after completion, want 0", got)
	}
}

// TestManagementSnapshotLockWait_RecordedOnConnectionSnapshot (MEDIUM-2): the
// permitted Server.Mutex.RLock a management snapshot takes is timed. The
// overview's production path uses the lock-free entity seam, so the reachable
// observation point is the connections/channels snapshot.
func TestManagementSnapshotLockWait_RecordedOnConnectionSnapshot(t *testing.T) {
	c := mgmtWireMetrics()
	e := basicEnv(t, true, nil)
	e.mgmt.sink = c

	before := mgmtScrapeHistogramCount(t, "amqp_management_snapshot_lock_wait_seconds", nil)
	rec := e.do("GET", "/api/connections", nil, basicAuthHeader())
	wantStatus(t, rec, 200)
	after := mgmtScrapeHistogramCount(t, "amqp_management_snapshot_lock_wait_seconds", nil)
	if after-before < 1 {
		t.Fatalf("amqp_management_snapshot_lock_wait_seconds delta = %d, want >= 1", after-before)
	}
}
