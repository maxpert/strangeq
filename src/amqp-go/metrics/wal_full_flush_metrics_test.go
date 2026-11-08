package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Shared because promauto registration is process-global (a second
// NewCollector with the same namespace panics under -count>1).
var walFullFlushTestCollector = NewCollector("walff")

// TestWALFullFlushCounterSink pins the barrier-mode gate metric: the labelled
// counter is pre-initialized to 0 per known site and increments per recorded
// F_FULLFSYNC.
func TestWALFullFlushCounterSink(t *testing.T) {
	c := walFullFlushTestCollector
	sites := []string{"group_commit", "close"}
	c.InitWALFullFlushSites(sites)

	for _, s := range sites {
		if got := testutil.ToFloat64(c.WALFullFlushTotal.WithLabelValues(s)); got != 0 {
			t.Fatalf("wal_full_flush_total{site=%q} after init = %v, want 0", s, got)
		}
	}

	c.RecordWALFullFlush("group_commit")
	c.RecordWALFullFlush("group_commit")
	c.RecordWALFullFlush("close")

	if got := testutil.ToFloat64(c.WALFullFlushTotal.WithLabelValues("group_commit")); got != 2 {
		t.Fatalf("wal_full_flush_total{site=group_commit} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(c.WALFullFlushTotal.WithLabelValues("close")); got != 1 {
		t.Fatalf("wal_full_flush_total{site=close} = %v, want 1", got)
	}
}
