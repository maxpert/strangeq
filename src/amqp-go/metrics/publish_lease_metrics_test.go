package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Shared because promauto registration is process-global (a second
// NewCollector with the same namespace panics under -count>1).
var leaseGaugeTestCollector = NewCollector("leasegf")

// WS-I review (item 8): the publish-lease gauges are GaugeFuncs reading the
// limiter's Stats at SCRAPE time (never stale), and the budget is initialized
// at bind time.
func TestPublishLeaseGaugeFuncsReadStatsAtScrape(t *testing.T) {
	c := leaseGaugeTestCollector

	var used, budget int64
	var waiters int
	c.BindPublishLeaseStats(func() (int64, int64, int) { return used, budget, waiters })

	used, budget, waiters = 1234, 256<<20, 7
	if got := testutil.ToFloat64(c.PublishLeaseUsedBytes); got != 1234 {
		t.Fatalf("used gauge = %v, want 1234", got)
	}
	if got := testutil.ToFloat64(c.PublishLeaseBudgetBytes); got != 256<<20 {
		t.Fatalf("budget gauge = %v, want %v", got, 256<<20)
	}
	if got := testutil.ToFloat64(c.PublishLeaseWaiters); got != 7 {
		t.Fatalf("waiters gauge = %v, want 7", got)
	}

	// Live at scrape: a change is visible on the next scrape with no Set.
	used, waiters = 99, 0
	if got := testutil.ToFloat64(c.PublishLeaseUsedBytes); got != 99 {
		t.Fatalf("used gauge after change = %v, want 99", got)
	}
}
