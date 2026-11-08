package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Shared because promauto registration is process-global (a second
// NewCollector with the same namespace panics under -count>1).
var walBookkeepingTestCollector = NewCollector("walbk")

// TestWALBookkeepingGaugesReadStatsAtScrape pins the WS-MEMORY-CUT §8
// diagnostics: the three gauges read the bound accessor AT SCRAPE TIME, so a
// measurement window sees the live values with no Set call.
func TestWALBookkeepingGaugesReadStatsAtScrape(t *testing.T) {
	c := walBookkeepingTestCollector

	var inventory, copies int
	var drained uint64
	c.BindWALBookkeepingStats(func() (int, int, uint64) { return inventory, copies, drained })

	inventory, copies, drained = 4000, 1, 7
	if got := testutil.ToFloat64(c.WALInventoryRecords); got != 4000 {
		t.Fatalf("wal_inventory_records = %v, want 4000", got)
	}
	if got := testutil.ToFloat64(c.WALIndexCopies); got != 1 {
		t.Fatalf("wal_index_copies = %v, want 1", got)
	}
	if got := testutil.ToFloat64(c.WALSettleDrained); got != 7 {
		t.Fatalf("wal_settle_drained = %v, want 7", got)
	}

	// Live at scrape: a change is visible on the next scrape with no Set.
	inventory = 3
	if got := testutil.ToFloat64(c.WALInventoryRecords); got != 3 {
		t.Fatalf("wal_inventory_records after change = %v, want 3", got)
	}
}
