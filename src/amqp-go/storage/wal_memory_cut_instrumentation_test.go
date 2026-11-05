package storage

// WS-MEMORY-CUT §8 instrumentation.
//
// The spec flags one gap it could not explain from the artifacts: the WAL
// index maps measure ~6× the standing publish/deliver backlog (F6). The
// instrumentation lands BEFORE the S1c sub-cut is written, so the split can be
// measured rather than guessed. These tests pin that the counters report the
// REAL maps under the real locks (not a shadow copy), which is what makes the
// measurement trustworthy.

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestWALBookkeepingStats_ReportsInventoryAndIndex proves the counters report
// the live maps: with N unacknowledged records the active inventory holds N
// records and the index holds N physical copies.
func TestWALBookkeepingStats_ReportsInventoryAndIndex(t *testing.T) {
	cfg := walIndexBoundConfig()
	cfg.SyncDisabled = true
	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const n = 100
	for i := uint64(1); i <= n; i++ {
		require.NoError(t, wm.Write("bk-q", &protocol.Message{
			RoutingKey:   "bk-q",
			Body:         []byte("x"),
			DeliveryMode: 2,
			DeliveryTag:  i,
		}, i))
	}

	inventoryRecords, indexCopies, drained := wm.WALBookkeepingStats()
	require.Equal(t, n, inventoryRecords,
		"the active inventory must report every unacknowledged record")
	require.Equal(t, n, indexCopies,
		"the index must report every unacknowledged physical copy")
	require.Zero(t, drained, "no settle-drain has run before the cut")

	// The unexported accessor and the exported one agree: the telemetry path
	// and the test path read the same maps.
	current, _ := wm.sharedWAL.inventorySizesForTest()
	require.Equal(t, inventoryRecords, current)
}
