//go:build darwin

package storage

// The barrier-arm gate: amqp_wal_full_flush_total must stay 0 for the whole
// measured window while the broker is serving traffic. These tests drive the
// load-coupled sites (rolls, reclaim, compaction) and assert no F_FULLFSYNC
// fires before a clean close, then that the close does issue one.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// bigBody makes each record large enough that a 4096-byte FileSize rolls the
// WAL several times within a small write count.
var bigBody = strings.Repeat("x", 400)

// fullFlushLoadFixture writes enough under a tiny FileSize to roll the WAL
// several times, then forces one reclaim and one compaction.
func fullFlushLoadFixture(t *testing.T, cfg WALConfig) *WALManager {
	t.Helper()
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	qw := wm.sharedWAL

	// Several rolls: 4096-byte files, ~300-byte records.
	for tag := uint64(1); tag <= 60; tag++ {
		require.NoError(t, wm.Write("ff", &protocol.Message{
			Exchange:     "amq.direct",
			RoutingKey:   "ff",
			Body:         []byte(fmt.Sprintf("full-flush-body-%02d-%s", tag, bigBody)),
			DeliveryMode: 2,
			DeliveryTag:  tag,
		}, tag))
	}

	// One compaction: roll, ack a majority, then sweep.
	qw.fileMutex.Lock()
	require.NoError(t, qw.rollFile())
	qw.fileMutex.Unlock()
	for tag := uint64(1); tag <= 60; tag++ {
		wm.Acknowledge("ff", tag)
	}
	qw.tryCompactOldFiles()
	qw.tryReclaimOldFiles()
	return wm
}

// TestWALFullFlushGate_BarrierStaysZeroUnderLoad: in barrier mode no
// load-coupled site issues F_FULLFSYNC; a clean close does.
func TestWALFullFlushGate_BarrierStaysZeroUnderLoad(t *testing.T) {
	cfg := crashModelTestConfig(false) // barrier
	cfg.FileSize = 4096

	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	qw := wm.sharedWAL

	// Everything above boot is off the traffic path; the gate is the window
	// AFTER boot and BEFORE close.
	before := WALFullFlushTotal()

	for tag := uint64(1); tag <= 60; tag++ {
		require.NoError(t, wm.Write("ff", &protocol.Message{
			Exchange:     "amq.direct",
			RoutingKey:   "ff",
			Body:         []byte(fmt.Sprintf("full-flush-body-%02d-%s", tag, bigBody)),
			DeliveryMode: 2,
			DeliveryTag:  tag,
		}, tag))
	}
	require.GreaterOrEqual(t, qw.fileNum.Load(), uint64(3),
		"the fixture must have rolled at least three times")

	qw.fileMutex.Lock()
	require.NoError(t, qw.rollFile())
	qw.fileMutex.Unlock()
	for tag := uint64(1); tag <= 60; tag++ {
		wm.Acknowledge("ff", tag)
	}
	qw.tryCompactOldFiles()
	qw.tryReclaimOldFiles()

	require.Equal(t, before, WALFullFlushTotal(),
		"barrier mode must issue ZERO F_FULLFSYNC during the served window "+
			"(rolls, reclaim and compaction included)")

	require.NoError(t, wm.Close())
	require.Greater(t, WALFullFlushTotal(), before,
		"a clean close must issue the full drive-cache flush")
}

// TestWALFullFlushGate_ForceFlushIssuesFullFlashes is the control: force_flush
// DOES issue F_FULLFSYNC during the same load, proving the counter is live.
func TestWALFullFlushGate_ForceFlushIssuesFullFlashes(t *testing.T) {
	cfg := crashModelTestConfig(true) // force_flush
	cfg.FileSize = 4096

	before := WALFullFlushTotal()
	wm := fullFlushLoadFixture(t, cfg)
	t.Cleanup(func() { _ = wm.Close() })

	require.Greater(t, WALFullFlushTotal(), before,
		"force_flush must issue F_FULLFSYNCs during the same load")
}

// TestWALFullFlushGate_CountsEveryObserved51 ties the counter to the cmd-51
// stream the site-pin hook observes: with the hook installed (so the real
// fcntl is replaced but the choke point still records success), the counter
// delta equals the number of 51s observed in force_flush mode.
func TestWALFullFlushGate_CountsEveryObserved51(t *testing.T) {
	rec := &syncCmdRecorder{}
	rec.install(t)

	cfg := crashModelTestConfig(true)
	cfg.FileSize = 4096
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)

	before := WALFullFlushTotal()
	rec.reset()
	for tag := uint64(1); tag <= 30; tag++ {
		require.NoError(t, wm.Write("ff", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "ff",
			Body: []byte(fmt.Sprintf("count-body-%02d", tag)), DeliveryMode: 2, DeliveryTag: tag,
		}, tag))
	}
	observed := 0
	for _, c := range rec.snapshot() {
		if c == cmFull {
			observed++
		}
	}
	require.Greater(t, observed, 0)
	require.Equal(t, uint64(observed), WALFullFlushTotal()-before,
		"the full-flush counter must count exactly the F_FULLFSYNCs issued")
	require.NoError(t, wm.Close())
}
