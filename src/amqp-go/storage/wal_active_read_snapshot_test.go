package storage

// Snapshot-inspection fixtures for N1. These reference the new API
// (QueueWAL.activeRead / walActiveRead) directly, so they live apart from the
// purely behavioral fixtures in wal_active_read_test.go.

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

// TestQueueWALActiveReadIsOffCommitterLine pinned the cache-line separation a
// padded activeRead would have bought. Measurement showed no effect (the
// snapshot is written only on a roll, not per batch), so it is intentionally
// NOT padded and this assertion is not made.

// TestWALActiveReadSnapshotTracksRoll pins that the published snapshot advances
// with the active file on a roll, and that records in a ROLLED file are still
// served (through the sealed arm) so the fast path did not narrow the read path.
func TestWALActiveReadSnapshotTracksRoll(t *testing.T) {
	wm, sw := newActiveReadWAL(t, oldHandleWALFileSize)

	const n = oldHandleRecords
	base := uint64(1) << 44
	body := make([]byte, oldHandleBodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	for i := 0; i < n; i++ {
		require.NoError(t, wm.Write(activeReadQueue, &protocol.Message{
			RoutingKey:   activeReadQueue,
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  base + uint64(i),
			MessageID:    "snap",
		}, base+uint64(i)))
	}

	ar := sw.activeRead.Load()
	require.NotNil(t, ar, "PREMISE BROKEN: the active-read snapshot was never published")
	require.Equal(t, sw.fileNum.Load(), ar.num,
		"the snapshot must name the ACTIVE file after a roll")
	require.Same(t, sw.currentReadFile, ar.f,
		"the snapshot's handle must be the active file's read handle")

	// The newest record is in the active file: served from the snapshot.
	newest := base + uint64(n-1)
	require.Equal(t, sw.fileNum.Load(), activeIndexPos(t, sw, activeReadQueue, newest).segmentID,
		"premise: the newest record is in the active file")
	got, err := wm.Read(activeReadQueue, newest)
	require.NoError(t, err)
	require.Equal(t, body, got.Body)

	// The oldest record is in a rolled file: served through the sealed arm.
	require.NotEqual(t, sw.fileNum.Load(), activeIndexPos(t, sw, activeReadQueue, base).segmentID,
		"premise: the oldest record is in a rolled file")
	old, err := wm.Read(activeReadQueue, base)
	require.NoError(t, err)
	require.Equal(t, body, old.Body)
}

// TestWALActiveReadDegradesOnClosedHandle proves the ErrClosed arm is a
// DEGRADE, not a fault: when the snapshot's handle has been closed under the
// reader (the one window close() opens), the read falls back to the locked
// authoritative path instead of reporting a transient close as a read error.
func TestWALActiveReadDegradesOnClosedHandle(t *testing.T) {
	wm, sw := newActiveReadWAL(t, DefaultWALFileSize)

	require.NoError(t, wm.Write(activeReadQueue, activeReadMessage(t, activeReadQueue, 1), 1))

	// A SECOND, independent read handle to the active file, closed so its
	// ReadAt returns a typed os.ErrClosed (the platform guarantee asserted by
	// TestClosedOldFileHandle_ReadAtYieldsTypedErrClosed).
	closed, err := os.Open(sw.currentFile.Name())
	require.NoError(t, err)
	require.NoError(t, closed.Close())

	sw.activeRead.Store(&walActiveRead{num: sw.fileNum.Load(), f: closed})
	t.Cleanup(func() { sw.activeRead.Store(nil) })

	msg, err := wm.Read(activeReadQueue, 1)
	require.NoError(t, err, "a closed snapshot handle must degrade to the locked path, not fault")
	require.Equal(t, []byte("active-read-body"), msg.Body)
}
