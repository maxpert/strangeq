package storage

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C5: retention used to force-delete an expired WAL file even when its
// checkpoint had failed (or when there was no segment manager at all) —
// destroying the only copy of unacknowledged durable records. Retention never
// wins over durability.

// TestRetention_CheckpointFailure_KeepsWAL (TDD test 19).
func TestRetention_CheckpointFailure_KeepsWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := coldTailWALConfig()
	cfg.RetentionPeriod = 50 * time.Millisecond
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	const n = 5
	for off := uint64(1); off <= n; off++ {
		require.NoError(t, wm.Write("q", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "q",
			Body: []byte("retention-keeps-wal-body-padding-padding"), DeliveryMode: 2, DeliveryTag: off,
		}, off))
	}
	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	walPath := sw.currentFile.Name()
	rolledNum := sw.fileNum.Load()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()

	// Every segment write fails: the checkpoint can never succeed.
	restore := SetSegmentWriteForTest(func(f *os.File, b []byte) (int, error) {
		if strings.HasPrefix(f.Name(), dir) {
			return 0, syscall.ENOSPC
		}
		return f.Write(b)
	})

	time.Sleep(100 * time.Millisecond) // expire
	sw.tryDeleteOldFiles()

	_, serr := os.Stat(walPath)
	require.False(t, os.IsNotExist(serr), "an expired file whose checkpoint FAILED must be kept")
	sw.oldFilesMutex.RLock()
	_, registered := sw.oldFiles[rolledNum]
	sw.oldFilesMutex.RUnlock()
	require.True(t, registered, "the expired file must stay registered in oldFiles")

	// Fault cleared: the next sweep checkpoints durably and THEN deletes.
	restore()
	sw.tryDeleteOldFiles()
	_, serr = os.Stat(walPath)
	require.True(t, os.IsNotExist(serr), "once the checkpoint succeeds the expired file is deleted")

	for off := uint64(1); off <= n; off++ {
		msg, rerr := sm.Read("q", off)
		require.NoError(t, rerr, "unacked offset %d must be served from the segment", off)
		require.NotEmpty(t, msg.Body)
	}
}

// TestRetention_NilSegmentManager_KeepsUnackedWAL (TDD test 20): with no
// segment manager there is nowhere to copy unacked records, so an expired
// file is kept; once everything is acked, the allAcked route deletes it.
func TestRetention_NilSegmentManager_KeepsUnackedWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := coldTailWALConfig()
	cfg.RetentionPeriod = 50 * time.Millisecond
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	// NOTE: no segment manager.

	const n = 3
	for off := uint64(1); off <= n; off++ {
		require.NoError(t, wm.Write("q", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "q",
			Body: []byte("no-segment-manager-body-padding-padding"), DeliveryMode: 2, DeliveryTag: off,
		}, off))
	}
	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	walPath := sw.currentFile.Name()
	rolledNum := sw.fileNum.Load()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()

	time.Sleep(100 * time.Millisecond) // expire
	sw.tryDeleteOldFiles()

	// Unacked records + no segment manager: the file is kept.
	_, serr := os.Stat(walPath)
	require.False(t, os.IsNotExist(serr), "an expired file with unacked records and no segment manager must be kept (nothing to copy them to)")
	sw.oldFilesMutex.RLock()
	_, registered := sw.oldFiles[rolledNum]
	sw.oldFilesMutex.RUnlock()
	require.True(t, registered)

	// Variant: everything acked -> the allAcked route deletes the file even
	// without a segment manager (deleting loses nothing).
	for off := uint64(1); off <= n; off++ {
		wm.Acknowledge("q", off)
	}
	require.Eventually(t, func() bool {
		sw.bitmapMutex.RLock()
		defer sw.bitmapMutex.RUnlock()
		return sw.ackBitmap.GetCardinality() == n
	}, 3*time.Second, 2*time.Millisecond, "ack bitmap did not register acks")

	sw.tryDeleteOldFiles()
	_, serr = os.Stat(walPath)
	require.True(t, os.IsNotExist(serr), "a fully-acknowledged file is deleted through allAcked even without a segment manager")
}
