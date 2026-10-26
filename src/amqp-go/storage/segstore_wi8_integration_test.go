package storage

import (
	"errors"
	"io/fs"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestDurableConfirmUsesGroupFsync(t *testing.T) {
	cfg := DefaultWALConfig()
	cfg.BatchSize = 4
	cfg.ChannelBuffer = 16
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	so := installStageObserver(t, 1)
	fsync := installGatedFsync(t, map[int]error{1: nil}, nil)
	defer so.releaseBrake()
	defer fsync.releaseAll()

	const n = 4
	completions := make(chan error, n)
	publish := func(tag uint64) {
		completions <- wm.Write("group-fsync", &protocol.Message{
			DeliveryTag: tag, DeliveryMode: 2, Body: []byte{byte(tag)},
		}, tag)
	}
	go publish(1)
	waitSignal(t, so.brakePoint(1).engaged, "the first staged publish")
	for tag := uint64(2); tag <= n; tag++ {
		go publish(tag)
	}

	// The stager is parked on record one. Wait until all three other sends are
	// queued, then release it so its coalescing drain must stage one four-record
	// batch rather than relying on goroutine scheduling or a sleep.
	deadline := time.Now().Add(10 * time.Second)
	for len(wm.sharedWAL.writeChan) != n-1 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for three queued publishes; queued=%d", len(wm.sharedWAL.writeChan))
		}
		runtime.Gosched()
	}
	so.releasePoint(1)
	waitSignal(t, fsync.entered[0], "the covering group fsync")
	require.Equal(t, int64(1), fsync.calls.Load())
	require.Equal(t, int64(n), so.count.Load())
	select {
	case err := <-completions:
		t.Fatalf("publish completed before its covering fsync was released: %v", err)
	default:
	}

	fsync.releaseCall(1)
	for i, err := range waitErrors(t, completions, n, "all four durable completions") {
		require.NoError(t, err, "completion %d", i)
	}
	require.Equal(t, int64(1), fsync.calls.Load(), "all four publishes must share exactly one fsync")
}

func TestNoCheckpointCopiesWALToLegacySegments(t *testing.T) {
	dir := t.TempDir()
	ds, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)

	const queue = "wal-only"
	require.NoError(t, ds.wal.Write(queue, &protocol.Message{
		DeliveryTag: 1, DeliveryMode: 2, Body: []byte("durable-in-wal"),
	}, 1))
	_, rolledPath := rollSegstoreReclaimWAL(t, ds.wal)

	var segmentWrites atomic.Int64
	restore := SetSegmentWriteForTest(func(*os.File, []byte) (int, error) {
		segmentWrites.Add(1)
		return 0, errors.New("legacy segment writes are forbidden")
	})
	defer restore()

	msg, err := ds.wal.Read(queue, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("durable-in-wal"), msg.Body)
	require.NoError(t, ds.Close())
	require.Zero(t, segmentWrites.Load(), "close must not checkpoint WAL records into legacy segments")
	_, err = os.Stat(rolledPath)
	require.NoError(t, err, "an unacknowledged WAL file must survive close")

	reopened, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	msg, err = reopened.wal.Read(queue, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("durable-in-wal"), msg.Body)
	_, err = reopened.segments.Read(queue, 1)
	require.Error(t, err, "no legacy copy may be created")
	require.Zero(t, segmentWrites.Load())
}

// writeLegacyFixture appends pre-upgrade records without using the deleted
// SegmentManager.Write/CheckpointBatch production APIs.
func writeLegacyFixture(t *testing.T, dir, queue string, messages ...*RecoveryMessage) string {
	t.Helper()
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	qs, err := sm.getOrCreateQueueSegments(queue)
	require.NoError(t, err)
	qs.mutex.Lock()
	path := qs.currentSegment.path
	qs.mutex.Unlock()
	require.NoError(t, sm.Close())

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	for _, rm := range messages {
		record, serErr := serializeSegmentMessage(rm.Message, rm.Offset, false)
		require.NoError(t, serErr)
		_, err = file.Write(record)
		require.NoError(t, err)
	}
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	return path
}

func TestLegacySegmentDrainAndDedup(t *testing.T) {
	dir := t.TempDir()
	const queue = "legacy-drain"
	legacyPath := writeLegacyFixture(t, dir, queue,
		&RecoveryMessage{QueueName: queue, Offset: 1, Message: &protocol.Message{
			DeliveryTag: 1, DeliveryMode: 2, Body: []byte("stale-legacy-copy"),
		}},
		&RecoveryMessage{QueueName: queue, Offset: 2, Message: &protocol.Message{
			DeliveryTag: 2, DeliveryMode: 2, Body: []byte("legacy-only"),
		}},
	)

	ds, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)
	ds.getOrCreateQueueHot(queue)
	for tag, body := range map[uint64]string{1: "newest-wal-copy", 3: "wal-only-survivor"} {
		require.NoError(t, ds.wal.Write(queue, &protocol.Message{
			DeliveryTag: tag, DeliveryMode: 2, Body: []byte(body),
		}, tag))
	}
	rollSegstoreReclaimWAL(t, ds.wal)

	recovered, err := ds.GetRecoverableMessages()
	require.NoError(t, err)
	require.Len(t, recovered[queue], 3, "WAL must win the duplicate while the legacy-only record drains")
	byTag := make(map[uint64]string, len(recovered[queue]))
	for _, msg := range recovered[queue] {
		byTag[msg.DeliveryTag] = string(msg.Body)
	}
	require.Equal(t, map[uint64]string{1: "newest-wal-copy", 2: "legacy-only", 3: "wal-only-survivor"}, byTag)

	require.NoError(t, ds.DeleteMessage(queue, 1))
	require.NoError(t, ds.DeleteMessage(queue, 2))
	value, ok := ds.segments.queueSegments.Load(queue)
	require.True(t, ok)
	qs := value.(*QueueSegments)
	// Settle the legacy physical copies synchronously; the public Acknowledge
	// batcher is intentionally asynchronous and must not make this test timed.
	qs.applyAck(1)
	qs.applyAck(2)
	qs.tryCompaction()
	_, err = os.Stat(legacyPath)
	require.True(t, errors.Is(err, fs.ErrNotExist), "fully acknowledged legacy data must be reclaimed: %v", err)
	ds.wal.sharedWAL.tryReclaimOldFiles()
	require.NoError(t, ds.Close())

	reopened, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err = reopened.GetRecoverableMessages()
	require.NoError(t, err)
	// The LEGACY copy of tag 1 is gone (reclaimed) and the legacy-only tag 2
	// is drained: neither may reappear. Tag 1's WAL copy DOES reappear, because
	// acks are process-volatile (the spec's contract: "Acks are
	// process-volatile: crash redelivery is allowed") — the same as the
	// pre-segstore baseline, where ackBitmap also starts empty at every boot.
	// The dedup property under test is that tag 1 appears EXACTLY ONCE (the
	// WAL copy wins over the legacy copy), and that no legacy-only record
	// survives.
	require.Len(t, recovered[queue], 2, "only the WAL copies may survive: tag 1 and tag 3")
	byTag = make(map[uint64]string, len(recovered[queue]))
	for _, msg := range recovered[queue] {
		byTag[msg.DeliveryTag] = string(msg.Body)
	}
	require.Equal(t, map[uint64]string{1: "newest-wal-copy", 3: "wal-only-survivor"}, byTag,
		"the WAL copy must win the duplicate, and the legacy-only record must be drained")
}
