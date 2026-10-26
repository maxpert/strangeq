package storage

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C7: read-ahead treated a stored CRC of 0 as a mismatch, so it was
// effectively disabled whenever crccheck was off; it had no blank-header stop
// (a zero region parsed as garbage records); and it read past the durable end
// of the file — bytes written but not yet fsynced (or belonging to a failed
// batch) were cached and served by GetMessage, breaking the
// persist ⇒ fsynced ⇒ observable ordering.

// newReadAheadCRCDisabledStorage is newReadAheadTestStorage's config with
// WALCRCDisabled set (fsync already disabled by the base helper, matching it).
func newReadAheadCRCDisabledStorage(t *testing.T, tmpDir string, ringSize int) *DisruptorStorage {
	t.Helper()
	ec := interfaces.EngineConfig{
		RingBufferSize:        ringSize,
		SpillThresholdPercent: 80,
		WALSyncDisabled:       true,
		WALCRCDisabled:        true,
	}
	storage, err := NewDisruptorStorageWithEngineConfig(tmpDir, ec)
	require.NoError(t, err)
	return storage
}

// TestReadAhead_ZeroCRC_ServesReads (TDD test 24).
func TestReadAhead_ZeroCRC_ServesReads(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadCRCDisabledStorage(t, tmpDir, 64)
	defer storage.Close()

	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))
	publishDurableMessages(t, storage, "q", 200)
	clearRingForWALFallback(storage, "q", 200)

	msg, err := storage.GetMessage("q", 0)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Tags 1..63 are then served from the buffer without another batch read.
	for tag := uint64(1); tag < readAheadMaxEntries; tag++ {
		m, gerr := storage.GetMessage("q", tag)
		require.NoError(t, gerr, "tag %d must be served from the read-ahead buffer", tag)
		require.Equal(t, []byte("msg-"+readAheadBody(int(tag))), m.Body)
	}
}

// TestReadAhead_DoesNotCacheBeyondDurableEnd (TDD test 25).
//
// SPEC DEVIATION (documented): the spec's variant blocks the group-commit
// fsync hook and then calls GetMessage — but every WAL read path serializes
// on fileMutex, which the blocked committer holds, so the public API cannot
// observe the in-flight window at all (the read blocks until the hook
// releases, after which the batch is durable and legitimately readable). The
// same property is therefore asserted two ways: (a) through the public API
// with a one-shot FAILING fsync (the written-but-nacked batch's bytes are
// past the durable end and must not be served), and (b) at the unit level
// with a BLOCKING fsync hook, calling collectBatchFrom directly (it takes no
// fileMutex) while the batch's bytes are on disk but not yet fsynced.
func TestReadAhead_DoesNotCacheBeyondDurableEnd(t *testing.T) {
	t.Run("public API: a written-but-nacked batch is not served or cached", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := newReadAheadTestStorageSync(t, tmpDir, 64)
		defer storage.Close()

		require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))
		publishDurableMessagesSync(t, storage, "q", 10)
		clearRingForWALFallback(storage, "q", 10)

		var fired atomic.Int32
		restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
			if fired.Add(1) == 1 {
				return syscall.EIO
			}
			return fdatasyncFile(f)
		})
		defer restore()

		const bad = uint64(10)
		done := make(chan error, 1)
		storage.StoreMessageAsync("q", &protocol.Message{
			Body: []byte("not-durable-yet"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: bad,
		}, func(e error) { done <- e })
		select {
		case e := <-done:
			require.Error(t, e, "tag 10's batch must be nacked")
		case <-time.After(5 * time.Second):
			t.Fatal("completion never fired")
		}

		_, err := storage.GetMessage("q", 9)
		require.NoError(t, err, "the last durable tag is served")
		_, _, cached := storage.hot.Get("q", bad)
		require.False(t, cached,
			"a tag whose batch was written but not fsynced (nacked) must not be cached")
		_, err = storage.GetMessage("q", bad)
		require.Error(t, err, "the nacked tag must not be served")
	})

	t.Run("unit: an in-flight (written, not yet fsynced) batch is beyond the durable end", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := newReadAheadTestStorageSync(t, tmpDir, 64)
		defer storage.Close()

		require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))
		publishDurableMessagesSync(t, storage, "q", 10)
		clearRingForWALFallback(storage, "q", 10)

		sw := storage.wal.sharedWAL
		// Snapshot the current read handle and tag 9's position BEFORE the
		// in-flight write (collectBatchFrom takes no fileMutex, so it can run
		// while the flush is parked inside the fsync hook).
		sw.fileMutex.Lock()
		readFile := sw.currentReadFile
		sw.fileMutex.Unlock()
		require.NotNil(t, readFile)

		entered := make(chan struct{})
		var once sync.Once
		release := make(chan struct{})
		var releaseOnce sync.Once
		doRelease := func() { releaseOnce.Do(func() { close(release) }) }
		restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
			once.Do(func() { close(entered) })
			<-release
			return fdatasyncFile(f)
		})
		defer restore()
		defer doRelease()

		const inflight = uint64(10)
		storage.StoreMessageAsync("q", &protocol.Message{
			Body: []byte("in-flight"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: inflight,
		}, func(error) {})

		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("fsync hook never entered")
		}

		// Tag 10's bytes are now on disk but NOT fsynced: fileOffset (the
		// durable frontier) has not advanced past them.
		sw.offsetIndexMutex.RLock()
		loc, ok := sw.offsetIndex[recordKey{queueName: "q", deliveryTag: 9}]
		sw.offsetIndexMutex.RUnlock()
		require.True(t, ok, "tag 9 must be indexed")

		// The durable-end bound must keep the batch read inside [0, fileOffset).
		got, err := sw.collectBatchFrom(readFile, "q", loc.pos.position, sw.fileOffset.Load())
		require.NoError(t, err)
		require.NotContains(t, got, inflight,
			"a written-but-not-yet-fsynced record must not be read-ahead cacheable (past the durable end)")
		require.Contains(t, got, uint64(9), "the durable tag is still readable")

		// Once the fsync releases and the batch completes, tag 10 IS readable.
		doRelease()
		require.Eventually(t, func() bool {
			sw.offsetIndexMutex.RLock()
			defer sw.offsetIndexMutex.RUnlock()
			_, ok := sw.offsetIndex[recordKey{queueName: "q", deliveryTag: inflight}]
			return ok
		}, 5*time.Second, 2*time.Millisecond, "the in-flight batch must complete after the hook releases")
	})
}

// TestReadAhead_StopsAtBlankHeader (TDD test 26): a unit test of
// collectBatchFrom against a buffer of N records followed by zeros, with CRC
// disabled: exactly N records, no error, no garbage.
func TestReadAhead_StopsAtBlankHeader(t *testing.T) {
	dir := t.TempDir()
	const n = 3

	// Build a WAL file: header + n CRC-disabled records + a zero-filled tail.
	buf := []byte{}
	buf = append(buf, WALMagic...)
	buf = append(buf, WALFormatVersion)
	dataStart := int64(len(buf))
	for i := uint64(1); i <= n; i++ {
		var err error
		buf, err = appendMessageRecord(buf, "q", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "q",
			Body: []byte("blank-header-stop-body-padding-padding"), DeliveryMode: 2, DeliveryTag: i,
		}, i, true /* crcDisabled */)
		require.NoError(t, err)
	}
	buf = append(buf, make([]byte, 128)...)

	path := filepath.Join(dir, "blank.wal")
	require.NoError(t, os.WriteFile(path, buf, 0644))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var qw QueueWAL
	qw.cfg = DefaultWALConfig()
	qw.cfg.CRCDisabled = true

	got, err := qw.collectBatchFrom(f, "q", dataStart, -1)
	require.NoError(t, err)
	require.Len(t, got, n, "the N records before the zero region must be decoded; the zeros must stop the scan")
	for i := uint64(1); i <= n; i++ {
		m, ok := got[i]
		require.True(t, ok, "record %d must be present", i)
		require.Equal(t, []byte("blank-header-stop-body-padding-padding"), m.Body)
	}
}

// newReadAheadTestStorageSync is newReadAheadTestStorage with fsync ENABLED
// (real durability barriers on the write path).
func newReadAheadTestStorageSync(t *testing.T, tmpDir string, ringSize int) *DisruptorStorage {
	t.Helper()
	ec := interfaces.EngineConfig{
		RingBufferSize:        ringSize,
		SpillThresholdPercent: 80,
	}
	storage, err := NewDisruptorStorageWithEngineConfig(tmpDir, ec)
	require.NoError(t, err)
	return storage
}

// publishDurableMessagesSync is publishDurableMessages for a fsync-enabled
// storage (StoreMessage blocks until the batch is durable).
func publishDurableMessagesSync(t *testing.T, storage *DisruptorStorage, queueName string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		msg := &protocol.Message{
			Body:         []byte("msg-" + readAheadBody(i)),
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
		}
		require.NoError(t, storage.StoreMessage(queueName, msg))
	}
}

func readAheadBody(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
