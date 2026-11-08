package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncTestMessage builds a durable message for offset tag.
func syncTestMessage(tag uint64) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte("body"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
}

// durabilityMetricsSpy records every spec 06 §3.3 durability fault through the
// optional-capability interfaces. It implements the full StorageMetrics surface
// so it can be bound to either the WAL or the segment tier.
type durabilityMetricsSpy struct {
	writeErrors      atomic.Int64
	fsyncErrors      atomic.Int64
	rollErrors       atomic.Int64
	compactionErrors atomic.Int64
	reclaimFsyncErrs atomic.Int64
	segRollErrors    atomic.Int64
	segFsyncErrors   atomic.Int64
}

func (m *durabilityMetricsSpy) UpdateDiskMetrics(float64, float64)            {}
func (m *durabilityMetricsSpy) RecordHotCacheDeliveryHit(string)              {}
func (m *durabilityMetricsSpy) RecordHotCacheDeliveryColdRead(string)         {}
func (m *durabilityMetricsSpy) RecordHotCacheDeliveryPread(string, int)       {}
func (m *durabilityMetricsSpy) UpdateWALSize(float64)                         {}
func (m *durabilityMetricsSpy) RecordWALWrite()                               {}
func (m *durabilityMetricsSpy) RecordWALFsync(float64)                        {}
func (m *durabilityMetricsSpy) RecordWALWriteError()                          { m.writeErrors.Add(1) }
func (m *durabilityMetricsSpy) RecordWALFsyncError()                          { m.fsyncErrors.Add(1) }
func (m *durabilityMetricsSpy) RecordWALRollError()                           { m.rollErrors.Add(1) }
func (m *durabilityMetricsSpy) RecordWALCompactionError()                     { m.compactionErrors.Add(1) }
func (m *durabilityMetricsSpy) RecordWALReclaimFsyncError()                   { m.reclaimFsyncErrs.Add(1) }
func (m *durabilityMetricsSpy) UpdateSegmentMetrics(string, float64, float64) {}
func (m *durabilityMetricsSpy) RecordSegmentCompaction()                      {}
func (m *durabilityMetricsSpy) RecordSegmentCompactionFailure()               {}
func (m *durabilityMetricsSpy) RecordSegmentReadError()                       {}
func (m *durabilityMetricsSpy) RecordSegmentRollError()                       { m.segRollErrors.Add(1) }
func (m *durabilityMetricsSpy) RecordSegmentFsyncError()                      { m.segFsyncErrors.Add(1) }

// TestWALFsyncError_DistinctFromWriteError (spec 06 §5 test #6): an fsync
// failure and a write failure land on DISJOINT counters.
func TestWALFsyncError_DistinctFromWriteError(t *testing.T) {
	t.Run("fsync error", func(t *testing.T) {
		spy := &durabilityMetricsSpy{}
		restore := SetWALGroupCommitFsyncForTest(func(*os.File) error {
			return errors.New("injected group-commit fsync fault")
		})
		defer restore()

		wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		defer wm.Close()
		wm.SetMetrics(spy)

		werr := wm.Write("q", syncTestMessage(1), 1)
		require.Error(t, werr, "a failed fsync must fail the batch")
		assert.Equal(t, int64(1), spy.fsyncErrors.Load(), "fsync error must be counted on the fsync series")
		assert.Equal(t, int64(0), spy.writeErrors.Load(), "an fsync error must NOT count as a write error")
	})

	t.Run("write error", func(t *testing.T) {
		spy := &durabilityMetricsSpy{}
		restore := SetWALWriteVecForTest(func(f *os.File, parts [][]byte) (int, error) {
			return 0, errors.New("injected write fault")
		})
		defer restore()

		wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		defer wm.Close()
		wm.SetMetrics(spy)

		werr := wm.Write("q", syncTestMessage(1), 1)
		require.Error(t, werr, "a failed write must fail the batch")
		assert.Equal(t, int64(1), spy.writeErrors.Load(), "write error must be counted on the write series")
		assert.Equal(t, int64(0), spy.fsyncErrors.Load(), "a write error must NOT count as an fsync error")
	})
}

// TestWALRollErrorCounter (spec 06 §5 test, roll/open seam): a failed roll
// increments the roll-error series.
func TestWALRollErrorCounter(t *testing.T) {
	spy := &durabilityMetricsSpy{}
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	defer wm.Close()
	wm.SetMetrics(spy)
	qw := wm.sharedWAL

	restore := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if strings.HasPrefix(name, dir) {
			return nil, syscall.ENFILE
		}
		return os.OpenFile(name, flag, perm)
	})
	defer restore()

	qw.fileMutex.Lock()
	rerr := qw.tryRollLocked(true)
	qw.fileMutex.Unlock()

	require.Error(t, rerr, "the roll must fail while the open hook faults")
	assert.Equal(t, int64(1), spy.rollErrors.Load())
	assert.Equal(t, int64(0), spy.writeErrors.Load(), "a roll failure is not a write failure")
}

// TestWALReclaimFsyncErrorCounter (spec 06 §5 test #8): a failed directory
// fsync after reclamation increments the dedicated series.
func TestWALReclaimFsyncErrorCounter(t *testing.T) {
	spy := &durabilityMetricsSpy{}
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	defer wm.Close()
	wm.SetMetrics(spy)
	qw := wm.sharedWAL

	// A stale file for the unlink pass to remove (so unlinkedAny becomes true
	// and the directory barrier runs).
	orphan := filepath.Join(qw.dataDir, "00000000000000000009.wal")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0644))

	restore := SetSyncDirForTest(func(string) error {
		return errors.New("injected reclaim dir fsync fault")
	})
	defer restore()

	qw.reclaimUnlinkPass([]string{orphan})
	assert.Equal(t, int64(1), spy.reclaimFsyncErrs.Load())
}

// TestWALCompactionErrorCounter (spec 06 §5 test #7): a compaction fsync fault
// increments the compaction-error series and leaves the bytes untouched.
func TestWALCompactionErrorCounter(t *testing.T) {
	spy := &durabilityMetricsSpy{}
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	defer wm.Close()
	wm.SetMetrics(spy)
	qw := wm.sharedWAL

	writeSegstoreReclaimRecord(t, wm, "live", 1, "live-body")
	writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("d", 4096))
	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("dead", 1)
	before := compactionReadFile(t, path)

	restore := SetWALCompactionFsyncForTest(func(*os.File) error {
		return errors.New("injected compaction fsync fault")
	})
	qw.tryCompactOldFiles()
	restore()

	assert.Equal(t, int64(1), spy.compactionErrors.Load())
	assert.Equal(t, before, compactionReadFile(t, path),
		"a compaction error must leave the file's bytes unchanged")
}

// TestSegmentRollErrorCounter (spec 06 §3.3): a failed segment create/open is
// counted on the segment roll series.
func TestSegmentRollErrorCounter(t *testing.T) {
	spy := &durabilityMetricsSpy{}
	sm, err := NewSegmentManager(t.TempDir())
	require.NoError(t, err)
	defer sm.Close()
	sm.SetMetrics(spy)

	restore := SetSegmentOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, syscall.EMFILE
	})
	defer restore()

	_, gerr := sm.getOrCreateQueueSegments("q")
	require.Error(t, gerr, "the queue's first segment cannot be opened while the hook faults")
	assert.Equal(t, int64(1), spy.segRollErrors.Load())
}

// TestSegmentFsyncErrorCounter (spec 06 §5 test #10): a failed segment seal
// fsync increments the segment fsync-error series, and the seal/roll fails.
func TestSegmentFsyncErrorCounter(t *testing.T) {
	spy := &durabilityMetricsSpy{}
	cfg := DefaultSegmentConfig()
	cfg.SegmentSize = 1 // every write immediately rolls -> seal -> fsync
	sm, err := NewSegmentManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	defer sm.Close()
	sm.SetMetrics(spy)

	restore := SetSegmentFsyncForTest(func(*os.File) error {
		return errors.New("injected segment fsync fault")
	})
	defer restore()

	werr := writeLegacyForTest(sm, "q", syncTestMessage(1), 1)
	require.Error(t, werr, "the roll's seal fsync must fail while the hook faults")
	assert.Equal(t, int64(1), spy.segFsyncErrors.Load())
	assert.Equal(t, int64(0), spy.segRollErrors.Load(), "a seal-fsync failure is not a roll/open failure")
}
