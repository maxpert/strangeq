//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package clusterraft

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type pairAdmissionGate struct {
	*os.File
	enabled atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (f *pairAdmissionGate) Sync() error {
	if f.enabled.Load() {
		select {
		case f.started <- struct{}{}:
		default:
		}
		<-f.release
	}
	return f.File.Sync()
}

// This fixture compiles against the already-verified, independent-owner
// prototype. Expected RED there: the 65th aggregate reservation is accepted.
func TestNodeJournalPairPreservesOriginalAggregateRequestAdmission(t *testing.T) {
	var gates [2]*pairAdmissionGate
	for index := range gates {
		gates[index] = &pairAdmissionGate{started: make(chan struct{}, 4), release: make(chan struct{})}
		defer close(gates[index].release)
	}
	node, err := openNodeJournalPair(t.TempDir(), true, 1, func(index int, file *os.File) journalFile {
		gates[index].File = file
		return gates[index]
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	for _, gate := range gates {
		gate.enabled.Store(true)
	}
	for index := range gates {
		_, err := node.streams[index].store.writer.enqueue(uint64(index+1), []byte("held"), true)
		require.NoError(t, err)
		select {
		case <-gates[index].started:
		case <-time.After(5 * time.Second):
			t.Fatal("stream never reached its held active durability fence")
		}
	}
	// Both active copies are still reserved. Add 62 queued copies, split
	// evenly, so neither original per-stream limit would reject the next one.
	for request := range 62 {
		index := request % 2
		_, err := node.streams[index].store.writer.enqueue(uint64(index+1), []byte("queued"), true)
		require.NoError(t, err)
	}
	result, err := node.streams[1].store.writer.enqueue(2, []byte("excess"), true)
	require.ErrorIs(t, err, errJournalBusy, "two streams must retain the original 64-request node cap")
	require.Nil(t, result)
}

func TestNodeJournalPairRecoversSharedBudgetFromSyncedValidatedEnds(t *testing.T) {
	root := t.TempDir()
	node, err := OpenNodeJournalPairWithRangeSync(root, true, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	for id := uint64(1); id <= 2; id++ {
		group, err := node.OpenGroup(Config{
			ID: 1, GroupID: id, Dir: root, Peers: []uint64{1},
			Machine: &testMachine{}, TickInterval: -1,
		})
		require.NoError(t, err)
		require.NoError(t, group.Close())
	}
	require.NoError(t, node.Close())
	var paths [2]string
	for i, stream := range node.streams {
		paths[i] = stream.store.reader.(*journalSegmentReader).store.path(1)
	}
	var expected int64
	for _, path := range paths {
		info, err := os.Stat(path)
		require.NoError(t, err)
		expected += info.Size()
	}
	// This physically persisted incomplete frame must be removed before its
	// stream contributes an end to the reconstructed aggregate budget.
	file, err := os.OpenFile(paths[0], os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, writeErr := file.Write([]byte{0x53, 0x51})
	syncErr, closeErr := file.Sync(), file.Close()
	require.NoError(t, writeErr)
	require.NoError(t, syncErr)
	require.NoError(t, closeErr)
	reopened, err := OpenNodeJournalPairWithRangeSync(root, false, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	budget := reopened.streams[0].store.writer.budget
	require.Same(t, budget, reopened.streams[1].store.writer.budget)
	quota := reopened.streams[0].store.reader.(*journalSegmentReader).store.quota
	require.Same(t, quota, reopened.streams[1].store.reader.(*journalSegmentReader).store.quota)
	quota.mu.Lock()
	actual := quota.used
	quota.mu.Unlock()
	require.Equal(t, expected, actual)
	require.Equal(t, int64(8<<30), quota.limit)
	require.Equal(t, 128<<20, budget.maxPendingBytes)
	require.Equal(t, 64, budget.maxPendingRequests)
	var physical int64
	for _, path := range paths {
		info, err := os.Stat(path)
		require.NoError(t, err)
		physical += info.Size()
	}
	require.Equal(t, expected, physical)
	require.NoError(t, reopened.Close())
}
