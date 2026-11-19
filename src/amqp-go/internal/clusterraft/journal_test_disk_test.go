package clusterraft

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errTestJournalWrite = errors.New("injected segmented journal write failure")
var testJournalDisks sync.Map

type testJournalWriteGate struct {
	entered  chan struct{}
	released chan struct{}
	once     sync.Once
}

type testJournalDisk struct {
	node *journalGroupNode
	gate atomic.Pointer[testJournalWriteGate]
	fail atomic.Bool
}

func (d *testJournalDisk) writeAt(file *os.File, data []byte, at int64) (int, error) {
	if gate := d.gate.Load(); gate != nil {
		gate.once.Do(func() { close(gate.entered) })
		<-gate.released
	}
	if d.fail.Load() {
		return 0, errTestJournalWrite
	}
	return file.WriteAt(data, at)
}

func testJournalDiskFor(group *Group) *testJournalDisk {
	disk, ok := testJournalDisks.Load(group)
	if !ok {
		panic("group has no test journal disk")
	}
	return disk.(*testJournalDisk)
}

func testJournalEnd(group *Group) int64 {
	reader := group.store.node.reader.(*journalSegmentReader)
	reader.store.mu.RLock()
	defer reader.store.mu.RUnlock()
	return reader.store.durableEnd
}

func corruptTestJournalEntry(t *testing.T, group *Group, index uint64) {
	t.Helper()
	store := group.store
	ref := store.state.entries[index-store.state.snapshot.metadata.GetIndex()-1]
	// Force the cold read path, then alter the real extent bytes after the
	// append fence. This must fail the journal checksum before application.
	store.node.recentEntries.clear()
	reader := store.node.reader.(*journalSegmentReader)
	reader.store.mu.RLock()
	extent := reader.store.manifest.extents[0]
	reader.store.mu.RUnlock()
	file, err := os.OpenFile(reader.store.path(extent.id), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0}, ref.offset-extent.base)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
