package clusterraft

import (
	"bytes"
	"errors"
	"io"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalProcessAppendNeedsOnlyKernelWrite(t *testing.T) {
	dir := t.TempDir()
	var syncs atomic.Int64
	node, err := openSegmentedGroupNode(dir, true, false, journalServingExtentBytes, journalGroupMaxFileBytes, nil, journalSegmentHooks{
		syncFile: func(file *os.File) error { syncs.Add(1); return syncJournalFile(file) },
	})
	require.NoError(t, err)
	node.writer.durability = &journalDurability{}
	t.Cleanup(func() { _ = node.close() })
	group := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1},
		Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true, ProcessDurability: true,
	})
	require.NoError(t, group.Campaign(t.Context()))
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	before := syncs.Load()
	body := bytes.Repeat([]byte{0xa7}, 64<<10)
	result, err := group.Propose(t.Context(), body)
	require.NoError(t, err)
	require.Equal(t, append([]byte("applied "), body...), result)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	require.Equal(t, before, syncs.Load(), "ordinary isolated process append must complete without a device fence")
	reader := node.reader.(*journalSegmentReader)
	reader.store.mu.RLock()
	durable, completed := reader.store.durableEnd, reader.store.completedEnd
	reader.store.mu.RUnlock()
	require.Greater(t, completed, durable, "readable process frames must not advance physical durability")
	group.mu.Lock()
	node.recentEntries.clear()
	last := group.store.state.hard.GetCommit()
	entries, err := group.store.Entries(last, last+1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	source, err := group.store.commandSource(entries[0])
	require.NoError(t, err)
	retained, release, err := retainJournalSourceFixture(source)
	require.NoError(t, err, "large bodies must remain retainable before a device fence")
	group.mu.Unlock()
	defer release()
	actual := make([]byte, len(body))
	require.NoError(t, readJournalAt(retained, actual, 0))
	require.Equal(t, body, actual)
	// Copy the live kernel image before Group.Close or Node.Close can fence it.
	crashDir := t.TempDir()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	bodyCopies := 0
	for _, file := range files {
		if file.IsDir() || file.Name() == journalSegmentLockName {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, file.Name()))
		require.NoError(t, err)
		bodyCopies += bytes.Count(data, body)
		require.NoError(t, os.WriteFile(filepath.Join(crashDir, file.Name()), data, 0600))
	}
	require.Equal(t, 1, bodyCopies, "one physical journal body is sufficient for replay and retained reads")
	recovered, err := OpenNodeJournalWithOptions(crashDir, false, JournalOptions{ProcessDurability: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = recovered.Close() })
	restored, err := recovered.OpenGroup(Config{ID: 1, GroupID: 11, Dir: crashDir, Machine: &testMachine{}, TickInterval: -1, ProcessDurability: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.Equal(t, last, restored.Status().Commit, "kernel image must include commit-only metadata")
	beforeSnapshot, err := group.MachineSnapshot(t.Context())
	require.NoError(t, err)
	afterSnapshot, err := restored.MachineSnapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, beforeSnapshot, afterSnapshot)
	require.NoError(t, group.Snapshot(t.Context()))
	require.Greater(t, syncs.Load(), before, "snapshot publication must retain its physical fence")
	reader.store.mu.RLock()
	require.Equal(t, reader.store.durableEnd, reader.store.completedEnd)
	reader.store.mu.RUnlock()
	beforeClose := syncs.Load()
	_, err = group.Propose(t.Context(), []byte("after snapshot"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	require.Equal(t, beforeClose, syncs.Load())
	release()
	require.NoError(t, group.Close())
	require.NoError(t, node.close())
	require.Greater(t, syncs.Load(), beforeClose, "owner shutdown must fence dirty process frames")
}

// Gate the kernel write separately from the physical fence so a completion
// cannot be mistaken for the start of a write or a sync.
type processJournalGate struct {
	*gatedJournalFile
	gate    atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (f *processJournalGate) WriteAt(data []byte, offset int64) (int, error) {
	if f.gate.CompareAndSwap(true, false) {
		close(f.started)
		<-f.release
	}
	return f.gatedJournalFile.WriteAt(data, offset)
}

func enqueueProcessJournal(t *testing.T, w *journalWriter, body []byte) <-chan journalCompletion {
	t.Helper()
	result, err := w.enqueueEncodedMode(1, len(body), false, true, nil, func(dst []byte) error { copy(dst, body); return nil })
	require.NoError(t, err)
	return result
}

func TestJournalProcessPromotionRechecksQueuedWrites(t *testing.T) {
	base := &gatedJournalFile{syncStarted: make(chan struct{}, 8), syncRelease: make(chan error, 8)}
	file := &processJournalGate{gatedJournalFile: base, started: make(chan struct{}), release: make(chan struct{})}
	file.gate.Store(true)
	w, err := newJournalWriter(file, 0, 1, testJournalLimits())
	require.NoError(t, err)
	w.durability = &journalDurability{}
	t.Cleanup(func() { close(base.syncRelease); _ = w.close() })
	first := enqueueProcessJournal(t, w, []byte("held write"))
	select {
	case <-file.started:
	case <-time.After(5 * time.Second):
		t.Fatal("kernel write not reached")
	}
	select {
	case <-first:
		t.Fatal("process append completed before kernel write")
	default:
	}
	second := enqueueProcessJournal(t, w, []byte("queued before activation"))
	promoted := make(chan error, 1)
	go func() { promoted <- w.requireFullDurability() }()
	require.Eventually(t, w.durability.strict.Load, time.Second, time.Millisecond)
	close(file.release)
	require.NoError(t, awaitJournalCompletion(t, first).err)
	awaitJournalSync(t, base, second)
	select {
	case <-promoted:
		t.Fatal("activation returned before physical fence")
	default:
	}
	base.syncRelease <- nil
	require.NoError(t, awaitJournalCompletion(t, second).err)
	select {
	case err := <-promoted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("activation did not finish")
	}
	third := enqueueProcessJournal(t, w, []byte("after activation"))
	awaitJournalSync(t, base, third)
	base.syncRelease <- nil
	require.NoError(t, awaitJournalCompletion(t, third).err)
}

func TestJournalProcessPromotionFailureIsTerminal(t *testing.T) {
	w, file := openGatedJournal(t, testJournalLimits(), false)
	w.durability = &journalDurability{}
	require.NoError(t, awaitJournalCompletion(t, enqueueProcessJournal(t, w, []byte("kernel only"))).err)
	failure := errors.New("promotion fence failed")
	promoted := make(chan error, 1)
	go func() { promoted <- w.requireFullDurability() }()
	select {
	case <-file.syncStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("promotion did not fence")
	}
	file.syncRelease <- failure
	require.ErrorIs(t, <-promoted, failure)
	require.ErrorIs(t, w.failure(), failure)
	require.True(t, w.durability.strict.Load())
	require.ErrorIs(t, w.requireFullDurability(), failure)
}

func TestJournalProcessShortWriteNeverCompletesOrPromotes(t *testing.T) {
	w, file := openGatedJournal(t, testJournalLimits(), true)
	w.durability = &journalDurability{}
	require.ErrorIs(t, awaitJournalCompletion(t, enqueueProcessJournal(t, w, []byte("short"))).err, io.ErrShortWrite)
	require.ErrorIs(t, w.requireFullDurability(), io.ErrShortWrite)
	select {
	case <-file.syncStarted:
		t.Fatal("short process frame was fenced")
	default:
	}
}

func TestJournalProcessEligibilityKeepsMetadataStrict(t *testing.T) {
	node, err := OpenNodeJournalWithOptions(t.TempDir(), true, JournalOptions{ProcessDurability: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	group := openTestJournalGroup(t, node.store, Config{ID: 1, GroupID: 11, Dir: t.TempDir(), Peers: []uint64{1}, Machine: &testMachine{}, ProcessDurability: true, TickInterval: -1})
	require.NoError(t, group.Campaign(t.Context()))
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	group.mu.Lock()
	defer group.mu.Unlock()
	store := group.store
	normal := func() readyWrite {
		last, err := store.LastIndex()
		require.NoError(t, err)
		write, err := store.planReady(raft.Ready{Entries: []*pb.Entry{{Index: new(last + 1), Term: store.state.hard.Term, Type: pb.EntryNormal.Enum()}}})
		require.NoError(t, err)
		return write
	}
	require.True(t, store.processReadyEligible(normal()))
	store.processDurability = false
	require.False(t, store.processReadyEligible(normal()), "catalog remains strict")
	store.processDurability = true
	term := normal()
	term.hard = &pb.HardState{Term: new(store.state.hard.GetTerm() + 1), Commit: store.state.hard.Commit}
	require.False(t, store.processReadyEligible(term))
	vote := normal()
	vote.hard = &pb.HardState{Term: store.state.hard.Term, Vote: new(uint64(2)), Commit: store.state.hard.Commit}
	require.False(t, store.processReadyEligible(vote))
	membership := normal()
	membership.ready.Entries[0].Type = pb.EntryConfChangeV2.Enum()
	require.False(t, store.processReadyEligible(membership))
	replacement := normal()
	replacement.replacesLog = true
	require.False(t, store.processReadyEligible(replacement))
}

func TestJournalProcessPairPromotionFencesEveryStreamAfterFailure(t *testing.T) {
	var pair NodeJournalPair
	files := make([]*gatedJournalFile, 2)
	mode := &journalDurability{}
	for i := range pair.streams {
		w, file := openGatedJournal(t, testJournalLimits(), false)
		w.durability = mode
		pair.streams[i] = &NodeJournal{store: &journalGroupNode{writer: w}}
		files[i] = file
		require.NoError(t, awaitJournalCompletion(t, enqueueProcessJournal(t, w, []byte("unfenced"))).err)
	}
	done := make(chan error, 1)
	go func() { done <- pair.RequireFullDurability() }()
	select {
	case <-files[0].syncStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("stream 0 not fenced")
	}
	require.True(t, pair.streams[1].store.writer.durability.strict.Load(), "both streams change mode before first fence")
	failure := errors.New("stream zero failed")
	files[0].syncRelease <- failure
	select {
	case <-files[1].syncStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("failed stream skipped peer fence")
	}
	select {
	case <-done:
		t.Fatal("promotion skipped stream 1 fence")
	default:
	}
	files[1].syncRelease <- nil
	require.ErrorIs(t, <-done, failure)
}
