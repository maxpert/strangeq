package clusterraft

import (
	"errors"
	"fmt"
	"io"

	"go.etcd.io/raft/v3"
)

const MaxAssignmentJournalEntries = 1024
const MaxAssignmentJournalSpans = 32

var ErrAssignmentJournalRange = errors.New("invalid assignment journal command range")

// StopOnAssignmentBodyFailure fences a local shard after a verified body
// read fails. A retry must never obtain those bytes from this shard as leader.
// It returns true only for the caller that stopped the shard.
func (g *Group) StopOnAssignmentBodyFailure(cause error) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped() != nil {
		return false
	}
	g.stop(fmt.Errorf("read local assignment journal body: %w", cause))
	return true
}

type assignmentJournalSource struct {
	journalCommandReader
	index, term uint64
}

func (s *assignmentJournalSource) Retain() (io.ReaderAt, func(), error) {
	if source, ok := s.journalCommandReader.(*journalSegmentCommandSource); ok {
		return source.Retain()
	}
	return s, func() {}, nil
}

func (s *assignmentJournalSource) AssignmentJournalEntry() (index, term uint64, size int64) {
	return s.index, s.term, s.Size()
}

// AssignmentJournalCoverage reports an advisory interval in the published,
// synced vector. The flat journal keeps its physical records until Close.
func (g *Group) AssignmentJournalCoverage() (term, first, last uint64) {
	if !g.mu.TryLock() {
		return 0, 0, 0
	}
	defer g.mu.Unlock()
	store, err := g.assignmentJournal()
	if err != nil || len(store.state.entries) == 0 {
		return 0, 0, 0
	}
	entries := store.state.entries
	end := len(entries) - 1
	for start := end; ; start-- {
		if start == 0 || end-start+1 == MaxAssignmentJournalEntries || entries[start-1].term != entries[end].term {
			return entries[end].term, entries[start].index, entries[end].index
		}
	}
}

type AssignmentJournalSpan struct {
	Index, Term    uint64
	Offset, Length uint32
}

// TryPinAssignmentJournal validates every requested ref before taking one
// lifetime pin. The returned reader may read the immutable records off-lock.
func (g *Group) TryPinAssignmentJournal(spans []AssignmentJournalSpan) (read func(int, []byte) error, release func(), err error) {
	if len(spans) == 0 || len(spans) > MaxAssignmentJournalSpans {
		return nil, nil, ErrAssignmentJournalRange
	}
	if !g.mu.TryLock() {
		return nil, nil, ErrBusy
	}
	defer g.mu.Unlock()
	store, err := g.assignmentJournal()
	if err != nil {
		return nil, nil, err
	}
	base := store.state.snapshot.metadata.GetIndex()
	var offsets [MaxAssignmentJournalSpans]int64
	var start, end int64
	keySize := uint32(len(key{}))
	for i, span := range spans {
		if span.Index <= base {
			return nil, nil, raft.ErrCompacted
		}
		if span.Index-base > uint64(len(store.state.entries)) {
			return nil, nil, raft.ErrUnavailable
		}
		ref := store.state.entries[span.Index-base-1]
		if ref.term != span.Term {
			return nil, nil, raft.ErrUnavailable
		}
		if ref.index != span.Index || ref.offset < 0 || ref.dataSize < keySize ||
			ref.dataOffset > ref.size || ref.dataSize > ref.size-ref.dataOffset {
			return nil, nil, ErrAssignmentJournalRange
		}
		commandSize := ref.dataSize - keySize
		if span.Length == 0 || span.Offset > commandSize || span.Length > commandSize-span.Offset {
			return nil, nil, ErrAssignmentJournalRange
		}
		offsets[i] = ref.offset + int64(ref.dataOffset) + int64(keySize) + int64(span.Offset)
		if i == 0 || start > offsets[i] {
			start = offsets[i]
		}
		end = max(end, offsets[i]+int64(span.Length))
	}
	reader := store.node.reader
	unpin := func() {}
	if segmented, ok := reader.(*journalSegmentReader); ok {
		unpin, err = segmented.store.pinReadRange(start, end)
		if err != nil {
			return nil, nil, ErrBusy
		}
	}
	g.replyWorkers.Add(1)
	read = func(i int, dst []byte) error {
		if i < 0 || i >= len(spans) || uint64(len(dst)) != uint64(spans[i].Length) {
			return ErrAssignmentJournalRange
		}
		n, err := reader.ReadAt(dst, offsets[i])
		if n == len(dst) && (err == nil || errors.Is(err, io.EOF)) {
			return nil
		}
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return read, func() {
		unpin()
		g.replyWorkers.Done()
	}, nil
}

func (g *Group) assignmentJournal() (*journalGroupStore, error) {
	if !g.cfg.AssignmentReplicaBodies {
		return nil, raft.ErrUnavailable
	}
	store, ok := g.store.(*journalGroupStore)
	if !ok {
		return nil, raft.ErrUnavailable
	}
	if g.terminal != nil {
		return nil, ErrBusy
	}
	select {
	case <-g.storeFailed:
		return nil, errors.Join(ErrAssignmentJournalRange, store.failureCause())
	default:
		return store, nil
	}
}
