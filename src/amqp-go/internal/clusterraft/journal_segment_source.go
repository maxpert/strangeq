package clusterraft

import (
	"fmt"
	"io"
	"os"
	"sort"
)

// Bound both lease handles and distinct ledger ranges. This is private local
// admission, not a replicated queue-capacity outcome or a total RSS bound.
const maxJournalSourceLeases = maxJournalOpEntries

// A command starts borrowed, with the same lifetime as the existing command
// reader. No ownership is registered until an explicit Retain succeeds.
type journalRetirementRange struct {
	group      uint64
	start, end int64
}

type journalSegmentCommandSource struct {
	*io.SectionReader
	owner *journalSegmentStorage
	span  journalRetirementRange
}

func (source *journalSegmentCommandSource) Retain() (io.ReaderAt, func(), error) {
	lease, err := source.owner.retainSource(source.span, nil, false)
	if err != nil {
		return nil, nil, err
	}
	return lease, lease.retire, nil
}

type journalRetainedSource struct {
	owner    *journalSegmentStorage
	span     journalRetirementRange
	released bool // Protected by owner.mu, including parent-to-child acquisition.
}

func (source *journalRetainedSource) Retain() (io.ReaderAt, func(), error) {
	lease, err := source.owner.retainSource(source.span, source, false)
	if err != nil {
		return nil, nil, err
	}
	return lease, lease.release, nil
}

// An issued read takes independent temporary ownership. Releasing the outer
// handle cannot remove its range or let owner close finish during disk I/O.
// Existing owners may finish reads while Close waits; new Retain calls cannot
// extend a closing owner's lifetime. Idle owners never occupy read slots.
func (source *journalRetainedSource) ReadAt(data []byte, offset int64) (int, error) {
	lease, err := source.owner.retainSource(source.span, source, true)
	if err != nil {
		return 0, err
	}
	defer lease.release()
	size := lease.span.end - lease.span.start
	if offset < 0 || offset >= size {
		return 0, io.EOF
	}
	count := int(min(int64(len(data)), size-offset))
	s := lease.owner
	s.mu.RLock()
	manifest, end := s.manifest, s.durableEnd
	s.mu.RUnlock()
	n, err := s.readAtView(manifest, end, data[:count], lease.span.start+offset, (*os.File).ReadAt)
	if err == nil && count < len(data) {
		err = io.EOF
	}
	return n, err
}

func (s *journalSegmentStorage) retainSource(span journalRetirementRange, parent *journalRetainedSource, reading bool) (*journalRetainedSource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if parent != nil && (parent.owner != s || parent.span != span || parent.released) {
		return nil, errJournalClosed
	}
	if reading && parent == nil {
		return nil, errJournalClosed
	}
	if s.closed && !reading {
		return nil, errJournalClosed
	}
	if s.recoveryErr != nil && !reading {
		return nil, s.recoveryErr
	}
	if s.retainedSourceOwners >= maxJournalSourceLeases {
		return nil, fmt.Errorf("%w: retained journal source ownership limit", errJournalTooLarge)
	}
	// Retain also works before the first checkpoint. Its range must already
	// lie in the synced append prefix and wholly inside one selected extent.
	if span.group == 0 || span.start < 0 || span.end <= span.start || span.end > s.durableEnd ||
		len(s.manifest.extents) == 0 {
		return nil, fmt.Errorf("%w: invalid retained command range", errJournalCorrupt)
	}
	extents := s.manifest.extents
	at := sort.Search(len(extents), func(i int) bool { return extents[i].base > span.start }) - 1
	if at < 0 {
		return nil, fmt.Errorf("%w: retained command precedes selected history", errJournalCorrupt)
	}
	end := s.durableEnd
	if extent := extents[at]; extent.sealedBytes != 0 {
		end = extent.base + extent.sealedBytes
	}
	if span.end > end {
		return nil, fmt.Errorf("%w: retained command crosses an extent", errJournalCorrupt)
	}
	if journalExtentOverlaps(s.retiring, span.start, span.end) {
		return nil, fmt.Errorf("%w: retained source overlaps retiring segment", errJournalBusy)
	}
	if s.retainedSources == nil {
		s.retainedSources = make(map[journalRetirementRange]uint64)
	}
	s.retainedSources[span]++
	s.retainedSourceOwners++
	// An existing parent keeps this counter positive while Close is waiting.
	// New parents are prohibited once Close marks the owner closed.
	s.reads.Add(1)
	return &journalRetainedSource{owner: s, span: span}, nil
}

func (source *journalRetainedSource) retire() { source.releaseSource(true) }

func (source *journalRetainedSource) release() { source.releaseSource(false) }

func (source *journalRetainedSource) releaseSource(deferForCapture bool) {
	s := source.owner
	s.mu.Lock()
	defer s.mu.Unlock()
	if source.released {
		return
	}
	source.released = true
	if captures := s.captures[source.span.group]; deferForCapture && captures != nil {
		captures.last.retired = append(captures.last.retired, source.span)
		return
	}
	s.releaseSourceLocked(source.span)
}

func (s *journalSegmentStorage) releaseSourceLocked(span journalRetirementRange) {
	count := s.retainedSources[span]
	if count == 0 || s.retainedSourceOwners == 0 {
		panic("invalid retained journal source release")
	}
	if count == 1 {
		delete(s.retainedSources, span)
	} else {
		s.retainedSources[span] = count - 1
	}
	s.retainedSourceOwners--
	s.reads.Done()
}
