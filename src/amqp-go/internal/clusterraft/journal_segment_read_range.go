package clusterraft

import "fmt"

// Bound metadata for issued reads, including callers waiting for a disk slot.
// The existing owner pin still protects lifetime; this ledger protects ranges.
const maxJournalReadRanges = 1024

type journalReadRange struct {
	start, end int64
}

// The caller already holds an owner-lifetime pin. Register the entire range
// before waiting for a disk slot or touching any filename, not one file at a
// time. Existing pinned callers may complete while reader Close is waiting.
func (s *journalSegmentStorage) pinReadRange(start, end int64) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pinReadRangeLocked(start, end)
}

func (s *journalSegmentStorage) pinReadRangeLocked(start, end int64) (func(), error) {
	if start < 0 || end <= start {
		return nil, fmt.Errorf("%w: invalid issued journal read range", errJournalCorrupt)
	}
	if start < s.manifest.extents[0].base {
		return nil, fmt.Errorf("%w: journal read precedes retained history", errJournalBusy)
	}
	if end > s.end {
		return nil, fmt.Errorf("%w: issued journal read exceeds visible bytes", errJournalCorrupt)
	}
	if journalExtentOverlaps(s.retiring, start, end) {
		return nil, fmt.Errorf("%w: journal read overlaps retiring segment", errJournalBusy)
	}
	return s.trackReadRangeLocked(start, end)
}

func (s *journalSegmentStorage) trackReadRangeLocked(start, end int64) (func(), error) {
	if len(s.readRanges) >= maxJournalReadRanges {
		return nil, fmt.Errorf("%w: issued journal read range limit", errJournalBusy)
	}
	if s.readRanges == nil {
		s.readRanges = make(map[*journalReadRange]struct{})
	}
	span := &journalReadRange{start: start, end: end}
	s.readRanges[span] = struct{}{}
	return func() {
		s.mu.Lock()
		delete(s.readRanges, span)
		s.mu.Unlock()
	}, nil
}

// Captures are ordered by when they copy queue state under Group.mu. An
// ACK hands the body's lease to the newest capture that could have seen it.
// Completed captures release those leases in order, even if a later capture
// finishes first. Captures in other groups never retain the body's segment.
type journalCapture struct {
	prev, next *journalCapture
	retired    []journalRetirementRange
	done       bool
}

type journalCaptureQueue struct {
	first, last *journalCapture
}

func (g *Group) pinJournalCapture() (func(), error) {
	store := g.store
	reader, ok := store.node.reader.(*journalSegmentReader)
	if !ok {
		return func() {}, nil
	}
	return reader.store.pinCapture(store.group)
}

func (s *journalSegmentStorage) pinCapture(group uint64) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errJournalClosed
	}
	if s.captures == nil {
		s.captures = make(map[uint64]*journalCaptureQueue)
	}
	queue := s.captures[group]
	capture := &journalCapture{}
	if queue == nil {
		queue = &journalCaptureQueue{first: capture}
		s.captures[group] = queue
	} else {
		capture.prev = queue.last
		queue.last.next = capture
	}
	queue.last = capture
	s.reads.Add(1)
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if capture.done {
			return
		}
		capture.done = true
		if capture.prev == nil {
			for _, span := range capture.retired {
				s.releaseSourceLocked(span)
			}
			queue.first = capture.next
		} else {
			capture.prev.retired = append(capture.prev.retired, capture.retired...)
			capture.prev.next = capture.next
		}
		capture.retired = nil
		if capture.next != nil {
			capture.next.prev = capture.prev
		} else {
			queue.last = capture.prev
		}
		if queue.first == nil {
			delete(s.captures, group)
		}
		s.reads.Done()
	}, nil
}
