package clusterraft

import (
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	maxJournalRecentEntries    = 256
	maxJournalRecentBytes      = 16 << 20
	maxJournalRecentEntryBytes = 1 << 20
)

type recentJournalEntry struct {
	ref   journalEntryRef
	entry *pb.Entry
	bytes int
}

// A journal's offsets are unique for the life of its node. Keep insertion
// order in a bounded ring, with no history for entries that have been evicted.
type journalRecentEntries struct {
	mu      sync.RWMutex
	slots   [maxJournalRecentEntries]recentJournalEntry
	offsets map[int64]int
	first   int
	bytes   int
}

func (c *journalRecentEntries) lookup(ref journalEntryRef) *pb.Entry {
	if ref.size > maxJournalRecentEntryBytes {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	slot, found := c.offsets[ref.offset]
	if !found {
		return nil
	}
	record := c.slots[slot]
	if record.ref != ref || !journalRecentEntryMatches(ref, record.entry) {
		return nil
	}
	return record.entry
}

// remember receives the original, immutable Ready entries only after
// publication of their synced locations. Reads never add decoded copies.
func (c *journalRecentEntries) remember(refs []journalEntryRef, entries []*pb.Entry) {
	if len(refs) == 0 || len(refs) != len(entries) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, ref := range refs {
		entry := entries[i]
		if ref.size > maxJournalRecentEntryBytes || !journalRecentEntryMatches(ref, entry) {
			continue
		}
		bodyBytes := cap(entry.GetData())
		unknownBytes := cap(entry.ProtoReflect().GetUnknown())
		if bodyBytes > maxJournalRecentEntryBytes || unknownBytes > maxJournalRecentEntryBytes-bodyBytes {
			continue
		}
		size := max(int(ref.size), bodyBytes+unknownBytes)
		if _, exists := c.offsets[ref.offset]; exists {
			continue
		}
		for len(c.offsets) == maxJournalRecentEntries || c.bytes+size > maxJournalRecentBytes {
			old := c.slots[c.first]
			delete(c.offsets, old.ref.offset)
			c.slots[c.first] = recentJournalEntry{}
			c.first = (c.first + 1) % maxJournalRecentEntries
			c.bytes -= old.bytes
		}
		if c.offsets == nil {
			c.offsets = make(map[int64]int, maxJournalRecentEntries)
		}
		slot := (c.first + len(c.offsets)) % maxJournalRecentEntries
		c.slots[slot] = recentJournalEntry{ref: ref, entry: entry, bytes: size}
		c.offsets[ref.offset] = slot
		c.bytes += size
	}
}

func journalRecentEntryMatches(ref journalEntryRef, entry *pb.Entry) bool {
	return entry != nil && entry.GetIndex() == ref.index && entry.GetTerm() == ref.term &&
		len(entry.GetData()) == int(ref.dataSize)
}

func (c *journalRecentEntries) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.slots[:])
	c.offsets = nil
	c.first, c.bytes = 0, 0
}
