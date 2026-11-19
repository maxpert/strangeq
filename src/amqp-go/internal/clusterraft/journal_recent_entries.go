package clusterraft

import (
	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	maxJournalRecentEntries    = 256
	maxJournalRecentBytes      = 16 << 20
	maxJournalRecentEntryBytes = 1 << 20
)

type recentJournalEntry struct {
	ref           journalEntryRef
	entry         *pb.Entry
	bytes         int
	bodyChecksums []clusterproposal.BodyChecksum
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
	c.rememberChecksummed(refs, entries, nil)
}

func (c *journalRecentEntries) rememberChecksummed(refs []journalEntryRef, entries []*pb.Entry, checksums [][]clusterproposal.BodyChecksum) {
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
		var bodyChecksums []clusterproposal.BodyChecksum
		if len(checksums) == len(entries) && len(checksums[i]) <= clusterproposal.MaxBodyChecksumRanges {
			bodyChecksums = checksums[i]
			metadataBytes := cap(bodyChecksums) * clusterproposal.BodyChecksumBytes
			if metadataBytes > maxJournalRecentEntryBytes-size {
				bodyChecksums = nil // Preserve the ordinary entry cache on overflow.
			} else {
				size += metadataBytes
			}
		}
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
		c.slots[slot] = recentJournalEntry{ref: ref, entry: entry, bytes: size, bodyChecksums: bodyChecksums}
		c.offsets[ref.offset] = slot
		c.bytes += size
	}
}

// Only the exact immutable Ready allocation that was prepared may consume the
// proof, once. Copies, cold decodes, retries and mismatching journal refs miss.
// Scalar sidecars occupy the existing cache byte budget until consumed/evicted.
func (c *journalRecentEntries) takeBodyChecksums(ref journalEntryRef, entry *pb.Entry) []clusterproposal.BodyChecksum {
	c.mu.Lock()
	defer c.mu.Unlock()
	slot, found := c.offsets[ref.offset]
	if !found {
		return nil
	}
	record := &c.slots[slot]
	if record.ref != ref || !journalRecentEntryMatches(ref, entry) || record.entry == nil ||
		len(entry.Data) < clusterproposal.PrefixBytes || len(record.entry.Data) != len(entry.Data) ||
		&record.entry.Data[0] != &entry.Data[0] {
		return nil
	}
	checksums := record.bodyChecksums
	record.bodyChecksums = nil
	metadataBytes := cap(checksums) * clusterproposal.BodyChecksumBytes
	record.bytes -= metadataBytes
	c.bytes -= metadataBytes
	return checksums
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
