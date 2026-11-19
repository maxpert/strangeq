package clusterqueue

import "sync"

const (
	maxHotPayloadBytes     = 16 << 20
	hotPayloadBuckets      = 8
	maxNodeHotPayloadBytes = 32 << 20
)

// HotPayloadCacheSet partitions one node's exact 32MiB payload allowance into
// eight independent 4MiB buckets. Selection is immutable and has no shared
// mutex; shards collide only when their IDs have the same remainder.
// This bounds live cache ownership, not total process RSS or Raft's own owners.
type HotPayloadCacheSet struct {
	buckets   [hotPayloadBuckets]HotPayloadCache
	pageSlots chan struct{}
}

func NewShardedHotPayloadCache() *HotPayloadCacheSet {
	cache := &HotPayloadCacheSet{pageSlots: make(chan struct{}, maxHotAssignmentPages)}
	for i := range cache.buckets {
		cache.buckets[i].limit = maxNodeHotPayloadBytes / hotPayloadBuckets
		cache.buckets[i].pageSlots = cache.pageSlots
	}
	return cache
}

func (c *HotPayloadCacheSet) ForShard(id uint64) *HotPayloadCache {
	if c == nil {
		return nil
	}
	return &c.buckets[id%hotPayloadBuckets]
}

// A hotPayloadEntry owns exactly one backing command allocation (or a private
// body copy for callers without provenance). A batch shares one entry and one
// charge. Eviction detaches lookup immediately; explicit body loans keep the
// data and its full charge until the last loan releases. Portable captures do
// not own slices. Every field is protected by the selected bucket's mutex.
type hotPayloadEntry struct {
	data       []byte
	charge     int
	bodies     int
	loans      int
	linked     bool
	prev, next *hotPayloadEntry
}

// hotCommand is borrowed only during apply. It is never retained by a journal
// body or its source. Once attempted, data is cleared and only the cache entry
// can retain that backing array beyond apply.
type hotCommand struct {
	data      []byte
	charge    int
	entry     *hotPayloadEntry
	attempted bool
}

func immutableHotCommand(data []byte, source any) *hotCommand {
	owned, ok := source.(interface{ ImmutableJournalCommandBacking() (prefix, capacity int) })
	if !ok {
		return nil
	}
	prefix, capacity := owned.ImmutableJournalCommandBacking()
	if prefix <= 0 || capacity <= prefix || cap(data) != capacity-prefix {
		return nil
	}
	return &hotCommand{data: data, charge: capacity}
}

// HotPayloadCache owns immutable payloads under a fixed byte cap. Copying into
// replies while holding mu keeps captures from pinning uncharged cache arrays.
// Snapshots always read and check the original physical journal source.
type HotPayloadCache struct {
	mu          sync.Mutex
	bytes       int
	limit       int
	pageSlots   chan struct{}
	first, last *hotPayloadEntry
}

func NewHotPayloadCache() *HotPayloadCache {
	return &HotPayloadCache{limit: maxHotPayloadBytes, pageSlots: make(chan struct{}, maxHotAssignmentPages)}
}

func (c *HotPayloadCache) admit(body *journalBody, borrowed []byte, command *hotCommand) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if command != nil {
		if !command.attempted {
			command.attempted = true
			if command.charge <= c.limit && hotCommandSpan(command.data, body.offset, borrowed) {
				command.entry = c.insertLocked(command.data, command.charge)
			}
			command.data = nil
		}
		entry := command.entry
		if entry == nil || !hotCommandSpan(entry.data, body.offset, borrowed) {
			return
		}
		body.hotPayload, body.hotStart = entry, int(body.offset)
		entry.bodies++
		return
	}
	if len(borrowed) > c.limit {
		return
	}
	// Uncertified readers keep the safe existing copy path. Reserve/evict
	// before allocation; borrowed input may be reused as soon as apply returns.
	if !c.evictForLocked(len(borrowed)) {
		return
	}
	c.bytes += len(borrowed)
	data := make([]byte, len(borrowed))
	copy(data, borrowed)
	entry := &hotPayloadEntry{data: data, charge: len(data), bodies: 1}
	c.linkLocked(entry)
	body.hotPayload = entry
}

// The CRC was computed over borrowed, so the certificate can authorize only
// that exact in-place subspan, not unrelated bytes with the same length.
func hotCommandSpan(command []byte, offset int64, borrowed []byte) bool {
	if len(borrowed) == 0 || offset < 0 || offset >= int64(len(command)) ||
		len(borrowed) > len(command)-int(offset) {
		return false
	}
	return &command[int(offset)] == &borrowed[0]
}

func (c *HotPayloadCache) insertLocked(data []byte, charge int) *hotPayloadEntry {
	if !c.evictForLocked(charge) {
		return nil
	}
	c.bytes += charge
	entry := &hotPayloadEntry{data: data, charge: charge}
	c.linkLocked(entry)
	return entry
}

func (c *HotPayloadCache) evictForLocked(charge int) bool {
	for c.bytes+charge > c.limit {
		if c.first == nil {
			// Slow writers may pin every byte. Admission never waits and
			// never releases their charge; this publish stays journal-backed.
			return false
		}
		c.removeLocked(c.first)
	}
	return true
}

func (c *HotPayloadCache) linkLocked(entry *hotPayloadEntry) {
	entry.linked = true
	entry.prev = c.last
	if c.last == nil {
		c.first = entry
	} else {
		c.last.next = entry
	}
	c.last = entry
}

func (c *HotPayloadCache) copyBody(body *journalBody, dst []byte) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := body.hotPayload
	if entry == nil || !entry.linked || entry.data == nil {
		return false
	}
	copy(dst, entry.data[body.hotStart:body.hotStart+body.size])
	return true
}

func (c *HotPayloadCache) remove(body *journalBody) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := body.hotPayload
	if entry == nil {
		return
	}
	body.hotPayload = nil
	entry.bodies--
	if entry.bodies == 0 {
		c.removeLocked(entry)
	}
}

func (c *HotPayloadCache) removeLocked(entry *hotPayloadEntry) {
	if entry.linked {
		if entry.prev == nil {
			c.first = entry.next
		} else {
			entry.prev.next = entry.next
		}
		if entry.next == nil {
			c.last = entry.prev
		} else {
			entry.next.prev = entry.prev
		}
		entry.prev, entry.next = nil, nil
		entry.linked = false
	}
	if entry.loans == 0 {
		c.freeLocked(entry)
	}
}

func (c *HotPayloadCache) freeLocked(entry *hotPayloadEntry) {
	if entry.data != nil {
		entry.data = nil
		c.bytes -= entry.charge
	}
}
