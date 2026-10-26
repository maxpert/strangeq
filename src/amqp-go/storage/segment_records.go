package storage

import (
	"sort"
	"sync/atomic"
)

// recordPos identifies one physical record, rather than a logical delivery tag.
// Compaction can therefore give a logical record a new position without
// confusing acknowledgements for the old and new copies.
type recordPos struct {
	segmentID uint64
	position  int64
}

type recordKey struct {
	queueName   string
	deliveryTag uint64
}

type segmentBodyBlock struct {
	bytes int64
	refs  atomic.Int64
}

type segmentRecord struct {
	pos            recordPos
	queueName      string
	deliveryTag    uint64
	bytes, bodyRef int64
	committed      bool
	inv            *segmentInventory
	acked          atomic.Bool
}

type segmentInventory struct {
	// records is the SOURCE OF TRUTH: every physical copy this inventory
	// holds, keyed by its file position.
	records map[int64]*segmentRecord

	// byTag indexes every physical copy of a delivery tag, so an ACK can be
	// resolved without walking `records`. It is a CACHE over `records`, not
	// a second source of truth: byTagCount is compared with len(records) and
	// an inventory whose two views disagree is resolved by the full scan of
	// `records` instead (see recordsForTag). An ACK must never be dropped,
	// so the scan is the fallback and the index is the fast path.
	byTag map[uint64][]*segmentRecord

	// byTagCount is the number of records currently held in byTag.
	byTagCount int

	// tagIndex says whether this inventory maintains the tag index at all.
	// It is OFF for the WAL tier's inventories: the index is a cache over
	// `records`, and a tier that does not resolve records by tag must not
	// pay the memory for it. The legacy segment tier - the one whose ACK
	// path resolves a tag per ACK - has it ON.
	tagIndex bool

	blocks map[int64]*segmentBodyBlock
	valid  bool

	// durableBytes is the file's durable size.
	//
	// IT IS ATOMIC because it is written by the WRITER (as each batch is
	// appended) and read by the COMPACTOR (which decides whether a rolled
	// file is more than half garbage) with no common lock. A file can roll
	// between the writer reading the current inventory and the writer
	// updating it, so the compactor can observe the very inventory the
	// writer is still updating. A plain int64 there is a data race, and the
	// ratio it feeds decides whether durable bytes are rewritten or dropped.
	durableBytes atomic.Int64
	unacked      atomic.Int64
	deadBytes    atomic.Int64
}

func newSegmentInventory(durableBytes int64) *segmentInventory {
	inv := &segmentInventory{
		records: make(map[int64]*segmentRecord),
		blocks:  make(map[int64]*segmentBodyBlock),
		valid:   true,
	}
	inv.durableBytes.Store(durableBytes)
	return inv
}

// newSegmentInventoryWithTagIndex builds a segment-tier inventory: one that
// maintains the tag index, so resolving a delivery tag does not walk every
// record. See segmentInventory.tagIndex.
func newSegmentInventoryWithTagIndex(durableBytes int64) *segmentInventory {
	inv := newSegmentInventory(durableBytes)
	inv.tagIndex = true
	inv.byTag = make(map[uint64][]*segmentRecord)
	return inv
}

// addRecord publishes a committed live record into an inventory that is still
// being built. BodyBlock reference counts are derived from these records.
func (inv *segmentInventory) addRecord(r *segmentRecord) {
	if inv.records == nil {
		inv.records = make(map[int64]*segmentRecord)
	}
	if inv.tagIndex && inv.byTag == nil {
		inv.byTag = make(map[uint64][]*segmentRecord)
	}
	r.inv = inv
	prev, existed := inv.records[r.pos.position]
	if existed && prev != r && inv.tagIndex {
		// The position is being reused. The previous copy must leave the tag
		// index, or the index and `records` would disagree about which copies
		// exist and the index could no longer be trusted.
		inv.dropFromByTag(prev)
	}
	inv.records[r.pos.position] = r
	if inv.tagIndex && (!existed || prev != r) {
		inv.byTag[r.deliveryTag] = append(inv.byTag[r.deliveryTag], r)
		inv.byTagCount++
	}
	if r.committed {
		inv.unacked.Add(r.bytes)
		if r.bodyRef >= 0 {
			if block := inv.blocks[r.bodyRef]; block != nil {
				block.refs.Add(1)
			}
		}
	}
}

// dropFromByTag removes one physical copy from the tag index. Caller holds no
// lock: an inventory is published to readers only once it is built, so the
// index is mutated while it is still private to its builder.
func (inv *segmentInventory) dropFromByTag(r *segmentRecord) {
	if inv == nil || r == nil || inv.byTag == nil {
		return
	}
	copies := inv.byTag[r.deliveryTag]
	for i, c := range copies {
		if c == r {
			copies = append(copies[:i], copies[i+1:]...)
			break
		}
	}
	if len(copies) == 0 {
		delete(inv.byTag, r.deliveryTag)
	} else {
		inv.byTag[r.deliveryTag] = copies
	}
	inv.byTagCount--
}

// tagIndexTrusted reports whether the tag index can be used as the answer to
// a lookup. It can be used only when the index and `records` hold the same
// number of records: a disagreeing inventory is one that is still being
// built, or one whose index was not maintained, and in that case the
// authoritative `records` map is scanned instead.
func (inv *segmentInventory) tagIndexTrusted() bool {
	return inv != nil && inv.tagIndex && inv.byTag != nil && inv.byTagCount == len(inv.records)
}

// recordsForTag returns every physical copy of one delivery tag.
//
// The fast path is the tag index: O(copies). The fallback is the full scan of
// `records`, which is O(records) but NEVER drops an ACK. The two are kept
// apart on purpose - `records` is authoritative and the index is only a cache
// over it.
func (inv *segmentInventory) recordsForTag(tag uint64) []*segmentRecord {
	if inv == nil {
		return nil
	}
	if inv.tagIndexTrusted() {
		copies := inv.byTag[tag]
		invScanObserve(len(copies))
		return copies
	}
	copies := make([]*segmentRecord, 0, 4)
	for _, r := range inv.records {
		if r.deliveryTag == tag {
			copies = append(copies, r)
		}
	}
	invScanObserve(len(inv.records))
	return copies
}

// tagsInBand returns every delivery tag in the inclusive band [minTag,
// maxTag] this inventory holds, sorted and unique.
//
// Like recordsForTag it prefers the tag index and falls back to scanning
// `records`, so a partially built inventory still yields the same band.
func (inv *segmentInventory) tagsInBand(minTag, maxTag uint64) []uint64 {
	if inv == nil {
		return nil
	}
	tags := make([]uint64, 0, 8)
	if inv.tagIndexTrusted() {
		visited := 0
		for tag := range inv.byTag {
			visited++
			if tag >= minTag && tag <= maxTag {
				tags = append(tags, tag)
			}
		}
		invScanObserve(visited)
	} else {
		seen := make(map[uint64]struct{}, 8)
		visited := 0
		for _, r := range inv.records {
			visited++
			if r.deliveryTag < minTag || r.deliveryTag > maxTag {
				continue
			}
			if _, dup := seen[r.deliveryTag]; dup {
				continue
			}
			seen[r.deliveryTag] = struct{}{}
			tags = append(tags, r.deliveryTag)
		}
		invScanObserve(visited)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags
}

func (inv *segmentInventory) addBlock(position, bytes int64) {
	if inv.blocks == nil {
		inv.blocks = make(map[int64]*segmentBodyBlock)
	}
	inv.blocks[position] = &segmentBodyBlock{bytes: bytes}
}

// markAck settles this physical copy exactly once. A shared body becomes dead
// only when its final committed reference is acknowledged.
func (r *segmentRecord) markAck() bool {
	if r == nil || !r.acked.CompareAndSwap(false, true) {
		return false
	}
	inv := r.inv
	if inv == nil {
		return true
	}
	inv.deadBytes.Add(r.bytes)
	inv.unacked.Add(-r.bytes)
	if r.bodyRef >= 0 {
		if block := inv.blocks[r.bodyRef]; block != nil && block.refs.Add(-1) == 0 {
			inv.deadBytes.Add(block.bytes)
		}
	}
	return true
}

func recordPositionNewer(a, b recordPos) bool {
	return a.segmentID > b.segmentID || (a.segmentID == b.segmentID && a.position > b.position)
}

// indexRecordLocked maintains both the newest-copy lookup and the complete
// physical-copy inventory. Caller holds offsetIndexMutex.Lock.
func (qw *QueueWAL) indexRecordLocked(r *segmentRecord) {
	if r == nil {
		return
	}
	key := recordKey{queueName: r.queueName, deliveryTag: r.deliveryTag}
	if qw.recordCopies == nil {
		qw.recordCopies = make(map[recordKey]map[recordPos]*segmentRecord)
	}
	copies := qw.recordCopies[key]
	if copies == nil {
		copies = make(map[recordPos]*segmentRecord)
		qw.recordCopies[key] = copies
	}
	copies[r.pos] = r
	if qw.offsetIndex == nil {
		qw.offsetIndex = make(map[recordKey]*segmentRecord)
	}
	if current := qw.offsetIndex[key]; current == nil || recordPositionNewer(r.pos, current.pos) {
		qw.offsetIndex[key] = r
	}
}

// unindexRecordLocked removes one physical copy and recomputes the newest view
// only when that copy was the published winner. Caller holds the write lock.
func (qw *QueueWAL) unindexRecordLocked(r *segmentRecord) {
	if r == nil {
		return
	}
	key := recordKey{queueName: r.queueName, deliveryTag: r.deliveryTag}
	copies := qw.recordCopies[key]
	if copies != nil {
		delete(copies, r.pos)
		if len(copies) == 0 {
			delete(qw.recordCopies, key)
		}
	}
	if qw.offsetIndex[key] != r {
		return
	}
	delete(qw.offsetIndex, key)
	var newest *segmentRecord
	for _, candidate := range copies {
		if newest == nil || recordPositionNewer(candidate.pos, newest.pos) {
			newest = candidate
		}
	}
	if newest != nil {
		qw.offsetIndex[key] = newest
	}
}

// replaceRecordLocked swaps a compacted physical copy under the same lock
// protocol as index creation and removal.
func (qw *QueueWAL) replaceRecordLocked(old, new *segmentRecord) {
	qw.unindexRecordLocked(old)
	qw.indexRecordLocked(new)
}

// ---------------------------------------------------------------------------
// TEST-ONLY observation point: how much of an inventory a lookup walked.
//
// The inventory lookups (recordsForTag, tagsInBand) are the ACK path's inner
// loop. Before the tag index existed, resolving ONE delivery tag walked EVERY
// record of the segment, under the segment's read lock, which blocked reads
// and compaction of that segment for as long as the walk took. At a 64 MiB
// segment that is ~200k records per ACK.
//
// The hook below makes that cost observable: it reports the number of
// inventory entries a lookup examined. It follows the pattern of
// fault_hooks.go - an atomic.Pointer to a function type, a wrapper that
// loads-and-calls the hook, and a SetXForTest setter that returns a restore
// function - so the production cost when unset is ONE atomic pointer load per
// lookup.
//
// TEST-ONLY: production code never calls the setter. Tests that install it
// must NOT be parallel (the hook is package-global).
// ---------------------------------------------------------------------------

// invScanHookFn observes one inventory lookup. visited is the number of
// inventory entries the lookup examined: the copies of one tag on the index
// path, or every record on the full-scan fallback.
type invScanHookFn func(visited int)

var invScanHook atomic.Pointer[invScanHookFn]

// invScanObserve reports one lookup's cost to the installed observer. A nil
// hook costs one atomic load.
func invScanObserve(visited int) {
	if p := invScanHook.Load(); p != nil {
		(*p)(visited)
	}
}

// SetSegmentInventoryScanObserverForTest installs fn as the observer of the
// inventory lookups the ACK and purge paths perform, and returns a restore
// function. TEST-ONLY: it lets a test assert that resolving one delivery tag
// does not walk the whole segment. Production never calls it.
func SetSegmentInventoryScanObserverForTest(fn func(visited int)) (restore func()) {
	prev := invScanHook.Load()
	f := invScanHookFn(fn)
	invScanHook.Store(&f)
	return func() { invScanHook.Store(prev) }
}
