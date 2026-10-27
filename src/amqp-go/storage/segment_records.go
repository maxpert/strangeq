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

	// bodyBlock is the shared body block this record references, captured
	// when the record was added to its inventory.
	//
	// IT IS A POINTER AND NOT A LOOKUP BECAUSE THE ACKNOWLEDGEMENT PATH
	// MUST NOT TOUCH THE INVENTORY. An inventory is PER FILE and is shared
	// by every index shard: a file's records carry tags of many different
	// shards, so an acknowledgement that looked the block up in the
	// inventory's map under its own shard lock would race the committer's
	// map write (a data race, and a map corruption at the same time).
	//
	// Capturing the block once, while the inventory's maps are still
	// private to their builder, leaves the acknowledgement path touching
	// only ATOMIC state -- exactly what it is allowed to touch.
	bodyBlock *segmentBodyBlock
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

	// uninterpretableBytes is the number of bytes in the file that this build
	// CANNOT INTERPRET: a message record whose payload sets the reserved
	// continuation flag, or whose body kind this build does not know. Such a
	// record is durable data a NEWER build could read, so it is never
	// garbage.
	//
	// IT IS COUNTED SEPARATELY FROM `unacked` ON PURPOSE. `unacked` is the
	// reclaim predicate's input ("unacked == 0" means "the file holds nothing
	// live"), and the uninterpretable bytes DO keep a file out of the reclaim
	// sweep. But they are not a RECORD, so the COMPACTION -- which copies the
	// records the inventory holds -- would drop them. Counting them in a
	// dedicated counter is what lets the compaction SKIP such a file
	// explicitly, instead of relying on a coincidence between two accounting
	// rules.
	//
	// See wal_compaction.go (compactOldFile) and wal_manager.go
	// (tryReclaimOldFiles): both consult it.
	uninterpretableBytes atomic.Int64
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
	if r.bodyRef >= 0 {
		if block := inv.blocks[r.bodyRef]; block != nil {
			// The block is captured HERE, while the inventory's maps are
			// still private to their builder, so the acknowledgement path
			// never has to look the map up (see segmentRecord.bodyBlock).
			r.bodyBlock = block
			if r.committed {
				block.refs.Add(1)
			}
		}
	}
	if r.committed {
		inv.unacked.Add(r.bytes)
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

// markAckDetailed settles this physical copy exactly once. A shared body
// becomes dead only when its final committed reference is acknowledged.
//
// It reports TWO things:
//
//   - settled: whether THIS call won the compare-and-swap, i.e. whether it is
//     the call that settled the copy. A second acknowledgement of the same
//     copy is a no-op and reports false.
//   - blockFreed: whether the copy's body block lost its LAST reference, so
//     the caller may drop the block from the inventory.
//
// THE ACCOUNTING IS EXACTLY WHAT IT WAS. The copy's bytes become dead bytes,
// the inventory's unacknowledged total drops by them, and a shared body whose
// final reference was acknowledged has its bytes counted as dead too. What
// the eager settling adds is only WHAT HAPPENS TO THE INDEX afterwards (see
// walIndexShard.settleRecordLocked).
//
// IT TOUCHES NO MAP OF THE INVENTORY. The inventory is per file and shared by
// every shard, so the acknowledgement path may only read ATOMIC state from
// it.
func (r *segmentRecord) markAckDetailed() (settled bool, blockFreed bool) {
	if r == nil || !r.acked.CompareAndSwap(false, true) {
		return false, false
	}
	inv := r.inv
	if inv == nil {
		return true, false
	}
	inv.deadBytes.Add(r.bytes)
	inv.unacked.Add(-r.bytes)
	// THE BLOCK IS THE ONE CAPTURED AT PUBLICATION TIME (see
	// segmentRecord.bodyBlock), NOT A LOOKUP IN THE INVENTORY: the
	// acknowledgement path must not touch the inventory's maps, which are
	// shared by every shard.
	if block := r.bodyBlock; block != nil && block.refs.Add(-1) == 0 {
		inv.deadBytes.Add(block.bytes)
		blockFreed = true
	}
	return true, blockFreed
}

// markAck settles this physical copy exactly once. It is the boolean form of
// markAckDetailed, for the callers that do not need to know whether the body
// block was freed.
func (r *segmentRecord) markAck() bool {
	settled, _ := r.markAckDetailed()
	return settled
}

func recordPositionNewer(a, b recordPos) bool {
	return a.segmentID > b.segmentID || (a.segmentID == b.segmentID && a.position > b.position)
}

// THE INDEX MAINTENANCE ITSELF LIVES IN wal_index_shards.go, next to the
// shard it operates on: the newest-copy map, the complete-copy map and their
// lock are PER SHARD now, so "the index" is no longer one pair of maps behind
// one lock but 64 pairs behind 64 locks.
//
// THE SHARD IS CHOSEN BY THE DELIVERY TAG ALONE, so every physical copy of
// one (queue, tag) lands in the SAME shard and a copy list never spans two
// shards.

// ---------------------------------------------------------------------------
// THE EAGER SETTLING.
//
// The WAL index used to hold a record for EVERY MESSAGE EVER PUBLISHED: a
// record left the index only when its whole WAL FILE was reclaimed, and the
// reclaim sweep ran every five minutes while a 64 MiB WAL file at F4 rolls
// about 28 times per SECOND. So the maps grew without bound -- 861,000 live
// records (~260 MB) after a 30 s window, all of them ALREADY ACKNOWLEDGED --
// and Go's GC (which targets twice the live heap) turned that into ~520 MB of
// extra RSS.
//
// The fix is to DROP A SETTLED RECORD EAGERLY, so the index holds the records
// that are NOT YET ACKNOWLEDGED and is therefore bounded by the in-flight set.
// The removal itself is the shard's unindexSettledRecordLocked, in
// wal_index_shards.go.
//
// WHAT DOES NOT CHANGE:
//
//   - the acknowledgement STATE lives in the queue-scoped roaring64 bitmap
//     (ackBitmaps), not in the dropped record, so it survives;
//   - the durable bytes are not deleted: the file still holds them until the
//     reclaim sweep unlinks it or the compaction rewrites it;
//   - the durability contract is untouched: a publisher confirm is still
//     sent only after the record's fdatasync.
// ---------------------------------------------------------------------------

// settleInventoryRecordLocked removes an ALREADY SETTLED record from its
// inventory. It is the compaction's own use: the copy it carried over was
// acknowledged while the rewrite was being written, so the copy is NOT in the
// rewritten file and must not be in the rewritten file's inventory either.
//
// IT TOUCHES ONLY THE INVENTORY, never the index: the record was never
// published to the index (the eager settling already removed the copy it
// replaced), so there is nothing to unindex.
//
// The inventory it mutates is the compaction's OWN new inventory, which is
// not published to anybody until the rename has been installed, so no other
// goroutine can be looking at it.
func settleInventoryRecordLocked(r *segmentRecord, blockFreed bool) {
	if r == nil {
		return
	}
	inv := r.inv
	if inv == nil {
		return
	}
	delete(inv.records, r.pos.position)
	if blockFreed && r.bodyRef >= 0 {
		delete(inv.blocks, r.bodyRef)
	}
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
