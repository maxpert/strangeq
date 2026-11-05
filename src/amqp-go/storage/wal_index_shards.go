package storage

import (
	"sync"
	"sync/atomic"
)

// THE SHARDED WAL INDEX.
//
// WHY THE INDEX IS SHARDED.
//
// The WAL's physical index used to be ONE pair of maps behind ONE RWMutex:
//
//	qw.offsetIndex  map[recordKey]*segmentRecord    (the newest copy per key)
//	qw.recordCopies map[recordKey][]*segmentRecord   (every physical copy)
//
// An acknowledgement MUTATES that index: a settled record leaves it EAGERLY,
// so the index holds only the records that are NOT yet acknowledged and stays
// bounded by the in-flight set (see wal_index_bound_test.go). That is the
// memory fix, and it is not negotiable -- without it the index grew without
// bound and the F4 peak RSS was 2,236 MB.
//
// But "the acknowledgement path mutates the index" means the acknowledgement
// path needs the WRITE lock, and at F2 (1 KiB bodies, 10 producers, 10
// consumers, prefetch 100, publisher confirms) the broker acknowledges about
// 100,000 messages per SECOND. ONE global exclusive lock cannot carry that:
//
//	mutex profile, F2, this build:
//	  total contention                                     391.66 s
//	  WALManager.Acknowledge -> RWMutex.Unlock            337.24 s  (96.06%)
//	  commitBatch -> RWMutex.Unlock (waiting)               13.62 s  ( 3.88%)
//	  hotCache.Get / Put / Delete (their own mutex)          ~6.1 s  ( 1.6%)
//
// The committer queues behind the acknowledgers for the same lock, so the
// group-commit cycle stretched from 5.9 ms to 9.9 ms even though the batch
// itself was healthy (970 messages per fsync) and the fsync took only 3.93 ms.
//
// THE FIX. The index is split into shards and the exclusive section of the
// acknowledgement path is PER SHARD, so the contention is spread over the
// shards instead of concentrated on one lock.
//
// THE SHARD IS CHOSEN BY THE DELIVERY TAG ALONE.
//
// Not by the queue name: every physical copy of one (queue, tag) must land in
// the SAME shard, because a key's copy list is ONE slice and a list that
// spanned two shards could not be updated under one lock. The broker's tags
// are globally unique (see broker.PackTag), so the tag alone identifies the
// key's shard.
//
// THE LOCK ORDER.
//
//	fileMutex -> (one shard lock at a time) -> oldFilesMutex -> oldFileCacheMutex
//
// bitmapMutex stays a leaf-only lock: it is never held while a shard lock
// is held. oldFileCacheMutex stays a leaf too.
//
// The committer and the acknowledgement path take ONE shard lock at a time
// (lock, work, release, next shard), so they can never hold two shards at
// once and therefore can never deadlock against each other or against the
// cold paths that take every shard. The cold paths -- the reclaim sweep and
// the compaction's install -- take EVERY shard's write lock in ASCENDING
// shard order, which is the sharded equivalent of the old single global
// write lock: they must exclude every reader and every writer at once
// (the reclaim unlinks a file, so no reader may be inside a ReadAt of it;
// the compaction renames a file over its original, so the index must not
// change under it).

// walIndexShardCount is the number of shards the WAL's physical index is
// split into. It is a power of two so the shard is a mask, not a modulo.
const walIndexShardCount = 64

// walIndexShard is one shard of the physical index: the newest-copy map, the
// complete-copy map, and the lock that guards them.
//
// THE LOCK IS PER SHARD, and it is the ONLY lock the acknowledgement path
// takes. That is the whole point of the shard: an acknowledgement of tag T
// excludes the acknowledgements of the tags in the other 63 shards, and
// nothing else.
type walIndexShard struct {
	mu     sync.RWMutex
	offset map[recordKey]*segmentRecord
	copies map[recordKey][]*segmentRecord

	// settledPending is the queue of records THIS shard's ack path has
	// settled but whose per-file inventory entry has not yet been deleted.
	//
	// THE ACK PATH MAY NOT DELETE THE INVENTORY ENTRY ITSELF: a file's
	// inventory is shared by every shard, so two acknowledgements of two tags
	// in the same file would mutate the same map under two different shard
	// locks (a data race, and a map corruption). It only QUEUES the record,
	// under the shard lock it already holds. The committer then drains the
	// queue under fileMutex, taking ONE shard at a time, and deletes each
	// entry from the ACTIVE file's inventory (see drainSettledInventoryLocked).
	//
	// Guarded by mu.
	settledPending []*segmentRecord

	// pending is a lock-free flag: true while settledPending is non-empty.
	// The committer reads it WITHOUT taking the shard lock (64 atomic loads
	// per batch), so an idle shard costs no lock; the drain re-checks the
	// queue under the lock and clears the flag only when it is empty, so a
	// concurrent settle can never be lost.
	pending atomic.Bool

	// highWater is the largest len(copies)+len(offset) this shard has held
	// since its maps were last rebuilt. A Go map never returns bucket memory
	// on delete, so this is the proxy for the map's retained bucket capacity:
	// after the unacked set's ramp-up peak, the maps keep the peak's buckets
	// even though the live count is tiny. settleRecordLocked rebuilds the
	// maps (S1c) when the live count falls below a quarter of this, and the
	// rebuild resets it.
	//
	// Guarded by mu.
	highWater int
}

// walIndexShardOf returns the shard a delivery tag belongs to.
//
// THE HASH IS THE splitmix64 FINALIZER. The broker packs a delivery tag as
// (queue ordinal << 44) | per-queue sequence, so the tags of one queue are
// dense and the tags of different queues differ only in the high bits. A raw
// tag % N would therefore crowd most of the traffic into a narrow band of
// shards; the finalizer spreads ANY input evenly over the shards.
func walIndexShardOf(tag uint64) int {
	x := tag + 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	x ^= x >> 31
	return int(x & uint64(walIndexShardCount-1))
}

// shardForTag returns the shard that holds the given delivery tag.
func (qw *QueueWAL) shardForTag(tag uint64) *walIndexShard {
	return &qw.indexShards[walIndexShardOf(tag)]
}

// shardForKey returns the shard that holds the given key.
func (qw *QueueWAL) shardForKey(key recordKey) *walIndexShard {
	return &qw.indexShards[walIndexShardOf(key.deliveryTag)]
}

// initIndexShards prepares every shard's maps. It is called once, when the
// WAL is built; the maps are lazily created as well, so a QueueWAL that is
// assembled by hand (the tests do) still works.
func (qw *QueueWAL) initIndexShards() {
	for i := range qw.indexShards {
		sh := &qw.indexShards[i]
		if sh.offset == nil {
			sh.offset = make(map[recordKey]*segmentRecord)
		}
		if sh.copies == nil {
			sh.copies = make(map[recordKey][]*segmentRecord)
		}
	}
}

// lockAllIndexShards takes EVERY shard's write lock, in ASCENDING shard
// order. unlockAllIndexShards releases them in the reverse order.
//
// THIS IS THE SHARDED EQUIVALENT OF THE OLD SINGLE GLOBAL INDEX LOCK, and it
// is used only by the two cold paths that must exclude every reader and every
// writer at once:
//
//   - the reclaim sweep, which unlinks a sealed file: no reader may be inside
//     a ReadAt of that file, and no acknowledgement may be settling a copy
//     out of it;
//   - the compaction's install, which renames the rewritten file over the
//     original: the index must not change while the bytes are being swapped.
//
// THE ORDER IS FIXED. A path that takes one shard at a time never holds two
// shards at once, so it cannot deadlock against this one; and two paths that
// take every shard take them in the same ascending order.
func (qw *QueueWAL) lockAllIndexShards() {
	for i := range qw.indexShards {
		qw.indexShards[i].mu.Lock()
	}
}

func (qw *QueueWAL) unlockAllIndexShards() {
	for i := len(qw.indexShards) - 1; i >= 0; i-- {
		qw.indexShards[i].mu.Unlock()
	}
}

// indexRecord publishes r into the shard its delivery tag belongs to,
// taking that shard's write lock.
//
// It is the committer's per-record step: the committer holds fileMutex (so
// the commits are serialized with each other) and it takes ONE shard at a
// time, so it never holds two shards at once.
func (qw *QueueWAL) indexRecord(r *segmentRecord) {
	if r == nil {
		return
	}
	sh := qw.shardForTag(r.deliveryTag)
	sh.mu.Lock()
	sh.indexRecordLocked(r)
	sh.mu.Unlock()
}

// indexRecordLocked maintains both the newest-copy lookup and the complete
// physical-copy inventory of ONE SHARD. Caller holds that shard's write lock.
//
// THE COPY LIST IS A SLICE, NOT A MAP.
//
// The copy list used to be map[recordKey]map[recordPos]*segmentRecord: a Go
// MAP PER PUBLISHED MESSAGE. The common case is exactly ONE physical copy per
// (queue, tag), so every message allocated a whole map header plus a bucket
// array (~256 bytes and two allocations) to hold a single pointer, and threw
// it away when the message was acknowledged. That was a large share of the
// allocation churn the GC was busy with on the hot publish path.
//
// It is now map[recordKey][]*segmentRecord -- a slice of the physical copies
// -- the same shape the legacy segment tier already uses
// (segmentInventory.byTag map[uint64][]*segmentRecord). One allocation for
// the one-element slice instead of two for a map.
//
// THE SEMANTICS ARE UNCHANGED:
//
//   - the list holds EVERY physical copy of the key;
//   - a POSITION is published at most once: re-publishing a position (the
//     compaction can move a copy onto a position the key already holds)
//     REPLACES that entry instead of duplicating it -- which is what the old
//     map's copies[r.pos] = r did;
//   - the newest-copy view still points at the newest copy.
func (s *walIndexShard) indexRecordLocked(r *segmentRecord) {
	if r == nil {
		return
	}
	key := recordKey{queueName: r.queueName, deliveryTag: r.deliveryTag}
	if s.copies == nil {
		s.copies = make(map[recordKey][]*segmentRecord)
	}
	copies := s.copies[key]
	replaced := false
	for i := range copies {
		if copies[i].pos == r.pos {
			copies[i] = r
			replaced = true
			break
		}
	}
	if !replaced {
		copies = append(copies, r)
	}
	s.copies[key] = copies
	if s.offset == nil {
		s.offset = make(map[recordKey]*segmentRecord)
	}
	if current := s.offset[key]; current == nil || recordPositionNewer(r.pos, current.pos) {
		s.offset[key] = r
	}
	if n := len(s.copies) + len(s.offset); n > s.highWater {
		s.highWater = n
	}
}

// removeRecordCopyLocked removes the copy at the given position from a copy
// list and returns the shortened list.
//
// The list is the key's OWN backing array, so the removal is in place: the
// survivors above the removed entry move down into it, and the returned slice
// is the same array with a shorter length. A position that is not in the
// list leaves it untouched.
//
// IT IS THE EXACT INVERSE of indexRecordLocked's "a position is published at
// most once" rule, and it is the same in-place removal the legacy segment
// tier's dropFromByTag already does.
func removeRecordCopyLocked(copies []*segmentRecord, pos recordPos) []*segmentRecord {
	for i := range copies {
		if copies[i].pos == pos {
			return append(copies[:i], copies[i+1:]...)
		}
	}
	return copies
}

// unindexRecordLocked removes one physical copy and recomputes the newest view
// only when that copy was the published winner. Caller holds the shard's write
// lock.
//
// IT RE-POINTS THE KEY AT THE NEWEST REMAINING COPY, and that is CORRECT
// here: a whole FILE went away, so the remaining copies are the only ones
// left and the newest of them is the right answer. The settled removal (see
// unindexSettledRecordLocked) does NOT do this, and must not.
func (s *walIndexShard) unindexRecordLocked(r *segmentRecord) {
	if r == nil {
		return
	}
	key := recordKey{queueName: r.queueName, deliveryTag: r.deliveryTag}
	copies := s.copies[key]
	if copies != nil {
		copies = removeRecordCopyLocked(copies, r.pos)
		if len(copies) == 0 {
			delete(s.copies, key)
		} else {
			s.copies[key] = copies
		}
	}
	if s.offset[key] != r {
		return
	}
	delete(s.offset, key)
	var newest *segmentRecord
	for _, candidate := range copies {
		if newest == nil || recordPositionNewer(candidate.pos, newest.pos) {
			newest = candidate
		}
	}
	if newest != nil {
		s.offset[key] = newest
	}
}

// replaceRecordLocked swaps a compacted physical copy under the same lock
// protocol as index creation and removal. Caller holds the shard's write
// lock.
//
// The old and the new copy carry the SAME delivery tag -- the compaction
// moves a record, it does not re-tag it -- so they are in the same shard.
func (s *walIndexShard) replaceRecordLocked(old, new *segmentRecord) {
	s.unindexRecordLocked(old)
	s.indexRecordLocked(new)
}

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

// unindexSettledRecordLocked removes one SETTLED physical copy from the
// shard's index. Caller holds the shard's write lock.
//
// IT NEVER RE-POINTS THE KEY AT AN OLDER COPY.
//
// unindexRecordLocked does re-point it, and that is CORRECT there: a whole
// FILE went away, so the remaining copies are the only ones left and the
// newest of them is the right answer. It is WRONG here: the copy was
// SETTLED, so the client acknowledged the tag, and re-pointing the key at an
// older physical copy of the same tag would REVIVE a message the client
// already acknowledged -- the delivery path would hand it back.
//
// The older copy may stay in the copy list (so a later acknowledgement of the
// same tag still settles it), but it must never become the newest-copy
// winner.
func (s *walIndexShard) unindexSettledRecordLocked(r *segmentRecord) {
	if r == nil {
		return
	}
	key := recordKey{queueName: r.queueName, deliveryTag: r.deliveryTag}
	copies := s.copies[key]
	if copies != nil {
		copies = removeRecordCopyLocked(copies, r.pos)
		if len(copies) == 0 {
			delete(s.copies, key)
		} else {
			s.copies[key] = copies
		}
	}
	if s.offset[key] == r {
		delete(s.offset, key)
	}
}

// settleRecordLocked settles one physical copy and removes it from the
// shard's index. Caller holds the shard's write lock.
//
// It reports whether THIS call settled the copy (i.e. whether it won the
// compare-and-swap). Only the winner removes anything, so acknowledging the
// same copy twice is a no-op the second time.
//
// THE INVENTORY ENTRY IS QUEUED, NOT DELETED, HERE.
//
// The record's per-file inventory entry (inv.records) cannot be deleted on
// this path: a file's inventory is PER FILE and SHARED BY EVERY SHARD -- a
// file's records carry tags of many different shards, so two acknowledgements
// of two tags in the same file would mutate the same map under two different
// shard locks (a data race, and a map corruption at the same time). So the
// settled record is APPENDED to this shard's settledPending queue, under the
// shard lock already held, and the committer drains the queue later under
// fileMutex, ONE shard at a time (see drainSettledInventoryLocked).
//
// THE ACCOUNTING DOES NOT DEPEND ON THE DELETION: inv.unacked and inv.deadBytes
// are updated by markAckDetailed above and are ATOMIC, and they are what the
// reclaim predicate (unacked == 0) and the compaction ratio
// (deadBytes / durableBytes) are computed from. The committer's delete is
// gated on the record being acknowledged, so a record can never leave the
// inventory unacked.
//
// The compaction already skips a record whose acked flag is set, so a settled
// record that has not yet been drained is still handled correctly.
func (s *walIndexShard) settleRecordLocked(r *segmentRecord) bool {
	if r == nil {
		return false
	}
	settled, _ := r.markAckDetailed()
	if !settled {
		return false
	}
	s.unindexSettledRecordLocked(r)
	// S1c: a Go map never returns bucket memory on delete, so once the
	// unacked set has drained the maps still hold the ramp-up's buckets.
	// Rebuild them when the live count falls well below the peak.
	s.shrinkMapsIfIdleLocked()
	// Queue the settled record for the inventory drain. The append happens
	// under the shard lock this call already holds, so no new lock and no new
	// atomic on the ack path; pending is a plain flag the committer polls.
	s.settledPending = append(s.settledPending, r)
	s.pending.Store(true)
	return true
}

// shrinkMapsIfIdleLocked rebuilds the shard's two maps when the live count has
// fallen below a quarter of the high-water mark, dropping the bucket arrays
// the deleted keys left behind. Caller holds the shard's write lock.
//
// THE REBUILD IS AMORTISED. The threshold is a quarter, so a rebuild can only
// fire again after the maps have grown back to four times the post-rebuild
// size; the aggregate cost is O(1) per settled record. A small floor keeps a
// tiny shard from reallocating for no reason.
func (s *walIndexShard) shrinkMapsIfIdleLocked() {
	if s.highWater < 64 {
		return
	}
	if len(s.copies)+len(s.offset) < s.highWater/4 {
		s.rebuildMapsLocked()
	}
}

// rebuildMapsLocked reallocates the shard's two maps at their CURRENT size and
// copies the live entries across. Caller holds the shard's write lock.
//
// THE SWAP IS SAFE BECAUSE EVERY ACCESS IS UNDER THAT LOCK: the ack path, the
// committer's index step, the reclaim sweep and the compaction's install all
// take this shard's write lock (the last two take every shard), and the read
// path takes the read lock. No caller holds a reference to the map itself
// across the lock -- they index it by key -- so replacing it is invisible to
// them. highWater is reset to the new live count, so the next rebuild is a
// further four-fold drop.
func (s *walIndexShard) rebuildMapsLocked() {
	copies := make(map[recordKey][]*segmentRecord, len(s.copies))
	for k, v := range s.copies {
		copies[k] = v
	}
	s.copies = copies
	offset := make(map[recordKey]*segmentRecord, len(s.offset))
	for k, v := range s.offset {
		offset[k] = v
	}
	s.offset = offset
	s.highWater = len(s.copies) + len(s.offset)
}

// indexShardHighWaterForTest sums every shard's retained-capacity high-water
// mark (see walIndexShard.highWater). TEST-ONLY: the S1c test uses it to prove
// the shard maps are rebuilt once the unacked set has drained.
func (qw *QueueWAL) indexShardHighWaterForTest() (total int) {
	for i := range qw.indexShards {
		sh := &qw.indexShards[i]
		sh.mu.RLock()
		total += sh.highWater
		sh.mu.RUnlock()
	}
	return total
}

// drainSettledInventory runs the inventory drain under fileMutex. It is the
// cleanup loop's own acquisition: fileMutex -> one shard at a time, sequential
// and released before tryReclaimOldFiles takes its shard -> oldFilesMutex
// order, so it can never deadlock against the reclaim or the compaction.
func (qw *QueueWAL) drainSettledInventory() {
	qw.fileMutex.Lock()
	qw.drainSettledInventoryLocked()
	qw.fileMutex.Unlock()
}

// drainSettledInventoryLocked deletes the settled records the ack path has
// queued from the ACTIVE file's inventory. Caller holds fileMutex.
//
// THE LOCK ORDER IS fileMutex -> (ONE shard lock at a time). It never holds
// two shard locks at once, so it cannot deadlock against the paths that take
// every shard (the reclaim sweep, the compaction's install), which take them
// in ascending order.
//
// ONLY THE ACTIVE FILE'S INVENTORY IS TOUCHED. A sealed file's inventory is
// read WITHOUT a lock by the compaction (which snapshots its two maps), so
// mutating it here would be a data race. A settled record whose file has
// since rolled is skipped: its inventory is dropped whole by the reclaim or
// rewritten by the compaction.
//
// The delete is GATED on the record being acknowledged, so a record can never
// leave the inventory unacked even if a future caller queues it early.
func (qw *QueueWAL) drainSettledInventoryLocked() {
	inv := qw.currentInv
	if inv == nil {
		return
	}
	for i := range qw.indexShards {
		sh := &qw.indexShards[i]
		if !sh.pending.Load() {
			continue
		}
		sh.mu.Lock()
		pending := sh.settledPending
		if len(pending) > 0 {
			// The drain holds ONE shard lock at this instant; the hook reports
			// it so a test can prove the "one shard at a time" rule (T2).
			held := qw.drainHeldShards.Add(1)
			walInventoryDrainFire(int(held))
			for _, r := range pending {
				if r == nil || r.inv != inv || !r.acked.Load() {
					continue
				}
				if cur := inv.records[r.pos.position]; cur == r {
					delete(inv.records, r.pos.position)
					qw.inventoryDrains.Add(1)
				}
			}
			qw.drainHeldShards.Add(-1)
			// Drop the record pointers so the queue's backing array does not
			// keep the drained records alive; keep the array for reuse.
			for j := range pending {
				pending[j] = nil
			}
			sh.settledPending = pending[:0]
		}
		sh.pending.Store(false)
		sh.mu.Unlock()
	}
}
