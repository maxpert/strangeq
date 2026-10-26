package storage

import "sync/atomic"

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
	records      map[int64]*segmentRecord
	blocks       map[int64]*segmentBodyBlock
	valid        bool
	durableBytes int64
	unacked      atomic.Int64
	deadBytes    atomic.Int64
}

func newSegmentInventory(durableBytes int64) *segmentInventory {
	return &segmentInventory{
		records:      make(map[int64]*segmentRecord),
		blocks:       make(map[int64]*segmentBodyBlock),
		valid:        true,
		durableBytes: durableBytes,
	}
}

// addRecord publishes a committed live record into an inventory that is still
// being built. BodyBlock reference counts are derived from these records.
func (inv *segmentInventory) addRecord(r *segmentRecord) {
	if inv.records == nil {
		inv.records = make(map[int64]*segmentRecord)
	}
	r.inv = inv
	inv.records[r.pos.position] = r
	if r.committed {
		inv.unacked.Add(r.bytes)
		if r.bodyRef >= 0 {
			if block := inv.blocks[r.bodyRef]; block != nil {
				block.refs.Add(1)
			}
		}
	}
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
