package storage

import (
	"runtime"
	"testing"
)

// THE SHARD MAPS MUST NOT GROW UNDER STEADY-STATE CHURN.
//
// The F1 fsync:off row holds its queue at the depth high-water mark: ~52k
// unacknowledged messages, constant, while ~136k keys per second enter the
// index (commit) and ~136k leave it (ack). The S1c rebuild fires only when the
// live count falls below a quarter of its high-water mark, which never
// happens at a constant backlog -- yet a Go map under insert/delete churn at a
// constant live count keeps growing: deletes leave tombstones, an insert that
// finds no free slot grows the table, and the table is never given back. In
// isolation (64 shards, 53,000 live keys, FIFO churn) the two shard maps grow
// from 6.8 MB after the fill to 25.7 MB after ~1.4 M inserts: 3.8x the live
// set's own footprint, every byte of it doubled again by the GC goal.
//
// The fix rebuilds a shard's maps after a bounded number of inserts relative
// to its live count, so the retained capacity stays within a constant factor
// of the live set no matter how many keys have passed through.

func churnShardHeap(t *testing.T, live, rounds int) (fill, after uint64) {
	t.Helper()
	var shards [walIndexShardCount]walIndexShard
	recs := make([]segmentRecord, live)
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	index := func(r *segmentRecord, tag uint64) {
		*r = segmentRecord{
			pos:         recordPos{segmentID: 1, position: int64(tag) * 128},
			queueName:   "churn-q",
			deliveryTag: tag,
			bytes:       128,
			bodyRef:     -1,
			committed:   true,
		}
		sh := &shards[walIndexShardOf(tag)]
		sh.mu.Lock()
		sh.indexRecordLocked(r)
		sh.mu.Unlock()
	}
	drain := func() {
		for i := range shards {
			sh := &shards[i]
			for j := range sh.settledPending {
				sh.settledPending[j] = nil
			}
			sh.settledPending = sh.settledPending[:0]
		}
	}

	base := heap()
	var next uint64 = 1
	for i := 0; i < live; i++ {
		index(&recs[i], next)
		next++
	}
	fill = heap() - base

	// FIFO churn at a constant live count: ack the oldest, commit a new one.
	for round := 0; round < rounds; round++ {
		for i := 0; i < live; i++ {
			r := &recs[i]
			sh := &shards[walIndexShardOf(r.deliveryTag)]
			sh.mu.Lock()
			if !sh.settleRecordLocked(r) {
				sh.mu.Unlock()
				t.Fatalf("tag %d did not settle", r.deliveryTag)
			}
			sh.mu.Unlock()
			index(r, next)
			next++
		}
		drain()
	}
	after = heap() - base

	// The live set itself must be intact: exactly `live` keys in each view.
	n := 0
	for i := range shards {
		n += len(shards[i].offset)
		if len(shards[i].offset) != len(shards[i].copies) {
			t.Fatalf("shard %d: offset %d keys, copies %d keys", i, len(shards[i].offset), len(shards[i].copies))
		}
	}
	if n != live {
		t.Fatalf("index holds %d keys after churn, want %d", n, live)
	}
	runtime.KeepAlive(&shards)
	runtime.KeepAlive(recs)
	return fill, after
}

func TestWALIndexShards_SteadyChurnDoesNotRetainBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("drives ~1.6M index inserts")
	}
	const live = 53000
	fill, after := churnShardHeap(t, live, 30)
	t.Logf("index heap: %.1f MB after the fill, %.1f MB after %d inserts of FIFO churn (%.2fx)",
		float64(fill)/1e6, float64(after)/1e6, live*30, float64(after)/float64(fill))
	// A rebuilt map that then absorbs churn settles at ~2x its compact size
	// (one doubling to make room for tombstones). Unbounded growth is ~3.8x.
	if float64(after) > 2.5*float64(fill) {
		t.Fatalf("the shard maps grew from %.1f MB to %.1f MB at a CONSTANT live count of %d: "+
			"they retain the buckets the churn's tombstones forced them to grow into",
			float64(fill)/1e6, float64(after)/1e6, live)
	}
}

// The churn rebuild must not fire on a shard that is still growing toward its
// live set (the fill itself), or every commit of a ramp would copy the maps.
func TestWALIndexShards_ChurnRebuildIsAmortised(t *testing.T) {
	var sh walIndexShard
	recs := make([]segmentRecord, 4096)
	for i := range recs {
		recs[i] = segmentRecord{pos: recordPos{segmentID: 1, position: int64(i)}, queueName: "q",
			deliveryTag: uint64(i + 1), bytes: 1, bodyRef: -1, committed: true}
		sh.indexRecordLocked(&recs[i])
	}
	if sh.churnRebuilds != 0 {
		t.Fatalf("a pure fill (no deletes) rebuilt the maps %d times", sh.churnRebuilds)
	}
	// Steady churn of 64x the live count rebuilds a bounded number of times:
	// at most one rebuild per walIndexChurnRebuildFactor x live inserts.
	tag := uint64(len(recs) + 1)
	for round := 0; round < 64; round++ {
		for i := range recs {
			r := &recs[i]
			if !sh.settleRecordLocked(r) {
				t.Fatal("settle failed")
			}
			sh.settledPending = sh.settledPending[:0]
			*r = segmentRecord{pos: recordPos{segmentID: 1, position: int64(tag)}, queueName: "q",
				deliveryTag: tag, bytes: 1, bodyRef: -1, committed: true}
			tag++
			sh.indexRecordLocked(r)
		}
	}
	maxRebuilds := 64/walIndexChurnRebuildFactor + 1
	if sh.churnRebuilds == 0 || sh.churnRebuilds > maxRebuilds {
		t.Fatalf("64 rounds of churn at %d live keys rebuilt %d times, want 1..%d",
			len(recs), sh.churnRebuilds, maxRebuilds)
	}
	if len(sh.offset) != len(recs) || len(sh.copies) != len(recs) {
		t.Fatalf("live set lost: offset %d copies %d, want %d", len(sh.offset), len(sh.copies), len(recs))
	}
}

// BenchmarkWALIndexShards_SteadyChurn is the committer+ack cost of one FIFO
// churn step (settle the oldest key, index a new one) at a constant live set
// of 53,000 keys -- the F1 fsync:off backlog. It prices the churn rebuild.
func BenchmarkWALIndexShards_SteadyChurn(b *testing.B) {
	const live = 53000
	var shards [walIndexShardCount]walIndexShard
	recs := make([]segmentRecord, live)
	var next uint64 = 1
	index := func(r *segmentRecord) {
		*r = segmentRecord{pos: recordPos{segmentID: 1, position: int64(next) * 128}, queueName: "churn-q",
			deliveryTag: next, bytes: 128, bodyRef: -1, committed: true}
		next++
		sh := &shards[walIndexShardOf(r.deliveryTag)]
		sh.mu.Lock()
		sh.indexRecordLocked(r)
		sh.mu.Unlock()
	}
	for i := range recs {
		index(&recs[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := &recs[i%live]
		sh := &shards[walIndexShardOf(r.deliveryTag)]
		sh.mu.Lock()
		sh.settleRecordLocked(r)
		if len(sh.settledPending) > 256 {
			sh.settledPending = sh.settledPending[:0]
		}
		sh.mu.Unlock()
		index(r)
	}
}
