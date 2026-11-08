package storage

// THE COST OF THE DURABILITY GATE AND THE PER-TAG SHARD LOCK, IN ISOLATION.
//
// These are FOCUSED micro-benchmarks for the two pieces the fixes add to the
// hot paths. They touch no WAL, no disk and no network: they measure the hot
// cache's operations and the per-tag lock in isolation, so the cost of each
// addition can be read directly.
//
//   - THE DURABILITY GATE: the hot cache's Get checks the entry's durability
//     flag before serving it (an atomic load), and the publish path's
//     admission is split into "admit, not durable" (the WAL write) and "mark
//     durable" (the completion, lock-free).
//   - THE PER-TAG SHARD LOCK: the delivery reader's (acknowledgement re-check
//     + admission) and the acknowledgement path's removal take the SAME
//     per-tag lock, which is what closes the acknowledgement/admission
//     window.

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// benchHotMsg builds a message of a realistic size for the hot cache.
func benchHotMsg(tag uint64) *protocol.Message {
	body := make([]byte, 1024)
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "bench-q",
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  tag,
		MessageID:    "bench",
	}
}

// BenchmarkHotCacheGetHotHit measures the hot-hit read: the delivery path's
// first step. The durability gate adds ONE atomic load to it.
func BenchmarkHotCacheGetHotHit(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	const q = "bench-q"
	if !c.Put(q, benchHotMsg(1), true) {
		b.Fatal("premise: the message must be admitted")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := c.Get(q, 1); !ok {
			b.Fatal("premise: the entry must be resident and durable")
		}
	}
}

// BenchmarkHotCacheGetNotDurable measures the read of an entry that is
// resident but NOT yet durable — the shape the hot-hit path now has to
// refuse. It is the read a delivery would do if it raced the fdatasync.
func BenchmarkHotCacheGetNotDurable(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	const q = "bench-q"
	if _, ok := c.PutBestEffortEntry(q, benchHotMsg(1), true, false); !ok {
		b.Fatal("premise: the message must be admitted")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := c.Get(q, 1); ok {
			b.Fatal("premise: a not-yet-durable entry must not be served")
		}
	}
}

// BenchmarkHotCacheAdmissionAndDurabilityPoint measures the publish path's
// two halves: the write-side admission (which creates a NOT-yet-durable
// entry) and the durability point (which marks it durable, LOCK-FREE).
func BenchmarkHotCacheAdmissionAndDurabilityPoint(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	const q = "bench-q"
	msg := benchHotMsg(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, ok := c.PutBestEffortEntry(q, msg, true, false)
		if !ok {
			b.Fatal("premise: the admission must succeed")
		}
		c.MarkDurableEntry(entry)
		c.Delete(q, 1)
	}
}

// BenchmarkPerTagShardLockUncontended measures the per-tag lock alone, with
// no contention: one acquisition and release.
func BenchmarkPerTagShardLockUncontended(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu := &coldRemovalShards[uint64(i)&63]
		mu.Lock()
		mu.Unlock()
	}
}

// BenchmarkPerTagShardLockContended measures the per-tag lock with the
// contention a real workload puts on it: 64 tags over 64 shards, hammered by
// one goroutine per shard (the shape the broker's ack path produces: the
// tags are consecutive, so the shards are too).
func BenchmarkPerTagShardLockContended(b *testing.B) {
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var counter atomic.Uint64
	for s := 0; s < 64; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu := &coldRemovalShards[counter.Add(1)&63]
				mu.Lock()
				mu.Unlock()
			}
		}()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu := &coldRemovalShards[uint64(i)&63]
		mu.Lock()
		mu.Unlock()
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}

// BenchmarkDeleteMessageAckPath measures the acknowledgement path's storage
// work: the residency probe, the removal and the per-tag shard lock.
//
// IT CALLS THE PRODUCTION DeleteMessage, with a storage that has no WAL and
// no segment tier, so the measurement is the fix's cost and NOT the WAL
// acknowledgement's I/O.
func BenchmarkDeleteMessageAckPath(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	ds := &StorageEngine{hot: c}
	const q = "bench-q"
	ds.getOrCreateQueueHot(q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tag := uint64(i + 1)
		c.Put(q, benchHotMsg(tag), true)
		if err := ds.DeleteMessage(q, tag); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeliveryAdmissionPath measures the delivery reader's admission:
// the per-tag shard lock (which makes the (acknowledgement re-check +
// admission) atomic with the acknowledgement) plus the hot-cache admission
// itself.
//
// IT IS THE "BEFORE" SHAPE of the admission: the epoch capture and the
// epoch/incarnation validation did not exist yet. The shape WITH them is
// BenchmarkDeliveryAdmissionPathWithEpoch below, so the two are directly
// comparable.
func BenchmarkDeliveryAdmissionPath(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	const q = "bench-q"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tag := uint64(i + 1)
		m := benchHotMsg(tag)
		mu := &coldRemovalShards[tag&63]
		mu.Lock()
		if _, ok := c.PutBestEffortEntry(q, m, true, true); !ok {
			b.Fatal("premise: the admission must succeed")
		}
		mu.Unlock()
		c.Delete(q, tag)
	}
}

// BenchmarkDeliveryAdmissionPathWithEpoch measures the delivery reader's
// admission WITH the admission epoch: the CAPTURE (before the cold read) and
// the VALIDATION (the incarnation and the epoch, under the queue's lock,
// with the admission).
//
// IT IS THE "AFTER" SHAPE. The delta against BenchmarkDeliveryAdmissionPath
// is the whole cost of the fix: one extra queue-lock acquisition for the
// capture, one sync.Map load for the incarnation check and one uint64
// compare for the epoch check.
func BenchmarkDeliveryAdmissionPathWithEpoch(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	ds := &StorageEngine{hot: c}
	const q = "bench-q"
	ds.getOrCreateQueueHot(q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tag := uint64(i + 1)
		m := benchHotMsg(tag)
		// THE CAPTURE: it happens BEFORE the cold read, outside the shard
		// lock.
		epoch, hotQueue := c.captureAdmission(q)
		mu := &coldRemovalShards[tag&63]
		mu.Lock()
		if err := ds.putBestEffortIfCurrent(q, ds.getQueueHot(q), hotQueue, epoch, m); err != nil {
			b.Fatal(err)
		}
		mu.Unlock()
		c.Delete(q, tag)
	}
}

// BenchmarkAdmissionEpochCapture measures the capture alone: one queue-lock
// acquisition and one uint64 read. It is the part of the fix that runs OUTSIDE
// the delivery reader's critical section.
func BenchmarkAdmissionEpochCapture(b *testing.B) {
	c := newHotCache(64<<20, 256<<20)
	const q = "bench-q"
	c.queueState(q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, q2 := c.captureAdmission(q); q2 == nil {
			b.Fatal("premise: the queue state must exist")
		}
	}
}

// deleteRangeWithoutEpoch is the PRE-CHANGE range removal, kept here as the
// benchmark's control: it is the same removal WITHOUT the admission-epoch
// increment, so the two benchmarks differ by exactly that increment.
func deleteRangeWithoutEpoch(c *hotCache, queue string, minTag, maxTag uint64) int {
	if minTag > maxTag {
		return 0
	}
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)
	removed := 0
	for tag, entry := range q.entries {
		if tag < minTag || tag > maxTag {
			continue
		}
		c.removeLocked(q, entry)
		removed++
	}
	if removed > 0 {
		c.noteQueueUsed(queue, q.used)
	}
	return removed
}

// BenchmarkDeleteRangeAdmissionEpoch measures the range removal with and
// without the admission-epoch increment. The increment is ONE add under a
// lock the removal already holds, so the two shapes must measure the same.
func BenchmarkDeleteRangeAdmissionEpoch(b *testing.B) {
	const q = "bench-q"
	newCache := func() *hotCache {
		c := newHotCache(64<<20, 256<<20)
		for i := 0; i < 64; i++ {
			c.Put(q, benchHotMsg(uint64(i+1)), true)
		}
		return c
	}

	b.Run("with-epoch", func(b *testing.B) {
		c := newCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.DeleteRange(q, 1, 64)
		}
	})
	b.Run("without-epoch", func(b *testing.B) {
		c := newCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			deleteRangeWithoutEpoch(c, q, 1, 64)
		}
	})
}
