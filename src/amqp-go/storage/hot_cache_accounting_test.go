package storage

import (
	"sync/atomic"
	"testing"
)

// THE PER-QUEUE ACCOUNTING IS EXACT.
//
// Each queue owns its byte accounting (hotCacheQueue.used, under the queue's
// own lock) and the cache publishes the sum of all of them in one atomic
// counter. These tests check the two against each other and against the
// observable mirror, so a leak or a double count cannot hide.

// hotCacheAssertAccountingIsExact checks the hot cache's byte accounting
// against the AUTHORITATIVE per-queue counters.
//
// The global counter is the sum of every queue's own accounting, and the
// mirror map is the observable projection of it. A leak or a double count
// shows up here as a mismatch.
func hotCacheAssertAccountingIsExact(t *testing.T, cache *hotCache) {
	t.Helper()

	perQueue := make(map[string]int64)
	var sum int64
	cache.queues.Range(func(key, value any) bool {
		q := value.(*hotCacheQueue)
		q.mu.Lock()
		perQueue[key.(string)] = q.used
		q.mu.Unlock()
		sum += perQueue[key.(string)]
		return true
	})

	if got := atomic.LoadInt64(&cache.globalUsed); got != sum {
		t.Fatalf("the global byte accounting = %d, want %d: it is the sum of the "+
			"queues' own accounting, so a mismatch is a leak or a double count",
			got, sum)
	}

	cache.queueUsedMu.Lock()
	mirror := make(map[string]int64, len(cache.queueUsed))
	for name, used := range cache.queueUsed {
		mirror[name] = used
	}
	cache.queueUsedMu.Unlock()

	for name, used := range perQueue {
		got, ok := mirror[name]
		if !ok {
			if used != 0 {
				t.Fatalf("the mirror has no entry for queue %q, but the queue holds %d bytes",
					name, used)
			}
			continue
		}
		if got != used {
			t.Fatalf("the mirror for queue %q = %d, want %d", name, got, used)
		}
	}
	for name := range mirror {
		if _, ok := perQueue[name]; !ok {
			t.Fatalf("the mirror holds an entry for the unknown queue %q", name)
		}
	}
}

// TestHotCachePerQueueAccountingIsExact pins that the accounting of every
// queue adds up to the global counter, and that the mirror never drifts from
// the queue's own counter -- including after the queues are emptied.
func TestHotCachePerQueueAccountingIsExact(t *testing.T) {
	queues := []string{"q0", "q1", "q2"}
	charge := cacheCharge("q0", perQueueTestMessage(1))
	cache := newHotCache(16*charge, 16*charge)

	for _, queue := range queues {
		for tag := uint64(1); tag <= 5; tag++ {
			if !cache.Put(queue, perQueueTestMessage(tag), true) {
				t.Fatalf("%s/%d was not admitted", queue, tag)
			}
		}
	}
	hotCacheAssertAccountingIsExact(t, cache)
	if got := atomic.LoadInt64(&cache.globalUsed); got != 15*charge {
		t.Fatalf("the global byte accounting = %d, want %d", got, 15*charge)
	}

	// Removing one queue's entries leaves the other queues' accounting alone.
	if removed := cache.DeleteRange("q1", 0, ^uint64(0)); removed != 5 {
		t.Fatalf("DeleteRange(q1) removed %d entries, want 5", removed)
	}
	hotCacheAssertAccountingIsExact(t, cache)
	if got := atomic.LoadInt64(&cache.globalUsed); got != 10*charge {
		t.Fatalf("the global byte accounting = %d after one queue was emptied, want %d",
			got, 10*charge)
	}

	for _, queue := range queues {
		cache.DeleteRange(queue, 0, ^uint64(0))
	}
	if got := atomic.LoadInt64(&cache.globalUsed); got != 0 {
		t.Fatalf("the global byte accounting = %d after the cache was emptied, want 0", got)
	}
	hotCacheAssertAccountingIsExact(t, cache)
}
