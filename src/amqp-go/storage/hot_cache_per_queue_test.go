package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// THE PER-QUEUE HOT CACHE.
//
// The hot cache used to have ONE mutex for the whole collection, so every
// queue's publish, delivery and acknowledgement serialised on the same
// lock: the aggregate throughput FELL as the queue count grew (measured on
// the 1 KiB multi-queue shape: 124,273 msg/s at one queue, 87,003 msg/s at
// four). These tests pin the shape that fixes it: ONE LOCK PER QUEUE, with
// the GLOBAL budget still enforced across all of them.

func perQueueTestMessage(tag uint64) *protocol.Message {
	return &protocol.Message{DeliveryTag: tag, Body: []byte("per-queue")}
}

// hotCacheAssertMirrorConsistent checks that the global byte accounting is
// EXACTLY the sum of the per-queue byte counts.
//
// The global counter is the sum of every queue's resident bytes, so a leak
// or a double count shows up here as a mismatch.
//
// It reads the mirror without any cache-wide lock, so it may only be called
// when no other goroutine is running.
func hotCacheAssertMirrorConsistent(t *testing.T, cache *hotCache) {
	t.Helper()
	var sum int64
	for _, used := range hotCacheMirror(cache) {
		sum += used
	}
	if got := atomic.LoadInt64(&cache.globalUsed); got != sum {
		t.Fatalf("the global byte accounting = %d, want %d: it is the sum of the "+
			"per-queue byte counts, so a mismatch is a leak or a double count",
			got, sum)
	}
}

// TestHotCacheQueuesDoNotShareALock pins that two DIFFERENT queues can be
// operated at the same time.
//
// The observer (the fault_hooks.go pattern) blocks INSIDE one queue's lock.
// If the queues shared ONE lock, the other queue's operations would block
// behind it and this test would time out. With one lock per queue, the other
// queue's operations complete while the first queue's lock is held.
func TestHotCacheQueuesDoNotShareALock(t *testing.T) {
	charge := cacheCharge("qa", perQueueTestMessage(1))
	cache := newHotCache(64*charge, 64*charge)
	if !cache.Put("qa", perQueueTestMessage(1), true) {
		t.Fatal("the first admission failed")
	}

	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	restore := SetHotCacheQueueLockForTest(func(queue string) {
		if queue != "qa" {
			return
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	defer restore()

	held := make(chan struct{})
	go func() {
		defer close(held)
		cache.Put("qa", perQueueTestMessage(2), true)
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the observer never saw queue qa's lock: the cache does not lock per queue")
	}

	// A DIFFERENT queue must not be blocked by queue qa's lock.
	other := make(chan struct{})
	go func() {
		defer close(other)
		if !cache.Put("qb", perQueueTestMessage(1), true) {
			t.Errorf("the admission into qb failed")
			return
		}
		if _, _, ok := cache.Get("qb", 1); !ok {
			t.Errorf("the entry admitted into qb is not readable")
			return
		}
		cache.Delete("qb", 1)
	}()

	select {
	case <-other:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("queue qb was blocked by queue qa's lock: the queues share ONE lock")
	}

	close(release)
	<-held
}

// TestHotCacheQueuesAreOperatedConcurrently hammers several queues from many
// goroutines. It is the -race workhorse: every queue's entries, LRU and
// accounting are touched concurrently, and the global budget is enforced
// across all of them at the same time.
func TestHotCacheQueuesAreOperatedConcurrently(t *testing.T) {
	charge := cacheCharge("q0", perQueueTestMessage(1))
	cache := newHotCache(1024*charge, 1024*charge)

	const queues = 4
	const workers = 8
	const iterations = 300

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			queue := fmt.Sprintf("q%d", worker%queues)
			for i := 0; i < iterations; i++ {
				tag := uint64(i + 1)
				cache.Put(queue, perQueueTestMessage(tag), true)
				cache.Get(queue, tag)
				cache.Count(queue)
				cache.Range(queue, 0, ^uint64(0))
				cache.TagsInRange(queue, 0, ^uint64(0))
				cache.Delete(queue, tag)
			}
		}(worker)
	}
	wg.Wait()

	if got := atomic.LoadInt64(&cache.globalUsed); got < 0 {
		t.Fatalf("the global byte accounting = %d, want a non-negative value", got)
	}
	hotCacheAssertMirrorConsistent(t, cache)
}

// TestHotCacheGlobalBudgetIsNeverExceeded pins the GLOBAL budget while
// several queues fill up TOGETHER, and pins that the accounting returns to
// zero when everything is removed.
//
// The global budget is the sum of every queue's resident bytes. It must
// never be exceeded -- not by one queue, not by four queues racing, and not
// by the cross-queue eviction that frees space for a binding global budget.
func TestHotCacheGlobalBudgetIsNeverExceeded(t *testing.T) {
	queues := []string{"q0", "q1", "q2", "q3"}
	charge := cacheCharge("q0", perQueueTestMessage(1))
	globalBudget := 8 * charge
	cache := newHotCache(4*charge, globalBudget)

	check := func(stage string) {
		t.Helper()
		if got := atomic.LoadInt64(&cache.globalUsed); got > globalBudget {
			t.Fatalf("%s: the global byte accounting = %d, over the %d byte budget",
				stage, got, globalBudget)
		}
	}

	// ONE queue at a time: the per-queue budget is 4 charges, the global one
	// is 8, so the global budget is the binding one here.
	for round := 0; round < 20; round++ {
		for _, queue := range queues {
			cache.Put(queue, perQueueTestMessage(uint64(round+1)), true)
			check("while filling the queues")
		}
	}
	hotCacheAssertMirrorConsistent(t, cache)

	// ALL of them at once, from many goroutines.
	var over atomic.Bool
	stop := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if atomic.LoadInt64(&cache.globalUsed) > globalBudget {
				over.Store(true)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			queue := queues[worker%len(queues)]
			for i := 0; i < 200; i++ {
				cache.Put(queue, perQueueTestMessage(uint64(i+1)), true)
				cache.Get(queue, uint64(i+1))
				cache.Delete(queue, uint64(i+1))
			}
		}(worker)
	}
	wg.Wait()
	close(stop)
	watcher.Wait()
	if over.Load() {
		t.Fatalf("the global byte accounting went over the %d byte budget while "+
			"several queues filled up together", globalBudget)
	}
	hotCacheAssertMirrorConsistent(t, cache)

	// EVERYTHING REMOVED: the accounting returns to zero, exactly.
	for _, queue := range queues {
		cache.DeleteRange(queue, 0, ^uint64(0))
	}
	if got := atomic.LoadInt64(&cache.globalUsed); got != 0 {
		t.Fatalf("the global byte accounting = %d after everything was removed, want 0", got)
	}
	remaining := len(hotCacheMirror(cache))
	if remaining != 0 {
		t.Fatalf("the mirror still holds %d queues after everything was removed", remaining)
	}
	hotCacheAssertMirrorConsistent(t, cache)
}

// TestHotCacheCrossQueueEvictionOnlyWhenTheGlobalBudgetBinds pins the split
// between the two budgets.
//
// A queue's admission satisfies its OWN budget out of its OWN entries: it
// must never cost an UNRELATED queue its ready messages. The cross-queue
// eviction is legitimate ONLY when the global budget is the binding one --
// then the cache as a whole is over its budget and any queue's entry may go.
func TestHotCacheCrossQueueEvictionOnlyWhenTheGlobalBudgetBinds(t *testing.T) {
	t.Run("a queue's admission does not evict another queue's entry while the global budget has slack", func(t *testing.T) {
		charge := cacheCharge("qa", perQueueTestMessage(1))
		// The per-queue budget binds at 4 charges, the global one at 64: the
		// global budget can never be the reason an entry leaves.
		cache := newHotCache(4*charge, 64*charge)

		if !cache.Put("qa", perQueueTestMessage(1), true) {
			t.Fatal("qa/1 was not admitted")
		}
		if !cache.Put("qb", perQueueTestMessage(1), true) {
			t.Fatal("qb/1 was not admitted")
		}
		for tag := uint64(2); tag <= 6; tag++ {
			if !cache.Put("qa", perQueueTestMessage(tag), true) {
				t.Fatalf("qa/%d was not admitted", tag)
			}
		}

		if _, _, ok := cache.Get("qb", 1); !ok {
			t.Fatal("qb/1 was evicted by qa's admission: a queue's admission must " +
				"never evict another queue's backed entry while the global budget has slack")
		}
		// qa's own least recently used entry is the victim.
		if _, _, ok := cache.Get("qa", 1); ok {
			t.Fatal("qa/1 survived: it was qa's least recently used entry and must be the victim")
		}
		if got := cache.Count("qa"); got != 4 {
			t.Fatalf("Count(qa) = %d, want 4 (the per-queue budget)", got)
		}
		if got := cache.Count("qb"); got != 1 {
			t.Fatalf("Count(qb) = %d, want 1", got)
		}
		hotCacheAssertMirrorConsistent(t, cache)
	})

	t.Run("a queue's admission evicts another queue's entry when the global budget is the binding one", func(t *testing.T) {
		charge := cacheCharge("qa", perQueueTestMessage(1))
		// The global budget binds at 2 charges, the per-queue one at 8.
		cache := newHotCache(8*charge, 2*charge)

		if !cache.Put("qa", perQueueTestMessage(1), true) {
			t.Fatal("qa/1 was not admitted")
		}
		if !cache.Put("qb", perQueueTestMessage(1), true) {
			t.Fatal("qb/1 was not admitted")
		}
		if !cache.Put("qc", perQueueTestMessage(1), true) {
			t.Fatal("qc/1 was not admitted although the global eviction could make room")
		}

		// The cache holds at most the global budget, and the victim was one
		// of the two entries that were already there.
		if got := atomic.LoadInt64(&cache.globalUsed); got > 2*charge {
			t.Fatalf("the global byte accounting = %d, over the %d byte budget", got, 2*charge)
		}
		resident := 0
		for _, queue := range []string{"qa", "qb", "qc"} {
			if _, _, ok := cache.Get(queue, 1); ok {
				resident++
			}
		}
		if resident != 2 {
			t.Fatalf("%d of the three entries are resident, want 2: exactly one backed "+
				"entry was evicted to make room for the third", resident)
		}
		hotCacheAssertMirrorConsistent(t, cache)
	})
}

// TestHotCacheAccountingIsExactAfterTheCacheIsEmptied pins rule 6: emptying
// the cache leaves NO leak and NO double count.
func TestHotCacheAccountingIsExactAfterTheCacheIsEmptied(t *testing.T) {
	queues := []string{"q0", "q1"}
	charge := cacheCharge("q0", perQueueTestMessage(1))
	cache := newHotCache(16*charge, 16*charge)

	for _, queue := range queues {
		for tag := uint64(1); tag <= 5; tag++ {
			if !cache.Put(queue, perQueueTestMessage(tag), true) {
				t.Fatalf("%s/%d was not admitted", queue, tag)
			}
		}
	}
	hotCacheAssertMirrorConsistent(t, cache)
	if got := atomic.LoadInt64(&cache.globalUsed); got != 10*charge {
		t.Fatalf("the global byte accounting = %d, want %d", got, 10*charge)
	}

	for _, queue := range queues {
		if removed := cache.DeleteRange(queue, 0, ^uint64(0)); removed != 5 {
			t.Fatalf("DeleteRange(%q) removed %d entries, want 5", queue, removed)
		}
	}
	if got := atomic.LoadInt64(&cache.globalUsed); got != 0 {
		t.Fatalf("the global byte accounting = %d after the cache was emptied, want 0", got)
	}
	remaining := len(hotCacheMirror(cache))
	if remaining != 0 {
		t.Fatalf("the mirror still holds %d queues after the cache was emptied", remaining)
	}
	hotCacheAssertMirrorConsistent(t, cache)
}
