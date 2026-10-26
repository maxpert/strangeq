package storage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

func hotCacheTestMessage(tag uint64) *protocol.Message {
	return &protocol.Message{DeliveryTag: tag}
}

func TestHotCacheQueueAndGlobalBounds(t *testing.T) {
	t.Run("queue bound evicts the least recently used backed entry", func(t *testing.T) {
		charge := cacheCharge("q", hotCacheTestMessage(1))
		cache := newHotCache(2*charge, 3*charge)
		for tag := uint64(1); tag <= 2; tag++ {
			if !cache.Put("q", hotCacheTestMessage(tag), true) {
				t.Fatalf("Put(tag=%d) failed", tag)
			}
		}
		if _, _, ok := cache.Get("q", 1); !ok {
			t.Fatal("tag 1 missing before LRU refresh")
		}
		if !cache.Put("q", hotCacheTestMessage(3), true) {
			t.Fatal("tag 3 was not admitted after backed eviction")
		}
		if _, _, ok := cache.Get("q", 2); ok {
			t.Fatal("tag 2 was LRU and should have been evicted")
		}
		for _, tag := range []uint64{1, 3} {
			if _, _, ok := cache.Get("q", tag); !ok {
				t.Fatalf("tag %d should remain cached", tag)
			}
		}
		if got := cache.Count("q"); got != 2 {
			t.Fatalf("Count() = %d, want 2", got)
		}
	})

	t.Run("global bound evicts across queues", func(t *testing.T) {
		chargeA := cacheCharge("a", hotCacheTestMessage(1))
		chargeB := cacheCharge("b", hotCacheTestMessage(1))
		if chargeA != chargeB {
			t.Fatalf("test queue charges differ: %d != %d", chargeA, chargeB)
		}
		charge := chargeA
		cache := newHotCache(3*charge, 3*charge)
		if !cache.Put("a", hotCacheTestMessage(1), true) ||
			!cache.Put("a", hotCacheTestMessage(2), true) ||
			!cache.Put("b", hotCacheTestMessage(1), true) {
			t.Fatal("initial cache fills failed")
		}
		if _, _, ok := cache.Get("a", 1); !ok {
			t.Fatal("a/1 missing before LRU refresh")
		}
		if !cache.Put("b", hotCacheTestMessage(2), true) {
			t.Fatal("global LRU eviction did not admit b/2")
		}
		if _, _, ok := cache.Get("a", 2); ok {
			t.Fatal("a/2 was global LRU and should have been evicted")
		}
		for _, key := range []struct {
			queue string
			tag   uint64
		}{{"a", 1}, {"b", 1}, {"b", 2}} {
			if _, _, ok := cache.Get(key.queue, key.tag); !ok {
				t.Fatalf("%s/%d should remain cached", key.queue, key.tag)
			}
		}
	})
}

func TestHotCachePinnedTransient(t *testing.T) {
	t.Run("transient entries are never eviction victims", func(t *testing.T) {
		charge := cacheCharge("q", hotCacheTestMessage(1))
		cache := newHotCache(charge, charge)
		if !cache.Put("q", hotCacheTestMessage(1), false) {
			t.Fatal("failed to cache transient tag 1")
		}
		if cache.Put("q", hotCacheTestMessage(2), true) {
			t.Fatal("backed tag 2 was admitted by evicting pinned transient data")
		}
		if m, backed, ok := cache.Get("q", 1); !ok || backed || m.DeliveryTag != 1 {
			t.Fatalf("transient tag 1 changed: message=%v backed=%v ok=%v", m, backed, ok)
		}
	})

	t.Run("eviction skips transient entries", func(t *testing.T) {
		charge := cacheCharge("q", hotCacheTestMessage(1))
		cache := newHotCache(2*charge, 2*charge)
		if !cache.Put("q", hotCacheTestMessage(1), false) ||
			!cache.Put("q", hotCacheTestMessage(2), true) {
			t.Fatal("initial cache fills failed")
		}
		if !cache.Put("q", hotCacheTestMessage(3), true) {
			t.Fatal("tag 3 was not admitted")
		}
		if _, _, ok := cache.Get("q", 1); !ok {
			t.Fatal("pinned transient tag 1 was evicted")
		}
		if _, _, ok := cache.Get("q", 2); ok {
			t.Fatal("backed tag 2 should have been evicted")
		}
		if _, _, ok := cache.Get("q", 3); !ok {
			t.Fatal("tag 3 missing after admission")
		}
	})

	t.Run("existing entry retains message and becomes backed", func(t *testing.T) {
		charge := cacheCharge("q", hotCacheTestMessage(1))
		cache := newHotCache(charge, charge)
		original := hotCacheTestMessage(1)
		if !cache.Put("q", original, false) {
			t.Fatal("initial Put failed")
		}
		replacement := hotCacheTestMessage(1)
		replacement.Body = []byte("must not replace or recharge the original")
		if !cache.Put("q", replacement, true) {
			t.Fatal("existing-tag Put failed")
		}
		m, backed, ok := cache.Get("q", 1)
		if !ok || m != original || !backed {
			t.Fatalf("Get() = (%p, %v, %v), want original %p, true, true", m, backed, ok, original)
		}
	})
}

func TestHotCacheOversizeRejected(t *testing.T) {
	message := hotCacheTestMessage(1)
	charge := cacheCharge("q", message)

	t.Run("queue budget", func(t *testing.T) {
		cache := newHotCache(charge-1, charge)
		if cache.Put("q", message, true) {
			t.Fatal("message larger than queue budget was admitted")
		}
		if got := cache.Count("q"); got != 0 {
			t.Fatalf("Count() = %d after rejection, want 0", got)
		}
	})

	t.Run("global budget", func(t *testing.T) {
		cache := newHotCache(charge, charge-1)
		if cache.Put("q", message, true) {
			t.Fatal("message larger than global budget was admitted")
		}
		if got := cache.Count("q"); got != 0 {
			t.Fatalf("Count() = %d after rejection, want 0", got)
		}
	})
}

func TestHotCacheDeleteUniqueWinner(t *testing.T) {
	charge := cacheCharge("q", hotCacheTestMessage(1))
	cache := newHotCache(charge, charge)
	if !cache.Put("q", hotCacheTestMessage(1), false) {
		t.Fatal("Put failed")
	}

	const contenders = 32
	var start sync.WaitGroup
	start.Add(1)
	var winners atomic.Int32
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			defer wg.Done()
			start.Wait()
			if cache.Delete("q", 1) {
				winners.Add(1)
			}
		}()
	}
	start.Done()
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("Delete winners = %d, want exactly 1", got)
	}
	if cache.Delete("q", 1) {
		t.Fatal("deleted entry won arbitration twice")
	}
	if _, _, ok := cache.Get("q", 1); ok {
		t.Fatal("deleted entry remains visible")
	}
}

func TestHotCacheZeroBodyEntriesBounded(t *testing.T) {
	message := hotCacheTestMessage(1)
	charge := cacheCharge("q", message)
	if charge <= 512 {
		t.Fatalf("zero-body charge = %d, want fixed overhead plus metadata", charge)
	}
	cache := newHotCache(3*charge, 3*charge)
	for tag := uint64(1); tag <= 3; tag++ {
		if !cache.Put("q", hotCacheTestMessage(tag), false) {
			t.Fatalf("zero-body tag %d did not fit", tag)
		}
	}
	if cache.Put("q", hotCacheTestMessage(4), false) {
		t.Fatal("fourth zero-body entry exceeded a three-entry byte budget")
	}
	if got := cache.Count("q"); got != 3 {
		t.Fatalf("Count() = %d, want 3", got)
	}
}
