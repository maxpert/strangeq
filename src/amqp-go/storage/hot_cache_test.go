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

// TestHotCacheQueueAdmissionIsQueueLocal pins the per-queue/global budget
// split. A queue's admission must NEVER evict another queue's ready messages:
// the two budgets are separate resources, and satisfying a per-queue budget out
// of the global LRU lets one busy queue starve an unrelated queue (a FIFO
// consumer whose next target was evicted has to take a cold read).
//
// The shape is deliberately discriminating: the queue whose budget is full
// ("qa") is NOT the global LRU tail ("qb" is), so a global-LRU eviction
// removes a FOREIGN queue's entry while a queue-local eviction removes the
// oldest entry of the queue being admitted. The global budget is deliberately
// generous here, so it can never be the reason an entry leaves.
func TestHotCacheQueueAdmissionIsQueueLocal(t *testing.T) {
	t.Run("a queue's admission never evicts ANOTHER queue's backed entry while the global budget is not exceeded", func(t *testing.T) {
		// Same queue-name length => identical charge for both queues.
		chargeA := cacheCharge("qa", hotCacheTestMessage(1))
		chargeB := cacheCharge("qb", hotCacheTestMessage(1))
		if chargeA != chargeB {
			t.Fatalf("test queue charges differ: %d != %d", chargeA, chargeB)
		}
		charge := chargeA

		// Each queue holds 2 entries; the global budget holds 8, so the global
		// budget is never the binding constraint in this test.
		cache := newHotCache(2*charge, 8*charge)

		if !cache.Put("qb", hotCacheTestMessage(1), true) {
			t.Fatal("qb/1 was not admitted")
		}
		if !cache.Put("qa", hotCacheTestMessage(1), true) {
			t.Fatal("qa/1 was not admitted")
		}
		if !cache.Put("qa", hotCacheTestMessage(2), true) {
			t.Fatal("qa/2 was not admitted")
		}

		// qa is now full (2 entries == its budget). The next qa admission must
		// evict qa's own least recently used entry.
		if !cache.Put("qa", hotCacheTestMessage(3), true) {
			t.Fatal("qa/3 was not admitted after a queue-local eviction")
		}

		if _, _, ok := cache.Get("qb", 1); !ok {
			t.Fatal("qb/1 was evicted by qa's admission: a queue's admission must " +
				"never evict another queue's backed entry while the global budget is not exceeded")
		}
		if _, _, ok := cache.Get("qa", 1); ok {
			t.Fatal("qa/1 survived: it was qa's least recently used entry and must be the victim")
		}
		for _, tag := range []uint64{2, 3} {
			if _, _, ok := cache.Get("qa", tag); !ok {
				t.Fatalf("qa/%d should remain cached", tag)
			}
		}
		if got := cache.Count("qa"); got != 2 {
			t.Fatalf("Count(qa) = %d, want 2", got)
		}
		if got := cache.Count("qb"); got != 1 {
			t.Fatalf("Count(qb) = %d, want 1", got)
		}
		if cache.globalUsed != 3*charge {
			t.Fatalf("globalUsed = %d, want %d: the global budget was not exceeded, "+
				"so no cross-queue eviction was warranted", cache.globalUsed, 3*charge)
		}
		if cache.queueUsed["qa"] != 2*charge || cache.queueUsed["qb"] != charge {
			t.Fatalf("per-queue accounting = qa %d qb %d, want %d/%d",
				cache.queueUsed["qa"], cache.queueUsed["qb"], 2*charge, charge)
		}
	})
}

// TestHotCacheEntryIdentityIsOwnedByEntry pins the entry's ownership of its
// own identity. The cache hands out the LIVE message pointer, so a caller that
// mutates message.DeliveryTag after admission must not be able to change which
// map key the entry occupies: the entry owns its tag, and every identity
// operation (delete, range ordering) uses the owned tag, never the mutable
// message field.
func TestHotCacheEntryIdentityIsOwnedByEntry(t *testing.T) {
	t.Run("mutating the handed-out message's delivery tag does not orphan the entry", func(t *testing.T) {
		charge := cacheCharge("q", hotCacheTestMessage(1))
		cache := newHotCache(charge, charge)
		msg := hotCacheTestMessage(1)
		if !cache.Put("q", msg, false) {
			t.Fatal("Put failed")
		}

		// The caller mutates the message it was handed.
		msg.DeliveryTag = 9999

		if !cache.Delete("q", 1) {
			t.Fatal("Delete by the admitted tag failed")
		}
		if got := cache.Count("q"); got != 0 {
			t.Fatalf("Count(q) = %d after deleting by the admitted tag, want 0 "+
				"(the entry stayed in the queue map under its original key)", got)
		}
		if cache.queueUsed["q"] != 0 || cache.globalUsed != 0 {
			t.Fatalf("byte accounting after delete = queue %d global %d, want 0/0",
				cache.queueUsed["q"], cache.globalUsed)
		}

		// The byte accounting really returned to zero: the budget binds again
		// at exactly one transient entry (transient entries are never eviction
		// victims, so a leaked charge shows up as a rejected admission).
		if !cache.Put("q", hotCacheTestMessage(2), false) {
			t.Fatal("re-admission after delete failed: the byte accounting did not return to zero")
		}
		if cache.Put("q", hotCacheTestMessage(3), false) {
			t.Fatal("budget did not bind after re-admission: the byte accounting drifted low")
		}
	})

	t.Run("Range orders by the admitted tag, not by the mutated message field", func(t *testing.T) {
		charge := cacheCharge("q", hotCacheTestMessage(1))
		cache := newHotCache(4*charge, 4*charge)
		m1 := hotCacheTestMessage(1)
		m2 := hotCacheTestMessage(2)
		m3 := hotCacheTestMessage(3)
		for _, m := range []*protocol.Message{m1, m2, m3} {
			if !cache.Put("q", m, true) {
				t.Fatalf("Put(%d) failed", m.DeliveryTag)
			}
		}

		// Mutate the middle entry: sorting by the message field would move it
		// to the end of the range.
		m2.DeliveryTag = 9999

		got := cache.Range("q", 0, ^uint64(0))
		if len(got) != 3 {
			t.Fatalf("Range returned %d messages, want 3", len(got))
		}
		if got[0] != m1 || got[1] != m2 || got[2] != m3 {
			t.Fatalf("Range order = [%p %p %p], want [%p %p %p] (admitted tag order)",
				got[0], got[1], got[2], m1, m2, m3)
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

// TestHotCacheTakeResidentSettlesAndMirrors pins the settlement contract of
// TakeResident: it finds a resident entry (durable or not), returns its
// message and backed flag, removes it (exactly once), and publishes the queue's
// zeroed resident bytes into the mirror — the same observable outcome as the
// GetResident-then-Delete pair it replaces in StorageEngine.DeleteMessage.
func TestHotCacheTakeResidentSettlesAndMirrors(t *testing.T) {
	cache := newHotCache(64<<20, 256<<20)
	msg := hotCacheTestMessage(7)
	if !cache.Put("q", msg, true) {
		t.Fatal("Put failed")
	}

	got, backed, ok := cache.TakeResident("q", 7)
	if !ok || !backed || got != msg {
		t.Fatalf("TakeResident = (%p, backed=%v, ok=%v), want (%p, true, true)", got, backed, ok, msg)
	}
	if _, _, ok := cache.TakeResident("q", 7); ok {
		t.Fatal("TakeResident won twice for the same tag")
	}
	if _, _, ok := cache.Get("q", 7); ok {
		t.Fatal("a taken entry is still visible to Get")
	}
	if got := cache.Count("q"); got != 0 {
		t.Fatalf("Count = %d, want 0 after TakeResident", got)
	}
	if got := cache.residentBytes("q"); got != 0 {
		t.Fatalf("mirror residentBytes = %d, want 0 after TakeResident", got)
	}

	// A never-admitted queue and an unknown tag both report absent, exactly as
	// GetResident-then-Delete did.
	if _, _, ok := cache.TakeResident("absent-queue", 1); ok {
		t.Fatal("TakeResident on a missing queue must report absent")
	}
	if _, _, ok := cache.TakeResident("q", 99); ok {
		t.Fatal("TakeResident on a missing tag must report absent")
	}
}

// TestHotCacheTakeResidentSkipsLruPromotion is the falsifiable form of N3's
// second half: the settlement must not touch the LRU of the entry it is about
// to delete. The recency stamp (hotCache.seq) is bumped by every promotion, so
// the test proves GetResident DOES promote and TakeResident does NOT.
func TestHotCacheTakeResidentSkipsLruPromotion(t *testing.T) {
	cache := newHotCache(64<<20, 256<<20)
	for tag := uint64(1); tag <= 3; tag++ {
		if !cache.Put("q", hotCacheTestMessage(tag), true) {
			t.Fatalf("Put(%d) failed", tag)
		}
	}

	before := cache.seq.Load()
	if _, _, ok := cache.GetResident("q", 2); !ok {
		t.Fatal("GetResident(2) failed")
	}
	if cache.seq.Load() == before {
		t.Fatal("premise: GetResident must promote the probed entry (advance the recency stamp)")
	}

	before = cache.seq.Load()
	if _, _, ok := cache.TakeResident("q", 2); !ok {
		t.Fatal("TakeResident(2) failed")
	}
	if got := cache.seq.Load(); got != before {
		t.Fatalf("TakeResident promoted the doomed entry: recency stamp %d -> %d", before, got)
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

// TestHotCacheMutatedRePutDoesNotDuplicateEntry pins that the "already
// present" test in Put is INDEPENDENT of the mutable message field.
//
// The cache hands out the LIVE message pointer, so a caller can mutate
// message.DeliveryTag after admission. Every other identity operation in the
// cache uses the entry's OWNED tag (the map key, the deletion, the range
// ordering) -- but Put's membership probe used m.DeliveryTag, the MUTABLE
// field. A mutated re-Put of the SAME pointer therefore MISSED the entry it
// had already admitted and created a SECOND one: the queue's ready set was
// double-counted and the byte budget was charged twice.
//
// WHAT A MUTATION WOULD BREAK: removing the message-identity probe from Put
// (or looking the entry up by m.DeliveryTag again) makes Count(q) == 2 and
// the byte accounting == 2 charges, which is exactly what the assertions
// below forbid.
func TestHotCacheMutatedRePutDoesNotDuplicateEntry(t *testing.T) {
	queue := "q"
	msg := &protocol.Message{DeliveryTag: 1, Body: []byte("body")}
	charge := cacheCharge(queue, msg)
	cache := newHotCache(4*charge, 4*charge)

	if !cache.Put(queue, msg, false) {
		t.Fatal("the initial Put failed")
	}
	if got := cache.Count(queue); got != 1 {
		t.Fatalf("Count(%q) = %d after the initial Put, want 1", queue, got)
	}

	// The caller mutates the message it was handed and re-Puts the SAME
	// pointer. This is a DURABLE COMPLETION of an already-admitted message:
	// it must UPGRADE the entry, not duplicate it.
	msg.DeliveryTag = 9999
	if !cache.Put(queue, msg, true) {
		t.Fatal("the re-Put of an already-admitted message failed")
	}

	if got := cache.Count(queue); got != 1 {
		t.Fatalf("Count(%q) = %d after a mutated re-Put, want 1: the same message "+
			"was admitted twice, so the queue's ready set is double-counted", queue, got)
	}
	if got := cache.queueUsed[queue]; got != charge {
		t.Fatalf("per-queue byte accounting = %d, want %d: the same message was "+
			"charged twice", got, charge)
	}
	if got := cache.globalUsed; got != charge {
		t.Fatalf("global byte accounting = %d, want %d: the same message was "+
			"charged twice", got, charge)
	}

	// The entry is still found under the tag it was ADMITTED under.
	got, backed, ok := cache.Get(queue, 1)
	if !ok {
		t.Fatal("the entry is no longer findable under the tag it was admitted under")
	}
	if got != msg {
		t.Fatalf("Get(%q, 1) returned %p, want the admitted message %p", queue, got, msg)
	}
	if !backed {
		t.Fatal("the re-Put must upgrade the entry's backed state")
	}

	// And it is NOT findable under the mutated tag: the entry owns its
	// identity.
	if _, _, ok := cache.Get(queue, 9999); ok {
		t.Fatal("the entry was re-keyed under the mutated delivery tag: the entry " +
			"owns its identity and must stay under the tag it was admitted under")
	}
}
