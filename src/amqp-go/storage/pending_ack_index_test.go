package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPendingAck(consumerTag string, deliveryTag uint64) *protocol.PendingAck {
	return &protocol.PendingAck{
		QueueName:   fmt.Sprintf("q.%s", consumerTag),
		DeliveryTag: deliveryTag,
		ConsumerTag: consumerTag,
		MessageID:   fmt.Sprintf("q.%s:%d", consumerTag, deliveryTag),
	}
}

func TestConsumerPendingAcksIndex_BasicCorrectness(t *testing.T) {
	ds := NewDisruptorStorage()

	require.NoError(t, ds.StorePendingAck(newTestPendingAck("A", 1)))
	require.NoError(t, ds.StorePendingAck(newTestPendingAck("A", 2)))
	require.NoError(t, ds.StorePendingAck(newTestPendingAck("A", 3)))
	require.NoError(t, ds.StorePendingAck(newTestPendingAck("B", 4)))
	require.NoError(t, ds.StorePendingAck(newTestPendingAck("B", 5)))

	acksA, err := ds.GetConsumerPendingAcks("A")
	require.NoError(t, err)
	assert.ElementsMatch(t, []uint64{1, 2, 3}, deliveryTags(acksA))

	acksB, err := ds.GetConsumerPendingAcks("B")
	require.NoError(t, err)
	assert.ElementsMatch(t, []uint64{4, 5}, deliveryTags(acksB))

	// O(M) lookup returns only this consumer's acks, never another's.
	acksC, err := ds.GetConsumerPendingAcks("C")
	require.NoError(t, err)
	assert.Empty(t, acksC)

	// Primary O(1) lookup still works.
	pa, err := ds.GetPendingAck("", 3)
	require.NoError(t, err)
	assert.Equal(t, "A", pa.ConsumerTag)

	// Delete removes from both the primary and the index.
	require.NoError(t, ds.DeletePendingAck("", 2))
	acksA, err = ds.GetConsumerPendingAcks("A")
	require.NoError(t, err)
	assert.ElementsMatch(t, []uint64{1, 3}, deliveryTags(acksA))
	_, err = ds.GetPendingAck("", 2)
	assert.ErrorIs(t, err, errPendingAckNotFoundSentinel(t))

	// Deleting an unknown tag is a safe no-op (no panic, no index corruption).
	require.NoError(t, ds.DeletePendingAck("", 9999))
}

// TestConsumerPendingAcksIndex_SupersetInvariant exercises the index under the
// broker's actual per-delivery-tag concurrency contract and asserts the core
// safety property at every quiescence point: the per-consumer index equals the
// subset of the global pendingAcks map belonging to that consumer. The index
// may transiently hold an extra (already-acked) entry during a race, but it can
// never drop a tag that is still pending.
//
// The broker's contract (which this test honours) is:
//   - For a given tag, at most one Store races one Delete (the window between
//     the channel send and StorePendingAck, during which an ack can arrive).
//     There is no "pre-store" -- the Store that races is the first and only one
//     for that delivery.
//   - A re-Store of a tag (re-delivery after requeue) happens strictly AFTER
//     the prior Delete returns -- requeue is issued after DeletePendingAck.
//
// Run under `-race` to also validate that all individual sync.Map operations
// are data-race-free under this load.
func TestConsumerPendingAcksIndex_SupersetInvariant(t *testing.T) {
	const consumers = 16
	const tagsPerConsumer = 200
	tagBase := func(c int, tag uint64) uint64 { return uint64(c)*1000 + tag }
	ctag := func(c int) string { return fmt.Sprintf("C%d", c) }

	ds := NewDisruptorStorage()
	var wg sync.WaitGroup

	// Phase 1: every consumer stores its own disjoint tag range concurrently.
	for c := 0; c < consumers; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := uint64(1); tag <= tagsPerConsumer; tag++ {
				_ = ds.StorePendingAck(newTestPendingAck(ctag(c), tagBase(c, tag)))
			}
		}()
	}
	// Concurrent readers (multiple-ack readers) racing the writers.
	for c := 0; c < consumers; c++ {
		ct := ctag(c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = ds.GetConsumerPendingAcks(ct)
			}
		}()
	}
	wg.Wait()
	assertIndexEqualsPrimary(t, ds) // Store-only concurrency: invariant holds.

	// Phase 2: delete the first half of each consumer's tags concurrently.
	// No re-Store of these tags runs here (broker never re-stores a tag while a
	// delete of it is in flight; requeue happens after the delete returns).
	for c := 0; c < consumers; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := uint64(1); tag <= tagsPerConsumer/2; tag++ {
				_ = ds.DeletePendingAck("", tagBase(c, tag))
			}
		}()
	}
	wg.Wait()
	assertIndexEqualsPrimary(t, ds)

	// Phase 3: re-delivery after requeue -- re-Store the tags deleted in Phase 2.
	// This runs strictly after those deletes completed (broker ordering), so no
	// same-tag Store races a delete here.
	for c := 0; c < consumers; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := uint64(1); tag <= tagsPerConsumer/2; tag++ {
				_ = ds.StorePendingAck(newTestPendingAck(ctag(c), tagBase(c, tag)))
			}
		}()
	}
	wg.Wait()
	assertIndexEqualsPrimary(t, ds)

	// Phase 4: the ack-during-deliver race on FRESH tags (never pre-stored). For
	// each tag, a SINGLE Store races a SINGLE Delete -- the only same-tag race
	// the broker produces. There is no second Store, so the violating
	// interleaving (a re-Store landing between a delete's two steps) cannot
	// occur. Each tag ends either present in both maps or absent from both.
	const raceTags = 100
	for c := 0; c < consumers; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := uint64(1); tag <= raceTags; tag++ {
				dtag := uint64(c)*100000 + tag // disjoint fresh range
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = ds.StorePendingAck(newTestPendingAck(ctag(c), dtag))
				}()
				_ = ds.DeletePendingAck("", dtag)
			}
		}()
	}
	wg.Wait()
	assertIndexEqualsPrimary(t, ds)

	// Phase 5: drain every tag. After all deletes, both maps must be empty
	// (orphan empty inner maps may remain but must contain no tags).
	for c := 0; c < consumers; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := uint64(1); tag <= tagsPerConsumer; tag++ {
				_ = ds.DeletePendingAck("", tagBase(c, tag))
			}
			for tag := uint64(1); tag <= raceTags; tag++ {
				_ = ds.DeletePendingAck("", uint64(c)*100000+tag)
			}
		}()
	}
	wg.Wait()
	assertIndexEqualsPrimary(t, ds)
	for cTag, acks := range mustGetAllConsumers(t, ds) {
		assert.Empty(t, acks, "no tags should remain after drain for consumer %q", cTag)
	}
}

// TestConsumerPendingAcksIndex_ReStoreRace is the focused regression test for
// the pending-ack index lost-tag bug. It exercises the exact interleaving that
// a non-atomic two-map index cannot tolerate: a re-Store (re-delivery after
// requeue) racing a Delete (ack) on a tag that is ALREADY present in both maps.
//
// Without per-tag atomicity between StorePendingAck and DeletePendingAck the
// following schedule loses the tag from the index while it survives in the
// primary:
//
//  1. re-Store: index.Store(t)              // index has t
//  2. Delete:   primary.LoadAndDelete(t) ok // primary loses t (old entry)
//  3. Delete:   index.Delete(t)             // index loses t
//  4. re-Store: primary.Store(t)            // primary has t, index does NOT
//
// Step 3 deletes the re-Store's index entry (same key), while step 4 lands the
// re-Store's primary entry after the Delete already removed the old one. The
// tag is now pending in the primary but invisible to GetConsumerPendingAcks.
//
// Every tag is pre-stored (a delivered, unacked message), then a re-Store in a
// fresh goroutine races a synchronous Delete for that tag -- the broker's
// ack-during-re-delivery window. assertIndexEqualsPrimary must hold at
// quiescence. Run with -race.
func TestConsumerPendingAcksIndex_ReStoreRace(t *testing.T) {
	const consumers = 16
	const raceTags = 100

	ds := NewDisruptorStorage()
	var wg sync.WaitGroup
	ctag := func(c int) string { return fmt.Sprintf("R%d", c) }
	dtag := func(c int, tag uint64) uint64 { return uint64(c)*100000 + tag }

	// Pre-store every tag so it exists in both maps: a delivered, unacked msg.
	for c := 0; c < consumers; c++ {
		for tag := uint64(1); tag <= raceTags; tag++ {
			require.NoError(t, ds.StorePendingAck(newTestPendingAck(ctag(c), dtag(c, tag))))
		}
	}

	// Re-Store (re-delivery) in a fresh goroutine races Delete (ack) run
	// synchronously in the loop -- one Store races one Delete per tag, on a
	// tag that is already pending. This is the schedule above.
	for c := 0; c < consumers; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := uint64(1); tag <= raceTags; tag++ {
				t := dtag(c, tag)
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = ds.StorePendingAck(newTestPendingAck(ctag(c), t))
				}()
				_ = ds.DeletePendingAck("", t)
			}
		}()
	}
	wg.Wait()
	assertIndexEqualsPrimary(t, ds)
}

// TestConsumerPendingAcksIndex_AdversarialNoRaces hammers the index with
// repeated same-tag Store/Delete cycles that EXCEED the broker's per-tag
// contract. The strong index==primary invariant is not claimed under this
// pattern; this test instead validates that the underlying sync.Map operations
// are data-race-free (via `-race`) and never panic under arbitrary concurrency.
func TestConsumerPendingAcksIndex_AdversarialNoRaces(t *testing.T) {
	const tags = 200
	const iters = 80

	ds := NewDisruptorStorage()
	var wg sync.WaitGroup

	for tag := uint64(1); tag <= tags; tag++ {
		tag := tag
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = ds.StorePendingAck(newTestPendingAck("X", tag))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = ds.DeletePendingAck("", tag)
				_, _ = ds.GetConsumerPendingAcks("X")
			}
		}()
	}
	wg.Wait() // race detector validates the absence of data races here.
}

func assertIndexEqualsPrimary(t *testing.T, ds *DisruptorStorage) {
	t.Helper()

	primary := make(map[string]map[uint64]struct{})
	ds.pendingAcks.Range(func(_, val interface{}) bool {
		pa := val.(*protocol.PendingAck)
		if primary[pa.ConsumerTag] == nil {
			primary[pa.ConsumerTag] = make(map[uint64]struct{})
		}
		primary[pa.ConsumerTag][pa.DeliveryTag] = struct{}{}
		return true
	})

	// Direction 1: for every consumer the index knows, its indexed tag set must
	// equal the primary's tag set for that consumer. (An orphaned EMPTY inner
	// map is allowed and yields no tags.)
	ds.consumerPendingAcks.Range(func(cTag, innerVal interface{}) bool {
		idx := innerVal.(*sync.Map)
		indexed := make(map[uint64]struct{})
		idx.Range(func(k, _ interface{}) bool {
			indexed[k.(uint64)] = struct{}{}
			return true
		})
		want := primary[cTag.(string)]
		if len(want) == 0 {
			want = map[uint64]struct{}{}
		}
		assert.Equal(t, want, indexed,
			"index != primary for consumer %q", cTag)
		return true
	})

	// Direction 2 (safety-critical): every primary entry must be reachable via
	// its consumer's index -- GetConsumerPendingAcks must never miss a tag that
	// is still pending.
	for cTag, tags := range primary {
		got, err := ds.GetConsumerPendingAcks(cTag)
		require.NoError(t, err)
		gotSet := make(map[uint64]struct{}, len(got))
		for _, pa := range got {
			gotSet[pa.DeliveryTag] = struct{}{}
		}
		assert.Equal(t, tags, gotSet,
			"GetConsumerPendingAcks(%q) is missing tags present in the primary", cTag)
	}
}

func mustGetAllConsumers(t *testing.T, ds *DisruptorStorage) map[string][]*protocol.PendingAck {
	t.Helper()
	out := make(map[string][]*protocol.PendingAck)
	ds.consumerPendingAcks.Range(func(cTag, innerVal interface{}) bool {
		inner := innerVal.(*sync.Map)
		inner.Range(func(_, val interface{}) bool {
			out[cTag.(string)] = append(out[cTag.(string)], val.(*protocol.PendingAck))
			return true
		})
		return true
	})
	return out
}

func deliveryTags(acks []*protocol.PendingAck) []uint64 {
	out := make([]uint64, 0, len(acks))
	for _, a := range acks {
		out = append(out, a.DeliveryTag)
	}
	return out
}

func errPendingAckNotFoundSentinel(t *testing.T) error {
	t.Helper()
	ds := NewDisruptorStorage()
	_, err := ds.GetPendingAck("", 1<<30)
	require.Error(t, err)
	return err
}

// BenchmarkGetConsumerPendingAcks_Indexed measures the new O(M) lookup: with
// N=20000 total pending acks spread across 200 consumers (M=100 each), the
// lookup touches only the target consumer's inner map.
func BenchmarkGetConsumerPendingAcks_Indexed(b *testing.B) {
	const consumers = 200
	const tagsPerConsumer = 100

	ds := NewDisruptorStorage()
	for c := 0; c < consumers; c++ {
		ct := fmt.Sprintf("C%d", c)
		for tag := uint64(1); tag <= tagsPerConsumer; tag++ {
			_ = ds.StorePendingAck(newTestPendingAck(ct, uint64(c)*1000+tag))
		}
	}
	target := fmt.Sprintf("C%d", consumers/2)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = ds.GetConsumerPendingAcks(target)
	}
}

// BenchmarkGetConsumerPendingAcks_FullScanBaseline replicates the OLD O(N)
// implementation for direct comparison against the indexed version above.
func BenchmarkGetConsumerPendingAcks_FullScanBaseline(b *testing.B) {
	const consumers = 200
	const tagsPerConsumer = 100

	ds := NewDisruptorStorage()
	for c := 0; c < consumers; c++ {
		ct := fmt.Sprintf("C%d", c)
		for tag := uint64(1); tag <= tagsPerConsumer; tag++ {
			_ = ds.StorePendingAck(newTestPendingAck(ct, uint64(c)*1000+tag))
		}
	}
	target := fmt.Sprintf("C%d", consumers/2)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []*protocol.PendingAck
		ds.pendingAcks.Range(func(_, val interface{}) bool {
			pa := val.(*protocol.PendingAck)
			if pa.ConsumerTag == target {
				result = append(result, pa)
			}
			return true
		})
		_ = result
	}
}
