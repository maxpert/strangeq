package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSmallRingStorage builds storage with a 16-slot ring and a 50% spill
// threshold so a handful of durable publishes crosses into WAL-only (spilled)
// territory.
func newSmallRingStorage(t *testing.T) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		RingBufferSize:        16,
		SpillThresholdPercent: 50,
		WALBatchSize:          1000,
		WALBatchTimeoutMS:     10,
	})
	require.NoError(t, err)
	t.Cleanup(func() { ds.Close() })
	return ds
}

// storeDurable stores a durable message; past the spill threshold the ring
// stays empty and the message is WAL-only (spilled).
func storeDurable(t *testing.T, ds *DisruptorStorage, queue string, tag uint64) {
	t.Helper()
	require.NoError(t, ds.StoreMessage(queue, &protocol.Message{
		DeliveryTag:  tag,
		Body:         []byte(fmt.Sprintf("body-%d", tag)),
		DeliveryMode: 2,
	}))
}

// spillTags returns (ringResident, walOnly) tags after storing n durable
// messages, verified via the ring's own residency view.
func spillTags(t *testing.T, ds *DisruptorStorage, queue string, n int) (resident, spilled []uint64) {
	t.Helper()
	for i := 1; i <= n; i++ {
		storeDurable(t, ds, queue, uint64(i))
	}
	ring := ds.getQueueRing(queue)
	require.NotNil(t, ring)
	for i := 1; i <= n; i++ {
		if _, ok := ring.ring.LoadByTag(uint64(i)); ok {
			resident = append(resident, uint64(i))
		} else {
			spilled = append(spilled, uint64(i))
		}
	}
	require.NotEmpty(t, resident, "test premise: some messages must be ring-resident")
	require.NotEmpty(t, spilled, "test premise: some messages must be WAL-only (spilled)")
	return resident, spilled
}

// TestDeleteIfPresentDetailed_Outcomes (T8.2) pins the four-outcome contract:
//
//	removed=true,  wasResident=true : won the ring CAS
//	removed=false, wasResident=true : lost the ring CAS to a concurrent remover
//	removed=true,  wasResident=false: spilled/WAL-only, present, this call acked it
//	removed=false, wasResident=false: absent (gap tag, never stored, already acked)
//
// Pre-fix mechanism: DeleteMessageIfPresent returned the ring CAS result, so a
// spilled tag reported false even though it WAL-acked the record — and every
// caller gated its depth decrement on true, leaking depth for spilled messages.
func TestDeleteIfPresentDetailed_Outcomes(t *testing.T) {
	ds := newSmallRingStorage(t)
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: "q"}))

	resident, spilled := spillTags(t, ds, "q", 12)

	// Ring win.
	removed, wasResident, err := ds.DeleteMessageIfPresentDetailed("q", resident[0])
	require.NoError(t, err)
	assert.True(t, removed, "first remover of a ring-resident tag wins")
	assert.True(t, wasResident)

	// Ring loss (second call on the same tag): the message was durable, so the
	// first call acknowledged the WAL; the second call observes absent.
	removed, wasResident, err = ds.DeleteMessageIfPresentDetailed("q", resident[0])
	require.NoError(t, err)
	assert.False(t, removed, "second remover of the same tag loses")
	assert.False(t, wasResident)

	// Spilled present → (true, false): removed from the cold tiers.
	removed, wasResident, err = ds.DeleteMessageIfPresentDetailed("q", spilled[0])
	require.NoError(t, err)
	assert.True(t, removed, "first remover of a spilled tag wins via the cold path")
	assert.False(t, wasResident, "spilled tag was not ring-resident")

	// Spilled second call → (false, false): already acknowledged.
	removed, wasResident, err = ds.DeleteMessageIfPresentDetailed("q", spilled[0])
	require.NoError(t, err)
	assert.False(t, removed)
	assert.False(t, wasResident)

	// Never-stored tag → (false, false).
	removed, wasResident, err = ds.DeleteMessageIfPresentDetailed("q", 999)
	require.NoError(t, err)
	assert.False(t, removed)
	assert.False(t, wasResident)

	// Durable ring win acks the WAL.
	assert.True(t, ds.wal.IsAcknowledged(resident[0]), "durable ring win must ack the WAL")

	// Legacy wrapper keeps its contract: ring-CAS result only.
	ds2 := newSmallRingStorage(t)
	require.NoError(t, ds2.StoreQueue(&protocol.Queue{Name: "q"}))
	res2, spill2 := spillTags(t, ds2, "q", 12)
	legacyRemoved, err := ds2.DeleteMessageIfPresent("q", res2[0])
	require.NoError(t, err)
	assert.True(t, legacyRemoved, "legacy wrapper: ring win reports true")
	legacyRemoved, err = ds2.DeleteMessageIfPresent("q", spill2[0])
	require.NoError(t, err)
	assert.False(t, legacyRemoved, "legacy wrapper: spilled win reports false (ring CAS only)")
}

// TestDeleteIfPresentDetailed_ConcurrentSpilledExactlyOnce (T8.3): the
// per-tag stripe serialises concurrent removers of a spilled tag so exactly
// one observes removed=true.
func TestDeleteIfPresentDetailed_ConcurrentSpilledExactlyOnce(t *testing.T) {
	ds := newSmallRingStorage(t)
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: "q"}))

	const iterations = 200
	const racers = 32
	tag := uint64(0)
	for iter := 0; iter < iterations; iter++ {
		tag++
		// Keep every iteration's tag WAL-only: a fresh durable store past the
		// spill threshold is WAL-only by construction.
		storeDurable(t, ds, "q", tag)
		ring := ds.getQueueRing("q")
		if _, ok := ring.ring.LoadByTag(tag); ok {
			// Ring got drained below the threshold by nothing — delete the slot
			// so the tag is WAL-only for this iteration.
			ring.ring.Delete(tag)
		}

		var wins atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if removed, _, _ := ds.DeleteMessageIfPresentDetailed("q", tag); removed {
					wins.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		require.Equal(t, int64(1), wins.Load(), "iteration %d: exactly one remover of spilled tag %d", iter, tag)
	}
}

// TestDeleteIfPresentDetailed_ConcurrentRingExactlyOnce: the stripe also
// covers the ring path, so a durable ring winner's WAL ack cannot be
// double-issued by a loser reaching the cold path in between.
func TestDeleteIfPresentDetailed_ConcurrentRingExactlyOnce(t *testing.T) {
	ds := newSmallRingStorage(t)
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: "q"}))

	const tag = uint64(1)
	storeDurable(t, ds, "q", tag)
	ring := ds.getQueueRing("q")
	_, ok := ring.ring.LoadByTag(tag)
	require.True(t, ok, "test premise: tag is ring-resident")

	const racers = 32
	var wins, residents atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			removed, resident, _ := ds.DeleteMessageIfPresentDetailed("q", tag)
			if removed {
				wins.Add(1)
			}
			if resident {
				residents.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.Equal(t, int64(1), wins.Load(), "exactly one winner")
	assert.Equal(t, int64(1), residents.Load(), "exactly one observer of ring residency")
	assert.True(t, ds.wal.IsAcknowledged(tag))
}
