package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func msgAt(tag uint64) *protocol.Message {
	return &protocol.Message{
		DeliveryTag: tag,
		Body:        []byte(fmt.Sprintf("msg-%d", tag)),
	}
}

func scanNonNil(r *AtomicRing) int {
	n := 0
	for i := range r.slots {
		if r.slots[i].Load() != nil {
			n++
		}
	}
	return n
}

func TestAtomicRing_StoreAndLoadByTag(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 5; tag++ {
		_, spilled, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
		require.False(t, spilled)
	}
	require.Equal(t, uint64(5), r.Count())
	for tag := uint64(1); tag <= 5; tag++ {
		got, ok := r.LoadByTag(tag)
		require.True(t, ok)
		require.Equal(t, tag, got.DeliveryTag)
		require.Equal(t, []byte(fmt.Sprintf("msg-%d", tag)), got.Body)
	}
}

func TestAtomicRing_StoreSpilledWhenSlotOccupied(t *testing.T) {
	r := NewAtomicRing(4)
	for tag := uint64(0); tag < 4; tag++ {
		_, spilled, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
		require.False(t, spilled)
	}
	require.Equal(t, uint64(4), r.Count())

	seq, spilled, err := r.Store(4, msgAt(4))
	require.NoError(t, err)
	require.True(t, spilled)
	require.Equal(t, uint64(4), seq)
	require.Equal(t, uint64(4), r.Count())

	got, ok := r.LoadByTag(0)
	require.True(t, ok)
	require.Equal(t, uint64(0), got.DeliveryTag)
}

func TestAtomicRing_LoadByTagReturnsFalseAfterDelete(t *testing.T) {
	r := NewAtomicRing(256)
	_, _, err := r.Store(7, msgAt(7))
	require.NoError(t, err)

	got, ok := r.LoadByTag(7)
	require.True(t, ok)
	require.NotNil(t, got)

	require.True(t, r.Delete(7))
	got, ok = r.LoadByTag(7)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestAtomicRing_LoadByTagReturnsFalseAfterWraparound(t *testing.T) {
	r := NewAtomicRing(4)
	_, _, err := r.Store(0, msgAt(0))
	require.NoError(t, err)
	require.True(t, r.Delete(0))

	for tag := uint64(1); tag < 4; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	_, _, err = r.Store(4, msgAt(4))
	require.NoError(t, err)

	got, ok := r.LoadByTag(0)
	require.False(t, ok)
	require.Nil(t, got)

	got, ok = r.LoadByTag(4)
	require.True(t, ok)
	require.Equal(t, uint64(4), got.DeliveryTag)
}

func TestAtomicRing_DeleteIdempotent_Sequential(t *testing.T) {
	r := NewAtomicRing(256)
	_, _, err := r.Store(10, msgAt(10))
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Count())

	require.True(t, r.Delete(10))
	require.Equal(t, uint64(0), r.Count())

	require.False(t, r.Delete(10))
	require.Equal(t, uint64(0), r.Count())
}

func TestAtomicRing_DeleteIdempotent_Concurrent(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(0); tag < 100; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(100), r.Count())

	var wg sync.WaitGroup
	var successes atomic.Int64
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.Delete(42) {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), successes.Load())
	require.Equal(t, uint64(99), r.Count())
}

func TestAtomicRing_DeleteTagGuard(t *testing.T) {
	r := NewAtomicRing(4)
	_, _, err := r.Store(1, msgAt(1))
	require.NoError(t, err)
	require.True(t, r.Delete(1))

	for tag := uint64(2); tag <= 4; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	_, _, err = r.Store(5, msgAt(5))
	require.NoError(t, err)

	r.tagMu.Lock()
	r.tagToSeq[1] = 0
	r.tagMu.Unlock()

	require.False(t, r.Delete(1))

	got, ok := r.LoadByTag(5)
	require.True(t, ok)
	require.Equal(t, uint64(5), got.DeliveryTag)
	require.Equal(t, uint64(4), r.Count())
}

func TestAtomicRing_MessageCountAccurate(t *testing.T) {
	r := NewAtomicRing(256)
	const N = 50
	for tag := uint64(1); tag <= N; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(N), r.Count())

	const M = 20
	for tag := uint64(1); tag <= M; tag++ {
		require.True(t, r.Delete(tag))
	}
	require.Equal(t, uint64(N-M), r.Count())
}

func TestAtomicRing_PurgeClearsAll(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 10; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(10), r.Count())

	removed := r.Purge()
	require.Equal(t, 10, removed)
	require.Equal(t, uint64(0), r.Count())
	for i := range r.slots {
		require.Nil(t, r.slots[i].Load())
	}
}

func TestAtomicRing_PowerOfTwoValidation(t *testing.T) {
	require.Panics(t, func() { NewAtomicRing(3) })
}

func TestAtomicRing_ConcurrentStoreDifferentTags(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	const G = 100
	var wg sync.WaitGroup
	for g := 1; g <= G; g++ {
		wg.Add(1)
		go func(tag uint64) {
			defer wg.Done()
			_, _, _ = r.Store(tag, msgAt(tag))
		}(uint64(g))
	}
	wg.Wait()

	require.Equal(t, uint64(G), r.Count())
	for g := 1; g <= G; g++ {
		got, ok := r.LoadByTag(uint64(g))
		require.True(t, ok, "tag %d should be present", g)
		require.Equal(t, uint64(g), got.DeliveryTag)
	}
}

func TestAtomicRing_ConcurrentStoreDeleteMixed(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(0); tag < 50; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(50), r.Count())

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(2)
		go func(tag uint64) {
			defer wg.Done()
			_, _, _ = r.Store(50+tag, msgAt(50+tag))
		}(uint64(g))
		go func(tag uint64) {
			defer wg.Done()
			r.Delete(tag)
		}(uint64(g))
	}
	wg.Wait()

	scanned := scanNonNil(r)
	require.Equal(t, uint64(scanned), r.Count())
}

func TestAtomicRing_GetAll(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 5; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	all := r.GetAll()
	require.Len(t, all, 5)

	tags := make(map[uint64]bool)
	for _, m := range all {
		tags[m.DeliveryTag] = true
	}
	for tag := uint64(1); tag <= 5; tag++ {
		require.True(t, tags[tag])
	}
}

func TestAtomicRing_GetRange(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 10; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	got := r.GetRange(3, 7)
	require.Len(t, got, 5)
	for _, m := range got {
		require.GreaterOrEqual(t, m.DeliveryTag, uint64(3))
		require.LessOrEqual(t, m.DeliveryTag, uint64(7))
	}
}

func TestAtomicRing_DeleteRange(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 10; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(10), r.Count())

	removed := r.DeleteRange(3, 7)
	require.Equal(t, 5, removed)
	require.Equal(t, uint64(5), r.Count())

	tags := make(map[uint64]bool)
	for _, m := range r.GetAll() {
		tags[m.DeliveryTag] = true
	}
	for tag := uint64(3); tag <= 7; tag++ {
		require.False(t, tags[tag])
	}
	require.True(t, tags[1])
	require.True(t, tags[10])
}

func TestAtomicRing_LoadBySeq(t *testing.T) {
	r := NewAtomicRing(256)
	seq, _, err := r.Store(42, msgAt(42))
	require.NoError(t, err)
	require.Equal(t, uint64(0), seq)

	got, ok := r.LoadBySeq(seq)
	require.True(t, ok)
	require.Equal(t, uint64(42), got.DeliveryTag)

	_, ok = r.LoadBySeq(999)
	require.False(t, ok)
}

func TestAtomicRing_LoadBySeqAfterWraparound(t *testing.T) {
	r := NewAtomicRing(4)
	r.Store(1, msgAt(1))
	r.Store(2, msgAt(2))
	r.Store(3, msgAt(3))
	r.Store(4, msgAt(4))
	r.Delete(1)
	r.Store(5, msgAt(5))

	got, ok := r.LoadBySeq(0)
	require.False(t, ok, "seq 0 was deleted, should not be found")

	got, ok = r.LoadBySeq(4)
	require.True(t, ok)
	require.Equal(t, uint64(5), got.DeliveryTag, "seq 4 should map to tag 5")
}

func TestAtomicRing_StoreNilMessageReturnsError(t *testing.T) {
	r := NewAtomicRing(256)
	_, _, err := r.Store(1, nil)
	require.Error(t, err)
	require.Equal(t, uint64(0), r.Count())
}

func TestAtomicRing_StoreAfterCloseReturnsError(t *testing.T) {
	r := NewAtomicRing(256)
	r.Close()
	_, _, err := r.Store(1, msgAt(1))
	require.Error(t, err)
	require.Equal(t, uint64(0), r.Count())
}

func TestAtomicRing_StoreZeroAllocs(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	msg := msgAt(1)
	_, _, _ = r.Store(1, msg)

	allocs := testing.AllocsPerRun(200, func() {
		_, _, _ = r.Store(1, msg)
	})
	require.Equal(t, float64(0), allocs)
}

func TestAtomicRing_DeleteZeroAllocs(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	for i := 0; i < 256; i++ {
		_, _, _ = r.Store(uint64(i), msgAt(uint64(i)))
	}

	var i uint64
	allocs := testing.AllocsPerRun(200, func() {
		r.Delete(i)
		i++
	})
	require.Equal(t, float64(0), allocs)
}

func TestAtomicRing_LoadZeroAllocs(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	_, _, _ = r.Store(1, msgAt(1))

	allocs := testing.AllocsPerRun(200, func() {
		_, _ = r.LoadByTag(1)
	})
	require.Equal(t, float64(0), allocs)
}
