package broker

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeclareQueue_MaxQueues_ConcurrentNeverExceeds (T15.2, -race): with a
// limit of 10, 100 goroutines declaring DISTINCT names yield exactly 10
// successes and 90 ErrQueueLimitReached; deletions free slots.
func TestDeclareQueue_MaxQueues_ConcurrentNeverExceeds(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	b.SetTopologyLimits(10, 0)

	const declarers = 100
	var succeeded, limited sync.Map
	var wg sync.WaitGroup
	for i := 0; i < declarers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("q-%d", i)
			_, err := b.DeclareQueue(name, false, false, false, nil)
			if err == nil {
				succeeded.Store(name, true)
			} else if errors.Is(err, ErrQueueLimitReached) {
				limited.Store(name, true)
			} else {
				t.Errorf("unexpected error for %s: %v", name, err)
			}
		}(i)
	}
	wg.Wait()

	var nOk, nLimited int
	succeeded.Range(func(_, _ interface{}) bool { nOk++; return true })
	limited.Range(func(_, _ interface{}) bool { nLimited++; return true })
	assert.Equal(t, 10, nOk, "exactly the limit succeeds")
	assert.Equal(t, declarers-10, nLimited, "the rest get ErrQueueLimitReached")

	// Redeclaring an existing queue at the limit is fine (it was already counted).
	succeeded.Range(func(k, _ interface{}) bool {
		_, err := b.DeclareQueue(k.(string), false, false, false, nil)
		assert.NoError(t, err, "redeclare of an existing queue never hits the limit")
		return false // one is enough
	})

	// Deleting frees a slot. (Track deleted names so each delete is DISTINCT —
	// deleting a missing queue decrements nothing.)
	var deleted []string
	succeeded.Range(func(k, _ interface{}) bool {
		name := k.(string)
		_, err := b.DeleteQueue(name, false, false)
		require.NoError(t, err)
		deleted = append(deleted, name)
		succeeded.Delete(name)
		return false
	})
	_, err := b.DeclareQueue("q-after-delete", false, false, false, nil)
	assert.NoError(t, err, "a delete frees a slot")

	// The counter tracks reality: back at the limit after q-after-delete.
	_, err = b.DeclareQueue("q-one-more", false, false, false, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrQueueLimitReached), "%v", err)
	assert.Contains(t, err.Error(), "cannot declare queue 'q-one-more': queue limit in cluster (10) is reached")

	// Another (distinct) delete frees a slot again.
	succeeded.Range(func(k, _ interface{}) bool {
		name := k.(string)
		_, derr := b.DeleteQueue(name, false, false)
		require.NoError(t, derr)
		succeeded.Delete(name)
		return false
	})
	_, err = b.DeclareQueue("q-over", false, false, false, nil)
	assert.NoError(t, err)
}

// TestDeclareQueue_MaxQueues_SeededFromStorage: the counter is seeded from
// the metadata store at broker construction (a restart doesn't reset it).
func TestDeclareQueue_MaxQueues_SeededFromStorage(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		_, err := b.DeclareQueue(fmt.Sprintf("seed-%d", i), true, false, false, nil)
		require.NoError(t, err)
	}
	// Simulate a restart: a NEW broker over the SAME storage.
	b2 := NewStorageBroker(b.storage, b.engineConfig)
	defer b2.Close()
	b2.SetTopologyLimits(3, 0)

	_, err := b2.DeclareQueue("seed-3", false, false, false, nil)
	assert.True(t, errors.Is(err, ErrQueueLimitReached), "seeded count: %v", err)
	// Redeclare of an existing one is fine.
	_, err = b2.DeclareQueue("seed-0", true, false, false, nil)
	assert.NoError(t, err)
}

// TestMaxExchanges_Enforced_DefaultsExcluded (T15.3): the default exchange
// and amq.* are NOT counted; user exchanges are.
func TestMaxExchanges_Enforced_DefaultsExcluded(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	b.SetTopologyLimits(0, 1)

	// The predeclared amq.* exchanges don't consume the budget: one user
	// exchange is still allowed.
	require.NoError(t, b.DeclareExchange("ux-1", "direct", false, false, false, nil))
	err := b.DeclareExchange("ux-2", "direct", false, false, false, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExchangeLimitReached), "%v", err)
	assert.Contains(t, err.Error(), "cannot declare exchange 'ux-2': exchange limit in cluster (1) is reached")

	// Redeclare of an existing exchange is fine at the limit.
	require.NoError(t, b.DeclareExchange("ux-1", "direct", false, false, false, nil))

	// Delete frees the slot.
	require.NoError(t, b.DeleteExchange("ux-1", false))
	require.NoError(t, b.DeclareExchange("ux-2", "direct", false, false, false, nil))
}

// TestTopologyLimits_ZeroUnlimited: 0 means unlimited (the default).
func TestTopologyLimits_ZeroUnlimited(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	b.SetTopologyLimits(0, 0)

	for i := 0; i < 20; i++ {
		_, err := b.DeclareQueue(fmt.Sprintf("uq-%d", i), false, false, false, nil)
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
		require.NoError(t, b.DeclareExchange(fmt.Sprintf("ux-%d", i), "direct", false, false, false, nil))
	}
}
