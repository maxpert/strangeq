package storage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestDeleteMessageIfPresent_SingleWinner is the linearization primitive behind
// the SQ-9 reaper/head-check: for a ring-resident tag, exactly one of many
// concurrent DeleteMessageIfPresent callers observes true. Without this, the
// reaper and a racing consumer could both decrement queue depth for the same
// waiting message.
func TestDeleteMessageIfPresent_SingleWinner(t *testing.T) {
	ds, err := NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: "q"}))

	const tag = uint64(1)
	require.NoError(t, ds.StoreMessage("q", &protocol.Message{DeliveryTag: tag, Body: []byte("x")}))

	const racers = 32
	var wins atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if removed, _ := ds.DeleteMessageIfPresent("q", tag); removed {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, int64(1), wins.Load(), "exactly one caller may remove a given ring tag")

	_, getErr := ds.GetMessage("q", tag)
	require.Error(t, getErr, "message must be gone after removal")
}

// TestDeleteMessageIfPresent_AbsentTagFalse verifies a tag never stored reports
// false (the reaper must not decrement depth for a non-existent tag).
func TestDeleteMessageIfPresent_AbsentTagFalse(t *testing.T) {
	ds, err := NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: "q"}))

	removed, err := ds.DeleteMessageIfPresent("q", 999)
	require.NoError(t, err)
	require.False(t, removed, "absent ring tag must report not-removed")
}
