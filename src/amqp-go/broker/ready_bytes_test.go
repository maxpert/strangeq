package broker

import (
	"sync"
	"testing"
)

// TestQueueState_ReadyBytes_ZeroByDefault verifies the SQ-11 byte counter is
// zero on a freshly created QueueState (nothing has wired it yet).
func TestQueueState_ReadyBytes_ZeroByDefault(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()
	if got := qs.ReadyBytes(); got != 0 {
		t.Fatalf("ReadyBytes() = %d, want 0", got)
	}
}

// TestQueueState_ReadyBytes_AddSub exercises the add/sub accessors used by SQ-11
// to track ready-message bytes for x-max-length-bytes enforcement.
func TestQueueState_ReadyBytes_AddSub(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()

	if got := qs.AddReadyBytes(100); got != 100 {
		t.Fatalf("AddReadyBytes(100) = %d, want 100", got)
	}
	if got := qs.AddReadyBytes(50); got != 150 {
		t.Fatalf("AddReadyBytes(50) = %d, want 150", got)
	}
	if got := qs.ReadyBytes(); got != 150 {
		t.Fatalf("ReadyBytes() = %d, want 150", got)
	}
	if got := qs.SubReadyBytes(120); got != 30 {
		t.Fatalf("SubReadyBytes(120) = %d, want 30", got)
	}
	if got := qs.ReadyBytes(); got != 30 {
		t.Fatalf("ReadyBytes() = %d, want 30", got)
	}
}

// TestQueueState_ReadyBytes_ConcurrentAtomic proves the counter is a real atomic
// (no lost updates under concurrent add/sub) — SQ-11 mutates it from the publish
// path and the claim/ack path concurrently.
func TestQueueState_ReadyBytes_ConcurrentAtomic(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()

	const goroutines = 8
	const perG = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				qs.AddReadyBytes(10)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				qs.SubReadyBytes(10)
			}
		}()
	}
	wg.Wait()

	if got := qs.ReadyBytes(); got != 0 {
		t.Fatalf("ReadyBytes() after balanced add/sub = %d, want 0", got)
	}
}
