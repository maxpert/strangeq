package storage

import (
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C9: a Write that enqueues after the batch writer's final drain used to
// hang forever. Write/WriteAsync/WriteSharedAsync select between
// `writeChan <- req` and `<-stopChan`; after close(stopChan) both cases can
// be ready and Go picks one at random. A send that wins after the writer's
// stop arm drained once and returned sits in the buffered channel forever:
// Write blocks on <-doneChan and an async onDone never fires.

// TestWAL_WriteAfterClose_NoHang (TDD test 29).
//
// 20 rounds; each round starts 1000 goroutines that loop Write/WriteAsync/
// WriteSharedAsync (chosen by i%3) until they get an error, while the manager
// is Closed after a random 0–2ms. Every goroutine must return within 5s per
// round; every async onDone must fire exactly once; the process's goroutine
// count must return to baseline afterwards (a hung sender leaks a goroutine
// blocked on <-doneChan forever); and every error observed after close must
// satisfy errors.Is(err, ErrWALClosed).
func TestWAL_WriteAfterClose_NoHang(t *testing.T) {
	rng := rand.New(rand.NewSource(20261022))

	baseline := runtime.NumGoroutine()

	for round := 0; round < 20; round++ {
		cfg := DefaultWALConfig()
		cfg.SyncDisabled = true // the race under test is enqueue-vs-close, not durability
		wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
		require.NoError(t, err)

		var fired atomic.Int64    // total onDone callbacks that fired
		var asyncCalls atomic.Int64 // total WriteAsync invocations (each fires exactly one onDone)
		var sharedOK atomic.Int64  // total WriteSharedAsync nil (enqueued) returns, each firing one sub onDone

		var wg sync.WaitGroup
		closeNow := time.Duration(rng.Intn(3)) * time.Millisecond
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for tag := uint64(1); ; tag++ {
					msg := &protocol.Message{Body: []byte("x"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: tag}
					switch i % 3 {
					case 0: // Write (synchronous)
						if err := wm.Write("q", msg, tag); err != nil {
							if !errors.Is(err, ErrWALClosed) {
								t.Errorf("round %d: Write after close returned %v, want ErrWALClosed", round, err)
							}
							return
						}
					case 1: // WriteAsync: wait for this call's callback, stop on error
						asyncCalls.Add(1)
						cb := make(chan error, 1)
						wm.WriteAsync("q", msg, tag, func(e error) {
							fired.Add(1)
							select {
							case cb <- e:
							default:
							}
						})
						e := <-cb
						if e != nil {
							if !errors.Is(e, ErrWALClosed) {
								t.Errorf("round %d: WriteAsync completion after close returned %v, want ErrWALClosed", round, e)
							}
							return
						}
					default: // WriteSharedAsync
						subs := []sharedSub{{queueName: "q", offset: tag, message: msg, onDone: func(e error) {
							fired.Add(1)
							if e != nil && !errors.Is(e, ErrWALClosed) {
								t.Errorf("round %d: shared onDone after close returned %v, want ErrWALClosed", round, e)
							}
						}}}
						if err := wm.WriteSharedAsync(subs, []byte("x"), 1); err != nil {
							if !errors.Is(err, ErrWALClosed) {
								t.Errorf("round %d: WriteSharedAsync after close returned %v, want ErrWALClosed", round, err)
							}
							return
						}
						sharedOK.Add(1)
					}
				}
			}(i)
		}

		// Close while the 1000 writers are in flight.
		time.Sleep(closeNow)
		require.NoError(t, wm.Close())

		finished := make(chan struct{})
		go func() {
			wg.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: writers still blocked 5s after Close (Write-after-Close hang)", round)
			return
		}

		// Every enqueued async/shared call fired exactly one completion: a
		// callback lost to the race (enqueued but never drained) undercounts,
		// a callback double-fired overcounts.
		require.Equal(t, asyncCalls.Load()+sharedOK.Load(), fired.Load(),
			"round %d: async completions fired %d, want %d (one per enqueued call)", round, fired.Load(), asyncCalls.Load()+sharedOK.Load())
	}

	// A hung sender leaks a goroutine (blocked on <-doneChan) that no Close
	// can reclaim. Allow a small slack for runtime-internal goroutines.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline+5
	}, 2*time.Second, 25*time.Millisecond, "goroutines leaked by Write-after-Close: baseline %d, now %d",
		baseline, runtime.NumGoroutine())
}

// TestWAL_EnqueueAfterClosing_ReturnsErrWALClosed (TDD test 30) pins the
// deterministic contract: once Close has begun, enqueue returns ErrWALClosed
// immediately, the request's done channel is untouched, and the transaction
// slow path refuses too.
func TestWAL_EnqueueAfterClosing_ReturnsErrWALClosed(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)

	sw := wm.sharedWAL
	require.NotNil(t, sw)
	require.NoError(t, wm.Close())

	req := &writeRequest{
		queueName: "q",
		message:   &protocol.Message{Body: []byte("late"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 1},
		offset:    1,
		done:      make(chan error, 1),
	}
	err = sw.enqueue(req)
	require.ErrorIs(t, err, ErrWALClosed, "enqueue after Close must fail fast with ErrWALClosed")

	select {
	case e := <-req.done:
		t.Fatalf("enqueue after Close must not touch the request's done channel, got %v", e)
	default:
	}

	err = sw.writeTxAtomic([]*RecoveryMessage{{
		QueueName: "q",
		Message:   &protocol.Message{Body: []byte("late-tx"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 2},
		Offset:    2,
	}})
	require.ErrorIs(t, err, ErrWALClosed, "writeTxAtomic after Close must refuse with ErrWALClosed")
}
