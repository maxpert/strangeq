package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatedAsyncStore wraps a real DisruptorStorage and HOLDS every durable
// completion until released, so tests can admit N publishes while the ready
// counters have not yet moved (the §7 race window). It still ring-stores
// synchronously, exactly like the real StoreMessageAsync.
type gatedAsyncStore struct {
	*storage.DisruptorStorage
	mu        sync.Mutex
	held      []func(error)
	failAsync atomic.Bool
}

func (g *gatedAsyncStore) StoreMessageAsync(queueName string, message *protocol.Message, onDurable func(error)) error {
	if g.failAsync.Load() {
		return fmt.Errorf("injected StoreMessageAsync failure")
	}
	// Ring-store synchronously (mirrors the real path's visible half).
	if err := g.DisruptorStorage.StoreMessage(queueName, message); err != nil {
		return err
	}
	g.mu.Lock()
	g.held = append(g.held, onDurable)
	g.mu.Unlock()
	return nil
}

func (g *gatedAsyncStore) releaseAll() {
	g.mu.Lock()
	held := g.held
	g.held = nil
	g.mu.Unlock()
	for _, cb := range held {
		cb(nil)
	}
}

func (g *gatedAsyncStore) heldCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.held)
}

func newGatedBroker(t *testing.T) (*StorageBroker, *gatedAsyncStore, func()) {
	t.Helper()
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	ds, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	gs := &gatedAsyncStore{DisruptorStorage: ds}
	b := NewStorageBroker(gs, ec)
	return b, gs, func() { b.Close(); _ = ds.Close() }
}

// TestAsyncRejectPublish_NoOvershoot (T7.1): with x-max-length=5 and
// x-overflow=reject-publish, 50 concurrent durable async publishes admitted
// BEFORE any completion fires must yield EXACTLY 5 accepts and 45
// ErrMaxLengthExceeded. Pre-fix mechanism: admission read WaitingCount()
// (still 0 behind the gate) for all 50, so all 50 were admitted.
func TestAsyncRejectPublish_NoOvershoot(t *testing.T) {
	b, gs, cleanup := newGatedBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(5), "x-overflow": "reject-publish",
	})

	const publishers = 50
	var accepted, refused atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := b.PublishMessageAsyncConfirm("", "q",
				&protocol.Message{Exchange: "", RoutingKey: "q", Body: []byte(fmt.Sprintf("m%d", i)), DeliveryMode: 2},
				func(error) {}, nil)
			if err == ErrMaxLengthExceeded {
				refused.Add(1)
			} else if err == nil {
				accepted.Add(1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(5), accepted.Load(), "exactly x-max-length admits (reservations close the race)")
	assert.Equal(t, int64(publishers-5), refused.Load(), "the rest are refused synchronously")
	assert.Equal(t, 5, gs.heldCount(), "every accepted publish has a held completion")

	// Release all completions: reservations transfer into the real counters.
	gs.releaseAll()
	require.Eventually(t, func() bool { return qs.WaitingCount() == 5 }, 5*time.Second, time.Millisecond)
	_, _, pending := b.QueueCounters("q")
	assert.Equal(t, int64(0), pending, "pending drains to zero once completions fire")
}

// TestAsyncRejectPublish_Bytes_NoOvershoot (T7.2): x-max-length-bytes with
// reject-publish: admitted bytes never exceed the limit modulo the
// at-or-over-empty-queue rule (one body always allowed past an empty queue).
func TestAsyncRejectPublish_Bytes_NoOvershoot(t *testing.T) {
	b, gs, cleanup := newGatedBroker(t)
	defer cleanup()

	// 100-byte bodies, limit 250 bytes, AT-OR-OVER-BEFORE-ADD semantics:
	// admissions observe 0, 100, 200 (all < 250) → three admitted (the queue
	// then holds 300); the fourth observes 300 >= 250 → refused.
	qs := declareMaxLen(t, b, "qb", map[string]interface{}{
		"x-max-length-bytes": int64(250), "x-overflow": "reject-publish",
	})
	_ = qs

	body := make([]byte, 100)
	const publishers = 20
	var accepted, refused atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := b.PublishMessageAsyncConfirm("", "qb",
				&protocol.Message{Exchange: "", RoutingKey: "qb", Body: body, DeliveryMode: 2},
				func(error) {}, nil)
			if err == ErrMaxLengthExceeded {
				refused.Add(1)
			} else if err == nil {
				accepted.Add(1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(3), accepted.Load(), "admitted bytes capped by the reservation, not the lagging counter")
	assert.Equal(t, int64(publishers-3), refused.Load())
	gs.releaseAll()
	require.Eventually(t, func() bool { return qs.ReadyBytes() == 300 }, 5*time.Second, time.Millisecond)
	_, _, pending := b.QueueCounters("qb")
	assert.Equal(t, int64(0), pending)
}

// TestRejectPublish_EnqueueFailureReleasesReservation (T7.3): when the async
// store's enqueue fails, the reservation is released (pending returns to 0)
// and later publishes are admitted.
func TestRejectPublish_EnqueueFailureReleasesReservation(t *testing.T) {
	b, gs, cleanup := newGatedBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "qe", map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "reject-publish",
	})

	// Fail the first enqueue AFTER admission reserved the slot.
	gs.failAsync.Store(true)
	_, _, err := b.PublishMessageAsyncConfirm("", "qe",
		&protocol.Message{Exchange: "", RoutingKey: "qe", Body: []byte("boom"), DeliveryMode: 2}, nil, nil)
	require.Error(t, err, "the injected enqueue error surfaces")
	gs.failAsync.Store(false)

	_, _, pending := b.QueueCounters("qe")
	assert.Equal(t, int64(0), pending, "the failed publish's reservation was released")

	// The next publish is admitted (the queue is not wedged at the cap).
	_, _, err = b.PublishMessageAsyncConfirm("", "qe",
		&protocol.Message{Exchange: "", RoutingKey: "qe", Body: []byte("ok"), DeliveryMode: 2}, func(error) {}, nil)
	require.NoError(t, err)
	gs.releaseAll()
	require.Eventually(t, func() bool { return qs.WaitingCount() == 1 }, 5*time.Second, time.Millisecond)
}

// TestRejectPublish_FrontierOutOfOrder_NoOvershoot (T7.5): two async publishes
// with tags t1<t2; complete t2 FIRST (t1 pending on the frontier), then a
// third admission at limit 2 must be refused — pending still counts t2 even
// though `waiting` has not moved (the contiguous-prefix pop is stuck behind
// t1). Pre-fix mechanism: admission read WaitingCount()==0 and admitted.
func TestRejectPublish_FrontierOutOfOrder_NoOvershoot(t *testing.T) {
	b, gs, cleanup := newGatedBroker(t)
	defer cleanup()

	declareMaxLen(t, b, "qo", map[string]interface{}{
		"x-max-length": int64(2), "x-overflow": "reject-publish",
	})

	// Admit t1 and t2 (completions held).
	_, _, err := b.PublishMessageAsyncConfirm("", "qo",
		&protocol.Message{Exchange: "", RoutingKey: "qo", Body: []byte("m1"), DeliveryMode: 2}, func(error) {}, nil)
	require.NoError(t, err)
	_, _, err = b.PublishMessageAsyncConfirm("", "qo",
		&protocol.Message{Exchange: "", RoutingKey: "qo", Body: []byte("m2"), DeliveryMode: 2}, func(error) {}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, gs.heldCount())

	// Complete t2 FIRST: the frontier pop is stuck behind pending t1, so
	// `waiting` stays 0 — but the reservation must still count t2.
	gs.mu.Lock()
	second := gs.held[1]
	gs.held = gs.held[:1]
	gs.mu.Unlock()
	second(nil)

	// Third admission must be REFUSED even though WaitingCount()==0.
	_, _, err = b.PublishMessageAsyncConfirm("", "qo",
		&protocol.Message{Exchange: "", RoutingKey: "qo", Body: []byte("m3"), DeliveryMode: 2}, func(error) {}, nil)
	assert.Equal(t, ErrMaxLengthExceeded, err, "pending reservations survive out-of-order completion")

	// Complete t1: both transfer; a new publish is admitted again.
	gs.releaseAll()
	qs := b.getOrCreateQueueState("qo")
	require.Eventually(t, func() bool { return qs.WaitingCount() == 2 }, 5*time.Second, time.Millisecond)
	_, _, pending := b.QueueCounters("qo")
	assert.Equal(t, int64(0), pending)
}

// TestTx_Publish_RejectPublish_Enforced (T7.4, replaces
// TestTx_RejectPublishAcceptedInsideTx; D7): tx publishes to a reject-publish
// queue at its cap are DROPPED (the refused copy never becomes visible, the tx
// proceeds — RabbitMQ silently drops a non-confirm reject-publish publish).
// Pre-fix mechanism: PublishMessageTx had NO admission at all.
func TestTx_Publish_RejectPublish_Enforced(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "reject-publish",
	})

	// Three tx publishes staged before any deferred runs: the first is
	// admitted (deferred closure), the other two are dropped with NO closure.
	var deferred []func(bool)
	for i := 0; i < 3; i++ {
		d, err := b.PublishMessageTx(b.storage, "", "q", &protocol.Message{
			Exchange: "", RoutingKey: "q", Body: []byte(fmt.Sprintf("m%d", i)),
		})
		require.NoError(t, err, "a dropped copy is not an error (D7: the tx proceeds)")
		deferred = append(deferred, d...)
	}
	assert.Len(t, deferred, 1, "exactly one copy was admitted (x-max-length=1)")

	for _, d := range deferred {
		d(true)
	}
	assert.Equal(t, int64(1), qs.WaitingCount(), "only the admitted copy becomes visible")
	got := drainBodies(t, b, "q")
	assert.Equal(t, []string{"m0"}, got, "the first staged copy survives")
}
