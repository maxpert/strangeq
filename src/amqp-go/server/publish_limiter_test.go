package server

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// limiterArrivalGate pairs with LimiterHooks.BeforeReserve to serialize
// Reserve arrivals deterministically: the hook (running under the limiter
// mutex) announces each arrival and blocks until the test lets it proceed, so
// FIFO enqueue order is exactly the test's schedule order.
type limiterArrivalGate struct {
	req chan string // connID of the arrival waiting in BeforeReserve
	go_ chan struct{}
}

func newLimiterArrivalGate() *limiterArrivalGate {
	return &limiterArrivalGate{req: make(chan string), go_: make(chan struct{})}
}

func (g *limiterArrivalGate) hook(connID string, _ int64) {
	g.req <- connID
	<-g.go_
}

// admit waits for the next arrival and lets it proceed.
func (g *limiterArrivalGate) admit(t *testing.T) string {
	t.Helper()
	select {
	case id := <-g.req:
		g.go_ <- struct{}{}
		return id
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a limiter arrival")
		return ""
	}
}

// L-1: a seeded 10k-step schedule of arrivals and releases: grant order equals
// arrival order, and used <= G unless an oversized lease is held.
func TestLimiter_FIFOAndBudgetInvariant(t *testing.T) {
	const G = int64(1 << 20)
	const steps = 10000
	l := NewPublishLimiter(G)
	defer l.Close()

	gate := newLimiterArrivalGate()
	var grantMu sync.Mutex
	var grantOrder []string
	l.hooks = &LimiterHooks{
		BeforeReserve: gate.hook,
		OnGrant: func(connID string, bytes int64, oversized bool, used int64, waiters int) {
			if !oversized && used > G {
				t.Errorf("used %d exceeds budget %d after non-oversized grant of %d", used, G, bytes)
			}
			grantMu.Lock()
			grantOrder = append(grantOrder, connID)
			grantMu.Unlock()
		},
	}

	rng := rand.New(rand.NewSource(1))
	type grantItem struct {
		id  string
		tok protocol.PublishLeaseToken
	}
	grantCh := make(chan grantItem)
	var wg sync.WaitGroup

	var arrivalOrder []string
	var held []grantItem
	arrivals := 0
	for step := 0; step < steps; step++ {
		// Collect freshly granted leases.
		for {
			select {
			case g := <-grantCh:
				held = append(held, g)
			default:
				goto drained
			}
		}
	drained:
		if len(held) > 0 && rng.Intn(100) < 40 {
			held[0].tok.Abort()
			held[0].tok = protocol.PublishLeaseToken{}
			held = held[1:]
			continue
		}
		// Arrival. The connID carries the arrival index so the FIFO check is
		// exact (each arrival is a distinct "connection" for the limiter).
		connID := fmt.Sprintf("arr-%d", arrivals)
		bytes := int64(1 + rng.Intn(int(G/4)))
		if rng.Intn(200) == 0 {
			bytes = 2 * G // rare oversized request
		}
		arrivalOrder = append(arrivalOrder, connID)
		arrivals++
		wg.Add(1)
		go func(id string, b int64) {
			defer wg.Done()
			le, err := l.Reserve(context.Background(), id, b)
			if err != nil {
				return
			}
			grantCh <- grantItem{id: id, tok: le}
		}(connID, bytes)
		got := gate.admit(t)
		require.Equal(t, connID, got, "arrival order must match the schedule")
	}

	// Drain: release everything still held, then keep releasing each new grant
	// until every goroutine has completed. grantCh is unbuffered, so once
	// wg.Wait() returns every granted lease has been received (and released)
	// by this loop.
	for _, g := range held {
		g.tok.Abort()
	}
	held = nil
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		select {
		case g := <-grantCh:
			g.tok.Abort()
		case <-done:
			goto finished
		}
	}
finished:

	grantMu.Lock()
	assert.Equal(t, arrivalOrder, grantOrder, "grants must be strict FIFO = arrival order")
	grantMu.Unlock()
	assert.Len(t, grantOrder, arrivals)
	used, _, waiters := l.Stats()
	assert.Equal(t, int64(0), used)
	assert.Equal(t, 0, waiters)
}

// L-2: the head waiter needs 3 with only 2 free; a later 1-unit waiter that
// WOULD fit must still wait (no barging).
func TestLimiter_NoBarging(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()
	ctx := context.Background()

	a, err := l.Reserve(ctx, "a", 8) // used=8
	require.NoError(t, err)

	gate := newLimiterArrivalGate()
	l.hooks = &LimiterHooks{BeforeReserve: gate.hook}

	bDone := make(chan protocol.PublishLeaseToken, 1)
	go func() {
		le, err := l.Reserve(ctx, "b", 3)
		if err == nil {
			bDone <- le
		}
	}()
	require.Equal(t, "b", gate.admit(t))

	cDone := make(chan protocol.PublishLeaseToken, 1)
	go func() {
		le, err := l.Reserve(ctx, "c", 1)
		if err == nil {
			cDone <- le
		}
	}()
	require.Equal(t, "c", gate.admit(t))

	// c fits (8+1 <= 10) but must not barge ahead of b.
	select {
	case <-cDone:
		t.Fatal("c barged ahead of the head waiter")
	default:
	}

	a.Abort() // used=0 → b granted (3)
	<-bDone
	// c still waits: 3+1 <= 10 actually fits now — b holds 3, so c is granted.
	<-cDone
	used, _, _ := l.Stats()
	assert.Equal(t, int64(4), used)
}

// L-3: a >G lease is granted only at used==0; nothing else is granted while it
// is held; oversized grants are globally serial.
func TestLimiter_OversizedSerialized(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()
	ctx := context.Background()

	a, err := l.Reserve(ctx, "a", 10) // used=10 (full)
	require.NoError(t, err)

	gate := newLimiterArrivalGate()
	l.hooks = &LimiterHooks{BeforeReserve: gate.hook}

	bDone := make(chan protocol.PublishLeaseToken, 1)
	go func() {
		if le, err := l.Reserve(ctx, "b", 20); err == nil { // oversized
			bDone <- le
		}
	}()
	require.Equal(t, "b", gate.admit(t))

	cDone := make(chan protocol.PublishLeaseToken, 1)
	go func() {
		if le, err := l.Reserve(ctx, "c", 5); err == nil {
			cDone <- le
		}
	}()
	require.Equal(t, "c", gate.admit(t))

	// Oversized b must wait: used != 0.
	select {
	case <-bDone:
		t.Fatal("oversized grant while used != 0")
	default:
	}
	// TryReserve never barges and cannot grant while b is queued.
	_, ok := l.TryReserve("t", 1)
	assert.False(t, ok)

	a.Abort() // used=0 → b (oversized) granted
	bLease := <-bDone
	used, _, _ := l.Stats()
	assert.Equal(t, int64(20), used, "oversized lease may exceed G only from used==0")

	// While the oversized lease is held nothing else is granted.
	_, ok = l.TryReserve("t2", 1)
	assert.False(t, ok)
	select {
	case <-cDone:
		t.Fatal("normal grant issued while an oversized lease is held")
	default:
	}

	// Releasing the oversized lease grants the next waiter; oversized grants
	// are globally serial.
	bLease.Abort()
	cLease := <-cDone
	used, _, _ = l.Stats()
	assert.Equal(t, int64(5), used)

	dDone := make(chan protocol.PublishLeaseToken, 1)
	go func() {
		if le, err := l.Reserve(ctx, "d", 20); err == nil { // second oversized
			dDone <- le
		}
	}()
	require.Equal(t, "d", gate.admit(t))
	select {
	case <-dDone:
		t.Fatal("second oversized grant while used != 0")
	default:
	}
	cLease.Abort()
	dLease := <-dDone // now used==0, so the oversized grant proceeds
	used, _, _ = l.Stats()
	assert.Equal(t, int64(20), used)
	dLease.Abort()
	used, _, waiters := l.Stats()
	assert.Equal(t, int64(0), used)
	assert.Equal(t, 0, waiters)
}

// L-4: cancellation removes the waiter; a cancel racing a grant returns the
// bytes and promotes the next waiter; waiters == 0 afterwards.
func TestLimiter_CancelWhileWaitingAndRacingGrant(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()
	ctx := context.Background()

	// Plain removal on cancel.
	a, err := l.Reserve(ctx, "a", 10)
	require.NoError(t, err)
	bCtx, bCancel := context.WithCancel(ctx)
	bErr := make(chan error, 1)
	go func() {
		_, err := l.Reserve(bCtx, "b", 5)
		bErr <- err
	}()
	require.Eventually(t, func() bool {
		_, _, w := l.Stats()
		return w == 1
	}, 30*time.Second, time.Millisecond)
	bCancel()
	require.ErrorIs(t, <-bErr, context.Canceled)
	_, _, w := l.Stats()
	assert.Equal(t, 0, w)

	// Racing grant: cancel b2 while a release grants it; the granted bytes must
	// come back and promote c.
	b2Ctx, b2Cancel := context.WithCancel(ctx)
	b2Err := make(chan error, 1)
	go func() {
		_, err := l.Reserve(b2Ctx, "b2", 6)
		b2Err <- err
	}()
	cDone := make(chan protocol.PublishLeaseToken, 1)
	go func() {
		if le, err := l.Reserve(ctx, "c", 5); err == nil {
			cDone <- le
		}
	}()
	require.Eventually(t, func() bool {
		_, _, w := l.Stats()
		return w == 2
	}, 30*time.Second, time.Millisecond)

	b2Cancel()
	a.Abort() // grants b2 (racing the cancel) or c directly
	require.ErrorIs(t, <-b2Err, context.Canceled)
	cLease := <-cDone // the 6 bytes must have come back for c (6+5 > 10)
	used, _, w := l.Stats()
	assert.Equal(t, int64(5), used, "a racing grant must return its bytes")
	assert.Equal(t, 0, w)
	cLease.Abort()
	used, _, _ = l.Stats()
	assert.Equal(t, int64(0), used)
}

// L-5: double or nil release has no effect; used is never negative.
func TestLimiter_ReleaseIdempotent(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()

	le, err := l.Reserve(context.Background(), "a", 5)
	require.NoError(t, err)
	le.Abort()
	le.Abort()
	le.Abort()
	le.Abort()
	used, _, _ := l.Stats()
	assert.Equal(t, int64(0), used, "double release must not drive used negative")

	var zero protocol.PublishLeaseToken
	zero.Abort() // must not panic
	zero.Abort() // idempotent
	assert.False(t, zero.Valid(), "the zero token is never valid")
	assert.Equal(t, tokenStateFree, l.tokenStateForTest(zero), "the zero token is always FREE")
}

// L-6: 1000 waiters (one per connection), each granted once in FIFO order;
// used <= G throughout.
func TestLimiter_1000ConnRoundRobin(t *testing.T) {
	const n = 1000
	const G = int64(64)
	l := NewPublishLimiter(G)
	defer l.Close()

	gate := newLimiterArrivalGate()
	var grantMu sync.Mutex
	var grantOrder []string
	l.hooks = &LimiterHooks{
		BeforeReserve: gate.hook,
		OnGrant: func(connID string, bytes int64, oversized bool, used int64, waiters int) {
			if used > G {
				t.Errorf("used %d exceeds budget %d", used, G)
			}
			grantMu.Lock()
			grantOrder = append(grantOrder, connID)
			grantMu.Unlock()
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("conn-%04d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			le, err := l.Reserve(context.Background(), id, G)
			if err != nil {
				return
			}
			le.Abort()
		}()
		require.Equal(t, id, gate.admit(t))
	}
	wg.Wait()

	grantMu.Lock()
	require.Len(t, grantOrder, n)
	for i, id := range grantOrder {
		assert.Equal(t, fmt.Sprintf("conn-%04d", i), id, "grant %d out of FIFO order", i)
	}
	grantMu.Unlock()
	used, _, waiters := l.Stats()
	assert.Equal(t, int64(0), used)
	assert.Equal(t, 0, waiters)
}

// L-7: budget -1 (disabled): 1e6 Reserve calls never block.
func TestLimiter_DisabledNeverBlocks(t *testing.T) {
	l := NewPublishLimiter(-1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1_000_000; i++ {
			le, err := l.Reserve(context.Background(), "c", 1<<30)
			if err != nil {
				t.Errorf("disabled limiter returned error: %v", err)
				return
			}
			le.Abort()
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("disabled limiter blocked")
	}
}

// L-8: Close fails waiters with ErrLimiterClosed; later Reserve/TryReserve fail.
func TestLimiter_CloseFailsWaiters(t *testing.T) {
	l := NewPublishLimiter(10)
	a, err := l.Reserve(context.Background(), "a", 10)
	require.NoError(t, err)

	errs := make(chan error, 2)
	for _, id := range []string{"b", "c"} {
		go func(id string) {
			_, err := l.Reserve(context.Background(), id, 5)
			errs <- err
		}(id)
	}
	require.Eventually(t, func() bool {
		_, _, w := l.Stats()
		return w == 2
	}, 30*time.Second, time.Millisecond)

	l.Close()
	require.ErrorIs(t, <-errs, ErrLimiterClosed)
	require.ErrorIs(t, <-errs, ErrLimiterClosed)

	_, err = l.Reserve(context.Background(), "d", 1)
	assert.ErrorIs(t, err, ErrLimiterClosed)
	_, ok := l.TryReserve("d", 1)
	assert.False(t, ok)
	a.Abort() // outstanding lease still releases normally
	used, _, _ := l.Stats()
	assert.Equal(t, int64(0), used)
}

// L-9: 64 goroutines hammering reserve/release; final used == 0, waiters == 0.
func TestLimiter_ConcurrentRace(t *testing.T) {
	l := NewPublishLimiter(1 << 16)
	defer l.Close()
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			id := fmt.Sprintf("g%d", g)
			for i := 0; i < 200; i++ {
				b := int64(1 + rng.Intn(1<<15))
				le, err := l.Reserve(context.Background(), id, b)
				if err != nil {
					return
				}
				le.Abort()
			}
		}(g)
	}
	wg.Wait()
	used, _, waiters := l.Stats()
	assert.Equal(t, int64(0), used)
	assert.Equal(t, 0, waiters)
}

// L-10: the exact leaseBytes formula across body sizes and frame_max values.
func TestLeaseBytes_Formula(t *testing.T) {
	cases := []struct {
		body     uint64
		frameMax uint32
		want     int64
	}{
		{0, 131072, 512},                        // no body frames: fixed cost only
		{1, 131072, 1 + 72 + 512},               // one frame
		{65536, 131072, 65536 + 72 + 512},       // single full frame
		{131064, 131072, 131064 + 72 + 512},     // exactly one frame of payload
		{131065, 131072, 131065 + 144 + 512},    // spills into a second frame
		{65536, 4096, 65536 + 17*72 + 512},      // 17 frames at 4088 payload each
		{1048576, 131072, 1048576 + 9*72 + 512}, // 9 frames (8 full + 8 bytes)
		{100, 8, 100 + 100*72 + 512},            // degenerate frame_max
		{16777216, 131072, 16777216 + 129*72 + 512},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, leaseBytes(tc.body, tc.frameMax),
			"body=%d frameMax=%d", tc.body, tc.frameMax)
		// Independently recomputed with the spec formula.
		perFrame := uint64(tc.frameMax)
		if perFrame > 8 {
			perFrame -= 8
		} else {
			perFrame = 1
		}
		frames := (tc.body + perFrame - 1) / perFrame
		assert.Equal(t, int64(tc.body+frames*72+512), leaseBytes(tc.body, tc.frameMax))
	}
}

// L-11: TryReserve never barges: fits-but-FIFO-non-empty → false; empty and
// fits → true; oversized only at used==0 with an empty FIFO.
func TestLimiter_TryReserveNonBarging(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()
	ctx := context.Background()

	a, err := l.Reserve(ctx, "a", 8)
	require.NoError(t, err)
	defer a.Abort()

	// Queue a waiter so the FIFO is non-empty.
	bDone := make(chan error, 1)
	go func() {
		_, err := l.Reserve(ctx, "b", 3)
		bDone <- err
	}()
	require.Eventually(t, func() bool {
		_, _, w := l.Stats()
		return w == 1
	}, 30*time.Second, time.Millisecond)

	// Fits (8+1<=10) but the FIFO is non-empty → false.
	_, ok := l.TryReserve("t", 1)
	assert.False(t, ok, "TryReserve must not barge ahead of a queued waiter")

	l.Close() // fail the waiter to clear the FIFO
	require.ErrorIs(t, <-bDone, ErrLimiterClosed)

	l2 := NewPublishLimiter(10)
	defer l2.Close()
	// Empty FIFO and fits → true.
	x, ok := l2.TryReserve("x", 4)
	require.True(t, ok)
	// Oversized at used != 0 → false.
	_, ok = l2.TryReserve("y", 20)
	assert.False(t, ok)
	x.Abort()
	// Oversized at used == 0 with an empty FIFO → true.
	y, ok := l2.TryReserve("y", 20)
	require.True(t, ok, "oversized TryReserve must grant at used==0 with an empty FIFO")
	y.Abort()
	used, _, _ := l2.Stats()
	assert.Equal(t, int64(0), used)
}

// L-12: Close with 3 outstanding leases; releasing them brings used to 0; no
// grant after Close.
func TestLimiter_CloseOutstandingReleaseNormally(t *testing.T) {
	l := NewPublishLimiter(1 << 20)
	ctx := context.Background()
	leases := make([]protocol.PublishLeaseToken, 3)
	for i := range leases {
		le, err := l.Reserve(ctx, fmt.Sprintf("c%d", i), 1000)
		require.NoError(t, err)
		leases[i] = le
	}
	l.Close()
	for i, le := range leases {
		le.Abort()
		used, _, _ := l.Stats()
		assert.Equal(t, int64(2000-1000*i), used, "outstanding leases must still release after Close")
	}
	_, err := l.Reserve(ctx, "late", 1)
	assert.ErrorIs(t, err, ErrLimiterClosed)
	_, ok := l.TryReserve("late", 1)
	assert.False(t, ok)
}

// L-13: the disabled limiter allocates nothing on Reserve/TryReserve + Abort.
func TestLimiter_DisabledZeroAlloc(t *testing.T) {
	l := NewPublishLimiter(0) // budget <= 0 → disabled
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		le, err := l.Reserve(ctx, "c", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		le.Abort()
	})
	assert.Equal(t, float64(0), allocs, "disabled limiter must be allocation-free")

	allocs = testing.AllocsPerRun(1000, func() {
		le, ok := l.TryReserve("c", 1<<20)
		if !ok {
			t.Fatal("disabled TryReserve must always grant")
		}
		le.Abort()
	})
	assert.Equal(t, float64(0), allocs, "disabled TryReserve must be allocation-free")
}

func TestPooledLeaseCombinedStateAndBits(t *testing.T) {
	t.Run("transfer preserves completion bits", func(t *testing.T) {
		p := &pooledLease{}
		p.gen.Store(1)
		p.combined.Store(tokenStateConnection | combinedBitBroker)

		p.LeaseTransfer(1)

		assert.Equal(t, uint32(tokenStateTransferred)|combinedBitBroker, p.combined.Load())
		assert.Equal(t, int32(tokenStateTransferred), p.LeaseState())
	})

	t.Run("final done frees and clears the combined word", func(t *testing.T) {
		l := NewPublishLimiter(10)
		l.used = 10
		p := &pooledLease{l: l, bytes: 10}
		p.gen.Store(1)
		p.combined.Store(tokenStateTransferred | combinedBitServer)

		p.LeaseBrokerDone(1)

		assert.Zero(t, p.combined.Load())
		assert.Equal(t, uint32(2), p.gen.Load())
		used, _, _ := l.Stats()
		assert.Zero(t, used)
	})

	t.Run("abort frees and clears the combined word", func(t *testing.T) {
		l := NewPublishLimiter(10)
		l.used = 10
		p := &pooledLease{l: l, bytes: 10}
		p.gen.Store(1)
		p.combined.Store(tokenStateConnection | combinedBitBroker)

		p.abortLim()

		assert.Zero(t, p.combined.Load())
		assert.Equal(t, uint32(2), p.gen.Load())
		used, _, _ := l.Stats()
		assert.Zero(t, used)
	})
}

// compile-time guard: the limiter exposes the spec surface.
var _ = ErrLimiterClosed
