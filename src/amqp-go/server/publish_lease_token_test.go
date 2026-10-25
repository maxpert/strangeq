package server

// TDD (red first): generic allocation-free publish lease tokens.
//
// Replaces the per-publish &Lease{} + joinRelease{} + defer-closure
// allocations with paged slot storage owned by the PublishLimiter:
//
//	Type: protocol.PublishLeaseToken is a VALUE {slot, gen} — zero-value is the
//	always-noop token (disabled limiter, malformed frames, transfers).
//	States per slot: FREE → CONNECTION (granted to a handler path) →
//	TRANSFERRED (owned by broker/WAL completion machinery). A slot releases
//	back to FREE only after BOTH serverDone and brokerDone are set (the
//	two-party join, formerly joinRelease) — or Abort (CONNECTION state
//	ownership, server-side teardown).
//	ABA defense: every signal validates token.gen against the slot's current
//	generation; a signal from a recycled-slot lifetime is a no-op.
//
// The queue FIFO becomes a capacity-preserving ring (the old
// `l.fifo = l.fifo[1:]` pattern drifts its backing array forward forever —
// an unbounded growth under queue churn). Slots grow lazily by pages and
// never shrink, so live-token addresses never move and overhead is
// amortized to zero at steady state.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ---- limiter grant path: tokens, not per-publish heap objects ----

func TestLeaseTokenGrantNoHeapAlloc(t *testing.T) {
	const budget = 32 << 20
	l := NewPublishLimiter(budget)
	// Warm the slot pool and ring so the steady-state cycle is what we assert.
	for i := 0; i < 64; i++ {
		tok, err := l.Reserve(context.Background(), "warm", leaseBytes(1024, 131072))
		if err != nil {
			t.Fatalf("warm reserve %d: %v", i, err)
		}
		tok.Abort()
	}
	runtime.GC()
	n := testing.AllocsPerRun(1000, func() {
		tok, err := l.Reserve(context.Background(), "hot", leaseBytes(1024, 131072))
		if err != nil {
			t.Fatal(err)
		}
		tok.Abort()
	})
	if n != 0 {
		t.Fatalf("steady-state grant/release allocs = %v, want 0", n)
	}
}

func TestLeaseTokenDisabledNoHeapAlloc(t *testing.T) {
	l := NewPublishLimiter(-1)
	n := testing.AllocsPerRun(1000, func() {
		tok, err := l.Reserve(context.Background(), "hot", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		tok.Abort()
	})
	if n != 0 {
		t.Fatalf("disabled-limiter allocs = %v, want 0", n)
	}
}

func TestLeaseTokenBudgetInvariant(t *testing.T) {
	budget := int64(3) * leaseBytes(1024, 131072)
	l := NewPublishLimiter(budget)
	var live []protocol.PublishLeaseToken
	for i := 0; i < 4; i++ {
		ctx := context.Background()
		if i == 3 {
			// The over-budget Reserve queues by contract; bound the probe so
			// its refusal to grant can be checked without releasing a holder.
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
		}
		tok, err := l.Reserve(ctx, "c", leaseBytes(1024, 131072))
		if i < 3 {
			if err != nil {
				t.Fatalf("reserve %d: %v", i, err)
			}
			live = append(live, tok)
		} else if err == nil {
			t.Fatal("4th concurrent reservation granted over budget")
		}
	}
	used, _, _ := l.Stats()
	if used != budget {
		t.Fatalf("used = %d, want %d", used, budget)
	}
	for _, tok := range live {
		tok.Abort()
	}
	used, _, _ = l.Stats()
	if used != 0 {
		t.Fatalf("used after all releases = %d, want 0", used)
	}
}

// ---- explicit join state machine (the joinRelease replacement) ----

func TestLeaseTokenStateTransitions(t *testing.T) {
	l := NewPublishLimiter(10 << 20)
	tok, err := l.Reserve(context.Background(), "c", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got := l.tokenStateForTest(tok); got != tokenStateConnection {
		t.Fatalf("state after grant = %v, want CONNECTION", got)
	}
	tok.BrokerDone()
	if got := l.tokenStateForTest(tok); got != tokenStateConnection {
		// brokerDone alone must not release: server-side teardown still owns it.
		t.Fatalf("state after brokerDone only = %v, want CONNECTION", got)
	}
	tok.Transfer()
	if got := l.tokenStateForTest(tok); got != tokenStateTransferred {
		t.Fatalf("state after transfer = %v, want TRANSFERRED", got)
	}
	tok.ServerDone()
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after both sides done = %d, want 0", used)
	}
	if got := l.tokenStateForTest(tok); got != tokenStateFree {
		t.Fatalf("state after both sides done = %v, want FREE", got)
	}
	// Post-release duplicate signals (either direction, any order) are no-ops.
	tok.ServerDone()
	tok.Transfer()
	tok.BrokerDone()
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after duplicate signals = %d, want 0", used)
	}
}

func TestLeaseTokenAbortPath(t *testing.T) {
	l := NewPublishLimiter(10 << 20)
	tok, err := l.Reserve(context.Background(), "c", 4096)
	if err != nil {
		t.Fatal(err)
	}
	tok.BrokerDone() // broker finished, but the reader-side teardown aborts
	tok.Abort()      // Abort: CONNECTION-ownership release
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after abort = %d, want 0", used)
	}
	tok.Abort() // idempotent
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after duplicate abort = %d, want 0", used)
	}
}

func TestLeaseTokenBrokerDoneThenAbortEquivalent(t *testing.T) {
	// Symmetric with the abort-path test: the join must release exactly once
	// regardless of which side signals first.
	l := NewPublishLimiter(10 << 20)
	tok, err := l.Reserve(context.Background(), "c", 4096)
	if err != nil {
		t.Fatal(err)
	}
	tok.ServerDone()
	tok.Transfer()
	tok.BrokerDone()
	tok.Transfer()
	tok.ServerDone()
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used = %d, want 0", used)
	}
}

// ---- ABA: recycled slots must be immune to stale-generation signals ----

func TestLeaseTokenLateDuplicateAfterSlotReuse(t *testing.T) {
	l := NewPublishLimiter(10 << 20)
	a, err := l.Reserve(context.Background(), "c", 2048)
	if err != nil {
		t.Fatal(err)
	}
	a.Transfer()
	a.BrokerDone()
	a.ServerDone() // released: slot recycled with generation bump
	b, err := l.Reserve(context.Background(), "c", 2048)
	if err != nil {
		t.Fatal(err)
	}
	if l.tokenStateForTest(b) != tokenStateConnection {
		t.Fatal("second grant not CONNECTION")
	}
	// Stale signals from lifetime A arrive late (e.g. the WAL writer goroutine
	// from message A racing message B's lifecycle). None may affect B.
	a.ServerDone()
	a.Transfer()
	a.BrokerDone()
	a.Abort()
	if used, _, _ := l.Stats(); used != 2048 {
		t.Fatalf("stale signals changed used: %d, want 2048", used)
	}
	if got := l.tokenStateForTest(b); got != tokenStateConnection {
		t.Fatalf("stale signals moved B state to %v", got)
	}
	// B completes normally, exactly once.
	b.Transfer()
	b.BrokerDone()
	b.ServerDone()
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after B completes = %d, want 0", used)
	}
}

func TestLeaseTokenConcurrentJoinsRaceClean(t *testing.T) {
	l := NewPublishLimiter(64 << 20)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				tok, err := l.Reserve(context.Background(), "c", 3072)
				if err != nil {
					panic(err)
				}
				var j sync.WaitGroup
				j.Add(3)
				go func() { defer j.Done(); tok.Transfer(); tok.BrokerDone() }()
				go func() { defer j.Done(); tok.ServerDone() }()
				go func() { defer j.Done(); tok.ServerDone() }() // duplicate server side
				j.Wait()
			}
		}()
	}
	wg.Wait()
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after storm = %d, want 0", used)
	}
}

// ---- FIFO ring: strict order preserved, backing array bounded ----

func TestLeaseFifoStrictOrderingUnchanged(t *testing.T) {
	budget := leaseBytes(1024, 131072) // exactly one in-flight
	l := NewPublishLimiter(budget)
	const nc = 4
	res := make(chan protocol.PublishLeaseToken, nc)
	var ord []int
	var mu sync.Mutex
	for i := 0; i < nc; i++ {
		i := i
		go func() {
			tok, err := l.Reserve(context.Background(), "conn-"+string(rune('a'+i)), leaseBytes(1024, 131072))
			mu.Lock()
			ord = append(ord, i)
			mu.Unlock()
			if err == nil {
				res <- tok
			}
		}()
		time.Sleep(2 * time.Millisecond) // deterministic arrival order
	}
	// Drain: releases grant strictly head-first.
	for i := 0; i < nc; i++ {
		tok := <-res
		tok.Abort()
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(ord); i++ {
		if ord[i] < ord[i-1] {
			t.Fatalf("FIFO violated: order %v", ord)
		}
	}
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used = %d, want 0", used)
	}
	if cap := l.fifoCapForTest(); cap > 8 {
		t.Fatalf("ring backing array grew to %d, want bounded small", cap)
	}
}

func TestLeaseFifoRingBoundedUnderChurn(t *testing.T) {
	l := NewPublishLimiter(leaseBytes(1024, 131072))
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connID := fmt.Sprintf("churn-%d", g)
			for i := 0; i < 300; i++ {
				tok, err := l.Reserve(context.Background(), connID, leaseBytes(1024, 131072))
				if err != nil {
					panic(err)
				}
				tok.Abort()
			}
		}()
	}
	wg.Wait()
	_, _, waiters := l.Stats()
	if waiters != 0 {
		t.Fatalf("waiters = %d, want 0", waiters)
	}
	// The old implementation's fifo backing array drifts forward unboundedly
	// under this exact churn pattern; the ring must stay tiny.
	if cap := l.fifoCapForTest(); cap > 16 {
		t.Fatalf("ring capacity after churn = %d, want bounded by max concurrent waiters", cap)
	}
}

// ---- slot pool: lazy pages, no growth at steady state ----

func TestLeaseSlotPoolPagesStabilize(t *testing.T) {
	l := NewPublishLimiter(1 << 30)
	// 2000 concurrent tiny leases: forces at least one page beyond the first.
	var toks []protocol.PublishLeaseToken
	for i := 0; i < 2000; i++ {
		tok, err := l.Reserve(context.Background(), "c", 512)
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		toks = append(toks, tok)
	}
	pg := l.slotPagesForTest()
	for _, tok := range toks {
		tok.Abort()
	}
	// Another wave of the same size must NOT add pages.
	for i := 0; i < 2000; i++ {
		tok, err := l.Reserve(context.Background(), "c", 512)
		if err != nil {
			t.Fatalf("warm reserve %d: %v", i, err)
		}
		tok.Abort()
	}
	if pg2 := l.slotPagesForTest(); pg2 != pg {
		t.Fatalf("pages grew %d -> %d across reused waves", pg, pg2)
	}
}

// ---- close semantics unchanged ----

func TestLeaseTokenCloseFailsWaitersOutstandingClean(t *testing.T) {
	l := NewPublishLimiter(leaseBytes(1024, 131072))
	tok, err := l.Reserve(context.Background(), "holder", leaseBytes(1024, 131072))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := l.Reserve(context.Background(), "waiter", 1024)
		errc <- err
	}()
	time.Sleep(10 * time.Millisecond)
	l.Close()
	if err := <-errc; err != ErrLimiterClosed {
		t.Fatalf("queued waiter err = %v, want ErrLimiterClosed", err)
	}
	tok.Transfer()
	tok.BrokerDone()
	tok.ServerDone()
	if _, got := l.TryReserve("late", 1024); got {
		t.Fatal("grant after close")
	}
	if used, _, _ := l.Stats(); used != 0 {
		t.Fatalf("used after outstanding released = %d, want 0", used)
	}
}

// ---- hooks / metrics semantics preserved ----

func TestLeaseTokenHooksPreserved(t *testing.T) {
	l := NewPublishLimiter(10 << 20)
	var grants, releases atomic.Int32
	l.hooks = &LimiterHooks{
		OnGrant:   func(connID string, bytes int64, oversized bool, used int64, waiters int) { grants.Add(1) },
		OnRelease: func(bytes int64, used int64, waiters int) { releases.Add(1) },
	}
	tok, err := l.Reserve(context.Background(), "c", 1024)
	if err != nil {
		t.Fatal(err)
	}
	releaseJoins := func() {
		tok.Transfer()
		tok.BrokerDone()
		tok.ServerDone()
	}
	releaseJoins()
	releaseJoins() // duplicates ignored — no extra hook fires
	if grants.Load() != 1 || releases.Load() != 1 {
		t.Fatalf("grants=%d releases=%d, want 1/1 (join duplicates ignored)", grants.Load(), releases.Load())
	}
}

// ---- zero-token is universally no-op ----

func TestLeaseTokenZeroValueNoop(t *testing.T) {
	var tok protocol.PublishLeaseToken
	tok.Abort()
	tok.Transfer()
	tok.ServerDone()
	tok.ServerFinish()
	tok.BrokerDone()
	var l *PublishLimiter
	l.Close()        // nil limiter is safe to close
	tok.BrokerDone() // zero token still a no-op
}

// ---- heap no-growth under GOGC=off (subprocess; catches slow per-slot leaks) ----

func TestPublishLeaseMemoryNoSlowLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("memory subprocess in short mode")
	}
	if os.Getenv("SQ_LEASE_MEM_CHILD") != "1" {
		// Parent: re-exec this test binary in a controlled child whose
		// environment turns GC off entirely, so any retained per-publish
		// allocation shows up as monotonic heap growth between waves.
		cmd := exec.Command(os.Args[0], "-test.run", "^TestPublishLeaseMemoryNoSlowLeak$", "-test.v", "-test.timeout=120s")
		cmd.Env = append(os.Environ(), "SQ_LEASE_MEM_CHILD=1", "GOGC=off", "GOMAXPROCS=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("subprocess failed: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "SUBPROCESS-PASS") {
			t.Fatalf("subprocess did not pass:\n%s", out)
		}
		return
	}
	// Child path: two identical waves. If any allocation held by the cycle
	// were retained, heap-after-wave-2 would exceed heap-after-wave-1.
	l := NewPublishLimiter(1 << 30)
	runWave := func() {
		var toks [2000]protocol.PublishLeaseToken
		for i := range toks {
			tok, err := l.Reserve(context.Background(), "c", 512)
			if err != nil {
				t.Fatal(err)
			}
			toks[i] = tok
		}
		for _, tok := range toks {
			tok.Transfer()
			tok.BrokerDone()
			tok.ServerDone()
		}
	}
	runtime.GC()
	var m0, m1 runtime.MemStats
	runWave()
	time.Sleep(50 * time.Millisecond) // settle pending releases
	runtime.GC()
	runtime.ReadMemStats(&m0)
	runWave()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	runtime.ReadMemStats(&m1)
	if m1.HeapAlloc > m0.HeapAlloc+1<<20 {
		t.Fatalf("heap grew across identical waves: %d -> %d (GOGC=off)\nSUBPROCESS-FAIL", m0.HeapAlloc, m1.HeapAlloc)
	}
	fmt.Println("SUBPROCESS-PASS")
}
