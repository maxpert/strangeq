package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/storage"
)

// ============================================================================
// Bet 0 — multi-queue delivery liveness and the invariants the per-queue dense
// sequencing fix must not break.
//
// THE BUG (P0, reproduced by TestBet0_ConcurrentMultiQueueDelivery): internal
// delivery tags are minted from ONE broker-global counter, but every queue's
// dispatch plane assumes its own tag space is DENSE — Claim walks tail one
// integer at a time up to head. With two or more queues publishing concurrently
// the tags interleave, so for each queue every tag minted by another queue is a
// permanent hole below its head. Each hole is only recognised as a hole AFTER a
// full cold-read miss (ring miss -> read-ahead miss -> WAL ReadBatch -> WAL Read
// -> queue-name mismatch -> segment miss). Hole-crawl throughput can never
// outrun tag-mint throughput, so consumers fall permanently behind and delivery
// collapses to ~0 msg/s on EVERY queue. Single queue is unaffected, which is why
// every published benchmark (all single-queue) missed it.
//
// Measured on main (commit 8a65f98) with the standalone perftest harness,
// 2 queues x 3 producers/3 consumers, durable+confirm, 1 KB bodies, 12s:
// 1,668,046 published, 558 consumed (99.97% never delivered).
//
// WHY THE EXISTING MULTI-QUEUE TESTS MISS IT: they publish sequentially or at
// low volume, so a queue's hole count stays small enough to crawl. The failure
// is a RATE failure — holes accrue faster than they can be crawled — so it only
// appears under SUSTAINED CONCURRENT publishing to two or more queues, and it is
// only visible if consumption is measured DURING the publish window rather than
// after an unbounded drain.
//
// The P0 sampling phase and fixture-shutdown guards are deliberately bounded.
// The exact drain has no total deadline: it can run indefinitely while expected
// identities continue to make progress, and fails only after inactivity.
// ============================================================================

var bet0PortCounter atomic.Int64

// bet0PortBase is the first port this file's embedded brokers bind. It is
// overridable because bet0PortCounter is PER-PROCESS: two concurrent copies of
// this test binary — a flake-soak runner, or two agents — both start at the same
// base and bind the same ports, so the second copy fails on bind and the harness
// manufactures failures that look like the flake it was launched to measure.
// Each concurrent worker must therefore set STRANGEQ_BET0_PORT_BASE to a block
// of its own, wide enough for every server the run creates.
//
// Read once at init rather than per call, so a soak cannot change ports
// mid-process. Mirrors STRANGEQ_FLIP_QUEUES in
// broker/frontier_flip_strand_test.go — same shape, same reason: a stress knob
// the committed default does not pay for.
var bet0PortBase = func() int {
	if s := os.Getenv("STRANGEQ_BET0_PORT_BASE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 1024 && n < 60000 {
			return n
		}
	}
	return 19200
}()

// bet0NextPort hands out a listen port distinct from every other embedded
// broker this process has created. It is the single place the base appears —
// there were six copies of the literal, which is the sibling-constant shape that
// makes a knob like the one above look applied when it is not.
func bet0NextPort() int {
	return bet0PortBase + int(bet0PortCounter.Add(1))
}

// brokerStartupPolls, together with brokerStartupPollInterval, bounds how
// long every embedded-broker helper in this package waits for the listener
// to come up. This is a listener-readiness POLL BUDGET, not a behavioural
// deadline: every behavioural deadline in these tests is a separate constant
// and is unchanged by this one. Conflating the two is exactly the failure
// mode this constant exists to prevent — see below.
//
// It was 400 (4.0s), and that is too tight to be a timeout — it was silently
// acting as a threshold. Under `go test -race` broker startup exceeds 4s, so
// TestBet0_DeleteRacingTxCommitCannotOrphanCommittedMessages and Case B both
// died at ~4.3s with "server listener not ready" and therefore NEVER RAN under
// the race detector at all: two correctness tests reported as covered by a
// slice that had not executed them.
//
// This is a startup timeout, not an assertion. Raising it weakens nothing —
// every behavioural deadline in these tests is separate and unchanged — it only
// stops the harness from failing a test before its subject has started.
// A too-tight budget does not make a test fail late — it makes the test fail
// BEFORE its subject has started, and the test then reports as covered while
// never having run.
//
// 2000 (20s) WAS ALSO OBSERVED INSUFFICIENT, so this is the same landmine going
// off a second time, one order of magnitude further out. Under a load average
// of ~4.8 — routine for a shared CI runner — five tests failed simultaneously
// with "server listener not ready", taking 21.8-25.0s each; the same five on a
// quiet machine start in 0.37-3.40s. A 10-60x swing in startup, and the harness
// failing tests whose subject never began, reported as if multi-queue recovery
// were broken.
//
// 6000 (60s) is ~18x the quiet-machine worst case. A genuine hang still fails,
// just later — which is the correct trade for a timeout that has now twice been
// mistaken for an assertion.
//
// Until this sweep, three more helpers (f1AggServer in f1_aggressive_test.go,
// f1Server in f1_starvation_test.go, and confBroker.startEmbedded in
// conformance_harness_test.go) each carried their OWN, independent copy of
// this budget — hardcoded at 200 polls x 10ms, or 100 polls x 20ms — both of
// which are the same 2s that had already been proven insufficient above. They
// survived at 2s only because nothing tied them to this constant: someone
// raised the copy in front of them and the other three did not move. That is
// how four separate near-misses of the same landmine coexisted in one
// package. All four sites now share brokerStartupPolls and
// brokerStartupPollInterval so tightening one tightens (and can be reasoned
// about) for all of them at once; re-tightening this constant re-arms the
// same failure for every site that reads it.
const (
	brokerStartupPolls        = 6000
	brokerStartupPollInterval = 10 * time.Millisecond
)

// bet0Server starts an embedded broker rooted at dir and returns it with its URI.
// dir is persistent across a Stop/restart so durable recovery can be exercised.
func bet0Server(t *testing.T, dir string) (*server.Server, string) {
	t.Helper()
	port := bet0NextPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "silent"

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err, "server build")
	waitForListening(t, srv)
	// These tests drive hundreds of thousands of messages through an embedded
	// broker. Leaving one running would let a finished (or failed) test's load
	// bleed into the next one and distort it, so every server is torn down here
	// with a bounded Shutdown. Shutdown is idempotent and safe after a mid-test
	// Shutdown — the restart tests close srv themselves first.
	t.Cleanup(func() { testShutdownServer(t, srv) })
	return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr)
}

// bet0Body builds a body whose first 8 bytes carry seq so a consumer can
// recover the publish sequence from the delivered bytes alone.
func bet0Body(seq uint64, n int) []byte {
	if n < 8 {
		n = 8
	}
	b := make([]byte, n)
	binary.BigEndian.PutUint64(b[0:8], seq)
	return b
}

func bet0Seq(body []byte) uint64 { return binary.BigEndian.Uint64(body[0:8]) }

// ----------------------------------------------------------------------------
// 1. The P0 repro: sustained concurrent publishing to N queues.
// ----------------------------------------------------------------------------

// bet0LoadResult is the per-queue tally of one concurrent-load run.
type bet0LoadResult struct {
	published atomic.Int64 // Publish() calls that returned nil
	consumed  atomic.Int64 // deliveries received and acked
	observed  *bet0ObservedIdentities
	// consumedAtWindow / publishedAtWindow are sampled at the SAME instant, at
	// the end of the publish window, with publishers still running. These are the
	// LIVENESS numbers: consumption that only happens after publishing stops does
	// not prove the broker can deliver while it is being published to, so the
	// ratio is deliberately measured under load.
	consumedAtWindow  int64
	publishedAtWindow int64
}

// TestBet0_ConcurrentMultiQueueDelivery is the P0 repro.
//
// Spec basis: AMQP 0-9-1 §1.1 — a queue delivers to its consumers; §2.1.1 the
// broker is a shared service across queues. A queue's delivery liveness MUST NOT
// depend on whether OTHER, unrelated queues are being published to. Two queues
// each under an identical load that one queue alone sustains at ~137K msg/s must
// both keep delivering.
//
// Shape (proven repro, dossier §6): 2 queues x 3 producers / 3 consumers each,
// sustained concurrent publishing. The assertion is on consumption measured
// DURING the publish window, and it goes through bet0CheckLiveness — see
// bet0_liveness_predicate_test.go for the predicate and for the measurements
// behind every threshold it applies.
//
// Uninstrumented, that predicate still enforces the original floor: each queue
// must consume at least bet0MinDeliveredFraction (0.50) of what it published.
// Healthy single-queue behaviour is ~0.97 and broken main is ~0.0006, so that
// floor has three orders of magnitude of margin and is not a tuning knob.
// UNDER -race THAT FLOOR IS UNDECIDABLE — the healthy and broken rate bands
// overlap — so it is disabled there and the diagnostic becomes cross-queue
// balance plus a liveness backstop. It is disabled, NOT lowered; lowering it is
// the banned fix. The predicate file explains why at length. Read it before
// changing anything here.
//
// The second assertion — every identity whose Publish call returned nil is
// eventually observed after a successful Ack — is the correctness half. Exact
// target inclusion proves safety; reset-on-exact-target-progress inactivity
// bounds a genuinely stalled mechanism without imposing a total drain deadline.
//
// Three shapes, because the transient failure mode is CONDITIONAL. Gap-crawling
// a foreign tag costs a full cold-read miss only when the WAL actually has
// content to read: on an all-transient broker with an empty WAL the miss returns
// almost immediately, so hole-crawl can outrun tag-mint and the queues stay
// healthy — until the ring fills past the spill threshold and transient
// publishes start writing to the WAL, at which point it collapses. The MIXED
// shape (one durable queue populating the WAL, one transient queue beside it) is
// the realistic transient failure and is deployed everywhere; all-transient is
// carried as a guard that must stay healthy.
func TestBet0_ConcurrentMultiQueueDelivery(t *testing.T) {
	shapes := []struct {
		name    string
		durable []bool // per queue
	}{
		{"durable", []bool{true, true}},
		{"mixed", []bool{true, false}},
		{"transient", []bool{false, false}},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			bet0RunConcurrentMultiQueue(t, sh.name, sh.durable)
		})
	}
}

// Load shape. The P0 assertion thresholds live in
// bet0_liveness_predicate_test.go, next to their measurements. The drain
// inactivity value is a one-sided mechanism liveness guard, not a throughput
// budget. The same-session selection campaign after exact identity wiring used
// 3.306079959s as its worst observed healthy inter-expected-progress gap across
// normal, -race, exact-CI, and constrained-CPU runs. The 60s window has more
// than 18x headroom over that selection sample. Focused runs keep logging the
// gap so premise drift remains visible and forces re-derivation rather than a
// silent threshold change.
const (
	bet0Queues                = 2
	bet0ProducersPerQueue     = 3
	bet0ConsumersPerQueue     = 3
	bet0BodySize              = 128
	bet0PublishWindow         = 4 * time.Second
	bet0DrainInactivityWindow = 60 * time.Second
	bet0PublisherStopGuard    = 60 * time.Second
	bet0Prefetch              = 100
)

// bet0StartBarrier releases producer goroutines together and ALWAYS releases:
// t.Cleanup (which runs on runtime.Goexit/FailNow) plus a sync.Once make both
// the normal path and the require-Goexit path in a launch loop safe (RH-2).
// Without it, q0's producers are dialled and launched before q1's, so both
// queues are measured over different intervals and the bias is systematically
// against the later queue (C9).
type bet0StartBarrier struct {
	ch   chan struct{}
	once sync.Once
}

func bet0NewStartBarrier(t *testing.T) *bet0StartBarrier {
	b := &bet0StartBarrier{ch: make(chan struct{})}
	t.Cleanup(b.Release) // registered before any require in the launch loop
	return b
}

// Wait blocks until Release, or returns false if stop is closed first.
func (b *bet0StartBarrier) Wait(stop <-chan struct{}) bool {
	select {
	case <-b.ch:
		return true
	case <-stop:
		return false
	}
}

// Release closes the gate exactly once, so the second and later calls are safe.
func (b *bet0StartBarrier) Release() { b.once.Do(func() { close(b.ch) }) }

// TestBet0_StartBarrierReleasesIdempotently: Release twice must not panic and
// the gate must be closed.
func TestBet0_StartBarrierReleasesIdempotently(t *testing.T) {
	b := bet0NewStartBarrier(t)
	b.Release()
	b.Release() // must not panic

	select {
	case <-b.ch:
	default:
		t.Fatal("Release must close the gate")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if !b.Wait(make(chan struct{})) {
			t.Error("Wait returned false after Release")
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock after Release")
	}
}

// TestBet0_StartBarrierWaitHonoursStop: Wait must return false when stop is
// already closed, so a producer cannot block forever on an unreleased barrier.
func TestBet0_StartBarrierWaitHonoursStop(t *testing.T) {
	b := bet0NewStartBarrier(t)
	stop := make(chan struct{})
	close(stop)
	if b.Wait(stop) {
		t.Fatal("Wait must return false when stop is closed first")
	}
}

// TestBet0_StartBarrierCleanupReleases: a producer blocked on Wait must be
// released by cleanup even when the test/subtest returns (Goexit) without an
// explicit Release.
func TestBet0_StartBarrierCleanupReleases(t *testing.T) {
	var b *bet0StartBarrier
	t.Run("returns without releasing", func(t *testing.T) {
		b = bet0NewStartBarrier(t)
		// No Release: the registered cleanup must do it.
	})
	select {
	case <-b.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not release the barrier")
	}
}

// bet0ConsumerDeathHint renders consumer-death evidence for a FAILURE MESSAGE.
// It is never an input to bet0CheckLiveness and never chooses the verdict; it
// only makes a cause that the old ordering (balance require before the ack-error
// drain) hid visible, so the next F2 recurrence is self-explaining.
func bet0ConsumerDeathHint(ackErr error, closed []int64, names []string) string {
	var b strings.Builder
	if ackErr != nil {
		fmt.Fprintf(&b, " ADDITIONALLY: a consumer aborted with an Ack error: %v;", ackErr)
	}
	for i, n := range closed {
		if n > 0 {
			fmt.Fprintf(&b, " consumer(s) on %s had their delivery channel closed %d time(s) during the window;",
				names[i], n)
		}
	}
	return b.String()
}

// bet0ClosedCounts snapshots the per-queue closed-delivery counts.
func bet0ClosedCounts(closed []*atomic.Int64) []int64 {
	out := make([]int64, len(closed))
	for i, c := range closed {
		out[i] = c.Load()
	}
	return out
}

// bet0TotalClosed sums the per-queue closed-delivery counts.
func bet0TotalClosed(closed []*atomic.Int64) int64 {
	var n int64
	for _, c := range closed {
		n += c.Load()
	}
	return n
}

// TestBet0_ConsumerDeathHintSurfacesAckError is the deterministic proof that a
// hidden Ack error / consumer death is named in the failure message.
func TestBet0_ConsumerDeathHintSurfacesAckError(t *testing.T) {
	boom := errors.New("ack boom")
	cases := []struct {
		name   string
		ackErr error
		closed []int64
		want   []string
		absent []string
	}{
		{name: "clean", ackErr: nil, closed: []int64{0, 0}, absent: []string{"Ack error", "delivery channel closed"}},
		{name: "ack only", ackErr: boom, closed: []int64{0, 0}, want: []string{"Ack error", "ack boom"}},
		{name: "death only", ackErr: nil, closed: []int64{0, 1}, want: []string{"q2", "delivery channel closed"}},
		{name: "both", ackErr: boom, closed: []int64{1, 1}, want: []string{"Ack error", "ack boom", "q1", "q2", "delivery channel closed"}},
	}
	names := []string{"q1", "q2"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bet0ConsumerDeathHint(tc.ackErr, tc.closed, names)
			if tc.ackErr == nil && len(tc.want) == 0 {
				require.Empty(t, got, "no ack error and no deaths must render nothing")
			}
			for _, w := range tc.want {
				require.Contains(t, got, w)
			}
			for _, a := range tc.absent {
				require.NotContains(t, got, a)
			}
		})
	}
}

// TestBet0_DeathHintIsAppendedNotSubstituted: the balance verdict still comes
// first and is never replaced by the death evidence.
func TestBet0_DeathHintIsAppendedNotSubstituted(t *testing.T) {
	samples := []bet0Sample{
		{name: "q1", published: 100000, consumed: 90000},
		{name: "q2", published: 100000, consumed: 35000},
	}
	livenessErr := bet0CheckLiveness(samples, bet0Policy())
	require.Error(t, livenessErr)
	require.Contains(t, livenessErr.Error(), "IMBALANCE",
		"the partial-starvation verdict class must be unchanged")

	hint := bet0ConsumerDeathHint(errors.New("ack boom"), []int64{1, 1}, []string{"q1", "q2"})
	require.NotEmpty(t, hint)

	// Exactly the composition the harness performs: the liveness verdict is
	// reported first, the death evidence is appended.
	reported := livenessErr.Error() + hint
	require.True(t, strings.HasPrefix(reported, livenessErr.Error()),
		"the reported failure must start with the unchanged liveness verdict")
	require.Contains(t, reported, hint)
	require.Contains(t, reported, "IMBALANCE")
}

// bet0RunConcurrentMultiQueue drives the multi-queue load shape used by the P0
// repro and the durable/transient variants.
func bet0RunConcurrentMultiQueue(t *testing.T, shape string, durable []bool) {
	require.Len(t, durable, bet0Queues)
	_, uri := bet0Server(t, t.TempDir())

	drainChanged := make(chan struct{}, 1)
	results := make([]*bet0LoadResult, bet0Queues)
	queueNames := make([]string, bet0Queues)
	// E10: one closed-delivery counter per queue, so a delivery channel that
	// closes during the run is no longer invisible.
	closed := make([]*atomic.Int64, bet0Queues)
	for i := range results {
		results[i] = &bet0LoadResult{observed: newBet0ObservedIdentities(drainChanged)}
		queueNames[i] = fmt.Sprintf("bet0-load-%s-q%d", shape, i+1)
		closed[i] = &atomic.Int64{}
	}
	ackErrors := make(chan error, 1)

	// Connections are tracked so the cleanup can force-close them: a producer
	// blocked in Publish against a wedged broker is only released by closing its
	// connection. This is what keeps a FAILING run bounded instead of hung.
	var connMu sync.Mutex
	var conns []*amqp.Connection
	track := func(c *amqp.Connection) *amqp.Connection {
		connMu.Lock()
		conns = append(conns, c)
		connMu.Unlock()
		return c
	}
	// CloseDeadline, not Close: a starved broker's reader is parked at the queue
	// depth high-water mark and never answers connection.close-ok, so a plain
	// Close blocks forever and would turn a FAILING run into a hung one. The
	// closes also run concurrently and are bounded as a group.
	closeAll := func() {
		connMu.Lock()
		pending := conns
		conns = nil
		connMu.Unlock()

		var wg sync.WaitGroup
		for _, c := range pending {
			wg.Add(1)
			go func(c *amqp.Connection) {
				defer wg.Done()
				_ = c.CloseDeadline(time.Now().Add(2 * time.Second))
			}(c)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Logf("warning: %d connections did not close within 10s", len(pending))
		}
	}
	t.Cleanup(closeAll)

	stopPub := make(chan struct{})
	stopCon := make(chan struct{})
	var stopPubOnce, stopConOnce sync.Once
	t.Cleanup(func() {
		stopPubOnce.Do(func() { close(stopPub) })
		stopConOnce.Do(func() { close(stopCon) })
	})

	// E2: producers must all start publishing at the same instant, so the two
	// queues are measured over the same interval (C9). The gate is registered
	// here, before any require in the launch loops, so a launch-loop failure
	// still releases it via t.Cleanup.
	barrier := bet0NewStartBarrier(t)

	// --- consumers first, so they are registered before any publish ---
	var conWG sync.WaitGroup
	for qi := 0; qi < bet0Queues; qi++ {
		for c := 0; c < bet0ConsumersPerQueue; c++ {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			track(conn)
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(queueNames[qi], durable[qi], false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
			deliveries, err := ch.Consume(queueNames[qi], "", false, false, false, false, nil)
			require.NoError(t, err)

			res := results[qi]
			queueIndex := uint16(qi)
			closedCount := closed[qi]
			conWG.Add(1)
			go func() {
				defer conWG.Done()
				for {
					select {
					case <-stopCon:
						return
					case d, ok := <-deliveries:
						if !ok {
							closedCount.Add(1)
							return
						}
						if err := bet0AckAndObserve(func() error { return d.Ack(false) }, res.observed,
							queueIndex, bet0ProducersPerQueue, d.Body); err != nil {
							select {
							case ackErrors <- fmt.Errorf("queue %s: delivery Ack failed: %w", queueNames[queueIndex], err):
							default:
							}
							return
						}
						res.consumed.Add(1)
					}
				}
			}()
		}
	}

	// --- producers ---
	// All consumers above are registered before any producer starts: ch.Consume
	// returns only after the server has RegisterConsumer'd, so the old fixed
	// "let consumer registration land" sleep is not needed (E2).
	successCounts := make([][]*atomic.Uint64, bet0Queues)
	var pubWG sync.WaitGroup
	for qi := 0; qi < bet0Queues; qi++ {
		successCounts[qi] = make([]*atomic.Uint64, bet0ProducersPerQueue)
		mode := amqp.Transient
		if durable[qi] {
			mode = amqp.Persistent
		}
		for p := 0; p < bet0ProducersPerQueue; p++ {
			successCounts[qi][p] = &atomic.Uint64{}
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			track(conn)
			ch, err := conn.Channel()
			require.NoError(t, err)

			res := results[qi]
			qName := queueNames[qi]
			queueIndex, producerIndex := uint16(qi), uint16(p)
			successCount := successCounts[qi][p]
			pubWG.Add(1)
			go func() {
				defer pubWG.Done()
				if !barrier.Wait(stopPub) {
					return
				}
				pub := amqp.Publishing{DeliveryMode: mode, Body: make([]byte, bet0BodySize)}
				var seq uint64
				for {
					select {
					case <-stopPub:
						return
					default:
					}
					bet0EncodeDrainIdentity(pub.Body, bet0DrainIdentity{
						queue: queueIndex, producer: producerIndex, seq: seq,
					})
					if err := ch.PublishWithContext(context.Background(), "", qName, false, false, pub); err != nil {
						return
					}
					seq++
					successCount.Store(seq)
					res.published.Add(1)
				}
			}()
		}
	}

	// E2 (PC-3): nothing may have published before the gate opens. If this fires,
	// a producer was launched outside the barrier — the systematic interval skew
	// C9 is back.
	var preRelease int64
	for _, res := range results {
		preRelease += res.published.Load()
	}
	require.Zero(t, preRelease, "a producer published before the start barrier was released")
	barrier.Release()

	// --- publish window ---
	// Both counters are sampled at the same instant with publishers still
	// running, so the ratio is consumption-under-load over the same interval.
	// The few thousand messages in flight at that instant (prefetch x consumers
	// plus socket buffers) are noise against the hundreds of thousands published.
	time.Sleep(bet0PublishWindow)
	for _, res := range results {
		res.publishedAtWindow = res.published.Load()
		res.consumedAtWindow = res.consumed.Load()
	}

	stopPubOnce.Do(func() { close(stopPub) })

	// --- LIVENESS assertion (this is the one that fails on broken main) ---
	// Evaluate the already-frozen under-window samples before publisher join. On
	// the broken tree a producer may be stuck inside PublishWithContext, so a join
	// guard must not mask this P0 assertion.
	//
	// The balance verdict is still evaluated and reported FIRST. Any Ack error or
	// closed delivery channel is APPENDED to that verdict and, when balance
	// passed, additionally fails the run — so a consumer death can no longer hide
	// behind an imbalance verdict. (The old ordering drained ackErrors only after
	// the balance require, which is exactly how the one recorded F2 recurrence
	// lost its cause.)
	//
	// The predicate itself, and the measurements behind every threshold it
	// applies, are in bet0_liveness_predicate_test.go. Read that file before
	// touching any number: under -race the consumed/published rate bands of a
	// healthy and a broken tree overlap, so the diagnostic here is cross-queue
	// BALANCE, not rate.
	samples := make([]bet0Sample, bet0Queues)
	for qi, res := range results {
		samples[qi] = bet0Sample{
			name:      queueNames[qi],
			published: res.publishedAtWindow,
			consumed:  res.consumedAtWindow,
		}
		t.Logf("queue %s: published=%d consumed-in-window=%d (%.4f)",
			queueNames[qi], res.publishedAtWindow, res.consumedAtWindow, samples[qi].ratio())
	}
	if notice := bet0PremiseNotice(samples, bet0Policy()); notice != "" {
		t.Log(notice)
	}
	if notice := bet0SignalNotice(samples); notice != "" {
		t.Log(notice)
	}

	// E10 (R2): snapshot what the old code only read after the balance require.
	// A consumer killed by an Ack error, or a delivery channel closed during the
	// window, is rendered into the failure message below instead of being hidden
	// behind it. This does not affect the verdict.
	var earlyAckErr error
	select {
	case earlyAckErr = <-ackErrors:
	default:
	}
	hint := bet0ConsumerDeathHint(earlyAckErr, bet0ClosedCounts(closed), queueNames)

	var publishersDone chan struct{}
	livenessErr := bet0CheckBeforePublisherJoin(func() error {
		return bet0CheckLiveness(samples, bet0Policy())
	}, func() {
		publishersDone = make(chan struct{})
		go func() {
			pubWG.Wait()
			close(publishersDone)
		}()
		select {
		case <-publishersDone:
		case <-time.After(bet0PublisherStopGuard):
			// PublishWithContext in amqp091-go v1.10.0 ignores its context. Force
			// closing connections is therefore the only reliable way to release a
			// Publish call wedged during fixture shutdown.
			closeAll()
			select {
			case <-publishersDone:
			case <-time.After(10 * time.Second):
			}
			t.Fatalf("publisher goroutines did not stop within %s; targets were not frozen",
				bet0PublisherStopGuard)
		}
	})
	require.NoErrorf(t, livenessErr, "shape=%s window=%s%s", shape, bet0PublishWindow, hint)
	targets := bet0FreezePublisherTargets(func() { <-publishersDone }, bet0Queues,
		bet0ProducersPerQueue, func(queue, producer int) uint64 {
			return successCounts[queue][producer].Load()
		})
	publishedTotals := make([]int64, bet0Queues)
	for qi, res := range results {
		publishedTotals[qi] = res.published.Load()
	}
	require.NoError(t, bet0ValidatePublisherTargets(targets, publishedTotals, bet0ProducersPerQueue),
		"publisher target wiring premise failed after publisher join")

	// E10 (R2): the balance verdict has been evaluated and reported first. Now
	// surface any Ack error / consumer death the old ordering hid — and fail the
	// run when balance passed, so a death cannot hide behind an imbalance.
	if earlyAckErr != nil {
		t.Fatal(earlyAckErr)
	}
	if n := bet0TotalClosed(closed); n > 0 {
		t.Fatalf("consumer death during the measured window: %d delivery channel(s) closed;%s", n, hint)
	}
	select {
	case err := <-ackErrors:
		t.Fatal(err)
	default:
	}

	// --- EXACT ZERO-LOSS assertion: no total drain deadline ---
	states := make([]*bet0DrainState, bet0Queues)
	for qi := range states {
		states[qi] = newBet0DrainState(targets[qi], results[qi].observed, bet0DrainInactivityWindow)
	}
	drainStart := time.Now() // explicit inactivity baseline: after publisher join and target freeze
	finalSnapshots := make([]bet0DrainSnapshot, bet0Queues)
	for {
		elapsed := time.Since(drainStart)
		allDone := true
		waitFor := bet0DrainInactivityWindow
		for qi, state := range states {
			snapshot := state.Observe(elapsed) // fresh snapshot also closes queued-timer/progress races
			finalSnapshots[qi] = snapshot
			switch snapshot.verdict {
			case bet0DrainDone:
				continue
			case bet0DrainStalled:
				t.Fatal(bet0DrainDiagnostic(queueNames[qi], snapshot))
			default:
				allDone = false
				if remaining := state.RemainingInactivity(elapsed); remaining < waitFor {
					waitFor = remaining
				}
			}
		}
		if allDone {
			break
		}

		timer := time.NewTimer(waitFor)
		select {
		case <-drainChanged:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case err := <-ackErrors:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			t.Fatal(err)
		case <-timer.C:
			// Loop and take a fresh exact missing-set snapshot before deciding.
		}
	}
	for qi, snapshot := range finalSnapshots {
		t.Logf("queue %s exact drain: observed=%d target=%d max healthy inter-expected-progress gap=%s",
			queueNames[qi], snapshot.observed, snapshot.target, snapshot.maxExpectedGap)
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ----------------------------------------------------------------------------
// 2. A queue declared on a busy broker must deliver promptly.
// ----------------------------------------------------------------------------

// TestBet0_LateDeclaredQueueDeliversPromptly is the deterministic (non-rate)
// face of the same bug, and the sharpest statement of the invariant the fix
// establishes: a queue's dispatch cursor must live in ITS OWN sequence space.
//
// A brand-new queue starts its claim cursor at 0 while its head is set from the
// tag its first message was minted at. If tags come from a broker-global
// counter, a queue declared after the broker has already minted N tags must
// crawl all N absent tags — each at full cold-read cost — before it can deliver
// its own first message. With per-queue dense sequencing that first message is
// sequence 0 and arrives immediately, no matter how busy the broker has been.
//
// Spec basis: AMQP 0-9-1 §1.1/§2.1.1 — queues are independent entities; nothing
// in the protocol makes one queue's first delivery a function of unrelated
// traffic the broker handled earlier.
func TestBet0_LateDeclaredQueueDeliversPromptly(t *testing.T) {
	const (
		warmupTags   = 400000 // global tags minted on an unrelated queue first
		firstMsgWait = 10 * time.Second
	)

	_, uri := bet0Server(t, t.TempDir())

	warmConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer warmConn.Close()

	const warmQ = "bet0-late-warm"
	// Drain the warm queue while publishing, so it never parks at the depth
	// high-water mark; the point is to advance the tag counter, not to build a
	// backlog.
	var warmConsumed atomic.Int64
	warmStop := make(chan struct{})
	var warmStopOnce sync.Once
	defer warmStopOnce.Do(func() { close(warmStop) })
	for c := 0; c < 3; c++ {
		ch, err := warmConn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(warmQ, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(warmQ, "", true, false, false, false, nil)
		require.NoError(t, err)
		go func() {
			for {
				select {
				case <-warmStop:
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					_ = d
					warmConsumed.Add(1)
				}
			}
		}()
	}

	var warmPublished atomic.Int64
	var warmWG sync.WaitGroup
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer warmCancel()
	for p := 0; p < 3; p++ {
		ch, err := warmConn.Channel()
		require.NoError(t, err)
		warmWG.Add(1)
		go func() {
			defer warmWG.Done()
			pub := amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(0, bet0BodySize)}
			for warmPublished.Load() < warmupTags {
				if err := ch.PublishWithContext(warmCtx, "", warmQ, false, false, pub); err != nil {
					return
				}
				warmPublished.Add(1)
			}
		}()
	}
	warmWG.Wait()
	require.GreaterOrEqual(t, warmPublished.Load(), int64(warmupTags),
		"warm-up did not mint enough delivery tags")
	t.Logf("warm-up: %d tags minted on %s (%d consumed)", warmPublished.Load(), warmQ, warmConsumed.Load())

	// Now declare a fresh queue on this busy broker and time its first delivery.
	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	const lateQ = "bet0-late-fresh"
	_, err = ch.QueueDeclare(lateQ, true, false, false, false, nil)
	require.NoError(t, err)
	deliveries, err := ch.Consume(lateQ, "", true, false, false, false, nil)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	require.NoError(t, ch.PublishWithContext(context.Background(), "", lateQ, false, false,
		amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(1, bet0BodySize)}))

	select {
	case d, ok := <-deliveries:
		require.True(t, ok, "delivery channel closed before first message")
		require.Equal(t, uint64(1), bet0Seq(d.Body))
		t.Logf("first delivery on late-declared queue after %v", time.Since(start))
	case <-time.After(firstMsgWait):
		t.Fatalf("LATE-DECLARED QUEUE STARVED: queue %q received no message within %s "+
			"after the broker had already handled %d messages on an unrelated queue. "+
			"A newly declared queue's first delivery must not be a function of unrelated "+
			"prior traffic (it is gap-crawling a global tag space).",
			lateQ, firstMsgWait, warmPublished.Load())
	}
}

// ----------------------------------------------------------------------------
// 3-5. Invariants the fix must not break.
// ----------------------------------------------------------------------------

// TestBet0_MultiQueueFIFOOrder asserts AMQP 0-9-1 §4.7's ordering guarantee
// still holds once the dispatch plane is re-sequenced: messages published to one
// queue on one channel, with no requeue and no priority, are delivered to a
// single consumer in publication order. Publishes are INTERLEAVED across two
// queues so each queue's internal sequence is exercised while another queue is
// concurrently minting — the exact condition the fix changes.
func TestBet0_MultiQueueFIFOOrder(t *testing.T) {
	const perQueue = 4000
	_, uri := bet0Server(t, t.TempDir())

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()

	names := []string{"bet0-fifo-q1", "bet0-fifo-q2"}
	pubChans := make([]*amqp.Channel, len(names))
	conChans := make([]<-chan amqp.Delivery, len(names))
	for i, n := range names {
		pch, err := conn.Channel()
		require.NoError(t, err)
		_, err = pch.QueueDeclare(n, true, false, false, false, nil)
		require.NoError(t, err)
		pubChans[i] = pch

		cch, err := conn.Channel()
		require.NoError(t, err)
		require.NoError(t, cch.Qos(bet0Prefetch, 0, false))
		d, err := cch.Consume(n, "", true, false, false, false, nil)
		require.NoError(t, err)
		conChans[i] = d
	}
	time.Sleep(200 * time.Millisecond)

	// One publisher per queue, interleaved round-robin: tags alternate between
	// the two queues, which is precisely what makes each queue's tag space
	// sparse under the global-counter design.
	for seq := uint64(0); seq < perQueue; seq++ {
		for i := range names {
			require.NoError(t, pubChans[i].PublishWithContext(context.Background(), "", names[i], false, false,
				amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
		}
	}

	for i, n := range names {
		var want uint64
		deadline := time.After(60 * time.Second)
		for want < perQueue {
			select {
			case d, ok := <-conChans[i]:
				require.True(t, ok, "queue %s: delivery channel closed at seq %d", n, want)
				got := bet0Seq(d.Body)
				require.Equalf(t, want, got,
					"FIFO VIOLATION on queue %s: expected publish sequence %d, got %d "+
						"(AMQP 0-9-1 §4.7: messages published on one channel to one queue are "+
						"delivered to a single consumer in publication order)", n, want, got)
				want++
			case <-deadline:
				t.Fatalf("queue %s: only %d/%d messages delivered before deadline", n, want, perQueue)
			}
		}
	}
}

// TestBet0_MultiQueueConfirmImpliesRecoverable asserts the durability invariant
// across a restart with MULTIPLE queues: once basic.ack (publisher confirm) is
// received, the message is fsynced and recoverable. After the fix, per-queue
// sequences must be rebuilt from the WAL on recovery — a queue whose recovered
// cursor does not match its recovered messages either loses them or wedges. The
// existing durable-recovery coverage is single-queue, so it cannot see a
// per-queue sequence rebuild that is only wrong when queues interleave.
//
// Spec basis: AMQP 0-9-1 + RabbitMQ publisher-confirms extension — basic.ack for
// a persistent message on a durable queue means the broker has taken
// responsibility for it.
func TestBet0_MultiQueueConfirmImpliesRecoverable(t *testing.T) {
	const perQueue = 2000
	dir := t.TempDir()
	srv, uri := bet0Server(t, dir)

	names := []string{"bet0-recover-q1", "bet0-recover-q2"}

	func() {
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()

		chans := make([]*amqp.Channel, len(names))
		confirms := make([]chan amqp.Confirmation, len(names))
		for i, n := range names {
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(n, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms[i] = ch.NotifyPublish(make(chan amqp.Confirmation, perQueue+16))
			chans[i] = ch
		}

		// Interleaved publishing across both durable queues.
		for seq := uint64(0); seq < perQueue; seq++ {
			for i, n := range names {
				require.NoError(t, chans[i].PublishWithContext(context.Background(), "", n, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
			}
		}

		// Every publish must be positively confirmed before we pull the plug.
		for i, n := range names {
			for got := 0; got < perQueue; got++ {
				select {
				case c := <-confirms[i]:
					require.Truef(t, c.Ack, "queue %s: publish %d was nacked", n, c.DeliveryTag)
				case <-time.After(60 * time.Second):
					t.Fatalf("queue %s: only %d/%d publishes confirmed", n, got, perQueue)
				}
			}
		}
	}()

	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()

	for _, n := range names {
		ch, err := conn2.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(n, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(n, "", true, false, false, false, nil)
		require.NoError(t, err)

		seen := make(map[uint64]int, perQueue)
		var want uint64
		deadline := time.After(60 * time.Second)
		for len(seen) < perQueue {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "queue %s: delivery channel closed after %d recovered", n, len(seen))
				got := bet0Seq(d.Body)
				seen[got]++
				require.Equalf(t, 1, seen[got], "queue %s: sequence %d recovered more than once", n, got)
				// Recovery must also preserve per-queue publication order.
				require.Equalf(t, want, got,
					"queue %s: recovered out of publication order — expected %d, got %d", n, want, got)
				want++
			case <-deadline:
				t.Fatalf("CONFIRMED-BUT-NOT-RECOVERABLE: queue %s recovered only %d/%d "+
					"confirmed persistent messages after restart", n, len(seen), perQueue)
			}
		}
		require.Len(t, seen, perQueue, "queue %s: recovered message count", n)
		t.Logf("queue %s: recovered %d/%d confirmed persistent messages in publication order",
			n, len(seen), perQueue)
		_ = ch.Close()
	}
}

// TestBet0_RequeueRedeliveredUnderMultiQueueLoad asserts requeue semantics
// survive the re-sequencing: a basic.nack/basic.reject with requeue=true returns
// the message to the queue, and the redelivery carries redelivered=true, with no
// loss and no duplicate settle. Requeued messages sit OUTSIDE the dense sequence
// (they are already-claimed sequences returning), which is exactly where a
// dense-sequencing fix can go wrong, so a second queue publishes concurrently to
// keep the sequence spaces interleaving.
//
// Spec basis: AMQP 0-9-1 §1.8.3.13 (basic.deliver redelivered flag) — the flag
// MUST be set on any delivery that has been delivered before.
func TestBet0_RequeueRedeliveredUnderMultiQueueLoad(t *testing.T) {
	const total = 500
	_, uri := bet0Server(t, t.TempDir())

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()

	const mainQ = "bet0-requeue-q1"
	const noiseQ = "bet0-requeue-q2"

	// Background noise on a second queue so the two sequence spaces interleave.
	noiseCh, err := conn.Channel()
	require.NoError(t, err)
	_, err = noiseCh.QueueDeclare(noiseQ, true, false, false, false, nil)
	require.NoError(t, err)
	noiseConCh, err := conn.Channel()
	require.NoError(t, err)
	noiseDeliveries, err := noiseConCh.Consume(noiseQ, "", true, false, false, false, nil)
	require.NoError(t, err)
	noiseStop := make(chan struct{})
	var noiseStopOnce sync.Once
	defer noiseStopOnce.Do(func() { close(noiseStop) })
	go func() {
		for {
			select {
			case <-noiseStop:
				return
			case <-noiseDeliveries:
			}
		}
	}()
	go func() {
		pub := amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(0, bet0BodySize)}
		for {
			select {
			case <-noiseStop:
				return
			default:
			}
			if err := noiseCh.PublishWithContext(context.Background(), "", noiseQ, false, false, pub); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	pubCh, err := conn.Channel()
	require.NoError(t, err)
	_, err = pubCh.QueueDeclare(mainQ, true, false, false, false, nil)
	require.NoError(t, err)

	conCh, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, conCh.Qos(bet0Prefetch, 0, false))
	deliveries, err := conCh.Consume(mainQ, "", false, false, false, false, nil)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	for seq := uint64(0); seq < total; seq++ {
		require.NoError(t, pubCh.PublishWithContext(context.Background(), "", mainQ, false, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
	}

	// Nack-requeue every message exactly once, ack it on its redelivery.
	firstSeen := make(map[uint64]bool, total)
	acked := make(map[uint64]bool, total)
	deadline := time.After(60 * time.Second)
	for len(acked) < total {
		select {
		case d, ok := <-deliveries:
			require.True(t, ok, "delivery channel closed early")
			seq := bet0Seq(d.Body)
			if !firstSeen[seq] {
				firstSeen[seq] = true
				require.Falsef(t, d.Redelivered,
					"first delivery of sequence %d must have redelivered=false", seq)
				require.NoError(t, d.Nack(false, true))
				continue
			}
			require.Truef(t, d.Redelivered,
				"REDELIVERED FLAG NOT SET: sequence %d was requeued and re-delivered, so "+
					"basic.deliver MUST carry redelivered=true (AMQP 0-9-1 §1.8.3.13)", seq)
			require.Falsef(t, acked[seq], "sequence %d delivered again after being acked", seq)
			acked[seq] = true
			require.NoError(t, d.Ack(false))
		case <-deadline:
			t.Fatalf("requeue round-trip incomplete: %d/%d acked (%d seen once) — "+
				"requeued messages must be redelivered", len(acked), total, len(firstSeen))
		}
	}
	require.Len(t, acked, total)
}

// TestBet0_SparseMultiQueueRecovery is the sharpest recovery test in this file
// and the one most likely to catch a per-queue-sequence rebuild that is subtly
// wrong.
//
// Today a queue's dispatch cursor is recovered as the RANGE [minTag, maxTag] of
// its surviving global tags, and holes inside that range (tags acked before the
// restart) are discovered lazily at delivery time. Once sequencing is per-queue
// and dense, the surviving messages of each queue must be RENUMBERED into a
// dense sequence at recovery — and the delivery tag is not persisted in the WAL
// record (it is derived on read-back), so that renumbering is genuinely
// reconstructed rather than read back. Renumbering is easy to get right when the
// survivors are a contiguous suffix and easy to get WRONG when they are
// scattered, so this test acks every EVEN sequence and leaves every ODD one
// outstanding, producing a maximally sparse survivor set on two interleaved
// queues at once.
//
// Invariants asserted after restart, per queue:
//   - every unacked (odd) message is redelivered — none lost;
//   - nothing is delivered twice within the post-restart pass;
//   - everything recovered arrives in ascending publication order.
//
// Deliberately NOT asserted: that an already-acked message never comes back.
// AMQP 0-9-1 is an at-least-once protocol, and Server.Stop() closes the listener
// without checkpointing the ack state, so a restart is crash-equivalent and
// redelivering an acked message is permitted. (Measured on main: acked messages
// DO resurface here. That is a real durability observation, but it is
// pre-existing, spec-legal, and out of scope for Bet 0 — it must not be smuggled
// into this test as a fix requirement.) Acked messages that resurface are
// tolerated and counted, not failed on; the loss and ordering invariants above
// are what a bad per-queue renumbering would break, and they remain strict.
func TestBet0_SparseMultiQueueRecovery(t *testing.T) {
	const perQueue = 1000 // 500 acked (even), 500 surviving (odd)
	dir := t.TempDir()
	srv, uri := bet0Server(t, dir)

	names := []string{"bet0-sparse-q1", "bet0-sparse-q2"}

	func() {
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()

		chans := make([]*amqp.Channel, len(names))
		confirms := make([]chan amqp.Confirmation, len(names))
		for i, n := range names {
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(n, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms[i] = ch.NotifyPublish(make(chan amqp.Confirmation, perQueue+16))
			chans[i] = ch
		}
		// Interleaved across both durable queues, so neither queue's surviving
		// sequence is contiguous in the global tag space.
		for seq := uint64(0); seq < perQueue; seq++ {
			for i, n := range names {
				require.NoError(t, chans[i].PublishWithContext(context.Background(), "", n, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
			}
		}
		for i, n := range names {
			for got := 0; got < perQueue; got++ {
				select {
				case c := <-confirms[i]:
					require.Truef(t, c.Ack, "queue %s: publish %d nacked", n, c.DeliveryTag)
				case <-time.After(60 * time.Second):
					t.Fatalf("queue %s: only %d/%d confirmed", n, got, perQueue)
				}
			}
		}

		// Deliver everything with manual ack; ack ONLY the even sequences.
		// Prefetch 0 (unlimited, broker-capped at UnlimitedPrefetchCap=2000) is
		// required here: half the messages are deliberately left unacked, so any
		// finite prefetch below perQueue/2 would fill with unacked odd sequences
		// and stall delivery before the queue is fully drained.
		for _, n := range names {
			ch, err := conn.Channel()
			require.NoError(t, err)
			require.NoError(t, ch.Qos(0, 0, false))
			deliveries, err := ch.Consume(n, "", false, false, false, false, nil)
			require.NoError(t, err)

			seen := make(map[uint64]bool, perQueue)
			deadline := time.After(60 * time.Second)
			for len(seen) < perQueue {
				select {
				case d, ok := <-deliveries:
					require.Truef(t, ok, "queue %s: channel closed after %d", n, len(seen))
					seq := bet0Seq(d.Body)
					if seen[seq] {
						continue
					}
					seen[seq] = true
					if seq%2 == 0 {
						require.NoError(t, d.Ack(false))
					}
				case <-deadline:
					t.Fatalf("queue %s: only %d/%d delivered before the ack phase finished", n, len(seen), perQueue)
				}
			}
		}
		// Give the acks time to be durably recorded before the restart.
		time.Sleep(1 * time.Second)
	}()

	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()

	for _, n := range names {
		ch, err := conn2.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(n, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(n, "", true, false, false, false, nil)
		require.NoError(t, err)

		want := perQueue / 2 // the odd sequences, which were never acked
		survivors := make(map[uint64]bool, want)
		seen := make(map[uint64]int, perQueue)
		resurfaced := 0
		var lastSeq uint64
		var haveLast bool
		deadline := time.After(60 * time.Second)
		for len(survivors) < want {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "queue %s: channel closed after %d/%d unacked messages recovered",
					n, len(survivors), want)
				seq := bet0Seq(d.Body)
				seen[seq]++
				require.Equalf(t, 1, seen[seq],
					"queue %s: sequence %d delivered more than once in the post-restart pass", n, seq)
				if haveLast {
					require.Greaterf(t, seq, lastSeq,
						"queue %s: recovery must preserve publication order — got %d after %d",
						n, seq, lastSeq)
				}
				lastSeq, haveLast = seq, true
				if seq%2 == 1 {
					survivors[seq] = true
				} else {
					resurfaced++ // spec-legal at-least-once redelivery; see doc comment
				}
			case <-deadline:
				t.Fatalf("SPARSE RECOVERY LOSS: queue %s redelivered only %d/%d messages that "+
					"were still unacked at restart", n, len(survivors), want)
			}
		}
		t.Logf("queue %s: recovered all %d unacked messages in order (%d already-acked messages also resurfaced, permitted)",
			n, len(survivors), resurfaced)
		_ = ch.Close()
	}
}

// bet0ServerWithConfig is bet0Server with a hook to tweak the config before the
// server is built (used to force fast checkpoint/compaction cycles).
func bet0ServerWithConfig(t *testing.T, dir string, tweak func(*config.AMQPConfig)) (*server.Server, string) {
	t.Helper()
	port := bet0NextPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "silent"
	if tweak != nil {
		tweak(cfg)
	}

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err, "server build")
	waitForListening(t, srv)
	t.Cleanup(func() { testShutdownServer(t, srv) })
	return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr)
}

// TestBet0_FreshQueueFirstDeliveryIsImmediate guards the single easiest way to
// get per-queue sequencing catastrophically wrong.
//
// If the dispatch tag carries a per-queue component in its high bits, a queue's
// tags no longer start near zero — queue number N mints its first tag at some
// base far above 0. QueueState.tail/head/minAckCursor are atomic.Uint64 whose
// zero value is 0, so if the fresh-queue path leaves them at 0 while head is set
// from the first minted tag, Claim's one-integer-at-a-time walk has to cross the
// entire base before it can deliver anything — a hang strictly worse than the P0
// this loop is fixing. (Recovery sets all three cursors from recovered tags, so
// it is the FRESH queue path that is exposed.)
//
// Several queues are declared in sequence on one broker so that they take
// DIFFERENT per-queue identities, and each must deliver its very first message
// promptly. The bound is wall-clock and generous: a healthy broker does this in
// single-digit milliseconds, and the broken behaviour is unbounded, so this
// fails as a clear assertion rather than hanging.
func TestBet0_FreshQueueFirstDeliveryIsImmediate(t *testing.T) {
	const (
		queues       = 8
		firstMsgWait = 5 * time.Second
	)
	_, uri := bet0Server(t, t.TempDir())

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()

	for i := 0; i < queues; i++ {
		// Durable and transient alternately: both publish paths mint tags.
		durable := i%2 == 0
		qName := fmt.Sprintf("bet0-fresh-q%d", i)

		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(qName, durable, false, false, false, nil)
		require.NoError(t, err)
		deliveries, err := ch.Consume(qName, "", true, false, false, false, nil)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		mode := amqp.Transient
		if durable {
			mode = amqp.Persistent
		}
		start := time.Now()
		require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
			amqp.Publishing{DeliveryMode: mode, Body: bet0Body(uint64(i), bet0BodySize)}))

		select {
		case d, ok := <-deliveries:
			require.Truef(t, ok, "queue %s: delivery channel closed", qName)
			require.Equal(t, uint64(i), bet0Seq(d.Body))
			t.Logf("queue %s (durable=%v): first delivery in %v", qName, durable, time.Since(start))
		case <-time.After(firstMsgWait):
			t.Fatalf("FRESH QUEUE NEVER DELIVERED: queue %s (durable=%v, the %d-th queue declared "+
				"on this broker) did not deliver its first message within %s. A newly declared "+
				"queue's dispatch cursors must start at that queue's own sequence origin — if they "+
				"start at zero while the queue's first tag is minted far above zero, the claim walk "+
				"has to cross the whole gap before delivering anything.",
				qName, durable, i, firstMsgWait)
		}
		_ = ch.Close()
	}
}

// TestBet0_CrossQueueAckIsolationAcrossCheckpoint asserts the hazard that makes
// the shared WAL dangerous under per-queue sequencing: acking messages on one
// queue must never affect another queue's durable state.
//
// There is ONE WAL shared by every queue, and its offsetIndex, ackBitmap and
// currentFileOffsets are keyed by a bare uint64 with no queue discriminator
// (storage/wal_manager.go:273, :1359, :2110), while the WAL stamps the delivery
// tag from that same offset on read-back (:1819). So the delivery tag IS the WAL
// key. If two queues can ever produce the same tag, an ack on queue A marks
// queue B's record acked, and the checkpoint/compaction pass then reclaims a
// live message — silent, durable data loss that no liveness test would notice.
//
// This test drives exactly that: queue A is fully consumed and acked while queue
// B is published and left entirely untouched, with checkpoint and compaction
// configured to run aggressively so a reclaim pass definitely happens in the
// window, and then the broker is restarted. Every one of queue B's messages must
// survive, in order. It passes today (tags are globally unique via one counter)
// and must keep passing once tags are per-queue — it is the regression gate on
// tag uniqueness.
func TestBet0_CrossQueueAckIsolationAcrossCheckpoint(t *testing.T) {
	const perQueue = 1500
	dir := t.TempDir()

	fastCheckpoint := func(cfg *config.AMQPConfig) {
		// Force checkpoint + compaction to run inside the test window instead of
		// the 5-minute / 30-minute production cadence, so an ack that wrongly
		// marks another queue's record actually gets a chance to reclaim it.
		cfg.Engine.SegmentCheckpointIntervalMS = 500
		cfg.Engine.CompactionIntervalMS = 500
		cfg.Engine.CompactionThreshold = 0.1
	}

	srv, uri := bet0ServerWithConfig(t, dir, fastCheckpoint)

	const ackedQ = "bet0-xack-acked"   // fully consumed and acked
	const intactQ = "bet0-xack-intact" // published, never consumed

	func() {
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()

		names := []string{ackedQ, intactQ}
		chans := make([]*amqp.Channel, len(names))
		confirms := make([]chan amqp.Confirmation, len(names))
		for i, n := range names {
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(n, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms[i] = ch.NotifyPublish(make(chan amqp.Confirmation, perQueue+16))
			chans[i] = ch
		}
		// Interleaved, so the two queues' records are adjacent in the shared WAL
		// and a mis-keyed ack lands on a neighbouring record rather than a
		// conveniently distant one.
		for seq := uint64(0); seq < perQueue; seq++ {
			for i, n := range names {
				require.NoError(t, chans[i].PublishWithContext(context.Background(), "", n, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
			}
		}
		for i, n := range names {
			for got := 0; got < perQueue; got++ {
				select {
				case c := <-confirms[i]:
					require.Truef(t, c.Ack, "queue %s: publish %d nacked", n, c.DeliveryTag)
				case <-time.After(60 * time.Second):
					t.Fatalf("queue %s: only %d/%d confirmed", n, got, perQueue)
				}
			}
		}

		// Drain and ack ALL of ackedQ. intactQ is never consumed at all.
		ch, err := conn.Channel()
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(ackedQ, "", false, false, false, false, nil)
		require.NoError(t, err)
		acked := make(map[uint64]bool, perQueue)
		deadline := time.After(60 * time.Second)
		for len(acked) < perQueue {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "queue %s: channel closed after %d acks", ackedQ, len(acked))
				require.NoError(t, d.Ack(false))
				acked[bet0Seq(d.Body)] = true
			case <-deadline:
				t.Fatalf("queue %s: only acked %d/%d", ackedQ, len(acked), perQueue)
			}
		}
		_ = ch.Close()

		// Let at least a few checkpoint + compaction cycles run against a WAL
		// that is now half fully-acked and half fully-live.
		time.Sleep(3 * time.Second)
	}()

	testShutdownServer(t, srv)
	_, uri2 := bet0ServerWithConfig(t, dir, fastCheckpoint)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(intactQ, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.Qos(bet0Prefetch, 0, false))
	deliveries, err := ch2.Consume(intactQ, "", true, false, false, false, nil)
	require.NoError(t, err)

	got := make(map[uint64]int, perQueue)
	var want uint64
	deadline := time.After(90 * time.Second)
	for len(got) < perQueue {
		select {
		case d, ok := <-deliveries:
			require.Truef(t, ok, "queue %s: channel closed after %d/%d", intactQ, len(got), perQueue)
			seq := bet0Seq(d.Body)
			got[seq]++
			require.Equalf(t, 1, got[seq], "queue %s: message %d delivered more than once", intactQ, seq)
			require.Equalf(t, want, seq,
				"queue %s: recovered out of publication order — expected %d, got %d", intactQ, want, seq)
			want++
		case <-deadline:
			missing := make([]uint64, 0, 8)
			for s := uint64(0); s < perQueue && len(missing) < 8; s++ {
				if got[s] == 0 {
					missing = append(missing, s)
				}
			}
			t.Fatalf("CROSS-QUEUE ACK CORRUPTION: queue %q was never consumed, yet only %d/%d of its "+
				"messages survived a checkpoint/compaction cycle during which a DIFFERENT queue (%q) "+
				"was fully acked. Acking one queue must never reclaim another queue's records from the "+
				"shared WAL. First missing sequences: %v",
				intactQ, len(got), perQueue, ackedQ, missing)
		}
	}
	t.Logf("queue %s: all %d messages survived intact across checkpoint/compaction + restart while %s was fully acked",
		intactQ, len(got), ackedQ)
}

// TestBet0_PublishAfterRestartDoesNotReuseSequences closes the hole that every
// other recovery test in this file leaves open: they all restart and then only
// CONSUME. None of them PUBLISHES after the restart, so none of them can see a
// per-queue sequence counter that failed to be restored.
//
// The invariant: after a restart, a queue must never mint a delivery tag it has
// already used. The delivery tag is the shared WAL's key — `offsetIndex` is
// written as `qw.offsetIndex[rec.Offset]` (storage/wal_manager.go:771, :1338,
// :1346) and read at :2199, and the WAL stamps `DeliveryTag: off` on read-back
// (:1819) — so a reissued tag overwrites a live index entry. It also lands below
// the queue's recovered claim cursor, where no consumer will ever reach it.
// Either way the message is silently lost, and no liveness test would notice.
//
// The sharpest case is a queue that was FULLY consumed and acked before the
// restart: nothing is recovered as unacked, so a sequence counter restored only
// from recovered messages has nothing to restore from and rewinds to zero, while
// the WAL records of those acked messages are still physically present. This
// test builds exactly that state on two interleaved durable queues, restarts,
// then publishes a fresh batch and requires every one of the new messages to be
// delivered, in order, with confirms honoured.
//
// SCOPE — what this test does and does not assert. It asserts only that every
// POST-restart confirmed message is delivered exactly once and in order. It does
// NOT assert that already-acked PRE-restart messages stay gone, even though they
// demonstrably resurface: Server.Stop() does not checkpoint ack state, so a
// restart is crash-equivalent, and AMQP 0-9-1 is at-least-once, which makes that
// redelivery spec-legal. TestBet0_SparseMultiQueueRecovery in this file already
// records that ruling, and an earlier draft of this test contradicted it by
// failing on any pre-restart delivery. That assertion was wrong against the spec
// and has been removed; resurrected messages are now counted and reported.
//
// The resurrection itself is a real and separate durability defect (acked
// records in the not-yet-rolled WAL file are never reclaimed, so they recover
// forever). It is PRE-EXISTING — this test's post-restart-loss assertion fails
// identically on unmodified main — and belongs to its own workstream, not to the
// per-queue sequencing change. Keeping the two apart is what lets this test be a
// gate for sequence reuse without being a hostage to the other bug.
func TestBet0_PublishAfterRestartDoesNotReuseSequences(t *testing.T) {
	const (
		beforeRestart = 500
		afterRestart  = 500
	)
	dir := t.TempDir()
	srv, uri := bet0Server(t, dir)

	names := []string{"bet0-reseq-q1", "bet0-reseq-q2"}

	// Phase 1: publish, confirm, then fully consume and ack BOTH queues, so the
	// post-restart recovered set is empty while the WAL still holds the records.
	func() {
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()

		chans := make([]*amqp.Channel, len(names))
		confirms := make([]chan amqp.Confirmation, len(names))
		for i, n := range names {
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(n, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms[i] = ch.NotifyPublish(make(chan amqp.Confirmation, beforeRestart+16))
			chans[i] = ch
		}
		for seq := uint64(0); seq < beforeRestart; seq++ {
			for i, n := range names {
				require.NoError(t, chans[i].PublishWithContext(context.Background(), "", n, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
			}
		}
		for i, n := range names {
			for got := 0; got < beforeRestart; got++ {
				select {
				case c := <-confirms[i]:
					require.Truef(t, c.Ack, "queue %s: pre-restart publish %d nacked", n, c.DeliveryTag)
				case <-time.After(60 * time.Second):
					t.Fatalf("queue %s: only %d/%d pre-restart publishes confirmed", n, got, beforeRestart)
				}
			}
		}
		for _, n := range names {
			ch, err := conn.Channel()
			require.NoError(t, err)
			require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
			deliveries, err := ch.Consume(n, "", false, false, false, false, nil)
			require.NoError(t, err)
			acked := 0
			deadline := time.After(60 * time.Second)
			for acked < beforeRestart {
				select {
				case d, ok := <-deliveries:
					require.Truef(t, ok, "queue %s: channel closed after %d acks", n, acked)
					require.NoError(t, d.Ack(false))
					acked++
				case <-deadline:
					t.Fatalf("queue %s: only acked %d/%d before restart", n, acked, beforeRestart)
				}
			}
			_ = ch.Close()
		}
		time.Sleep(1 * time.Second) // let the acks settle
	}()

	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	// Phase 2: publish a fresh batch to each queue and require all of it back.
	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()

	for _, n := range names {
		pubCh, err := conn2.Channel()
		require.NoError(t, err)
		_, err = pubCh.QueueDeclare(n, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, pubCh.Confirm(false))
		confirms := pubCh.NotifyPublish(make(chan amqp.Confirmation, afterRestart+16))

		conCh, err := conn2.Channel()
		require.NoError(t, err)
		require.NoError(t, conCh.Qos(bet0Prefetch, 0, false))
		deliveries, err := conCh.Consume(n, "", true, false, false, false, nil)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		// Sequences are offset well clear of the pre-restart ones so a delivered
		// body identifies unambiguously which era it came from.
		const era = uint64(1_000_000)
		for i := uint64(0); i < afterRestart; i++ {
			require.NoError(t, pubCh.PublishWithContext(context.Background(), "", n, false, false,
				amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(era+i, bet0BodySize)}))
		}
		for got := 0; got < afterRestart; got++ {
			select {
			case c := <-confirms:
				require.Truef(t, c.Ack, "queue %s: post-restart publish %d nacked", n, c.DeliveryTag)
			case <-time.After(60 * time.Second):
				t.Fatalf("queue %s: only %d/%d post-restart publishes confirmed", n, got, afterRestart)
			}
		}

		// Every POST-restart confirmed message must arrive exactly once. Whether
		// already-acked PRE-restart messages resurface is deliberately NOT
		// asserted here — see the doc comment: that is spec-legal at-least-once
		// redelivery after a crash-equivalent restart, and asserting against it
		// would contradict the ruling already recorded on
		// TestBet0_SparseMultiQueueRecovery in this same file. Resurrected
		// messages are counted and reported, not failed on.
		seen := make(map[uint64]int, afterRestart)
		var want uint64
		resurrected := 0
		deadline := time.After(60 * time.Second)
		for len(seen) < afterRestart {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "queue %s: channel closed after %d/%d", n, len(seen), afterRestart)
				seq := bet0Seq(d.Body)
				if seq < era {
					resurrected++ // pre-restart, already acked; permitted
					continue
				}
				seen[seq]++
				require.Equalf(t, 1, seen[seq], "queue %s: post-restart message %d delivered twice", n, seq)
				require.Equalf(t, era+want, seq,
					"queue %s: post-restart messages must be delivered in publication order — "+
						"expected %d, got %d", n, era+want, seq)
				want++
			case <-deadline:
				missing := make([]uint64, 0, 8)
				for i := uint64(0); i < afterRestart && len(missing) < 8; i++ {
					if seen[era+i] == 0 {
						missing = append(missing, era+i)
					}
				}
				t.Fatalf("POST-RESTART SEQUENCE REUSE: queue %s confirmed all %d post-restart "+
					"publishes but only %d were ever delivered (%d resurrected pre-restart "+
					"messages also arrived). After a restart a queue must not mint a delivery tag "+
					"it has already used: the reissued tag collides with the still-resident WAL "+
					"record at that offset, so the newly confirmed message is either shadowed by "+
					"the old record or lands below the recovered claim cursor — lost either way, "+
					"despite having been confirmed. First missing: %v",
					n, afterRestart, len(seen), resurrected, missing)
			}
		}
		t.Logf("queue %s: all %d post-restart confirmed messages delivered in order (%d already-acked "+
			"pre-restart messages also resurfaced, permitted)", n, len(seen), resurrected)
		_ = pubCh.Close()
		_ = conCh.Close()
	}
}

// TestBet0_ConcurrentDeclareSurvivesRestart guards the queue-identity record
// against concurrent declares.
//
// Idempotent "declare the queue, then use it" is the single most common AMQP
// client pattern, and several connections doing it at once for the same
// brand-new queue is completely ordinary. If a queue's persisted identity (the
// record that tells a restarted broker which delivery-tag space the queue's
// existing messages live in) can be established by one declarer and then
// clobbered by a slower concurrent declarer writing a default record, the queue
// comes back after a restart believing it owns a different tag space from the
// one its own durable messages were written in. Its claim cursor and its
// minting then sit in different regions of the tag space, and the dispatch
// plane — which advances one integer at a time — can never bridge them.
//
// That failure is invisible until the restart, and then presents as a queue
// that accepts and confirms publishes but delivers nothing, which is why it
// needs a black-box test rather than an assertion on internals.
//
// Shape: many queues, each declared concurrently by several connections, all
// durable and all confirmed, then a restart, then a fresh publish to every
// queue. Every queue must deliver both its recovered messages and its new one.
// Deliberately many queues, because the race window is narrow and one queue
// hitting it is enough to fail the test.
//
// THIS IS A RACE DETECTOR, NOT A DETERMINISTIC TEST. A single run does not
// prove absence. Measured against the known-bad build it caught the defect in
// roughly 1 run in 3 (30 queues x 12 declarers), so a clean single run is weak
// evidence. Run it with -count=5 or more when using it as a gate:
//
//	go test -run TestBet0_ConcurrentDeclareSurvivesRestart -count=5 .
//
// A failure is never a flake — the queue identity either survived the restart
// or it did not, and the assertion only fires when it did not.
func TestBet0_ConcurrentDeclareSurvivesRestart(t *testing.T) {
	const (
		queues         = 30
		declarersEach  = 12
		msgsPerQueue   = 20
		deliveryBudget = 20 * time.Second
	)
	dir := t.TempDir()
	srv, uri := bet0Server(t, dir)

	qName := func(i int) string { return fmt.Sprintf("bet0-cdecl-q%d", i) }

	// Phase 1: race the declares, then publish and confirm.
	func() {
		conns := make([]*amqp.Connection, declarersEach)
		for i := range conns {
			c, err := amqp.Dial(uri)
			require.NoError(t, err)
			conns[i] = c
			defer c.Close()
		}

		// All declarers hit every queue name at the same instant.
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, queues*declarersEach)
		for d := 0; d < declarersEach; d++ {
			for q := 0; q < queues; q++ {
				wg.Add(1)
				go func(conn *amqp.Connection, q int) {
					defer wg.Done()
					ch, err := conn.Channel()
					if err != nil {
						errs <- err
						return
					}
					defer ch.Close()
					<-start
					if _, err := ch.QueueDeclare(qName(q), true, false, false, false, nil); err != nil {
						errs <- err
					}
				}(conns[d], q)
			}
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err, "concurrent declare must succeed")
		}

		pubConn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer pubConn.Close()
		ch, err := pubConn.Channel()
		require.NoError(t, err)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, queues*msgsPerQueue+16))
		for q := 0; q < queues; q++ {
			for m := uint64(0); m < msgsPerQueue; m++ {
				require.NoError(t, ch.PublishWithContext(context.Background(), "", qName(q), false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(m, bet0BodySize)}))
			}
		}
		for got := 0; got < queues*msgsPerQueue; got++ {
			select {
			case c := <-confirms:
				require.Truef(t, c.Ack, "publish %d nacked", c.DeliveryTag)
			case <-time.After(60 * time.Second):
				t.Fatalf("only %d/%d publishes confirmed", got, queues*msgsPerQueue)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	testShutdownServer(t, srv)
	_, uri2 := bet0ServerWithConfig(t, dir, nil)

	// Phase 2: every queue must still work — recovered messages AND a new one.
	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()

	const marker = uint64(999_999)
	for q := 0; q < queues; q++ {
		ch, err := conn2.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(qName(q), true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(qName(q), "", true, false, false, false, nil)
		require.NoError(t, err)

		require.NoError(t, ch.PublishWithContext(context.Background(), "", qName(q), false, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(marker, bet0BodySize)}))

		// Expect the msgsPerQueue recovered messages plus the new marker.
		wantTotal := msgsPerQueue + 1
		seen := make(map[uint64]int, wantTotal)
		deadline := time.After(deliveryBudget)
		for len(seen) < wantTotal {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "queue %s: channel closed after %d/%d", qName(q), len(seen), wantTotal)
				seq := bet0Seq(d.Body)
				seen[seq]++
				require.Equalf(t, 1, seen[seq], "queue %s: message %d delivered twice", qName(q), seq)
			case <-deadline:
				_, gotMarker := seen[marker]
				t.Fatalf("QUEUE IDENTITY LOST ACROSS RESTART: queue %s delivered only %d of %d "+
					"expected messages within %s after a restart (new post-restart publish "+
					"delivered: %v). It was declared concurrently by %d connections before the "+
					"restart; if a concurrent declare overwrote the queue's persisted identity, the "+
					"queue now believes it owns a different delivery-tag space than its own durable "+
					"messages occupy, and its dispatch cursor can never reach them.",
					qName(q), len(seen), wantTotal, deliveryBudget, gotMarker, declarersEach)
			}
		}
		require.Contains(t, seen, marker, "queue %s: post-restart publish must be delivered", qName(q))
		_ = ch.Close()
	}
	t.Logf("all %d concurrently-declared queues recovered and accepted new publishes after restart", queues)
}

// TestBet0_RestartWithBacklogThenPublish is permanent coverage for the exact
// shape that let a silent-message-loss regression get written: a durable queue
// is restarted while it still holds an unacked backlog, and then MORE messages
// are published to it.
//
// Every other restart test in this suite either only consumes after the restart
// or only restarts an empty/fully-drained queue, so none of them can observe a
// queue whose post-restart minting overlaps tags it already used. That gap is
// why this needs to exist as its own test rather than as a variation of one.
//
// The invariant is the product's core durability promise — a publisher confirm
// means the broker has taken responsibility for the message, so every confirmed
// message must be consumable, whether it was confirmed before or after the
// restart. A queue that resumes minting at a tag it has already issued violates
// this twice over: the new message overwrites the shared WAL's index entry for
// the old one, and it lands below the queue's recovered claim cursor where no
// consumer will ever reach it — while the publisher has already been told it is
// safe.
//
// Deterministic by construction: no races, no timing windows. It either
// preserves every confirmed message or it does not.
func TestBet0_RestartWithBacklogThenPublish(t *testing.T) {
	const (
		beforeRestart = 400
		afterRestart  = 400
		era           = uint64(500_000) // post-restart sequences start here
	)
	dir := t.TempDir()
	srv, uri := bet0Server(t, dir)

	const qName = "bet0-backlog-q"

	// Phase 1: publish a durable backlog and confirm it. Deliberately NO
	// consumer — the queue must still be holding all of it at restart.
	func() {
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, beforeRestart+16))

		for seq := uint64(0); seq < beforeRestart; seq++ {
			require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
				amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
		}
		for got := 0; got < beforeRestart; got++ {
			select {
			case c := <-confirms:
				require.Truef(t, c.Ack, "pre-restart publish %d nacked", c.DeliveryTag)
			case <-time.After(60 * time.Second):
				t.Fatalf("only %d/%d pre-restart publishes confirmed", got, beforeRestart)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	// Phase 2: publish MORE to the same queue, on top of the recovered backlog.
	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	pubCh, err := conn2.Channel()
	require.NoError(t, err)
	_, err = pubCh.QueueDeclare(qName, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, pubCh.Confirm(false))
	confirms := pubCh.NotifyPublish(make(chan amqp.Confirmation, afterRestart+16))

	for i := uint64(0); i < afterRestart; i++ {
		require.NoError(t, pubCh.PublishWithContext(context.Background(), "", qName, false, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(era+i, bet0BodySize)}))
	}
	for got := 0; got < afterRestart; got++ {
		select {
		case c := <-confirms:
			require.Truef(t, c.Ack, "post-restart publish %d nacked", c.DeliveryTag)
		case <-time.After(60 * time.Second):
			t.Fatalf("only %d/%d post-restart publishes confirmed", got, afterRestart)
		}
	}

	// Phase 3: every confirmed message — from BOTH eras — must be consumable.
	conCh, err := conn2.Channel()
	require.NoError(t, err)
	require.NoError(t, conCh.Qos(bet0Prefetch, 0, false))
	deliveries, err := conCh.Consume(qName, "", true, false, false, false, nil)
	require.NoError(t, err)

	want := beforeRestart + afterRestart
	seen := make(map[uint64]int, want)
	// A healthy run drains all 800 in well under a second; this bound exists
	// only so the failure mode is a fast, legible assertion instead of a hang.
	deadline := time.After(30 * time.Second)
	for len(seen) < want {
		select {
		case d, ok := <-deliveries:
			require.Truef(t, ok, "delivery channel closed after %d/%d", len(seen), want)
			seq := bet0Seq(d.Body)
			seen[seq]++
			require.Equalf(t, 1, seen[seq], "message %d delivered more than once", seq)
		case <-deadline:
			missingOld, missingNew := 0, 0
			for s := uint64(0); s < beforeRestart; s++ {
				if seen[s] == 0 {
					missingOld++
				}
			}
			for i := uint64(0); i < afterRestart; i++ {
				if seen[era+i] == 0 {
					missingNew++
				}
			}
			t.Fatalf("CONFIRMED MESSAGE LOST ACROSS RESTART-WITH-BACKLOG: %d/%d delivered "+
				"(%d of %d pre-restart missing, %d of %d post-restart missing). Every one of "+
				"these was positively confirmed to the publisher, so every one must be "+
				"consumable. A queue that resumes minting at a tag it already issued "+
				"overwrites the earlier message's WAL index entry and lands the new message "+
				"below the recovered claim cursor, losing one or both.",
				len(seen), want, missingOld, beforeRestart, missingNew, afterRestart)
		}
	}
	t.Logf("all %d confirmed messages survived (%d pre-restart backlog + %d published after restart)",
		len(seen), beforeRestart, afterRestart)
}

// TestBet0_DeleteRedeclareDoesNotReuseIdentity pins the queue-identity lifecycle
// across delete and re-declare of the SAME queue name.
//
// Under composite tag = (queueOrdinal, perQueueSequence), a queue's ordinal is
// the only thing separating its tags from every other queue's in a broker-wide,
// bare-uint64-keyed WAL. If an ordinal were ever recycled onto a new incarnation
// of a deleted queue name while the old incarnation still had state on disk —
// unreclaimed WAL records, index entries, ack-bitmap bits — the new queue would
// be minting into a tag space that already has occupants. That is the exact
// cross-queue corruption class the composite tag exists to prevent, reached from
// inside a single queue name instead of across two.
//
// The current policy is monotonic ordinals with no reuse, which makes this test
// pass trivially. That is the point: it pins the policy so that a later ordinal
// reclaim optimisation cannot quietly violate it without turning something red.
//
// Asserted, per cycle: a re-declared queue delivers exactly the messages
// published to THIS incarnation, and never a message from a previous one. Then
// the whole thing is put through a restart, because recovery is where a
// mis-assigned ordinal actually surfaces.
func TestBet0_DeleteRedeclareDoesNotReuseIdentity(t *testing.T) {
	const (
		cycles      = 4
		perCycle    = 100
		cycleStride = uint64(10_000) // incarnation N uses sequences N*stride...
	)
	dir := t.TempDir()
	srv, uri := bet0Server(t, dir)

	const qName = "bet0-redeclare-q"

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()

	runCycle := func(t *testing.T, cycle int) {
		base := uint64(cycle) * cycleStride

		ch, err := conn.Channel()
		require.NoError(t, err)
		defer ch.Close()

		_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, perCycle+16))

		for i := uint64(0); i < perCycle; i++ {
			require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
				amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(base+i, bet0BodySize)}))
		}
		for got := 0; got < perCycle; got++ {
			select {
			case c := <-confirms:
				require.Truef(t, c.Ack, "cycle %d: publish %d nacked", cycle, c.DeliveryTag)
			case <-time.After(30 * time.Second):
				t.Fatalf("cycle %d: only %d/%d confirmed", cycle, got, perCycle)
			}
		}

		conCh, err := conn.Channel()
		require.NoError(t, err)
		defer conCh.Close()
		require.NoError(t, conCh.Qos(bet0Prefetch, 0, false))
		deliveries, err := conCh.Consume(qName, "", true, false, false, false, nil)
		require.NoError(t, err)

		seen := make(map[uint64]int, perCycle)
		deadline := time.After(30 * time.Second)
		for len(seen) < perCycle {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "cycle %d: channel closed after %d/%d", cycle, len(seen), perCycle)
				seq := bet0Seq(d.Body)
				require.GreaterOrEqualf(t, seq, base,
					"QUEUE IDENTITY REUSED: cycle %d received sequence %d, which belongs to a "+
						"PREVIOUS incarnation of queue %q. A re-declared queue must never deliver "+
						"messages published to the deleted queue that preceded it — if it does, the "+
						"new incarnation is minting into a tag space that still has occupants.",
					cycle, seq, qName)
				seen[seq]++
				require.Equalf(t, 1, seen[seq], "cycle %d: sequence %d delivered twice", cycle, seq)
			case <-deadline:
				t.Fatalf("cycle %d: only %d/%d of this incarnation's messages delivered",
					cycle, len(seen), perCycle)
			}
		}
		_ = conCh.Close()

		// Tear the queue down so the next cycle is a genuine re-declare.
		delCh, err := conn.Channel()
		require.NoError(t, err)
		_, err = delCh.QueueDelete(qName, false, false, false)
		require.NoError(t, err, "cycle %d: queue delete", cycle)
		_ = delCh.Close()
		time.Sleep(100 * time.Millisecond)
	}

	for cycle := 1; cycle <= cycles; cycle++ {
		runCycle(t, cycle)
	}

	// Final incarnation left in place with an unconsumed backlog, then restarted:
	// recovery is where a mis-assigned or recycled ordinal actually surfaces.
	finalBase := uint64(cycles+1) * cycleStride
	func() {
		ch, err := conn.Channel()
		require.NoError(t, err)
		defer ch.Close()
		_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, perCycle+16))
		for i := uint64(0); i < perCycle; i++ {
			require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
				amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(finalBase+i, bet0BodySize)}))
		}
		for got := 0; got < perCycle; got++ {
			select {
			case c := <-confirms:
				require.True(t, c.Ack, "final incarnation publish nacked")
			case <-time.After(30 * time.Second):
				t.Fatalf("final incarnation: only %d/%d confirmed", got, perCycle)
			}
		}
	}()
	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qName, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.Qos(bet0Prefetch, 0, false))
	deliveries, err := ch2.Consume(qName, "", true, false, false, false, nil)
	require.NoError(t, err)

	seen := make(map[uint64]int, perCycle)
	deadline := time.After(30 * time.Second)
	for len(seen) < perCycle {
		select {
		case d, ok := <-deliveries:
			require.Truef(t, ok, "after restart: channel closed after %d/%d", len(seen), perCycle)
			seq := bet0Seq(d.Body)
			require.GreaterOrEqualf(t, seq, finalBase,
				"QUEUE IDENTITY REUSED ACROSS RESTART: recovered sequence %d, which belongs to a "+
					"deleted earlier incarnation of %q. Recovery must not resurrect a previous "+
					"incarnation's messages into the current one.", seq, qName)
			seen[seq]++
			require.Equalf(t, 1, seen[seq], "after restart: sequence %d delivered twice", seq)
		case <-deadline:
			t.Fatalf("after restart: only %d/%d of the final incarnation's messages recovered",
				len(seen), perCycle)
		}
	}
	t.Logf("%d delete/redeclare cycles plus a restart: every incarnation delivered only its own messages", cycles)
}

// TestBet0_OrdinalAllocatorHighWaterSurvivesRestart pins the second of the two
// persistence properties that ordinal-aware recovery makes load-bearing.
//
// Property (a) — each queue's own ordinal is durably persisted — is exercised by
// TestBet0_DeleteRedeclareDoesNotReuseIdentity and by the restart tests.
//
// Property (b), covered here, is the ordinal ALLOCATOR's high-water mark. When a
// queue is deleted its metadata record goes away, but its durable records stay
// in the shared WAL (there is no purge-by-queue). If the allocator rebuilds its
// high-water only from the metadata of queues that still EXIST, then after a
// restart it can hand out an ordinal that a deleted queue already used — and the
// new owner of that ordinal starts minting tags that collide, in a broker-wide
// bare-uint64-keyed index, with records that are still physically present. That
// is cross-incarnation bleed reached through the allocator rather than through a
// single queue name, and no per-queue check would catch it.
//
// Two sequences, both requiring a restart to sit BETWEEN the delete and the next
// declare — which is exactly what none of the other tests do:
//
//  1. same name:  declare A -> publish -> delete A -> RESTART -> declare A ->
//     publish -> RESTART -> consume. A's second incarnation must serve only its
//     own messages.
//  2. fresh name: after A is deleted and the broker restarted, a brand-new queue
//     B must not be handed A's retired ordinal. B publishes, restarts, and must
//     get every one of its messages back.
//
// Sequence 2 is the one that isolates the allocator: B has no history of its
// own, so anything it loses can only have come from inheriting a dirty ordinal.
func TestBet0_OrdinalAllocatorHighWaterSurvivesRestart(t *testing.T) {
	const (
		firstBatch  = 200
		secondBatch = 200
		eraFirst    = uint64(0)
		eraSecond   = uint64(100_000)
		eraFresh    = uint64(200_000)
	)
	dir := t.TempDir()

	const qA = "bet0-hw-a"
	const qB = "bet0-hw-b"

	publishConfirmed := func(t *testing.T, uri, queue string, base uint64, n int) {
		t.Helper()
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, n+16))
		for i := uint64(0); i < uint64(n); i++ {
			require.NoError(t, ch.PublishWithContext(context.Background(), "", queue, false, false,
				amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(base+i, bet0BodySize)}))
		}
		for got := 0; got < n; got++ {
			select {
			case c := <-confirms:
				require.Truef(t, c.Ack, "%s: publish %d nacked", queue, c.DeliveryTag)
			case <-time.After(30 * time.Second):
				t.Fatalf("%s: only %d/%d confirmed", queue, got, n)
			}
		}
	}

	consumeExactly := func(t *testing.T, uri, queue string, base uint64, n int) {
		t.Helper()
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
		require.NoError(t, err)

		seen := make(map[uint64]int, n)
		deadline := time.After(30 * time.Second)
		for len(seen) < n {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "%s: channel closed after %d/%d", queue, len(seen), n)
				seq := bet0Seq(d.Body)
				require.GreaterOrEqualf(t, seq, base,
					"RETIRED ORDINAL REUSED: queue %q was served sequence %d, which belongs to a "+
						"deleted queue's records still resident in the shared WAL. The ordinal "+
						"allocator's high-water mark must survive restart, or a new queue inherits "+
						"a retired ordinal and mints tags that collide with those records.",
					queue, seq)
				seen[seq]++
				require.Equalf(t, 1, seen[seq], "%s: sequence %d delivered twice", queue, seq)
			case <-deadline:
				t.Fatalf("%s: only %d/%d of its own messages delivered", queue, len(seen), n)
			}
		}
	}

	// --- Sequence 1: same name, restart between delete and redeclare ---
	srv1, uri1 := bet0Server(t, dir)
	publishConfirmed(t, uri1, qA, eraFirst, firstBatch)

	func() {
		conn, err := amqp.Dial(uri1)
		require.NoError(t, err)
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDelete(qA, false, false, false)
		require.NoError(t, err, "delete %s", qA)
	}()
	testShutdownServer(t, srv1)

	// The allocator must come back knowing qA's retired ordinal is spent.
	srv2, uri2 := bet0Server(t, dir)
	publishConfirmed(t, uri2, qA, eraSecond, secondBatch)
	testShutdownServer(t, srv2)

	srv3, uri3 := bet0Server(t, dir)
	consumeExactly(t, uri3, qA, eraSecond, secondBatch)

	// --- Sequence 2: a brand-new name must not inherit a retired ordinal ---
	func() {
		conn, err := amqp.Dial(uri3)
		require.NoError(t, err)
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDelete(qA, false, false, false)
		require.NoError(t, err, "second delete of %s", qA)
	}()
	testShutdownServer(t, srv3)

	srv4, uri4 := bet0Server(t, dir)
	publishConfirmed(t, uri4, qB, eraFresh, firstBatch)
	testShutdownServer(t, srv4)

	_, uri5 := bet0Server(t, dir)
	consumeExactly(t, uri5, qB, eraFresh, firstBatch)

	t.Logf("ordinal allocator high-water survived %d restarts across delete/redeclare and a fresh queue", 4)
}

// TestBet0_LegacyDirMessageOnlyForUnversionedRecords pins the operator-facing
// distinction between two recovery states that look superficially alike but
// demand opposite responses.
//
//   - A queue was DELETED while its durable records were still in the shared WAL
//     (there is no purge-by-queue). Its metadata record is gone, so recovery
//     finds records under a name it knows nothing about. Correct response:
//     discard the dead queue's records and boot. Nothing is wrong.
//   - A data directory genuinely PREDATES per-queue delivery-tag sequencing: the
//     queue's metadata exists but carries no ordinal, while its records do.
//     Correct response: refuse to start, because the tags cannot be interpreted.
//
// Reporting the first as the second is the nastiest part of this defect class:
// it tells an operator their data directory is obsolete when they merely deleted
// a queue, and the remedy that message recommends — recreate the data directory —
// destroys every durable message they still have. So the message is not
// cosmetic, and it is asserted here rather than left to the implementation.
func TestBet0_LegacyDirMessageOnlyForUnversionedRecords(t *testing.T) {
	const legacyMarker = "predates per-queue delivery-tag sequencing"

	// --- Case C: deleted queue with records left behind. Must boot cleanly. ---
	t.Run("deleted_queue_boots_and_is_not_reported_as_legacy", func(t *testing.T) {
		dir := t.TempDir()
		srv, uri := bet0Server(t, dir)
		const qName = "bet0-legacymsg-deleted"

		func() {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			defer conn.Close()
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 64))
			for i := uint64(0); i < 50; i++ {
				require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(i, bet0BodySize)}))
			}
			for got := 0; got < 50; got++ {
				select {
				case c := <-confirms:
					require.True(t, c.Ack)
				case <-time.After(30 * time.Second):
					t.Fatalf("only %d/50 confirmed", got)
				}
			}
			_, err = ch.QueueDelete(qName, false, false, false)
			require.NoError(t, err)
		}()
		testShutdownServer(t, srv)

		// Must start. A deleted queue's leftover records are not a legacy dir.
		_, uri2, err := bet0TryServer(t, dir)
		require.NoErrorf(t, err,
			"deleting a durable queue and restarting must NOT prevent the broker from "+
				"starting — its leftover WAL records belong to a queue that no longer exists "+
				"and must simply be discarded: %v", err)
		require.NotContainsf(t, fmt.Sprint(err), legacyMarker,
			"a deleted queue must never be reported as a legacy data directory")
		conn, derr := amqp.Dial(uri2)
		require.NoError(t, derr, "broker must accept connections after the restart")
		_ = conn.Close()
	})

	// --- Case D: genuine pre-packing directory. Must refuse, with THE message. ---
	t.Run("unversioned_records_refuse_with_legacy_message", func(t *testing.T) {
		dir := t.TempDir()
		srv, uri := bet0Server(t, dir)
		const qName = "bet0-legacymsg-old"

		func() {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			defer conn.Close()
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 64))
			for i := uint64(0); i < 50; i++ {
				require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(i, bet0BodySize)}))
			}
			for got := 0; got < 50; got++ {
				select {
				case c := <-confirms:
					require.True(t, c.Ack)
				case <-time.After(30 * time.Second):
					t.Fatalf("only %d/50 confirmed", got)
				}
			}
		}()
		testShutdownServer(t, srv)

		// Forge the pre-packing state: the queue's metadata record survives but
		// carries no ordinal, while its durable records remain in the WAL. This
		// is exactly what an upgrade from a build without per-queue sequencing
		// looks like.
		qPath := filepath.Join(dir, storage.MetadataDir, storage.QueuesDir, qName+storage.FileExtension)
		raw, err := os.ReadFile(qPath)
		require.NoError(t, err, "queue metadata must exist at %s", qPath)
		var q protocol.Queue
		require.NoError(t, cbor.Unmarshal(raw, &q))
		require.NotZero(t, q.Ordinal, "precondition: the queue should have been assigned an ordinal")
		q.Ordinal = 0
		forged, err := cbor.Marshal(&q)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(qPath, forged, 0o644))

		_, _, err = bet0TryServer(t, dir)
		require.Errorf(t, err,
			"a data directory whose queue metadata carries no ordinal while its records do "+
				"must refuse to start — those tags cannot be interpreted")
		require.Containsf(t, err.Error(), legacyMarker,
			"the refusal must name the real cause so an operator is directed at an upgrade "+
				"path rather than at deleting data; got: %v", err)
	})
}

// bet0TryServer is bet0Server without the build assertion: it returns the build
// error instead of failing the test, so recovery-refusal behaviour can be
// asserted on directly.
func bet0TryServer(t *testing.T, dir string) (*server.Server, string, error) {
	t.Helper()
	port := bet0NextPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "silent"

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		return nil, "", err
	}
	// bet0TryServer, unlike every other embedded-broker helper in this file,
	// deliberately does NOT fail the test on a startup problem — callers
	// (e.g. the legacy-data-directory refusal-to-boot cases) assert on the
	// returned error themselves. So it cannot route through waitForListening,
	// which is FailNow-on-failure by design. It still must not discard
	// Start()'s error the way the pre-fix pattern did: a bind failure is
	// surfaced here as the real error rather than the generic "not ready".
	startErrCh := make(chan error, 1)
	go func() { startErrCh <- srv.Start() }()
	for i := 0; i < brokerStartupPolls; i++ {
		if srv.IsListening() {
			t.Cleanup(func() { testShutdownServer(t, srv) })
			return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr), nil
		}
		select {
		case startErr := <-startErrCh:
			return nil, "", startErr
		default:
		}
		time.Sleep(brokerStartupPollInterval)
	}
	return nil, "", fmt.Errorf("server listener not ready")
}

// TestBet0_MultiQueueWithConfirmsDelivers closes the blind spot that let a
// multi-queue starvation bug survive this entire loop's test suite.
//
// TestBet0_ConcurrentMultiQueueDelivery — the original P0 repro — publishes
// WITHOUT publisher confirms. It passes on the fixed broker. But the real
// benchmark workload (and the one the whole bet exists to unlock) is durable
// publishes WITH confirms and a bounded outstanding window, and under THAT shape
// one queue still receives essentially nothing while its neighbour runs at full
// speed. Measured with the standalone perftest harness, 2 queues x 3 producers /
// 3 consumers, durable+confirm, 1 KB bodies, 12s: the starved queue consumed
// 304 / 430 / 302 across three runs while its neighbour consumed ~1.72M each
// time — and with confirms turned OFF and nothing else changed, both queues ran
// healthily at ~199K/s aggregate.
//
// The confirm path matters mechanically, not incidentally: a bounded outstanding
// window couples the producer to the broker's confirm latency, so a queue whose
// confirms or deliveries stall throttles its own publishers. Testing only the
// unbounded no-confirm path cannot observe that coupling at all.
//
// It asserts the same liveness property as the original repro — each queue must
// consume a substantial fraction of what it published, measured DURING the
// publish window — because the defect is a rate failure, invisible to any
// after-the-fact drain.
//
// HONEST LIMITATION — THIS TEST DOES NOT REPRODUCE THE *PERFTEST* STARVATION,
// the concurrent-declare one described above. Uninstrumented it passes at
// 0.997/0.998 for 2 queues and ~0.985 for 4 at outstanding=1000. Do not read
// its green as coverage of that defect.
//
// ⚠️ CORRECTION, measured 2026-07-26: that sentence used to read "THIS TEST DOES
// NOT REPRODUCE THAT BUG" without qualification, and on the blanket reading it
// is FALSE. Under -race this shape DOES reproduce the multi-queue P0 on the
// pre-fix tree: ported to 8a65f98 it starves q2 to consumed=0, 3 runs of 3
// (q1 ratios 0.2141 / 0.2729 / 0.3011 beside q2 at 0.0000). So this test carries
// a real pristine-red obligation and its thresholds get the same treatment and
// the same proof as the P0 repro's — not a weaker one. The original claim was
// about the perftest case specifically; it read as blanket, and was believed as
// blanket, which is why it is spelled out here.
//
// The reason it cannot reproduce it is itself the key diagnostic. The trigger is
// CONCURRENT QUEUE DECLARATION, not load and not confirms alone. Two perftest
// processes declare their queues at the same instant and one queue then starves
// for the life of the process (4/4 runs, including at 1 producer / 1 consumer
// where the healthy queue runs at 101K/s, far below the 145K single-queue
// ceiling — so there is spare capacity the starved queue cannot use). Staggering
// the two declares by three seconds recovers it 3/3. Turning confirms off with
// simultaneous declares is also healthy. It needs BOTH concurrent declare AND
// confirms.
//
// This test declares every queue sequentially from one client before publishing,
// which is exactly the staggered-declare case. A test that reproduces the real
// defect must race the DECLARES across independent connections, and should be
// written once the mechanism is understood — guessing at the race shape would
// produce a test that passes for the wrong reason.
func TestBet0_MultiQueueWithConfirmsDelivers(t *testing.T) {
	const (
		queues      = 2
		producers   = 3
		consumers   = 3
		outstanding = 200 // per producer; mirrors perftest's bounded window
		bodySize    = 1024
		window      = 5 * time.Second
	)
	// Exactly the same predicate and the same thresholds as the P0 repro — this
	// shape reproduces the same defect on the pre-fix tree (see the correction
	// in the header), so it gets the same gate, not a weaker one. Thresholds and
	// the measurements behind them live in bet0_liveness_predicate_test.go.
	policy := bet0Policy()
	_, uri := bet0Server(t, t.TempDir())

	names := make([]string, queues)
	published := make([]*atomic.Int64, queues)
	consumed := make([]*atomic.Int64, queues)
	// E10: one closed-delivery counter per queue plus an Ack-error channel, so a
	// consumer death is surfaced rather than silent.
	closed := make([]*atomic.Int64, queues)
	for i := range names {
		names[i] = fmt.Sprintf("bet0-cfm-q%d", i+1)
		published[i] = &atomic.Int64{}
		consumed[i] = &atomic.Int64{}
		closed[i] = &atomic.Int64{}
	}
	ackErrors := make(chan error, 1)

	var connMu sync.Mutex
	var conns []*amqp.Connection
	track := func(c *amqp.Connection) *amqp.Connection {
		connMu.Lock()
		conns = append(conns, c)
		connMu.Unlock()
		return c
	}
	t.Cleanup(func() {
		connMu.Lock()
		pending := conns
		conns = nil
		connMu.Unlock()
		var wg sync.WaitGroup
		for _, c := range pending {
			wg.Add(1)
			go func(c *amqp.Connection) { defer wg.Done(); _ = c.CloseDeadline(time.Now().Add(2 * time.Second)) }(c)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})

	stop := make(chan struct{})
	var stopOnce sync.Once
	t.Cleanup(func() { stopOnce.Do(func() { close(stop) }) })

	// E2: same symmetric start gate as the P0 repro, so both queues' producers
	// begin publishing at one instant.
	start := bet0NewStartBarrier(t)

	// Consumers first so they are registered before any publish.
	for qi := 0; qi < queues; qi++ {
		for c := 0; c < consumers; c++ {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			track(conn)
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch.QueueDeclare(names[qi], true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
			deliveries, err := ch.Consume(names[qi], "", false, false, false, false, nil)
			require.NoError(t, err)
			cnt := consumed[qi]
			qName := names[qi]
			closedCount := closed[qi]
			go func() {
				for {
					select {
					case <-stop:
						return
					case d, ok := <-deliveries:
						if !ok {
							closedCount.Add(1)
							return
						}
						if err := d.Ack(false); err != nil {
							select {
							case ackErrors <- fmt.Errorf("queue %s: delivery Ack failed: %w", qName, err):
							default:
							}
							return
						}
						cnt.Add(1)
					}
				}
			}()
		}
	}

	// Producers with publisher confirms and a bounded outstanding window.
	body := make([]byte, bodySize)
	for qi := 0; qi < queues; qi++ {
		for p := 0; p < producers; p++ {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			track(conn)
			ch, err := conn.Channel()
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, outstanding+16))
			qName := names[qi]
			cnt := published[qi]
			go func() {
				if !start.Wait(stop) {
					return
				}
				pub := amqp.Publishing{DeliveryMode: amqp.Persistent, Body: body}
				inFlight := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					// Bounded outstanding: drain a confirm before exceeding the cap.
					if inFlight >= outstanding {
						select {
						case <-confirms:
							inFlight--
						case <-stop:
							return
						case <-time.After(5 * time.Second):
							return // confirms wedged; leave the count as evidence
						}
						continue
					}
					if err := ch.PublishWithContext(context.Background(), "", qName, false, false, pub); err != nil {
						return
					}
					inFlight++
					cnt.Add(1)
				}
			}()
		}
	}

	// E2 (PC-3): no producer may have published before the gate opens.
	var preRelease int64
	for _, p := range published {
		preRelease += p.Load()
	}
	require.Zero(t, preRelease, "a producer published before the start barrier was released")
	start.Release()

	time.Sleep(window)
	pubAt := make([]int64, queues)
	conAt := make([]int64, queues)
	for i := range names {
		pubAt[i] = published[i].Load()
		conAt[i] = consumed[i].Load()
	}
	stopOnce.Do(func() { close(stop) })

	samples := make([]bet0Sample, queues)
	for i, n := range names {
		// Identical consumer count and prefetch per queue — the premise the
		// balance term rests on. See bet0_liveness_predicate_test.go.
		samples[i] = bet0Sample{name: n, published: pubAt[i], consumed: conAt[i]}
		t.Logf("queue %s: published=%d consumed-in-window=%d (%.4f)",
			n, pubAt[i], conAt[i], samples[i].ratio())
	}
	if notice := bet0PremiseNotice(samples, policy); notice != "" {
		t.Log(notice)
	}
	if notice := bet0SignalNotice(samples); notice != "" {
		t.Log(notice)
	}
	// E10 (R2): surface any Ack error / consumer death in the failure message.
	// The verdict itself is unchanged.
	var earlyAckErr error
	select {
	case earlyAckErr = <-ackErrors:
	default:
	}
	hint := bet0ConsumerDeathHint(earlyAckErr, bet0ClosedCounts(closed), names)
	require.NoErrorf(t, bet0CheckLiveness(samples, policy),
		"UNDER PUBLISHER CONFIRMS, window=%s. With confirms disabled and nothing else "+
			"changed, both queues run healthily — so a failure here is specific to the "+
			"confirm path, and it is the exact workload (durable + confirms + multiple "+
			"queues) that multi-queue scaling exists to serve.%s", window, hint)
}

// ----------------------------------------------------------------------------
// 16. C1 — a queue.delete racing a concurrent declare must not leave behind a
//     live queue that CONFIRMS durable publishes recovery will then discard.
// ----------------------------------------------------------------------------

// c1PinnedStorage wraps the real storage so a test can PIN the one window that
// makes the C1 defect reachable, deterministically and without a sleep.
//
// The window: DeclareQueue reads the queue's metadata record LOCK-FREE
// (storage_broker.go, top of DeclareQueue) and, when the record exists, hands
// that pointer straight to declareExistingQueue WITHOUT ever taking the
// per-name create mutex. declareExistingQueue re-installs the queue in
// activeQueues and calls getOrCreateQueueState. If a DeleteQueue completes in
// between, the declarer is holding a record that no longer exists on disk: it
// republishes the queue into the routing cache and creates a fresh QueueState
// whose ordinal resolveQueueOrdinal allocates but — with no record to write it
// into — never persists. The result is a live, publishable, record-less queue.
//
// Pinning is done by blocking INSIDE storage.GetQueue, after the real read has
// returned, so the declarer holds exactly the stale value it would hold in the
// real interleaving. The trap fires only for a call whose stack is inside
// DeclareQueue, so unrelated GetQueue callers (DeleteQueue's own pre-check runs
// on another goroutine while this one is parked) pass through untouched — the
// window is pinned by identity, not by call ordering or timing.
type c1PinnedStorage struct {
	*storage.DisruptorStorage

	mu      sync.Mutex
	armed   bool
	target  string
	caller  string
	entered chan struct{}
	release chan struct{}

	// Fault injection, independent of the pin above: see armFault.
	faultArmed  bool
	faultTarget string
	faultCaller string
	faultSkip   int
	faultErr    error

	// Passive record probe, independent of both mechanisms above: see
	// armRecordProbe.
	probeArmed   bool
	probeTarget  string
	probeCaller  string
	probeAbsent  int
	probePresent int
}

// arm makes the next read of `name` block, but ONLY when the call comes from
// the broker function named by `caller` — so unrelated readers of the same name
// (DeleteQueue's own pre-checks, running on another goroutine while this one is
// parked) pass through untouched. The window is pinned by IDENTITY, not by call
// ordering or timing. Returns a channel closed once the caller is parked, and a
// channel the test closes to let it out.
func (s *c1PinnedStorage) arm(name, caller string) (entered, release chan struct{}) {
	entered, release = make(chan struct{}), make(chan struct{})
	s.mu.Lock()
	s.armed, s.target, s.caller = true, name, caller
	s.entered, s.release = entered, release
	s.mu.Unlock()
	return entered, release
}

// pin blocks the calling goroutine if it is the armed one. It is called AFTER
// the wrapped read has returned, so the caller is parked holding exactly the
// value the real interleaving would leave it holding — stale by the time it
// resumes.
func (s *c1PinnedStorage) pin(name string) {
	s.mu.Lock()
	trap := s.armed && name == s.target
	if trap {
		var buf [8192]byte
		if !strings.Contains(string(buf[:runtime.Stack(buf[:], false)]), s.caller) {
			trap = false // not the window we are pinning
		} else {
			s.armed = false // one-shot
		}
	}
	entered, release := s.entered, s.release
	s.mu.Unlock()

	if trap {
		close(entered)
		<-release
	}
}

// armFault makes a later GetQueue(name) return err instead of the real result,
// but only for a call whose stack is inside caller, and only after `skip`
// matching calls have been let through untouched. That precision is what makes
// it usable on DeclareQueue, which reads the same name twice for two different
// purposes: skip=1 leaves the lock-free pre-check alone and fails only the
// re-read under the create mutex. One-shot.
func (s *c1PinnedStorage) armFault(name, caller string, skip int, err error) {
	s.mu.Lock()
	s.faultArmed, s.faultTarget, s.faultCaller, s.faultSkip, s.faultErr = true, name, caller, skip, err
	s.mu.Unlock()
}

// fault reports the injected error if this call is the armed one.
func (s *c1PinnedStorage) fault(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.faultArmed || name != s.faultTarget {
		return nil
	}
	var buf [8192]byte
	if !strings.Contains(string(buf[:runtime.Stack(buf[:], false)]), s.faultCaller) {
		return nil
	}
	if s.faultSkip > 0 {
		s.faultSkip--
		return nil
	}
	s.faultArmed = false // one-shot
	return s.faultErr
}

// armRecordProbe makes every later GetQueue(name) whose stack is inside caller
// record whether the metadata record the caller RECEIVED was present or absent.
// It never blocks, never injects, and is not one-shot — the counts are the
// observable, so every matching call must be seen.
//
// It is a premise instrument, and it is usable as one precisely because it is
// INVARIANT under the code it gates: it reads the storage layer's answer, which
// sits upstream of both createQueueStateLocked's record-existence guard and
// republishToTargets' StopCh bail. Breaking either of those does not move these
// counts, so a regression cannot make the premise look unmet and slip out
// through a skip.
func (s *c1PinnedStorage) armRecordProbe(name, caller string) {
	s.mu.Lock()
	s.probeArmed, s.probeTarget, s.probeCaller = true, name, caller
	s.probeAbsent, s.probePresent = 0, 0
	s.mu.Unlock()
}

// observeRecord counts one armed GetQueue result, by what the caller receives
// rather than by what the disk held — the caller branches on the former.
func (s *c1PinnedStorage) observeRecord(name string, q *protocol.Queue, err error) {
	s.mu.Lock()
	armed, caller := s.probeArmed && name == s.probeTarget, s.probeCaller
	s.mu.Unlock()
	if !armed {
		return
	}
	var buf [8192]byte
	if !strings.Contains(string(buf[:runtime.Stack(buf[:], false)]), caller) {
		return
	}
	s.mu.Lock()
	if err != nil || q == nil {
		s.probeAbsent++
	} else {
		s.probePresent++
	}
	s.mu.Unlock()
}

// recordProbeCounts reports how the armed caller resolved the armed name:
// absent = the record was gone, present = it was there.
func (s *c1PinnedStorage) recordProbeCounts() (absent, present int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probeAbsent, s.probePresent
}

func (s *c1PinnedStorage) GetQueue(name string) (*protocol.Queue, error) {
	q, err := s.DisruptorStorage.GetQueue(name)
	if ferr := s.fault(name); ferr != nil {
		s.observeRecord(name, nil, ferr)
		return nil, ferr
	}
	s.observeRecord(name, q, err)
	s.pin(name)
	return q, err
}

func (s *c1PinnedStorage) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	bs, err := s.DisruptorStorage.GetExchangeBindings(exchangeName)
	s.pin(exchangeName)
	return bs, err
}

// bet0PinnedServer is bet0Server with a storage layer the test can pin. It
// builds the same concrete storage the builder would (embedding it, so every
// optional capability the broker type-asserts for — async durable writes,
// shared bodies, atomic transactions — is promoted unchanged and the flagship
// durable+confirm publish path is the one under test).
func bet0PinnedServer(t *testing.T, dir string) (*server.Server, string, *c1PinnedStorage) {
	t.Helper()
	port := bet0NextPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "silent"

	raw, err := storage.NewDisruptorStorageWithEngineConfig(
		dir,
		cfg.GetEngine(),
	)
	require.NoError(t, err, "build storage")
	pinned := &c1PinnedStorage{DisruptorStorage: raw}

	srv, err := server.NewServerBuilder().WithConfig(cfg).WithStorage(pinned).Build()
	require.NoError(t, err, "server build")
	waitForListening(t, srv)
	// WithStorage ⇒ ownsStorage=false, so Shutdown will NOT close raw; the test
	// owns it and closes it after the broker has stopped its reapers.
	t.Cleanup(func() {
		testShutdownServer(t, srv)
		if err := raw.Close(); err != nil {
			t.Logf("injected storage close: %v", err)
		}
	})
	return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr), pinned
}

// TestBet0_DeleteRacingDeclareCannotOrphanConfirmedMessages asserts the
// end-to-end property, NOT the presence of any particular lock: whatever the
// broker CONFIRMS it has taken responsibility for must still be there after a
// restart, even when the confirm was accepted by a queue that a concurrent
// queue.delete was tearing down.
//
// Spec basis: AMQP 0-9-1 §1.8.3.9 / the publisher-confirms contract — a
// basic.ack for a persistent publish is the broker's promise that the message
// is safely stored and will survive a restart. A queue.delete racing a declare
// is an ordinary concurrent-client situation; it may legally destroy the queue
// (and everything in it), and it may legally make subsequent publishes
// unroutable. What it may NOT do is take the promise and then break it.
//
// The obligation set is therefore acked-MINUS-returned: publishes go out
// mandatory, so a message the broker declined to route comes back as a
// basic.return before its ack (spec ordering) and is correctly excluded — the
// broker never promised to store it. Everything remaining was promised, and
// every one of those must be consumable after the restart. If the broker
// refuses the whole batch the obligation set is empty and the property holds
// vacuously; that is a legitimate way to pass, and the counts are logged so a
// vacuous pass is never mistaken for a meaningful one.
//
// Why this cannot pass by accident: the assertion is on messages the broker
// positively acknowledged, read back through a real restart and a real
// consumer. No internal state is inspected, so the test stays valid under any
// fix — a lock, a tombstone, a refusal to resurrect, or a redesign.
func TestBet0_DeleteRacingDeclareCannotOrphanConfirmedMessages(t *testing.T) {
	const orphanPublishes = 200
	const qname = "bet0-c1-orphan"

	dir := t.TempDir()
	srv, uri, pinned := bet0PinnedServer(t, dir)

	// The queue exists for real: record persisted, ordinal allocated.
	setupConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	setupCh, err := setupConn.Channel()
	require.NoError(t, err)
	_, err = setupCh.QueueDeclare(qname, true, false, false, false, nil)
	require.NoError(t, err, "initial declare")
	require.NoError(t, setupConn.Close())

	// Park a redeclare inside the window, holding the (about to be stale) record.
	entered, release := pinned.arm(qname, "DeclareQueue")

	declConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer declConn.Close()
	declCh, err := declConn.Channel()
	require.NoError(t, err)

	declDone := make(chan error, 1)
	go func() {
		_, derr := declCh.QueueDeclare(qname, true, false, false, false, nil)
		declDone <- derr
	}()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("declare never reached the pinned window — the pin no longer matches the code path")
	}

	// Delete the queue to completion on another connection while the declarer
	// is parked. This connection is independent, so it is not blocked behind
	// the parked declare's channel.
	delConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	delCh, err := delConn.Channel()
	require.NoError(t, err)
	_, err = delCh.QueueDelete(qname, false, false, false)
	require.NoError(t, err, "queue.delete")
	require.NoError(t, delConn.Close())

	// Let the declarer out. It now finishes against a record that is gone.
	close(release)
	require.NoError(t, <-declDone, "the parked redeclare")

	// Publish persistent + mandatory with confirms and record what the broker
	// promised to keep.
	pubCh, err := declConn.Channel()
	require.NoError(t, err)
	require.NoError(t, pubCh.Confirm(false))
	confirms := pubCh.NotifyPublish(make(chan amqp.Confirmation, orphanPublishes+16))
	returned := pubCh.NotifyReturn(make(chan amqp.Return, orphanPublishes+16))

	for seq := uint64(0); seq < orphanPublishes; seq++ {
		require.NoError(t, pubCh.PublishWithContext(context.Background(), "", qname, true, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(seq, bet0BodySize)}))
	}

	acked := make(map[uint64]struct{}, orphanPublishes)
	for got := 0; got < orphanPublishes; got++ {
		select {
		case c := <-confirms:
			if c.Ack {
				acked[c.DeliveryTag-1] = struct{}{} // confirm tags are 1-based publish order
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("only %d/%d publishes were confirmed either way", got, orphanPublishes)
		}
	}
	// basic.return precedes the ack for the same message, so every return that
	// is coming has already arrived.
	unroutable := make(map[uint64]struct{}, orphanPublishes)
	for draining := true; draining; {
		select {
		case r := <-returned:
			unroutable[bet0Seq(r.Body)] = struct{}{}
		default:
			draining = false
		}
	}

	promised := make([]uint64, 0, len(acked))
	for seq := range acked {
		if _, no := unroutable[seq]; !no {
			promised = append(promised, seq)
		}
	}
	t.Logf("after the racing delete: %d/%d publishes acked, %d returned unroutable, "+
		"%d messages the broker PROMISED to store", len(acked), orphanPublishes, len(unroutable), len(promised))
	require.NoError(t, declConn.Close())

	// Restart on the same data directory, with plain storage (no pin).
	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	if len(promised) == 0 {
		t.Log("broker took responsibility for nothing — property holds vacuously " +
			"(the racing delete made the queue unroutable rather than orphaning it)")
		return
	}

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qname, true, false, false, false, nil)
	require.NoError(t, err, "redeclare after restart")
	require.NoError(t, ch2.Qos(bet0Prefetch, 0, false))
	deliveries, err := ch2.Consume(qname, "", true, false, false, false, nil)
	require.NoError(t, err)

	seen := make(map[uint64]struct{}, len(promised))
	deadline := time.After(60 * time.Second)
	for len(seen) < len(promised) {
		select {
		case d, ok := <-deliveries:
			require.Truef(t, ok, "delivery channel closed after %d/%d recovered", len(seen), len(promised))
			seen[bet0Seq(d.Body)] = struct{}{}
		case <-deadline:
			t.Fatalf("CONFIRMED-BUT-NOT-RECOVERABLE: the broker acked %d persistent publishes "+
				"to queue %q after a queue.delete raced a concurrent declare, then recovered only "+
				"%d of them across a restart. A queue orphaned by that race — live and publishable "+
				"but with no metadata record — confirms durable publishes that recovery classifies "+
				"as belonging to a deleted queue and discards. confirm => recoverable is violated.",
				len(promised), qname, len(seen))
		}
	}
	for _, seq := range promised {
		_, ok := seen[seq]
		require.Truef(t, ok, "confirmed publish %d was not recovered after restart", seq)
	}
	t.Logf("recovered all %d promised messages after restart", len(promised))
}

// TestBet0_DeleteRacingPublishCannotOrphanConfirmedMessages is the second
// entry point to the same violation, and it is deliberately independent of the
// first: it never redeclares, so it cannot ride on the declare path being
// unfixed. A fix that only re-validates the record in declareExistingQueue
// leaves this one red.
//
// The route: a publish resolves its target queues from the exchange BINDINGS
// (routeMessage's direct-exchange path) and then hands those names straight to
// getOrCreateQueueState with no re-check that the queue still exists. Nothing
// upstream re-validates the metadata record. DeleteQueue removes the bindings
// well BEFORE the create-mutex span, so the interval between "routing resolved
// this binding" and "the publish creates the queue state" is strictly wider
// than the mutex span — the create-mutex fix does not narrow this window at
// all. The publisher resumes after the queue is fully gone, getOrCreateQueueState
// takes its cold path, resolveQueueOrdinal finds no record and allocates a
// fresh ordinal it never persists, and the durable publish is stored and
// CONFIRMED against a queue with no metadata record.
//
// Spec basis is identical to the declare-race case: a basic.ack for a
// persistent publish is a promise the message survives a restart. Publishing
// through an exchange whose binding is concurrently torn down may legally lose
// the route (the message comes back as a basic.return, mandatory=true, and is
// excluded from the obligation set). It may not silently promise and discard.
func TestBet0_DeleteRacingPublishCannotOrphanConfirmedMessages(t *testing.T) {
	const qname = "bet0-c1-pub-orphan"
	const exname = "bet0-c1-ex"
	const rkey = "k"

	dir := t.TempDir()
	srv, uri, pinned := bet0PinnedServer(t, dir)

	setupConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	setupCh, err := setupConn.Channel()
	require.NoError(t, err)
	require.NoError(t, setupCh.ExchangeDeclare(exname, "direct", true, false, false, false, nil))
	_, err = setupCh.QueueDeclare(qname, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, setupCh.QueueBind(qname, rkey, exname, false, nil))
	require.NoError(t, setupConn.Close())

	// Park a publish inside routing, holding the binding set it just resolved.
	entered, release := pinned.arm(exname, "routeMessage")

	pubConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer pubConn.Close()
	pubCh, err := pubConn.Channel()
	require.NoError(t, err)
	require.NoError(t, pubCh.Confirm(false))
	confirms := pubCh.NotifyPublish(make(chan amqp.Confirmation, 8))
	returned := pubCh.NotifyReturn(make(chan amqp.Return, 8))

	pubDone := make(chan error, 1)
	go func() {
		pubDone <- pubCh.PublishWithContext(context.Background(), exname, rkey, true, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(0, bet0BodySize)})
	}()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("publish never reached the pinned routing window — the pin no longer matches the code path")
	}

	// Tear the queue down completely while the publisher is parked.
	delConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	delCh, err := delConn.Channel()
	require.NoError(t, err)
	_, err = delCh.QueueDelete(qname, false, false, false)
	require.NoError(t, err, "queue.delete")
	require.NoError(t, delConn.Close())

	close(release)
	require.NoError(t, <-pubDone, "the parked publish")

	// One publish, so exactly one confirm is outstanding.
	acked := false
	select {
	case c := <-confirms:
		acked = c.Ack
	case <-time.After(60 * time.Second):
		t.Fatal("the publish was never confirmed either way")
	}
	wasReturned := false
	select {
	case <-returned:
		wasReturned = true // returned unroutable: the broker promised nothing
	default:
	}
	promised := acked && !wasReturned
	t.Logf("after the racing delete: acked=%v returned=%v — broker promised to store it=%v",
		acked, wasReturned, promised)

	require.NoError(t, pubConn.Close())
	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	if !promised {
		// Once the record-existence guard is in place this is the EXPECTED
		// outcome on every run: the pin makes the publish land after the
		// delete, so it is always refused and the obligation set is always
		// empty. A pass here is therefore vacuous with respect to recovery,
		// and the guard against that becoming a meaningless green tick is
		// this assertion plus the mutation proof recorded in the gate file.
		//
		// Assert the refusal was ACTIVE rather than merely absent: the broker
		// must have said no, by nacking the confirm or by returning the
		// message unroutable. Without this, a test that silently failed to set
		// the race up at all — pin never fired, publish never sent — would
		// take the same early return and report success.
		require.Truef(t, !acked || wasReturned,
			"publish was neither nacked nor returned, so the broker did not actively refuse it; "+
				"the race was not exercised and this pass would be meaningless (acked=%v returned=%v)",
			acked, wasReturned)
		t.Log("broker actively refused the publish (nack or unroutable return) — it took " +
			"responsibility for nothing, so confirm => recoverable holds with an empty obligation set")
		return
	}

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qname, true, false, false, false, nil)
	require.NoError(t, err, "redeclare after restart")
	deliveries, err := ch2.Consume(qname, "", true, false, false, false, nil)
	require.NoError(t, err)

	select {
	case d, ok := <-deliveries:
		require.True(t, ok, "delivery channel closed before the confirmed message arrived")
		require.Equal(t, uint64(0), bet0Seq(d.Body), "recovered the wrong message")
	case <-time.After(30 * time.Second):
		t.Fatal("CONFIRMED-BUT-NOT-RECOVERABLE: the broker acked a persistent publish that was " +
			"routed to a queue a concurrent queue.delete was tearing down, then recovered none of " +
			"it across a restart. Routing resolved the binding before the delete removed it, and " +
			"the publish then re-created the queue's runtime state with a fresh ordinal it never " +
			"persisted — so recovery classifies the record as belonging to a deleted queue and " +
			"discards it. confirm => recoverable is violated on the publish path, independently " +
			"of the declare path.")
	}
	t.Log("the confirmed message survived the restart")
}

// TestBet0_DeleteRacingTxCommitCannotOrphanCommittedMessages closes the third
// and worst entry point: the TRANSACTION path.
//
// Why this one is separate from the publish-race test even though the mechanism
// rhymes. The two async-confirm publish paths guard the teardown window with a
// real select on StopCh(). PublishMessage, fanoutSharedSync and
// PublishMessageTx do not — they delegate entirely to
// WaitForCapacity(queueState.StopCh()), which for most of this loop returned
// true on its below-high-water-mark fast path WITHOUT consulting the queue's
// closed flag or the stop channel at all. A torn-down queue has depth 0, so it
// is never at the high-water mark, so that gate waved every publish straight
// through into a closed, record-less queue. The other tests could not see this:
// they drive PublishMessageAsyncConfirm, one of the two paths that genuinely
// implemented the property.
//
// PublishMessageTx is the one that matters most in production. It is used for
// EVERY transactional publish regardless of storage backend
// (transaction/broker_executor.go -> server/broker_adapters.go), so unlike the
// other two it is reachable on the default backend. And the promise it breaks
// is stronger than a publisher confirm: tx.commit-ok tells the client the whole
// transaction is durably committed (AMQP 0-9-1 §1.9.2.3). Returning commit-ok
// for a message that recovery will discard is the worst version of this bug.
//
// The window is pinned at COMMIT time, not publish time: the tx manager buffers
// publishes and executes them inside ExecuteAtomic, so PublishMessageTx — and
// therefore its routing — runs when tx.commit is called. Pinning the publish
// would pin nothing.
//
// Obligation, same shape as the sibling tests: if commit succeeds the broker
// promised to store the message and it MUST survive the restart. If commit
// fails, the broker refused and owes nothing — but the refusal must be ACTIVE
// (a non-nil commit error), so a run that never exercised the race cannot slip
// through the empty-obligation branch reporting success.
func TestBet0_DeleteRacingTxCommitCannotOrphanCommittedMessages(t *testing.T) {
	const qname = "bet0-c1-tx-orphan"
	const exname = "bet0-c1-tx-ex"
	const rkey = "k"

	dir := t.TempDir()
	srv, uri, pinned := bet0PinnedServer(t, dir)

	setupConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	setupCh, err := setupConn.Channel()
	require.NoError(t, err)
	require.NoError(t, setupCh.ExchangeDeclare(exname, "direct", true, false, false, false, nil))
	_, err = setupCh.QueueDeclare(qname, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, setupCh.QueueBind(qname, rkey, exname, false, nil))
	require.NoError(t, setupConn.Close())

	txConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer txConn.Close()
	txCh, err := txConn.Channel()
	require.NoError(t, err)
	require.NoError(t, txCh.Tx())
	require.NoError(t, txCh.PublishWithContext(context.Background(), exname, rkey, true, false,
		amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(0, bet0BodySize)}),
		"staging the transactional publish")

	// Park the COMMIT inside routing, holding the binding set it just resolved.
	entered, release := pinned.arm(exname, "PublishMessageTx")

	commitDone := make(chan error, 1)
	go func() { commitDone <- txCh.TxCommit() }()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("tx.commit never reached the pinned routing window — the pin no longer matches " +
			"the code path (does PublishMessageTx still resolve routing at commit time?)")
	}

	// Tear the queue down completely while the commit is parked mid-routing.
	delConn, err := amqp.Dial(uri)
	require.NoError(t, err)
	delCh, err := delConn.Channel()
	require.NoError(t, err)
	_, err = delCh.QueueDelete(qname, false, false, false)
	require.NoError(t, err, "queue.delete")
	require.NoError(t, delConn.Close())

	close(release)
	commitErr := <-commitDone
	promised := commitErr == nil
	t.Logf("after the racing delete: tx.commit err=%v — broker promised to store it=%v",
		commitErr, promised)

	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	if !promised {
		t.Log("broker actively refused the commit — it took responsibility for nothing, " +
			"so commit => recoverable holds with an empty obligation set")
		return
	}

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qname, true, false, false, false, nil)
	require.NoError(t, err, "redeclare after restart")
	deliveries, err := ch2.Consume(qname, "", true, false, false, false, nil)
	require.NoError(t, err)

	select {
	case d, ok := <-deliveries:
		require.True(t, ok, "delivery channel closed before the committed message arrived")
		require.Equal(t, uint64(0), bet0Seq(d.Body), "recovered the wrong message")
	case <-time.After(30 * time.Second):
		t.Fatal("COMMITTED-BUT-NOT-RECOVERABLE: the broker returned tx.commit-ok for a persistent " +
			"publish routed to a queue a concurrent queue.delete was tearing down, then recovered " +
			"none of it across a restart. Routing resolved the binding before the delete landed, " +
			"and the commit then wrote into a closed, record-less queue whose records recovery " +
			"discards as a deleted queue's. A committed transaction was lost — a stronger promise " +
			"broken than a publisher confirm.")
	}
	t.Log("the committed message survived the restart")
}

// ----------------------------------------------------------------------------
// 17. Recovery Case B (dead-incarnation discard) and the ErrOrdinalMismatch
//     refusal — the two ordinal-aware recovery branches that shipped untested.
// ----------------------------------------------------------------------------

// bet0LogCapturingServer is bet0TryServer with the broker's own logging turned
// on and pointed at logFile.
//
// This adds NO production seam. ServerBuilder.WithLogger cannot be used with a
// test-written logger at all — builder.go type-asserts the logger to a concrete
// *ZapLoggerAdapter in two places and would panic on anything else — but
// Server.LogFile is an ordinary supported configuration option that routes the
// very same zap logger to a file (createZapLogger sets OutputPaths from it). So
// the test observes exactly the operator-visible output a real deployment
// would, through the path a real deployment uses.
//
// The error is returned rather than fatal so recovery REFUSALS can be asserted
// on directly, the same way bet0TryServer allows.
func bet0LogCapturingServer(t *testing.T, dir, logFile string) (*server.Server, string, error) {
	t.Helper()
	port := bet0NextPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "warn"
	cfg.Server.LogFile = logFile

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		return nil, "", err
	}
	// See bet0TryServer for why this does not route through waitForListening:
	// it deliberately returns a startup problem to the caller instead of
	// failing the test, so it must still surface Start()'s real error instead
	// of discarding it into a generic "not ready".
	startErrCh := make(chan error, 1)
	go func() { startErrCh <- srv.Start() }()
	for i := 0; i < brokerStartupPolls; i++ {
		if srv.IsListening() {
			t.Cleanup(func() { testShutdownServer(t, srv) })
			return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr), nil
		}
		select {
		case startErr := <-startErrCh:
			return nil, "", startErr
		default:
		}
		time.Sleep(brokerStartupPollInterval)
	}
	return nil, "", fmt.Errorf("server listener not ready")
}

// bet0LogRecords parses the zap JSON lines written to logFile. A missing file
// means nothing was logged at all, which is a legitimate observation (no
// records) rather than an error.
func bet0LogRecords(t *testing.T, logFile string) []map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(logFile)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err, "read broker log %s", logFile)

	var out []map[string]interface{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec map[string]interface{}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue // non-JSON noise on the sink is not a log record
		}
		out = append(out, rec)
	}
	return out
}

// bet0RetainArtifacts copies paths out of the test's temp tree when the test
// does not PASS, so the broker-side evidence for a rare failure survives the
// process that produced it.
//
// It exists because an external harness CANNOT retain a t.TempDir(). Verified
// against the toolchain installed here (go1.26.0,
// GOROOT=/opt/homebrew/Cellar/go/1.26.0/libexec): testing.go's makeTempDir
// registers
//
//	c.Cleanup(func() { if err := removeAll(c.tempDir); err != nil { … } })
//
// with NO Failed() guard, so the directory is removed on the failing branch
// exactly as on the passing one, INSIDE the test process — before any `mv` a
// runner could perform. A soak that moved 26 failed TMPDIRs aside kept 26 empty
// directories and lost the only diagnostic for the only reproductions it had.
//
// The seam is cleanup ORDER. testing.go's runCleanup pops c.cleanups from the
// tail, so cleanups run LIFO: a Cleanup registered AFTER the first t.TempDir()
// call runs BEFORE that directory is removed. Registering this later than the
// t.TempDir() whose contents it saves is therefore load-bearing, not incidental.
//
// Destination is $STRANGEQ_ARTIFACT_DIR when set — a soak points it at a
// directory it owns, OUTSIDE the per-worker TMPDIR it recycles — else a fresh
// os.MkdirTemp. Either way the destination is logged: an artifact nobody can
// locate is the same as no artifact. Nothing is retained on a pass, so the
// committed default litters only when there is something to look at.
func bet0RetainArtifacts(t *testing.T, paths ...string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() && !t.Skipped() {
			return
		}
		dest := os.Getenv("STRANGEQ_ARTIFACT_DIR")
		if dest == "" {
			d, err := os.MkdirTemp("", "strangeq-artifacts-")
			if err != nil {
				t.Logf("ARTIFACT RETENTION FAILED: MkdirTemp: %v", err)
				return
			}
			dest = d
		}
		dest = filepath.Join(dest, strings.ReplaceAll(t.Name(), "/", "_"))
		if err := os.MkdirAll(dest, 0o755); err != nil {
			t.Logf("ARTIFACT RETENTION FAILED: MkdirAll %s: %v", dest, err)
			return
		}
		for _, p := range paths {
			body, err := os.ReadFile(p)
			if err != nil {
				t.Logf("ARTIFACT RETENTION FAILED: read %s: %v", p, err)
				continue
			}
			out := filepath.Join(dest, filepath.Base(p))
			if err := os.WriteFile(out, body, 0o644); err != nil {
				t.Logf("ARTIFACT RETENTION FAILED: write %s: %v", out, err)
				continue
			}
			t.Logf("RETAINED ARTIFACT: %s -> %s (%d bytes)", p, out, len(body))
		}
	})
}

// bet0LogRecordsWithMsg returns the parsed records whose "msg" is exactly msg.
func bet0LogRecordsWithMsg(t *testing.T, logFile, msg string) []map[string]interface{} {
	t.Helper()
	var hits []map[string]interface{}
	for _, rec := range bet0LogRecords(t, logFile) {
		if m, _ := rec["msg"].(string); m == msg {
			hits = append(hits, rec)
		}
	}
	return hits
}

// TestBet0_OrdinalAwareRecoveryCaseBAndMismatch covers the two branches of
// ordinal-aware recovery that no test has ever exercised. Cases C (records with
// no metadata => deleted queue => discard) and D (metadata with Ordinal==0 =>
// genuine pre-packing directory => refuse) are covered end-to-end by
// TestBet0_LegacyDirMessageOnlyForUnversionedRecords. These two are not:
//
//   - Case B: metadata carries ordinal N and records exist for SEVERAL
//     ordinals — the current incarnation's (N) plus dead earlier incarnations'
//     (< N), because queue.delete purges the in-memory ring but never the
//     shared WAL. Recovery must recover N and discard the rest. Getting this
//     wrong in either direction is severe: discard too much and a live queue
//     silently loses confirmed messages; discard too little and the queue's
//     ordinal-based dispatch cursor is seeded into a dead incarnation's band,
//     where it must crawl the ~8.8e13-tag gap back to its own records and
//     delivers nothing at all.
//
//   - ErrOrdinalMismatch: a record carrying an ordinal GREATER than the one the
//     queue is assigned. That is never a dead incarnation — ordinals only ever
//     move forward — so it means the persisted ordinal REGRESSED, and recovery
//     is about to hand this queue's consumers tags that belong to another
//     queue's space. Since the shared WAL's offsetIndex/ackBitmap and the
//     broker's deliveryIndex are all keyed by a bare uint64 with no queue
//     discriminator, proceeding would corrupt both queues. It must be fatal.
//
// Both subtests assert on behaviour a client or an operator can see — messages
// consumed after a real restart, and the broker's own startup refusal — not on
// internal recovery state.
func TestBet0_OrdinalAwareRecoveryCaseBAndMismatch(t *testing.T) {
	const legacyMarker = "predates per-queue delivery-tag sequencing"

	// --- Case B: dead incarnation discarded, current incarnation recovered. ---
	t.Run("dead_incarnation_discarded_current_incarnation_fully_recovered", func(t *testing.T) {
		const (
			qName     = "bet0-caseb-q"
			deadBatch = 50
			liveBatch = 30
			deadEra   = uint64(0)
			liveEra   = uint64(1_000_000)
		)
		dir := t.TempDir()
		logPath := filepath.Join(t.TempDir(), "broker-caseb.log")

		srv, uri := bet0Server(t, dir)

		publish := func(t *testing.T, ch *amqp.Channel, base uint64, n int) {
			t.Helper()
			require.NoError(t, ch.Confirm(false))
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, n+16))
			for i := 0; i < n; i++ {
				require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(base+uint64(i), bet0BodySize)}))
			}
			for got := 0; got < n; got++ {
				select {
				case c := <-confirms:
					require.True(t, c.Ack, "publish nacked")
				case <-time.After(30 * time.Second):
					t.Fatalf("only %d/%d confirmed", got, n)
				}
			}
		}

		func() {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			defer conn.Close()

			// Incarnation 1: publish and DELETE WITHOUT CONSUMING. The records
			// stay in the shared WAL (there is no purge-by-queue), so they are
			// still physically recoverable at the next restart — which is the
			// whole point: these are the records Case B must discard.
			ch1, err := conn.Channel()
			require.NoError(t, err)
			_, err = ch1.QueueDeclare(qName, true, false, false, false, nil)
			require.NoError(t, err)
			publish(t, ch1, deadEra, deadBatch)
			_, err = ch1.QueueDelete(qName, false, false, false)
			require.NoError(t, err, "delete the first incarnation")
			require.NoError(t, ch1.Close())

			// Incarnation 2: same name, new identity, new ordinal band. Left
			// with an unconsumed backlog so recovery has something to recover.
			ch2, err := conn.Channel()
			require.NoError(t, err)
			defer ch2.Close()
			_, err = ch2.QueueDeclare(qName, true, false, false, false, nil)
			require.NoError(t, err)
			publish(t, ch2, liveEra, liveBatch)
		}()
		testShutdownServer(t, srv)

		// Precondition: this really is a LATER incarnation. Ordinals are handed
		// out monotonically and never reused, so an ordinal of at least 2 is
		// what distinguishes "case B, records from several incarnations" from
		// "case A, one incarnation" — without it the subtest could pass while
		// exercising nothing.
		qPath := filepath.Join(dir, storage.MetadataDir, storage.QueuesDir, qName+storage.FileExtension)
		raw, err := os.ReadFile(qPath)
		require.NoError(t, err, "queue metadata must exist at %s", qPath)
		var qRec protocol.Queue
		require.NoError(t, cbor.Unmarshal(raw, &qRec))
		require.GreaterOrEqualf(t, qRec.Ordinal, uint64(2),
			"precondition: the re-declared queue must hold a LATER ordinal than the "+
				"incarnation that was deleted, otherwise there is no dead incarnation to "+
				"discard and this subtest proves nothing; got ordinal %d", qRec.Ordinal)

		_, uri2, err := bet0LogCapturingServer(t, dir, logPath)
		require.NoErrorf(t, err,
			"a queue whose name has records from an earlier, deleted incarnation must "+
				"still boot: those records identify themselves as dead by their ordinal "+
				"and are simply discarded: %v", err)

		// The property itself: the surviving incarnation gets ALL of its own
		// messages back, and NONE of the dead one's.
		conn, err := amqp.Dial(uri2)
		require.NoError(t, err, "broker must accept connections after the restart")
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Qos(bet0Prefetch, 0, false))
		deliveries, err := ch.Consume(qName, "", true, false, false, false, nil)
		require.NoError(t, err)

		seen := make(map[uint64]int, liveBatch)
		deadline := time.After(30 * time.Second)
		for len(seen) < liveBatch {
			select {
			case d, ok := <-deliveries:
				require.Truef(t, ok, "delivery channel closed after %d/%d", len(seen), liveBatch)
				seq := bet0Seq(d.Body)
				require.GreaterOrEqualf(t, seq, liveEra,
					"DEAD INCARNATION RESURRECTED: recovered sequence %d belongs to the "+
						"incarnation of %q that was DELETED before this one existed. Recovery "+
						"identified it as dead by its ordinal and must have discarded it; "+
						"delivering it violates AMQP 0-9-1 §1.7.2.10 (queue.delete removes the "+
						"queue and all its messages) and means this queue's dispatch cursor is "+
						"now seeded in a retired ordinal band.", seq, qName)
				seen[seq]++
				require.Equalf(t, 1, seen[seq], "sequence %d delivered twice", seq)
			case <-deadline:
				t.Fatalf("only %d/%d of the live incarnation's confirmed messages were recovered. "+
					"Every one of them was CONFIRMED before the restart, so all %d must come back; "+
					"a shortfall means case B discarded records it should have kept, or seeded the "+
					"queue's cursor outside its own ordinal band.", len(seen), liveBatch, liveBatch)
			}
		}

		// Nothing beyond this incarnation's own backlog may follow it.
		select {
		case d, ok := <-deliveries:
			require.Falsef(t, ok && d.Body != nil,
				"an extra message (sequence %d) arrived after all %d of the live incarnation's "+
					"messages had been delivered — the dead incarnation's records were recovered too",
				bet0Seq(d.Body), liveBatch)
		case <-time.After(2 * time.Second):
		}
		// The discard is a specified operator-visible event, not an internal
		// detail: the same standing rule that requires Case C discards to be
		// loudly logged (queue, count, ordinal span) applies here for the same
		// reason — a branch that silently destroys durable records must be
		// forensically visible, because the records themselves survive in the
		// WAL until checkpoint and a mis-discard has to be diagnosable.
		//
		// It is also this subtest's non-vacuity backstop. It is asserted AFTER
		// the property for the same reason as in the route-(c) test: a mutation
		// proves only the FIRST assertion it trips, and a regression that stops
		// dead-incarnation records being discarded ALSO stops this log line
		// being written — so checking it first would red here and never reach
		// the property at all, leaving the property unproven. Non-vacuity does
		// not depend on this assertion being early: it is already established
		// structurally by the 50 confirmed publishes made under incarnation 1
		// and by the ordinal >= 2 precondition above.
		//
		// It is also this subtest's non-vacuity guard. Without it the test
		// would still pass if the dead records had never been present at all,
		// and would then be gating nothing.
		hits := bet0LogRecordsWithMsg(t, logPath, "discarding records from a dead incarnation of a queue")
		require.Lenf(t, hits, 1,
			"recovery must report exactly one dead-incarnation discard for %q. Zero means "+
				"either the dead incarnation's records were never discarded (they were loaded "+
				"into the live queue, corrupting its ordinal band) or they were never there, in "+
				"which case this test is not exercising case B at all. Log: %s", qName, logPath)
		require.Equal(t, qName, hits[0]["queue"], "the discard must name the queue it destroyed records for")
		require.Equalf(t, float64(deadBatch), hits[0]["discarded"],
			"every one of the deleted incarnation's %d records must be discarded, and only "+
				"those: %v", deadBatch, hits[0])

		t.Logf("case B: %d dead-incarnation records discarded, all %d confirmed messages of the live incarnation recovered",
			deadBatch, liveBatch)
	})

	// --- ErrOrdinalMismatch: a regressed persisted ordinal must be fatal. ---
	t.Run("record_ordinal_ahead_of_metadata_refuses_to_boot", func(t *testing.T) {
		const (
			qName = "bet0-mismatch-q"
			decoy = "bet0-mismatch-decoy"
			batch = 50
		)
		dir := t.TempDir()
		srv, uri := bet0Server(t, dir)

		func() {
			conn, err := amqp.Dial(uri)
			require.NoError(t, err)
			defer conn.Close()
			ch, err := conn.Channel()
			require.NoError(t, err)

			// The decoy consumes the first ordinal so the queue under test is
			// assigned at least 2 — leaving room to regress its ordinal to a
			// non-zero value. Regressing it to 0 would be case D (a genuine
			// pre-packing directory), an entirely different branch.
			_, err = ch.QueueDeclare(decoy, true, false, false, false, nil)
			require.NoError(t, err)

			_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, ch.Confirm(false))
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, batch+16))
			for i := uint64(0); i < batch; i++ {
				require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
					amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(i, bet0BodySize)}))
			}
			for got := 0; got < batch; got++ {
				select {
				case c := <-confirms:
					require.True(t, c.Ack)
				case <-time.After(30 * time.Second):
					t.Fatalf("only %d/%d confirmed", got, batch)
				}
			}
		}()
		testShutdownServer(t, srv)

		// Forge the regression: the records on disk were minted under ordinal
		// N, but the metadata record now claims N-1. Nothing legitimate can
		// produce this — ordinals are write-once per incarnation and only move
		// forward — so it stands in for the metadata corruption or rollback
		// that the check exists to catch.
		qPath := filepath.Join(dir, storage.MetadataDir, storage.QueuesDir, qName+storage.FileExtension)
		raw, err := os.ReadFile(qPath)
		require.NoError(t, err, "queue metadata must exist at %s", qPath)
		var qRec protocol.Queue
		require.NoError(t, cbor.Unmarshal(raw, &qRec))
		require.GreaterOrEqualf(t, qRec.Ordinal, uint64(2),
			"precondition: the queue under test must hold an ordinal of at least 2 so its "+
				"ordinal can be regressed to a NON-ZERO value; ordinal 0 is case D, a "+
				"different branch with a different verdict. Got %d", qRec.Ordinal)
		qRec.Ordinal--
		forged, err := cbor.Marshal(&qRec)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(qPath, forged, 0o644))

		_, _, err = bet0TryServer(t, dir)
		require.Errorf(t, err,
			"a queue whose recovered records carry an ordinal AHEAD of its assigned one must "+
				"stop the broker starting. Those tags belong to a tag space this queue does not "+
				"own, and the shared WAL's offset index and ack bitmap plus the broker's delivery "+
				"index are all keyed by a bare uint64 with no queue discriminator — recovering "+
				"them would let two queues act on each other's tags")
		require.Containsf(t, err.Error(), "recovered delivery tag ordinal does not match",
			"the refusal must be the ordinal-mismatch one; got: %v", err)
		require.Containsf(t, err.Error(), qName,
			"the refusal must name the queue so an operator knows which one to act on; got: %v", err)
		require.NotContainsf(t, err.Error(), legacyMarker,
			"an ordinal MISMATCH must never be reported as a legacy data directory: that message "+
				"tells an operator to drain and recreate the queue or the data directory, i.e. to "+
				"destroy data, and it is reserved for a directory that genuinely predates per-queue "+
				"sequencing (a persisted ordinal of 0). Got: %v", err)
		t.Logf("regressed ordinal refused to boot with the mismatch error: %v", err)
	})
}

// ----------------------------------------------------------------------------
// 18. C1 route (c) — the dead-letter republish reaches queue-state creation
//     WITHOUT going through routing, so it needs its own gating test.
// ----------------------------------------------------------------------------

// bet0PinnedLoggingServer is bet0PinnedServer with the broker's warn-level
// logging routed to logFile, so a test can pin a window AND observe the
// operator-visible event the broker emits inside it. See
// bet0LogCapturingServer for why LogFile is the only usable seam.
func bet0PinnedLoggingServer(t *testing.T, dir, logFile string) (*server.Server, string, *c1PinnedStorage) {
	t.Helper()
	port := bet0NextPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "warn"
	cfg.Server.LogFile = logFile

	raw, err := storage.NewDisruptorStorageWithEngineConfig(
		dir,
		cfg.GetEngine(),
	)
	require.NoError(t, err, "build storage")
	pinned := &c1PinnedStorage{DisruptorStorage: raw}

	srv, err := server.NewServerBuilder().WithConfig(cfg).WithStorage(pinned).Build()
	require.NoError(t, err, "server build")
	waitForListening(t, srv)
	// WithStorage ⇒ ownsStorage=false, so Shutdown will NOT close raw; the test
	// owns it and closes it after the broker has stopped its reapers.
	t.Cleanup(func() {
		testShutdownServer(t, srv)
		if err := raw.Close(); err != nil {
			t.Logf("injected storage close: %v", err)
		}
	})
	return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr), pinned
}

// TestBet0_DeleteRacingDeadLetterCannotOrphanTargetQueue gates C1 route (c).
//
// Routes (a) and (b) reach queue-state creation through DeclareQueue and
// through routeMessage respectively, and each has its own test. Route (c) is
// different in a way that matters: republishToTargets calls
// getOrCreateQueueState(target) DIRECTLY, on a target list resolved earlier.
// It never consults activeQueues, so unlike route (b) it does not need the
// routing cache to still hold the queue — a target deleted in the meantime is
// reached anyway. Today all three routes are covered by the same
// record-existence guard in createQueueStateLocked, which is exactly why route
// (c) needs its own test: a future change that kept the guard working for the
// routing paths while breaking it here would leave routes (a) and (b) green.
//
// THE WINDOW, and why the pin is load-bearing rather than decorative.
// queue.delete removes the queue's BINDINGS as well as the queue. So if this
// test simply deleted the target and then dead-lettered, routing would resolve
// ZERO targets, republishToTargets would never run, and the test would pass
// while exercising nothing at all. The defect needs the republish to hold a
// target list resolved BEFORE the delete and act on it AFTER — so the pin
// blocks inside storage.GetExchangeBindings, after the real read has returned,
// for the one call whose stack is inside deadLetter. The delete then completes
// while the republisher is parked holding precisely the stale list the real
// interleaving gives it. Pinned by identity, no sleeps, no timing assumptions.
//
// WHAT IS ASSERTED, at two different strengths, deliberately — and IN THIS
// ORDER, for the reason given at the second one:
//
//  2. The republish is DROPPED, observably. This is route (c)'s own bail (the
//     StopCh select in republishToTargets) and the broker's warn is the only
//     observable that distinguishes it: the optional metrics counter
//     (RecordDeadLetterDropped) is SHARED with the store-failure and
//     x-overflow=reject-publish drops, so a counter assertion could not tell
//     this cause from the other two and would go green on a fixture where this
//     path never ran. Both other causes are also structurally impossible here
//     — the target carries no x-max-length/x-overflow policy at all, and no
//     store failure is injected — but the test asserts on the discriminating
//     message anyway rather than resting on that argument.
//
//  1. The end-to-end C1 property: nothing the broker CONFIRMS may be lost. A
//     re-declared queue must be a clean, correctly-identified queue. This is
//     what catches the severe form of the defect — without the record-existence
//     guard the republish installs a LIVE, record-less QueueState for the
//     deleted name, holding an ordinal that was allocated but never persisted;
//     a later declare of that name then finds and reuses that state, so the
//     queue's metadata says one ordinal while its dispatch plane mints tags in
//     another, and every durable message confirmed against it is discarded at
//     the next restart as a dead incarnation's.
func TestBet0_DeleteRacingDeadLetterCannotOrphanTargetQueue(t *testing.T) {
	const (
		srcQueue    = "bet0-c1-dlsrc"
		dlxTarget   = "bet0-c1-dltarget"
		dlxName     = "bet0-c1-dlx"
		afterBatch  = 200
		targetGone  = "dead-letter dropped: target queue was deleted"
		publishBase = uint64(700_000)
	)
	dir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "broker-dlx.log")
	// AFTER both t.TempDir() calls, so LIFO cleanup order copies the log out
	// before the temp tree is removed. The assertions below cite this log; on a
	// failure or a skip it must outlive the process, or the citation is dead.
	bet0RetainArtifacts(t, logPath)
	srv, uri, pinned := bet0PinnedLoggingServer(t, dir, logPath)

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)

	// A fanout DLX so the dead-letter's routing key plays no part in whether a
	// target is resolved — the target list is what this test is about.
	require.NoError(t, ch.ExchangeDeclare(dlxName, "fanout", true, false, false, false, nil))
	_, err = ch.QueueDeclare(dlxTarget, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(dlxTarget, "", dlxName, false, nil))
	_, err = ch.QueueDeclare(srcQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	require.NoError(t, err)

	// One persistent message into the source, confirmed, then taken un-acked by
	// a consumer so the test can dead-letter it on demand with basic.nack
	// (requeue=false) — a deterministic client action, not a timer.
	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))
	require.NoError(t, ch.PublishWithContext(context.Background(), "", srcQueue, false, false,
		amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(1, bet0BodySize)}))
	select {
	case c := <-confirms:
		require.True(t, c.Ack, "the source publish must be confirmed")
	case <-time.After(30 * time.Second):
		t.Fatal("source publish never confirmed")
	}

	conCh, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, conCh.Qos(1, 0, false))
	deliveries, err := conCh.Consume(srcQueue, "", false, false, false, false, nil)
	require.NoError(t, err)
	var toNack amqp.Delivery
	select {
	case d, ok := <-deliveries:
		require.True(t, ok, "source delivery channel closed")
		toNack = d
	case <-time.After(30 * time.Second):
		t.Fatal("the source message was never delivered, so it can never be dead-lettered")
	}

	// Park the republisher inside routing, holding the pre-delete target list.
	//
	// The probe is armed alongside the pin and records how the republish later
	// resolves the target's metadata record. That is the observable which
	// separates the two readings of a missing drop record at the end of this
	// test; the reasoning is at the assertion, where it is used.
	pinned.armRecordProbe(dlxTarget, "deadLetter")
	entered, release := pinned.arm(dlxName, "deadLetter")
	nacked := make(chan error, 1)
	go func() { nacked <- toNack.Nack(false, false) }()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the dead-letter republish never reached routing; the window was never pinned, " +
			"so this test would prove nothing")
	}

	// The target is destroyed, completely, while the republisher is parked.
	delCh, err := conn.Channel()
	require.NoError(t, err)
	_, err = delCh.QueueDelete(dlxTarget, false, false, false)
	require.NoError(t, err, "delete the dead-letter target")
	require.NoError(t, delCh.Close())

	close(release) // the republish resumes against a target that no longer exists
	select {
	case nerr := <-nacked:
		require.NoError(t, nerr, "basic.nack must not fail the channel")
	case <-time.After(30 * time.Second):
		t.Fatal("basic.nack never returned")
	}

	// (1) The C1 property. The name is re-declared, so it is a live queue again
	// — a fresh incarnation, which must be clean: everything confirmed against
	// it survives a restart. Publishes go out mandatory so an unroutable
	// message returns before its ack and is excluded from the obligation set;
	// the broker only ever promised the rest.
	pubCh, err := conn.Channel()
	require.NoError(t, err)
	_, err = pubCh.QueueDeclare(dlxTarget, true, false, false, false, nil)
	require.NoError(t, err, "re-declare the target after its deletion")
	require.NoError(t, pubCh.Confirm(false))
	afterConfirms := pubCh.NotifyPublish(make(chan amqp.Confirmation, afterBatch+16))
	returns := pubCh.NotifyReturn(make(chan amqp.Return, afterBatch+16))
	for i := uint64(0); i < afterBatch; i++ {
		require.NoError(t, pubCh.PublishWithContext(context.Background(), "", dlxTarget, true, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(publishBase+i, bet0BodySize)}))
	}
	acked := 0
	for got := 0; got < afterBatch; got++ {
		select {
		case c := <-afterConfirms:
			if c.Ack {
				acked++
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d/%d publishes settled after the re-declare", got, afterBatch)
		}
	}
	// Returns are emitted before the ack that follows them, so anything unroutable
	// is already in the channel by now.
	returned := 0
	for drained := false; !drained; {
		select {
		case <-returns:
			returned++
		default:
			drained = true
		}
	}
	promised := acked - returned
	require.GreaterOrEqualf(t, promised, 0, "more returns (%d) than acks (%d)", returned, acked)
	t.Logf("after the re-declare: %d acked, %d returned unroutable, %d messages the broker PROMISED to store",
		acked, returned, promised)
	require.NotZerof(t, promised,
		"the re-declared target accepted NOTHING (%d acked, %d returned). A queue.delete may "+
			"legally destroy a queue, but a subsequent successful queue.declare must produce a "+
			"usable queue; if it does not, the dead-letter republish left the name in a state a "+
			"declare cannot recover from", acked, returned)

	require.NoError(t, conn.Close())
	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err, "the broker must restart after a dead-letter raced a queue.delete")
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(dlxTarget, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.Qos(bet0Prefetch, 0, false))
	recovered, err := ch2.Consume(dlxTarget, "", true, false, false, false, nil)
	require.NoError(t, err)

	seen := make(map[uint64]int, promised)
	deadline := time.After(60 * time.Second)
	for len(seen) < promised {
		select {
		case d, ok := <-recovered:
			require.Truef(t, ok, "delivery channel closed after %d/%d", len(seen), promised)
			seen[bet0Seq(d.Body)]++
		case <-deadline:
			t.Fatalf("CONFIRMED BUT NOT RECOVERABLE: only %d of %d messages the broker promised to "+
				"store came back after the restart. Every one of them was published to a queue the "+
				"broker had accepted a declare for, and acknowledged. A dead-letter republish that "+
				"raced this queue's deletion must not leave the name owning a record-less queue "+
				"state, because a later declare reuses that state and its unpersisted ordinal — and "+
				"recovery then reads the resulting records as a dead incarnation's and discards "+
				"them.", len(seen), promised)
		}
	}
	// (2) The drop is observable, and identified by ITS cause rather than by
	// the counter it shares with the store-failure and overflow drops.
	//
	// This is asserted LAST, and the order is load-bearing rather than
	// stylistic. Checked before the durability property it MASKS it: a
	// regression that resurrects the target as a live record-less queue also
	// stops the drop being recorded (the state is live, so the StopCh bail
	// never fires), so this assertion fires first and the property assertion
	// never runs — leaving the property untested by that regression. Measured,
	// not assumed: with the guard mutated to store a live state, this test
	// failed here and never reached the restart. Assert the weaker, faster
	// observable after the stronger one, so each is proven by a mutation of its
	// own.
	var goneHits []map[string]interface{}
	dropDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(dropDeadline) {
		if goneHits = bet0LogRecordsWithMsg(t, logPath, targetGone); len(goneHits) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// An empty goneHits has two readings, and only one of them is a defect. The
	// probe armed with the pin separates them, so the verdict is decided on
	// evidence rather than on which reading the reader finds plausible.
	//
	// The republish resolves its target through getOrCreateQueueState, and
	// queue.delete returned to its client before `release` was closed — by which
	// point deleteQueueIncarnation has run BOTH queueStates.LoadAndDelete(name)
	// and storage.DeleteQueue(name) (storage_broker.go, in that order). So a
	// republish that resumes into the deleted window misses the state map, falls
	// into createQueueStateLocked, and reads an ABSENT record. Every step from
	// there is unconditional: absent record => closed record-less QueueState =>
	// republishToTargets' StopCh bail => recordDeadLetterTargetGone. Hence:
	//
	//	absent > 0  — the republish DID reach the deleted target. The drop record
	//	              is then mandatory, and its absence is the second reading:
	//	              it wrote into that state anyway, leaving a durable record
	//	              for a queue with no metadata, which recovery discards. That
	//	              is a durability defect and it must FAIL.
	//
	//	absent == 0 — the republish never saw a deleted target. It resolved the
	//	              name only after the re-declare below had restored it (a
	//	              queueStates hit, or a GetQueue that found the new record),
	//	              so the dead-letter went into a live, correctly-identified
	//	              queue. The window this test exists to pin was not pinned and
	//	              the run proves nothing: INCONCLUSIVE, not FAIL.
	//
	// Measured at 26/2000 = 1.3%, roughly 9x more likely than the defect this
	// fixture is here to catch (bounded at 1/667 over the same soak). Reporting
	// that as a failure is worse than a silent false positive, because the
	// failure text says the run proves nothing — so a triager who reads it
	// correctly concludes there is no defect, and learns that this assertion
	// reddening is ignorable, on an assertion doing real work the other 98.7%
	// of the time.
	//
	// The probe cannot manufacture a skip: it reads the storage layer's answer,
	// upstream of both the record-existence guard and the StopCh bail, so
	// breaking either leaves absent > 0 and the run still fails here.
	absent, present := pinned.recordProbeCounts()
	if len(goneHits) == 0 && absent == 0 {
		t.Skipf("INCONCLUSIVE (premise not established, not a defect): the dead-letter republish "+
			"never resolved %q while it was deleted — the pinned storage saw %d absent and %d "+
			"present metadata reads from the republish path, so the republish reached the target "+
			"only after the re-declare had restored it and this run cannot say anything about the "+
			"drop. Log: %s", dlxTarget, absent, present, logPath)
	}
	// The converse, which keeps the probe honest: a drop record can only have
	// come from the closed state createQueueStateLocked returns on an absent
	// read, so observing one with absent == 0 means the probe did not see a call
	// it must have seen — a caller-filter or a truncated stack — and the skip
	// above would be handing out false greens.
	require.NotZerof(t, absent,
		"the drop was recorded (%d hits) but the record probe counted no absent read from the "+
			"republish path (absent=%d present=%d). The probe is not observing the call it gates "+
			"the skip on, so an empty goneHits would be skipped rather than failed. Log: %s",
		len(goneHits), absent, present, logPath)
	require.Lenf(t, goneHits, 1,
		"the dead-letter republish must be DROPPED and say why, once, when its target queue has "+
			"been deleted out from under it. The republish resolved %q with its metadata record "+
			"ABSENT %d time(s), so it reached the deleted target and was handed a closed, "+
			"record-less QueueState; the vacuous reading is excluded by that count and would have "+
			"skipped above. What remains is that it wrote into that state anyway — a durable "+
			"record for a queue with no metadata, which recovery discards. Log: %s",
		dlxTarget, absent, logPath)
	require.Equal(t, srcQueue, goneHits[0]["source_queue"],
		"the drop must name the source queue whose message was destroyed")
	require.Equal(t, dlxTarget, goneHits[0]["target_exchange"],
		"the drop must name the target it gave up on")

	t.Logf("route (c): republish dropped observably, and all %d messages confirmed by the "+
		"re-declared target survived the restart", promised)
}

// ----------------------------------------------------------------------------
// 19. DeclareQueue must not read a transient storage failure as "deleted".
// ----------------------------------------------------------------------------

// TestBet0_DeclareQueueTransientReadFailureDoesNotRebandLiveQueue pins a
// two-layer contract that only a test can hold against drift.
//
// DeclareQueue's re-read under the create mutex has three outcomes, and the
// third is the dangerous one: the record is there (redeclare it), the record is
// genuinely gone (a concurrent delete won — fall through and mint a NEW
// incarnation with a fresh, PERSISTED ordinal band), or the read FAILED. Only
// the second may fall through. Treating a failed read as "gone" would allocate
// and persist a new ordinal band for a queue that is still alive, re-banding
// its future delivery tags and stranding every record already written under the
// old band — recovery would then read those as a dead incarnation's and discard
// them, losing confirmed messages.
//
// The distinction rests on a contract spanning TWO layers: the storage layer
// must report genuine absence as a bare interfaces.ErrQueueNotFound and every
// other failure as something else, and DeclareQueue must gate its fallthrough
// on exactly that. Reading the code proves today's values; a future storage
// change that wrapped ErrQueueNotFound anywhere would silently invert the
// branch, and nothing but a test would notice. That is what this pins.
//
// The failure is injected at the seam the other C1 tests already use
// (ServerBuilder.WithStorage), targeted precisely enough to hit the re-read
// under the mutex while leaving the lock-free pre-check untouched — see
// armFault. No production code is involved in the injection.
//
// Assertion order is deliberate and follows the lesson from the route-(c)
// test: the end-to-end property is checked FIRST, so that a regression cannot
// be absorbed by the weaker API-level assertion before the property is ever
// evaluated.
func TestBet0_DeclareQueueTransientReadFailureDoesNotRebandLiveQueue(t *testing.T) {
	const (
		qName = "bet0-declare-ioerr-q"
		batch = 200
	)
	dir := t.TempDir()
	srv, uri, pinned := bet0PinnedServer(t, dir)

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, batch+16))
	for i := uint64(0); i < batch; i++ {
		require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: bet0Body(i, bet0BodySize)}))
	}
	for got := 0; got < batch; got++ {
		select {
		case c := <-confirms:
			require.True(t, c.Ack, "publish nacked")
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d/%d confirmed", got, batch)
		}
	}

	qPath := filepath.Join(dir, storage.MetadataDir, storage.QueuesDir, qName+storage.FileExtension)
	readOrdinal := func() uint64 {
		raw, rerr := os.ReadFile(qPath)
		require.NoError(t, rerr, "queue metadata must exist at %s", qPath)
		var q protocol.Queue
		require.NoError(t, cbor.Unmarshal(raw, &q))
		return q.Ordinal
	}
	ordinalBefore := readOrdinal()
	require.NotZerof(t, ordinalBefore, "precondition: the live queue must hold a persisted ordinal")

	// Re-declare while the re-read under the create mutex fails with an error
	// that is NOT ErrQueueNotFound. skip=1 lets DeclareQueue's lock-free
	// pre-check through, so the queue is still seen to exist and the re-read is
	// the call that fails — exactly the transient-I/O shape this gates.
	injected := fmt.Errorf("bet0 injected metadata read failure")
	pinned.armFault(qName, "DeclareQueue", 1, injected)

	redeclareCh, err := conn.Channel()
	require.NoError(t, err)
	_, declareErr := redeclareCh.QueueDeclare(qName, true, false, false, false, nil)

	// (1) The property: the live queue's ordinal band is untouched, so every
	// message already confirmed under it is still recoverable.
	ordinalAfter := readOrdinal()
	require.Equalf(t, ordinalBefore, ordinalAfter,
		"THE LIVE QUEUE WAS RE-BANDED: its persisted ordinal moved from %d to %d because a "+
			"declare could not read its metadata record. A read failure is not evidence the "+
			"queue was deleted. Every one of the %d messages already confirmed under ordinal "+
			"%d is now stranded in a band the queue no longer mints into, and recovery will "+
			"discard them as a dead incarnation's.",
		ordinalBefore, ordinalAfter, batch, ordinalBefore)

	// The refusal is delivered as a CONNECTION-level exception, not a channel
	// one, so this connection is already gone by now — Close reports 504 and
	// that is not a failure of anything under test. (Worth knowing, but out of
	// scope here: a channel-level refusal would be friendlier, and it is the
	// same shape as the publish-path errors that were wrapped in ErrQueueClosed
	// precisely so they would not tear the connection down. This path is
	// pre-existing and untouched by the Bet 0 diff.)
	_ = conn.Close()
	testShutdownServer(t, srv)
	_, uri2 := bet0Server(t, dir)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err, "the broker must restart after a declare hit a transient read failure")
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qName, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.Qos(bet0Prefetch, 0, false))
	deliveries, err := ch2.Consume(qName, "", true, false, false, false, nil)
	require.NoError(t, err)

	seen := make(map[uint64]int, batch)
	deadline := time.After(60 * time.Second)
	for len(seen) < batch {
		select {
		case d, ok := <-deliveries:
			require.Truef(t, ok, "delivery channel closed after %d/%d", len(seen), batch)
			seen[bet0Seq(d.Body)]++
		case <-deadline:
			t.Fatalf("CONFIRMED BUT NOT RECOVERABLE: only %d of %d messages came back after the "+
				"restart. They were confirmed before any of this happened; a declare that failed "+
				"to read the queue's metadata must leave the queue exactly as it found it.",
				len(seen), batch)
		}
	}

	// (2) The API-level contract, asserted last so it cannot mask the property
	// above: the declare must FAIL rather than silently succeeding on a new
	// incarnation. A failed declare is safe — the client retries.
	require.Errorf(t, declareErr,
		"a queue.declare whose metadata re-read failed must be REFUSED, not completed. Completing "+
			"it means the broker decided the queue had been deleted on the strength of an "+
			"unreadable record, and handed the client a different queue wearing the same name")
	t.Logf("declare refused as required, ordinal held at %d, all %d confirmed messages recovered: %v",
		ordinalBefore, batch, declareErr)
}
