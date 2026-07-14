package broker

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// frontier_flip_strand_test.go — the frontier visibility harness.
//
// HISTORY: iter6 fixed the durable-victim frontier-flip race (a transient's
// raw casMaxHead jumping past a pending durable tag). iter6 kept a lock-free
// fast path for queues not yet frontier-active, which left a pre-existing
// transient-vs-transient visibility race (a neighbor's casMaxHead running over
// a still-unstored lower transient on a pure-transient queue).
//
// CURRENT FIX (always-reserve-at-mint): every publish — transient or durable —
// now goes through FrontierReserve at mint + FrontierComplete after store. This
// closes both the durable-victim race AND the transient-vs-transient race. The
// frontier is activated on the first publish to any queue (monotonically, never
// reset), so pure-transient queues also go through the frontier after their
// first publish.
//
// WHAT THIS FILE ASSERTS:
// The reliable strand signal is a WaitingCount leak after full drain: a real
// accepted-but-never-delivered message leaves waiting permanently +1. All three
// modes (all-durable, mixed, pure-transient) now assert leakedWaiting==0 and
// leakedInflight==0.
//
// It deliberately does NOT assert delivered==published: at high queue counts a
// FLAKY delivered<published discrepancy with leakedWaiting==0 can appear — a
// harness delivery-count artifact in the consumer/shutdown race, NOT a strand.

const (
	flipDurableMsgs          = 3
	flipTransientGoroutines  = 8
	flipTransientMsgsPerGoro = 2
	flipMsgsPerQueue         = flipDurableMsgs + flipTransientGoroutines*flipTransientMsgsPerGoro
)

// flipResult aggregates per-queue outcomes across a harness run, split by message
// type so a durable strand (the frontier-flip bug) is distinguished from a
// transient strand (the pre-existing lock-free race), and separating the ROBUST
// signal (leaked counters) from the artifact-prone one (delivered count).
type flipResult struct {
	publishedDurable   int64
	publishedTransient int64
	deliveredDurable   int64
	deliveredTransient int64
	leakedWaiting      int64
	leakedInflight     int64
	strandedQueues     int
}

// runFrontierFlipHarness drives numQueues fresh brokers, one queue each.
//   - "mixed":     flipDurableMsgs durable (PublishMessageAsyncConfirm) race
//     flipTransientGoroutines*flipTransientMsgsPerGoro transient (PublishMessage).
//   - "durable":   every publisher durable (reserve-at-mint control; never flips late).
//   - "transient": every publisher transient, NO durable, so the frontier is never
//     activated — isolates the pre-existing transient-vs-transient race.
func runFrontierFlipHarness(t *testing.T, mode string, numQueues int) flipResult {
	t.Helper()

	// Stress override: STRANGEQ_FLIP_QUEUES scales the queue count up (e.g. 1500)
	// WITHOUT bumping the committed default (kept modest so the guard is cheap).
	if s := os.Getenv("STRANGEQ_FLIP_QUEUES"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			numQueues = n
		}
	}

	// The durability barrier gate: open by default so broker/queue setup fsyncs
	// pass; closed only across each queue's publish race, then reopened to let the
	// durables complete. A short spin parks the WAL batch-writer in the gated fsync
	// while closed, holding a prematurely-exposed durable non-ring-resident.
	var gateOpen atomic.Bool
	gateOpen.Store(true)
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		for !gateOpen.Load() {
			time.Sleep(150 * time.Microsecond)
		}
		return nil
	})
	defer restore()

	var res flipResult
	for q := 0; q < numQueues; q++ {
		runOneFrontierFlipQueue(t, mode, &gateOpen, &res)
	}
	return res
}

func runOneFrontierFlipQueue(t *testing.T, mode string, gateOpen *atomic.Bool, res *flipResult) {
	t.Helper()

	b, cleanup := createTestBroker(t)
	// On ANY exit path (including a require.* FailNow mid-run) reopen the barrier
	// FIRST, so the WAL writer is never wedged in a gated fsync while Close() waits.
	defer func() {
		gateOpen.Store(true)
		cleanup()
	}()

	const qname = "q"
	_, err := b.DeclareQueue(qname, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(qname)

	// A real live no-ack consumer, counting deliveries split by body so a durable
	// victim ("d") is distinguished from a transient victim ("x").
	msgs := make(chan *protocol.Delivery, 256)
	var deliveredDurable, deliveredTransient atomic.Int64
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		for d := range msgs {
			if len(d.Message.Body) > 0 && d.Message.Body[0] == 'd' {
				deliveredDurable.Add(1)
			} else {
				deliveredTransient.Add(1)
			}
		}
	}()
	cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
	require.NoError(t, b.RegisterConsumer(qname, "c", cons))

	// Close the barrier: a prematurely-exposed durable is now not ring-resident and
	// GetMessage-misses -> gap-skip. Transient stores are synchronous + ring-resident
	// and are unaffected by this gate.
	gateOpen.Store(false)

	var confirms atomic.Int64
	confirmCB := func(e error) {
		if e == nil {
			confirms.Add(1)
		}
	}

	var pubDurable, pubTransient int64
	switch mode {
	case "durable":
		pubDurable = int64(flipMsgsPerQueue)
	case "transient":
		pubTransient = int64(flipMsgsPerQueue)
	default: // mixed
		pubDurable = int64(flipDurableMsgs)
		pubTransient = int64(flipTransientGoroutines * flipTransientMsgsPerGoro)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	// The first publisher goroutine performs the flipDurableMsgs publishes. In
	// mixed/durable modes those are durable (the 0->1 frontier flip the transients
	// race); in pure-transient mode they publish transient too, so pubTransient
	// (=flipMsgsPerQueue) matches what is actually published.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < flipDurableMsgs; i++ {
			if mode == "transient" {
				m := &protocol.Message{RoutingKey: qname, Body: []byte("x"), DeliveryMode: 1}
				_ = b.PublishMessage("", qname, m)
			} else {
				m := &protocol.Message{RoutingKey: qname, Body: []byte("d"), DeliveryMode: 2}
				_, _, _ = b.PublishMessageAsyncConfirm("", qname, m, confirmCB)
			}
		}
	}()

	// The racing publishers.
	for g := 0; g < flipTransientGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < flipTransientMsgsPerGoro; i++ {
				if mode == "durable" {
					m := &protocol.Message{RoutingKey: qname, Body: []byte("d"), DeliveryMode: 2}
					_, _, _ = b.PublishMessageAsyncConfirm("", qname, m, confirmCB)
				} else {
					m := &protocol.Message{RoutingKey: qname, Body: []byte("x"), DeliveryMode: 1}
					_ = b.PublishMessage("", qname, m)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	// Drain everything currently visible while the durables are still gated. Once
	// tail catches head the consumer has claimed (and, for any prematurely-exposed
	// pending durable, gap-skipped) the entire visible range — locking in a strand
	// if the bug fired.
	require.Eventually(t, func() bool {
		return qs.tail.Load() >= qs.head.Load()
	}, 10*time.Second, 100*time.Microsecond, "consumer did not drain the gated-visible range")

	// Open the barrier: durables complete, become ring-resident, and their
	// FrontierComplete advances the frontier. A non-stranded durable is delivered
	// now; a stranded one only leaks waiting+1 (tail already passed it).
	gateOpen.Store(true)

	durableConfirms := pubDurable
	if mode == "durable" {
		durableConfirms = int64(flipMsgsPerQueue)
	}
	if durableConfirms > 0 {
		require.Eventually(t, func() bool {
			return confirms.Load() == durableConfirms
		}, 15*time.Second, time.Millisecond, "durable confirms did not all fire")
	}
	require.Eventually(t, func() bool {
		return qs.tail.Load() >= qs.head.Load()
	}, 15*time.Second, time.Millisecond, "consumer did not catch up after the barrier opened")

	// Synchronize final counts: stop the consumer (waits for the poll loop to exit,
	// so no send can follow), then close + drain the delivery channel.
	require.NoError(t, b.UnregisterConsumer("c"))
	close(msgs)
	<-drainerDone

	dd := deliveredDurable.Load()
	dx := deliveredTransient.Load()
	w := qs.WaitingCount()
	inf := qs.InflightCount()

	res.publishedDurable += pubDurable
	res.publishedTransient += pubTransient
	res.deliveredDurable += dd
	res.deliveredTransient += dx
	res.leakedWaiting += w
	res.leakedInflight += inf
	if w != 0 || inf != 0 {
		res.strandedQueues++
	}
}

// TestFrontierFlip_AllDurableControl is the committed reliable guard: all-durable
// publishers (reserve-at-mint everywhere; the frontier never flips late and there
// is no lock-free transient path). After full drain the summed WaitingCount and
// InflightCount MUST reconcile to 0 — the durable path never strands, before OR
// after the fix. It asserts the LEAK invariant (the robust strand signal), NOT
// delivered==published (a flaky harness count-artifact at scale).
func TestFrontierFlip_AllDurableControl(t *testing.T) {
	const numQueues = 100
	res := runFrontierFlipHarness(t, "durable", numQueues)
	t.Logf("ALL-DURABLE CONTROL q=%d durable=%d/%d leakedWaiting=%d leakedInflight=%d strandedQueues=%d",
		numQueues, res.deliveredDurable, res.publishedDurable, res.leakedWaiting, res.leakedInflight, res.strandedQueues)
	require.Zero(t, res.leakedWaiting, "all-durable path must not leak WaitingCount (no durable strand)")
	require.Zero(t, res.leakedInflight, "all-durable path must not leak InflightCount")
}

// TestFrontierFlip_MixedNoStrand asserts that mixed durable + transient
// concurrent publishers leave no leaked counters after full drain. The
// always-reserve-at-mint fix closes both the durable-victim frontier-flip race
// and the transient-vs-transient visibility race, so leakedWaiting and
// leakedInflight must both be zero.
func TestFrontierFlip_MixedNoStrand(t *testing.T) {
	const numQueues = 150
	res := runFrontierFlipHarness(t, "mixed", numQueues)
	t.Logf("MIXED q=%d durable=%d/%d transient=%d/%d leakedWaiting=%d leakedInflight=%d strandedQueues=%d",
		numQueues, res.deliveredDurable, res.publishedDurable, res.deliveredTransient, res.publishedTransient,
		res.leakedWaiting, res.leakedInflight, res.strandedQueues)
	require.Zero(t, res.leakedWaiting, "mixed durable+transient must not leak WaitingCount")
	require.Zero(t, res.leakedInflight, "mixed durable+transient must not leak InflightCount")
}

// TestFrontierFlip_PureTransientNoStrand asserts that pure-transient concurrent
// publishers (zero durables) leave no leaked counters after full drain. The
// always-reserve-at-mint fix activates the frontier on the first publish and
// closes the transient-vs-transient visibility race.
func TestFrontierFlip_PureTransientNoStrand(t *testing.T) {
	const numQueues = 100
	res := runFrontierFlipHarness(t, "transient", numQueues)
	t.Logf("PURE-TRANSIENT q=%d transient=%d/%d leakedWaiting=%d leakedInflight=%d strandedQueues=%d",
		numQueues, res.deliveredTransient, res.publishedTransient, res.leakedWaiting, res.leakedInflight, res.strandedQueues)
	require.Zero(t, res.leakedWaiting, "pure-transient must not leak WaitingCount")
	require.Zero(t, res.leakedInflight, "pure-transient must not leak InflightCount")
}
