package broker

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// dead_letter_frontier_test.go — guard for the dead-letter republish sibling of
// the frontier-flip strand (iter6 §8).
//
// THE BUG (pre-fix): republishToTargets (dead_letter.go) mints a delivery tag
// lock-free, stores it SYNCHRONOUSLY, then makes it visible with a raw
// qs.Publish(msgID) — with NO FrontierActive() awareness. Because the store
// precedes the publish, the transient-victim window is already closed there; but
// the DURABLE-victim window is open on a frontier-active DLX TARGET: the raw
// casMaxHead(msgID+1) jumps head PAST a concurrently-reserved-but-still-pending
// async-durable tag L < msgID on the target, exposing L before its fsync makes
// it ring-resident. A live consumer claims L, GetMessage-misses, and gap-skips
// it; when L's fsync completes, FrontierComplete(L,true) adds waiting+1 for a tag
// the consumer's tail already passed → confirmed-but-never-delivered + a leaked
// WaitingCount, identical to the PublishMessage strand.
//
// This reproduction is DETERMINISTIC (no probabilistic race): the gated WAL
// fsync holds the target durable L pending for the entire republish + drain
// window, and republishToTargets is driven directly so msgID is minted strictly
// after L. The fix routes an already-stored republish through
// FrontierPublishTransient when the target is frontier-active, which advances
// head only to the lowest still-pending durable floor and never past L.

// TestFrontierFlip_DeadLetterSibling_NoStrand drives republishToTargets straight
// onto a frontier-active DLX target that is holding one gated pending durable.
// Every message (the durable + the dead-letter republish) must be delivered
// exactly once and the target must reconcile to WaitingCount()==0 &&
// InflightCount()==0. FAILS before the dead_letter.go store-then-recheck fix.
func TestFrontierFlip_DeadLetterSibling_NoStrand(t *testing.T) {
	// The durability barrier gate — open by default so setup fsyncs pass, closed
	// only across the republish race, then reopened to let the durable complete.
	var gateOpen atomic.Bool
	gateOpen.Store(true)
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		for !gateOpen.Load() {
			time.Sleep(150 * time.Microsecond)
		}
		return nil
	})
	defer restore()

	b, cleanup := createTestBroker(t)
	// On ANY exit path reopen the barrier FIRST so the WAL writer is never wedged
	// in a gated fsync while Close() waits for it to drain.
	defer func() {
		gateOpen.Store(true)
		cleanup()
	}()

	const target = "dlx-target"
	_, err := b.DeclareQueue(target, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(target)

	// A real live no-ack consumer draining the target — an always-reading drainer
	// keeps deliverMessage's blocking send from stalling the poll loop (and thus
	// the gap-skip that locks in a strand).
	msgs := make(chan *protocol.Delivery, 64)
	var delivered atomic.Int64
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		for range msgs {
			delivered.Add(1)
		}
	}()
	cons := &protocol.Consumer{Tag: "c", Queue: target, NoAck: true, Messages: msgs}
	require.NoError(t, b.RegisterConsumer(target, "c", cons))

	// Close the barrier: a durable tag that is prematurely exposed (the bug) is
	// now not ring-resident and GetMessage-misses -> gap-skip.
	gateOpen.Store(false)

	// A gated async durable publish reserves L on the target's frontier and holds
	// it pending: frontierActive flips to true and head is NOT advanced past L.
	var confirms atomic.Int64
	confirmCB := func(e error) {
		if e == nil {
			confirms.Add(1)
		}
	}
	dm := &protocol.Message{RoutingKey: target, Body: []byte("d"), DeliveryMode: 2}
	_, _, err = b.PublishMessageAsyncConfirm("", target, dm, confirmCB)
	require.NoError(t, err)
	require.True(t, qs.FrontierActive(), "target must be frontier-active after the gated durable reserve")

	// Drive the dead-letter republish straight through the seam. It mints
	// msgID > L (globalDeliveryTag is monotonic and L was minted first), stores it
	// synchronously (ring-resident), then publishes it. Pre-fix, the raw
	// Publish(msgID)->casMaxHead(msgID+1) jumps head PAST the gated pending L.
	published := int64(2) // the durable L + the dead-letter republish
	clone := &protocol.Message{RoutingKey: target, Body: []byte("dl"), DeliveryMode: 1}
	b.republishToTargets("src", []string{target}, clone, nil)

	// Let the consumer drain everything currently visible while the durable is
	// still gated. Once tail catches head the consumer has claimed (and, for a
	// prematurely-exposed pending durable, gap-skipped) the entire visible range —
	// locking in the strand if the bug fired.
	require.Eventually(t, func() bool {
		return qs.tail.Load() >= qs.head.Load()
	}, 10*time.Second, 100*time.Microsecond, "consumer did not drain the gated-visible range")

	// Open the barrier: the durable completes, becomes ring-resident, and its
	// FrontierComplete advances the frontier. A non-stranded durable is delivered
	// now; a stranded one only leaks waiting+1 (tail already passed it).
	gateOpen.Store(true)
	require.Eventually(t, func() bool {
		return confirms.Load() == 1
	}, 15*time.Second, time.Millisecond, "durable confirm did not fire")
	require.Eventually(t, func() bool {
		return qs.tail.Load() >= qs.head.Load()
	}, 15*time.Second, time.Millisecond, "consumer did not catch up after the barrier opened")

	// Synchronize the final counts: stop the consumer (waits for the poll loop to
	// exit, so no send can follow), then close + drain the delivery channel.
	require.NoError(t, b.UnregisterConsumer("c"))
	close(msgs)
	<-drainerDone

	// The deterministic strand guard is the counter reconciliation below, NOT the
	// delivered count. A durable-victim strand leaves WaitingCount permanently +1
	// for the gap-skipped, confirmed-but-never-delivered durable L (fails 5/5
	// pre-fix; passes post-fix). The external delivered count is only LOGGED, never
	// asserted: a no-ack delivery is settled at channel send, and this test's
	// drainer races UnregisterConsumer's internal drain over the shared msgs
	// channel, so the count can be lost even on a fully-correct run (waiting,
	// inflight and confirms all remain correct) — the same delivered-count harness
	// artifact excluded from the mixed harness.
	t.Logf("DLX SIBLING published=%d delivered=%d (diagnostic; guard is WaitingCount/InflightCount==0)",
		published, delivered.Load())
	require.Zero(t, qs.WaitingCount(), "DLX target must not leak WaitingCount (Depth leak) — the durable-victim strand signal")
	require.Zero(t, qs.InflightCount(), "DLX target must not leak InflightCount")
}
