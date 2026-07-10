package broker

import (
	"fmt"
	"os"
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

// TestAsyncConfirm_NotVisibleBeforeDurable (TDD test 7, guards A3).
//
// PublishMessageAsyncConfirm defers a durable copy's consumer-visibility
// (queueState.Publish) into that copy's WAL fsync completion. With the fsync
// gated, the published tag must NOT be claimable and the ready count must stay
// zero; only after the fsync is released does the message become visible and
// the aggregator fire onDurable exactly once.
func TestAsyncConfirm_NotVisibleBeforeDurable(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	defer once.Do(func() { close(release) })

	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("dq", true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState("dq")

	var fired atomic.Int32
	msg := &protocol.Message{Body: []byte("durable"), RoutingKey: "dq", DeliveryMode: 2}
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("", "dq", msg, func(error) {
		fired.Add(1)
	})
	require.NoError(t, perr)
	assert.True(t, durableInflight, "a routed durable publish must report durableInflight=true")

	// While the fsync is gated: not durable => not visible.
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("dq"), "durable message must NOT be visible before fsync (A3)")
	assert.Equal(t, int32(0), fired.Load(), "onDurable must not fire before fsync")

	stop, cancel := makeStop()
	defer cancel()

	// A consumer claiming from the queue must PARK (not obtain the tag) for as
	// long as the fsync is gated — the message is not yet durable, so not visible.
	claimed := make(chan uint64, 1)
	go func() {
		if tag, _, ok := qs.Claim(stop, testTimer(qs)); ok {
			claimed <- tag
		}
	}()
	select {
	case tag := <-claimed:
		t.Fatalf("claimed tag %d before durable — visibility leaked (A3)", tag)
	case <-time.After(200 * time.Millisecond):
		// still parked — correct
	}

	// Release the durability barrier: the copy becomes visible and the confirm
	// aggregator fires exactly once.
	once.Do(func() { close(release) })

	select {
	case <-claimed:
		// Claim unparked (ok=true): the head cursor advanced, i.e. the copy
		// became visible — but only after the fsync was released. (The claimed
		// value is the delivery cursor, not the msgID, so its magnitude is not
		// asserted.)
	case <-time.After(5 * time.Second):
		t.Fatal("message never became claimable after the fsync completed")
	}
	assert.Eventually(t, func() bool { return fired.Load() == 1 }, 5*time.Second, 5*time.Millisecond,
		"onDurable must fire exactly once after durability")
	assert.Equal(t, uint32(1), b.GetQueueReadyCount("dq"), "message must be visible (ready) after durability")
}

// TestAsyncConfirm_TransientIsSynchronousAndVisible pins the sync side of the
// contract: a routed TRANSIENT publish through the same API is made visible
// immediately (no deferred barrier) and reports durableInflight=false, so the
// server settles its confirm synchronously (preserving inline-flush latency).
func TestAsyncConfirm_TransientIsSynchronousAndVisible(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("tq", false, false, false, nil)
	require.NoError(t, err)

	msg := &protocol.Message{Body: []byte("transient"), RoutingKey: "tq", DeliveryMode: 1}
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("", "tq", msg, func(error) {
		t.Fatal("onDurable must not be invoked by the broker on the transient/sync path")
	})
	require.NoError(t, perr)
	assert.False(t, durableInflight, "a transient publish must report durableInflight=false")
	assert.Equal(t, uint32(1), b.GetQueueReadyCount("tq"), "transient message is visible synchronously")
}

// TestAsyncConfirm_AtHWMAdmitsWithoutParkAndReportsBackpressure (iteration 2,
// option A — reader-level depth backpressure).
//
// A durable+confirm publish to a queue already at its high-water mark must NOT
// park the frame processor in the broker. The confirm tag was minted upstream
// (server side), so parking here until a consumer drains — the old WaitForCapacity
// behavior — STRANDS that tag whenever the consumer has stopped (the
// benchmark-teardown deadlock: the queue stays at HWM forever, the call never
// returns, the confirm is never sent). Instead the publish is admitted (always
// durablizes+confirms) and a non-nil backpressure gate is returned so the caller
// pauses the READER (pacing the producer, keeping the queue small). This fills a
// queue to its HWM with no consumer, then requires the next publish to return
// promptly with a gate reporting AtHighWaterMark.
func TestAsyncConfirm_AtHWMAdmitsWithoutParkAndReportsBackpressure(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("bpq", true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState("bpq")
	qs.SetDepthHighWM(2)

	// Fill exactly to HWM with transient publishes (waiting is incremented
	// synchronously; each checks capacity BEFORE its own increment, so neither
	// parks). No consumer drains, so the queue now sits AT the high-water mark —
	// a further transient publish WOULD park here, but the durable path must not.
	for i := 0; i < 2; i++ {
		require.NoError(t, b.PublishMessage("", "bpq", &protocol.Message{
			Body: []byte("fill"), RoutingKey: "bpq", DeliveryMode: 1,
		}))
	}
	require.True(t, qs.AtHighWaterMark(), "queue must be at HWM after filling")

	// The next durable+confirm publish must return promptly (no processor park) and
	// report a backpressure gate. A park would block this goroutine forever.
	type result struct {
		bp  protocol.DepthGate
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, bp, perr := b.PublishMessageAsyncConfirm("", "bpq", &protocol.Message{
			Body: []byte("durable"), RoutingKey: "bpq", DeliveryMode: 2,
		}, func(error) {})
		done <- result{bp, perr}
	}()

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.NotNil(t, r.bp, "an at-HWM publish must report a backpressure gate for reader-level pacing")
		require.True(t, r.bp.AtHighWaterMark(), "the reported gate must observe the queue at HWM")
	case <-time.After(3 * time.Second):
		t.Fatal("PublishMessageAsyncConfirm parked at HWM — it must admit (bounded overshoot), never strand the confirm tag")
	}
}

// TestAsyncConfirm_FanoutFiresOnceAfterAllCopies (TDD test 4, fanout aggregator).
//
// A durable publish fanned out to two queues carries ONE confirm tag; onDurable
// must fire exactly once, and only after BOTH copies are durable. Combined with
// the protocol-level fold unit tests (reordered completion => contiguous
// watermark), this guards A1 for multi-copy publishes.
func TestAsyncConfirm_FanoutFiresOnceAfterAllCopies(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	defer once.Do(func() { close(release) })

	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"fq1", "fq2"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}

	var fired atomic.Int32
	msg := &protocol.Message{Body: []byte("fanout"), RoutingKey: "", DeliveryMode: 2}
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("fx", "", msg, func(error) { fired.Add(1) })
	require.NoError(t, perr)
	assert.True(t, durableInflight)

	// Both copies gated: no confirm, neither queue visible.
	assert.Equal(t, int32(0), fired.Load())
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("fq1"))
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("fq2"))

	once.Do(func() { close(release) })

	assert.Eventually(t, func() bool {
		return b.GetQueueReadyCount("fq1") == 1 && b.GetQueueReadyCount("fq2") == 1
	}, 5*time.Second, 5*time.Millisecond, "both fanout copies must become visible")
	assert.Eventually(t, func() bool { return fired.Load() == 1 }, 5*time.Second, 5*time.Millisecond,
		"onDurable must fire exactly once after ALL copies are durable")
	// Give any erroneous extra completion a moment to (not) arrive.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), fired.Load(), "onDurable must fire EXACTLY once for a fanout publish")
}

// TestFrontier_OutOfOrderCompletionBlocksHead (TDD test 8 core, guards A3).
//
// Two durable tags registered in order (10, 11); the HIGHER tag (11) completes
// its fsync FIRST. head must NOT advance past the still-pending lower tag 10
// (so 11 is not claimable), and must advance to expose both only once 10's own
// fsync completes. This is the exact multi-publisher / cross-connection
// out-of-order case that eng-a's per-copy CAS-max advance got wrong.
func TestFrontier_OutOfOrderCompletionBlocksHead(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()

	t10 := qs.FrontierReserve(func() uint64 { return 10 })
	t11 := qs.FrontierReserve(func() uint64 { return 11 })
	require.Equal(t, uint64(10), t10)
	require.Equal(t, uint64(11), t11)
	require.True(t, qs.FrontierActive())
	assert.Equal(t, uint64(0), qs.head.Load(), "no tag visible until its fsync")

	// 11 completes first (out of order): head must stay at/below 10 — 11 is NOT
	// claimable because the lower tag 10 is still pending.
	qs.FrontierComplete(11, true)
	assert.LessOrEqual(t, qs.head.Load(), uint64(10),
		"head must not pass the still-pending lower tag 10 (no delivery-before-durable, A3)")

	// 10 completes: the frontier folds across 10 and the already-done 11 → head
	// exposes both (head = maxSeen+1 = 12).
	qs.FrontierComplete(10, true)
	assert.Equal(t, uint64(12), qs.head.Load(), "both tags visible once the lower tag is durable")
	assert.Equal(t, int64(2), qs.WaitingCount(), "both real messages counted ready exactly once")
}

// TestFrontier_TransientBehindPendingDurable (guards refinement 1/2 FIFO): a
// transient tag published to a frontier-active queue while an earlier durable
// tag is still pending must NOT be exposed until the durable tag is durable
// (publish-order FIFO across a mixed queue).
func TestFrontier_TransientBehindPendingDurable(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()

	qs.FrontierReserve(func() uint64 { return 10 }) // durable, pending
	qs.FrontierPublishTransient(11)                 // transient, already stored
	assert.LessOrEqual(t, qs.head.Load(), uint64(10),
		"transient tag 11 must not jump head past the pending durable tag 10 (FIFO, A3)")

	qs.FrontierComplete(10, true)
	assert.Equal(t, uint64(12), qs.head.Load(), "both visible once the durable tag is durable")
}

// TestFrontier_FsyncErrorAdvancesButNotReady (guards refinement 3): an fsync
// error advances the frontier past the tag (so later tags are not stranded) but
// never counts it ready — it is gap-skipped, never delivered.
func TestFrontier_FsyncErrorAdvancesButNotReady(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()

	qs.FrontierReserve(func() uint64 { return 5 })
	qs.FrontierComplete(5, false) // fsync error
	assert.Equal(t, uint64(6), qs.head.Load(), "frontier must advance past the errored tag (no stranding of later tags)")
	assert.Equal(t, int64(0), qs.WaitingCount(), "an fsync-errored tag is never counted ready")
}

// TestAsyncConfirm_MultiPublisherNotVisibleBeforeDurable (TDD test 8 end to
// end): two producer goroutines publish durable+confirm to the SAME queue with
// the fsync gated open. NOTHING may be claimable/ready while gated; once
// released every tag becomes durable, visible, and confirmed.
func TestAsyncConfirm_MultiPublisherNotVisibleBeforeDurable(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	defer once.Do(func() { close(release) })

	b, cleanup := createTestBroker(t)
	defer cleanup()
	_, err := b.DeclareQueue("mq", true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState("mq")

	const perProducer = 25
	var confirmed atomic.Int32
	var wg sync.WaitGroup
	for p := 0; p < 2; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				msg := &protocol.Message{RoutingKey: "mq", Body: []byte("x"), DeliveryMode: 2}
				_, _, perr := b.PublishMessageAsyncConfirm("", "mq", msg, func(e error) {
					if e == nil {
						confirmed.Add(1)
					}
				})
				require.NoError(t, perr)
			}
		}()
	}
	wg.Wait() // all publishes enqueued (non-blocking); fsync still gated

	// Nothing durable yet ⇒ nothing visible. Give any erroneous completion a
	// moment, then assert the queue exposes nothing.
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("mq"), "no durable message may be visible before fsync (A3)")
	assert.Equal(t, int32(0), confirmed.Load(), "no confirm before fsync (A1)")
	stop, cancel := makeStop()
	defer cancel()
	claimed := make(chan uint64, 1)
	go func() {
		if tag, _, ok := qs.Claim(stop, testTimer(qs)); ok {
			claimed <- tag
		}
	}()
	select {
	case tag := <-claimed:
		t.Fatalf("claimed cursor %d before any message was durable (A3 violation)", tag)
	case <-time.After(150 * time.Millisecond):
	}

	// Release: every tag becomes durable, visible, and confirmed.
	once.Do(func() { close(release) })
	select {
	case <-claimed:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing became claimable after the fsync completed")
	}
	require.Eventually(t, func() bool { return confirmed.Load() == 2*perProducer }, 5*time.Second, 10*time.Millisecond,
		"every publish must be confirmed after durability")
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("mq") == 2*perProducer }, 5*time.Second, 10*time.Millisecond,
		"every durable message must be visible after fsync")
}

// TestFrontier_ConcurrentDurableTransientNoRace (refinement 2 race guard, run
// under -race): interleave durable-confirm and transient publishes to the SAME
// queue from separate goroutines; after all fsyncs complete, drain and assert
// every message is delivered exactly once (no strand, no early-delivery gap, no
// data race on the frontier).
func TestFrontier_ConcurrentDurableTransientNoRace(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	_, err := b.DeclareQueue("cq2", true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState("cq2")

	const n = 200
	var confirmed atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)
	// Durable-confirm producer.
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			msg := &protocol.Message{RoutingKey: "cq2", Body: []byte("d"), DeliveryMode: 2}
			_, _, _ = b.PublishMessageAsyncConfirm("", "cq2", msg, func(e error) {
				if e == nil {
					confirmed.Add(1)
				}
			})
		}
	}()
	// Transient producer to the same (now frontier-active) queue.
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			msg := &protocol.Message{RoutingKey: "cq2", Body: []byte("t"), DeliveryMode: 1}
			_ = b.PublishMessage("", "cq2", msg)
		}
	}()
	wg.Wait()

	require.Eventually(t, func() bool { return confirmed.Load() == n }, 10*time.Second, 10*time.Millisecond,
		"all durable publishes must confirm")

	// Every durable+transient message must eventually become ready (visible) —
	// none stranded behind the frontier. WaitingCount counts each real message
	// ready exactly once. Under -race this also proves the frontier's concurrent
	// durable/transient access is data-race free.
	require.Eventually(t, func() bool { return qs.WaitingCount() == int64(2*n) }, 10*time.Second, 10*time.Millisecond,
		"all %d messages must become ready (no strand); got %d", 2*n, qs.WaitingCount())

	// Drain via a consumer: every ready message must be deliverable exactly once
	// (a stranded gap-skip tag would leave delivered < 2n and WaitingCount > 0 at
	// the end — the I1 leak). Real deliveries settle through the depth ledger;
	// gap tags advance the frontier without counting.
	stop, cancel := makeStop()
	var delivered atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			tag, _, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				return
			}
			if _, gerr := b.storage.GetMessage("cq2", tag); gerr == nil {
				qs.ClaimInflight(tag)
				qs.AckAdvance(tag)
				delivered.Add(1)
			} else {
				qs.GapSkipAdvance(tag)
			}
		}
	}()
	require.Eventually(t, func() bool { return delivered.Load() == int64(2*n) }, 10*time.Second, 10*time.Millisecond,
		"every durable+transient message must be delivered exactly once (no strand); delivered %d of %d", delivered.Load(), 2*n)
	cancel()
	<-done
	assert.Equal(t, int64(0), qs.WaitingCount(), "ready count must reconcile to 0 after full drain (no Depth leak)")
}

// TestFrontier_PerQueueFrontiersIndependent (refinement 1): each queue has its
// OWN frontier — a fanout copy to queue A becoming durable exposes A's copy
// without touching B's still-pending copy, and vice versa. Deterministic (drives
// two QueueState frontiers directly).
func TestFrontier_PerQueueFrontiersIndependent(t *testing.T) {
	qa := NewQueueState(0)
	defer qa.Close()
	qb := NewQueueState(0)
	defer qb.Close()

	// One fanout publish assigns tag 7 to A's copy and tag 8 to B's copy (distinct
	// global tags, distinct per-queue frontiers).
	qa.FrontierReserve(func() uint64 { return 7 })
	qb.FrontierReserve(func() uint64 { return 8 })
	assert.Equal(t, uint64(0), qa.head.Load())
	assert.Equal(t, uint64(0), qb.head.Load())

	// A's copy becomes durable first: only A advances; B stays blocked.
	qa.FrontierComplete(7, true)
	assert.Equal(t, uint64(8), qa.head.Load(), "queue A exposes its copy once A's fsync completes")
	assert.Equal(t, uint64(0), qb.head.Load(), "queue B must NOT advance on A's completion (independent frontiers)")

	// B's copy becomes durable: B advances independently.
	qb.FrontierComplete(8, true)
	assert.Equal(t, uint64(9), qb.head.Load(), "queue B exposes its copy once B's own fsync completes")
}

// TestAsyncConfirm_FanoutConfirmOnlyAfterAllCopies (review ask #2): a fanout
// publish to two durable queues carries ONE confirm tag; with the fsync gated
// per call and released ONE AT A TIME, each queue's copy becomes visible as its
// own fsync completes (possibly in different batches), and the single confirm
// fires ONLY after BOTH copies are durable — never after just one.
func TestAsyncConfirm_FanoutConfirmOnlyAfterAllCopies(t *testing.T) {
	// One release token per group-commit fsync call; the test paces them.
	release := make(chan struct{})
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	// Drain any stragglers so the WAL writer can shut down cleanly on Close.
	defer func() {
		go func() {
			for {
				select {
				case release <- struct{}{}:
				case <-time.After(time.Second):
					return
				}
			}
		}()
	}()

	b, cleanup := createTestBroker(t)
	defer cleanup()
	require.NoError(t, b.DeclareExchange("fx2", "fanout", true, false, false, nil))
	for _, q := range []string{"fa", "fb"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx2", "", nil))
	}

	var fired atomic.Int32
	msg := &protocol.Message{Body: []byte("fan"), RoutingKey: "", DeliveryMode: 2}
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("fx2", "", msg, func(error) { fired.Add(1) })
	require.NoError(t, perr)
	require.True(t, durableInflight)

	// Both copies gated: no confirm, neither queue visible.
	assert.Equal(t, int32(0), fired.Load())
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("fa"))
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("fb"))

	// Release fsync calls one at a time until the confirm fires. Before the final
	// release the confirm must NOT have fired (both copies not yet durable), and
	// the number of visible queues must equal the number of copies made durable
	// so far — proving per-copy independent visibility across batches.
	visibleCount := func() int {
		n := 0
		if b.GetQueueReadyCount("fa") == 1 {
			n++
		}
		if b.GetQueueReadyCount("fb") == 1 {
			n++
		}
		return n
	}
	for released := 1; released <= 2; released++ {
		select {
		case release <- struct{}{}:
		case <-time.After(2 * time.Second):
			t.Fatalf("no fsync call pending for release %d", released)
		}
		// Let the completion propagate.
		deadline := time.After(2 * time.Second)
		for visibleCount() < released {
			select {
			case <-deadline:
				t.Fatalf("expected >=%d queue(s) visible after %d fsync release(s), got %d", released, released, visibleCount())
			case <-time.After(2 * time.Millisecond):
			}
			if visibleCount() >= released {
				break
			}
		}
		if visibleCount() < 2 {
			// Not all copies durable yet ⇒ the confirm MUST NOT have fired.
			assert.Equal(t, int32(0), fired.Load(),
				"confirm must not fire until ALL fanout copies are durable (only %d visible)", visibleCount())
		}
		if visibleCount() == 2 {
			break
		}
	}

	// Both copies durable ⇒ both queues visible and the confirm fired exactly once.
	require.Eventually(t, func() bool { return visibleCount() == 2 }, 3*time.Second, 5*time.Millisecond,
		"both fanout copies must be visible once durable")
	require.Eventually(t, func() bool { return fired.Load() == 1 }, 3*time.Second, 5*time.Millisecond,
		"the single confirm must fire exactly once, after BOTH copies are durable")
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, int32(1), fired.Load(), "confirm must fire EXACTLY once for the fanout publish")
}

// flakyAsyncStore wraps a real DisruptorStorage and can fail StoreMessageAsync
// on demand, to exercise the enqueue-error path (M1).
type flakyAsyncStore struct {
	*storage.DisruptorStorage
	failStoreAsync atomic.Bool
}

func (f *flakyAsyncStore) StoreMessageAsync(queueName string, message *protocol.Message, onDurable func(error)) error {
	if f.failStoreAsync.Load() {
		return fmt.Errorf("injected StoreMessageAsync failure")
	}
	return f.DisruptorStorage.StoreMessageAsync(queueName, message, onDurable)
}

// TestFrontier_EnqueueErrorDoesNotWedgeQueue (M1). A durable publish reserves a
// frontier slot at mint, then StoreMessageAsync fails; the reserved slot MUST be
// released (FrontierComplete(tag,false)) or it stays pending forever and wedges
// the per-queue frontier — every later publish's head advance stalls behind the
// stuck tag. Asserts a subsequent durable publish still becomes visible.
func TestFrontier_EnqueueErrorDoesNotWedgeQueue(t *testing.T) {
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	ds, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	fs := &flakyAsyncStore{DisruptorStorage: ds}
	b := NewStorageBroker(fs, ec)
	defer func() { b.Close(); _ = ds.Close() }()

	_, err = b.DeclareQueue("wedge-q", true, false, false, nil)
	require.NoError(t, err)

	// First durable publish: StoreMessageAsync fails after the frontier reserve.
	fs.failStoreAsync.Store(true)
	_, _, perr := b.PublishMessageAsyncConfirm("", "wedge-q",
		&protocol.Message{RoutingKey: "wedge-q", Body: []byte("boom"), DeliveryMode: 2}, nil)
	require.Error(t, perr, "the injected enqueue error must surface")
	fs.failStoreAsync.Store(false)

	// The queue must NOT be wedged: a subsequent durable publish becomes visible.
	confirmed := make(chan error, 1)
	_, _, perr = b.PublishMessageAsyncConfirm("", "wedge-q",
		&protocol.Message{RoutingKey: "wedge-q", Body: []byte("ok"), DeliveryMode: 2},
		func(e error) { confirmed <- e })
	require.NoError(t, perr)
	select {
	case e := <-confirmed:
		require.NoError(t, e)
	case <-time.After(5 * time.Second):
		t.Fatal("second publish never confirmed")
	}
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("wedge-q") == 1 }, 5*time.Second, 10*time.Millisecond,
		"queue WEDGED: the second durable publish never became visible after an enqueue error (M1)")
}

// TestFrontier_TransientReservedAtMintNotPassed (I1). A transient tag reserved at
// mint on a frontier-active (mixed) queue must block head exactly like a durable
// pending tag: even when a HIGHER durable tag completes first, head must not
// advance past the reserved-but-not-yet-ready transient (which would gap-skip and
// strand it + leak its ready count). Deterministic frontier-level guard.
func TestFrontier_TransientReservedAtMintNotPassed(t *testing.T) {
	qs := NewQueueState(0)
	defer qs.Close()

	qs.FrontierReserve(func() uint64 { return 5 }) // durable, pending
	qs.FrontierReserve(func() uint64 { return 6 }) // transient RESERVED AT MINT (I1), pending
	qs.FrontierReserve(func() uint64 { return 7 }) // durable, pending

	qs.FrontierComplete(7, true) // higher tag done first
	assert.LessOrEqual(t, qs.head.Load(), uint64(5), "head blocked at pending 5")

	qs.FrontierComplete(5, true) // 5 done; head may reach 6 but not pass it
	assert.LessOrEqual(t, qs.head.Load(), uint64(6),
		"head must NOT pass the reserved-but-unstored transient 6 (no strand, A3/I1)")

	qs.FrontierComplete(6, true) // transient stored+ready
	assert.Equal(t, uint64(8), qs.head.Load(), "all exposed once the transient is ready")
	assert.Equal(t, int64(3), qs.WaitingCount(), "each real message counted ready exactly once (no Depth leak)")
}

// TestAsyncConfirm_DropHeadDeadLetterFromCompletionNoDeadlock (C1 — CRITICAL,
// A4 hard-deadlock guard). A durable drop-head queue with a durable dead-letter
// exchange: enough durable-confirm publishes force a max-length eviction FROM
// the async completion (which runs on the shared WAL batch-writer goroutine).
// The eviction dead-letters the evicted durable message, whose republish is a
// synchronous durable StoreMessage -> wal.Write -> <-doneChan; that doneChan is
// only signalled by the batch-writer goroutine, now blocked inside the
// completion => the entire durable pipeline deadlocks. The deferred-trim fix
// hands eviction to a separate worker, so every publish confirms promptly.
// (The transient-spill-over-threshold variant is the same class — any blocking
// StoreMessage/Write from a completion — and is covered by the same fix.)
func TestAsyncConfirm_DropHeadDeadLetterFromCompletionNoDeadlock(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("dlx-c1", "fanout", true, false, false, nil))
	_, err := b.DeclareQueue("dlq-c1", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq-c1", "dlx-c1", "", nil))

	_, err = b.DeclareQueue("main-c1", true, false, false, map[string]interface{}{
		"x-max-length":           int64(2),
		"x-overflow":             "drop-head",
		"x-dead-letter-exchange": "dlx-c1",
	})
	require.NoError(t, err)

	const n = 40
	confirmed := make(chan error, n)
	for i := 0; i < n; i++ {
		msg := &protocol.Message{RoutingKey: "main-c1", Body: []byte("payload"), DeliveryMode: 2}
		_, _, perr := b.PublishMessageAsyncConfirm("", "main-c1", msg, func(e error) { confirmed <- e })
		require.NoError(t, perr)
	}

	for i := 0; i < n; i++ {
		select {
		case <-confirmed:
		case <-time.After(10 * time.Second):
			t.Fatalf("DEADLOCK: only %d/%d durable publishes confirmed — eviction-in-completion blocked the WAL batch writer (A4 violation)", i, n)
		}
	}
}

// TestCrash_ConfirmedImpliesRecoverable (TDD test 5, guards A1 end to end).
//
// Every message whose publisher confirm fired (onDurable(nil)) must survive a
// simulated crash: publish N durable+confirm messages, wait for all confirms,
// close the store (crash), reopen the same data dir, and assert every confirmed
// message is recovered from the WAL with its body intact.
func TestCrash_ConfirmedImpliesRecoverable(t *testing.T) {
	dataDir := t.TempDir()
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}

	store1, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	b1 := NewStorageBroker(store1, ec)
	_, err = b1.DeclareQueue("cq", true, false, false, nil)
	require.NoError(t, err)

	const n = 300
	confirmed := make(chan error, n)
	bodies := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		body := fmt.Sprintf("msg-%04d", i)
		bodies[body] = false
		msg := &protocol.Message{RoutingKey: "cq", Body: []byte(body), DeliveryMode: 2}
		durableInflight, _, perr := b1.PublishMessageAsyncConfirm("", "cq", msg, func(e error) { confirmed <- e })
		require.NoError(t, perr)
		require.True(t, durableInflight)
	}

	// Every publish must confirm (no fsync error injected).
	for i := 0; i < n; i++ {
		select {
		case e := <-confirmed:
			require.NoError(t, e, "no publish may be nacked without an injected error")
		case <-time.After(15 * time.Second):
			t.Fatalf("only %d/%d confirms fired", i, n)
		}
	}

	b1.Close()
	require.NoError(t, store1.Close())

	// Crash + restart: reopen the same data dir and recover from the WAL.
	store2, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	defer store2.Close()

	recovered, err := store2.GetRecoverableMessages()
	require.NoError(t, err)
	got := recovered["cq"]
	require.Len(t, got, n, "every confirmed message must be recoverable after a crash")
	for _, m := range got {
		bodies[string(m.Body)] = true
	}
	for body, seen := range bodies {
		assert.True(t, seen, "confirmed message %q missing after recovery", body)
	}
}
