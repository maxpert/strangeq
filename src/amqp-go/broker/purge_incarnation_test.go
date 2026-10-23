package broker

import (
	"errors"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// publishConfirmedDurable synchronously publishes n confirmed durable
// (DeliveryMode 2) messages to queueName via the default exchange. PublishMessage
// is synchronous for the shared body threshold's non-fanout path, so a nil
// return means the WAL fsync for that message already completed and the
// message is ring-resident (broker.PublishMessage -> storage.StoreMessage ->
// FrontierComplete(tag, true), all before this call returns).
func publishConfirmedDurable(t *testing.T, b *StorageBroker, queueName string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		msg := &protocol.Message{
			Body:         []byte("payload"),
			DeliveryMode: 2,
		}
		if err := b.PublishMessage("", queueName, msg); err != nil {
			t.Fatalf("PublishMessage(%s) #%d: %v", queueName, i, err)
		}
	}
}

// TestPurgeIncarnationAppliesOnlyToTheHandedIncarnation is T2 from spec-b4.
// Declares two distinctly-named (hence distinctly-ordinaled) queues and calls
// purgeIncarnation with a NAME/INCARNATION mismatch — "qB" paired with qA's
// QueueState — the same disagreement a concurrent delete+redeclare race would
// produce, made deterministic. The fix must apply every effect to the
// incarnation it was HANDED (qsA), never to whatever "qB" currently names.
func TestPurgeIncarnationAppliesOnlyToTheHandedIncarnation(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	if _, err := b.DeclareQueue("qA", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(qA): %v", err)
	}
	if _, err := b.DeclareQueue("qB", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(qB): %v", err)
	}

	qsA := b.getOrCreateQueueState("qA")
	qsB := b.getOrCreateQueueState("qB")

	if qsA.Ordinal() == qsB.Ordinal() {
		t.Fatalf("qA and qB share ordinal %d; the test needs distinct incarnations", qsA.Ordinal())
	}

	const n = 3
	publishConfirmedDurable(t, b, "qA", n)
	publishConfirmedDurable(t, b, "qB", n)

	// Snapshot qB's cursors and tags BEFORE the mismatched call.
	preTail := qsB.tail.Load()
	preHead := qsB.head.Load()
	preMinAck := qsB.minAckCursor.Load()
	preWaiting := qsB.waiting.Load()
	preInflight := qsB.inflight.Load()
	minB, _ := qsB.TagBand()

	// Sanity: qA has unclaimed messages, so its tail != head before the call —
	// otherwise assertion (3) below (tail becomes head) would pass vacuously.
	if qsA.tail.Load() == qsA.head.Load() {
		t.Fatalf("qsA.tail already equals qsA.head before the call; the reset assertion would be vacuous")
	}

	count, err := b.purgeIncarnation("qB", qsA)

	// (1) err == nil, count == 0: qB's ring holds no tags in qA's band.
	if err != nil {
		t.Errorf("purgeIncarnation(qB, qsA) unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("purgeIncarnation(qB, qsA) count = %d, want 0", count)
	}

	// (2) qB's cursors unchanged AND all qB messages still resident.
	if got := qsB.tail.Load(); got != preTail {
		t.Errorf("qB.tail changed: %d -> %d, want unchanged", preTail, got)
	}
	if got := qsB.head.Load(); got != preHead {
		t.Errorf("qB.head changed: %d -> %d, want unchanged", preHead, got)
	}
	if got := qsB.minAckCursor.Load(); got != preMinAck {
		t.Errorf("qB.minAckCursor changed: %d -> %d, want unchanged", preMinAck, got)
	}
	if got := qsB.waiting.Load(); got != preWaiting {
		t.Errorf("qB.waiting changed: %d -> %d, want unchanged", preWaiting, got)
	}
	if got := qsB.inflight.Load(); got != preInflight {
		t.Errorf("qB.inflight changed: %d -> %d, want unchanged", preInflight, got)
	}
	for i := uint64(0); i < uint64(n); i++ {
		tag := minB + i
		if _, err := b.storage.GetMessage("qB", tag); err != nil {
			t.Errorf("GetMessage(qB, tag=%d) = %v, want the message still resident", tag, err)
		}
	}

	// (3) The operation applied to the incarnation it was HANDED, under the
	// WS-D §4 claim-mechanics contract: qsA's ready tags were POPPED (tail
	// advanced to head), but because storage is name-keyed the deletes were
	// issued against "qB" and won NOTHING (count==0 above) — so qsA's depth
	// counters move only per actual removal (none), exactly like a consumer's
	// GetMessage-miss gap-skip. The pre-§4 contract reset waiting/inflight
	// unconditionally, which is what lost unacked bodies (T4.1); that
	// assertion is deliberately gone. A no-op purge would leave tail==tail0
	// below head, so the tail assertion still forces the handed incarnation
	// to have been operated on.
	if got := qsA.tail.Load(); got != qsA.head.Load() {
		t.Errorf("qsA.tail=%d, qsA.head=%d after purgeIncarnation(qB, qsA); want tail==head (ready cursor consumed)", got, qsA.head.Load())
	}
	if got := qsA.waiting.Load(); got != int64(n) {
		t.Errorf("qsA.waiting=%d after purgeIncarnation(qB, qsA), want %d (no name-keyed deletion won, so depth is untouched)", got, n)
	}
	if got := qsA.inflight.Load(); got != 0 {
		t.Errorf("qsA.inflight=%d after purgeIncarnation(qB, qsA), want 0", got)
	}
}

// TestPurgeIncarnationRefusesSupersededIncarnation is T3 from spec-b4. A
// caller holding a QueueState from BEFORE a delete+redeclare must be refused
// (ErrQueueNotFound), not applied to the successor and not silently
// no-op-succeeding against nothing.
func TestPurgeIncarnationRefusesSupersededIncarnation(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	if _, err := b.DeclareQueue("q", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q): %v", err)
	}
	qs1 := b.getOrCreateQueueState("q")
	publishConfirmedDurable(t, b, "q", 2)

	if _, err := b.DeleteQueue("q", false, false); err != nil {
		t.Fatalf("DeleteQueue(q): %v", err)
	}
	if _, err := b.DeclareQueue("q", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q) redeclare: %v", err)
	}
	qs2 := b.getOrCreateQueueState("q")

	// PREMISE of the whole design: ordinals are never reused within a run.
	// Assert it at runtime rather than assuming it.
	if qs2.Ordinal() == qs1.Ordinal() {
		t.Fatalf("redeclare reused ordinal %d from the deleted incarnation; ordinals must never repeat within a broker run", qs1.Ordinal())
	}

	const n = 3
	publishConfirmedDurable(t, b, "q", n)
	minSucc, _ := qs2.TagBand()

	// Snapshot the successor's DISPATCH CURSORS, not just its storage residency.
	// GetMessage falls back readAhead -> WAL -> segments
	// (storage/disruptor_storage.go GetMessage), so a durable message stays
	// retrievable even after its ring slot is wiped. Residency therefore CANNOT
	// observe this defect: the confirm-then-strand failure is the cursor reset
	// landing on the successor, which leaves every message intact on disk and
	// permanently unclaimable. Assert the cursors or assert nothing.
	preTail := qs2.Tail()
	preHead := qs2.Head()
	preWaiting := qs2.WaitingCount()
	if preWaiting != int64(n) {
		t.Fatalf("successor waiting=%d before the call, want %d; the cursor assertions below would be vacuous", preWaiting, n)
	}

	count, err := b.purgeIncarnation("q", qs1)
	if !errors.Is(err, interfaces.ErrQueueNotFound) {
		t.Errorf("purgeIncarnation(q, qs1) err = %v, want interfaces.ErrQueueNotFound", err)
	}
	if count != 0 {
		t.Errorf("purgeIncarnation(q, qs1) count = %d, want 0", count)
	}
	if got := qs2.Tail(); got != preTail {
		t.Errorf("successor tail changed: %d -> %d; a purge of the DELETED incarnation stranded the successor's ready set", preTail, got)
	}
	if got := qs2.Head(); got != preHead {
		t.Errorf("successor head changed: %d -> %d, want unchanged", preHead, got)
	}
	if got := qs2.WaitingCount(); got != preWaiting {
		t.Errorf("successor waiting changed: %d -> %d; its confirmed durable messages are no longer claimable", preWaiting, got)
	}
	for i := uint64(0); i < uint64(n); i++ {
		tag := minSucc + i
		if _, err := b.storage.GetMessage("q", tag); err != nil {
			t.Errorf("GetMessage(q, tag=%d) = %v, want the successor's message still resident", tag, err)
		}
	}
}

// TestQueuePurgeUnderConcurrentRedeclareLeavesSuccessorIntact is T4 from
// spec-b4: reachability of the T3 guard under a real goroutine interleaving,
// with a channel rendezvous rather than a stress loop. G1 resolves the
// pre-redeclare incarnation, blocks on a gate, then calls purgeIncarnation.
// Main deletes and redeclares "q" and publishes to the successor BEFORE
// opening the gate, so G1's purgeIncarnation always runs against an
// already-closed incarnation — a deterministic seam, not a race that
// sometimes reproduces. One round is sufficient; do not loop it.
func TestQueuePurgeUnderConcurrentRedeclareLeavesSuccessorIntact(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	if _, err := b.DeclareQueue("q", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q): %v", err)
	}
	qs := b.getOrCreateQueueState("q")

	gate := make(chan struct{})
	type g1Result struct {
		count int
		err   error
	}
	resultCh := make(chan g1Result, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-gate
		count, err := b.purgeIncarnation("q", qs)
		resultCh <- g1Result{count, err}
	}()

	if _, err := b.DeleteQueue("q", false, false); err != nil {
		t.Fatalf("DeleteQueue(q): %v", err)
	}
	if _, err := b.DeclareQueue("q", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q) redeclare: %v", err)
	}
	const n = 4
	publishConfirmedDurable(t, b, "q", n)
	qs2 := b.getOrCreateQueueState("q")
	minSucc, _ := qs2.TagBand()

	// The load-bearing observable. See the note in
	// TestPurgeIncarnationRefusesSupersededIncarnation: storage residency cannot
	// observe this defect because GetMessage falls back to the WAL, so a
	// residency-only assertion stays GREEN on the very tree this test exists to
	// keep broken. What HEAD's shape moves is the successor's dispatch cursors.
	preTail := qs2.Tail()
	preHead := qs2.Head()
	preWaiting := qs2.WaitingCount()
	if preWaiting != int64(n) {
		t.Fatalf("successor waiting=%d before the racing purge, want %d; the cursor assertions below would be vacuous", preWaiting, n)
	}

	close(gate)
	wg.Wait()
	res := <-resultCh

	if !errors.Is(res.err, interfaces.ErrQueueNotFound) {
		t.Errorf("G1 purgeIncarnation err = %v, want interfaces.ErrQueueNotFound", res.err)
	}
	if res.count != 0 {
		t.Errorf("G1 purgeIncarnation count = %d, want 0", res.count)
	}
	if got := qs2.Tail(); got != preTail {
		t.Errorf("successor tail changed: %d -> %d; the racing purge stranded the successor's confirmed durable messages", preTail, got)
	}
	if got := qs2.Head(); got != preHead {
		t.Errorf("successor head changed: %d -> %d, want unchanged", preHead, got)
	}
	if got := qs2.WaitingCount(); got != preWaiting {
		t.Errorf("successor waiting changed: %d -> %d; its confirmed durable messages are no longer claimable", preWaiting, got)
	}
	for i := uint64(0); i < uint64(n); i++ {
		tag := minSucc + i
		if _, err := b.storage.GetMessage("q", tag); err != nil {
			t.Errorf("GetMessage(q, tag=%d) = %v, want the successor's message resident after the racing purge", tag, err)
		}
	}
}
