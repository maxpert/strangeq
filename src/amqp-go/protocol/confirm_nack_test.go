package protocol

import (
	"sort"
	"sync"
	"testing"
)

// confirmEvent records one emitted publisher-confirm frame (ack or nack) so a
// test can assert on the exact stream the client would observe.
type confirmEvent struct {
	kind     string // "ack" or "nack"
	tag      uint64
	multiple bool
}

// confirmRecorder captures the ack/nack stream. Guarded so it is safe to record
// from the inline flush, the flusher goroutine, and SettleConfirmNack
// concurrently (SQ-11 race coverage).
type confirmRecorder struct {
	mu     sync.Mutex
	events []confirmEvent
}

func (r *confirmRecorder) ack(tag uint64, multiple bool) error {
	r.mu.Lock()
	r.events = append(r.events, confirmEvent{kind: "ack", tag: tag, multiple: multiple})
	r.mu.Unlock()
	return nil
}

func (r *confirmRecorder) nack(tag uint64) error {
	r.mu.Lock()
	r.events = append(r.events, confirmEvent{kind: "nack", tag: tag, multiple: false})
	r.mu.Unlock()
	return nil
}

func (r *confirmRecorder) snapshot() []confirmEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]confirmEvent, len(r.events))
	copy(out, r.events)
	return out
}

func (r *confirmRecorder) settleNack(ch *Channel, n uint64) error {
	return ch.SettleConfirmNack(n, r.ack, r.nack)
}

func (r *confirmRecorder) flush(ch *Channel) error {
	return ch.FlushConfirms(r.ack)
}

// TestSettleConfirmNack_FirstTagNoPriorConfirms verifies the simplest reject:
// the very first publish (tag 1) is rejected on a confirm channel with nothing
// pending. The client must see EXACTLY ONE nack for tag 1 and NO ack.
func TestSettleConfirmNack_FirstTagNoPriorConfirms(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.ConfirmMode.Store(true)
	rec := &confirmRecorder{}

	if err := rec.settleNack(ch, 1); err != nil {
		t.Fatalf("SettleConfirmNack: %v", err)
	}

	events := rec.snapshot()
	if len(events) != 1 || events[0] != (confirmEvent{kind: "nack", tag: 1, multiple: false}) {
		t.Fatalf("events = %+v, want single nack(1, multiple=false)", events)
	}
	// The hole at 1 is settled so a later ack starts at 2 and never covers 1.
	if got := ch.confirmAcked.Load(); got != 1 {
		t.Fatalf("confirmAcked = %d, want 1", got)
	}
	if got := ch.confirmDurable.Load(); got != 1 {
		t.Fatalf("confirmDurable = %d, want 1", got)
	}
}

// TestSettleConfirmNack_FlushesPendingBelowN verifies that durable-but-unacked
// tags strictly below the nacked tag are flushed as ONE cumulative ack BEFORE
// the nack, and the nack itself is multiple=false.
func TestSettleConfirmNack_FlushesPendingBelowN(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.ConfirmMode.Store(true)
	rec := &confirmRecorder{}

	// Tags 1..3 crossed the durability barrier but were not yet flushed (a
	// pipelining publisher). Publish 4 is then rejected.
	ch.AdvanceConfirmDurable(3)
	if err := rec.settleNack(ch, 4); err != nil {
		t.Fatalf("SettleConfirmNack: %v", err)
	}

	events := rec.snapshot()
	want := []confirmEvent{
		{kind: "ack", tag: 3, multiple: true}, // covers 1..3 in one frame
		{kind: "nack", tag: 4, multiple: false},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %+v, want %+v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("event[%d] = %+v, want %+v (full: %+v)", i, events[i], want[i], events)
		}
	}
	if got := ch.confirmAcked.Load(); got != 4 {
		t.Fatalf("confirmAcked = %d, want 4", got)
	}
	if got := ch.confirmDurable.Load(); got != 4 {
		t.Fatalf("confirmDurable = %d, want 4", got)
	}
}

// TestSettleConfirmNack_NoLaterAckCoversN is THE contiguous-watermark interlock
// assertion: after a nack for N, a later multiple=true flush must NOT re-confirm
// N. No emitted ack equals N, and the first ack after the nack has a tag > N
// (its cumulative lower bound is N+1, so it settles only N+1.., never N).
func TestSettleConfirmNack_NoLaterAckCoversN(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.ConfirmMode.Store(true)
	rec := &confirmRecorder{}

	const n = 4
	ch.AdvanceConfirmDurable(3) // 1..3 durable
	if err := rec.settleNack(ch, n); err != nil {
		t.Fatalf("SettleConfirmNack: %v", err)
	}
	// Two more publishes (5, 6) become durable and are flushed as one batch.
	ch.AdvanceConfirmDurable(6)
	if err := rec.flush(ch); err != nil {
		t.Fatalf("FlushConfirms: %v", err)
	}

	events := rec.snapshot()

	nackCount := 0
	var firstAckAfterNack *confirmEvent
	seenNack := false
	for i := range events {
		e := events[i]
		if e.kind == "nack" {
			if e.tag != n {
				t.Fatalf("unexpected nack for tag %d, want only %d", e.tag, n)
			}
			nackCount++
			seenNack = true
		}
		if e.kind == "ack" {
			if e.tag == n {
				t.Fatalf("ack covering the nacked tag %d emitted: %+v", n, events)
			}
			if seenNack && firstAckAfterNack == nil {
				ev := e
				firstAckAfterNack = &ev
			}
		}
	}
	if nackCount != 1 {
		t.Fatalf("nack count for tag %d = %d, want exactly 1 (%+v)", n, nackCount, events)
	}
	if firstAckAfterNack == nil {
		t.Fatalf("expected an ack after the nack, got %+v", events)
	}
	if firstAckAfterNack.tag <= n {
		t.Fatalf("first ack after nack = %+v, want tag > %d so it cannot re-confirm the nacked tag", *firstAckAfterNack, n)
	}
	if firstAckAfterNack.tag != 6 {
		t.Fatalf("first ack after nack = %+v, want tag 6", *firstAckAfterNack)
	}
}

// TestSettleConfirmNack_PartiallyPreAcked verifies the flush-below-N step only
// covers the still-unacked window when some tags were already acked by an
// earlier flush.
func TestSettleConfirmNack_PartiallyPreAcked(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.ConfirmMode.Store(true)
	rec := &confirmRecorder{}

	ch.AdvanceConfirmDurable(3)
	if err := rec.flush(ch); err != nil { // acks 1..3
		t.Fatalf("flush: %v", err)
	}
	ch.AdvanceConfirmDurable(5) // 4,5 durable
	if err := rec.settleNack(ch, 6); err != nil {
		t.Fatalf("SettleConfirmNack: %v", err)
	}

	events := rec.snapshot()
	want := []confirmEvent{
		{kind: "ack", tag: 3, multiple: true}, // first flush: 1..3
		{kind: "ack", tag: 5, multiple: true}, // settle flush below 6: 4..5
		{kind: "nack", tag: 6, multiple: false},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %+v, want %+v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("event[%d] = %+v, want %+v (full: %+v)", i, events[i], want[i], events)
		}
	}
}

// TestSettleConfirmNack_ClosedChannelIsNoop verifies a closed confirm channel
// emits nothing (mirrors FlushConfirms on a closed channel).
func TestSettleConfirmNack_ClosedChannelIsNoop(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.ConfirmMode.Store(true)
	rec := &confirmRecorder{}

	if err := ch.FlushAndCloseConfirms(rec.ack); err != nil {
		t.Fatalf("FlushAndCloseConfirms: %v", err)
	}
	if err := rec.settleNack(ch, 1); err != nil {
		t.Fatalf("SettleConfirmNack: %v", err)
	}
	if events := rec.snapshot(); len(events) != 0 {
		t.Fatalf("closed channel emitted %+v, want nothing", events)
	}
}

// TestSettleConfirmNack_ConcurrentFlusher mirrors production: the serial frame
// processor advances the durability watermark and one reject, while a SEPARATE
// flusher goroutine drains confirms. The rejected tag is never made durable
// (its publish failed), exactly as in the broker. Under -race the invariant
// must hold: the nacked tag appears exactly once as a nack, no ack equals it,
// and the ack stream is strictly increasing (so no cumulative ack re-confirms
// the hole).
func TestSettleConfirmNack_ConcurrentFlusher(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.ConfirmMode.Store(true)
	rec := &confirmRecorder{}

	const rejectTag = 500
	const maxTag = 1000

	wake := make(chan struct{}, 1)
	done := make(chan struct{})
	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	go func() {
		defer flusherWG.Done()
		for {
			select {
			case <-wake:
				_ = rec.flush(ch)
			case <-done:
				_ = rec.flush(ch) // final drain
				return
			}
		}
	}()

	poke := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	// Serial "frame processor": advance durable for every tag except the
	// rejected one, which is settled as a nack (never made durable).
	for tag := uint64(1); tag <= maxTag; tag++ {
		if tag == rejectTag {
			if err := rec.settleNack(ch, tag); err != nil {
				t.Fatalf("SettleConfirmNack: %v", err)
			}
			continue
		}
		ch.AdvanceConfirmDurable(tag)
		poke()
	}
	close(done)
	flusherWG.Wait()

	events := rec.snapshot()
	nackCount := 0
	var ackTags []uint64
	for _, e := range events {
		switch e.kind {
		case "nack":
			if e.tag != rejectTag {
				t.Fatalf("unexpected nack for %d, want only %d", e.tag, rejectTag)
			}
			nackCount++
		case "ack":
			if e.tag == rejectTag {
				t.Fatalf("ack equals the nacked tag %d: %+v", rejectTag, events)
			}
			ackTags = append(ackTags, e.tag)
		}
	}
	if nackCount != 1 {
		t.Fatalf("nack count = %d, want exactly 1", nackCount)
	}
	if !sort.SliceIsSorted(ackTags, func(i, j int) bool { return ackTags[i] < ackTags[j] }) {
		t.Fatalf("ack tags not strictly increasing: %v", ackTags)
	}
	for i := 1; i < len(ackTags); i++ {
		if ackTags[i] == ackTags[i-1] {
			t.Fatalf("duplicate ack tag %d (double-confirm): %v", ackTags[i], ackTags)
		}
	}
	// The final watermark must have advanced to the last durable tag.
	if got := ch.confirmAcked.Load(); got != maxTag {
		t.Fatalf("final confirmAcked = %d, want %d", got, maxTag)
	}
}
