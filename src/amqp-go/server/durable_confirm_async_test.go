package server

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// highestConfirmAck drains basic.ack frames from frameCh until the cumulative
// confirm watermark reaches want, or the deadline elapses. Returns the highest
// tag acked. basic.ack with multiple=true covers all tags <= DeliveryTag, so the
// highest DeliveryTag seen is the confirm watermark.
func highestConfirmAck(t *testing.T, frameCh chan *protocol.Frame, want uint64, within time.Duration) uint64 {
	t.Helper()
	deadline := time.After(within)
	var maxAck uint64
	for maxAck < want {
		select {
		case frame, ok := <-frameCh:
			if !ok {
				return maxAck
			}
			if frame.Type != protocol.FrameMethod || len(frame.Payload) < 4 {
				continue
			}
			classID := binary.BigEndian.Uint16(frame.Payload[0:2])
			methodID := binary.BigEndian.Uint16(frame.Payload[2:4])
			if classID == 60 && methodID == 80 { // basic.ack
				ack := &protocol.BasicAckMethod{}
				if err := ack.Deserialize(frame.Payload[4:]); err == nil && ack.DeliveryTag > maxAck {
					maxAck = ack.DeliveryTag
				}
			}
		case <-deadline:
			return maxAck
		}
	}
	return maxAck
}

// TestConfirmFlusher_TailAckedAfterPublishStops (teardown confirm-liveness).
//
// Reproduces the benchmark regression: on a confirm channel, publish N
// persistent messages with the WAL fsync gated (so NO completions/pokes fire and
// the flusher goes idle), then STOP publishing and release ALL completions in a
// burst. Every tag MUST be acked within a bounded time — a stranded tail (the
// flusher never re-scans after the final advance) fails here.
func TestConfirmFlusher_TailAckedAfterPublishStops(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	defer once.Do(func() { close(release) })

	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "tail-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	const n = 400
	for i := 0; i < n; i++ {
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "tail-q", "m")))
	}

	// Publishing has STOPPED. Release all WAL completions at once — the confirm
	// flusher's only remaining wakeups are the completion pokes.
	once.Do(func() { close(release) })

	got := highestConfirmAck(t, frameCh, n, 5*time.Second)
	require.Equal(t, uint64(n), got, "confirm tail stranded: acked up to %d of %d after publishing stopped", got, n)
}

// TestConfirmFlusher_TailAckedUnderTrickleCompletion (teardown liveness, harder
// timing): completions arrive one at a time with the flusher going IDLE between
// each — the exact sleep/wake oscillation at benchmark teardown. Releases WAL
// fsyncs one per token so batches complete singly, then asserts the full tail is
// acked. Any lost wakeup (advance whose poke never re-scans the channel) strands.
func TestConfirmFlusher_TailAckedUnderTrickleCompletion(t *testing.T) {
	release := make(chan struct{})
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release // one token per group-commit fsync call
		return nil
	})
	defer restore()
	// Unblock any stragglers on teardown so the WAL writer can stop.
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

	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "trickle-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	const n = 120
	for i := 0; i < n; i++ {
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "trickle-q", "m")))
	}

	// Trickle fsync completions, letting the flusher settle (and likely block on
	// ConfirmWake) between each release — this is the teardown oscillation.
	go func() {
		for {
			select {
			case release <- struct{}{}:
				time.Sleep(200 * time.Microsecond)
			case <-conn.Done:
				return
			}
		}
	}()

	got := highestConfirmAck(t, frameCh, n, 8*time.Second)
	require.Equal(t, uint64(n), got, "confirm tail stranded under trickle: acked up to %d of %d", got, n)
}

// TestConfirmFlusher_FlushesWithoutFreshPoke (teardown liveness — the core gap).
//
// The confirm flusher must NOT depend solely on a ConfirmWake poke: if a durable
// completion advances the confirm watermark but its wakeup is missed/coalesced
// (the benchmark teardown symptom — a stranded in-flight window once publishing
// and pokes stop), the flusher must still flush the outstanding confirm within a
// bounded time. This drives the missing safety re-check: it advances the
// watermark AFTER the flusher has gone idle, WITHOUT poking, and requires the ack
// to still reach the client. Fails on a poke-only flusher (blocks forever).
func TestConfirmFlusher_FlushesWithoutFreshPoke(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	ch := sq5Setup(t, srv, conn, frameCh, "nopoke-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	// Let the flusher run its initial pass and settle into its idle wait.
	time.Sleep(80 * time.Millisecond)

	// Advance the confirm watermark as a durable completion would — but do NOT
	// poke the flusher (simulating a missed/coalesced wakeup at teardown).
	const tag = uint64(7)
	ch.AdvanceConfirmDurable(tag)

	got := highestConfirmAck(t, frameCh, tag, 3*time.Second)
	require.Equal(t, tag, got, "flusher must flush an advanced confirm watermark even without a fresh poke (got %d)", got)
}

// TestConfirm_GenericRouteErrorDoesNotStallFold (fold-stall-on-error guard).
//
// A confirm publish that fails with a GENERIC error (neither ErrNoRoute nor
// ErrMaxLengthExceeded — e.g. an unknown exchange) still consumed a confirm
// sequence number. If that tag is never settled, the per-channel contiguous
// confirm fold wedges at it: every LATER confirm on the channel buffers in
// confirmAhead behind the un-settled hole and can never be acked. This publishes
// a doomed tag (unknown exchange) followed by a good durable publish and requires
// the good tag to still be confirmed — proving the fold advanced past the failed
// tag. Fails if processCompleteMessage returns the generic error without settling
// the confirm tag.
func TestConfirm_GenericRouteErrorDoesNotStallFold(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "fold-stall-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	// Tag 1: publish to a NON-EXISTENT exchange. The broker returns a generic
	// "exchange not found" error (not ErrNoRoute / ErrMaxLengthExceeded), so this
	// takes the generic-error path in processCompleteMessage. The publish MUST
	// fail, but the confirm tag it consumed must be settled so it cannot wedge the
	// fold.
	err := srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("no-such-exchange", "k", "doomed"))
	require.Error(t, err, "publish to unknown exchange must fail")

	// Tag 2: a well-formed durable publish to the declared queue.
	require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "fold-stall-q", "good")))

	// Tag 2 must be confirmed within a bounded time. If tag 1 stranded the fold,
	// tag 2 stays buffered in confirmAhead forever and this times out at 0.
	got := highestConfirmAck(t, frameCh, 2, 3*time.Second)
	require.GreaterOrEqual(t, got, uint64(2),
		"confirm fold wedged at the failed tag: watermark only reached %d, tag 2 never confirmed", got)
}

// Iteration 2 — async durable + publisher-confirm wiring (server layer).
//
// Guards, end to end through the real publish handler + confirm flusher:
//   A1 — no basic.ack for a confirm tag until its WAL fsync has completed
//        (test 3); an fsync error produces a basic.nack(requeue=false), never an
//        ack, and the copy is not made visible (test 6).

// TestDurablePublish_OptsIntoAlarmBackstop (iteration 2, option X — the durable
// admission path is bounded by the SQ-12 resource alarm, not the transient HWM).
//
// Option X removes the durable path's per-queue depth park (it stranded confirm
// tags at teardown), so the ONLY backpressure on a durable flood is the SQ-12
// resource-alarm reader-pause — which pauses a connection's reader ONLY once the
// connection is marked HasPublished. This proves a durable publish sets that
// flag, so a durable producer is subject to the alarm backstop (whose pause /
// resume / liveness behaviour is proven by reader_pause_test.go). Without the
// flag the reader-pause would never engage and a durable flood would grow disk
// unbounded even under an active alarm.
func TestDurablePublish_OptsIntoAlarmBackstop(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declPayload, err := (&protocol.QueueDeclareMethod{Queue: "backstop-q", Durable: true}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleQueueDeclare(conn, 1, declPayload))
	drainFrame(t, frameCh) // queue.declare-ok

	require.False(t, conn.HasPublished.Load(), "fresh connection is consumer-only until it publishes")

	require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "backstop-q", "m")))

	require.True(t, conn.HasPublished.Load(),
		"a durable publish must mark the connection HasPublished so the SQ-12 alarm reader-pause backstop (option X's disk/RAM bound) applies to durable producers")
}

// makeDurablePendingMessage builds a persistent (DeliveryMode=2) basic.publish
// so processCompleteMessage takes the async durable path.
func makeDurablePendingMessage(exchange, routingKey, body string) *protocol.PendingMessage {
	pm := makePendingMessage(exchange, routingKey, body)
	pm.Header.DeliveryMode = 2
	return pm
}

// TestConfirm_NotSentBeforeFsync (TDD test 3, guards A1).
//
// With the WAL group-commit fsync gated open, a durable + confirm publish must
// NOT produce a basic.ack on the wire; the ack appears only after the fsync is
// released (the confirm is driven by the fsync completion, not by the publish
// handler returning).
func TestConfirm_NotSentBeforeFsync(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	defer once.Do(func() { close(release) })

	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "dc-nofsync-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	// Durable + confirm publish: the handler returns immediately (durableInflight),
	// the confirm is deferred to the fsync completion.
	require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "dc-nofsync-q", "m")))

	// No ack may appear while the fsync is gated open.
	assertNoFrame(t, frameCh)

	// Release the durability barrier: exactly one basic.ack(1) must now appear.
	once.Do(func() { close(release) })
	tag, multiple := decodeAck(t, frameCh)
	assert.Equal(t, uint64(1), tag, "confirm must be for tag 1 after fsync")
	assert.False(t, multiple, "a single confirm must not claim multiple")
}

// TestFsyncError_Nacks (TDD test 6).
//
// An injected WAL fsync error must settle the confirm as basic.nack(tag,
// multiple=false, requeue=false) and NEVER emit an ack for the tag.
func TestFsyncError_Nacks(t *testing.T) {
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		return errors.New("injected fsync failure")
	})
	defer restore()

	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "dc-fsyncerr-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "dc-fsyncerr-q", "m")))

	// The failed durability barrier must yield exactly one basic.nack, no ack.
	classID, methodID, payload := nextMethodFrame(t, frameCh)
	require.Equal(t, uint16(60), classID, "expected class basic")
	require.Equal(t, uint16(120), methodID, "fsync error must produce basic.nack")
	nack := &protocol.BasicNackMethod{}
	require.NoError(t, nack.Deserialize(payload))
	assert.Equal(t, uint64(1), nack.DeliveryTag)
	assert.False(t, nack.Multiple, "the nack settles only its own tag")
	assert.False(t, nack.Requeue, "a message that never became durable is not requeued")

	// No ack may accompany or follow the nack.
	assertNoFrame(t, frameCh)
}
