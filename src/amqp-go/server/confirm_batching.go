package server

import (
	"sort"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// confirmFlusherSafetyInterval bounds how long an advanced-but-unflushed
// publisher-confirm watermark can wait when no ConfirmWake poke arrives (e.g. a
// missed/coalesced poke once publishing stops at teardown). In steady state the
// poke flushes immediately and resets the timer, so this adds no confirm latency
// and no hot-path cost; it only elapses on an otherwise-idle confirm connection.
const confirmFlusherSafetyInterval = 20 * time.Millisecond

// SQ-5: publisher-confirm batching.
//
// Instead of writing one basic.ack (serialize + WriteMutex + write syscall) per
// confirmed publish, each channel tracks the highest contiguous tag that has
// crossed the durability barrier in a lock-free watermark
// (protocol.Channel.AdvanceConfirmDurable). Acks are then flushed as a single
// basic.ack with multiple=true covering the whole watermark:
//
//   - inline, on the frame-processor goroutine, when the connection's
//     FrameQueue is empty (no batching opportunity — a lone publish is
//     confirmed with the same latency as the unbatched design), or
//   - by the per-connection confirmFlusher goroutine when the publisher is
//     pipelining. The flusher is poked through a capacity-1 channel, so while
//     it writes one ack every publish that completes meanwhile folds into the
//     next flush — batch size adapts automatically to how far the publisher
//     runs ahead, with no timers and nothing accumulated but a uint64.
//
// The per-publish hot path is atomics only; the flush lock inside
// protocol.Channel.FlushConfirms is taken once per emitted ack (batch
// granularity), never per message.

// confirmPublishDurable settles a confirm tag from a SYNCHRONOUS barrier
// crossing on the frame-processor goroutine — a transient-confirm publish, or
// an unroutable-mandatory publish confirmed after its basic.return. It folds
// the tag into the per-channel contiguous watermark (so on a mixed channel it
// is held behind any earlier still-pending durable tag) and arranges the ack.
//
// Iteration 2: the fold replaces the direct AdvanceConfirmDurable so that a
// synchronous transient settlement can never advance the ack watermark past a
// lower durable tag whose async fsync has not completed (invariant A1).
func (s *Server) confirmPublishDurable(conn *protocol.Connection, ch *protocol.Channel, tag uint64) error {
	w := ch.MarkConfirmTagDurable(tag)
	ch.AdvanceConfirmDurable(w)

	// Idle heuristic: nothing further queued for the processor means no
	// batching opportunity, so flush inline for minimal confirm latency. A
	// pipelining publisher keeps FrameQueue non-empty and takes the flusher
	// path, where acks coalesce. (A frame momentarily parked in the reader's
	// overflow buffer can make this flush a smaller batch than theoretically
	// possible — that is only suboptimal, never incorrect.)
	if len(conn.FrameQueue) == 0 {
		return s.flushChannelConfirms(conn, ch)
	}
	conn.WakeConfirmFlusher()
	return nil
}

// markConfirmDurable settles a confirm tag from an ASYNCHRONOUS durability
// completion — the WAL group-commit batch-writer goroutine (iteration 2). It
// folds the tag and pokes the per-connection confirm flusher to emit the
// batched ack. It MUST NOT flush inline: it runs on the shared WAL writer, and
// a socket write there would stall every connection's durability (invariant
// A4). The fold guarantees AdvanceConfirmDurable is fed a provably-contiguous
// watermark even when copies complete out of order.
func (s *Server) markConfirmDurable(conn *protocol.Connection, ch *protocol.Channel, tag uint64) {
	w := ch.MarkConfirmTagDurable(tag)
	ch.AdvanceConfirmDurable(w)
	conn.WakeConfirmFlusher()
}

// failConfirmDurable records that confirm tag `tag` on `ch` FAILED its
// durability barrier (WAL write/fsync error) and must be settled as a
// basic.nack. It enqueues the (ch, tag) on the connection's pending-nack list
// and pokes the flusher; the actual nack frame is emitted by the flusher on its
// own goroutine (socket I/O is unsafe on the WAL batch writer). Non-blocking:
// safe to call from the WAL batch-writer goroutine (invariant A4).
func (s *Server) failConfirmDurable(conn *protocol.Connection, ch *protocol.Channel, tag uint64) {
	conn.AddPendingConfirmNack(ch, tag)
	conn.WakeConfirmFlusher()
}

// resolveConfirmNack emits a settled nack for `tag` and folds it into the
// contiguous watermark. It first flushes every durable-but-unacked confirm
// strictly below `tag` as one cumulative ack and emits basic.nack(tag), then
// advances the fold past the nacked tag (pulling in any higher durable tags
// buffered out of order) and feeds the new watermark so a later flush acks
// them. The caller MUST have verified ch.ConfirmContig() >= tag-1 (every
// predecessor already durable), so the pre-nack ack covers exactly tags
// (confirmAcked, tag-1]. Runs on a goroutine where socket I/O is safe.
func (s *Server) resolveConfirmNack(conn *protocol.Connection, ch *protocol.Channel, tag uint64) error {
	if err := s.settleConfirmNack(conn, ch, tag); err != nil {
		return err
	}
	w := ch.ResolveConfirmContigNack(tag)
	ch.AdvanceConfirmDurable(w)
	return nil
}

// settleRejectConfirm settles a reject-publish (SQ-11) confirm tag. If every
// predecessor is already durable (the common case — including the all-transient
// path) it emits the nack synchronously on the frame processor, preserving the
// existing inline behavior. Otherwise an earlier durable tag on this channel is
// still pipelined (async fsync pending): the nack is deferred to the flusher,
// which emits it once the contiguous frontier reaches tag-1 so the nack never
// jumps the ack watermark past a not-yet-durable predecessor.
func (s *Server) settleRejectConfirm(conn *protocol.Connection, ch *protocol.Channel, tag uint64) error {
	if ch.ConfirmContig() >= tag-1 {
		if err := s.resolveConfirmNack(conn, ch, tag); err != nil {
			return err
		}
		if len(conn.FrameQueue) == 0 {
			return s.flushChannelConfirms(conn, ch)
		}
		conn.WakeConfirmFlusher()
		return nil
	}
	s.failConfirmDurable(conn, ch, tag)
	return nil
}

// flushChannelConfirms emits at most one basic.ack covering every
// durable-but-unacked confirm tag on ch. Ordering with basic.return is
// inherent: a returned tag's watermark is advanced only after the basic.return
// frames were written, so any ack covering it is serialized behind the return
// on the wire.
func (s *Server) flushChannelConfirms(conn *protocol.Connection, ch *protocol.Channel) error {
	return ch.FlushConfirms(func(tag uint64, multiple bool) error {
		return s.sendBasicAck(conn, ch.ID, tag, multiple)
	})
}

// settleConfirmNack settles confirm tag `tag` on `ch` as a basic.nack (SQ-11
// x-overflow=reject-publish) WITHOUT the contiguous ack watermark ever
// re-confirming it. It force-flushes the durable-but-unacked confirms strictly
// below `tag` as one cumulative basic.ack, emits
// basic.nack(tag, multiple=false, requeue=false), then advances the watermark
// past the settled hole. See protocol.Channel.SettleConfirmNack for the full
// interlock proof — this is only the server-side frame wiring.
func (s *Server) settleConfirmNack(conn *protocol.Connection, ch *protocol.Channel, tag uint64) error {
	return ch.SettleConfirmNack(tag,
		func(t uint64, multiple bool) error { return s.sendBasicAck(conn, ch.ID, t, multiple) },
		func(t uint64) error { return s.sendBasicNack(conn, ch.ID, t, false, false) },
	)
}

// confirmFlusher is the per-connection publisher-confirm flusher goroutine. It
// wakes on ConfirmWake pokes, scans the connection's channels, and flushes each
// channel's pending confirms as one batched ack. Exits when the connection's
// Done channel closes (teardown); outstanding unflushed confirms are dropped at
// that point, which is indistinguishable from the connection dying an instant
// earlier — the client's confirm wait fails with the connection error either
// way.
func (s *Server) confirmFlusher(conn *protocol.Connection, done chan struct{}) {
	defer close(done)
	var safety *time.Timer
	defer func() {
		if safety != nil {
			safety.Stop()
		}
	}()
	for {
		// Settle failed-durability nacks first, in ascending-tag order, gated on
		// the contiguous frontier so every durable predecessor is acked before the
		// nacked tag (iteration 2). Then flush each channel's durable-but-unacked
		// confirms as one batched ack.
		if err := s.drainConfirmNacks(conn); err != nil {
			s.closeOnConfirmFlushError(conn, "nack write failed", err)
			return
		}
		if err := s.flushAllChannelConfirms(conn); err != nil {
			s.closeOnConfirmFlushError(conn, "ack write failed", err)
			return
		}

		// Wait for the next durability completion's poke. On a connection that
		// uses publisher confirms, ALSO re-check on a bounded safety interval:
		// this makes confirm liveness independent of a single ConfirmWake poke, so
		// a completion whose poke is missed or coalesced away — the benchmark
		// teardown symptom, where publishing stops and no further pokes arrive —
		// can never leave an advanced confirm watermark unflushed. Steady-state
		// latency is unchanged (the poke fires the flush immediately and resets
		// the timer); the timer only elapses when the connection is otherwise
		// idle. Connections that never enable confirms block indefinitely (no
		// timer, no periodic wakeups — zero cost). This runs on the per-connection
		// flusher goroutine, off the publish/deliver hot path.
		if conn.ConfirmActive.Load() {
			if safety == nil {
				safety = time.NewTimer(confirmFlusherSafetyInterval)
			} else {
				safety.Reset(confirmFlusherSafetyInterval)
			}
			select {
			case <-conn.ConfirmWake:
				if !safety.Stop() {
					<-safety.C
				}
			case <-safety.C:
			case <-conn.Done:
				return
			}
		} else {
			select {
			case <-conn.ConfirmWake:
			case <-conn.Done:
				return
			}
		}
	}
}

// flushAllChannelConfirms flushes every confirm-mode channel's durable-but-
// unacked confirms as one batched ack each. Returns the first send error.
func (s *Server) flushAllChannelConfirms(conn *protocol.Connection) error {
	var flushErr error
	conn.Channels.Range(func(_, value interface{}) bool {
		ch := value.(*protocol.Channel)
		if !ch.ConfirmMode.Load() || !ch.HasUnflushedConfirms() {
			return true
		}
		if err := s.flushChannelConfirms(conn, ch); err != nil {
			flushErr = err
			return false
		}
		return true
	})
	return flushErr
}

// closeOnConfirmFlushError tears the connection down when a confirm ack/nack
// write fails, so the client's confirm wait fails promptly instead of idling
// until the heartbeat timeout.
func (s *Server) closeOnConfirmFlushError(conn *protocol.Connection, what string, err error) {
	s.Log.Info("confirm flusher: "+what+", closing connection",
		zap.String("connection_id", conn.ID),
		zap.Error(err))
	conn.Closed.Store(true)
	conn.Conn.Close()
}

// drainConfirmNacks emits every pending failed-durability nack whose contiguous
// frontier has caught up (ConfirmContig >= tag-1), in ascending-tag order per
// channel, and re-queues the rest for a later poke. Emitting in ascending order
// and gating on the frontier guarantees each nacked tag's durable predecessors
// are acked before it is nacked, and that a nack never advances the ack
// watermark past a not-yet-durable predecessor.
func (s *Server) drainConfirmNacks(conn *protocol.Connection) error {
	pending := conn.DrainPendingConfirmNacks()
	if len(pending) == 0 {
		return nil
	}
	// Ascending by tag so predecessors settle first (fold advances monotonically).
	sort.Slice(pending, func(i, j int) bool { return pending[i].Tag < pending[j].Tag })

	for i, pn := range pending {
		// A tag already resolved past (should not happen — a nacked tag is never
		// durable-folded) is skipped defensively.
		if pn.Ch.ConfirmContig() >= pn.Tag {
			continue
		}
		if pn.Ch.ConfirmContig() < pn.Tag-1 {
			// A durable predecessor is still pipelined; leave this and every later
			// nack pending and retry when the frontier advances (a durable
			// completion pokes the flusher).
			for _, rest := range pending[i:] {
				conn.AddPendingConfirmNack(rest.Ch, rest.Tag)
			}
			return nil
		}
		if err := s.resolveConfirmNack(conn, pn.Ch, pn.Tag); err != nil {
			return err
		}
	}
	return nil
}
