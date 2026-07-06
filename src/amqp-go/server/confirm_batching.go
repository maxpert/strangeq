package server

import (
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

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

// DurabilityBarrier abstracts the event that makes a publish confirmable to the
// publisher (SQ-5). Implementations must not return until the message has
// crossed the barrier — whatever "the barrier" means for them. The synchronous
// broker implementation below defines it as Broker.PublishMessage returning,
// which for durable messages blocks until the WAL group-commit fsync covering
// the message completes. Swapping the definition of "confirmed" (e.g. an async
// WAL-completion callback, or replication) means providing another
// implementation and delivering the completion event to
// Server.confirmPublishDurable — the watermark/flush machinery is already safe
// to drive from any goroutine (see the contiguity contract on
// protocol.Channel.AdvanceConfirmDurable).
type DurabilityBarrier interface {
	// PublishAndAwaitDurable routes the message and returns once it is safe to
	// confirm to the publisher. A broker.ErrNoRoute return is NOT a barrier
	// crossing: the caller must send basic.return first and then report the tag
	// durable itself (unroutable mandatory messages are confirmed after their
	// basic.return per AMQP semantics).
	PublishAndAwaitDurable(exchange, routingKey string, message *protocol.Message) error
}

// brokerSyncBarrier is today's barrier definition: Broker.PublishMessage
// returning IS the durability barrier.
type brokerSyncBarrier struct {
	broker UnifiedBroker
}

func (b brokerSyncBarrier) PublishAndAwaitDurable(exchange, routingKey string, message *protocol.Message) error {
	return b.broker.PublishMessage(exchange, routingKey, message)
}

// confirmPublishDurable is the barrier-crossing completion hook: it records
// that confirm tag `tag` on `ch` is durable and arranges for the ack to reach
// the client. Called on the frame-processor goroutine today (synchronous
// barrier); safe from any goroutine as long as per-channel tags arrive in
// order.
func (s *Server) confirmPublishDurable(conn *protocol.Connection, ch *protocol.Channel, tag uint64) error {
	ch.AdvanceConfirmDurable(tag)

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

// confirmFlusher is the per-connection publisher-confirm flusher goroutine. It
// wakes on ConfirmWake pokes, scans the connection's channels, and flushes each
// channel's pending confirms as one batched ack. Exits when the connection's
// Done channel closes (teardown); outstanding unflushed confirms are dropped at
// that point, which is indistinguishable from the connection dying an instant
// earlier — the client's confirm wait fails with the connection error either
// way.
func (s *Server) confirmFlusher(conn *protocol.Connection, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-conn.ConfirmWake:
		case <-conn.Done:
			return
		}

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
		if flushErr != nil {
			// The socket is broken; accelerate teardown so the client's confirm
			// wait fails promptly instead of idling until heartbeat timeout.
			s.Log.Info("confirm flusher: ack write failed, closing connection",
				zap.String("connection_id", conn.ID),
				zap.Error(flushErr))
			conn.Closed.Store(true)
			conn.Conn.Close()
			return
		}
	}
}
