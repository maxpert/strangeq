package server

import (
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// defaultFrameQueueByteBudget bounds the bytes buffered in a connection's
// FrameQueue (§9). Small-message traffic (a publish is three frames under
// 1KiB each) fills all 10,000 slots at roughly 10MiB, so the budget never
// binds on the hot path; it binds only on large-frame floods, which is the
// memory risk. Follow-up for WS-E: optional config knob (D2).
const defaultFrameQueueByteBudget = 32 << 20 // 32 MiB

// maxAckFramePayload bounds a basic.ack/nack/reject payload (exactly
// 4+8+1 = 13 bytes on the wire). An "ack" is classified by class/method only,
// so without this a malicious 128KiB ack payload would be accepted into the
// 4096-slot AckQueue (512MiB per connection).
const maxAckFramePayload = 64

// Server field (append-only): frameQueueByteBudgetOverride int64 // tests
func (s *Server) frameQueueByteBudget() int64 {
	if s.frameQueueByteBudgetOverride > 0 {
		return s.frameQueueByteBudgetOverride
	}
	return defaultFrameQueueByteBudget
}

// frameCost is the budget charge for a queued frame: payload plus ~64 bytes
// for the *Frame struct and channel bookkeeping.
func frameCost(f *protocol.Frame) int64 { return int64(f.Size) + 64 }

// frameBudgetAllows always admits into an EMPTY queue so a single frame larger
// than the budget can never deadlock.
func (s *Server) frameBudgetAllows(conn *protocol.Connection, cost int64) bool {
	q := conn.FrameQueueBytes.Load()
	return q <= 0 || q+cost <= s.frameQueueByteBudget()
}

// releaseFrameBudget returns cost to the budget after the processor finishes
// with a frame, and pokes FrameBudgetWake whenever the queue is below budget
// afterwards so a budget-parked reader re-evaluates.
//
// DEVIATION from spec §9.2 (documented): the spec pokes only on the downward
// crossing of the budget (n < b && n+cost >= b). That misses the common park
// case — the reader parks when q > 0 && q+cost > b, which happens with q
// already BELOW b (e.g. 3×64KiB queued under a 256KiB budget: 196800 < 262144,
// but the 4th frame's 262400 > 262144). Under the spec's crossing condition
// the poke would never fire in exactly that case and a budget-parked reader
// would sleep until the probe or conn.Done. Poking on every below-budget
// release is one cap-1 non-blocking send per completed frame — far below the
// per-frame channel operations the processor already performs.
func (s *Server) releaseFrameBudget(conn *protocol.Connection, cost int64) {
	n := conn.FrameQueueBytes.Add(-cost)
	if n < s.frameQueueByteBudget() {
		select {
		case conn.FrameBudgetWake <- struct{}{}:
		default:
		}
	}
}

// admitOversizedAckFrame reports whether an ack-classified frame is within the
// ack payload bound. Oversized frames hard-close the connection with no
// connection.close frame (the reader must not write), the same pattern as the
// reader-overflow hard cap.
func (s *Server) admitOversizedAckFrame(conn *protocol.Connection, frame *protocol.Frame) bool {
	if len(frame.Payload) > maxAckFramePayload {
		s.Log.Warn("oversized ack frame; closing connection",
			zap.String("connection_id", conn.ID),
			zap.Int("payload_bytes", len(frame.Payload)))
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordConnectionError("oversized_ack")
		}
		conn.Closed.Store(true)
		_ = conn.Conn.Close()
		return false
	}
	return true
}
