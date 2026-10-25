package server

import (
	"encoding/binary"
	"fmt"
	"time"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// WS-I Phase B: header-time fair publish lease — reader/handler wiring.
//
// The lease bounds DECLARED publish body bytes (plus fixed framing) from the
// content-header read until the broker reports every accepted durable copy
// complete (or a synchronous path returns, or teardown discards the publish).
// The strict multiplexed-connection rule (spec §B.3a):
//
//   - A blocking Reserve is allowed ONLY when no earlier leased body is still
//     unread on this connection (owed == 0).
//   - Otherwise the reader uses a non-barging TryReserve; if it cannot grant,
//     the connection is closed promptly with 506 RESOURCE_ERROR and the nested
//     content is NEVER admitted unleased.
//   - While a leased body is owed, the read deadline is
//     min(hbDeadline, oldest(lastProgress) + writeTimeout); heartbeats and
//     frames on other channels do NOT extend it, and the alarm/depth parks are
//     deferred so a parked reader never holds an unread leased body.

// publishLimiter returns the server's global publish-inflight lease limiter,
// building it lazily so direct-constructed test servers (no Build) get the
// configured behavior too. The budget comes from
// Config.PublishInflightBudget(): 0 → 4 × WAL batch seal (256 MiB), −1 →
// disabled, >0 → explicit.
func (s *Server) publishLimiter() *PublishLimiter {
	s.limiterOnce.Do(func() {
		budget := int64(-1) // no config → disabled
		if s.Config != nil {
			budget = s.Config.PublishInflightBudget()
		}
		l := NewPublishLimiter(budget)
		l.hooks = s.buildLimiterHooks(l)
		s.limiter = l
		// Wire the PutPendingMessage lease-leak recovery counter (process-wide
		// hook; last server wins — harmless for multi-server tests). Resolved
		// at call time so a collector installed after Build still reports.
		protocol.SetLeaseLeakReporter(func() {
			if lm := s.leaseMetrics(); lm != nil {
				lm.RecordPublishLeaseLeaked()
			}
		})
	})
	return s.limiter
}

// buildLimiterHooks wires the limiter's observation callbacks to metrics and
// the test-only step hook and created/released counters. The hooks consult
// the server fields AT CALL TIME: they are set before Start (production:
// during Build) and never change while traffic flows, so the reads are
// stable; the per-call nil checks keep the production cost at a few branches
// per grant/release.
func (s *Server) buildLimiterHooks(l *PublishLimiter) *LimiterHooks {
	return &LimiterHooks{
		BeforeReserve: func(connID string, bytes int64) {
			if h := s.limiterStepHook; h != nil {
				h("reserve:" + connID)
			}
		},
		OnWait: func(d time.Duration) {
			if lm := s.leaseMetrics(); lm != nil {
				lm.RecordPublishLeaseWait(d.Seconds())
			}
		},
		OnGrant: func(connID string, bytes int64, oversized bool, used int64, waiters int) {
			if c := s.leaseCreated; c != nil {
				c.Add(1)
			}
			if lm := s.leaseMetrics(); lm != nil {
				if oversized {
					lm.RecordPublishLeaseOversized()
				}
				lm.UpdatePublishLeaseGauges(used, l.budget, waiters)
			}
			if h := s.limiterStepHook; h != nil {
				h("grant:" + connID)
			}
		},
		OnRelease: func(bytes int64, used int64, waiters int) {
			if c := s.leaseReleased; c != nil {
				c.Add(1)
			}
			if lm := s.leaseMetrics(); lm != nil {
				lm.UpdatePublishLeaseGauges(used, l.budget, waiters)
			}
		},
	}
}

// owedBody is the reader-local progress tracker for one leased header whose
// body bytes have not all been read yet (spec §B.3a).
type owedBody struct {
	remaining    uint64
	lastProgress time.Time
}

// takeLease nil-safely transfers the publish lease out of pm: it sets
// pm.Lease = nil and returns the lease's pre-bound ReleaseFunc, or the shared
// noopRelease when there was none. A second take returns noopRelease.
func takeLease(pm *protocol.PendingMessage) func() {
	if pm == nil || pm.Lease == nil {
		return noopRelease
	}
	fn := pm.Lease.ReleaseFunc()
	pm.Lease = nil
	if fn == nil {
		return noopRelease
	}
	return fn
}

// releaseHeaderLease releases a leftover processor-owned HeaderLease (a header
// that was discarded or errored before its lease transferred to a
// PendingMessage) and clears the slot.
func releaseHeaderLease(conn *protocol.Connection) {
	if conn.HeaderLease != nil {
		conn.HeaderLease.Release()
		conn.HeaderLease = nil
	}
}

// frameMaxForLease returns the negotiated frame_max for the leaseBytes
// framing estimate, falling back to the protocol default when unset.
func frameMaxForLease(conn *protocol.Connection) uint32 {
	if conn.MaxFrameSize > 0 {
		return conn.MaxFrameSize
	}
	return protocol.MaxInboundFrameSize
}

// leaseForHeaderFrame applies the WS-I Phase B header-time lease rule for a
// just-read content-header frame and returns whether the reader may continue
// (the frame is enqueued normally then). A false return means the reader must
// exit AT ONCE: either the wait was cancelled (teardown / write failure /
// limiter close), or a nested header could not be leased and the connection
// is being closed with 506 — the nested content is never admitted.
//
// Pairing contract: on a true return EXACTLY one entry (lease or nil) has
// been pushed to conn.LeaseFIFO for this frame, so the processor's Pop pairs
// 1:1 with enqueued header frames.
func (s *Server) leaseForHeaderFrame(rt *connRuntime, frame *protocol.Frame, owed map[uint16]*owedBody) bool {
	conn := rt.conn

	// Decode the declared body size from payload bytes [4:12] (class-id,
	// weight, then body-size). A short payload — or a declared size over
	// MaxMessageSize — pairs a nil lease: the processor produces the existing
	// malformed/406 outcome and its body frames are discarded by the existing
	// channel-close gates, never entering the broker.
	valid := false
	var bodySize uint64
	if len(frame.Payload) >= 12 {
		bodySize = binary.BigEndian.Uint64(frame.Payload[4:12])
		// EXACTLY the processor's admission condition (basic_handlers.go):
		// a nil config skips the check; a non-positive MaxMessageSize rejects
		// any non-empty body.
		valid = s.Config == nil || bodySize <= uint64(s.Config.Server.MaxMessageSize)
	}
	if !valid {
		conn.LeaseFIFO.Push(nil)
		return true
	}

	bytes := leaseBytes(bodySize, frameMaxForLease(conn))
	limiter := s.publishLimiter()

	var lease *Lease
	if len(owed) == 0 {
		// No earlier leased body is still unread: a blocking Reserve is
		// allowed. The wait is bounded by leaseCtx (write failure / teardown),
		// the heartbeat writer (hb > 0), WAL progress, or limiter.Close.
		if s.limiterStepHook != nil {
			s.limiterStepHook("reserve-wait:" + conn.ID)
		}
		le, err := limiter.Reserve(rt.leaseCtx, conn.ID, bytes)
		if s.limiterStepHook != nil {
			s.limiterStepHook("reserve-return:" + conn.ID)
		}
		if err != nil {
			// Teardown or limiter close: exit without enqueueing the frame.
			// The FIFO gets NO entry (the processor never sees this frame);
			// outstanding leases release at teardown.
			protocol.PutFrame(frame)
			return false
		}
		lease = le
	} else {
		// An earlier leased body is still unread: non-barging TryReserve only.
		le, ok := limiter.TryReserve(conn.ID, bytes)
		if !ok {
			// Close the connection promptly with 506; the nested content is
			// NEVER admitted unleased. Teardown releases the FIFO, the
			// HeaderLease and the pending-message leases.
			if lm := s.leaseMetrics(); lm != nil {
				lm.RecordPublishLeaseOverbudgetClose()
			}
			channel := frame.Channel
			s.Log.Warn("publish lease budget exhausted while body owed; closing connection",
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", channel),
				zap.Uint16("owed_channels", uint16(len(owed))))
			s.goConn(rt, "lease-close", func() {
				_ = s.connectionError(conn, amqperrors.ResourceError,
					fmt.Sprintf("RESOURCE_ERROR - publish lease budget exhausted while body owed on channel %d", channel),
					60, 40)
				conn.Closed.Store(true)
				_ = conn.Conn.Close()
			})
			protocol.PutFrame(frame)
			return false
		}
		lease = le
	}

	conn.LeaseFIFO.Push(lease)
	if bodySize > 0 {
		// bodySize == 0 takes the lease but owes nothing (the message
		// completes at the header).
		owed[frame.Channel] = &owedBody{remaining: bodySize, lastProgress: time.Now()}
	}
	return true
}

// trackBodyProgress records read progress against the owed leased body on the
// frame's channel. Heartbeats and frames on other channels do NOT count (the
// owed deadline is only extended by body bytes on the owing channel).
func (s *Server) trackBodyProgress(frame *protocol.Frame, owed map[uint16]*owedBody) {
	ob, ok := owed[frame.Channel]
	if !ok {
		return
	}
	n := uint64(len(frame.Payload))
	if n >= ob.remaining {
		// Overrun (more body bytes than declared) is rejected downstream by
		// the existing processor check; the owed entry is done either way.
		delete(owed, frame.Channel)
		return
	}
	ob.remaining -= n
	ob.lastProgress = time.Now()
}

// oldestOwedProgress returns the oldest lastProgress across owed bodies; the
// owed read deadline is oldest + writeTimeout.
func oldestOwedProgress(owed map[uint16]*owedBody) time.Time {
	var oldest time.Time
	for _, ob := range owed {
		if oldest.IsZero() || ob.lastProgress.Before(oldest) {
			oldest = ob.lastProgress
		}
	}
	return oldest
}
