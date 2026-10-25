package server

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
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

// readerState is the reader goroutine's working state (WS-I Phase B), living
// on the connRuntime so the processor can clear owed entries on a
// server-initiated channel close. pending/pendingBytes are READER-OWNED (no
// lock); owed is guarded by owedMu; owedCount is the lock-free owed==0 check
// for the reader's hot loop.
type readerState struct {
	pending      []*protocol.Frame
	pendingBytes int64

	owedMu    sync.Mutex
	owed      map[uint16]owedBody // values, not pointers
	owedCount atomic.Int32
}

// owedBody is the progress tracker for one leased header whose body bytes
// have not all been read yet (spec §B.3a).
type owedBody struct {
	remaining    uint64
	lastProgress time.Time
}

func (rs *readerState) owedLen() int { return int(rs.owedCount.Load()) }

func (rs *readerState) addOwed(channel uint16, bodySize uint64) {
	rs.owedMu.Lock()
	if rs.owed == nil {
		rs.owed = make(map[uint16]owedBody)
	}
	rs.owed[channel] = owedBody{remaining: bodySize, lastProgress: time.Now()}
	rs.owedCount.Store(int32(len(rs.owed)))
	rs.owedMu.Unlock()
}

func (rs *readerState) clearOwed(channel uint16) {
	rs.owedMu.Lock()
	delete(rs.owed, channel)
	rs.owedCount.Store(int32(len(rs.owed)))
	rs.owedMu.Unlock()
}

// trackBody records read progress against the owed leased body on the frame's
// channel. Heartbeats, frames on other channels, and ZERO-LENGTH body frames
// do NOT count: only positive body bytes extend the owed deadline. Exact
// completion removes the entry; overrun is rejected downstream by the
// existing processor check (the entry is removed either way).
func (rs *readerState) trackBody(frame *protocol.Frame) {
	if rs.owedCount.Load() == 0 {
		return
	}
	n := uint64(len(frame.Payload))
	if n == 0 {
		return // zero-length body frames are never progress
	}
	rs.owedMu.Lock()
	ob, ok := rs.owed[frame.Channel]
	if ok {
		if n >= ob.remaining {
			delete(rs.owed, frame.Channel)
		} else {
			ob.remaining -= n
			ob.lastProgress = time.Now()
			rs.owed[frame.Channel] = ob
		}
		rs.owedCount.Store(int32(len(rs.owed)))
	}
	rs.owedMu.Unlock()
}

// oldestProgress returns the oldest lastProgress across owed bodies; the owed
// read deadline is oldest + writeTimeout.
func (rs *readerState) oldestProgress() time.Time {
	rs.owedMu.Lock()
	defer rs.owedMu.Unlock()
	var oldest time.Time
	for _, ob := range rs.owed {
		if oldest.IsZero() || ob.lastProgress.Before(oldest) {
			oldest = ob.lastProgress
		}
	}
	return oldest
}

// joinRelease is the WS-I two-party publish-lease retention (review item 5):
// the lease releases only when BOTH the broker part (onReleased — for a
// durable publish, WAL completion; for a synchronous path, the broker call's
// return) AND the server part (the deferred completion of
// processCompleteMessage, covering basic.return/nack/error wire writes) are
// done. The release fires exactly once; duplicate signals are ignored.
type joinRelease struct {
	bits atomic.Int32 // 1 = brokerDone, 2 = serverDone
	fn   func()
}

func newJoinRelease(fn func()) *joinRelease { return &joinRelease{fn: fn} }

func (j *joinRelease) bit(b int32) {
	if j == nil || j.fn == nil {
		return
	}
	for {
		old := j.bits.Load()
		if old == 3 {
			return // already released — duplicate signals ignored
		}
		if j.bits.CompareAndSwap(old, old|b) {
			if old|b == 3 {
				j.fn()
			}
			return
		}
	}
}

// brokerDone marks the broker part complete (wired as onReleased).
func (j *joinRelease) brokerDone() { j.bit(1) }

// serverDone marks the server part complete (deferred over the publish's
// return/nack/error wire writes).
func (j *joinRelease) serverDone() { j.bit(2) }

// releaseNow forces both parts (used when the publish leaves the lease's
// scope WITHOUT waiting for either party — e.g. a transient publish that
// waits on queue capacity: no lease is ever held across such a wait).
func (j *joinRelease) releaseNow() { j.bit(3) }

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
// exit AT ONCE: the wait was aborted (teardown / limiter close / processor
// stall), or a nested header could not be leased and the connection is being
// closed with 506 — the nested content is never admitted.
//
// Pairing contract: on a true return EXACTLY one entry (lease or nil) has
// been pushed to conn.LeaseFIFO for this frame, so the processor's Pop pairs
// 1:1 with enqueued header frames.
func (s *Server) leaseForHeaderFrame(rt *connRuntime, frame *protocol.Frame, rs *readerState) bool {
	conn := rt.conn

	// While the connection is closing, never reserve and NEVER wait: pair a
	// nil lease (the closing gate discards the frame) and keep reading so
	// the client's close-ok is seen.
	if rt.connClosing.Load() {
		conn.LeaseFIFO.Push(nil)
		return true
	}

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
	if rs.owedLen() == 0 {
		// No earlier leased body is still unread: a blocking wait is allowed.
		// It is bounded by leaseCtx (write failure / teardown / connection
		// close), the heartbeat writer (hb > 0), WAL progress, the
		// processor-stall guard, or limiter.Close.
		if s.limiterStepHook != nil {
			s.limiterStepHook("reserve-wait:" + conn.ID)
		}
		le, wait, err := limiter.Begin(conn.ID, bytes)
		if err != nil {
			if s.limiterStepHook != nil {
				s.limiterStepHook("reserve-return:" + conn.ID)
			}
			protocol.PutFrame(frame)
			return false
		}
		if wait != nil {
			var ok bool
			le, ok = s.waitForHeaderLease(rt, frame.Channel, wait, rs)
			if s.limiterStepHook != nil {
				s.limiterStepHook("reserve-return:" + conn.ID)
			}
			if !ok {
				protocol.PutFrame(frame)
				return false
			}
			if le == nil {
				// connClosing: pair a nil lease and keep reading close-ok.
				conn.LeaseFIFO.Push(nil)
				return true
			}
		} else if s.limiterStepHook != nil {
			s.limiterStepHook("reserve-return:" + conn.ID)
		}
		lease = le
	} else {
		// An earlier leased body is still unread: non-barging TryReserve only.
		le, ok := limiter.TryReserve(conn.ID, bytes)
		if !ok {
			if limiter.Closed() {
				// Shutdown raced us — exit quietly (NOT an overbudget close).
				protocol.PutFrame(frame)
				return false
			}
			// Close the connection promptly with 506; the nested content is
			// NEVER admitted unleased. Teardown releases the FIFO, the
			// HeaderLease and the pending-message leases.
			s.leaseClose506(rt, frame.Channel,
				fmt.Sprintf("RESOURCE_ERROR - publish lease budget exhausted while body owed on channel %d", frame.Channel),
				"publish lease budget exhausted while body owed; closing connection", rs.owedLen())
			protocol.PutFrame(frame)
			return false
		}
		lease = le
	}

	conn.LeaseFIFO.Push(lease)
	if bodySize > 0 && !limiter.disabled() {
		// bodySize == 0 takes the lease but owes nothing (the message
		// completes at the header). A disabled limiter skips owed tracking
		// entirely (the pre-WS-I baseline).
		rs.addOwed(frame.Channel, bodySize)
	}
	return true
}

// waitForGateDrain waits (processor-side) for a depth gate to drop below the
// high-water mark, or for the connection to die. Polling on the reader
// backpressure probe interval; never blocks the reader itself.
func (s *Server) waitForGateDrain(conn *protocol.Connection, gate protocol.DepthGate) bool {
	t := time.NewTimer(s.readerBackpressureProbeInterval())
	defer t.Stop()
	for gate.AtHighWaterMark() {
		select {
		case <-conn.Done:
			return false
		case <-t.C:
			t.Reset(s.readerBackpressureProbeInterval())
		}
	}
	return true
}

// revokeAndDrainConnectionLeases releases every publish lease the connection
// still owns — header leases queued for the processor, the in-flight header
// lease, and leases held by half-received publishes. Once-only (the
// leasesRevoked flag also tells the processor to discard further header/body
// content instead of admitting it unleased). Broker-owned transferred leases
// are NOT touched: they release independently on WAL completion.
func (s *Server) revokeAndDrainConnectionLeases(rt *connRuntime) {
	conn := rt.conn
	rt.leasesDrainOnce.Do(func() {
		for _, le := range conn.LeaseFIFO.Drain() {
			if le != nil {
				le.Release()
			}
		}
		releaseHeaderLease(conn)
		for _, pm := range conn.PendingMessages {
			takeLease(pm)()
		}
	})
}

// bodyStallClose closes a connection whose leased publish body made no
// progress within writeTimeout of its last body byte. This is a body stall,
// NOT a heartbeat death: the close is accounted separately and teardown
// releases every lease the connection owns.
func (s *Server) bodyStallClose(conn *protocol.Connection, rs *readerState) {
	if lm := s.leaseMetrics(); lm != nil {
		lm.RecordPublishLeaseBodyStallClose()
	}
	s.Log.Warn("leased publish body stalled; closing connection",
		zap.String("connection_id", conn.ID),
		zap.Int("owed_channels", rs.owedLen()))
	conn.Closed.Store(true)
}

// leaseClose506 makes a best-effort connection.close 506 under writeTimeout,
// then closes the socket; the reader returns at once.
func (s *Server) leaseClose506(rt *connRuntime, channel uint16, wireReason, logReason string, owedChannels int) {
	conn := rt.conn
	if lm := s.leaseMetrics(); lm != nil {
		lm.RecordPublishLeaseOverbudgetClose()
	}
	s.Log.Warn(logReason,
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel", channel),
		zap.Int("owed_channels", owedChannels))
	s.goConn(rt, "lease-close", func() {
		_ = s.connectionError(conn, amqperrors.ResourceError, wireReason, 60, 40)
		conn.Closed.Store(true)
		_ = conn.Conn.Close()
	})
}

// waitForHeaderLease waits for a queued lease reservation WITHOUT reading the
// socket. While waiting it keeps the SQ-13 hand-off alive: buffered (pending)
// frames are handed to processFrames whenever the frame budget allows, so a
// full FrameQueue plus a parked processor can never wedge behind the wait.
// Returns (lease, true) on grant, (nil, true) when the connection is closing
// (the caller pairs a nil lease and keeps reading close-ok), or (_, false)
// when the reader must exit (teardown, limiter close, or a processor stall).
func (s *Server) waitForHeaderLease(rt *connRuntime, channel uint16, wait *LeaseWait, rs *readerState) (*Lease, bool) {
	conn := rt.conn
	stall := time.NewTimer(s.writeTimeout())
	defer stall.Stop()
	seq := rt.procSeq.Load()
	for {
		// Hand pending[0] to the processor whenever the frame budget allows.
		var q chan *protocol.Frame
		var head *protocol.Frame
		var headCost int64
		if len(rs.pending) > 0 {
			head = rs.pending[0]
			headCost = frameCost(head)
			if s.frameBudgetAllows(conn, headCost) {
				q = conn.FrameQueue
			}
		}
		select {
		case q <- head:
			conn.FrameQueueBytes.Add(headCost)
			rs.pendingBytes -= int64(head.Size)
			rs.pending[0] = nil
			rs.pending = rs.pending[1:]
			continue
		case g := <-wait.C():
			if g.err != nil {
				return nil, false
			}
			return g.lease, true
		case <-conn.FrameBudgetWake:
			continue // re-evaluate the budget
		case <-rt.leaseCtx.Done():
			wait.Cancel()
			if rt.connClosing.Load() {
				return nil, true // closing: pair nil, keep reading close-ok
			}
			return nil, false
		case <-conn.Done:
			wait.Cancel()
			return nil, false
		case <-stall.C:
			if rt.procSeq.Load() == seq && (len(rs.pending) > 0 || conn.LeaseFIFO.Len() > 0 || len(conn.FrameQueue) > 0) {
				// PROCESSOR STALL: content is queued but the processor made
				// no progress for a whole writeTimeout — the wait can never
				// complete. Cancel it and close with 506.
				wait.Cancel()
				s.leaseClose506(rt, channel,
					fmt.Sprintf("RESOURCE_ERROR - publish lease wait stalled (no processor progress) on channel %d", channel),
					"publish lease wait stalled: no processor progress; closing connection", rs.owedLen())
				return nil, false
			}
			seq = rt.procSeq.Load()
			stall.Reset(s.writeTimeout())
		}
	}
}
