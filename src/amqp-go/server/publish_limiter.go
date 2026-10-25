package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// WS-I Phase B: header-time fair publish lease.
//
// The PublishLimiter bounds exactly one quantity: DECLARED publish body bytes
// plus fixed framing, reserved when the content-header frame is read and held
// until the broker reports every accepted durable copy complete (or a
// synchronous path returns). Grants are strict FIFO across connections (one
// waiter per connection ⇒ round-robin), with a single globally serialized
// oversized exception granted only at used == 0. Invariant: used <= G outside
// that one oversized lease.
//
// The limiter NEVER does I/O: grant and release are short mutex sections, so
// Lease.Release is safe to call from the WAL batch-writer goroutine (A4).

// ErrLimiterClosed is returned by Reserve (and reported to queued waiters)
// after Close. Outstanding leases still release normally.
var ErrLimiterClosed = errors.New("publish limiter closed")

// noopRelease is the shared no-op release function: the disabled limiter and
// every nil-lease path return it instead of allocating a closure.
var noopRelease = func() {}

// disabledLease is the single shared lease handed out by a disabled limiter
// (budget <= 0). Its ReleaseFunc is noopRelease, so the whole disabled path is
// allocation-free.
var disabledLease = &Lease{fn: noopRelease}

// Lease is one granted reservation of bytes against a PublishLimiter. Release
// is idempotent and nil-safe. The release function is pre-bound once at grant
// (fn) so ReleaseFunc never allocates.
type Lease struct {
	l        *PublishLimiter
	bytes    int64
	released atomic.Bool
	fn       func()
}

// releaseOnce performs the idempotent release; nil-safe and safe for
// concurrent use.
func (le *Lease) releaseOnce() {
	if le == nil {
		return
	}
	if le.released.CompareAndSwap(false, true) && le.l != nil {
		le.l.release(le.bytes)
	}
}

// Release returns the lease's bytes to the limiter, granting waiters from the
// head of the FIFO while the grant rule holds. Idempotent; nil-safe.
func (le *Lease) Release() { le.releaseOnce() }

// ReleaseFunc returns the lease's pre-bound release function, or the shared
// noopRelease for a nil/disabled lease. Never allocates.
func (le *Lease) ReleaseFunc() func() {
	if le == nil || le.fn == nil {
		return noopRelease
	}
	return le.fn
}

// LimiterHooks are optional observation callbacks. Nil in production unless
// metrics/tests wire them. BeforeReserve runs UNDER the limiter mutex (so a
// test can serialize arrivals deterministically) and must never block in
// production; the others are quick counter/gauge updates.
type LimiterHooks struct {
	// BeforeReserve is invoked under the limiter mutex at the top of Reserve,
	// before the grant decision. TEST-ONLY arrival serialization.
	BeforeReserve func(connID string, bytes int64)
	// OnWait observes one completed blocking wait (granted or failed).
	OnWait func(d time.Duration)
	// OnGrant observes every grant (immediate or from the FIFO), with the
	// post-grant used and waiter count.
	OnGrant func(connID string, bytes int64, oversized bool, used int64, waiters int)
	// OnRelease observes every effective (first) release, with the
	// post-release used and waiter count.
	OnRelease func(bytes int64, used int64, waiters int)
}

// limiterWaiter is one queued Reserve. ch is capacity-1: the grant (or the
// Close failure) is delivered under the mutex and never blocks.
type limiterWaiter struct {
	connID  string
	bytes   int64
	ch      chan limiterGrant
	granted bool // set under mu when the grant is delivered
}

type limiterGrant struct {
	lease *Lease
	err   error
}

// PublishLimiter is the global publish-body byte lease (WS-I Phase B).
type PublishLimiter struct {
	mu     sync.Mutex
	budget int64
	used   int64
	fifo   []*limiterWaiter
	closed bool
	// waitingConns enforces the one-waiter-per-connection debug assertion:
	// each connection has exactly one reader goroutine, so a second queued
	// Reserve for the same connID is a bug.
	waitingConns map[string]struct{}
	hooks        *LimiterHooks
}

// NewPublishLimiter returns a limiter granting up to budget bytes of
// in-flight publish bodies. budget <= 0 disables the limiter: Reserve and
// TryReserve always succeed with the shared disabledLease and never allocate.
func NewPublishLimiter(budget int64) *PublishLimiter {
	return &PublishLimiter{budget: budget, waitingConns: make(map[string]struct{})}
}

// disabled reports whether the limiter is switched off (budget <= 0).
func (l *PublishLimiter) disabled() bool { return l == nil || l.budget <= 0 }

// canGrantLocked is the grant rule: the head waiter h is granted iff
// used + h.bytes <= budget, or h.bytes > budget AND used == 0 (the single
// globally serialized oversized exception).
func (l *PublishLimiter) canGrantLocked(bytes int64) bool {
	return l.used+bytes <= l.budget || (bytes > l.budget && l.used == 0)
}

// newLeaseLocked mints a lease for a grant decided under mu.
func (l *PublishLimiter) newLeaseLocked(connID string, bytes int64) *Lease {
	le := &Lease{l: l, bytes: bytes}
	le.fn = le.releaseOnce
	if l.hooks != nil && l.hooks.OnGrant != nil {
		l.hooks.OnGrant(connID, bytes, bytes > l.budget, l.used, len(l.fifo))
	}
	return le
}

// grantLocked grants waiters from the head of the FIFO while the grant rule
// holds. Strict FIFO: a later waiter that would fit is never granted ahead of
// a blocked head (no barging).
func (l *PublishLimiter) grantLocked() {
	for len(l.fifo) > 0 {
		h := l.fifo[0]
		if !l.canGrantLocked(h.bytes) {
			return
		}
		l.fifo[0] = nil
		l.fifo = l.fifo[1:]
		delete(l.waitingConns, h.connID)
		l.used += h.bytes
		h.granted = true
		h.ch <- limiterGrant{lease: l.newLeaseLocked(h.connID, h.bytes)}
	}
}

// Reserve blocks until bytes are granted, ctx is cancelled, or the limiter is
// closed. Grants are strict FIFO across connections: a Reserve that cannot be
// granted immediately queues behind every earlier waiter.
func (l *PublishLimiter) Reserve(ctx context.Context, connID string, bytes int64) (*Lease, error) {
	if l.disabled() {
		return disabledLease, nil
	}
	l.mu.Lock()
	if l.hooks != nil && l.hooks.BeforeReserve != nil {
		l.hooks.BeforeReserve(connID, bytes)
	}
	if l.closed {
		l.mu.Unlock()
		return nil, ErrLimiterClosed
	}
	if len(l.fifo) == 0 && l.canGrantLocked(bytes) {
		l.used += bytes
		le := l.newLeaseLocked(connID, bytes)
		l.mu.Unlock()
		return le, nil
	}
	if _, dup := l.waitingConns[connID]; dup {
		l.mu.Unlock()
		panic("publish limiter: two concurrent waiters for connection " + connID)
	}
	w := &limiterWaiter{connID: connID, bytes: bytes, ch: make(chan limiterGrant, 1)}
	l.waitingConns[connID] = struct{}{}
	l.fifo = append(l.fifo, w)
	l.mu.Unlock()

	start := time.Now()
	select {
	case g := <-w.ch:
		if l.hooks != nil && l.hooks.OnWait != nil {
			l.hooks.OnWait(time.Since(start))
		}
		return g.lease, g.err
	case <-ctx.Done():
		l.mu.Lock()
		if w.granted {
			// The grant raced the cancellation and is already delivered: take
			// it and release it so the bytes come back and the next waiter is
			// promoted.
			l.mu.Unlock()
			g := <-w.ch
			if g.lease != nil {
				g.lease.Release()
			}
			if l.hooks != nil && l.hooks.OnWait != nil {
				l.hooks.OnWait(time.Since(start))
			}
			return nil, ctx.Err()
		}
		for i, x := range l.fifo {
			if x == w {
				copy(l.fifo[i:], l.fifo[i+1:])
				l.fifo[len(l.fifo)-1] = nil
				l.fifo = l.fifo[:len(l.fifo)-1]
				break
			}
		}
		delete(l.waitingConns, connID)
		l.mu.Unlock()
		if l.hooks != nil && l.hooks.OnWait != nil {
			l.hooks.OnWait(time.Since(start))
		}
		return nil, ctx.Err()
	}
}

// TryReserve grants only if the FIFO is EMPTY and the grant rule holds: it
// never jumps a queued waiter. The second result is false when no grant was
// made (including after Close).
func (l *PublishLimiter) TryReserve(connID string, bytes int64) (*Lease, bool) {
	if l.disabled() {
		return disabledLease, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || len(l.fifo) > 0 || !l.canGrantLocked(bytes) {
		return nil, false
	}
	l.used += bytes
	return l.newLeaseLocked(connID, bytes), true
}

// release returns bytes and grants from the head while the rule holds. After
// Close, outstanding leases still decrement used but nothing new is granted.
func (l *PublishLimiter) release(bytes int64) {
	l.mu.Lock()
	l.used -= bytes
	if !l.closed {
		l.grantLocked()
	}
	used, waiters := l.used, len(l.fifo)
	l.mu.Unlock()
	if l.hooks != nil && l.hooks.OnRelease != nil {
		l.hooks.OnRelease(bytes, used, waiters)
	}
}

// Close fails every queued waiter and every later Reserve/TryReserve with
// ErrLimiterClosed. Leases already outstanding still release normally.
func (l *PublishLimiter) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	for _, w := range l.fifo {
		w.ch <- limiterGrant{err: ErrLimiterClosed}
	}
	l.fifo = nil
	l.waitingConns = make(map[string]struct{})
}

// Stats reports the currently leased bytes, the budget, and the number of
// queued waiters.
func (l *PublishLimiter) Stats() (used, budget int64, waiters int) {
	if l == nil {
		return 0, 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used, l.budget, len(l.fifo)
}

// leaseBytes charges a declared publish body against the lease budget: the
// body itself, 72 bytes of framing per content-body frame it will arrive in
// (frame_max − 8 payload bytes per frame), plus a fixed 512 bytes of
// header/method bookkeeping.
func leaseBytes(bodySize uint64, frameMax uint32) int64 {
	perFrame := uint64(frameMax)
	if perFrame > 8 {
		perFrame -= 8
	} else {
		perFrame = 1 // degenerate frame_max: one payload byte per frame
	}
	frames := (bodySize + perFrame - 1) / perFrame
	return int64(bodySize + frames*72 + 512)
}
