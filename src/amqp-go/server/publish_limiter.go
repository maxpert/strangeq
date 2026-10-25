package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// WS-I Phase B (generational tokens): header-time fair publish lease.
//
// The PublishLimiter bounds exactly one quantity: DECLARED publish body bytes
// plus fixed framing, reserved when the content-header frame is read and held
// until the broker reports every accepted durable copy complete (or a
// synchronous path returns). Grants are strict FIFO across connections (one
// waiter per connection ⇒ round-robin), with a single globally serialized
// oversized exception granted only at used == 0. Invariant: used <= G outside
// that one oversized lease.
//
// ZERO steady-state allocation: granted leases are slots in lazily-grown pages
// owned by the limiter (protocol.PublishLeaseToken carries {slotRef, gen} by
// value). The old per-publish &Lease{} + joinRelease{} + defer-closure
// allocations are gone; the two-party retention (serverDone AND brokerDone)
// lives in slot bits, and every signal re-validates the token generation so a
// recycled slot is immune to late duplicate signals (ABA defense).
//
// The limiter NEVER does I/O: the join signal paths are lock-free atomics and
// only the final free takes the mutex, so LeaseAbort/LeaseBrokerDone are safe
// to call from the WAL batch-writer goroutine (A4).

// ErrLimiterClosed is returned by Reserve (and reported to queued waiters)
// after Close. Outstanding tokens still release normally.
var ErrLimiterClosed = errors.New("publish limiter closed")

// token/lifecycle state constants (package-visible for tests).
const (
	tokenStateFree = iota
	tokenStateConnection
	tokenStateTransferred
)

const (
	leaseBitServer uint32 = 1
	leaseBitBroker uint32 = 2
)

// leaseSlotsPageBits: slots are allocated 2^12 per page; page arrays never
// move or shrink after creation, so slot pointers stay valid for the lifetime
// of the limiter.
const leaseSlotsPageBits = 12
const leaseSlotsPerPage = 1 << leaseSlotsPageBits

// pooledLease is one slot in the limiter's page pool. It implements
// protocol.leaseTokenOps (exported methods — asserted from protocol token
// values) with a generation-validated state machine. Per-publish allocation:
// zero.
type pooledLease struct {
	l     *PublishLimiter
	idx   uint32 // global slot index (pageShift|offset), fixed at allocation
	bytes int64
	gen   atomic.Uint32 // bumped to a fresh value when the slot returns FREE
	state atomic.Int32  // tokenState*
	bits  atomic.Uint32 // leaseBitServer|leaseBitBroker
}

// LeaseAbort aborts the lease from server-side ownership (teardown or error
// path). Generation-checked; idempotent.
func (p *pooledLease) LeaseAbort(gen uint32) {
	if p.gen.Load() != gen {
		return // stale signal into a reused slot lifetime
	}
	p.abortLim()
}

func (p *pooledLease) abortLim() {
	for {
		st := p.state.Load()
		switch st {
		case tokenStateFree:
			return
		case tokenStateConnection, tokenStateTransferred:
			if p.state.CompareAndSwap(st, tokenStateFree) {
				p.l.freeSlot(p)
				return
			}
		default:
			return
		}
	}
}

// LeaseTransfer moves ownership to the broker/WAL completion machinery: the
// lease may now only be freed by the serverDone+brokerDone join (or a
// teardown Abort). Idempotent.
func (p *pooledLease) LeaseTransfer(gen uint32) {
	if p.gen.Load() != gen {
		return
	}
	p.state.CompareAndSwap(tokenStateConnection, tokenStateTransferred)
}

// LeaseServerDone signals the server-side wire-write part is done.
func (p *pooledLease) LeaseServerDone(gen uint32) { p.done(gen, leaseBitServer) }

// LeaseBrokerDone signals every accepted durable copy is complete (or a
// synchronous path finished / was refused).
func (p *pooledLease) LeaseBrokerDone(gen uint32) { p.done(gen, leaseBitBroker) }

// LeaseServerFinish is the deferred processCompleteMessage completion: server
// part done; then, when the lease was never transferred (still CONNECTION),
// abort it — the server-side early-return path.
func (p *pooledLease) LeaseServerFinish(gen uint32) {
	p.done(gen, leaseBitServer)
	if p.gen.Load() != gen {
		return // freed by done() or already recycled: nothing to abort
	}
	if st := p.state.Load(); st == tokenStateConnection {
		p.abortLim()
	}
}

func (p *pooledLease) done(gen uint32, bit uint32) {
	if p.gen.Load() != gen {
		return
	}
	for {
		if p.state.Load() == tokenStateFree {
			return
		}
		old := p.bits.Load()
		if old&bit != 0 {
			return // duplicate signal from the same side: no-op
		}
		if !p.bits.CompareAndSwap(old, old|bit) {
			continue
		}
		if old|bit == leaseBitServer|leaseBitBroker {
			// Both sides done: free exactly once (state CAS; an Abort that won
			// the race already freed it — then state is FREE and we no-op).
			for {
				st := p.state.Load()
				if st == tokenStateFree {
					return
				}
				if p.state.CompareAndSwap(st, tokenStateFree) {
					p.l.freeSlot(p)
					return
				}
			}
		}
		return
	}
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
	tok protocol.PublishLeaseToken
	err error
}

// PublishLimiter is the global publish-body byte lease (WS-I Phase B).
type PublishLimiter struct {
	mu     sync.Mutex
	budget int64
	used   int64
	// waiter ring: FIFO of queued Reserve requests, bounded backing array
	// (grows only when a fully-full ring pushes again — bounded by the max
	// concurrent waiters ever; never drifts forward under churn).
	ring   []*limiterWaiter
	head   int
	count  int
	closed bool
	// waitingConns enforces the one-waiter-per-connection debug assertion:
	// each connection has exactly one reader goroutine, so a second queued
	// Reserve for the same connID is a bug.
	waitingConns map[string]struct{}
	hooks        *LimiterHooks

	// slot pool (paged, never shrunk)
	pages    [][]*pooledLease // page>> == leaseSlotsPerPage
	freeList []uint32         // LIFO of free slot indices (under mu)
}

// NewPublishLimiter returns a limiter granting up to budget bytes of
// in-flight publish bodies. budget <= 0 disables the limiter: Reserve and
// TryReserve always succeed with the zero token and never allocate.
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

// claimSlotLocked pops a free slot (growing the pool by one page when
// exhausted) and prepares it for a grant. Returns the slot ready to receive
// the state transition (still FREE) and its index.
func (l *PublishLimiter) claimSlotLocked(bytes int64) *pooledLease {
	if len(l.freeList) == 0 {
		l.growPageLocked()
	}
	idx := l.freeList[len(l.freeList)-1]
	l.freeList = l.freeList[:len(l.freeList)-1]
	page := idx >> leaseSlotsPageBits
	off := idx & (leaseSlotsPerPage - 1)
	slot := l.pages[page][off]
	slot.bytes = bytes
	return slot
}

func (l *PublishLimiter) growPageLocked() {
	base := uint32(len(l.pages)) << leaseSlotsPageBits
	page := make([]*pooledLease, leaseSlotsPerPage)
	for i := range page {
		slot := &pooledLease{l: l, idx: base | uint32(i)}
		slot.gen.Store(1) // zero token {0,0} can never alias a real slot
		page[i] = slot
		l.freeList = append(l.freeList, slot.idx)
	}
	l.pages = append(l.pages, page)
}

// grantLocked grants waiters from the head of the FIFO while the grant rule
// holds. Strict FIFO: a later waiter that would fit is never granted ahead of
// a blocked head (no barging).
func (l *PublishLimiter) grantLocked() {
	for l.count > 0 {
		h := l.ring[l.head]
		if !l.canGrantLocked(h.bytes) {
			return
		}
		l.ringPopLocked()
		delete(l.waitingConns, h.connID)
		l.used += h.bytes
		tok := l.grantWaiterLocked(h.connID, h.bytes)
		h.granted = true
		h.ch <- limiterGrant{tok: tok}
	}
}

// grantWaiterLocked performs the used-accounted grant for a decided request
// (also used by TryReserve and immediate Begin grants).
func (l *PublishLimiter) grantWaiterLocked(connID string, bytes int64) protocol.PublishLeaseToken {
	slot := l.claimSlotLocked(bytes)
	tok := protocol.LeaseTokenOf(slot, slot.gen.Load())
	slot.state.Store(tokenStateConnection)
	if l.hooks != nil && l.hooks.OnGrant != nil {
		l.hooks.OnGrant(connID, bytes, bytes > l.budget, l.used, l.count)
	}
	return tok
}

func (l *PublishLimiter) ringPushLocked(w *limiterWaiter) {
	if l.count == len(l.ring) {
		// Grow the ring: bounded by the max concurrent waiters ever seen.
		capacity := len(l.ring) * 2
		if capacity == 0 {
			capacity = 1
		}
		nb := make([]*limiterWaiter, capacity)
		for i := 0; i < l.count; i++ {
			nb[i] = l.ring[(l.head+i)%len(l.ring)]
		}
		l.ring = nb
		l.head = 0
	}
	l.ring[(l.head+l.count)%len(l.ring)] = w
	l.count++
}

func (l *PublishLimiter) ringPopLocked() {
	l.ring[l.head] = nil
	l.head = (l.head + 1) % len(l.ring)
	l.count--
	if l.count == 0 {
		l.head = 0
		// Keep len(l.ring) as-is: bounded capacity, reused in place.
	}
}

// freeSlot returns a slot to the pool, returns its bytes to the budget,
// promotes granted waiters, and fires OnRelease exactly once per ticket.
func (l *PublishLimiter) freeSlot(p *pooledLease) {
	l.mu.Lock()
	l.used -= p.bytes
	// Fresh generation BEFORE the recycle: any signal carrying the old gen
	// becomes a no-op from this point on.
	releasedBytes := p.bytes
	p.bits.Store(0)
	p.gen.Add(1)
	p.bytes = 0
	l.freeList = append(l.freeList, p.idx)
	if !l.closed {
		l.grantLocked()
	}
	used, waiters := l.used, l.count
	l.mu.Unlock()
	if l.hooks != nil && l.hooks.OnRelease != nil {
		l.hooks.OnRelease(releasedBytes, used, waiters)
	}
}

// LeaseWait is a queued reservation returned by Begin. C delivers the
// resolution (a grant, or ErrLimiterClosed after Close). Cancel is idempotent
// and grant/cancel race safe: if the grant was already delivered it is
// drained and aborted; otherwise the waiter is removed from the ring FIFO,
// the per-connection waiting marker is cleared, and the next waiter is
// promoted if it now fits.
type LeaseWait struct {
	l    *PublishLimiter
	w    *limiterWaiter
	done atomic.Bool
}

// C returns the channel the resolution arrives on (capacity 1, never blocks
// the limiter).
func (lw *LeaseWait) C() <-chan limiterGrant { return lw.w.ch }

// Cancel abandons the wait. Idempotent.
func (lw *LeaseWait) Cancel() {
	if lw == nil || !lw.done.CompareAndSwap(false, true) {
		return
	}
	l := lw.l
	w := lw.w
	l.mu.Lock()
	if w.granted {
		// The grant raced the cancellation and is already delivered: take it
		// and abort it so the bytes come back and the next waiter promotes.
		l.mu.Unlock()
		if g := <-w.ch; g.err == nil {
			g.tok.Abort()
		}
		return
	}
	for i := 0; i < l.count; i++ {
		idx := (l.head + i) % len(l.ring)
		if l.ring[idx] == w {
			copy(l.ring[idx:], l.ring[idx+1:])
			l.ring[(l.head+l.count-1)%len(l.ring)] = nil
			l.count--
			if idx == l.head && l.count == 0 {
				l.head = 0
			}
			break
		}
	}
	delete(l.waitingConns, w.connID)
	// A canceled head may have been the only thing between the next waiter
	// and a grant (it asked for MORE than the next waiter needs).
	if !l.closed {
		l.grantLocked()
	}
	l.mu.Unlock()
}

// Begin starts a reservation. On an immediate grant it returns (token, nil,
// nil); otherwise (zero, wait, nil) and the wait resolves via LeaseWait.C.
// After Close it returns ErrLimiterClosed. A disabled limiter returns the
// zero token and never allocates.
func (l *PublishLimiter) Begin(connID string, bytes int64) (protocol.PublishLeaseToken, *LeaseWait, error) {
	if l.disabled() {
		return protocol.PublishLeaseToken{}, nil, nil
	}
	l.mu.Lock()
	if l.hooks != nil && l.hooks.BeforeReserve != nil {
		l.hooks.BeforeReserve(connID, bytes)
	}
	if l.closed {
		l.mu.Unlock()
		return protocol.PublishLeaseToken{}, nil, ErrLimiterClosed
	}
	if l.count == 0 && l.canGrantLocked(bytes) {
		l.used += bytes
		tok := l.grantWaiterLocked(connID, bytes)
		l.mu.Unlock()
		return tok, nil, nil
	}
	if _, dup := l.waitingConns[connID]; dup {
		l.mu.Unlock()
		panic("publish limiter: two concurrent waiters for connection " + connID)
	}
	w := &limiterWaiter{connID: connID, bytes: bytes, ch: make(chan limiterGrant, 1)}
	l.waitingConns[connID] = struct{}{}
	l.ringPushLocked(w)
	l.mu.Unlock()
	return protocol.PublishLeaseToken{}, &LeaseWait{l: l, w: w}, nil
}

// Reserve blocks until bytes are granted, ctx is cancelled, or the limiter is
// closed. Grants are strict FIFO across connections: a Reserve that cannot be
// granted immediately queues behind every earlier waiter.
func (l *PublishLimiter) Reserve(ctx context.Context, connID string, bytes int64) (protocol.PublishLeaseToken, error) {
	tok, wait, err := l.Begin(connID, bytes)
	if err != nil || tok.Valid() || wait == nil {
		return tok, err
	}
	start := time.Now()
	select {
	case g := <-wait.C():
		if l.hooks != nil && l.hooks.OnWait != nil {
			l.hooks.OnWait(time.Since(start))
		}
		return g.tok, g.err
	case <-ctx.Done():
		wait.Cancel()
		if l.hooks != nil && l.hooks.OnWait != nil {
			l.hooks.OnWait(time.Since(start))
		}
		return protocol.PublishLeaseToken{}, ctx.Err()
	}
}

// TryReserve grants only if the FIFO is EMPTY and the grant rule holds: it
// never jumps a queued waiter. The second result is false when no grant was
// made (including after Close).
func (l *PublishLimiter) TryReserve(connID string, bytes int64) (protocol.PublishLeaseToken, bool) {
	if l.disabled() {
		return protocol.PublishLeaseToken{}, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.count > 0 || !l.canGrantLocked(bytes) {
		return protocol.PublishLeaseToken{}, false
	}
	l.used += bytes
	return l.grantWaiterLocked(connID, bytes), true
}

// Close fails every queued waiter and every later Reserve/TryReserve with
// ErrLimiterClosed. Outstanding tokens still release normally.
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
	for i := 0; i < l.count; i++ {
		w := l.ring[(l.head+i)%len(l.ring)]
		w.ch <- limiterGrant{err: ErrLimiterClosed}
	}
	l.ring = nil
	l.head = 0
	l.count = 0
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
	return l.used, l.budget, l.count
}

// Closed reports whether the limiter has been closed: a false TryReserve can
// then be told apart from an overbudget refusal.
func (l *PublishLimiter) Closed() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
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

// ---- test hooks ----

func (l *PublishLimiter) tokenStateForTest(tok protocol.PublishLeaseToken) int {
	return int(tok.State())
}

func (p *pooledLease) LeaseState() int32 { return p.state.Load() }

func (l *PublishLimiter) fifoCapForTest() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ring == nil {
		return 0
	}
	return len(l.ring)
}

func (l *PublishLimiter) slotPagesForTest() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pages)
}
