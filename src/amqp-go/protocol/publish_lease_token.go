package protocol

import "sync"

// WS-I Phase B (generational tokens): PublishLeaseToken is the zero-allocation
// replacement for the per-publish Lease heap object + joinRelease closure
// pair. It is an opaque VALUE; the zero value is the universal no-op token
// (disabled limiter, malformed headers, doubly-released leases).
//
// Token lifecycle (state machine lives in the server's pooled slot):
//
//	FREE --grant--> CONNECTION --transfer--> TRANSFERRED --join(FREE)
//	CONNECTION --abort--> FREE
//	join = serverDone AND brokerDone both set (two-party retention).
//
// Every signal validates the token's generation against the slot it Located:
// a signal from a recycled-slot lifetime (late WAL-side completion racing a
// reuse) is a no-op — exact ABA defense. This type contains NO behavior of
// its own beyond plumbing signals into the concrete lease implementation (a
// server-side pooled slot) through the token-ops interface, so it can live in
// protocol without an import cycle.

// PublishLeaseToken is an opaque, copyable lease handle. Copying it is safe
// and required (PendingMessage stores a copy); the underlying slot stays in
// place (paged allocation, addresses never move). `hs` is deliberately an
// empty interface holding the server-internal slot pointer: storing/copying
// the value never allocates.
type PublishLeaseToken struct {
	hs  any    // concrete server lease slot (opaque; never nil for a live lease)
	gen uint32 // slot generation at grant; validated on every signal
}

// Valid reports whether the token carries a live lease reference. The zero
// token (and any token from a disabled limiter) is invalid: every signal on it
// is a silent no-op.
func (t PublishLeaseToken) Valid() bool { return t.hs != nil }

// ServerFinish is the deferred processCompleteMessage completion: signals the
// server part done AND, when the lease was never transferred (still
// CONNECTION), also aborts it — exactly the old joinRelease serverDone +
// abort-after-return pair, with zero allocation. Safe to call twice.
func (t PublishLeaseToken) ServerFinish() {
	if o := t.ops(); o != nil {
		o.LeaseServerFinish(t.gen)
	}
}

// leaseTokenOps is implemented by the server's pooled lease slot. Methods are
// exported so protocol can assert into them; the concretely-owning type is
// server-internal. Each op re-validates gen for ABA defense.
type leaseTokenOps interface {
	LeaseAbort(gen uint32)
	LeaseTransfer(gen uint32)
	LeaseServerDone(gen uint32)
	LeaseBrokerDone(gen uint32)
	LeaseServerFinish(gen uint32)
	// LeaseState reports the slot state for tests/observability (no gen check:
	// the caller interprets a stale view via its own gen).
	LeaseState() int32
}

func (t PublishLeaseToken) ops() leaseTokenOps {
	o, _ := t.hs.(leaseTokenOps)
	return o
}

// LeaseTokenOf wraps a concrete lease slot and its grant-time generation into
// a token. Called by the server limiter only; zero-alloc (pure struct value).
func LeaseTokenOf(slot any, gen uint32) PublishLeaseToken {
	return PublishLeaseToken{hs: slot, gen: gen}
}

// Abort releases the lease from CONNECTION ownership: server-side teardown or
// an error return before the lease was transferred. Idempotent,
// generation-checked, and valid in the TRANSFERRED state too exactly where the
// old joinRelease allowed a server release to win the race (teardown after
// transfer); a later brokerDone on the freed slot is a no-op.
func (t PublishLeaseToken) Abort() {
	if o := t.ops(); o != nil {
		o.LeaseAbort(t.gen)
	}
}

// Transfer marks the broker/WAL completion machinery as the release-owner: the
// lease may now only be freed by the serverDone+brokerDone join (or a teardown
// Abort).
func (t PublishLeaseToken) Transfer() {
	if o := t.ops(); o != nil {
		o.LeaseTransfer(t.gen)
	}
}

// ServerDone signals that the server-side wire-write part of the publish is
// done (the deferred completion of processCompleteMessage). The slot frees
// once BrokerDone has also been signaled (in any order).
func (t PublishLeaseToken) ServerDone() {
	if o := t.ops(); o != nil {
		o.LeaseServerDone(t.gen)
	}
}

// BrokerDone signals that every accepted durable copy is complete (or a
// synchronous path finished / was refused). The slot frees once ServerDone has
// also been signaled (in any order).
func (t PublishLeaseToken) BrokerDone() {
	if o := t.ops(); o != nil {
		o.LeaseBrokerDone(t.gen)
	}
}

// State returns the token's slot state (0=FREE) or 0 for a zero/invalid token.
func (t PublishLeaseToken) State() int32 {
	if o := t.ops(); o != nil {
		return o.LeaseState()
	}
	return 0
}

// TokenQueue is a small mutex-guarded FIFO pairing each content-header frame
// with the publish lease the reader reserved for it (WS-I Phase B). The reader
// Pushes once per enqueued header frame (zero token for a malformed or
// over-MaxMessageSize header), the processor Pops on dequeue, and teardown
// Drains the rest. FIFO order equals frame order because the reader enqueues
// headers in read order. The backing slice is consumed with a moving head and
// compacted, so queue churn does not drift the backing array forward forever.
type TokenQueue struct {
	mu   sync.Mutex
	head int
	// items holds the queue with items[:head] logically popped (nil'd
	// lazily on compaction).
	items []PublishLeaseToken
}

// Push appends one token token to the FIFO.
func (q *TokenQueue) Push(t PublishLeaseToken) {
	q.mu.Lock()
	if q.head > 0 && q.head == len(q.items) {
		// Reset when fully drained: reuse the backing array from the front.
		q.items = q.items[:0]
		q.head = 0
	}
	q.items = append(q.items, t)
	q.mu.Unlock()
}

// Pop removes and returns the oldest queued token, or the zero token when
// empty.
func (q *TokenQueue) Pop() PublishLeaseToken {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head >= len(q.items) {
		return PublishLeaseToken{}
	}
	t := q.items[q.head]
	q.items[q.head] = PublishLeaseToken{}
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return t
}

// Drain removes and returns every queued token in FIFO order (teardown).
func (q *TokenQueue) Drain() []PublishLeaseToken {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]PublishLeaseToken, len(q.items)-q.head)
	copy(out, q.items[q.head:])
	q.items = q.items[:0]
	q.head = 0
	return out
}

// Len returns the number of queued tokens.
func (q *TokenQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) - q.head
}
