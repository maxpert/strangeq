package storage

import (
	"sync"
	"sync/atomic"
)

// WS-COLDREAD-PATH: the cold-read buffer pool.
//
// A size-aware, byte-budgeted, bounded free-list of read buffers for the WAL
// cold-read path. It is NOT sync.Pool: sync.Pool cannot match a requested
// capacity, so a 1 KiB record would be handed a 64 KiB buffer (the D3 arena's
// over-allocation). Here the checked-out slice is exactly the record size --
// `cap == recordBytes` -- so the in-flight footprint is the same as today's
// `make([]byte, dataLen)`, and the only new retained memory is the free-list
// itself, capped at coldReadPoolBudgetBytes.
//
// THE OWNERSHIP RULE (the whole point of this file): a pooled buffer is NEVER
// admitted into the hot cache. On a successful admission the body is copied
// exactly out of the pooled buffer (copy-on-admit) and the buffer is released
// immediately. A buffer is only ever aliased by a message that is on its way to
// a consumer, and it is released after the socket write. A missed release leaks
// pool capacity only; a duplicate release is a no-op (the CAS in coldReadBuf).
const (
	// coldReadPoolMinBytes is the smallest record that is pooled. Smaller
	// records (the 1 KiB rows) keep their plain make: the churn they represent
	// is not the target and the per-acquire cost would dominate.
	coldReadPoolMinBytes = 32 * 1024
	// coldReadPoolMaxBufBytes is the largest record that is pooled. A larger
	// record allocates plainly and is never returned to the pool, so the pool
	// cannot grow past the budget with one huge record.
	coldReadPoolMaxBufBytes = 1024 * 1024
	// coldReadPoolBudgetBytes is the total retained pool bytes. It is the ONLY
	// new retained memory this change introduces (acceptance AC4 allows exactly
	// this much peak-RSS growth).
	coldReadPoolBudgetBytes = 8 * 1024 * 1024
)

// coldReadPool is the free-list itself. The mutex is a LEAF: it is never held
// while calling into anything that can re-enter the pool.
type coldReadPool struct {
	mu    sync.Mutex
	free  [][]byte
	bytes int
}

// acquire checks out a buffer of exactly `need` bytes. It returns the buffer
// and whether it was served from the free-list (a pool hit).
//
// A need outside [coldReadPoolMinBytes, coldReadPoolMaxBufBytes] is NEVER
// pooled: it is a plain make, uncounted, so the pool hit/miss series reflects
// only the records the gate actually targets.
//
// THE RETURNED SLICE HAS cap == need: the free-list may hold a larger buffer
// (from a larger record), and the full slice expression clips the capacity so
// the "no 2x slack" invariant holds.
func (p *coldReadPool) acquire(need int) ([]byte, bool) {
	if need < coldReadPoolMinBytes || need > coldReadPoolMaxBufBytes {
		return make([]byte, need), false
	}

	var out []byte
	hit := false
	p.mu.Lock()
	for i, b := range p.free {
		if cap(b) >= need {
			p.free = append(p.free[:i], p.free[i+1:]...)
			p.bytes -= cap(b)
			out = b[:need:need]
			hit = true
			break
		}
	}
	retained := p.bytes
	p.mu.Unlock()
	if out == nil {
		out = make([]byte, need)
	}

	if m := coldReadMetricsObs.Load(); m != nil {
		if hit {
			(*m).RecordColdReadBufferPoolHit()
		} else {
			(*m).RecordColdReadBufferPoolMiss()
		}
		(*m).SetColdReadBufferPoolBytes(int64(retained))
	}
	fireColdReadPoolAcquire(out, hit)
	return out, hit
}

// release returns a buffer to the free-list. A buffer whose capacity is outside
// the gate is dropped (GC), and a buffer that would push the retained bytes
// over the budget is dropped too -- the pool never exceeds the budget.
func (p *coldReadPool) release(b []byte) {
	c := cap(b)
	if c < coldReadPoolMinBytes || c > coldReadPoolMaxBufBytes {
		return
	}
	p.mu.Lock()
	if p.bytes+c <= coldReadPoolBudgetBytes {
		p.free = append(p.free, b[:c:c])
		p.bytes += c
	}
	retained := p.bytes
	p.mu.Unlock()

	if m := coldReadMetricsObs.Load(); m != nil {
		(*m).SetColdReadBufferPoolBytes(int64(retained))
	}
}

// retained reports the current retained pool bytes.
func (p *coldReadPool) retained() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bytes
}

// coldReadBufferPool is the package-global pool, in the style of protocol's
// frame pools. There is one cold-read path, so one pool.
var coldReadBufferPool coldReadPool

// coldReadLeases counts live pooled checkouts. It is the leak detector behind
// amqp_cold_read_leases_outstanding: every pooled checkout that is leased to a
// message is counted once, and the message's release (idempotent) decrements it
// once. A non-pooled (plain make) read is never counted.
var coldReadLeases atomic.Int64

// coldReadBuf wraps one pooled buffer with an atomic release-once guard. A FRESH
// wrapper is created per checkout, so a stale closure can only ever fire the
// already-CAS'd wrapper -- there is no recycle-ABA.
type coldReadBuf struct {
	buf      []byte
	released atomic.Bool
}

// newColdReadLease wraps buf in a fresh release-once token and returns it. The
// token increments the live-lease count at creation and decrements it exactly
// once, when the returned closure first fires.
func newColdReadLease(buf []byte) (*coldReadBuf, func()) {
	b := &coldReadBuf{buf: buf}
	coldReadLeases.Add(1)
	if m := coldReadMetricsObs.Load(); m != nil {
		(*m).AddColdReadLease(1)
	}
	release := func() {
		if !b.released.CompareAndSwap(false, true) {
			return
		}
		coldReadBufferPool.release(b.buf)
		coldReadLeases.Add(-1)
		if m := coldReadMetricsObs.Load(); m != nil {
			(*m).AddColdReadLease(-1)
		}
	}
	return b, release
}

// ColdReadMetrics is an OPTIONAL extension of the storage metrics interfaces
// (the LeaseMetrics pattern), so custom collectors keep compiling. It is
// implemented by metrics.Collector, and storage type-asserts for it once in
// SetMetrics.
//
// The by-read counters (hot/cold, one/two pread, current/old file) are recorded
// AT THE READ, independent of whether the admission that follows succeeds, so a
// refused admission still counts its read.
type ColdReadMetrics interface {
	RecordDeliveryHotHit()
	RecordDeliveryColdRead()
	RecordColdReadOnePread()
	RecordColdReadTwoPreadFallback()
	RecordColdReadCurrentFile()
	RecordColdReadOldFile()
	RecordColdReadBufferPoolHit()
	RecordColdReadBufferPoolMiss()
	SetColdReadBufferPoolBytes(bytes int64)
	AddColdReadLease(delta int64)
	SetHotCacheBytes(bytes int64)
}

// coldReadMetricsObs is the package-global observer the POOL routes its own
// counters through. The pool is global, so its hits/misses/retained-bytes have
// no per-storage home; the read-path counters reach their metrics through the
// per-storage observer instead.
var coldReadMetricsObs atomic.Pointer[ColdReadMetrics]

// SetColdReadPoolMetrics installs m as the observer of the pool's counters, or
// clears it when m is nil. Called once from SetMetrics; production never calls
// it directly.
func SetColdReadPoolMetrics(m ColdReadMetrics) {
	if m == nil {
		coldReadMetricsObs.Store(nil)
		return
	}
	coldReadMetricsObs.Store(&m)
}

// --- test observation hooks -------------------------------------------------

// coldReadPoolAcquireFn observes one pool acquisition. TEST-ONLY.
type coldReadPoolAcquireFn func(buf []byte, hit bool)

var coldReadPoolAcquireHook atomic.Pointer[coldReadPoolAcquireFn]

func fireColdReadPoolAcquire(buf []byte, hit bool) {
	if p := coldReadPoolAcquireHook.Load(); p != nil {
		(*p)(buf, hit)
	}
}

// SetColdReadPoolAcquireObserverForTest installs fn as the observer of pool
// acquisitions and returns a restore function. TEST-ONLY: it lets a test assert
// that every acquired buffer has cap == recordBytes.
func SetColdReadPoolAcquireObserverForTest(fn func(buf []byte, hit bool)) (restore func()) {
	prev := coldReadPoolAcquireHook.Load()
	f := coldReadPoolAcquireFn(fn)
	coldReadPoolAcquireHook.Store(&f)
	return func() { coldReadPoolAcquireHook.Store(prev) }
}

// test-only observation of the global pool's state. The setter names are in
// fault_hooks.go; these readers are the accessors.

// coldReadPoolSnapshot is the state a test reads.
type coldReadPoolSnapshot struct {
	retainedBytes int
	freeCount     int
	leases        int64
}

func coldReadPoolStats() coldReadPoolSnapshot {
	coldReadBufferPool.mu.Lock()
	freeCount := len(coldReadBufferPool.free)
	retained := coldReadBufferPool.bytes
	coldReadBufferPool.mu.Unlock()
	return coldReadPoolSnapshot{retainedBytes: retained, freeCount: freeCount, leases: coldReadLeases.Load()}
}

func coldReadPoolReset() {
	coldReadBufferPool.mu.Lock()
	coldReadBufferPool.free = nil
	coldReadBufferPool.bytes = 0
	coldReadBufferPool.mu.Unlock()
	coldReadLeases.Store(0)
}

func coldReadPoolFreeSlices() [][]byte {
	coldReadBufferPool.mu.Lock()
	defer coldReadBufferPool.mu.Unlock()
	out := make([][]byte, len(coldReadBufferPool.free))
	copy(out, coldReadBufferPool.free)
	return out
}
