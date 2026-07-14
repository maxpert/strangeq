package broker

import (
	"sync"
	"sync/atomic"
	"time"
)

const requeueInitialCap = 4096

type requeueEntry struct {
	tag         uint64
	redelivered bool
}

type QueueState struct {
	tail           atomic.Uint64
	head           atomic.Uint64
	minAckCursor   atomic.Uint64
	waiting        atomic.Int64
	inflight       atomic.Int64
	wake           chan struct{}
	parkedCount    atomic.Int64
	producerParked atomic.Int64
	requeueMu      sync.Mutex
	requeueBuf     []requeueEntry
	requeueHead    int
	requeueLen     int
	requeueCount   atomic.Int64
	depthHighWM    atomic.Uint64
	closed         atomic.Bool
	stopCh         chan struct{}
	parkTimeout    time.Duration

	// readyBytes tracks the total body bytes of ready (not-yet-delivered)
	// messages for x-max-length-bytes enforcement (SQ-11). It is left at zero
	// and untouched by the dispatch hot paths in W1 — SQ-11 wires the add on
	// enqueue and the sub on claim/ack/evict — so an unset max-length-bytes
	// policy costs nothing. Lockless atomic.
	readyBytes atomic.Int64

	// maxLenMu serializes the reject-publish (x-overflow=reject-publish)
	// admission decision — the read of WaitingCount()/ReadyBytes() and the
	// matching Publish()/AddReadyBytes() increment — so concurrent publishers
	// can never both pass the AT-OR-OVER check at count == limit-1 and push the
	// ready set permanently past the cap (SQ-11). It is taken ONLY on the
	// reject-publish admission path: queues with no policy or with the drop-head
	// default never touch it (drop-head self-corrects on the next publish, so it
	// tolerates transient overshoot lock-free). Zero cost on the hot path.
	maxLenMu sync.Mutex

	// policy is the queue's resolved x-argument policy (SQ-7), set at declare
	// time (client declare and durable recovery both route through
	// StorageBroker.DeclareQueue). It is attached here — on the per-queue
	// hot-path struct — so Wave 2 enforcement (TTL SQ-9, DLX SQ-10,
	// max-length SQ-11) costs exactly one atomic load plus a nil branch on
	// paths that already hold the QueueState (publish, delivery, reject).
	// nil means "no policy". Lockless by design.
	policy atomic.Pointer[QueuePolicy]

	// reaperStarted guards single-start of the per-queue SQ-9 TTL/x-expires
	// reaper goroutine (started at declare/recovery when the policy needs it,
	// stopped when the queue's stopCh closes). Redeclare must not spawn a second.
	reaperStarted atomic.Bool

	// lastActivityMilli is the Unix-milli timestamp of the most recent queue
	// "use" for x-expires (SQ-9): consumer register, basic.get, and (re)declare
	// reset it. The reaper deletes the queue after QueueExpires ms of no use with
	// no consumers. Only meaningful when the policy has HasQueueExpires.
	lastActivityMilli atomic.Int64

	// Contiguous durable-visibility frontier (iteration 2). A durable publish's
	// consumer-visibility (head advance) is DEFERRED to its WAL fsync completion,
	// which can complete out of tag order across connections on a shared queue.
	// A naive per-tag CAS-max head advance would then jump head PAST a still-
	// pending lower tag, exposing it before its own fsync (delivery-before-
	// durable, A3 violation). The frontier fixes this: head advances only to the
	// LOWEST still-pending routed tag (or frontierMax+1 if none pending), so a
	// tag is never claimable before its own fsync.
	//
	// frontierActive is set (permanently) BEFORE a durable publish assigns its
	// global delivery tag, so a concurrent transient publisher that assigns a
	// higher tag observes it (sequentially-consistent atomics) and routes through
	// the frontier instead of the lock-free fast path — closing the 0→1 race.
	// A pure-transient queue never sets it: one atomic load selects today's
	// lock-free CAS-max Publish (zero regression). frontierMu guards the frontier
	// structures and is held ONLY for O(1)-amortized bookkeeping, never across
	// I/O, so a durable completion can drive it from the WAL batch-writer goroutine
	// (A4). frontierPending holds durable tags awaiting fsync in increasing
	// (registration) order; frontierDone maps a completed tag to whether it is a
	// REAL (delivered) message (true) or a dropped fsync-error tag (false, never
	// delivered — gap-skipped). frontierMax is the highest tag registered.
	frontierActive  atomic.Bool
	frontierMu      sync.Mutex
	frontierPending []uint64
	frontierPHead   int
	frontierDone    map[uint64]bool
	frontierMax     uint64
}

// Policy returns the queue's resolved policy, or nil if the queue has no
// known x-arguments. Safe for concurrent use; a single atomic load.
func (qs *QueueState) Policy() *QueuePolicy {
	return qs.policy.Load()
}

// SetPolicy atomically replaces the queue's resolved policy. Called from
// declare/recovery paths only — never on the hot path.
func (qs *QueueState) SetPolicy(p *QueuePolicy) {
	qs.policy.Store(p)
}

func NewQueueState(depthHighWM uint64) *QueueState {
	qs := &QueueState{
		wake:        make(chan struct{}, 128),
		stopCh:      make(chan struct{}),
		parkTimeout: 1 * time.Millisecond,
		requeueBuf:  make([]requeueEntry, requeueInitialCap),
	}
	qs.depthHighWM.Store(depthHighWM)
	return qs
}

func (qs *QueueState) SetDepthHighWM(wm uint64) {
	qs.depthHighWM.Store(wm)
}

func (qs *QueueState) SetParkTimeout(d time.Duration) {
	qs.parkTimeout = d
}

func (qs *QueueState) WaitForCapacity(stop <-chan struct{}) bool {
	if !qs.AtHighWaterMark() {
		return true
	}
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	qs.producerParked.Add(1)
	defer qs.producerParked.Add(-1)
	for qs.AtHighWaterMark() {
		if qs.closed.Load() {
			return false
		}
		select {
		case <-stop:
			return false
		default:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(10 * time.Millisecond)
		select {
		case <-qs.wake:
		case <-stop:
			return false
		case <-timer.C:
		}
	}
	return !qs.closed.Load()
}

func (qs *QueueState) Publish(tag uint64) {
	if qs.closed.Load() {
		return
	}
	for {
		cur := qs.head.Load()
		if tag+1 <= cur {
			break
		}
		if qs.head.CompareAndSwap(cur, tag+1) {
			break
		}
	}
	qs.waiting.Add(1)
	qs.NotifyNewMessage()
}

// FrontierActive reports whether this queue uses the contiguous durable
// frontier for visibility. A single sequentially-consistent atomic load; false
// (a pure-transient queue) selects the lock-free CAS-max Publish fast path.
func (qs *QueueState) FrontierActive() bool { return qs.frontierActive.Load() }

// FrontierReserve is the atomic {activate + assign tag + register pending} step
// for a publish routed to this queue that must be ordered through the contiguous
// durable frontier — a durable publish, OR a transient publish on an already
// frontier-active queue (mixed queue). assign is called to mint the global
// delivery tag WHILE frontierMu is held, so this queue's frontier tags are
// registered strictly in tag order (the frontier ring stays increasing) and no
// completion can advance head past this tag before it is registered. It
// activates the frontier BEFORE calling assign, so any concurrent transient
// publisher that mints a HIGHER tag observes frontierActive==true and routes
// through the frontier rather than the fast path (closing the 0→1 race). The tag
// is recorded pending (not yet visible — head is not advanced); the caller marks
// it ready via FrontierComplete(tag, true) after the message is stored, or
// releases it via FrontierComplete(tag, false) if the store fails. Returns the
// assigned tag.
func (qs *QueueState) FrontierReserve(assign func() uint64) uint64 {
	qs.frontierMu.Lock()
	qs.frontierActive.Store(true) // BEFORE assign (see field doc)
	tag := assign()
	qs.frontierPending = append(qs.frontierPending, tag)
	if tag > qs.frontierMax {
		qs.frontierMax = tag
	}
	qs.frontierMu.Unlock()
	return tag
}

// FrontierComplete records that a frontier-reserved tag's store completed:
// real=true makes it a delivered message (durable fsync succeeded, or a
// transient message was stored); real=false (fsync/write/store error) releases
// it — it was never made ring-resident, so head is advanced PAST it but it is
// never delivered (gap-skipped) and never counted ready. Releasing on error is
// also what keeps a per-queue frontier from wedging when a reserved tag's store
// fails (M1). It advances head across the newly contiguous done-prefix (never
// past a still-pending lower tag), increments the ready count for each real
// message exposed, and wakes consumers. Runs on the WAL batch-writer goroutine
// (durable) or the frame processor (transient): O(1)-amortized under frontierMu,
// no I/O (A4).
func (qs *QueueState) FrontierComplete(tag uint64, real bool) {
	qs.frontierMu.Lock()
	if qs.frontierDone == nil {
		qs.frontierDone = make(map[uint64]bool)
	}
	qs.frontierDone[tag] = real
	newHead, newlyReal := qs.frontierAdvanceLocked()
	qs.frontierMu.Unlock()

	qs.casMaxHead(newHead)
	if newlyReal > 0 {
		qs.waiting.Add(int64(newlyReal))
	}
	qs.NotifyNewMessage()
}

// FrontierPublishTransient records an already-stored tag on a frontier-active
// queue at COMPLETE time (not reserved at mint). It bumps frontierMax so the tag
// is exposed once the contiguous frontier reaches it, and advances head if no
// lower still-pending durable tag blocks it (publish-order FIFO). The caller
// MUST have stored the message first — the store establishes the happens-before
// that makes late registration strand-free.
//
// Caller: the transactional deferred-visibility closure (PublishMessageTx),
// where the tag is minted at stage time, stored, and made visible after commit.
//
// The async durable-confirm path (PublishMessageAsyncConfirm) does NOT use this:
// it reserves at mint via FrontierReserve + FrontierComplete because its ring
// store is DEFERRED into the fsync completion (StoreMessageAsync), so the tag is
// not yet claim-safe at publish time.
func (qs *QueueState) FrontierPublishTransient(tag uint64) {
	qs.frontierMu.Lock()
	if tag > qs.frontierMax {
		qs.frontierMax = tag
	}
	newHead, _ := qs.frontierAdvanceLocked()
	qs.frontierMu.Unlock()

	qs.casMaxHead(newHead)
	qs.waiting.Add(1) // a transient publish is immediately a ready message
	qs.NotifyNewMessage()
}

// frontierAdvanceLocked pops the contiguous done-prefix off the pending ring and
// returns the new head target (lowest still-pending tag, or frontierMax+1 when
// none pending) plus the count of REAL messages the pop newly exposes. Caller
// holds frontierMu.
func (qs *QueueState) frontierAdvanceLocked() (newHead uint64, newlyReal int) {
	for qs.frontierPHead < len(qs.frontierPending) {
		front := qs.frontierPending[qs.frontierPHead]
		real, done := qs.frontierDone[front]
		if !done {
			break
		}
		delete(qs.frontierDone, front)
		qs.frontierPHead++
		if real {
			newlyReal++
		}
	}
	// Compact the ring when the consumed prefix dominates, bounding memory.
	if qs.frontierPHead > 1024 && qs.frontierPHead*2 > len(qs.frontierPending) {
		live := qs.frontierPending[qs.frontierPHead:]
		n := copy(qs.frontierPending, live)
		qs.frontierPending = qs.frontierPending[:n]
		qs.frontierPHead = 0
	}
	if qs.frontierPHead < len(qs.frontierPending) {
		return qs.frontierPending[qs.frontierPHead], newlyReal
	}
	return qs.frontierMax + 1, newlyReal
}

// casMaxHead advances head to newHead via CAS-max (never regresses), then wakes
// parked consumers if it moved.
func (qs *QueueState) casMaxHead(newHead uint64) {
	for {
		cur := qs.head.Load()
		if newHead <= cur {
			return
		}
		if qs.head.CompareAndSwap(cur, newHead) {
			return
		}
	}
}

func (qs *QueueState) NotifyNewMessage() {
	if qs.parkedCount.Load() <= 0 && qs.producerParked.Load() <= 0 {
		return
	}
	select {
	case qs.wake <- struct{}{}:
	default:
	}
}

func (qs *QueueState) WakeAll() {
	for {
		parked := qs.parkedCount.Load()
		if parked <= 0 {
			return
		}
		for i := 0; i < int(parked)+8; i++ {
			select {
			case qs.wake <- struct{}{}:
			default:
				return
			}
		}
		if qs.parkedCount.Load() <= 0 {
			return
		}
	}
}

func (qs *QueueState) Claim(stop <-chan struct{}, timer *time.Timer) (uint64, bool, bool) {
	select {
	case <-stop:
		return 0, false, false
	default:
	}
	if t, r, ok := qs.tryPopRequeue(); ok {
		return t, r, true
	}
	for {
		t := qs.tail.Load()
		h := qs.head.Load()
		if t < h {
			if qs.tail.CompareAndSwap(t, t+1) {
				return t, false, true
			}
			continue
		}
		select {
		case <-stop:
			return 0, false, false
		default:
		}
		if t2, r, ok := qs.tryPopRequeue(); ok {
			return t2, r, true
		}
		if !qs.park(stop, timer) {
			return 0, false, false
		}
	}
}

func (qs *QueueState) park(stop <-chan struct{}, timer *time.Timer) bool {
	select {
	case <-stop:
		return false
	default:
	}
	if qs.closed.Load() {
		return false
	}
	qs.parkedCount.Add(1)
	if qs.tail.Load() < qs.head.Load() || qs.requeueCount.Load() > 0 {
		qs.parkedCount.Add(-1)
		return true
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(qs.parkTimeout)
	select {
	case <-qs.wake:
		qs.parkedCount.Add(-1)
		return true
	case <-stop:
		qs.parkedCount.Add(-1)
		return false
	case <-timer.C:
		qs.parkedCount.Add(-1)
		return true
	}
}

func (qs *QueueState) Requeue(tag uint64) {
	qs.requeueMu.Lock()
	bufCap := cap(qs.requeueBuf)
	if qs.requeueLen >= bufCap {
		newCap := bufCap * 2
		newBuf := make([]requeueEntry, newCap)
		for i := 0; i < qs.requeueLen; i++ {
			newBuf[i] = qs.requeueBuf[(qs.requeueHead+i)%bufCap]
		}
		qs.requeueBuf = newBuf
		qs.requeueHead = 0
		bufCap = newCap
	}
	writeIdx := (qs.requeueHead + qs.requeueLen) % bufCap
	qs.requeueBuf[writeIdx] = requeueEntry{tag: tag, redelivered: true}
	qs.requeueLen++
	qs.requeueCount.Add(1)
	qs.requeueMu.Unlock()
	qs.waiting.Add(1)
	qs.inflight.Add(-1)
	qs.NotifyNewMessage()
}

func (qs *QueueState) tryPopRequeue() (uint64, bool, bool) {
	if qs.requeueCount.Load() <= 0 {
		return 0, false, false
	}
	qs.requeueMu.Lock()
	if qs.requeueLen == 0 {
		qs.requeueMu.Unlock()
		return 0, false, false
	}
	bufCap := cap(qs.requeueBuf)
	entry := qs.requeueBuf[qs.requeueHead]
	qs.requeueHead = (qs.requeueHead + 1) % bufCap
	qs.requeueLen--
	qs.requeueCount.Add(-1)

	if qs.requeueLen > 0 && qs.requeueLen < bufCap/4 && bufCap > requeueInitialCap*2 {
		newCap := bufCap / 2
		if newCap < requeueInitialCap {
			newCap = requeueInitialCap
		}
		newBuf := make([]requeueEntry, newCap)
		for i := 0; i < qs.requeueLen; i++ {
			newBuf[i] = qs.requeueBuf[(qs.requeueHead+i)%bufCap]
		}
		qs.requeueBuf = newBuf
		qs.requeueHead = 0
	}

	qs.requeueMu.Unlock()
	return entry.tag, entry.redelivered, true
}

func (qs *QueueState) ClaimInflight(tag uint64) {
	qs.waiting.Add(-1)
	qs.inflight.Add(1)
}

// PopOldestReady pops the tag of the globally-oldest READY message for SQ-11
// drop-head eviction. It mirrors Claim's ordering — the requeue ring FIRST
// (redelivered messages are dispatched before tail-cursor ones and are therefore
// older in the queue), then the tail cursor — so the popped message is the exact
// one that would be delivered next, and the just-published newest message
// (highest tail tag) is popped last. Returns (0, false) when no ready message
// exists.
//
// Like Claim it does NOT touch `waiting` or `inflight`: the caller inspects the
// tag first, then either GapSkipAdvance()s a gap tag (a tag never stored / already
// acked, which was never counted as ready) or completes the eviction of a real
// message via ClaimInflight()+AckAdvance(). Going through inflight for real
// evictions keeps AckAdvance from mis-jumping minAckCursor while live deliveries
// are outstanding (a concurrent real ack keeps `remaining` non-zero).
func (qs *QueueState) PopOldestReady() (uint64, bool) {
	if t, _, ok := qs.tryPopRequeue(); ok {
		return t, true
	}
	for {
		t := qs.tail.Load()
		h := qs.head.Load()
		if t < h {
			if qs.tail.CompareAndSwap(t, t+1) {
				return t, true
			}
			continue
		}
		// tail caught up to head: excess may still sit in the requeue ring
		// (tail>=head), so try it once more before giving up.
		if t2, _, ok := qs.tryPopRequeue(); ok {
			return t2, true
		}
		return 0, false
	}
}

func (qs *QueueState) AckAdvance(tag uint64) {
	remaining := qs.inflight.Add(-1)
	if remaining < 0 {
		for {
			cur := qs.inflight.Load()
			if cur >= 0 {
				break
			}
			if qs.inflight.CompareAndSwap(cur, 0) {
				break
			}
		}
		remaining = 0
	}
	if remaining == 0 && qs.requeueCount.Load() <= 0 {
		qs.minAckCursor.Store(qs.head.Load())
	} else if tag == qs.minAckCursor.Load() {
		qs.minAckCursor.CompareAndSwap(tag, tag+1)
	}
	qs.NotifyNewMessage()
}

// ReapDrop accounts a READY (never-delivered) message removed by the SQ-9 TTL
// reaper or the delivery-time head-check: it decrements the ready count only. It
// never touches the inflight counter (a reaped message was never delivered) and
// is invoked ONLY by the winner of the ring Delete (deleteIfPresent==true), so
// across {reaper drop, consumer head-check} at most one caller decrements depth
// per tag. minAckCursor is intentionally left to the storage-synced ack path;
// it is not read for any depth/backpressure decision (Depth uses waiting +
// inflight). waiting is clamped at zero defensively.
func (qs *QueueState) ReapDrop() {
	if qs.waiting.Add(-1) < 0 {
		for {
			cur := qs.waiting.Load()
			if cur >= 0 {
				break
			}
			if qs.waiting.CompareAndSwap(cur, 0) {
				break
			}
		}
	}
	qs.NotifyNewMessage()
}

// StartReaperOnce reports true exactly once per queue lifetime, gating the
// single-start of the per-queue reaper goroutine against redeclare.
func (qs *QueueState) StartReaperOnce() bool {
	return qs.reaperStarted.CompareAndSwap(false, true)
}

// MarkActivity records queue use for the x-expires idle clock (SQ-9).
func (qs *QueueState) MarkActivity(nowMillis int64) {
	qs.lastActivityMilli.Store(nowMillis)
}

// LastActivityMilli returns the last recorded x-expires activity timestamp.
func (qs *QueueState) LastActivityMilli() int64 {
	return qs.lastActivityMilli.Load()
}

func (qs *QueueState) GapSkipAdvance(tag uint64) {
	if qs.inflight.Load() == 0 && qs.requeueCount.Load() <= 0 {
		qs.minAckCursor.Store(qs.head.Load())
	} else if tag == qs.minAckCursor.Load() {
		qs.minAckCursor.CompareAndSwap(tag, tag+1)
	}
	qs.NotifyNewMessage()
}

func (qs *QueueState) Recover(minTag, maxTag, count uint64) {
	qs.tail.Store(minTag)
	if maxTag < minTag {
		maxTag = minTag
	}
	qs.minAckCursor.Store(minTag)
	qs.inflight.Store(0)
	// Store `waiting` BEFORE `head`: head is the visibility gate the SQ-9 reaper
	// scans against (it walks tail→head), so publishing the recovered depth first
	// closes the window where a reaper sweep between the two stores could decrement
	// `waiting` only to have it clobbered by a later waiting.Store. Under Go's
	// sequentially-consistent atomics, a reaper that observes the new head is then
	// guaranteed to observe the already-set waiting, and its ReapDrop applies on top.
	qs.waiting.Store(int64(count))
	qs.head.Store(maxTag + 1)
	qs.requeueMu.Lock()
	qs.requeueBuf = make([]requeueEntry, requeueInitialCap)
	qs.requeueHead = 0
	qs.requeueLen = 0
	qs.requeueCount.Store(0)
	qs.requeueMu.Unlock()
	qs.NotifyNewMessage()
}

func (qs *QueueState) Depth() uint64 {
	w := qs.waiting.Load()
	if w < 0 {
		w = 0
	}
	i := qs.inflight.Load()
	if i < 0 {
		i = 0
	}
	return uint64(w + i)
}

func (qs *QueueState) DepthHighWM() uint64 {
	return qs.depthHighWM.Load()
}

func (qs *QueueState) AtHighWaterMark() bool {
	wm := qs.depthHighWM.Load()
	if wm == 0 {
		return false
	}
	return qs.Depth() >= wm
}

func (qs *QueueState) SetMinAckCursor(val uint64) {
	for {
		cur := qs.minAckCursor.Load()
		if val <= cur {
			return
		}
		if qs.minAckCursor.CompareAndSwap(cur, val) {
			return
		}
	}
}

func (qs *QueueState) Close() {
	if !qs.closed.CompareAndSwap(false, true) {
		return
	}
	close(qs.stopCh)
	for i := 0; i < 256; i++ {
		select {
		case qs.wake <- struct{}{}:
		default:
			return
		}
	}
}

func (qs *QueueState) StopCh() <-chan struct{} {
	return qs.stopCh
}

func (qs *QueueState) Head() uint64         { return qs.head.Load() }
func (qs *QueueState) Tail() uint64         { return qs.tail.Load() }
func (qs *QueueState) InflightCount() int64 { return qs.inflight.Load() }
func (qs *QueueState) WaitingCount() int64  { return qs.waiting.Load() }
func (qs *QueueState) RequeueDepth() int64  { return qs.requeueCount.Load() }
func (qs *QueueState) ParkedCount() int64   { return qs.parkedCount.Load() }

// ReadyBytes returns the tracked total body bytes of ready messages, used by
// SQ-11 for x-max-length-bytes enforcement. A single atomic load.
func (qs *QueueState) ReadyBytes() int64 { return qs.readyBytes.Load() }

// AddReadyBytes adds n to the ready-bytes counter and returns the new value.
// SQ-11 calls this on enqueue. Lockless.
func (qs *QueueState) AddReadyBytes(n int64) int64 { return qs.readyBytes.Add(n) }

// SubReadyBytes subtracts n from the ready-bytes counter and returns the new
// value. SQ-11 calls this when a ready message is claimed, acked, or evicted.
// Lockless.
func (qs *QueueState) SubReadyBytes(n int64) int64 { return qs.readyBytes.Add(-n) }
