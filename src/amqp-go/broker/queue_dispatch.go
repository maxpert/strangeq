package broker

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	requeueInitialCap         = 4096
	defaultCapacityFallback   = 10 * time.Millisecond
	maxCapacityFallbackMillis = ^uint32(0)
)

type requeueEntry struct {
	tag         uint64
	redelivered bool
}

type QueueState struct {
	// ordinalBase is this queue's composite-tag ordinal pre-shifted into
	// position (PackTag(ordinal, 0)) so minting is a single OR. Set exactly
	// once, before any publish or claim on this queue, by SetOrdinal. Zero
	// value (0) is only ever observed if SetOrdinal was never called, which
	// is a caller bug — see SetOrdinal's doc comment.
	ordinalBase uint64

	// nextSeq is this queue's private, zero-based, dense delivery-tag
	// sequence (broker/tag_packing.go). It is guarded by frontierMu:
	// FrontierReserve is the ONLY minter and already holds frontierMu for its
	// other bookkeeping, so nextSeq rides along for free. Deliberately NOT
	// atomic — an atomic here would let a reader observe it out of step with
	// the frontierPending append it must stay coupled to.
	nextSeq uint64

	tail                   atomic.Uint64
	head                   atomic.Uint64
	minAckCursor           atomic.Uint64
	waiting                atomic.Int64
	inflight               atomic.Int64
	wake                   chan struct{}
	parkedCount            atomic.Int64
	producerParked         atomic.Int64
	requeueMu              sync.Mutex
	requeueBuf             []requeueEntry
	requeueHead            int
	requeueLen             int
	requeueCount           atomic.Int64
	depthHighWM            atomic.Uint64
	closed                 atomic.Bool
	capacityFallbackMillis uint32 // configured before concurrent use; see SetCapacityFallback
	stopCh                 chan struct{}
	parkTimeout            time.Duration

	// readyBytes tracks the total body bytes of ready (not-yet-delivered)
	// messages for x-max-length-bytes enforcement (SQ-11). It is left at zero
	// and untouched by the dispatch hot paths in W1 — SQ-11 wires the add on
	// enqueue and the sub on claim/ack/evict — so an unset max-length-bytes
	// policy costs nothing. Lockless atomic.
	readyBytes atomic.Int64

	// maxLenMu serializes the reject-publish (x-overflow=reject-publish)
	// admission decision — the read of WaitingCount()/ReadyBytes() and the
	// matching FrontierComplete()/AddReadyBytes() increment — so concurrent publishers
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
	// EVERY publish routes through the frontier — there is no longer a
	// conditional fast path to select between, so no activation flag is needed.
	// frontierMu guards the frontier structures and is held ONLY for
	// O(1)-amortized bookkeeping, never across I/O, so a durable completion can
	// drive it from the WAL batch-writer goroutine (A4). frontierPending holds
	// durable tags awaiting fsync in increasing (registration) order;
	// frontierDone maps a completed tag to whether it is a REAL (delivered)
	// message (true) or a dropped fsync-error tag (false, never delivered —
	// gap-skipped). frontierMax is the highest tag registered.
	frontierMu      sync.Mutex
	frontierPending []uint64
	frontierPHead   int
	frontierDone    map[uint64]bool
	frontierMax     uint64

	// SQ-11 reject-publish reservations (WS-D §7): admitted-but-not-yet-counted
	// copies. Incremented ONLY under maxLenMu (admission); transferred into
	// waiting/readyBytes atomically with the frontier pop that makes the tag
	// visible (FrontierComplete, under frontierMu), or released if the tag
	// completes real=false. frontierResv (guarded by frontierMu, nil unless the
	// queue has ever reserved) maps a reserved tag to its reserved body bytes.
	pendingCount atomic.Int64
	pendingBytes atomic.Int64
	frontierResv map[uint64]int64
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
		wake:                   make(chan struct{}, 128),
		capacityFallbackMillis: uint32(defaultCapacityFallback / time.Millisecond),
		stopCh:                 make(chan struct{}),
		parkTimeout:            1 * time.Millisecond,
		requeueBuf:             make([]requeueEntry, requeueInitialCap),
	}
	qs.depthHighWM.Store(depthHighWM)
	return qs
}

func (qs *QueueState) SetDepthHighWM(wm uint64) {
	qs.depthHighWM.Store(wm)
}

// SetOrdinal assigns this queue's composite-tag ordinal (broker/tag_packing.go)
// and MUST be called exactly once, before any publish or claim on this queue
// (the broker's getOrCreateQueueState cold path does this under its creation
// mutex, before the QueueState is published to other goroutines).
//
// CRITICAL: this also seeds tail, head, and minAckCursor to ordinalBase. These
// atomic.Uint64 cursors zero-value to 0, but this queue's tags all live at
// ordinal<<OrdinalShift and up — if the cursors were left at 0, Claim would
// see tail=0 < head=ordinalBase+1 and gap-crawl the ENTIRE unused tag range
// below this queue's ordinal (up to ~8.8e13 tags for a mid-range ordinal)
// one CAS at a time. That is a hang far worse than the starvation bug this
// design fixes, and it is the single easiest way to get this design
// catastrophically wrong — so every cursor a fresh queue starts from must be
// seeded here, together, before anything else touches this QueueState.
func (qs *QueueState) SetOrdinal(ordinal uint64) {
	qs.ordinalBase = PackTag(ordinal, 0)
	qs.tail.Store(qs.ordinalBase)
	qs.head.Store(qs.ordinalBase)
	qs.minAckCursor.Store(qs.ordinalBase)
}

// Ordinal returns this queue's composite-tag ordinal (broker/tag_packing.go),
// as assigned by SetOrdinal. Read-only; ordinalBase is set exactly once
// before this QueueState is published to other goroutines (see SetOrdinal's
// doc comment), so no synchronization is needed to read it afterward.
// Recovery uses this to assert every recovered tag's ordinal actually
// matches the queue it was recovered into (server/recovery_manager.go) —
// the single safeguard against silent cross-queue delivery-tag collision
// from ordinal drift or a pre-packing (legacy) data directory.
func (qs *QueueState) Ordinal() uint64 {
	return TagOrdinal(qs.ordinalBase)
}

// TagBand returns the inclusive composite-delivery-tag range this queue
// INCARNATION owns (broker/tag_packing.go).
//
// It exists so a control-plane mutator can be handed an INCARNATION rather than
// only a name. Queue ordinals are never reused within a broker run, so a
// destructive range-scoped operation carrying one incarnation's band cannot
// touch a successor that later took the same name — which is what makes the
// purge path safe without holding a lock across it.
//
// DO NOT read this as "every message in this queue's ring lies in this band".
// An earlier version of this comment claimed exactly that, on the grounds that
// FrontierReserve is the only minter and mints from ordinalBase. The minting is
// in-band; the STORING is not. republishToTargets (broker/dead_letter.go) and
// PublishMessage both resolve a target QueueState, mint from it, and then store
// BY NAME with nothing revalidating in between — so a delete+redeclare in that
// gap lands a predecessor-band tag in the successor's ring. That race is the
// deliberately deferred half of B-2, so out-of-band residents are reachable
// today. See purgeIncarnation for what that costs.
func (qs *QueueState) TagBand() (minTag, maxTag uint64) {
	return qs.ordinalBase, qs.ordinalBase | SeqMask
}

func (qs *QueueState) SetParkTimeout(d time.Duration) {
	qs.parkTimeout = d
}

// SetCapacityFallback overrides WaitForCapacity's safety-net wake interval.
// Set it before the QueueState is used concurrently, like SetParkTimeout.
// The compact millisecond representation preserves QueueState's dispatch-plane
// layout while keeping this override per queue. Production leaves the
// constructor's 10ms default unchanged; tests can move the fallback out of
// reach to attribute a wake to NotifyNewMessage.
func (qs *QueueState) SetCapacityFallback(d time.Duration) {
	if d <= 0 || d%time.Millisecond != 0 || uint64(d/time.Millisecond) > uint64(maxCapacityFallbackMillis) {
		panic("QueueState.SetCapacityFallback: duration must be a positive whole number of milliseconds fitting uint32")
	}
	qs.capacityFallbackMillis = uint32(d / time.Millisecond)
}

func (qs *QueueState) capacityFallback() time.Duration {
	return time.Duration(qs.capacityFallbackMillis) * time.Millisecond
}

func (qs *QueueState) WaitForCapacity(stop <-chan struct{}) bool {
	// Teardown must be consulted BEFORE the below-HWM fast return. A queue torn
	// down by DeleteQueue — or the record-less state createQueueStateLocked
	// returns for a name with no metadata record — has depth 0, so it is never
	// AtHighWaterMark and would otherwise sail straight through this gate: the
	// publish would be durably written and CONFIRMED, then discarded at the next
	// restart as a deleted queue's records. This is THE teardown gate for the
	// three publish paths that have no explicit StopCh select of their own
	// (PublishMessage, fanoutSharedSync, PublishMessageTx).
	if qs.closed.Load() {
		return false
	}
	// NOT redundant with the check above, despite Close() setting `closed`
	// before it closes stopCh. The two statements are not atomic together, so
	// a Close() landing BETWEEN them is caught here and nowhere else:
	//   this goroutine: closed.Load() -> false
	//   Close():        closed.CAS(false,true); close(stopCh)
	//   this goroutine: select -> stop is closed -> refuse
	// Without this select that interleaving falls through to the below-HWM
	// fast return and publishes into a queue that is already tearing down.
	// It narrows — it cannot close — the inherent race where the whole gate
	// runs before Close() begins; that residual window is spec-sanctioned
	// (queue.delete destroys enqueued messages) and leaves no record-less
	// queue. Do not delete this as dead code.
	select {
	case <-stop:
		return false
	default:
	}
	if !qs.AtHighWaterMark() {
		return true
	}
	timer := time.NewTimer(qs.capacityFallback())
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
		timer.Reset(qs.capacityFallback())
		select {
		case <-qs.wake:
		case <-stop:
			return false
		case <-timer.C:
		}
	}
	return !qs.closed.Load()
}

// FrontierReserve is the atomic {mint tag + register pending} step for a publish
// routed to this queue. EVERY publish — durable or transient — goes through it;
// it is this queue's ONLY tag minter. The tag is assigned from this queue's own
// dense, zero-based sequence (nextSeq), packed with this queue's ordinal
// (broker/tag_packing.go), WHILE frontierMu is held, so this queue's frontier
// tags are registered strictly in tag order (the frontier ring stays increasing)
// and no completion can advance head past this tag before it is registered. The
// tag is recorded pending (not yet visible — head is not advanced); the caller
// marks it ready via FrontierComplete(tag, true) after the message is stored, or
// releases it via FrontierComplete(tag, false) if the store fails. Returns the
// assigned tag.
//
// Panics if this queue's sequence would exceed SeqMask — silent wraparound
// would collide with the next queue's ordinal bits (cross-queue data
// corruption), and at 44 bits of per-queue sequence this is unreachable in
// practice (see broker/tag_packing.go).
func (qs *QueueState) FrontierReserve() uint64 {
	qs.frontierMu.Lock()
	seq := qs.nextSeq
	if seq > SeqMask {
		qs.frontierMu.Unlock()
		panic("QueueState.FrontierReserve: per-queue delivery-tag sequence exhausted (SeqMask overflow)")
	}
	qs.nextSeq++
	tag := qs.ordinalBase | seq
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
	// Count the popped real tags ready ATOMICALLY WITH THE POP, before releasing
	// frontierMu. This upholds the invariant Recover already documents ("Store
	// `waiting` BEFORE `head`: head is the visibility gate the SQ-9 reaper scans
	// against"): a real tag must be counted before any casMaxHead capable of
	// exposing it can run.
	//
	// Counting after the unlock — even immediately before casMaxHead — leaves a
	// CROSS-CALL hole that is the actual defect: this call can pop tags, unlock,
	// and stall, and then a DIFFERENT publisher's casMaxHead exposes those
	// popped-but-uncounted tags. Under x-message-ttl they are already expired
	// when exposed, so the reaper and drop-head eviction delete them at once.
	// Those decrements are legitimate but uncompensated, so `waiting` dives
	// negative, and a reaper win during the excursion hits ReapDrop's clamp,
	// which rewrites the whole negative balance to 0 and destroys every in-flight
	// credit at once. The stalled +N then lands unopposed, leaving `waiting`
	// permanently above the true ready count. Popping and counting under one lock
	// closes it: visible-real implies counted, so the gated decrementers can
	// never drive `waiting` below zero and the clamp can never erase live
	// credits. The clamp stays as a dead-man's guard against genuine double
	// decrements. casMaxHead deliberately remains OUTSIDE the lock — frontierMu
	// is never held across anything but O(1) bookkeeping (invariant A4).
	if newlyReal > 0 {
		qs.waiting.Add(int64(newlyReal))
	}
	qs.frontierMu.Unlock()

	qs.casMaxHead(newHead)
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
	qs.RecoverSeq(TagSeq(maxTag) + 1)
	qs.NotifyNewMessage()
}

// RecoverSeq restores this queue's private delivery-tag sequence (nextSeq)
// after recovery, so the next FrontierReserve mints strictly above every tag
// this queue is about to re-expose as recovered. Called by Recover with
// TagSeq(maxTag)+1 — the caller (server/recovery_manager.go) has already
// asserted TagOrdinal(minTag)==TagOrdinal(maxTag)==this queue's ordinal
// before Recover runs, so maxTag's low bits are a genuine per-queue sequence
// value, not a foreign ordinal's bits.
//
// Guarded by frontierMu because nextSeq is otherwise only ever touched by
// FrontierReserve under that same lock (see nextSeq's field doc); Recover
// runs once, before any consumer/producer touches this queue, but taking the
// lock here costs nothing and keeps nextSeq's single-writer-under-frontierMu
// invariant exceptionless.
//
// SAFETY of leaving nextSeq at its zero value when this queue recovers ZERO
// messages (Recover is never called at all — see RecoverQueue's doc comment
// and server/recovery_manager.go, which only calls it when hasRecovered):
// storage/wal_manager.go RecoverFromWAL and storage/segment_manager.go
// RecoverFromSegments do NOT exclude a record from the recovered set because
// it was acked — RecoverFromWAL filters against the WAL's own ackBitmap,
// which is this incarnation's live ack set and is therefore still empty at
// boot, and RecoverFromSegments filters nothing at all. So a not-yet-reclaimed
// record is ALWAYS returned regardless of ack status. The only way a record is
// excluded from RECOVERY is physical absence: the WAL file was deleted
// (performCheckpoint / tryDeleteOldFiles, both of which only remove a record
// once it is ACKed) or the segment was compacted (compactSegment, same
// precondition). Therefore "this queue recovered zero messages" is exactly
// equivalent to "every record this queue ever wrote is physically gone from
// disk" — reused low sequence numbers have nothing left anywhere to collide
// with. Any record still physically present (acked or not) comes back through
// this exact path with its real tag inside [minTag, maxTag], and nextSeq above
// resumes past it. No per-queue mirror of a persisted delivery-tag counter is
// needed to close this gap.
//
// THE UNWRITTEN PREMISE, made explicit because a feature nearly falsified it:
// everything above reduces tag-reuse safety to "a tag is only dangerous while
// its bytes are physically present". That reduction is sound only while NO
// durable, tag-keyed state outlives those bytes. Today none does —
// acknowledgement is not persisted (storage/wal_boot_state.go), so the WAL's
// ackBitmap is empty at every boot and nothing survives a record's reclamation
// that still refers to its tag.
//
// A durable ack snapshot was built, reviewed three times and WITHDRAWN for
// exactly this reason: a drained queue whose WAL files were reclaimed recovers
// zero records, resumes at seq 0, and re-mints tags straight into an inherited
// ack set — silently destroying every message published on them (measured:
// 198/200). Intersecting the ack set with the physically-present offsets does
// not fix it, because the intersection compares TAG VALUES, and tag values are
// re-mintable by design; it delays the hazard by one incarnation. Anything
// added here that is durable AND keyed by delivery tag must first solve
// attribution to the RECORD an ack cancels, not to its tag. See
// .notes/loop-2/deferred-ack-durability.md.
func (qs *QueueState) RecoverSeq(nextSeq uint64) {
	qs.frontierMu.Lock()
	qs.nextSeq = nextSeq
	qs.frontierMu.Unlock()
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
