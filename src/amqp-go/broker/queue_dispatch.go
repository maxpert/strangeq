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
	tail         atomic.Uint64
	head         atomic.Uint64
	minAckCursor atomic.Uint64
	waiting      atomic.Int64
	inflight     atomic.Int64
	wake         chan struct{}
	parkedCount  atomic.Int64
	requeueMu    sync.Mutex
	requeueBuf   []requeueEntry
	requeueHead  int
	requeueLen   int
	requeueCount atomic.Int64
	depthHighWM  atomic.Uint64
	closed       atomic.Bool
	stopCh       chan struct{}
	parkTimeout  time.Duration

	// readyBytes tracks the total body bytes of ready (not-yet-delivered)
	// messages for x-max-length-bytes enforcement (SQ-11). It is left at zero
	// and untouched by the dispatch hot paths in W1 — SQ-11 wires the add on
	// enqueue and the sub on claim/ack/evict — so an unset max-length-bytes
	// policy costs nothing. Lockless atomic.
	readyBytes atomic.Int64

	// policy is the queue's resolved x-argument policy (SQ-7), set at declare
	// time (client declare and durable recovery both route through
	// StorageBroker.DeclareQueue). It is attached here — on the per-queue
	// hot-path struct — so Wave 2 enforcement (TTL SQ-9, DLX SQ-10,
	// max-length SQ-11) costs exactly one atomic load plus a nil branch on
	// paths that already hold the QueueState (publish, delivery, reject).
	// nil means "no policy". Lockless by design.
	policy atomic.Pointer[QueuePolicy]
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

func (qs *QueueState) NotifyNewMessage() {
	if qs.parkedCount.Load() <= 0 {
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
	qs.head.Store(maxTag + 1)
	qs.minAckCursor.Store(minTag)
	qs.waiting.Store(int64(count))
	qs.inflight.Store(0)
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
