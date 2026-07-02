package broker

import (
	"sync"
	"sync/atomic"
	"time"
)

type QueueState struct {
	tail           atomic.Uint64
	head           atomic.Uint64
	minAckCursor   atomic.Uint64
	inflight       atomic.Int64
	wake           chan struct{}
	parkedCount    atomic.Int64
	requeueMu      sync.Mutex
	requeueTags    []uint64
	requeueCount   atomic.Int64
	depthHighWM    atomic.Uint64
	inflightOwners sync.Map
	closed         atomic.Bool
	stopCh         chan struct{}
	parkTimeout    time.Duration
}

func NewQueueState(depthHighWM uint64) *QueueState {
	qs := &QueueState{
		wake:        make(chan struct{}, 128),
		stopCh:      make(chan struct{}),
		parkTimeout: 1 * time.Millisecond,
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

func (qs *QueueState) Claim(stop <-chan struct{}, timer *time.Timer) (uint64, bool) {
	if t, ok := qs.tryPopRequeue(); ok {
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
		if t2, ok := qs.tryPopRequeue(); ok {
			return t2, true
		}
		if !qs.park(stop, timer) {
			return 0, false
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
	if qs.requeueTags == nil {
		qs.requeueTags = make([]uint64, 0, 4096)
	}
	qs.requeueTags = append(qs.requeueTags, tag)
	qs.requeueCount.Add(1)
	qs.requeueMu.Unlock()
	qs.inflight.Add(-1)
	qs.NotifyNewMessage()
}

func (qs *QueueState) tryPopRequeue() (uint64, bool) {
	if qs.requeueCount.Load() <= 0 {
		return 0, false
	}
	qs.requeueMu.Lock()
	if len(qs.requeueTags) == 0 {
		qs.requeueMu.Unlock()
		return 0, false
	}
	tag := qs.requeueTags[0]
	qs.requeueTags = qs.requeueTags[1:]
	qs.requeueCount.Add(-1)
	if len(qs.requeueTags) == 0 {
		qs.requeueTags = nil
	}
	qs.requeueMu.Unlock()
	return tag, true
}

func (qs *QueueState) ClaimInflight(tag uint64) {
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

func (qs *QueueState) Recover(minTag, maxTag uint64) {
	qs.tail.Store(minTag)
	if maxTag < minTag {
		maxTag = minTag
	}
	qs.head.Store(maxTag + 1)
	qs.minAckCursor.Store(minTag)
	qs.inflight.Store(0)
	qs.requeueMu.Lock()
	qs.requeueTags = nil
	qs.requeueCount.Store(0)
	qs.requeueMu.Unlock()
	qs.NotifyNewMessage()
}

func (qs *QueueState) Depth() uint64 {
	h := qs.head.Load()
	c := qs.minAckCursor.Load()
	if h <= c {
		return 0
	}
	return h - c
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

func (qs *QueueState) StoreInflight(msgID uint64, consumerTag string) {
	qs.inflightOwners.Store(msgID, consumerTag)
}

func (qs *QueueState) LoadInflight(msgID uint64) (string, bool) {
	v, ok := qs.inflightOwners.Load(msgID)
	if !ok {
		return "", false
	}
	return v.(string), true
}

func (qs *QueueState) DeleteInflight(msgID uint64) {
	qs.inflightOwners.Delete(msgID)
}

// RangeInflightForConsumer iterates over all in-flight delivery tags owned
// by the given consumer tag. Used during consumer cancellation to find
// messages that have already left consumer.Messages (pulled by the server's
// forwarder goroutine or sent to the client) but have not yet been ACKed.
//
// Tags are collected in a first pass and fn is called in a second pass.
// This is critical because sync.Map.Range does not guarantee visiting all
// keys when entries are deleted concurrently during iteration — including
// by fn itself. Collecting first ensures no tags are skipped.
func (qs *QueueState) RangeInflightForConsumer(consumerTag string, fn func(deliveryTag uint64)) {
	var tags []uint64
	qs.inflightOwners.Range(func(key, value interface{}) bool {
		if value.(string) == consumerTag {
			tags = append(tags, key.(uint64))
		}
		return true
	})
	for _, tag := range tags {
		fn(tag)
	}
}

func (qs *QueueState) Head() uint64         { return qs.head.Load() }
func (qs *QueueState) Tail() uint64         { return qs.tail.Load() }
func (qs *QueueState) InflightCount() int64 { return qs.inflight.Load() }
func (qs *QueueState) RequeueDepth() int64  { return qs.requeueCount.Load() }
func (qs *QueueState) ParkedCount() int64   { return qs.parkedCount.Load() }
