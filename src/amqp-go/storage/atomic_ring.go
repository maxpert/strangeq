package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/maxpert/amqp-go/protocol"
)

const DefaultAtomicRingSize = 1 << 16

type AtomicRing struct {
	slots        []atomic.Pointer[protocol.Message]
	size         int
	mask         uint64
	publishSeq   atomic.Uint64
	messageCount atomic.Uint64
	tagToSeq     map[uint64]uint64
	tagMu        sync.RWMutex
	closed       atomic.Bool
}

func NewAtomicRing(size int) *AtomicRing {
	if size <= 0 {
		size = DefaultAtomicRingSize
	}
	if size&(size-1) != 0 {
		panic("AtomicRing: size must be a power of two")
	}
	r := &AtomicRing{
		size:     size,
		mask:     uint64(size - 1),
		tagToSeq: make(map[uint64]uint64, size),
	}
	r.slots = make([]atomic.Pointer[protocol.Message], size)
	return r
}

func (r *AtomicRing) Store(deliveryTag uint64, msg *protocol.Message) (seq uint64, spilled bool, err error) {
	if msg == nil {
		return 0, false, fmt.Errorf("message is nil")
	}
	if r.closed.Load() {
		return 0, false, fmt.Errorf("ring is closed")
	}
	seq = r.publishSeq.Add(1) - 1
	msg.DeliveryTag = deliveryTag
	if r.slots[seq&r.mask].CompareAndSwap(nil, msg) {
		r.tagMu.Lock()
		r.tagToSeq[deliveryTag] = seq
		r.tagMu.Unlock()
		r.messageCount.Add(1)
		return seq, false, nil
	}
	return seq, true, nil
}

func (r *AtomicRing) LoadByTag(deliveryTag uint64) (*protocol.Message, bool) {
	r.tagMu.RLock()
	seq, ok := r.tagToSeq[deliveryTag]
	r.tagMu.RUnlock()
	if !ok {
		return nil, false
	}
	msg := r.slots[seq&r.mask].Load()
	if msg != nil && msg.DeliveryTag == deliveryTag {
		return msg, true
	}
	return nil, false
}

func (r *AtomicRing) LoadBySeq(seq uint64) (*protocol.Message, bool) {
	msg := r.slots[seq&r.mask].Load()
	if msg != nil {
		r.tagMu.RLock()
		s, ok := r.tagToSeq[msg.DeliveryTag]
		r.tagMu.RUnlock()
		if ok && s == seq {
			return msg, true
		}
	}
	return nil, false
}

func (r *AtomicRing) Delete(deliveryTag uint64) bool {
	r.tagMu.RLock()
	seq, ok := r.tagToSeq[deliveryTag]
	r.tagMu.RUnlock()
	if !ok {
		return false
	}
	slot := &r.slots[seq&r.mask]
	msg := slot.Load()
	if msg == nil || msg.DeliveryTag != deliveryTag {
		return false
	}
	if slot.CompareAndSwap(msg, nil) {
		r.messageCount.Add(^uint64(0))
		r.tagMu.Lock()
		delete(r.tagToSeq, deliveryTag)
		r.tagMu.Unlock()
		return true
	}
	return false
}

func (r *AtomicRing) Purge() int {
	removed := 0
	for i := range r.slots {
		if r.slots[i].Swap(nil) != nil {
			removed++
		}
	}
	for {
		cur := r.messageCount.Load()
		if cur < uint64(removed) {
			if r.messageCount.CompareAndSwap(cur, 0) {
				break
			}
			continue
		}
		if r.messageCount.CompareAndSwap(cur, cur-uint64(removed)) {
			break
		}
	}
	r.tagMu.Lock()
	r.tagToSeq = make(map[uint64]uint64, r.size)
	r.tagMu.Unlock()
	return removed
}

func (r *AtomicRing) Count() uint64 {
	return r.messageCount.Load()
}

func (r *AtomicRing) Size() int {
	return r.size
}

func (r *AtomicRing) GetAll() []*protocol.Message {
	result := make([]*protocol.Message, 0, int(r.messageCount.Load()))
	for i := range r.slots {
		if msg := r.slots[i].Load(); msg != nil {
			result = append(result, msg)
		}
	}
	return result
}

func (r *AtomicRing) GetRange(startTag, endTag uint64) []*protocol.Message {
	if startTag > endTag {
		return nil
	}
	result := make([]*protocol.Message, 0)
	for i := range r.slots {
		msg := r.slots[i].Load()
		if msg != nil && msg.DeliveryTag >= startTag && msg.DeliveryTag <= endTag {
			result = append(result, msg)
		}
	}
	return result
}

func (r *AtomicRing) DeleteRange(startTag, endTag uint64) int {
	if startTag > endTag {
		return 0
	}
	removed := 0
	for i := range r.slots {
		msg := r.slots[i].Load()
		if msg == nil || msg.DeliveryTag < startTag || msg.DeliveryTag > endTag {
			continue
		}
		if r.slots[i].CompareAndSwap(msg, nil) {
			r.messageCount.Add(^uint64(0))
			r.tagMu.Lock()
			delete(r.tagToSeq, msg.DeliveryTag)
			r.tagMu.Unlock()
			removed++
		}
	}
	return removed
}

func (r *AtomicRing) Close() {
	r.closed.Store(true)
}
