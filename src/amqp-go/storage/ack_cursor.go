package storage

import (
	"sync"
	"sync/atomic"
)

const noLowest = ^uint64(0)

type consumerAckState struct {
	unacked       map[uint64]struct{}
	lowestUnacked atomic.Uint64
}

type AckCursor struct {
	minAckCursor atomic.Uint64
	head         atomic.Uint64
	mu           sync.Mutex
	consumers    map[string]*consumerAckState
}

func NewAckCursor() *AckCursor {
	ac := &AckCursor{
		consumers: make(map[string]*consumerAckState),
	}
	ac.minAckCursor.Store(0)
	ac.head.Store(0)
	return ac
}

func (ac *AckCursor) OnPublish(tag uint64) {
	if tag > ac.head.Load() {
		ac.head.Store(tag)
	}
}

func (ac *AckCursor) OnDeliver(tag uint64, consumerTag string) {
	ac.mu.Lock()
	c := ac.consumers[consumerTag]
	if c == nil {
		c = &consumerAckState{unacked: make(map[uint64]struct{})}
		c.lowestUnacked.Store(noLowest)
		ac.consumers[consumerTag] = c
	}
	c.unacked[tag] = struct{}{}
	if tag < c.lowestUnacked.Load() {
		c.lowestUnacked.Store(tag)
	}
	ac.recomputeMinLocked()
	ac.mu.Unlock()
}

func (ac *AckCursor) OnAck(tag uint64, consumerTag string) bool {
	ac.mu.Lock()
	c := ac.consumers[consumerTag]
	if c == nil {
		ac.mu.Unlock()
		return false
	}
	if _, ok := c.unacked[tag]; !ok {
		ac.mu.Unlock()
		return false
	}
	delete(c.unacked, tag)
	if c.lowestUnacked.Load() == tag {
		c.lowestUnacked.Store(lowestOf(c.unacked))
	}
	ac.recomputeMinLocked()
	ac.mu.Unlock()
	return true
}

func (ac *AckCursor) OnNack(tag uint64, consumerTag string) bool {
	return ac.OnAck(tag, consumerTag)
}

func (ac *AckCursor) OnConsumerRegister(consumerTag string) {
	ac.mu.Lock()
	if ac.consumers[consumerTag] == nil {
		c := &consumerAckState{unacked: make(map[uint64]struct{})}
		c.lowestUnacked.Store(noLowest)
		ac.consumers[consumerTag] = c
	}
	ac.recomputeMinLocked()
	ac.mu.Unlock()
}

func (ac *AckCursor) OnConsumerUnregister(consumerTag string) {
	ac.mu.Lock()
	delete(ac.consumers, consumerTag)
	ac.recomputeMinLocked()
	ac.mu.Unlock()
}

func (ac *AckCursor) MinAckCursor() uint64 {
	return ac.minAckCursor.Load()
}

func (ac *AckCursor) IsSafeToOverwrite(oldTag, newTag uint64, ringSize int) bool {
	mask := uint64(ringSize - 1)
	if oldTag&mask != newTag&mask {
		return true
	}
	return oldTag < ac.MinAckCursor()
}

func (ac *AckCursor) Head() uint64 {
	return ac.head.Load()
}

func (ac *AckCursor) ConsumerCount() int {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return len(ac.consumers)
}

func (ac *AckCursor) GetUnackedTags(consumerTag string) []uint64 {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	c, ok := ac.consumers[consumerTag]
	if !ok {
		return nil
	}
	tags := make([]uint64, 0, len(c.unacked))
	for tag := range c.unacked {
		tags = append(tags, tag)
	}
	return tags
}

func (ac *AckCursor) GetUnackedCount(consumerTag string) int {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	c, ok := ac.consumers[consumerTag]
	if !ok {
		return 0
	}
	return len(c.unacked)
}

func (ac *AckCursor) recomputeMinLocked() {
	lowest := noLowest
	for _, c := range ac.consumers {
		l := c.lowestUnacked.Load()
		if l == noLowest {
			continue
		}
		if l < lowest {
			lowest = l
		}
	}
	if lowest == noLowest {
		ac.minAckCursor.Store(ac.head.Load())
	} else {
		ac.minAckCursor.Store(lowest)
	}
}

func lowestOf(m map[uint64]struct{}) uint64 {
	min := noLowest
	for t := range m {
		if t < min {
			min = t
		}
	}
	return min
}
