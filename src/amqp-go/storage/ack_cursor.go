package storage

import (
	"sync"
	"sync/atomic"
)

const noLowest = ^uint64(0)

type consumerAckState struct {
	unacked       sync.Map // tag -> struct{}
	lowestUnacked atomic.Uint64
}

type AckCursor struct {
	minAckCursor atomic.Uint64
	head         atomic.Uint64
	consumers    sync.Map // consumerTag -> *consumerAckState
	dirty        atomic.Bool
}

func NewAckCursor() *AckCursor {
	ac := &AckCursor{}
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
	v, _ := ac.consumers.LoadOrStore(consumerTag, newConsumerAckState())
	c := v.(*consumerAckState)
	c.unacked.Store(tag, struct{}{})
	for {
		cur := c.lowestUnacked.Load()
		if cur != noLowest && tag >= cur {
			break
		}
		if c.lowestUnacked.CompareAndSwap(cur, tag) {
			break
		}
	}
	ac.dirty.Store(true)
}

func (ac *AckCursor) OnAck(tag uint64, consumerTag string) bool {
	v, ok := ac.consumers.Load(consumerTag)
	if !ok {
		return false
	}
	c := v.(*consumerAckState)
	if _, ok := c.unacked.LoadAndDelete(tag); !ok {
		return false
	}
	if c.lowestUnacked.Load() == tag {
		c.lowestUnacked.Store(scanLowest(&c.unacked))
	}
	ac.dirty.Store(true)
	return true
}

func (ac *AckCursor) OnNack(tag uint64, consumerTag string) bool {
	return ac.OnAck(tag, consumerTag)
}

func (ac *AckCursor) OnConsumerRegister(consumerTag string) {
	ac.consumers.LoadOrStore(consumerTag, newConsumerAckState())
	ac.dirty.Store(true)
}

func newConsumerAckState() *consumerAckState {
	c := &consumerAckState{}
	c.lowestUnacked.Store(noLowest)
	return c
}

func (ac *AckCursor) OnConsumerUnregister(consumerTag string) {
	ac.consumers.Delete(consumerTag)
	ac.dirty.Store(true)
}

func (ac *AckCursor) MinAckCursor() uint64 {
	if ac.dirty.CompareAndSwap(true, false) {
		ac.recomputeMin()
	}
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
	count := 0
	ac.consumers.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (ac *AckCursor) GetUnackedTags(consumerTag string) []uint64 {
	v, ok := ac.consumers.Load(consumerTag)
	if !ok {
		return nil
	}
	c := v.(*consumerAckState)
	var tags []uint64
	c.unacked.Range(func(k, _ any) bool {
		tags = append(tags, k.(uint64))
		return true
	})
	return tags
}

func (ac *AckCursor) GetUnackedCount(consumerTag string) int {
	v, ok := ac.consumers.Load(consumerTag)
	if !ok {
		return 0
	}
	c := v.(*consumerAckState)
	count := 0
	c.unacked.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (ac *AckCursor) recomputeMin() {
	lowest := noLowest
	ac.consumers.Range(func(_, v any) bool {
		l := v.(*consumerAckState).lowestUnacked.Load()
		if l < lowest {
			lowest = l
		}
		return true
	})
	if lowest == noLowest {
		ac.minAckCursor.Store(ac.head.Load())
	} else {
		ac.minAckCursor.Store(lowest)
	}
}

func scanLowest(m *sync.Map) uint64 {
	min := noLowest
	m.Range(func(k, _ any) bool {
		t := k.(uint64)
		if t < min {
			min = t
		}
		return true
	})
	return min
}
