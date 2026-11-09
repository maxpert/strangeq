package storage

import (
	"sync"
	"sync/atomic"
)

// consumerAckState is one consumer's unacked delivery-tag set. The set is the
// only state the ack machinery needs: multi-ack (basic.ack multiple=true),
// basic.nack, and consumer-cancel requeue-all all enumerate it through
// GetUnackedTags, and the prefetch gate reads its size through GetUnackedCount.
type consumerAckState struct {
	unacked sync.Map // tag -> struct{}
}

// AckCursor tracks the delivery tags a queue has published and the per-consumer
// sets of tags that have been delivered but not yet settled.
//
// It deliberately keeps NO running "minimum unacked tag" cursor. That cursor
// (a lowestUnacked atomic per consumer plus a dirty-flag-triggered O(consumers)
// recompute) was maintained on every deliver and every ack and then read by the
// broker's ack path — but nothing ever consumed the value for a decision:
//
//   - the storage cursor was only mirrored into QueueState.minAckCursor, which
//     is written but never read (Depth() is waiting+inflight; see
//     QueueState.ReapDrop's doc in broker/queue_dispatch.go);
//   - the per-consumer scan it required (a full sync.Map.Range over the
//     consumer's unacked set on every in-order ack) was 83% of a CPU profile of
//     the ack path.
//
// The value is now computed ON DEMAND by MinAckCursor (see its doc); nothing on
// the deliver/ack path calls it.
type AckCursor struct {
	head      atomic.Uint64
	consumers sync.Map // consumerTag -> *consumerAckState
}

func NewAckCursor() *AckCursor {
	return &AckCursor{}
}

func (ac *AckCursor) OnPublish(tag uint64) {
	if tag > ac.head.Load() {
		ac.head.Store(tag)
	}
}

func (ac *AckCursor) OnDeliver(tag uint64, consumerTag string) {
	v, ok := ac.consumers.Load(consumerTag)
	if !ok {
		// Slow path only: a delivery to a consumer that was never registered
		// (the broker always registers first). LoadOrStore must NOT be on the
		// fast path: it evaluates newConsumerAckState() unconditionally, which
		// allocated a throwaway state on EVERY delivery.
		v, _ = ac.consumers.LoadOrStore(consumerTag, &consumerAckState{})
	}
	v.(*consumerAckState).unacked.Store(tag, struct{}{})
}

func (ac *AckCursor) OnAck(tag uint64, consumerTag string) bool {
	v, ok := ac.consumers.Load(consumerTag)
	if !ok {
		return false
	}
	_, ok = v.(*consumerAckState).unacked.LoadAndDelete(tag)
	return ok
}

func (ac *AckCursor) OnNack(tag uint64, consumerTag string) bool {
	return ac.OnAck(tag, consumerTag)
}

func (ac *AckCursor) OnConsumerRegister(consumerTag string) {
	ac.consumers.LoadOrStore(consumerTag, &consumerAckState{})
}

func (ac *AckCursor) OnConsumerUnregister(consumerTag string) {
	ac.consumers.Delete(consumerTag)
}

// MinAckCursor returns the lowest delivery tag still unacked by any registered
// consumer, or the highest published tag when no consumer holds an unacked
// message.
//
// This is an ON-DEMAND scan of every consumer's unacked set, and it exists
// solely to satisfy the interfaces.Storage contract (StorageEngine.GetMinAckCursor).
// It has NO production caller: the broker no longer maintains or reads a
// running min-ack cursor, because no depth/backpressure decision consumes it
// (Depth() is waiting+inflight). Do not call it from the delivery or ack hot
// path — it is O(consumers × window).
func (ac *AckCursor) MinAckCursor() uint64 {
	lowest := ^uint64(0)
	ac.consumers.Range(func(_, v any) bool {
		v.(*consumerAckState).unacked.Range(func(k, _ any) bool {
			if t := k.(uint64); t < lowest {
				lowest = t
			}
			return true
		})
		return true
	})
	if lowest == ^uint64(0) {
		return ac.head.Load()
	}
	return lowest
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
