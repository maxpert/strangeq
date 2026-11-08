package broker

// This file holds the read-only management accessors (BOB-1, BOB-8) and the
// BOB-11 entity-observer seam. Everything here is deliberately lock-free on the
// hot path: it reads sync.Map entries, immutable struct fields and atomics, and
// never takes a Channel/Connection/broker lock. See spec mgmt-03 §4.

// QueueMessageCounts is the instantaneous depth of one queue, read from the
// queue's lock-free QueueState atomics. Zero-valued with Known==false for an
// unknown queue (the accessor never creates queue state).
type QueueMessageCounts struct {
	Ready   int64 // waiting: published/requeued, available for delivery
	Unacked int64 // inflight: delivered, not yet settled (ack/nack/reject)
	Total   int64 // Ready + Unacked (RabbitMQ `messages`)
	Known   bool  // false => the queue has no live QueueState in this process
}

// QueueMessageCounts reports name's instantaneous depths. O(1), lock-free: one
// sync.Map load plus three atomic loads. It never creates queue state, so an
// unknown/uninitialised queue reports Known==false rather than 0-as-a-value.
func (b *StorageBroker) QueueMessageCounts(name string) QueueMessageCounts {
	val, ok := b.queueStates.Load(name)
	if !ok {
		return QueueMessageCounts{}
	}
	qs := val.(*QueueState)

	ready := qs.WaitingCount()
	if ready < 0 {
		ready = 0
	}
	unacked := qs.InflightCount()
	if unacked < 0 {
		unacked = 0
	}
	return QueueMessageCounts{
		Ready:   ready,
		Unacked: unacked,
		Total:   ready + unacked,
		Known:   true,
	}
}

// InflightCount returns the gate's current inflight credit count (delivered,
// not yet settled). Lock-free; it takes no lock the gate itself uses.
func (g *prefetchGate) InflightCount() int64 { return g.inflight.Load() }

// InflightCount returns the total number of messages delivered but not yet
// settled (ack/nack/reject) across every live queue. It is the broker-wide
// aggregate behind amqp_messages_inflight (spec 06 §3.5). Lock-free: one atomic
// load per QueueState, no broker lock, so a scrape never stalls a publisher.
func (b *StorageBroker) InflightCount() int64 {
	var total int64
	b.queueStates.Range(func(_, value interface{}) bool {
		total += value.(*QueueState).InflightCount()
		return true
	})
	return total
}

// ConsumerView is the management view of one live consumer. ID is the
// broker-internal identity (the activeConsumers key); it MUST NOT be
// serialised. ChannelID/ConnectionID are the linkage values that were null
// until BOB-8.
type ConsumerView struct {
	ID            string `json:"-"` // broker-internal activeConsumers key (DATA-030: never serialised)
	Tag           string // client-visible; NOT unique broker-wide
	Queue         string
	NoAck         bool
	Exclusive     bool
	Args          map[string]interface{} // raw AMQP field table
	PrefetchCount uint16

	HasChannel   bool   // false => Consumer.Channel was nil at registration
	ChannelID    uint16 // owning channel number
	ConnectionID string // owning connection id ("" when HasChannel==false)

	Unacked  int64 // see the per-consumer table in spec mgmt-03 §2.1
	AckKnown bool  // false => Unacked is unknown; emit JSON null, never 0
}

// ConsumerViews returns one view per live consumer, in indeterminate order.
// Lock-free: it ranges activeConsumers and reads immutable fields and atomics
// only. It takes no broker lock and never reads a Channel.Mutex-guarded field.
func (b *StorageBroker) ConsumerViews() []ConsumerView {
	var views []ConsumerView
	b.activeConsumers.Range(func(key, value interface{}) bool {
		id := key.(string)
		state := value.(*ConsumerState)
		c := state.consumer

		v := ConsumerView{
			ID:            id,
			Tag:           c.Tag,
			Queue:         c.Queue,
			NoAck:         c.NoAck,
			Exclusive:     c.Exclusive,
			Args:          c.Args,
			PrefetchCount: c.PrefetchCount,
		}
		// Channel/Connection are set once at registration and never mutated.
		if c.Channel != nil {
			v.HasChannel = true
			v.ChannelID = c.Channel.ID
			if c.Channel.Connection != nil {
				v.ConnectionID = c.Channel.Connection.ID
			}
		}

		switch {
		case c.NoAck:
			// No-ack deliveries are settled at send; the unacked set is exactly 0.
			v.Unacked = 0
			v.AckKnown = true
		case c.PrefetchCount > 0:
			// The gate's inflight is exact by construction.
			v.Unacked = state.gate.InflightCount()
			v.AckKnown = true
		default:
			// Manual-ack, prefetch-count 0 ("unlimited"). The gate is capped at
			// a large finite limit rather than bypassed, so gate.inflight would
			// technically be exact — but the spec's contract (mgmt-03 §2.1,
			// C-5) defines prefetch-0's unacked set as UNKNOWN and requires the
			// management layer to render null, never 0. The consumer struct's
			// PrefetchCount is the registration-time value (basic.qos does not
			// update it), so this is a conservative report either way.
			v.Unacked = 0
			v.AckKnown = false
		}

		views = append(views, v)
		return true
	})
	return views
}

// EntityObserver is notified after every queue/exchange/consumer registration
// or removal. kind is exactly "queue", "exchange", or "consumer"; delta is +1
// or -1. The call is made OUTSIDE any broker lock, on the mutating goroutine,
// after the registry has changed.
type EntityObserver interface {
	OnBrokerEntityChange(kind string, delta int64)
}

// SetEntityObserver installs the entity observer. It is set once by the
// server's entity-counter wiring; nil clears it. Lock-free on the mutation
// paths (atomic pointer load).
func (b *StorageBroker) SetEntityObserver(o EntityObserver) {
	if o == nil {
		b.entityObserver.Store(nil)
		return
	}
	b.entityObserver.Store(&o)
}

// notifyEntity fires the installed entity observer, if any. It must be called
// with no broker lock held.
func (b *StorageBroker) notifyEntity(kind string, delta int64) {
	if p := b.entityObserver.Load(); p != nil {
		(*p).OnBrokerEntityChange(kind, delta)
	}
}
