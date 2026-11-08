package broker

// This file declares ONLY the frozen management-side view types of
// `.notes/specs/mgmt-03-broker-accessors.md` §7. The accessor METHODS that
// populate them (QueueMessageCounts, ConsumerViews, SetEntityObserver) are the
// broker-side work of spec mgmt-03 (BOB-1/BOB-8/BOB-11) and are intentionally
// NOT implemented here: the management package discovers them through the
// optional interfaces `queueMessageTotals`, `consumerViews`, and
// `EntityObserver` and degrades to `null`/`stats_partial` when a broker lacks
// them. The types live in package broker so the discovery interfaces can name
// the exact frozen signature.

// QueueMessageCounts is the instantaneous depth of one queue, read from the
// queue's lock-free QueueState atomics. Zero-valued with Known==false for an
// unknown queue (the accessor never creates queue state).
type QueueMessageCounts struct {
	Ready   int64 // waiting: published/requeued, available for delivery
	Unacked int64 // inflight: delivered, not yet settled (ack/nack/reject)
	Total   int64 // Ready + Unacked (RabbitMQ `messages`)
	Known   bool  // false => the queue has no live QueueState in this process
}

// ConsumerView is the management view of one live consumer. ID is the
// broker-internal identity (the activeConsumers key); it MUST NOT be
// serialised (DATA-030). ChannelID/ConnectionID are the linkage values that
// were null until BOB-8.
type ConsumerView struct {
	ID            string
	Tag           string // client-visible; NOT unique broker-wide
	Queue         string
	NoAck         bool
	Exclusive     bool
	Args          map[string]interface{} // raw AMQP field table
	PrefetchCount uint16

	HasChannel   bool   // false => Consumer.Channel was nil at registration
	ChannelID    uint16 // owning channel number
	ConnectionID string // owning connection id ("" when HasChannel==false)

	Unacked  int64 // see mgmt-03 §2.1
	AckKnown bool  // false => Unacked is unknown; emit JSON null, never 0
}

// EntityObserver is notified after every queue/exchange/consumer registration
// or removal. kind is exactly "queue", "exchange", or "consumer"; delta is +1
// or -1. The call is made OUTSIDE any broker lock, on the mutating goroutine,
// after the registry has changed.
type EntityObserver interface {
	OnBrokerEntityChange(kind string, delta int64)
}
