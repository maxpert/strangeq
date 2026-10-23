package server

import "github.com/maxpert/amqp-go/protocol"

// UnifiedBroker defines the interface that both broker implementations must satisfy
// This allows the Server to work with either the original in-memory broker or the storage-backed broker
type UnifiedBroker interface {
	// Exchange operations
	DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error
	DeleteExchange(name string, ifUnused bool) error

	// Queue operations
	DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error)
	DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error)

	// Binding operations
	BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error
	UnbindQueue(queueName, exchangeName, routingKey string) error
	BindExchange(destination, source, routingKey string, arguments map[string]interface{}) error
	UnbindExchange(destination, source, routingKey string) error

	// Message operations
	PublishMessage(exchangeName, routingKey string, message *protocol.Message) error

	// PublishMessageAsyncConfirm is the async-completion publish path for durable
	// and/or publisher-confirm messages: it routes synchronously (returning
	// ErrNoRoute / ErrMaxLengthExceeded inline) but enqueues a durable copy's WAL
	// write for group commit and defers the copy's visibility to the fsync
	// completion, firing onDurable once every durable copy is fsynced. Returns
	// durableInflight=true iff onDurable will fire asynchronously (the caller must
	// not settle the confirm itself); false for transient / no-route-non-mandatory
	// success (the caller settles the confirm synchronously).
	//
	// backpressure is non-nil iff a routed target queue is at/over its high-water
	// mark: the caller (a producer-only connection) records it so the reader stops
	// pulling new publishes off the socket until the consumer drains the queue
	// (iteration 2, option A — reader-level depth backpressure that paces the
	// producer, keeping the durable queue small + ring-resident). The publish is
	// still admitted (it always durablizes + confirms); the processor is never
	// parked, so the confirm tag is never stranded. nil means capacity headroom.
	PublishMessageAsyncConfirm(exchangeName, routingKey string, message *protocol.Message, onDurable func(error)) (durableInflight bool, backpressure protocol.DepthGate, err error)

	// Consumer operations. Every consumerID below is the BROKER-INTERNAL
	// consumer identity (protocol.Consumer.ID), never the client-visible
	// consumer tag: AMQP 0-9-1 scopes the tag per channel, so it cannot key a
	// broker-wide map without letting one client's registration overwrite
	// another's.
	RegisterConsumer(queueName, consumerID string, consumer *protocol.Consumer) error
	UnregisterConsumer(consumerID string) error

	// Acknowledgment operations
	AcknowledgeMessage(consumerID string, deliveryTag uint64, multiple bool) error
	RejectMessage(consumerID string, deliveryTag uint64, requeue bool) error
	NacknowledgeMessage(consumerID string, deliveryTag uint64, multiple, requeue bool) error

	// Recovery — requeue all unacked messages for a consumer (basic.recover)
	RequeueAllForConsumer(consumerID string) error

	// Delivery lookup (NEW - for O(1) ACK routing). Returns the owning
	// consumer's internal identity, or "" for a basic.get delivery.
	GetConsumerForDelivery(deliveryTag uint64) (string, bool)

	// Synchronous message retrieval (basic.get)
	GetMessageForGet(queueName string, noAck bool) (*protocol.Message, uint64, uint32, error)
	// GetMessageForGetOwned is GetMessageForGet with the owning server
	// connection ID, so connection close requeues only its own gets (WS-B §10).
	GetMessageForGetOwned(queueName string, noAck bool, connID string) (*protocol.Message, uint64, uint32, error)

	// Queue purge (queue.purge)
	PurgeQueue(name string) (int, error)

	// Acknowledgment for basic.get deliveries (empty consumer tag)
	AcknowledgeGetDelivery(deliveryTag uint64) error
	RejectGetDelivery(deliveryTag uint64, requeue bool) error
	NackGetDelivery(deliveryTag uint64, requeue bool) error

	// Requeue the in-flight basic.get deliveries owned by connID on connection
	// close (owner-less deliveries are never touched).
	RequeueGetDeliveries(connID string)

	// Recovery support — initializes a queue's dispatch cursor from the
	// recovered message tag range [minTag, maxTag], making recovered
	// messages immediately claimable by consumers. Replaces the old
	// per-message EnqueueRecoveredMessage.
	RecoverQueue(queueName string, minTag, maxTag, count uint64)

	// Recovery support — rebuilds delivery index entry for a recovered pending ack
	RebuildDeliveryIndex(deliveryTag uint64, consumerID string)

	// Management operations (optional for some implementations).
	// GetConsumers is keyed by internal consumer identity; each value's Tag
	// field carries the client-visible tag.
	GetQueues() map[string]*protocol.Queue
	GetExchanges() map[string]*protocol.Exchange
	GetConsumers() map[string]*protocol.Consumer

	UpdateConsumerPrefetch(consumerID string, prefetchCount uint16)
}
