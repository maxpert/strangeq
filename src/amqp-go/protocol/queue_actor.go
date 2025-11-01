package protocol

import (
	"container/list"
	"sync/atomic"
)

const (
	QueueInboxSize      = 50000 // 250ms burst protection at 200K msg/s
	ConsumerChannelSize = 100   // Matches AMQP prefetch semantics
)

// QueueCommand represents commands sent to the queue actor
type QueueCommand interface {
	Execute(*QueueActor)
}

// EnqueueCmd adds a message to the queue
type EnqueueCmd struct {
	DeliveryTag uint64
	Message     *Message
}

func (c *EnqueueCmd) Execute(q *QueueActor) {
	q.messages.PushBack(&QueueMessage{
		DeliveryTag: c.DeliveryTag,
		Message:     c.Message,
	})
	q.deliverMessages()
}

// RegisterConsumerCmd registers a new consumer
type RegisterConsumerCmd struct {
	Consumer *Consumer
}

func (c *RegisterConsumerCmd) Execute(q *QueueActor) {
	state := &ConsumerState{
		Consumer:   c.Consumer,
		DeliveryCh: c.Consumer.Messages,
		Unacked:    0,
	}
	q.consumers[c.Consumer.Tag] = state
	q.deliverMessages()
}

// UnregisterConsumerCmd removes a consumer
type UnregisterConsumerCmd struct {
	ConsumerTag string
}

func (c *UnregisterConsumerCmd) Execute(q *QueueActor) {
	delete(q.consumers, c.ConsumerTag)
}

// AckCmd acknowledges a message delivery
type AckCmd struct {
	ConsumerTag string
}

func (c *AckCmd) Execute(q *QueueActor) {
	if state, ok := q.consumers[c.ConsumerTag]; ok {
		if state.Unacked > 0 {
			state.Unacked--
		}
		q.deliverMessages()
	}
}

// QueueMessage wraps a message with its delivery tag
type QueueMessage struct {
	DeliveryTag uint64
	Message     *Message
}

// ConsumerState tracks per-consumer delivery state
type ConsumerState struct {
	Consumer   *Consumer
	DeliveryCh chan *Delivery
	Unacked    int
}

// QueueActor implements actor-based queue (RabbitMQ-style)
// Single goroutine processes all commands - NO LOCKS!
type QueueActor struct {
	Name        string
	Inbox       chan QueueCommand
	messages    *list.List // Unbounded message storage
	consumers   map[string]*ConsumerState
	running     atomic.Bool
	deliveryTag uint64
}

// NewQueueActor creates and starts a new queue actor
func NewQueueActor(name string) *QueueActor {
	q := &QueueActor{
		Name:      name,
		Inbox:     make(chan QueueCommand, QueueInboxSize),
		messages:  list.New(),
		consumers: make(map[string]*ConsumerState),
	}
	q.running.Store(true)
	go q.run()
	return q
}

// run is the actor's main loop - processes commands sequentially
func (q *QueueActor) run() {
	for cmd := range q.Inbox {
		if !q.running.Load() {
			return
		}
		cmd.Execute(q)
	}
}

// deliverMessages attempts to deliver queued messages to consumers
func (q *QueueActor) deliverMessages() {
	if q.messages.Len() == 0 || len(q.consumers) == 0 {
		return
	}

	// Try to deliver to all consumers in round-robin
	for _, state := range q.consumers {
		if q.messages.Len() == 0 {
			break
		}

		// Check prefetch limit
		prefetch := state.Consumer.PrefetchCount
		if prefetch > 0 && state.Unacked >= int(prefetch) {
			continue
		}

		// Get next message
		elem := q.messages.Front()
		if elem == nil {
			break
		}

		qm := elem.Value.(*QueueMessage)
		delivery := &Delivery{
			Message:     qm.Message,
			DeliveryTag: qm.DeliveryTag,
			Redelivered: qm.Message.Redelivered,
			Exchange:    qm.Message.Exchange,
			RoutingKey:  qm.Message.RoutingKey,
			ConsumerTag: state.Consumer.Tag,
		}

		// Try non-blocking send to consumer channel
		select {
		case state.DeliveryCh <- delivery:
			q.messages.Remove(elem)
			if !state.Consumer.NoAck {
				state.Unacked++
			}
		default:
			// Consumer channel full, will retry on next command
		}
	}
}

// Stop gracefully stops the queue actor
func (q *QueueActor) Stop() {
	q.running.Store(false)
	close(q.Inbox)
}

// MessageCount returns the current queue depth
func (q *QueueActor) MessageCount() int {
	// Send command and get response (for stats/monitoring)
	// For now, approximate using atomic counter if needed
	return q.messages.Len()
}
