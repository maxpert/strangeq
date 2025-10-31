package broker

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/maxpert/amqp-go/protocol"
)

// BrokerV2 uses RabbitMQ-style per-queue goroutines and lock-free maps
// This eliminates the global broker lock bottleneck
type BrokerV2 struct {
	// Use sync.Map for lock-free concurrent reads
	Exchanges      sync.Map // string -> *protocol.Exchange (exported for adapter)
	Queues         sync.Map // string -> *QueueProcess (exported for adapter)
	Consumers      sync.Map // consumerTag (string) -> *protocol.Consumer (exported for adapter)
	QueueConsumers sync.Map // queueName (string) -> *consumerList
}

// QueueProcess represents a single queue with its own goroutine (like Erlang process)
type QueueProcess struct {
	Queue         *protocol.Queue // Exported for adapter access
	msgChan       chan *queueMessage
	stopChan      chan struct{}
	consumerRobin atomic.Uint32 // For round-robin consumer selection
}

type queueMessage struct {
	message    *protocol.Message
	resultChan chan<- error // Optional: for synchronous publish confirmation
}

type consumerList struct {
	tags  []string
	mutex sync.RWMutex
}

// NewBrokerV2 creates a new high-performance broker
func NewBrokerV2() *BrokerV2 {
	return &BrokerV2{}
}

// RegisterConsumer registers a new consumer for a queue
func (b *BrokerV2) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	// Check if queue exists
	queueVal, exists := b.Queues.Load(queueName)
	if !exists {
		return errors.New("queue does not exist")
	}

	// Add consumer to broker tracking
	b.Consumers.Store(consumerTag, consumer)

	// Add consumer to queue's consumer list
	listVal, _ := b.QueueConsumers.LoadOrStore(queueName, &consumerList{
		tags: make([]string, 0),
	})
	list := listVal.(*consumerList)
	list.mutex.Lock()
	list.tags = append(list.tags, consumerTag)
	list.mutex.Unlock()

	// Notify queue process that new consumer is available
	qp := queueVal.(*QueueProcess)
	// Send a signal to drain pending messages (if any)
	select {
	case qp.msgChan <- &queueMessage{message: nil}: // nil signals consumer change
	default:
		// Channel full, queue processor will catch up
	}

	return nil
}

// UnregisterConsumer removes a consumer
func (b *BrokerV2) UnregisterConsumer(consumerTag string) error {
	consumerVal, exists := b.Consumers.Load(consumerTag)
	if !exists {
		return errors.New("consumer not found")
	}
	consumer := consumerVal.(*protocol.Consumer)

	// Remove from queue's consumer list
	if listVal, ok := b.QueueConsumers.Load(consumer.Queue); ok {
		list := listVal.(*consumerList)
		list.mutex.Lock()
		newTags := make([]string, 0, len(list.tags)-1)
		for _, tag := range list.tags {
			if tag != consumerTag {
				newTags = append(newTags, tag)
			}
		}
		list.tags = newTags
		list.mutex.Unlock()
	}

	// Remove from broker tracking
	b.Consumers.Delete(consumerTag)

	return nil
}

// AcknowledgeMessage handles message acknowledgment
func (b *BrokerV2) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	consumerVal, exists := b.Consumers.Load(consumerTag)
	if !exists {
		return errors.New("consumer not found")
	}
	consumer := consumerVal.(*protocol.Consumer)

	// Decrement unacknowledged message count
	consumer.Channel.Mutex.Lock()
	if multiple {
		consumer.CurrentUnacked = 0
	} else {
		if consumer.CurrentUnacked > 0 {
			consumer.CurrentUnacked--
		}
	}
	consumer.Channel.Mutex.Unlock()

	// Signal queue process that consumer has capacity
	// The queue goroutine will automatically drain messages
	if queueVal, ok := b.Queues.Load(consumer.Queue); ok {
		qp := queueVal.(*QueueProcess)
		select {
		case qp.msgChan <- &queueMessage{message: nil}: // nil signals capacity change
		default:
			// Channel full, queue processor will catch up anyway
		}
	}

	return nil
}

// RejectMessage handles message rejection
func (b *BrokerV2) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	consumerVal, exists := b.Consumers.Load(consumerTag)
	if !exists {
		return errors.New("consumer not found")
	}
	consumer := consumerVal.(*protocol.Consumer)

	consumer.Channel.Mutex.Lock()
	if consumer.CurrentUnacked > 0 {
		consumer.CurrentUnacked--
	}
	consumer.Channel.Mutex.Unlock()

	// Signal queue process
	if queueVal, ok := b.Queues.Load(consumer.Queue); ok {
		qp := queueVal.(*QueueProcess)
		select {
		case qp.msgChan <- &queueMessage{message: nil}:
		default:
		}
	}

	return nil
}

// NacknowledgeMessage handles negative acknowledgment
func (b *BrokerV2) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	consumerVal, exists := b.Consumers.Load(consumerTag)
	if !exists {
		return errors.New("consumer not found")
	}
	consumer := consumerVal.(*protocol.Consumer)

	consumer.Channel.Mutex.Lock()
	if multiple {
		consumer.CurrentUnacked = 0
	} else {
		if consumer.CurrentUnacked > 0 {
			consumer.CurrentUnacked--
		}
	}
	consumer.Channel.Mutex.Unlock()

	// Signal queue process
	if queueVal, ok := b.Queues.Load(consumer.Queue); ok {
		qp := queueVal.(*QueueProcess)
		select {
		case qp.msgChan <- &queueMessage{message: nil}:
		default:
		}
	}

	return nil
}

// PublishMessage routes a message to appropriate queues
func (b *BrokerV2) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	// Get the exchange (lock-free read)
	exchangeVal, exists := b.Exchanges.Load(exchangeName)
	if !exists {
		// Default direct exchange behavior
		if exchangeName == "" || exchangeName == "amq.default" {
			return b.publishToQueue(routingKey, message)
		}
		return errors.New("exchange not found")
	}

	exchange := exchangeVal.(*protocol.Exchange)

	// Route message based on exchange type
	switch exchange.Kind {
	case "direct":
		return b.routeDirect(exchange, routingKey, message)
	case "fanout":
		return b.routeFanout(exchange, message)
	case "topic":
		return b.routeTopic(exchange, routingKey, message)
	case "headers":
		return b.routeHeaders(exchange, message, message.Headers)
	default:
		return errors.New("unsupported exchange type")
	}
}

// publishToQueue sends message to queue's processing goroutine
func (b *BrokerV2) publishToQueue(queueName string, message *protocol.Message) error {
	queueVal, exists := b.Queues.Load(queueName)
	if !exists {
		return nil // Queue doesn't exist, silently drop (AMQP behavior)
	}

	qp := queueVal.(*QueueProcess)

	// Send to queue's processing channel (non-blocking to avoid publisher hang)
	select {
	case qp.msgChan <- &queueMessage{message: message}:
		return nil
	default:
		// Queue processing channel full - this means queue goroutine is overwhelmed
		// Store in queue using index+cache architecture
		qp.Queue.Mutex.Lock()
		qp.Queue.AddMessage(message.DeliveryTag, message)
		qp.Queue.Mutex.Unlock()
		return nil
	}
}

// queueProcessor is the per-queue goroutine (like RabbitMQ's Erlang process)
func (b *BrokerV2) queueProcessor(qp *QueueProcess) {
	for {
		select {
		case <-qp.stopChan:
			return
		case qm := <-qp.msgChan:
			// Process message
			if qm.message != nil {
				// New message to deliver
				b.deliverMessage(qp, qm.message)
			} else {
				// Signal to drain pending messages (consumer capacity changed)
				b.drainPendingMessages(qp)
			}
		}
	}
}

// deliverMessage attempts to deliver a single message to an available consumer
func (b *BrokerV2) deliverMessage(qp *QueueProcess, message *protocol.Message) {
	// Get consumers for this queue
	listVal, ok := b.QueueConsumers.Load(qp.Queue.Name)
	if !ok {
		// No consumers, store message in queue
		qp.Queue.Mutex.Lock()
		qp.Queue.AddMessage(message.DeliveryTag, message)
		qp.Queue.Mutex.Unlock()
		return
	}

	list := listVal.(*consumerList)
	list.mutex.RLock()
	consumerTags := make([]string, len(list.tags))
	copy(consumerTags, list.tags)
	list.mutex.RUnlock()

	if len(consumerTags) == 0 {
		// No consumers, store message
		qp.Queue.Mutex.Lock()
		qp.Queue.AddMessage(message.DeliveryTag, message)
		qp.Queue.Mutex.Unlock()
		return
	}

	// Round-robin consumer selection
	startIdx := int(qp.consumerRobin.Add(1)) % len(consumerTags)

	// Try to find an available consumer
	for i := 0; i < len(consumerTags); i++ {
		idx := (startIdx + i) % len(consumerTags)
		consumerTag := consumerTags[idx]

		consumerVal, ok := b.Consumers.Load(consumerTag)
		if !ok {
			continue
		}
		consumer := consumerVal.(*protocol.Consumer)

		// Check if consumer has capacity
		consumer.Channel.Mutex.Lock()
		hasCapacity := consumer.PrefetchCount == 0 || consumer.CurrentUnacked < uint64(consumer.PrefetchCount)
		if !hasCapacity {
			consumer.Channel.Mutex.Unlock()
			continue
		}

		// Create delivery
		consumer.Channel.DeliveryTag++
		delivery := &protocol.Delivery{
			Message:     message,
			Exchange:    message.Exchange,
			RoutingKey:  message.RoutingKey,
			ConsumerTag: consumer.Tag,
			DeliveryTag: consumer.Channel.DeliveryTag,
		}

		if !consumer.NoAck {
			consumer.CurrentUnacked++
		}
		consumer.Channel.Mutex.Unlock()

		// Try to send (non-blocking)
		select {
		case consumer.Messages <- delivery:
			// Successfully delivered
			return
		default:
			// Consumer channel full, revert and try next consumer
			consumer.Channel.Mutex.Lock()
			if !consumer.NoAck && consumer.CurrentUnacked > 0 {
				consumer.CurrentUnacked--
			}
			consumer.Channel.Mutex.Unlock()
		}
	}

	// No available consumer, store in queue
	qp.Queue.Mutex.Lock()
	qp.Queue.AddMessage(message.DeliveryTag, message)
	qp.Queue.Mutex.Unlock()
}

// drainPendingMessages delivers queued messages to available consumers
func (b *BrokerV2) drainPendingMessages(qp *QueueProcess) {
	for {
		// Check if there are pending messages
		qp.Queue.Mutex.Lock()
		if qp.Queue.MessageCount() == 0 {
			qp.Queue.Mutex.Unlock()
			return
		}

		// Get first message from index
		idx, found := qp.Queue.IndexManager.Peek()
		if !found {
			qp.Queue.Mutex.Unlock()
			return
		}

		message, _ := qp.Queue.GetMessage(idx.DeliveryTag)
		if message == nil {
			// Cache miss - skip for now
			qp.Queue.Mutex.Unlock()
			return
		}

		// Remove from queue before attempting delivery
		qp.Queue.RemoveMessage(idx.DeliveryTag)
		qp.Queue.Mutex.Unlock()

		// Try to deliver this message
		b.deliverMessage(qp, message)

		// Check if message was delivered or re-queued
		qp.Queue.Mutex.Lock()
		stillHasMessages := qp.Queue.MessageCount() > 0
		qp.Queue.Mutex.Unlock()

		// If message was re-queued (queue not empty), stop draining
		// This means no consumers are available
		if stillHasMessages {
			// Check if the queue grew (message was re-queued)
			return
		}
	}
}

// Routing methods (similar to broker.go but using sync.Map)

func (b *BrokerV2) routeDirect(exchange *protocol.Exchange, routingKey string, message *protocol.Message) error {
	exchange.Mutex.RLock()
	bindings := make([]*protocol.Binding, len(exchange.Bindings))
	copy(bindings, exchange.Bindings)
	exchange.Mutex.RUnlock()

	for _, binding := range bindings {
		if binding.RoutingKey == routingKey {
			b.publishToQueue(binding.Queue, message)
		}
	}
	return nil
}

func (b *BrokerV2) routeFanout(exchange *protocol.Exchange, message *protocol.Message) error {
	exchange.Mutex.RLock()
	bindings := make([]*protocol.Binding, len(exchange.Bindings))
	copy(bindings, exchange.Bindings)
	exchange.Mutex.RUnlock()

	for _, binding := range bindings {
		b.publishToQueue(binding.Queue, message)
	}
	return nil
}

func (b *BrokerV2) routeTopic(exchange *protocol.Exchange, routingKey string, message *protocol.Message) error {
	exchange.Mutex.RLock()
	bindings := make([]*protocol.Binding, len(exchange.Bindings))
	copy(bindings, exchange.Bindings)
	exchange.Mutex.RUnlock()

	for _, binding := range bindings {
		if topicMatches(binding.RoutingKey, routingKey) {
			b.publishToQueue(binding.Queue, message)
		}
	}
	return nil
}

func (b *BrokerV2) routeHeaders(exchange *protocol.Exchange, message *protocol.Message, headers map[string]interface{}) error {
	exchange.Mutex.RLock()
	bindings := make([]*protocol.Binding, len(exchange.Bindings))
	copy(bindings, exchange.Bindings)
	exchange.Mutex.RUnlock()

	for _, binding := range bindings {
		if headersMatch(binding.Arguments, headers) {
			b.publishToQueue(binding.Queue, message)
		}
	}
	return nil
}

// DeclareExchange creates a new exchange
func (b *BrokerV2) DeclareExchange(name, kind string, durable, autoDelete, internal bool, args map[string]interface{}) error {
	if name == "" {
		name = "amq." + kind
	}

	// Check if exists
	if _, exists := b.Exchanges.Load(name); exists {
		return nil
	}

	exchange := &protocol.Exchange{
		Name:       name,
		Kind:       kind,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
		Arguments:  args,
		Bindings:   make([]*protocol.Binding, 0),
	}

	b.Exchanges.Store(name, exchange)
	return nil
}

// DeleteExchange removes an exchange
func (b *BrokerV2) DeleteExchange(name string, ifUnused bool) error {
	exchangeVal, exists := b.Exchanges.Load(name)
	if !exists {
		return errors.New("exchange not found")
	}

	exchange := exchangeVal.(*protocol.Exchange)

	if ifUnused && len(exchange.Bindings) > 0 {
		return errors.New("exchange in use")
	}

	b.Exchanges.Delete(name)
	return nil
}

// DeclareQueue creates a new queue with its own processing goroutine
func (b *BrokerV2) DeclareQueue(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) (*protocol.Queue, error) {
	if name == "" {
		name = generateUniqueQueueName()
	}

	// Check if queue already exists
	if queueVal, exists := b.Queues.Load(name); exists {
		qp := queueVal.(*QueueProcess)
		return qp.Queue, nil
	}

	// Create new queue
	queue := &protocol.Queue{
		Name:       name,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		Arguments:  args,
	}

	// Initialize unified storage architecture
	queue.InitializeStorage(128 * 1024 * 1024) // 128MB cache per queue

	// Create queue process with its goroutine
	qp := &QueueProcess{
		Queue:    queue,
		msgChan:  make(chan *queueMessage, 10000), // Large buffer for high throughput
		stopChan: make(chan struct{}),
	}

	b.Queues.Store(name, qp)

	// Start queue processor goroutine
	go b.queueProcessor(qp)

	return queue, nil
}

// DeleteQueue removes a queue and stops its processor
func (b *BrokerV2) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	queueVal, exists := b.Queues.Load(name)
	if !exists {
		return errors.New("queue not found")
	}

	qp := queueVal.(*QueueProcess)

	if ifEmpty && qp.Queue.MessageCount() > 0 {
		return errors.New("queue not empty")
	}

	// Stop queue processor
	close(qp.stopChan)

	// Remove from broker
	b.Queues.Delete(name)

	// Remove bindings to this queue
	b.Exchanges.Range(func(key, value interface{}) bool {
		exchange := value.(*protocol.Exchange)
		exchange.Mutex.Lock()
		newBindings := make([]*protocol.Binding, 0)
		for _, binding := range exchange.Bindings {
			if binding.Queue != name {
				newBindings = append(newBindings, binding)
			}
		}
		exchange.Bindings = newBindings
		exchange.Mutex.Unlock()
		return true
	})

	return nil
}

// BindQueue creates a binding between an exchange and a queue
func (b *BrokerV2) BindQueue(queueName, exchangeName, routingKey string, args map[string]interface{}) error {
	exchangeVal, exExists := b.Exchanges.Load(exchangeName)
	if !exExists {
		return errors.New("exchange not found")
	}

	_, qExists := b.Queues.Load(queueName)
	if !qExists {
		return errors.New("queue not found")
	}

	exchange := exchangeVal.(*protocol.Exchange)
	exchange.Mutex.Lock()
	defer exchange.Mutex.Unlock()

	// Check if binding already exists
	for _, binding := range exchange.Bindings {
		if binding.Queue == queueName && binding.RoutingKey == routingKey {
			return nil
		}
	}

	binding := &protocol.Binding{
		Exchange:   exchangeName,
		Queue:      queueName,
		RoutingKey: routingKey,
		Arguments:  args,
	}

	exchange.Bindings = append(exchange.Bindings, binding)
	return nil
}

// UnbindQueue removes a binding
func (b *BrokerV2) UnbindQueue(queueName, exchangeName, routingKey string) error {
	exchangeVal, exExists := b.Exchanges.Load(exchangeName)
	if !exExists {
		return errors.New("exchange not found")
	}

	exchange := exchangeVal.(*protocol.Exchange)
	exchange.Mutex.Lock()
	defer exchange.Mutex.Unlock()

	newBindings := make([]*protocol.Binding, 0)
	found := false
	for _, binding := range exchange.Bindings {
		if binding.Queue != queueName || binding.RoutingKey != routingKey {
			newBindings = append(newBindings, binding)
		} else {
			found = true
		}
	}

	if !found {
		return errors.New("binding not found")
	}

	exchange.Bindings = newBindings
	return nil
}

// Helper functions are in broker.go (generateUniqueQueueName, generateID)
