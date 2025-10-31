package broker

import (
	"errors"
	"fmt"
	"math/rand"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// Broker manages exchanges, queues, and routing
type Broker struct {
	Exchanges      map[string]*protocol.Exchange
	Queues         map[string]*protocol.Queue
	QueueConsumers map[string][]string           // queue name to list of consumer tags
	Consumers      map[string]*protocol.Consumer // consumer tag to consumer
	MemoryManager  *MemoryManager                // Optional: manages memory pressure and paging
	Mutex          sync.RWMutex
}

// NewBroker creates a new broker instance
func NewBroker() *Broker {
	b := &Broker{
		Exchanges:      make(map[string]*protocol.Exchange),
		Queues:         make(map[string]*protocol.Queue),
		QueueConsumers: make(map[string][]string),
		Consumers:      make(map[string]*protocol.Consumer),
		MemoryManager:  nil,
	}

	// Enable memory management by default to prevent unbounded growth
	// This provides automatic memory pressure handling for all queues
	b.EnableMemoryManagement(2 * 1024 * 1024 * 1024) // 2GB default

	return b
}

// EnableMemoryManagement initializes and starts the memory manager
func (b *Broker) EnableMemoryManagement(maxMemory int64) {
	if maxMemory <= 0 {
		maxMemory = 2 * 1024 * 1024 * 1024 // 2GB default
	}

	config := DefaultMemoryManagerConfig()
	config.MaxMemory = maxMemory
	b.MemoryManager = NewMemoryManager(config)

	// Register all existing queues (both durable and non-durable)
	// This prevents unbounded memory growth for all queue types
	b.Mutex.RLock()
	for _, queue := range b.Queues {
		b.MemoryManager.RegisterQueue(queue)
	}
	b.Mutex.RUnlock()
}

// StopMemoryManagement stops the memory manager
func (b *Broker) StopMemoryManagement() {
	if b.MemoryManager != nil {
		b.MemoryManager.Stop()
		b.MemoryManager = nil
	}
}

// RegisterConsumer registers a new consumer for a queue
func (b *Broker) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	var messagesToDeliver []*protocol.Message
	var queue *protocol.Queue

	// Critical section: register consumer and get pending messages
	b.Mutex.Lock()
	// Check if queue exists
	_, exists := b.Queues[queueName]
	if !exists {
		b.Mutex.Unlock()
		return errors.New("queue does not exist")
	}

	// Add consumer to the broker's tracking
	b.Consumers[consumerTag] = consumer

	// Add consumer to the queue's consumer list
	queueConsumers := b.QueueConsumers[queueName]
	b.QueueConsumers[queueName] = append(queueConsumers, consumerTag)

	// Check if there are any pending messages in the queue that need to be delivered
	queue, exists = b.Queues[queueName]
	if exists {
		queue.Mutex.Lock()
		// Lazy loading from index+cache
		indices := queue.IndexManager.GetAll()
		if len(indices) > 0 {
			messagesToDeliver = make([]*protocol.Message, 0, len(indices))
			for _, idx := range indices {
				// Try cache first
				msg, _ := queue.GetMessage(idx.DeliveryTag)
				if msg != nil {
					messagesToDeliver = append(messagesToDeliver, msg)
				}
				// Note: Cache misses would require disk load, but for now we skip them
				// In a full implementation, we'd load from BadgerDB here
			}
			// Messages will be delivered, so remove from index
			for _, idx := range indices {
				queue.RemoveMessage(idx.DeliveryTag)
			}
		}
		queue.Mutex.Unlock()
	}
	b.Mutex.Unlock()

	// Deliver messages outside the critical section
	if len(messagesToDeliver) > 0 {
		for _, message := range messagesToDeliver {
			b.notifyQueueConsumers(queue, message, []string{consumerTag})
		}
	}

	return nil
}

// UnregisterConsumer removes a consumer
func (b *Broker) UnregisterConsumer(consumerTag string) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Remove from queue's consumer list
	queueConsumers := b.QueueConsumers[consumer.Queue]
	newQueueConsumers := make([]string, 0)
	for _, tag := range queueConsumers {
		if tag != consumerTag {
			newQueueConsumers = append(newQueueConsumers, tag)
		}
	}
	b.QueueConsumers[consumer.Queue] = newQueueConsumers

	// Remove from broker's tracking
	delete(b.Consumers, consumerTag)

	return nil
}

// AcknowledgeMessage decrements the unacknowledged message counter for a consumer
// and attempts to drain queued messages (RabbitMQ-style behavior)
func (b *Broker) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	b.Mutex.Lock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.Unlock()
		return errors.New("consumer not found")
	}

	// Get queue name before unlocking
	queueName := consumer.Queue

	// Decrement unacknowledged message count
	consumer.Channel.Mutex.Lock()
	if multiple {
		// Acknowledge all messages up to and including deliveryTag
		// In a real implementation, you'd track individual delivery tags
		// For now, we'll just reset the counter (simplified approach)
		consumer.CurrentUnacked = 0
	} else {
		// Acknowledge single message
		if consumer.CurrentUnacked > 0 {
			consumer.CurrentUnacked--
		}
	}
	consumer.Channel.Mutex.Unlock()

	// After ACK, try to drain queued messages (RabbitMQ behavior)
	// This ensures consumers continuously receive messages when they have capacity
	b.drainQueuedMessages(queueName)

	b.Mutex.Unlock()
	return nil
}

// RejectMessage handles message rejection, potentially requeuing
func (b *Broker) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	b.Mutex.Lock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.Unlock()
		return errors.New("consumer not found")
	}

	// Get queue name before unlocking
	queueName := consumer.Queue

	// Decrement unacknowledged message count
	consumer.Channel.Mutex.Lock()
	if consumer.CurrentUnacked > 0 {
		consumer.CurrentUnacked--
	}
	consumer.Channel.Mutex.Unlock()

	// In a real implementation, you would:
	// 1. Remove the message from unacknowledged tracking
	// 2. If requeue=true, put the message back on the queue
	// 3. If requeue=false, potentially dead-letter the message

	// After reject, try to drain queued messages (RabbitMQ behavior)
	b.drainQueuedMessages(queueName)

	b.Mutex.Unlock()
	return nil
}

// NacknowledgeMessage handles negative acknowledgment, potentially requeuing
func (b *Broker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	b.Mutex.Lock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.Unlock()
		return errors.New("consumer not found")
	}

	// Get queue name before unlocking
	queueName := consumer.Queue

	// Decrement unacknowledged message count(s)
	consumer.Channel.Mutex.Lock()
	if multiple {
		// Nacknowledge all messages up to and including deliveryTag
		// In a real implementation, you'd track individual delivery tags
		// For now, we'll just reset the counter (simplified approach)
		consumer.CurrentUnacked = 0
	} else {
		// Nacknowledge single message
		if consumer.CurrentUnacked > 0 {
			consumer.CurrentUnacked--
		}
	}
	consumer.Channel.Mutex.Unlock()

	// In a real implementation, you would:
	// 1. Remove the message(s) from unacknowledged tracking
	// 2. If requeue=true, put the message(s) back on the queue
	// 3. If requeue=false, potentially dead-letter the message

	// After nack, try to drain queued messages (RabbitMQ behavior)
	b.drainQueuedMessages(queueName)

	b.Mutex.Unlock()
	return nil
}

// PublishMessage routes a message to appropriate queues based on exchange type
func (b *Broker) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()

	// Get the exchange
	exchange, exists := b.Exchanges[exchangeName]
	if !exists {
		// If exchange doesn't exist, and it's not the default exchange, return error
		if exchangeName != "" && exchangeName != "amq.default" {
			return errors.New("exchange not found")
		}
		// For empty exchange name, this is the default direct exchange behavior
		// In AMQP, publishing to an empty exchange means send directly to queue named in routing key
		if queue, qExists := b.Queues[routingKey]; qExists {
			queue.Mutex.Lock()
			queue.AddMessage(message.DeliveryTag, message)
			queue.Mutex.Unlock()

			// Notify any consumers waiting on this queue
			consumerTags := b.QueueConsumers[routingKey]
			b.notifyQueueConsumers(queue, message, consumerTags)
		}
		return nil
	}

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

// notifyQueueConsumers sends the message to any waiting consumers with flow control
// Messages are left in the queue if no consumers have capacity (RabbitMQ behavior)
func (b *Broker) notifyQueueConsumers(queue *protocol.Queue, message *protocol.Message, consumerTags []string) {
	// In this implementation, we'll deliver messages respecting prefetch limits
	// We'll implement round-robin delivery among consumers on the queue

	b.Mutex.RLock()
	// Filter out consumers that have reached their prefetch limits
	availableConsumers := make([]*protocol.Consumer, 0)

	for _, consumerTag := range consumerTags {
		consumer, exists := b.Consumers[consumerTag]
		if !exists {
			continue
		}

		// Skip consumers that have reached their prefetch limit
		if consumer.PrefetchCount > 0 && consumer.CurrentUnacked >= uint64(consumer.PrefetchCount) {
			continue
		}

		availableConsumers = append(availableConsumers, consumer)
	}

	// If no consumers are available (all at prefetch limit), message stays in queue
	// It will be drained when consumers acknowledge messages and free up capacity
	if len(availableConsumers) == 0 {
		b.Mutex.RUnlock()
		// Message remains in queue - this is RabbitMQ behavior
		// When consumers ACK and free up capacity, drainQueuedMessages() will deliver it
		return
	}

	// Round-robin delivery - simple approach: deliver to first available consumer
	// In a real implementation, you'd maintain state to rotate through consumers properly
	consumer := availableConsumers[0]

	// Create a delivery object
	delivery := &protocol.Delivery{
		Message:     message,
		Exchange:    message.Exchange,
		RoutingKey:  message.RoutingKey,
		ConsumerTag: consumer.Tag,
	}

	// Release broker lock BEFORE doing potentially blocking operations
	b.Mutex.RUnlock()

	// Increment the delivery tag
	consumer.Channel.Mutex.Lock()
	consumer.Channel.DeliveryTag++
	delivery.DeliveryTag = consumer.Channel.DeliveryTag

	// Increment unacknowledged message count if not in no-ack mode
	if !consumer.NoAck {
		consumer.CurrentUnacked++
	}
	consumer.Channel.Mutex.Unlock()

	// Send to the consumer's message channel with blocking
	// This implements proper TCP backpressure: if the consumer's buffer is full,
	// we block the publishing goroutine, which blocks the TCP read handler,
	// which applies backpressure to the TCP connection, naturally throttling
	// the publisher.
	//
	// This is how RabbitMQ implements flow control in AMQP 0.9.1 - by blocking
	// the connection when queues can't keep up, rather than using channel.flow
	//
	// CRITICAL: We've already released the broker lock before this blocking send
	// to avoid deadlocks
	consumer.Messages <- delivery
	// Message successfully sent to consumer (blocking if buffer full)
}

// drainQueuedMessages attempts to deliver queued messages to consumers with available capacity
// This implements RabbitMQ-style queue draining after acknowledgments
// MUST be called while holding b.Mutex (it will be unlocked and re-locked during delivery)
func (b *Broker) drainQueuedMessages(queueName string) {
	// Check if queue exists and has messages
	queue, exists := b.Queues[queueName]
	if !exists {
		return
	}

	queue.Mutex.Lock()
	hasMessages := queue.MessageCount() > 0
	queue.Mutex.Unlock()

	if !hasMessages {
		return
	}

	// Get consumers for this queue
	consumerTags, exists := b.QueueConsumers[queueName]
	if !exists || len(consumerTags) == 0 {
		return
	}

	// Try to drain as many messages as possible
	// Keep looping until no more messages can be delivered
	for {
		queue.Mutex.Lock()
		var message *protocol.Message
		var deliveryTag uint64

		// Get from index+cache
		if queue.MessageCount() == 0 {
			queue.Mutex.Unlock()
			break
		}
		// Peek at first message in index
		idx, found := queue.IndexManager.Peek()
		if !found {
			queue.Mutex.Unlock()
			break
		}
		deliveryTag = idx.DeliveryTag
		message, _ = queue.GetMessage(deliveryTag)
		if message == nil {
			// Cache miss - skip for now (would load from disk in full implementation)
			queue.Mutex.Unlock()
			break
		}
		queue.Mutex.Unlock()

		// Find an available consumer (one that hasn't reached prefetch limit)
		var availableConsumer *protocol.Consumer
		for _, consumerTag := range consumerTags {
			consumer, exists := b.Consumers[consumerTag]
			if !exists {
				continue
			}

			// Check if consumer has capacity
			consumer.Channel.Mutex.Lock()
			hasCapacity := consumer.PrefetchCount == 0 || consumer.CurrentUnacked < uint64(consumer.PrefetchCount)
			consumer.Channel.Mutex.Unlock()

			if hasCapacity {
				availableConsumer = consumer
				break
			}
		}

		// No available consumers, stop draining
		if availableConsumer == nil {
			break
		}

		// Remove message from queue before delivery
		queue.Mutex.Lock()
		queue.RemoveMessage(deliveryTag)
		queue.Mutex.Unlock()

		// Deliver the message outside the broker lock to avoid blocking
		// We need to temporarily release and re-acquire the lock
		b.Mutex.Unlock()

		// Create delivery
		delivery := &protocol.Delivery{
			Message:     message,
			Exchange:    message.Exchange,
			RoutingKey:  message.RoutingKey,
			ConsumerTag: availableConsumer.Tag,
		}

		// Increment delivery tag and unacked count
		availableConsumer.Channel.Mutex.Lock()
		availableConsumer.Channel.DeliveryTag++
		delivery.DeliveryTag = availableConsumer.Channel.DeliveryTag
		if !availableConsumer.NoAck {
			availableConsumer.CurrentUnacked++
		}
		availableConsumer.Channel.Mutex.Unlock()

		// Send to consumer (non-blocking with select to avoid deadlock)
		select {
		case availableConsumer.Messages <- delivery:
			// Message delivered successfully
		default:
			// Consumer channel full, requeue the message
			queue.Mutex.Lock()
			queue.AddMessage(deliveryTag, message)
			queue.Mutex.Unlock()

			// Revert the unacked count
			availableConsumer.Channel.Mutex.Lock()
			if !availableConsumer.NoAck && availableConsumer.CurrentUnacked > 0 {
				availableConsumer.CurrentUnacked--
			}
			availableConsumer.Channel.Mutex.Unlock()

			// Re-acquire lock and stop draining
			b.Mutex.Lock()
			break
		}

		// Re-acquire the broker lock for next iteration
		b.Mutex.Lock()
	}
}

// routeDirect routes messages to queues that have bindings with matching routing key
func (b *Broker) routeDirect(exchange *protocol.Exchange, routingKey string, message *protocol.Message) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	for _, binding := range exchange.Bindings {
		if binding.RoutingKey == routingKey {
			// Send to the bound queue
			if queue, exists := b.Queues[binding.Queue]; exists {
				queue.Mutex.Lock()
				queue.AddMessage(message.DeliveryTag, message)
				queue.Mutex.Unlock()

				// Notify any consumers waiting on this queue
				b.notifyQueueConsumers(queue, message, b.QueueConsumers[binding.Queue])
			}
		}
	}

	return nil
}

// routeFanout routes messages to all bound queues regardless of routing key
func (b *Broker) routeFanout(exchange *protocol.Exchange, message *protocol.Message) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	for _, binding := range exchange.Bindings {
		if queue, exists := b.Queues[binding.Queue]; exists {
			queue.Mutex.Lock()
			queue.AddMessage(message.DeliveryTag, message)
			queue.Mutex.Unlock()

			// Notify any consumers waiting on this queue
			b.notifyQueueConsumers(queue, message, b.QueueConsumers[binding.Queue])
		}
	}

	return nil
}

// routeTopic routes messages based on routing pattern matching
func (b *Broker) routeTopic(exchange *protocol.Exchange, routingKey string, message *protocol.Message) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	// For topic routing, we match routing keys using patterns with * and #
	for _, binding := range exchange.Bindings {
		if topicMatches(binding.RoutingKey, routingKey) {
			// Send to the bound queue
			if queue, exists := b.Queues[binding.Queue]; exists {
				queue.Mutex.Lock()
				queue.AddMessage(message.DeliveryTag, message)
				queue.Mutex.Unlock()

				// Notify any consumers waiting on this queue
				b.notifyQueueConsumers(queue, message, b.QueueConsumers[binding.Queue])
			}
		}
	}

	return nil
}

// routeHeaders routes messages based on headers matching
func (b *Broker) routeHeaders(exchange *protocol.Exchange, message *protocol.Message, headers map[string]interface{}) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	for _, binding := range exchange.Bindings {
		// Check if headers match based on binding arguments
		if headersMatch(binding.Arguments, headers) {
			// Send to the bound queue
			if queue, exists := b.Queues[binding.Queue]; exists {
				queue.Mutex.Lock()
				queue.AddMessage(message.DeliveryTag, message)
				queue.Mutex.Unlock()

				// Notify any consumers waiting on this queue
				b.notifyQueueConsumers(queue, message, b.QueueConsumers[binding.Queue])
			}
		}
	}

	return nil
}

// topicMatches checks if a routing key matches a topic pattern
func topicMatches(pattern, routingKey string) bool {
	patternParts := splitTopic(pattern)
	keyParts := splitTopic(routingKey)

	return matchTopicParts(patternParts, keyParts)
}

// splitTopic splits a topic pattern/routing key into parts
func splitTopic(topic string) []string {
	// Very basic implementation - in reality you'd want a proper string splitting function
	result := make([]string, 0)
	current := ""

	for _, char := range topic {
		if char == '.' {
			result = append(result, current)
			current = ""
		} else {
			current += string(char)
		}
	}

	if current != "" {
		result = append(result, current)
	}

	return result
}

// matchTopicParts checks if key parts match pattern parts considering wildcards
func matchTopicParts(patternParts, keyParts []string) bool {
	return doMatch(patternParts, 0, keyParts, 0)
}

// doMatch is a recursive helper for topic matching
func doMatch(pattern []string, pIdx int, key []string, kIdx int) bool {
	// Both exhausted - match
	if pIdx == len(pattern) && kIdx == len(key) {
		return true
	}

	// Pattern exhausted but key not - no match
	if pIdx == len(pattern) {
		return false
	}

	patternPart := pattern[pIdx]

	if patternPart == "#" {
		// Match 0 or more words
		// Try matching 0 words first
		if doMatch(pattern, pIdx+1, key, kIdx) {
			return true
		}
		// Try matching 1 or more words
		if kIdx < len(key) && doMatch(pattern, pIdx, key, kIdx+1) {
			return true
		}
		return false
	} else if patternPart == "*" {
		// Match exactly one word
		if kIdx < len(key) {
			return doMatch(pattern, pIdx+1, key, kIdx+1)
		}
		return false
	} else {
		// Exact match
		if kIdx < len(key) && patternPart == key[kIdx] {
			return doMatch(pattern, pIdx+1, key, kIdx+1)
		}
		return false
	}
}

// headersMatch checks if message headers match binding arguments
func headersMatch(bindingArgs, msgHeaders map[string]interface{}) bool {
	// For now, simplified implementation
	// In a real implementation, you'd handle header matching rules (all vs any matching)
	for key, value := range bindingArgs {
		if msgValue, exists := msgHeaders[key]; !exists || msgValue != value {
			return false
		}
	}
	return true
}

// DeclareExchange creates a new exchange if it doesn't exist
func (b *Broker) DeclareExchange(name, kind string, durable, autoDelete, internal bool, args map[string]interface{}) error {
	if name == "" {
		name = "amq." + kind // Default exchange names
	}

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// Check if exchange already exists
	if _, exists := b.Exchanges[name]; exists {
		return nil // Exchange already exists
	}

	// Create new exchange
	exchange := &protocol.Exchange{
		Name:       name,
		Kind:       kind,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
		Arguments:  args,
		Bindings:   make([]*protocol.Binding, 0),
	}

	b.Exchanges[name] = exchange
	return nil
}

// DeleteExchange removes an exchange
func (b *Broker) DeleteExchange(name string, ifUnused bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	exchange, exists := b.Exchanges[name]
	if !exists {
		return errors.New("exchange not found")
	}

	// Check if exchange has bindings and ifUnused is true
	if ifUnused && len(exchange.Bindings) > 0 {
		return errors.New("exchange in use")
	}

	// Remove the exchange
	delete(b.Exchanges, name)

	return nil
}

// DeclareQueue creates a new queue if it doesn't exist
func (b *Broker) DeclareQueue(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) (*protocol.Queue, error) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// Generate a unique name if not provided (for exclusive queues)
	if name == "" {
		// Generate a unique name using UUID or similar
		name = generateUniqueQueueName()
	}

	// Check if queue already exists
	queue, exists := b.Queues[name]
	if exists {
		// Check if queue properties match (this is simplified)
		// In a real implementation, you'd validate properties match
		return queue, nil
	}

	// Create new queue
	queue = &protocol.Queue{
		Name:       name,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		Arguments:  args,
	}

	// Enable RabbitMQ-style index+cache storage for ALL queues
	// This provides 5x memory reduction and consistent behavior
	// The difference: durable queues persist immediately, non-durable only under pressure
	queue.InitializeStorage(128 * 1024 * 1024) // 128MB cache per queue

	// Register with memory manager
	// This prevents unbounded memory growth for all queue types
	if b.MemoryManager != nil {
		b.MemoryManager.RegisterQueue(queue)
	}

	b.Queues[name] = queue
	return queue, nil
}

// DeleteQueue removes a queue
func (b *Broker) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	queue, exists := b.Queues[name]
	if !exists {
		return errors.New("queue not found")
	}

	// Check ifUnused (simplified - check if any bindings exist to this queue)
	if ifUnused {
		// Check all exchanges for bindings to this queue
		for _, ex := range b.Exchanges {
			ex.Mutex.Lock()
			for _, binding := range ex.Bindings {
				if binding.Queue == name {
					ex.Mutex.Unlock()
					return errors.New("queue in use")
				}
			}
			ex.Mutex.Unlock()
		}
	}

	// Check ifEmpty
	if ifEmpty && queue.MessageCount() != 0 {
		return errors.New("queue not empty")
	}

	// Unregister from memory manager (handles both storage types)
	if b.MemoryManager != nil {
		b.MemoryManager.UnregisterQueue(name)
	}

	// Remove the queue
	delete(b.Queues, name)

	// Also remove any bindings that point to this queue
	for _, ex := range b.Exchanges {
		ex.Mutex.Lock()
		newBindings := make([]*protocol.Binding, 0)
		for _, binding := range ex.Bindings {
			if binding.Queue != name {
				newBindings = append(newBindings, binding)
			}
		}
		ex.Bindings = newBindings
		ex.Mutex.Unlock()
	}

	return nil
}

// BindQueue creates a binding between an exchange and a queue
func (b *Broker) BindQueue(queueName, exchangeName, routingKey string, args map[string]interface{}) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// Check if exchange exists
	exchange, exExists := b.Exchanges[exchangeName]
	if !exExists {
		return errors.New("exchange not found")
	}

	// Check if queue exists
	_, qExists := b.Queues[queueName]
	if !qExists {
		return errors.New("queue not found")
	}

	// Add binding to exchange
	exchange.Mutex.Lock()
	defer exchange.Mutex.Unlock()

	// Check if binding already exists
	for _, binding := range exchange.Bindings {
		if binding.Queue == queueName && binding.RoutingKey == routingKey {
			return nil // Binding already exists
		}
	}

	// Create new binding
	binding := &protocol.Binding{
		Exchange:   exchangeName,
		Queue:      queueName,
		RoutingKey: routingKey,
		Arguments:  args,
	}

	exchange.Bindings = append(exchange.Bindings, binding)
	return nil
}

// UnbindQueue removes a binding between an exchange and a queue
func (b *Broker) UnbindQueue(queueName, exchangeName, routingKey string) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// Check if exchange exists
	exchange, exExists := b.Exchanges[exchangeName]
	if !exExists {
		return errors.New("exchange not found")
	}

	// Remove binding from exchange
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

// generateUniqueQueueName generates a unique queue name
func generateUniqueQueueName() string {
	// In a real implementation, you'd use UUID or a similar approach
	// For now, we'll use a very simplified approach
	return "queue_" + generateID()
}

// generateID generates a simple unique ID
func generateID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// This shouldn't happen in practice, but if it does, we'll use a simple fallback
		mrand.Seed(time.Now().UnixNano())
		fallbackID := mrand.Int63()
		return fmt.Sprintf("id-%d", fallbackID)
	}

	return fmt.Sprintf("%x", b)
}
