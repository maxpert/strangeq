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
	Mutex          sync.RWMutex
}

// NewBroker creates a new broker instance
func NewBroker() *Broker {
	return &Broker{
		Exchanges:      make(map[string]*protocol.Exchange),
		Queues:         make(map[string]*protocol.Queue),
		QueueConsumers: make(map[string][]string),
		Consumers:      make(map[string]*protocol.Consumer),
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
	if exists && len(queue.Messages) > 0 {

		// Deliver pending messages to the new consumer
		queue.Mutex.Lock()
		messagesToDeliver = make([]*protocol.Message, len(queue.Messages))
		copy(messagesToDeliver, queue.Messages)
		// Clear the queue - messages will be delivered to consumer
		queue.Messages = queue.Messages[:0]
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
func (b *Broker) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Decrement unacknowledged message count
	consumer.Channel.Mutex.Lock()
	defer consumer.Channel.Mutex.Unlock()

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

	return nil
}

// RejectMessage handles message rejection, potentially requeuing
func (b *Broker) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Decrement unacknowledged message count
	consumer.Channel.Mutex.Lock()
	defer consumer.Channel.Mutex.Unlock()

	if consumer.CurrentUnacked > 0 {
		consumer.CurrentUnacked--
	}

	// In a real implementation, you would:
	// 1. Remove the message from unacknowledged tracking
	// 2. If requeue=true, put the message back on the queue
	// 3. If requeue=false, potentially dead-letter the message

	return nil
}

// NacknowledgeMessage handles negative acknowledgment, potentially requeuing
func (b *Broker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Decrement unacknowledged message count(s)
	consumer.Channel.Mutex.Lock()
	defer consumer.Channel.Mutex.Unlock()

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

	// In a real implementation, you would:
	// 1. Remove the message(s) from unacknowledged tracking
	// 2. If requeue=true, put the message(s) back on the queue
	// 3. If requeue=false, potentially dead-letter the message

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
			queue.Messages = append(queue.Messages, message)
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
func (b *Broker) notifyQueueConsumers(queue *protocol.Queue, message *protocol.Message, consumerTags []string) {
	// In this implementation, we'll deliver messages respecting prefetch limits
	// We'll implement round-robin delivery among consumers on the queue

	b.Mutex.RLock()
	defer b.Mutex.RUnlock()

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

	// If no consumers are available (all at prefetch limit), queue the message for later
	if len(availableConsumers) == 0 {
		// In a real implementation, we would queue this message for when consumers become available
		// For now, we'll drop it
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

	// Increment the delivery tag
	consumer.Channel.Mutex.Lock()
	consumer.Channel.DeliveryTag++
	delivery.DeliveryTag = consumer.Channel.DeliveryTag

	// Increment unacknowledged message count if not in no-ack mode
	if !consumer.NoAck {
		consumer.CurrentUnacked++
	}
	consumer.Channel.Mutex.Unlock()

	// Try to send to the consumer's message channel
	select {
	case consumer.Messages <- delivery:
		// Message successfully sent to consumer
	default:
		// Channel is full, decrement unacknowledged count and skip delivery
		// Channel is full, decrement unacknowledged count and skip delivery
		if !consumer.NoAck {
			consumer.Channel.Mutex.Lock()
			if consumer.CurrentUnacked > 0 {
				consumer.CurrentUnacked--
			}
			consumer.Channel.Mutex.Unlock()
		}
		// In a real implementation, you'd handle this differently
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
				queue.Messages = append(queue.Messages, message)
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
			queue.Messages = append(queue.Messages, message)
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
				queue.Messages = append(queue.Messages, message)
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
				queue.Messages = append(queue.Messages, message)
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
		Messages:   make([]*protocol.Message, 0),
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
	if ifEmpty && len(queue.Messages) > 0 {
		return errors.New("queue not empty")
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
