package broker

import (
	"errors"
	"fmt"
	"math/rand"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// Broker manages exchanges, queues, and routing (Actor Model - NO LOCKS!)
type Broker struct {
	Exchanges      map[string]*protocol.Exchange
	Queues         map[string]*protocol.Queue
	QueueConsumers map[string][]string           // queue name to list of consumer tags
	Consumers      map[string]*protocol.Consumer // consumer tag to consumer
	Mutex          sync.RWMutex                  // Only for broker metadata, NOT queue operations!
	DeliveryTag    uint64                        // Atomic delivery tag counter
}

// NewBroker creates a new broker instance
func NewBroker() *Broker {
	return &Broker{
		Exchanges:      make(map[string]*protocol.Exchange),
		Queues:         make(map[string]*protocol.Queue),
		QueueConsumers: make(map[string][]string),
		Consumers:      make(map[string]*protocol.Consumer),
		DeliveryTag:    0,
	}
}

// EnableMemoryManagement is a no-op in actor model (memory managed by queue actors)
func (b *Broker) EnableMemoryManagement(maxMemory int64) {
	// No-op: Actor model handles memory implicitly through channel buffering
}

// StopMemoryManagement is a no-op in actor model
func (b *Broker) StopMemoryManagement() {
	// No-op: Actors stop when queues are deleted
}

// RegisterConsumer registers a new consumer using actor model (NO LOCKS!)
func (b *Broker) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	b.Mutex.Lock()
	queue, exists := b.Queues[queueName]
	if !exists {
		b.Mutex.Unlock()
		return errors.New("queue does not exist")
	}

	b.Consumers[consumerTag] = consumer
	queueConsumers := b.QueueConsumers[queueName]
	b.QueueConsumers[queueName] = append(queueConsumers, consumerTag)
	b.Mutex.Unlock()

	// Register consumer with queue actor - actor handles delivery!
	queue.RegisterConsumer(consumer)

	return nil
}

// UnregisterConsumer removes a consumer using actor model
func (b *Broker) UnregisterConsumer(consumerTag string) error {
	b.Mutex.Lock()

	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.Unlock()
		return errors.New("consumer not found")
	}

	queueName := consumer.Queue

	// Remove from queue's consumer list
	queueConsumers := b.QueueConsumers[queueName]
	newQueueConsumers := make([]string, 0)
	for _, tag := range queueConsumers {
		if tag != consumerTag {
			newQueueConsumers = append(newQueueConsumers, tag)
		}
	}
	b.QueueConsumers[queueName] = newQueueConsumers

	// Remove from broker's tracking
	delete(b.Consumers, consumerTag)

	// Get queue for unregistering
	queue := b.Queues[queueName]
	b.Mutex.Unlock()

	// Unregister from queue actor
	if queue != nil {
		queue.UnregisterConsumer(consumerTag)
	}

	return nil
}

// AcknowledgeMessage handles acknowledgment using actor model
func (b *Broker) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	b.Mutex.RLock()
	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.RUnlock()
		return errors.New("consumer not found")
	}

	queueName := consumer.Queue
	queue := b.Queues[queueName]
	b.Mutex.RUnlock()

	if queue == nil {
		return errors.New("queue not found")
	}

	// Send ack to queue actor - actor handles delivery resumption
	queue.Ack(consumerTag)

	return nil
}

// RejectMessage handles message rejection using actor model
func (b *Broker) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	b.Mutex.RLock()
	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.RUnlock()
		return errors.New("consumer not found")
	}

	queueName := consumer.Queue
	queue := b.Queues[queueName]
	b.Mutex.RUnlock()

	if queue == nil {
		return errors.New("queue not found")
	}

	// Send ack to queue actor (rejection = ack in simple implementation)
	queue.Ack(consumerTag)

	// TODO: Handle requeue logic in actor
	return nil
}

// NacknowledgeMessage handles negative acknowledgment using actor model
func (b *Broker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	b.Mutex.RLock()
	consumer, exists := b.Consumers[consumerTag]
	if !exists {
		b.Mutex.RUnlock()
		return errors.New("consumer not found")
	}

	queueName := consumer.Queue
	queue := b.Queues[queueName]
	b.Mutex.RUnlock()

	if queue == nil {
		return errors.New("queue not found")
	}

	// Send ack to queue actor (nack = ack in simple implementation)
	queue.Ack(consumerTag)

	// TODO: Handle multiple and requeue logic in actor
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
			deliveryTag := atomic.AddUint64(&b.DeliveryTag, 1)
			queue.Enqueue(deliveryTag, message)
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

// routeDirect routes messages to queues using actor model
func (b *Broker) routeDirect(exchange *protocol.Exchange, routingKey string, message *protocol.Message) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	for _, binding := range exchange.Bindings {
		if binding.RoutingKey == routingKey {
			if queue, exists := b.Queues[binding.Queue]; exists {
				deliveryTag := atomic.AddUint64(&b.DeliveryTag, 1)
				queue.Enqueue(deliveryTag, message)
			}
		}
	}

	return nil
}

// routeFanout routes messages to all bound queues using actor model
func (b *Broker) routeFanout(exchange *protocol.Exchange, message *protocol.Message) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	for _, binding := range exchange.Bindings {
		if queue, exists := b.Queues[binding.Queue]; exists {
			deliveryTag := atomic.AddUint64(&b.DeliveryTag, 1)
			queue.Enqueue(deliveryTag, message)
		}
	}

	return nil
}

// routeTopic routes messages based on routing pattern matching using actor model
func (b *Broker) routeTopic(exchange *protocol.Exchange, routingKey string, message *protocol.Message) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	// For topic routing, we match routing keys using patterns with * and #
	for _, binding := range exchange.Bindings {
		if topicMatches(binding.RoutingKey, routingKey) {
			if queue, exists := b.Queues[binding.Queue]; exists {
				deliveryTag := atomic.AddUint64(&b.DeliveryTag, 1)
				queue.Enqueue(deliveryTag, message)
			}
		}
	}

	return nil
}

// routeHeaders routes messages based on headers matching using actor model
func (b *Broker) routeHeaders(exchange *protocol.Exchange, message *protocol.Message, headers map[string]interface{}) error {
	exchange.Mutex.RLock()
	defer exchange.Mutex.RUnlock()

	for _, binding := range exchange.Bindings {
		if headersMatch(binding.Arguments, headers) {
			if queue, exists := b.Queues[binding.Queue]; exists {
				deliveryTag := atomic.AddUint64(&b.DeliveryTag, 1)
				queue.Enqueue(deliveryTag, message)
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

	// Create new queue with actor
	queue = protocol.NewQueue(name, durable, autoDelete, exclusive, args)

	// Actor model handles storage internally - no initialization needed!
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

	// Stop the queue actor
	queue.Stop()

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

// GetQueues returns the queues map (UnifiedBroker interface)
func (b *Broker) GetQueues() map[string]*protocol.Queue {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()
	return b.Queues
}

// GetExchanges returns the exchanges map (UnifiedBroker interface)
func (b *Broker) GetExchanges() map[string]*protocol.Exchange {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()
	return b.Exchanges
}

// GetConsumers returns the consumers map (UnifiedBroker interface)
func (b *Broker) GetConsumers() map[string]*protocol.Consumer {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()
	return b.Consumers
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
