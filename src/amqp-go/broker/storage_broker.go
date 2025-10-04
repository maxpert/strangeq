package broker

import (
	"errors"
	"fmt"
	"sync"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// StorageBroker manages exchanges, queues, and routing using persistent storage
type StorageBroker struct {
	storage        interfaces.Storage
	activeQueues   map[string]*protocol.Queue    // In-memory cache of active queues
	activeConsumers map[string]*protocol.Consumer // In-memory cache of active consumers
	mutex          sync.RWMutex
}

// NewStorageBroker creates a new storage-backed broker instance
func NewStorageBroker(storage interfaces.Storage) *StorageBroker {
	broker := &StorageBroker{
		storage:        storage,
		activeQueues:   make(map[string]*protocol.Queue),
		activeConsumers: make(map[string]*protocol.Consumer),
	}
	
	// Initialize with default exchanges
	broker.initializeDefaultExchanges()
	
	return broker
}

// initializeDefaultExchanges creates the standard AMQP exchanges
func (b *StorageBroker) initializeDefaultExchanges() {
	// Default exchange (direct)
	defaultExchange := &protocol.Exchange{
		Name:       "",
		Kind:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	
	// Standard exchanges
	directExchange := &protocol.Exchange{
		Name:       "amq.direct",
		Kind:       "direct", 
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	
	fanoutExchange := &protocol.Exchange{
		Name:       "amq.fanout",
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	
	topicExchange := &protocol.Exchange{
		Name:       "amq.topic",
		Kind:       "topic",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	
	headersExchange := &protocol.Exchange{
		Name:       "amq.headers",
		Kind:       "headers",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	
	// Store default exchanges (ignore errors if they already exist)
	b.storage.StoreExchange(defaultExchange)
	b.storage.StoreExchange(directExchange)
	b.storage.StoreExchange(fanoutExchange)
	b.storage.StoreExchange(topicExchange)
	b.storage.StoreExchange(headersExchange)
}

// DeclareExchange creates or updates an exchange
func (b *StorageBroker) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	// Check if exchange already exists
	existing, err := b.storage.GetExchange(name)
	if err != nil && !errors.Is(err, interfaces.ErrExchangeNotFound) {
		return fmt.Errorf("failed to check existing exchange: %w", err)
	}
	
	// If exists, validate properties match
	if existing != nil {
		if existing.Kind != exchangeType {
			return fmt.Errorf("exchange '%s' type mismatch: expected %s, got %s", name, existing.Kind, exchangeType)
		}
		// Exchange already exists with matching properties
		return nil
	}
	
	// Create new exchange
	exchange := &protocol.Exchange{
		Name:       name,
		Kind:       exchangeType,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
		Arguments:  make(map[string]interface{}),
	}
	
	// Copy arguments
	for k, v := range arguments {
		exchange.Arguments[k] = v
	}
	
	return b.storage.StoreExchange(exchange)
}

// DeleteExchange removes an exchange
func (b *StorageBroker) DeleteExchange(name string, ifUnused bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	// Check if exchange exists
	_, err := b.storage.GetExchange(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return nil // Exchange doesn't exist, consider it deleted
		}
		return err
	}
	
	// If ifUnused is true, check for bindings
	if ifUnused {
		bindings, err := b.storage.GetExchangeBindings(name)
		if err != nil {
			return fmt.Errorf("failed to check exchange bindings: %w", err)
		}
		if len(bindings) > 0 {
			return fmt.Errorf("exchange '%s' in use", name)
		}
	}
	
	return b.storage.DeleteExchange(name)
}

// DeclareQueue creates or updates a queue
func (b *StorageBroker) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	// Check if queue already exists
	existing, err := b.storage.GetQueue(name)
	if err != nil && !errors.Is(err, interfaces.ErrQueueNotFound) {
		return nil, fmt.Errorf("failed to check existing queue: %w", err)
	}
	
	// If exists, validate properties match
	if existing != nil {
		if existing.Durable != durable || existing.AutoDelete != autoDelete || existing.Exclusive != exclusive {
			return nil, fmt.Errorf("queue '%s' properties mismatch", name)
		}
		
		// Add to active cache
		b.activeQueues[name] = existing
		return existing, nil
	}
	
	// Create new queue
	queue := &protocol.Queue{
		Name:       name,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		Arguments:  make(map[string]interface{}),
		Messages:   make([]*protocol.Message, 0), // This will not be persisted
	}
	
	// Copy arguments
	for k, v := range arguments {
		queue.Arguments[k] = v
	}
	
	// Store queue
	err = b.storage.StoreQueue(queue)
	if err != nil {
		return nil, err
	}
	
	// Add to active cache
	b.activeQueues[name] = queue
	
	return queue, nil
}

// DeleteQueue removes a queue
func (b *StorageBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	// Check if queue exists
	_, err := b.storage.GetQueue(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return nil // Queue doesn't exist, consider it deleted
		}
		return err
	}
	
	// Check if unused (no consumers)
	if ifUnused {
		consumers, err := b.storage.GetQueueConsumers(name)
		if err != nil {
			return fmt.Errorf("failed to check queue consumers: %w", err)
		}
		if len(consumers) > 0 {
			return fmt.Errorf("queue '%s' in use", name)
		}
	}
	
	// Check if empty (no messages)
	if ifEmpty {
		count, err := b.storage.GetQueueMessageCount(name)
		if err != nil {
			return fmt.Errorf("failed to check queue message count: %w", err)
		}
		if count > 0 {
			return fmt.Errorf("queue '%s' not empty", name)
		}
	}
	
	// Delete all messages in the queue
	b.storage.PurgeQueue(name)
	
	// Delete all bindings for this queue
	bindings, err := b.storage.GetQueueBindings(name)
	if err == nil {
		for _, binding := range bindings {
			b.storage.DeleteBinding(binding.QueueName, binding.ExchangeName, binding.RoutingKey)
		}
	}
	
	// Delete all consumers for this queue
	consumers, err := b.storage.GetQueueConsumers(name)
	if err == nil {
		for _, consumer := range consumers {
			b.storage.DeleteConsumer(name, consumer.Tag)
			delete(b.activeConsumers, consumer.Tag)
		}
	}
	
	// Remove from active cache
	delete(b.activeQueues, name)
	
	return b.storage.DeleteQueue(name)
}

// BindQueue binds a queue to an exchange
func (b *StorageBroker) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	// Validate exchange exists
	_, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("exchange '%s' not found", exchangeName)
		}
		return err
	}
	
	// Validate queue exists
	_, err = b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return fmt.Errorf("queue '%s' not found", queueName)
		}
		return err
	}
	
	// Create binding
	return b.storage.StoreBinding(queueName, exchangeName, routingKey, arguments)
}

// UnbindQueue removes a binding between queue and exchange
func (b *StorageBroker) UnbindQueue(queueName, exchangeName, routingKey string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	return b.storage.DeleteBinding(queueName, exchangeName, routingKey)
}

// RegisterConsumer registers a new consumer for a queue
func (b *StorageBroker) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	var messagesToDeliver []*protocol.Message
	
	b.mutex.Lock()
	
	// Check if queue exists
	_, err := b.storage.GetQueue(queueName)
	if err != nil {
		b.mutex.Unlock()
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return errors.New("queue does not exist")
		}
		return err
	}
	
	// Store consumer
	err = b.storage.StoreConsumer(queueName, consumerTag, consumer)
	if err != nil {
		b.mutex.Unlock()
		return fmt.Errorf("failed to store consumer: %w", err)
	}
	
	// Add to active cache
	b.activeConsumers[consumerTag] = consumer
	
	// Get pending messages for immediate delivery
	messagesToDeliver, err = b.storage.GetQueueMessages(queueName)
	if err != nil {
		b.mutex.Unlock()
		return fmt.Errorf("failed to get queue messages: %w", err)
	}
	
	b.mutex.Unlock()
	
	// Deliver pending messages (outside lock to avoid deadlock)
	for _, message := range messagesToDeliver {
		b.deliverMessageToConsumer(consumer, message)
	}
	
	return nil
}

// UnregisterConsumer removes a consumer
func (b *StorageBroker) UnregisterConsumer(consumerTag string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	
	// Find the queue for this consumer
	consumer, exists := b.activeConsumers[consumerTag]
	if !exists {
		return fmt.Errorf("consumer '%s' not found", consumerTag)
	}
	
	// Remove from storage
	err := b.storage.DeleteConsumer(consumer.Queue, consumerTag)
	if err != nil && !errors.Is(err, interfaces.ErrConsumerNotFound) {
		return err
	}
	
	// Remove from active cache
	delete(b.activeConsumers, consumerTag)
	
	return nil
}

// PublishMessage publishes a message to an exchange
func (b *StorageBroker) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	b.mutex.RLock()
	
	// Get exchange
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		b.mutex.RUnlock()
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("exchange '%s' not found", exchangeName)
		}
		return err
	}
	
	// Find target queues based on exchange type and routing
	targetQueues, err := b.findTargetQueues(exchange, routingKey, message)
	b.mutex.RUnlock()
	
	if err != nil {
		return err
	}
	
	// Deliver to all target queues
	for _, queueName := range targetQueues {
		err := b.deliverMessageToQueue(queueName, message)
		if err != nil {
			return fmt.Errorf("failed to deliver message to queue '%s': %w", queueName, err)
		}
	}
	
	return nil
}

// findTargetQueues determines which queues should receive the message
func (b *StorageBroker) findTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message) ([]string, error) {
	var targetQueues []string
	
	// For default exchange, route directly to queue with same name as routing key
	if exchange.Name == "" {
		// Check if queue exists
		_, err := b.storage.GetQueue(routingKey)
		if err == nil {
			targetQueues = append(targetQueues, routingKey)
		}
		return targetQueues, nil
	}
	
	// Get all bindings for this exchange
	bindings, err := b.storage.GetExchangeBindings(exchange.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get exchange bindings: %w", err)
	}
	
	// Route based on exchange type
	switch exchange.Kind {
	case "direct":
		for _, binding := range bindings {
			if binding.RoutingKey == routingKey {
				targetQueues = append(targetQueues, binding.QueueName)
			}
		}
		
	case "fanout":
		for _, binding := range bindings {
			targetQueues = append(targetQueues, binding.QueueName)
		}
		
	case "topic":
		for _, binding := range bindings {
			if b.matchTopicPattern(binding.RoutingKey, routingKey) {
				targetQueues = append(targetQueues, binding.QueueName)
			}
		}
		
	case "headers":
		for _, binding := range bindings {
			if b.matchHeaders(binding.Arguments, message.Headers) {
				targetQueues = append(targetQueues, binding.QueueName)
			}
		}
	}
	
	return targetQueues, nil
}

// deliverMessageToQueue delivers a message to a specific queue
func (b *StorageBroker) deliverMessageToQueue(queueName string, message *protocol.Message) error {
	// Store message persistently
	err := b.storage.StoreMessage(queueName, message)
	if err != nil {
		return fmt.Errorf("failed to store message: %w", err)
	}
	
	// Try immediate delivery to active consumers
	consumers, err := b.storage.GetQueueConsumers(queueName)
	if err != nil {
		return fmt.Errorf("failed to get queue consumers: %w", err)
	}
	
	// Deliver to first available consumer
	for _, consumer := range consumers {
		if activeConsumer, exists := b.activeConsumers[consumer.Tag]; exists {
			b.deliverMessageToConsumer(activeConsumer, message)
			// Remove message from storage after successful delivery
			b.storage.DeleteMessage(queueName, message.DeliveryTag)
			break
		}
	}
	
	return nil
}

// deliverMessageToConsumer delivers a message to a specific consumer
func (b *StorageBroker) deliverMessageToConsumer(consumer *protocol.Consumer, message *protocol.Message) {
	if consumer.Messages == nil {
		return // Consumer not ready to receive messages
	}
	
	delivery := &protocol.Delivery{
		Message:     message,
		ConsumerTag: consumer.Tag,
		DeliveryTag: message.DeliveryTag,
		Exchange:    message.Exchange,
		RoutingKey:  message.RoutingKey,
	}
	
	// Non-blocking delivery
	select {
	case consumer.Messages <- delivery:
		// Message delivered successfully
	default:
		// Consumer channel is full, message stays in queue
	}
}

// Helper methods for routing

func (b *StorageBroker) matchTopicPattern(pattern, routingKey string) bool {
	// Simplified topic matching - in a full implementation, this would handle wildcards
	// * matches one word
	// # matches zero or more words
	return pattern == routingKey || pattern == "#"
}

func (b *StorageBroker) matchHeaders(bindingArgs map[string]interface{}, messageHeaders map[string]interface{}) bool {
	if bindingArgs == nil || messageHeaders == nil {
		return false
	}
	
	// Simplified headers matching - in a full implementation, this would handle x-match
	for key, value := range bindingArgs {
		if key == "x-match" {
			continue // Skip matching mode
		}
		
		msgValue, exists := messageHeaders[key]
		if !exists || msgValue != value {
			return false
		}
	}
	
	return true
}

// Compatibility methods for existing broker interface

// GetQueues returns all queues (for management interface)
func (b *StorageBroker) GetQueues() map[string]*protocol.Queue {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	
	queues, err := b.storage.ListQueues()
	if err != nil {
		return make(map[string]*protocol.Queue)
	}
	
	result := make(map[string]*protocol.Queue)
	for _, queue := range queues {
		result[queue.Name] = queue
	}
	
	return result
}

// GetExchanges returns all exchanges (for management interface)
func (b *StorageBroker) GetExchanges() map[string]*protocol.Exchange {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	
	exchanges, err := b.storage.ListExchanges()
	if err != nil {
		return make(map[string]*protocol.Exchange)
	}
	
	result := make(map[string]*protocol.Exchange)
	for _, exchange := range exchanges {
		result[exchange.Name] = exchange
	}
	
	return result
}

// GetConsumers returns all active consumers (for management interface)
func (b *StorageBroker) GetConsumers() map[string]*protocol.Consumer {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	
	// Return copy of active consumers
	result := make(map[string]*protocol.Consumer)
	for tag, consumer := range b.activeConsumers {
		result[tag] = consumer
	}
	
	return result
}

// Compatibility methods matching the original broker interface

// AcknowledgeMessage handles message acknowledgment
func (b *StorageBroker) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	consumer, exists := b.activeConsumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Update consumer's unacked count
	consumer.Channel.Mutex.Lock()
	defer consumer.Channel.Mutex.Unlock()

	if multiple {
		// Acknowledge all messages up to and including deliveryTag
		consumer.CurrentUnacked = 0
	} else {
		// Acknowledge single message
		if consumer.CurrentUnacked > 0 {
			consumer.CurrentUnacked--
		}
	}

	// In a complete implementation, we would:
	// 1. Remove specific messages from storage based on delivery tag tracking
	// 2. Handle persistent acknowledgment records
	// For now, we provide basic compatibility

	return nil
}

// RejectMessage handles message rejection
func (b *StorageBroker) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	consumer, exists := b.activeConsumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Update consumer's unacked count
	consumer.Channel.Mutex.Lock()
	defer consumer.Channel.Mutex.Unlock()

	if consumer.CurrentUnacked > 0 {
		consumer.CurrentUnacked--
	}

	// In a complete implementation, we would:
	// 1. Remove the message from unacknowledged tracking
	// 2. If requeue=true, put the message back on the queue storage
	// 3. If requeue=false, potentially dead-letter the message

	return nil
}

// NacknowledgeMessage handles negative acknowledgment
func (b *StorageBroker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	consumer, exists := b.activeConsumers[consumerTag]
	if !exists {
		return errors.New("consumer not found")
	}

	// Update consumer's unacked count
	consumer.Channel.Mutex.Lock()
	defer consumer.Channel.Mutex.Unlock()

	if multiple {
		// Nacknowledge all messages up to and including deliveryTag
		consumer.CurrentUnacked = 0
	} else {
		// Nacknowledge single message
		if consumer.CurrentUnacked > 0 {
			consumer.CurrentUnacked--
		}
	}

	// In a complete implementation, we would:
	// 1. Remove the message(s) from unacknowledged tracking
	// 2. If requeue=true, put the message(s) back on the queue storage
	// 3. If requeue=false, potentially dead-letter the message

	return nil
}