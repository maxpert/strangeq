package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"golang.org/x/sync/semaphore"
)

// ErrConsumerChannelFull is returned when a consumer channel is full and cannot accept more messages
var ErrConsumerChannelFull = errors.New("consumer channel full")

// QueueState tracks the state of a queue using lock-free data structures
// This enables O(1) message delivery and atomic operations
type QueueState struct {
	available chan uint64   // Lock-free queue of available message IDs
	inflight  sync.Map      // msgID → consumerTag (in-flight messages)
	nextMsgID atomic.Uint64 // Monotonic counter for message IDs
}

// NewQueueState creates a new queue state with a configured buffered channel
// Phase 6E: Reduced from 100M to 10M based on benchmark analysis
// Phase 6G: Made configurable via EngineConfig.AvailableChannelBuffer
func NewQueueState(bufferSize int) *QueueState {
	return &QueueState{
		available: make(chan uint64, bufferSize),
	}
}

// ConsumerState tracks the state of an active consumer for pull-based delivery
type ConsumerState struct {
	consumer    *protocol.Consumer
	queueName   string
	prefetchSem *semaphore.Weighted // Prefetch flow control (nil for unlimited)
	semCtx      context.Context     // Context for semaphore cancellation
	semCancel   context.CancelFunc  // Cancel function for semaphore
	stopCh      chan struct{}       // Stop signal for goroutine
}

// StorageBroker manages exchanges, queues, and routing using persistent storage
// All fields use lock-free data structures for maximum concurrency
type StorageBroker struct {
	storage         interfaces.Storage
	engineConfig    interfaces.EngineConfig
	queueStates     sync.Map // queueName -> *QueueState (lock-free)
	activeQueues    sync.Map // queueName -> *protocol.Queue (lock-free)
	activeConsumers sync.Map // consumerTag -> *ConsumerState (lock-free)
	queueConsumers  sync.Map // queueName -> []*ConsumerState (lock-free)
	deliveryIndex   sync.Map // deliveryTag → consumerTag (global delivery lookup for O(1) ACK routing)
}

// NewStorageBroker creates a new storage-backed broker instance
// Phase 6G: Now accepts EngineConfig for tunable parameters
func NewStorageBroker(storage interfaces.Storage, engineConfig interfaces.EngineConfig) *StorageBroker {
	broker := &StorageBroker{
		storage:      storage,
		engineConfig: engineConfig,
		// All sync.Map fields don't need initialization
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

// getOrCreateQueueState returns the queue state, creating it if needed (lock-free)
func (b *StorageBroker) getOrCreateQueueState(queueName string) *QueueState {
	val, ok := b.queueStates.Load(queueName)
	if ok {
		return val.(*QueueState)
	}

	// Create new queue state with configured buffer size
	newState := NewQueueState(b.engineConfig.AvailableChannelBuffer)
	actual, _ := b.queueStates.LoadOrStore(queueName, newState)
	return actual.(*QueueState)
}

// consumerPollLoop implements three-stage pipeline with semaphore-based flow control
// Stage 1: Acquire semaphore permit (blocks if at prefetch limit)
// Stage 2: Pull message ID from available channel (only after capacity confirmed)
// Stage 3: Deliver message with blocking send (provides TCP backpressure)
func (b *StorageBroker) consumerPollLoop(state *ConsumerState, queueState *QueueState) {
	for {
		// STAGE 1: Acquire capacity permit (blocks if at prefetch limit)
		// This eliminates CPU spinning when consumer is at capacity
		if state.prefetchSem != nil {
			if err := state.prefetchSem.Acquire(state.semCtx, 1); err != nil {
				// Context cancelled (consumer unregistered)
				return
			}
		}

		// STAGE 2: Pull message ID from available channel
		// Only reached after semaphore acquired, ensuring we have capacity
		var msgID uint64
		select {
		case <-state.stopCh:
			// Consumer stopped, release unused permit and exit
			if state.prefetchSem != nil {
				state.prefetchSem.Release(1)
			}
			return
		case msgID = <-queueState.available:
			// Got message ID, proceed to delivery
		}

		// STAGE 3: Deliver message (blocking send provides backpressure)
		b.deliverMessage(queueState, state, msgID)
	}
}

// deliverMessage delivers a message to a consumer with blocking send
// This provides natural TCP backpressure when consumer is slow
// Messages are never put back in normal operation, preserving FIFO order
// Put-backs only occur on shutdown/cancellation (rare events)
func (b *StorageBroker) deliverMessage(queueState *QueueState, state *ConsumerState, msgID uint64) {
	// Mark as in-flight
	queueState.inflight.Store(msgID, state.consumer.Tag)
	b.deliveryIndex.Store(msgID, state.consumer.Tag)

	// Get message from storage
	message, err := b.storage.GetMessage(state.queueName, msgID)
	if err != nil {
		// Message not found - clean up and release permit
		queueState.inflight.Delete(msgID)
		b.deliveryIndex.Delete(msgID)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}
		return
	}

	// Set delivery tag
	message.DeliveryTag = msgID

	// Create pending ack record
	pendingAck := &protocol.PendingAck{
		QueueName:       state.queueName,
		DeliveryTag:     msgID,
		ConsumerTag:     state.consumer.Tag,
		MessageID:       fmt.Sprintf("%s:%d", state.queueName, msgID),
		DeliveredAt:     time.Now(),
		RedeliveryCount: 0,
		Redelivered:     false,
	}

	// Create delivery
	delivery := &protocol.Delivery{
		Message:     message,
		DeliveryTag: msgID,
		Redelivered: false,
		Exchange:    message.Exchange,
		RoutingKey:  message.RoutingKey,
		ConsumerTag: state.consumer.Tag,
	}

	// BLOCKING send to consumer channel with cancellation support
	// This provides TCP backpressure - if consumer is slow, we block here
	// Semaphore permit is held until ACK arrives
	select {
	case state.consumer.Messages <- delivery:
		// Success! Write pending ack after successful delivery
		b.storage.StorePendingAck(pendingAck)
		// Semaphore permit remains held until ACK/NACK/Reject

	case <-state.stopCh:
		// Consumer stopped - put message back (acceptable on shutdown)
		queueState.available <- msgID
		queueState.inflight.Delete(msgID)
		b.deliveryIndex.Delete(msgID)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}

	case <-state.semCtx.Done():
		// Context cancelled - put message back
		queueState.available <- msgID
		queueState.inflight.Delete(msgID)
		b.deliveryIndex.Delete(msgID)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}
	}
}

// DeclareExchange creates or updates an exchange
func (b *StorageBroker) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	// Check if exchange already exists (lock-free)
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

	err = b.storage.StoreExchange(exchange)
	if err != nil {
		return err
	}

	// Update durable entity metadata if this is a durable exchange
	if durable {
		err = b.updateDurableMetadata()
		if err != nil {
			// Log but don't fail - metadata update is not critical for operation
		}
	}

	return nil
}

// DeleteExchange removes an exchange
func (b *StorageBroker) DeleteExchange(name string, ifUnused bool) error {
	// Check if exchange exists (lock-free)
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
	// Check if queue already exists (lock-free)
	existing, err := b.storage.GetQueue(name)
	if err != nil && !errors.Is(err, interfaces.ErrQueueNotFound) {
		return nil, fmt.Errorf("failed to check existing queue: %w", err)
	}

	// If exists, validate properties match
	if existing != nil {
		if existing.Durable != durable || existing.AutoDelete != autoDelete || existing.Exclusive != exclusive {
			return nil, fmt.Errorf("queue '%s' properties mismatch", name)
		}

		// Add to active cache (lock-free)
		b.activeQueues.Store(name, existing)

		// Ensure queue state exists
		b.getOrCreateQueueState(name)

		return existing, nil
	}

	// Create new queue
	queue := protocol.NewQueue(name, durable, autoDelete, exclusive, arguments)

	// Store queue
	err = b.storage.StoreQueue(queue)
	if err != nil {
		return nil, err
	}

	// Add to active cache (lock-free)
	b.activeQueues.Store(name, queue)

	// Ensure queue state exists
	b.getOrCreateQueueState(name)

	// Update durable entity metadata if this is a durable queue
	if queue.Durable {
		err = b.updateDurableMetadata()
		if err != nil {
			// Log but don't fail - metadata update is not critical for operation
		}
	}

	return queue, nil
}

// DeleteQueue removes a queue
func (b *StorageBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	// Check if queue exists (lock-free)
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

	// Stop all consumers for this queue
	if val, ok := b.queueConsumers.Load(name); ok {
		consumers := val.([]*ConsumerState)
		for _, state := range consumers {
			close(state.stopCh)
			b.activeConsumers.Delete(state.consumer.Tag)
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
			b.activeConsumers.Delete(consumer.Tag)
		}
	}

	// Remove from active caches (lock-free)
	b.activeQueues.Delete(name)
	b.queueConsumers.Delete(name)
	b.queueStates.Delete(name)

	return b.storage.DeleteQueue(name)
}

// BindQueue binds a queue to an exchange
func (b *StorageBroker) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	// Validate exchange exists (lock-free)
	_, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("exchange '%s' not found", exchangeName)
		}
		return err
	}

	// Validate queue exists (lock-free)
	_, err = b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return fmt.Errorf("queue '%s' not found", queueName)
		}
		return err
	}

	// Create binding
	err = b.storage.StoreBinding(queueName, exchangeName, routingKey, arguments)
	if err != nil {
		return err
	}

	// Update durable entity metadata since bindings affect durable entities
	err = b.updateDurableMetadata()
	if err != nil {
		// Log but don't fail - metadata update is not critical for operation
	}

	return nil
}

// UnbindQueue removes a binding between queue and exchange
func (b *StorageBroker) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return b.storage.DeleteBinding(queueName, exchangeName, routingKey)
}

// RegisterConsumer registers a new consumer for a queue
func (b *StorageBroker) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	// Check if queue exists (lock-free)
	_, err := b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return errors.New("queue does not exist")
		}
		return err
	}

	// Get or create queue state (lock-free)
	queueState := b.getOrCreateQueueState(queueName)

	// Calculate semaphore capacity for prefetch flow control
	var prefetchSem *semaphore.Weighted
	var semCtx context.Context
	var semCancel context.CancelFunc

	prefetchCount := consumer.PrefetchCount
	if prefetchCount == 0 {
		// Unlimited prefetch: use RabbitMQ quorum queue limit (2000)
		prefetchSem = semaphore.NewWeighted(2000)
		semCtx, semCancel = context.WithCancel(context.Background())
	} else {
		// Limited prefetch: use exact prefetch count
		prefetchSem = semaphore.NewWeighted(int64(prefetchCount))
		semCtx, semCancel = context.WithCancel(context.Background())
	}

	// Create consumer state
	state := &ConsumerState{
		consumer:    consumer,
		queueName:   queueName,
		prefetchSem: prefetchSem,
		semCtx:      semCtx,
		semCancel:   semCancel,
		stopCh:      make(chan struct{}),
	}

	// Store in sync.Map (lock-free)
	b.activeConsumers.Store(consumerTag, state)

	// Add to queue consumers list (lock-free)
	val, _ := b.queueConsumers.LoadOrStore(queueName, []*ConsumerState{})
	consumers := val.([]*ConsumerState)
	consumers = append(consumers, state)
	b.queueConsumers.Store(queueName, consumers)

	// Start polling goroutine
	go b.consumerPollLoop(state, queueState)

	// Store consumer in storage
	err = b.storage.StoreConsumer(queueName, consumerTag, consumer)
	if err != nil {
		return fmt.Errorf("failed to store consumer: %w", err)
	}

	return nil
}

// UnregisterConsumer removes a consumer
func (b *StorageBroker) UnregisterConsumer(consumerTag string) error {
	// Load consumer state (lock-free)
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return errors.New("consumer not found")
	}
	state := val.(*ConsumerState)

	// Cancel semaphore context first to unblock any waiting goroutines
	if state.semCancel != nil {
		state.semCancel()
	}

	// Then stop polling goroutine
	close(state.stopCh)

	// Remove from activeConsumers
	b.activeConsumers.Delete(consumerTag)

	// Remove from queueConsumers list (lock-free)
	if val, ok := b.queueConsumers.Load(state.queueName); ok {
		consumers := val.([]*ConsumerState)
		// Filter out this consumer
		newConsumers := make([]*ConsumerState, 0, len(consumers)-1)
		for _, c := range consumers {
			if c.consumer.Tag != consumerTag {
				newConsumers = append(newConsumers, c)
			}
		}
		if len(newConsumers) > 0 {
			b.queueConsumers.Store(state.queueName, newConsumers)
		} else {
			b.queueConsumers.Delete(state.queueName)
		}
	}

	// Delete from storage
	err := b.storage.DeleteConsumer(state.queueName, consumerTag)
	if err != nil && !errors.Is(err, interfaces.ErrConsumerNotFound) {
		return err
	}

	return nil
}

// PublishMessage publishes a message to an exchange
func (b *StorageBroker) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	// Get exchange (lock-free)
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("exchange '%s' not found", exchangeName)
		}
		return err
	}

	// Find target queues based on exchange type and routing
	targetQueues, err := b.findTargetQueues(exchange, routingKey, message)
	if err != nil {
		return err
	}

	// Enqueue to all target queues (lock-free, no consumer iteration)
	for _, queueName := range targetQueues {
		// Get or create queue state (lock-free)
		queueState := b.getOrCreateQueueState(queueName)

		// Assign message ID atomically
		msgID := queueState.nextMsgID.Add(1)
		message.DeliveryTag = msgID

		// Store to persistent storage
		err := b.storage.StoreMessage(queueName, message)
		if err != nil {
			return fmt.Errorf("failed to store message to queue '%s': %w", queueName, err)
		}

		// Enqueue ID in available channel
		queueState.available <- msgID

		// Done - polling loops will discover it (NO consumer iteration needed)
	}

	return nil
}

// findTargetQueues determines which queues should receive the message
// CRITICAL FIX: Check in-memory cache FIRST before falling back to storage
func (b *StorageBroker) findTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message) ([]string, error) {
	var targetQueues []string

	// For default exchange, route directly to queue with same name as routing key
	if exchange.Name == "" {
		// FIX: Check in-memory cache FIRST (lock-free)
		if _, ok := b.activeQueues.Load(routingKey); ok {
			targetQueues = append(targetQueues, routingKey)
			return targetQueues, nil
		}

		// Fallback to storage if not in cache
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
	// Return copy of active consumers (lock-free iteration)
	result := make(map[string]*protocol.Consumer)
	b.activeConsumers.Range(func(key, value interface{}) bool {
		tag := key.(string)
		state := value.(*ConsumerState)
		result[tag] = state.consumer
		return true // continue iteration
	})

	return result
}

// updateDurableMetadata updates the durable entity metadata in storage
func (b *StorageBroker) updateDurableMetadata() error {
	// Collect all durable exchanges from storage
	exchanges, err := b.storage.ListExchanges()
	if err != nil {
		return err
	}

	// Collect all durable queues from storage
	queues, err := b.storage.ListQueues()
	if err != nil {
		return err
	}

	// Collect all bindings from storage
	allBindings := []protocol.Binding{}
	for _, exchange := range exchanges {
		if exchange.Durable {
			bindings, err := b.storage.GetExchangeBindings(exchange.Name)
			if err != nil {
				continue // Skip this exchange if we can't get its bindings
			}
			// Convert from QueueBinding to Binding
			for _, queueBinding := range bindings {
				binding := protocol.Binding{
					Exchange:   queueBinding.ExchangeName,
					Queue:      queueBinding.QueueName,
					RoutingKey: queueBinding.RoutingKey,
					Arguments:  queueBinding.Arguments,
				}
				allBindings = append(allBindings, binding)
			}
		}
	}

	// Create metadata structure
	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []*protocol.Exchange{},
		Queues:      []*protocol.Queue{},
		Bindings:    allBindings,
		LastUpdated: time.Now(),
	}

	// Filter durable exchanges
	for _, exchange := range exchanges {
		if exchange.Durable {
			exchangeCopy := exchange.Copy()
			metadata.Exchanges = append(metadata.Exchanges, &exchangeCopy)
		}
	}

	// Filter durable queues
	for _, queue := range queues {
		if queue.Durable {
			metadata.Queues = append(metadata.Queues, queue)
		}
	}

	// Store updated metadata
	return b.storage.StoreDurableEntityMetadata(metadata)
}

// Compatibility methods matching the original broker interface

// AcknowledgeMessage handles message acknowledgment (lock-free hot path)
func (b *StorageBroker) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	// Load consumer state (lock-free)
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return errors.New("consumer not found")
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	if multiple {
		// Acknowledge all messages up to and including deliveryTag
		ackedCount := 0
		pendingAcks, err := b.storage.GetConsumerPendingAcks(consumerTag)
		if err == nil {
			for _, pendingAck := range pendingAcks {
				if pendingAck.DeliveryTag <= deliveryTag {
					// Delete message and pending ack from storage
					b.storage.DeleteMessage(pendingAck.QueueName, pendingAck.DeliveryTag)
					b.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)

					// Remove from inflight and delivery index
					queueState.inflight.Delete(pendingAck.DeliveryTag)
					b.deliveryIndex.Delete(pendingAck.DeliveryTag)
					ackedCount++
				}
			}
		}
		// Release semaphore permits for all ACKed messages
		if state.prefetchSem != nil && ackedCount > 0 {
			state.prefetchSem.Release(int64(ackedCount))
		}
	} else {
		// Acknowledge single message
		// Delete message and pending ack from storage
		b.storage.DeleteMessage(state.queueName, deliveryTag)
		b.storage.DeletePendingAck(state.queueName, deliveryTag)

		// Remove from inflight and delivery index
		queueState.inflight.Delete(deliveryTag)
		b.deliveryIndex.Delete(deliveryTag)

		// Release semaphore permit (frees capacity for next message)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}
	}

	// Done - polling loop will acquire permit and pull next message
	return nil
}

// RejectMessage handles message rejection (lock-free hot path)
func (b *StorageBroker) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	// Load consumer state (lock-free)
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return errors.New("consumer not found")
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	// Remove from inflight and delivery index
	queueState.inflight.Delete(deliveryTag)
	b.deliveryIndex.Delete(deliveryTag)

	// Handle acknowledgment tracking for rejected message
	pendingAck, err := b.storage.GetPendingAck(state.queueName, deliveryTag)
	if err == nil && pendingAck != nil {
		if requeue {
			// Remove from pending acks and requeue
			b.storage.DeletePendingAck(state.queueName, deliveryTag)

			// Put back in available queue
			queueState.available <- deliveryTag
		} else {
			// Remove from both pending acks and storage (message discarded)
			b.storage.DeletePendingAck(state.queueName, deliveryTag)
			b.storage.DeleteMessage(state.queueName, deliveryTag)
		}
	}

	// Release semaphore permit (frees capacity for next message)
	if state.prefetchSem != nil {
		state.prefetchSem.Release(1)
	}

	return nil
}

// NacknowledgeMessage handles negative acknowledgment (lock-free hot path)
func (b *StorageBroker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	// Load consumer state (lock-free)
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return errors.New("consumer not found")
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	if multiple {
		// Nacknowledge all messages up to and including deliveryTag
		nackedCount := 0
		pendingAcks, err := b.storage.GetConsumerPendingAcks(consumerTag)
		if err == nil {
			for _, pendingAck := range pendingAcks {
				if pendingAck.DeliveryTag <= deliveryTag {
					// Remove from inflight and delivery index
					queueState.inflight.Delete(pendingAck.DeliveryTag)
					b.deliveryIndex.Delete(pendingAck.DeliveryTag)

					// Handle requeue vs discard
					if requeue {
						// Remove from pending acks and requeue
						b.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)

						// Put back in available queue
						queueState.available <- pendingAck.DeliveryTag
					} else {
						// Remove from both pending acks and storage (message discarded)
						b.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
						b.storage.DeleteMessage(pendingAck.QueueName, pendingAck.DeliveryTag)
					}
					nackedCount++
				}
			}
		}
		// Release semaphore permits for all NACKed messages
		if state.prefetchSem != nil && nackedCount > 0 {
			state.prefetchSem.Release(int64(nackedCount))
		}
	} else {
		// Nacknowledge single message
		// Remove from inflight and delivery index
		queueState.inflight.Delete(deliveryTag)
		b.deliveryIndex.Delete(deliveryTag)

		pendingAck, err := b.storage.GetPendingAck(state.queueName, deliveryTag)
		if err == nil && pendingAck != nil {
			if requeue {
				// Remove from pending acks and requeue
				b.storage.DeletePendingAck(state.queueName, deliveryTag)

				// Put back in available queue
				queueState.available <- deliveryTag
			} else {
				// Remove from both pending acks and storage (message discarded)
				b.storage.DeletePendingAck(state.queueName, deliveryTag)
				b.storage.DeleteMessage(state.queueName, deliveryTag)
			}
		}

		// Release semaphore permit (frees capacity for next message)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}
	}

	return nil
}

// GetConsumerForDelivery returns the consumer tag for a given delivery tag
// This provides O(1) lookup for ACK routing, fixing the broken consumer lookup bug
func (b *StorageBroker) GetConsumerForDelivery(deliveryTag uint64) (string, bool) {
	val, ok := b.deliveryIndex.Load(deliveryTag)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// RebuildDeliveryIndex rebuilds a single delivery index entry (used during crash recovery)
func (b *StorageBroker) RebuildDeliveryIndex(deliveryTag uint64, consumerTag string) {
	b.deliveryIndex.Store(deliveryTag, consumerTag)
}
