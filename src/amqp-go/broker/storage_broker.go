package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"golang.org/x/sync/semaphore"
)

// ErrConsumerChannelFull is returned when a consumer channel is full and cannot accept more messages
var ErrConsumerChannelFull = errors.New("consumer channel full")

// QueueState and its constructor NewQueueState now live in queue_dispatch.go,
// replacing the old `available chan uint64` with a lock-free tail cursor +
// condvar park/wake + bounded requeue ring. See queue_dispatch.go for the
// full design.

// ConsumerState tracks the state of an active consumer for pull-based delivery
type ConsumerState struct {
	consumer    *protocol.Consumer
	queueName   string
	prefetchSem *semaphore.Weighted // Prefetch flow control (nil for unlimited)
	semCtx      context.Context     // Context for semaphore cancellation
	semCancel   context.CancelFunc  // Cancel function for semaphore
	stopCh      chan struct{}       // Stop signal for goroutine
	stopOnce    sync.Once           // Ensures stopCh is closed exactly once
	done        chan struct{}       // Closed when consumerPollLoop has fully exited
}

// StorageBroker manages exchanges, queues, and routing using persistent storage
// All fields use lock-free data structures for maximum concurrency
type StorageBroker struct {
	storage           interfaces.Storage
	engineConfig      interfaces.EngineConfig
	queueStates       sync.Map      // queueName -> *QueueState (lock-free)
	activeQueues      sync.Map      // queueName -> *protocol.Queue (lock-free)
	activeConsumers   sync.Map      // consumerTag -> *ConsumerState (lock-free)
	queueConsumers    sync.Map      // queueName -> []*ConsumerState (lock-free)
	queueConsumersMu  sync.Map      // queueName -> *sync.Mutex (protects queueConsumers slice mutation)
	deliveryIndex     sync.Map      // deliveryTag → consumerTag (global delivery lookup for O(1) ACK routing)
	globalDeliveryTag atomic.Uint64 // Global counter for unique delivery tags across all queues
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

// getQueueConsumersMutex returns the per-queue mutex for protecting queueConsumers slice mutations.
// Uses sync.Map LoadOrStore to ensure exactly one mutex per queue.
func (b *StorageBroker) getQueueConsumersMutex(queueName string) *sync.Mutex {
	val, _ := b.queueConsumersMu.LoadOrStore(queueName, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// getOrCreateQueueState returns the queue state, creating it if needed (lock-free)
func (b *StorageBroker) getOrCreateQueueState(queueName string) *QueueState {
	val, ok := b.queueStates.Load(queueName)
	if ok {
		return val.(*QueueState)
	}

	// Create new queue state with a backpressure high-water mark derived from
	// the ring-buffer spill threshold. The depth gate (head - minAckCursor)
	// coordinates broker-level publisher backpressure with storage-level
	// spilling: once unacked depth reaches the spill threshold, publishers
	// block in WaitForCapacity until consumers ack and drain.
	newState := NewQueueState(b.computeDepthHighWM())
	actual, _ := b.queueStates.LoadOrStore(queueName, newState)
	return actual.(*QueueState)
}

// computeDepthHighWM derives the per-queue backpressure high-water mark from
// the engine config: ring buffer size × spill threshold %. This is the depth
// (head - minAckCursor) at which publishers block or spill. Falls back to
// sensible defaults when config fields are unset.
func (b *StorageBroker) computeDepthHighWM() uint64 {
	ringSize := b.engineConfig.RingBufferSize
	if ringSize <= 0 {
		ringSize = 1024 * 256
	}
	spillPct := b.engineConfig.SpillThresholdPercent
	if spillPct <= 0 {
		spillPct = 80
	}
	return uint64(ringSize) * uint64(spillPct) / 100
}

// consumerPollLoop implements three-stage pipeline with semaphore-based flow control
// Stage 1: Acquire semaphore permit (blocks if at prefetch limit)
// Stage 2: Claim a delivery tag from the queue's tail cursor (parks if none)
// Stage 3: Deliver message with blocking send (provides TCP backpressure)
func (b *StorageBroker) consumerPollLoop(state *ConsumerState, queueState *QueueState) {
	defer close(state.done)
	parkTimer := time.NewTimer(queueState.parkTimeout)
	defer parkTimer.Stop()
	for {
		if state.prefetchSem != nil {
			if err := state.prefetchSem.Acquire(state.semCtx, 1); err != nil {
				return
			}
		}

		tag, ok := queueState.Claim(state.stopCh, parkTimer)
		if !ok {
			// Consumer stopped, release unused permit and exit
			if state.prefetchSem != nil {
				state.prefetchSem.Release(1)
			}
			return
		}

		// STAGE 3: Deliver message (blocking send provides backpressure)
		b.deliverMessage(queueState, state, tag)
	}
}

// deliverMessage delivers a message to a consumer with blocking send
// This provides natural TCP backpressure when consumer is slow
// Messages are never put back in normal operation, preserving FIFO order
// Put-backs only occur on shutdown/cancellation (rare events) via Requeue
func (b *StorageBroker) deliverMessage(queueState *QueueState, state *ConsumerState, msgID uint64) {
	// Mark as in-flight: ownership map + depth counter.
	queueState.StoreInflight(msgID, state.consumer.Tag)
	queueState.ClaimInflight(msgID)
	b.deliveryIndex.Store(msgID, state.consumer.Tag)

	// Get message from storage
	message, err := b.storage.GetMessage(state.queueName, msgID)
	if err != nil {
		// Message not found (gap in recovered range, or spilled/corrupted).
		// Treat as resolved: release in-flight accounting and permit, then
		// continue to the next claim. The tag is gone and must not count
		// toward queue depth.
		queueState.DeleteInflight(msgID)
		b.deliveryIndex.Delete(msgID)
		queueState.AckAdvance(msgID)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}
		return
	}

	// Create pending ack record
	pendingAck := &protocol.PendingAck{
		QueueName:   state.queueName,
		DeliveryTag: msgID,
		ConsumerTag: state.consumer.Tag,
		// DeliveredAt and MessageID intentionally omitted:
		// - MessageID is never read in production (only in tests)
		// - DeliveredAt is only read in crash recovery (recovery_manager.go),
		//   and pending acks are in-memory only (not persisted), so
		//   GetAllPendingAcks returns empty after restart. The IsZero()
		//   guard in recovery skips the age check for zero-valued timestamps.
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
		// Success! Register with AckCursor for wraparound tracking
		b.storage.DeliverToConsumer(state.queueName, state.consumer.Tag, msgID)
		b.storage.StorePendingAck(pendingAck)
		// Semaphore permit remains held until ACK/NACK/Reject

	case <-state.stopCh:
		// Consumer stopped - put message back via Requeue so another
		// consumer redelivers it. Requeue releases the in-flight slot
		// (depth counter) and pushes the tag to the requeue ring.
		queueState.DeleteInflight(msgID)
		b.deliveryIndex.Delete(msgID)
		queueState.Requeue(msgID)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}

	case <-state.semCtx.Done():
		// Context cancelled - put message back via Requeue.
		queueState.DeleteInflight(msgID)
		b.deliveryIndex.Delete(msgID)
		queueState.Requeue(msgID)
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

	// Stop all consumers for this queue (protected by per-queue mutex)
	mu := b.getQueueConsumersMutex(name)
	mu.Lock()
	if val, ok := b.queueConsumers.Load(name); ok {
		consumers := val.([]*ConsumerState)
		for _, state := range consumers {
			if state.semCancel != nil {
				state.semCancel()
			}
			state.stopOnce.Do(func() { close(state.stopCh) })
			b.activeConsumers.Delete(state.consumer.Tag)
		}
	}
	b.queueConsumers.Delete(name)
	mu.Unlock()

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
	// Close the dispatch cursor: wakes parked consumers and blocked
	// publishers so they exit promptly, then drop our reference.
	if qval, qok := b.queueStates.LoadAndDelete(name); qok {
		qval.(*QueueState).Close()
	}

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
		done:        make(chan struct{}),
	}

	// Store in sync.Map (lock-free)
	b.activeConsumers.Store(consumerTag, state)

	// Register consumer with AckCursor for wraparound safety tracking
	b.storage.RegisterConsumerCursor(queueName, consumerTag)

	// Add to queue consumers list (protected by per-queue mutex to prevent lost updates)
	mu := b.getQueueConsumersMutex(queueName)
	mu.Lock()
	defer mu.Unlock()
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

// UnregisterConsumer removes a consumer and requeues all its in-flight
// messages (delivered but not yet ACKed) so they can be redelivered to other
// consumers. Without this cleanup, messages buffered in consumer.Messages or
// already pulled by the server's forwarder goroutine would be permanently
// orphaned: the deliveryIndex would still map their tags, the QueueState
// inflight counter would still count them, and pendingAck records would
// persist in storage — but no one would ever ACK or requeue them.
func (b *StorageBroker) UnregisterConsumer(consumerTag string) error {
	// Load consumer state (lock-free)
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return errors.New("consumer not found")
	}
	state := val.(*ConsumerState)

	// 1. Cancel semaphore context — unblocks the poll loop if it is parked
	//    in prefetchSem.Acquire(semCtx, 1).
	if state.semCancel != nil {
		state.semCancel()
	}

	// 2. Stop the poll loop (idempotent via sync.Once). After this, the poll
	//    loop's Claim() returns false (stopCh path) and deliverMessage's
	//    select takes the stopCh branch (requeuing the in-flight message).
	//    NOTE: Go's select chooses randomly when both the send case and the
	//    stopCh case are ready, so deliverMessage MAY still send one last
	//    message to consumer.Messages before the loop exits. We handle this
	//    by draining the channel after the loop has fully exited (step 6).
	state.stopOnce.Do(func() { close(state.stopCh) })

	// 3. Wake all parked consumers on this queue so the unregistered
	//    consumer's Claim() returns promptly. Other consumers re-check and
	//    re-park harmlessly.
	queueState := b.getOrCreateQueueState(state.queueName)
	queueState.WakeAll()

	// 4. Wait for the poll loop goroutine to fully exit. This is CRITICAL:
	//    it guarantees no new messages can enter consumer.Messages after
	//    this point. Without this wait, we would race with deliverMessage's
	//    blocking send — a message could be sent to the channel AFTER our
	//    drain loop finishes, orphaning it.
	<-state.done

	// 5. Remove from activeConsumers BEFORE draining. This prevents new
	//    ACK/NACK/Reject calls from racing with the cleanup. In-flight ACK
	//    calls that already loaded the state are handled safely by the
	//    deliveryIndex.LoadAndDelete guard in requeueInflightDelivery (and
	//    in the ACK/NACK/Reject handlers, which were changed to use
	//    LoadAndDelete for the same reason). Only one side wins the
	//    LoadAndDelete; the other sees !loaded and skips — no double
	//    decrement of the inflight counter, no double requeue.
	b.activeConsumers.Delete(consumerTag)

	// 6. Drain consumer.Messages — these are deliveries that were buffered
	//    in the channel but not yet pulled by the server's forwarder
	//    goroutine. Each delivery is requeued for redelivery to other
	//    consumers. The requeue preserves the original delivery tag, so the
	//    redelivered message carries the same tag (AMQP redelivery
	//    semantics). No semaphore permit is released: the cancelled
	//    consumer's semaphore is dead, and the requeued message will be
	//    claimed by another consumer which acquires its own permit.
	for {
		select {
		case delivery, ok := <-state.consumer.Messages:
			if !ok {
				// Channel closed (defensive — Messages is never closed
				// in current code, but guard against future changes).
				goto drainComplete
			}
			b.requeueInflightDelivery(queueState, state.queueName, delivery.DeliveryTag)
		default:
			goto drainComplete
		}
	}
drainComplete:

	// 7. Scan inflightOwners for remaining in-flight messages owned by this
	//    consumer. These are deliveries that already LEFT consumer.Messages
	//    (pulled by the server's forwarder goroutine, sitting in the fanIn
	//    channel buffer, or already sent to the client over TCP) but were
	//    never ACKed. Without this scan, they would be permanently
	//    orphaned. Messages that were already requeued by deliverMessage's
	//    stopCh/semCtx path or by the drain above are no longer in
	//    inflightOwners (DeleteInflight was called), so there is no
	//    double-requeue.
	//
	//    The scan is O(total inflight for this queue) — acceptable because
	//    consumer cancellation is a rare event.
	queueState.RangeInflightForConsumer(consumerTag, func(deliveryTag uint64) {
		b.requeueInflightDelivery(queueState, state.queueName, deliveryTag)
	})

	// 8. Unregister from AckCursor — safe now: all in-flight messages have
	//    been requeued, so minAckCursor recomputation won't skip them. If
	//    this were done before the requeue, minAckCursor would jump forward
	//    past the consumer's unacked tags, making the queue appear emptier
	//    than it actually is and potentially failing to apply backpressure.
	b.storage.UnregisterConsumerCursor(state.queueName, consumerTag)

	// 9. Remove from queueConsumers list (protected by per-queue mutex)
	mu := b.getQueueConsumersMutex(state.queueName)
	mu.Lock()
	defer mu.Unlock()
	if val, ok := b.queueConsumers.Load(state.queueName); ok {
		consumers := val.([]*ConsumerState)
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

	// 10. Delete from storage
	err := b.storage.DeleteConsumer(state.queueName, consumerTag)
	if err != nil && !errors.Is(err, interfaces.ErrConsumerNotFound) {
		return err
	}

	return nil
}

// requeueInflightDelivery atomically claims a delivery tag via
// deliveryIndex.LoadAndDelete and, if claimed, requeues the message for
// redelivery to other consumers. This is race-free: if an ACK/NACK/Reject
// handler already claimed the tag via its own LoadAndDelete, this is a
// no-op — preventing double-decrement of the inflight counter and
// double-requeue of the message.
func (b *StorageBroker) requeueInflightDelivery(qs *QueueState, queueName string, deliveryTag uint64) {
	if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
		return
	}
	qs.DeleteInflight(deliveryTag)
	qs.Requeue(deliveryTag)
	b.storage.DeletePendingAck(queueName, deliveryTag)
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
	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		if !queueState.WaitForCapacity(queueState.StopCh()) {
			return fmt.Errorf("queue '%s' closed during backpressure wait", queueName)
		}

		msgID := b.globalDeliveryTag.Add(1)

		var storeMsg *protocol.Message
		if i == 0 {
			// First (and often only) queue: store the original directly.
			// message is freshly allocated in processCompleteMessage — no aliasing.
			message.DeliveryTag = msgID
			storeMsg = message
		} else {
			// Fanout: each subsequent queue needs its own *Message with its own DeliveryTag.
			msgCopy := *message
			msgCopy.DeliveryTag = msgID
			storeMsg = &msgCopy
		}

		// Store to persistent storage
		err := b.storage.StoreMessage(queueName, storeMsg)
		if err != nil {
			return fmt.Errorf("failed to store message to queue '%s': %w", queueName, err)
		}

		// Advance the per-queue head cursor and wake parked consumers.
		// Replaces the old `available <- msgID` channel send.
		queueState.Publish(msgID)

		// Done - polling loops will discover it (NO consumer iteration needed)
	}

	return nil
}

// findTargetQueues determines which queues should receive the message
// CRITICAL FIX: Check in-memory cache FIRST before falling back to storage
func (b *StorageBroker) findTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message) ([]string, error) {
	// For default exchange, route directly to queue with same name as routing key
	if exchange.Name == "" {
		if _, ok := b.activeQueues.Load(routingKey); ok {
			return []string{routingKey}, nil
		}

		_, err := b.storage.GetQueue(routingKey)
		if err == nil {
			return []string{routingKey}, nil
		}
		return nil, nil
	}

	// Get all bindings for this exchange
	bindings, err := b.storage.GetExchangeBindings(exchange.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get exchange bindings: %w", err)
	}

	targetQueues := make([]string, 0, len(bindings))

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
	if pattern == routingKey || pattern == "#" {
		return true
	}

	pi := strings.IndexByte(pattern, '.')
	var pWord, pRest string
	if pi >= 0 {
		pWord, pRest = pattern[:pi], pattern[pi+1:]
	} else {
		pWord = pattern
	}

	if pWord == "#" {
		if b.matchTopicPattern(pRest, routingKey) {
			return true
		}
		if routingKey != "" {
			ki := strings.IndexByte(routingKey, '.')
			var kRest string
			if ki >= 0 {
				kRest = routingKey[ki+1:]
			}
			return b.matchTopicPattern(pattern, kRest)
		}
		return false
	}

	if routingKey == "" {
		return false
	}

	ki := strings.IndexByte(routingKey, '.')
	var kWord, kRest string
	if ki >= 0 {
		kWord, kRest = routingKey[:ki], routingKey[ki+1:]
	} else {
		kWord = routingKey
	}

	if pWord == "*" || pWord == kWord {
		return b.matchTopicPattern(pRest, kRest)
	}
	return false
}

func (b *StorageBroker) matchHeaders(bindingArgs map[string]interface{}, messageHeaders map[string]interface{}) bool {
	if len(bindingArgs) == 0 {
		return false
	}

	anyMatch := false
	headerCount := 0
	for key, value := range bindingArgs {
		if key == "x-match" {
			if s, ok := value.(string); ok && s == "any" {
				anyMatch = true
			}
			continue
		}
		headerCount++
	}

	if headerCount == 0 {
		return true
	}

	if messageHeaders == nil {
		return false
	}

	for key, value := range bindingArgs {
		if key == "x-match" {
			continue
		}
		msgValue, exists := messageHeaders[key]
		matched := exists && reflect.DeepEqual(msgValue, value)
		if anyMatch {
			if matched {
				return true
			}
		} else if !matched {
			return false
		}
	}
	return !anyMatch
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
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	if multiple {
		ackedCount := 0
		pendingAcks, err := b.storage.GetConsumerPendingAcks(consumerTag)
		if err == nil {
			for _, pendingAck := range pendingAcks {
				if pendingAck.DeliveryTag <= deliveryTag {
					// Atomic guard: claim the tag. If already processed
					// (by cleanup in UnregisterConsumer or a duplicate
					// multiple-ACK), skip to avoid double-decrementing
					// the inflight counter.
					if _, loaded := b.deliveryIndex.LoadAndDelete(pendingAck.DeliveryTag); !loaded {
						continue
					}
					b.storage.DeleteMessage(pendingAck.QueueName, pendingAck.DeliveryTag)
					b.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
					b.storage.AckFromConsumer(pendingAck.QueueName, consumerTag, pendingAck.DeliveryTag)
					queueState.DeleteInflight(pendingAck.DeliveryTag)
					queueState.AckAdvance(pendingAck.DeliveryTag)
					ackedCount++
				}
			}
		}
		queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))
		if state.prefetchSem != nil && ackedCount > 0 {
			state.prefetchSem.Release(int64(ackedCount))
		}
	} else {
		// Acknowledge single message
		// Atomic guard against duplicate ACK or concurrent cleanup in
		// UnregisterConsumer: LoadAndDelete ensures only one caller
		// processes this tag, preventing double-decrement of the inflight
		// counter.
		if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
			return nil
		}
		// Delete message and pending ack from storage
		b.storage.DeleteMessage(state.queueName, deliveryTag)
		b.storage.DeletePendingAck(state.queueName, deliveryTag)

		// Notify AckCursor that this consumer acked this tag
		b.storage.AckFromConsumer(state.queueName, state.consumer.Tag, deliveryTag)

		// Sync the true minAckCursor from storage's AckCursor (which tracks
		// per-consumer lowest unacked with O(1) lookup) into QueueState for
		// accurate backpressure depth calculation.
		queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))

		// Remove from inflight ownership and advance the depth frontier
		// (may jump minAckCursor to head if queue drained).
		queueState.DeleteInflight(deliveryTag)
		queueState.AckAdvance(deliveryTag)

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
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	// Atomic guard against duplicate reject or concurrent cleanup in
	// UnregisterConsumer: LoadAndDelete ensures only one caller processes
	// this tag, preventing double-decrement of the inflight counter.
	if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
		return nil
	}

	// Remove from inflight ownership
	queueState.DeleteInflight(deliveryTag)

	// Notify AckCursor (NACK removes from unacked set regardless of requeue)
	b.storage.NackFromConsumer(state.queueName, state.consumer.Tag, deliveryTag)

	// Reject always requeues or discards — no PendingAck dependency needed.
	// The message was stored in deliverMessage; here we either requeue it
	// for redelivery or delete it.
	if requeue {
		b.storage.DeletePendingAck(state.queueName, deliveryTag)
		queueState.Requeue(deliveryTag)
	} else {
		b.storage.DeletePendingAck(state.queueName, deliveryTag)
		b.storage.DeleteMessage(state.queueName, deliveryTag)
		queueState.AckAdvance(deliveryTag)
	}

	// Release semaphore permit (frees capacity for next message)
	if state.prefetchSem != nil {
		state.prefetchSem.Release(1)
	}

	return nil
}

// NacknowledgeMessage handles negative acknowledgment (lock-free hot path)
func (b *StorageBroker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	val, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	if multiple {
		nackedCount := 0
		pendingAcks, err := b.storage.GetConsumerPendingAcks(consumerTag)
		if err == nil {
			for _, pendingAck := range pendingAcks {
				if pendingAck.DeliveryTag <= deliveryTag {
					// Atomic guard: claim the tag. If already processed
					// (by cleanup in UnregisterConsumer or a duplicate
					// multiple-NACK), skip to avoid double-decrementing
					// the inflight counter.
					if _, loaded := b.deliveryIndex.LoadAndDelete(pendingAck.DeliveryTag); !loaded {
						continue
					}
					queueState.DeleteInflight(pendingAck.DeliveryTag)
					b.storage.NackFromConsumer(pendingAck.QueueName, consumerTag, pendingAck.DeliveryTag)

					if requeue {
						b.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
						queueState.Requeue(pendingAck.DeliveryTag)
					} else {
						b.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
						b.storage.DeleteMessage(pendingAck.QueueName, pendingAck.DeliveryTag)
						queueState.AckAdvance(pendingAck.DeliveryTag)
					}
					nackedCount++
				}
			}
		}
		queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))
		if state.prefetchSem != nil && nackedCount > 0 {
			state.prefetchSem.Release(int64(nackedCount))
		}
	} else {
		// Nacknowledge single message
		// Atomic guard against duplicate nack or concurrent cleanup in
		// UnregisterConsumer: LoadAndDelete ensures only one caller
		// processes this tag, preventing double-decrement of the inflight
		// counter.
		if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
			return nil
		}
		// Remove from inflight ownership
		queueState.DeleteInflight(deliveryTag)

		// Notify AckCursor (NACK removes from unacked set)
		b.storage.NackFromConsumer(state.queueName, state.consumer.Tag, deliveryTag)

		if requeue {
			b.storage.DeletePendingAck(state.queueName, deliveryTag)
			queueState.Requeue(deliveryTag)
		} else {
			b.storage.DeletePendingAck(state.queueName, deliveryTag)
			b.storage.DeleteMessage(state.queueName, deliveryTag)
			queueState.AckAdvance(deliveryTag)
		}

		// Release semaphore permit (frees capacity for next message)
		if state.prefetchSem != nil {
			state.prefetchSem.Release(1)
		}
	}

	return nil
}

// GetConsumerForDelivery returns the consumer tag for a given delivery tag
// This provides O(1) lookup for ACK routing using globally unique delivery tags.
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

// AdvanceDeliveryTag advances the global delivery tag counter past the given value.
// Called during recovery to ensure new delivery tags don't collide with recovered ones.
func (b *StorageBroker) AdvanceDeliveryTag(tag uint64) {
	for {
		current := b.globalDeliveryTag.Load()
		if tag <= current {
			return
		}
		if b.globalDeliveryTag.CompareAndSwap(current, tag) {
			return
		}
	}
}

// RecoverQueue initializes a queue's dispatch cursor from the recovered
// message tag range, making [minTag, maxTag] immediately claimable by
// consumers without per-message enqueueing.
//
//   - minTag: lowest delivery tag among recovered messages for this queue.
//     tail starts here so consumers claim recovered messages first.
//   - maxTag: highest delivery tag among recovered messages for this queue.
//     head starts at maxTag+1 so the range is claimable without parking.
//
// Called by the recovery manager after loading recovered messages into the
// ring/WAL (via LoadMessageFromRecovery), BEFORE any consumer is registered
// or any new Publish. Gaps inside [minTag, maxTag] (tags acked before
// restart) are skipped at delivery time via the GetMessage-not-found path
// in deliverMessage.
//
// If the queue has no recovered messages, callers should pass (0, 0) — tail
// and head stay at 0 and consumers park until the first Publish.
func (b *StorageBroker) RecoverQueue(queueName string, minTag, maxTag uint64) {
	queueState := b.getOrCreateQueueState(queueName)
	queueState.Recover(minTag, maxTag)
}
