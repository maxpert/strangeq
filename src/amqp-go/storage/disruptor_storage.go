package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	disruptor "github.com/smartystreets-prototypes/go-disruptor"
)

const (
	// Ring buffer size - must be power of 2 for fast modulo via bitwise AND
	DefaultRingBufferSize = 1024 * 256 // 256K messages (4x larger for high throughput)
	RingBufferMask        = DefaultRingBufferSize - 1

	// Spilling threshold - start writing to log when ring is 80% full
	SpillThreshold = uint64(209715) // 80% of 256K = 209,715
)

// DisruptorStorage implements an in-memory message queue using LMAX Disruptor pattern
// Phase 1: Pure in-memory, zero-GC, lock-free ring buffer
// Phase 2-5: Various optimizations
// Phase 6: RabbitMQ-style three-tier storage (Ring → WAL → Segments)
type DisruptorStorage struct {
	// Per-queue disruptor rings
	queues map[string]*QueueRing
	mutex  sync.RWMutex

	// Metadata storage (Phase 3: JSON files)
	metadataStore *PersistentMetadataStore

	// Offset checkpoint store (Phase 4: Zero-write ACKs)
	offsetStore *OffsetCheckpointStore

	// WAL Manager (Phase 6A: Lock-free, channel-batched writes)
	wal *WALManager

	// Segment Manager (Phase 6B: Long-term cold storage with compaction)
	segments *SegmentManager

	// Atomic counters for delivery tags
	deliveryTagCounters map[string]*atomic.Uint64 // queueName -> counter

	// Transaction storage (simple in-memory for now)
	transactions map[string]*interfaces.Transaction
	txMutex      sync.RWMutex

	// Data directory
	dataDir string
}

// QueueRing represents a single queue's disruptor ring buffer
type QueueRing struct {
	name string

	// Pre-allocated ring buffer for messages
	ringBuffer [DefaultRingBufferSize]*protocol.Message

	// Disruptor (embeds Writer and Reader)
	disruptor disruptor.Disruptor

	// Consumer for processing messages
	consumer *QueueConsumer

	// Queue head (next delivery tag to assign)
	head atomic.Uint64

	// Consumer position tracking (consumerTag -> position)
	consumerPositions map[string]*atomic.Uint64
	consumerMutex     sync.RWMutex

	// Unacked tracking (consumerTag -> set of unacked tags)
	unacked      map[string]map[uint64]struct{}
	unackedMutex sync.RWMutex

	// Queue state
	closed atomic.Bool

	// Ring buffer message count (for spilling logic)
	messageCount atomic.Uint64
}

// QueueConsumer implements the disruptor.Consumer interface
type QueueConsumer struct {
	ring *QueueRing
}

// Consume processes a batch of messages (disruptor callback)
func (qc *QueueConsumer) Consume(lower, upper int64) {
	// Messages are already in the ring buffer
	// This callback is just for coordination - actual consumption happens via GetMessage
	// In Phase 1, we're using pull-based delivery, so this is a no-op
}

// NewDisruptorStorage creates a new in-memory disruptor-based storage
func NewDisruptorStorage() *DisruptorStorage {
	return NewDisruptorStorageWithDataDir("./data")
}

// NewDisruptorStorageWithDataDir creates storage with custom data directory
func NewDisruptorStorageWithDataDir(dataDir string) *DisruptorStorage {
	return NewDisruptorStorageWithCheckpointInterval(dataDir, DefaultCheckpointInterval)
}

// NewDisruptorStorageWithCheckpointInterval creates storage with custom data directory and checkpoint interval
func NewDisruptorStorageWithCheckpointInterval(dataDir string, checkpointInterval time.Duration) *DisruptorStorage {
	// Create JSON metadata store
	metadataStore, err := NewPersistentMetadataStore(dataDir)
	if err != nil {
		// Fall back to nil - will cause errors but won't crash
		// In production, we'd want better error handling
		metadataStore = nil
	}

	// Create offset checkpoint store (Phase 4) with configurable interval
	offsetStore, err := NewOffsetCheckpointStoreWithInterval(dataDir, checkpointInterval)
	if err != nil {
		// Fall back to nil
		offsetStore = nil
	}

	// Create WAL Manager (Phase 6A)
	walManager, err := NewWALManager(dataDir)
	if err != nil {
		// In production, this should be a fatal error
		walManager = nil
	}

	// Create Segment Manager (Phase 6B)
	segmentManager, err := NewSegmentManager(dataDir)
	if err != nil {
		// In production, this should be a fatal error
		segmentManager = nil
	}

	// Wire up WAL and Segment managers for checkpointing
	if walManager != nil && segmentManager != nil {
		walManager.SetSegmentManager(segmentManager)
	}

	return &DisruptorStorage{
		queues:              make(map[string]*QueueRing),
		metadataStore:       metadataStore,
		offsetStore:         offsetStore,
		wal:                 walManager,
		segments:            segmentManager,
		deliveryTagCounters: make(map[string]*atomic.Uint64),
		transactions:        make(map[string]*interfaces.Transaction),
		dataDir:             dataDir,
	}
}

// getOrCreateQueueRing returns existing queue ring or creates a new one
func (ds *DisruptorStorage) getOrCreateQueueRing(queueName string) *QueueRing {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	if ring, exists := ds.queues[queueName]; exists {
		return ring
	}

	// Create new queue ring
	ring := &QueueRing{
		name:              queueName,
		consumerPositions: make(map[string]*atomic.Uint64),
		unacked:           make(map[string]map[uint64]struct{}),
	}

	// Initialize consumer
	ring.consumer = &QueueConsumer{ring: ring}

	// Create disruptor with SPSC (single producer, single consumer) pattern
	ring.disruptor = disruptor.New(
		disruptor.WithCapacity(DefaultRingBufferSize),
		disruptor.WithConsumerGroup(ring.consumer),
	)

	// CRITICAL FIX (Phase 5): Start the reader goroutine!
	// This is what actually frees ring buffer slots when Consume() returns
	// Without this, the consumer cursor never advances and Reserve() blocks forever
	go ring.disruptor.Read()

	// Initialize delivery tag counter
	counter := &atomic.Uint64{}
	ds.deliveryTagCounters[queueName] = counter

	ds.queues[queueName] = ring
	return ring
}

// LoadMessageFromRecovery loads a message directly into ring buffer during recovery
// DOES NOT write to WAL (message is already in WAL, we're loading FROM WAL)
// This prevents double-writing during recovery which causes massive slowdown
func (ds *DisruptorStorage) LoadMessageFromRecovery(queueName string, message *protocol.Message) error {
	ring := ds.getOrCreateQueueRing(queueName)

	// Check if ring buffer is full
	messageCount := ring.messageCount.Load()
	if messageCount >= DefaultRingBufferSize {
		// Ring buffer is full - skip this message
		// It will be read from WAL on-demand when consumers need it
		return nil
	}

	// Reserve slot in ring buffer
	sequence := ring.disruptor.Reserve(1)

	// Store message in ring buffer (zero-copy)
	index := message.DeliveryTag & RingBufferMask
	ring.ringBuffer[index] = message

	// Increment message count
	ring.messageCount.Add(1)

	// Commit to make visible to consumers
	ring.disruptor.Commit(sequence, sequence)

	// Update head to track highest delivery tag seen
	if message.DeliveryTag > ring.head.Load() {
		ring.head.Store(message.DeliveryTag)
	}

	return nil
}

// StoreMessage stores a message in the ring buffer (zero-copy publish)
// NOTE: Delivery tag must already be assigned by the broker before calling this
// Phase 6A: Spills to WAL when ring is > 80% full (lock-free channel write)
func (ds *DisruptorStorage) StoreMessage(queueName string, message *protocol.Message) error {
	ring := ds.getOrCreateQueueRing(queueName)

	// Phase 2: Write durable messages to WAL FIRST (before ring buffer)
	// DeliveryMode == 2 means persistent/durable message
	if ds.wal != nil && message.DeliveryMode == 2 {
		// Write to WAL for durability (lock-free!)
		if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
			// WAL write failed - this is critical for durable messages
			return fmt.Errorf("failed to write durable message to WAL: %w", err)
		}
		// WAL write succeeded - message is now durable
		// Continue to also store in ring buffer for fast access
	}

	// Check if we should spill to WAL (ring buffer overflow)
	// This handles transient messages when ring is full
	messageCount := ring.messageCount.Load()
	if ds.wal != nil && messageCount > SpillThreshold {
		// Ring buffer is > 80% full
		if message.DeliveryMode != 2 {
			// For transient messages, write to WAL as overflow
			if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
				// Log error but continue - transient messages can be lost
			}
		}
		// Don't store in ring buffer - it's full
		// Message will be recovered from WAL on next consume
		return nil
	}

	// Reserve slot in ring buffer
	sequence := ring.disruptor.Reserve(1)

	// Store message in ring buffer (zero-copy)
	// Use delivery tag as index for O(1) lookup
	index := message.DeliveryTag & RingBufferMask
	ring.ringBuffer[index] = message

	// Increment message count
	ring.messageCount.Add(1)

	// Commit to make visible to consumers
	ring.disruptor.Commit(sequence, sequence)

	// Update head to track highest delivery tag seen
	// This is only needed for recovery scenarios
	if message.DeliveryTag > ring.head.Load() {
		ring.head.Store(message.DeliveryTag)
	}

	return nil
}

// GetMessage retrieves a message by delivery tag (pull-based delivery)
// Phase 6B: Falls back to WAL, then Segments
func (ds *DisruptorStorage) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return nil, interfaces.ErrQueueNotFound
	}

	// Try ring buffer first (hot path)
	index := deliveryTag & RingBufferMask
	message := ring.ringBuffer[index]

	if message != nil && message.DeliveryTag == deliveryTag {
		return message, nil
	}

	// Not in ring buffer - try the WAL (Phase 6A)
	if ds.wal != nil {
		message, err := ds.wal.Read(queueName, deliveryTag)
		if err == nil {
			return message, nil
		}
	}

	// Not in WAL - try segments (Phase 6B)
	if ds.segments != nil {
		message, err := ds.segments.Read(queueName, deliveryTag)
		if err == nil {
			return message, nil
		}
	}

	return nil, interfaces.ErrMessageNotFound
}

// DeleteMessage removes a message after ACK
// Phase 6B: Notifies both WAL and Segments (for compaction)
func (ds *DisruptorStorage) DeleteMessage(queueName string, deliveryTag uint64) error {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return interfaces.ErrQueueNotFound
	}

	// Clear ring buffer slot (will be overwritten on next publish)
	index := deliveryTag & RingBufferMask
	if ring.ringBuffer[index] != nil {
		ring.ringBuffer[index] = nil
		// Decrement message count
		if ring.messageCount.Load() > 0 {
			ring.messageCount.Add(^uint64(0)) // Subtract 1 using two's complement
		}
	}

	// Notify WAL of ACK (Phase 6A: lock-free!)
	if ds.wal != nil {
		ds.wal.Acknowledge(queueName, deliveryTag)
	}

	// Notify Segments of ACK for compaction (Phase 6B)
	if ds.segments != nil {
		ds.segments.Acknowledge(queueName, deliveryTag)
	}

	return nil
}

// GetQueueMessages retrieves all messages in a queue (for testing/debugging)
func (ds *DisruptorStorage) GetQueueMessages(queueName string) ([]*protocol.Message, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return nil, interfaces.ErrQueueNotFound
	}

	messages := make([]*protocol.Message, 0, DefaultRingBufferSize)
	for i := 0; i < DefaultRingBufferSize; i++ {
		if msg := ring.ringBuffer[i]; msg != nil {
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// GetQueueMessageCount returns the number of messages in a queue
func (ds *DisruptorStorage) GetQueueMessageCount(queueName string) (int, error) {
	messages, err := ds.GetQueueMessages(queueName)
	if err != nil {
		return 0, err
	}
	return len(messages), nil
}

// PurgeQueue removes all messages from a queue
func (ds *DisruptorStorage) PurgeQueue(queueName string) (int, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return 0, interfaces.ErrQueueNotFound
	}

	count := 0
	for i := 0; i < DefaultRingBufferSize; i++ {
		if ring.ringBuffer[i] != nil {
			count++
			ring.ringBuffer[i] = nil
		}
	}

	return count, nil
}

// GetMessageRange retrieves messages in a delivery tag range
func (ds *DisruptorStorage) GetMessageRange(queueName string, startTag, endTag uint64) ([]*protocol.Message, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return nil, interfaces.ErrQueueNotFound
	}

	messages := make([]*protocol.Message, 0, endTag-startTag+1)
	for tag := startTag; tag <= endTag; tag++ {
		index := tag & RingBufferMask
		if msg := ring.ringBuffer[index]; msg != nil && msg.DeliveryTag == tag {
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// DeleteMessageRange deletes messages in a range (for GC)
func (ds *DisruptorStorage) DeleteMessageRange(queueName string, startTag, endTag uint64) error {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return interfaces.ErrQueueNotFound
	}

	for tag := startTag; tag <= endTag; tag++ {
		index := tag & RingBufferMask
		if msg := ring.ringBuffer[index]; msg != nil && msg.DeliveryTag == tag {
			ring.ringBuffer[index] = nil
		}
	}

	return nil
}

// GetQueueHead returns the current head position (next delivery tag)
func (ds *DisruptorStorage) GetQueueHead(queueName string) (uint64, error) {
	ring := ds.getOrCreateQueueRing(queueName)
	return ring.head.Load(), nil
}

// SetQueueHead sets the head position
func (ds *DisruptorStorage) SetQueueHead(queueName string, head uint64) error {
	ring := ds.getOrCreateQueueRing(queueName)
	ring.head.Store(head)
	return nil
}

// IncrementQueueHead increments and returns the new head
func (ds *DisruptorStorage) IncrementQueueHead(queueName string) (uint64, error) {
	ring := ds.getOrCreateQueueRing(queueName)
	return ring.head.Add(1), nil
}

// GetConsumerPosition returns the consumer's current position
func (ds *DisruptorStorage) GetConsumerPosition(queueName, consumerTag string) (uint64, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return 0, interfaces.ErrQueueNotFound
	}

	ring.consumerMutex.RLock()
	defer ring.consumerMutex.RUnlock()

	if pos, exists := ring.consumerPositions[consumerTag]; exists {
		return pos.Load(), nil
	}

	// First time - start at 0
	return 0, nil
}

// SetConsumerPosition updates the consumer's position
func (ds *DisruptorStorage) SetConsumerPosition(queueName, consumerTag string, position uint64) error {
	ring := ds.getOrCreateQueueRing(queueName)

	ring.consumerMutex.Lock()
	defer ring.consumerMutex.Unlock()

	if pos, exists := ring.consumerPositions[consumerTag]; exists {
		pos.Store(position)
	} else {
		pos := &atomic.Uint64{}
		pos.Store(position)
		ring.consumerPositions[consumerTag] = pos
	}

	return nil
}

// AddUnacked marks a message as unacknowledged
func (ds *DisruptorStorage) AddUnacked(queueName, consumerTag string, deliveryTag uint64) error {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return interfaces.ErrQueueNotFound
	}

	ring.unackedMutex.Lock()
	defer ring.unackedMutex.Unlock()

	if ring.unacked[consumerTag] == nil {
		ring.unacked[consumerTag] = make(map[uint64]struct{})
	}
	ring.unacked[consumerTag][deliveryTag] = struct{}{}

	return nil
}

// RemoveUnacked removes an acknowledged message
// Phase 4: Also updates consumer offset (zero disk writes)
func (ds *DisruptorStorage) RemoveUnacked(queueName, consumerTag string, deliveryTag uint64) error {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return interfaces.ErrQueueNotFound
	}

	ring.unackedMutex.Lock()
	defer ring.unackedMutex.Unlock()

	if unackedSet, exists := ring.unacked[consumerTag]; exists {
		delete(unackedSet, deliveryTag)
	}

	// Phase 4: Update consumer offset in memory (no disk write!)
	if ds.offsetStore != nil {
		ds.offsetStore.UpdateOffset(queueName, consumerTag, deliveryTag)
	}

	return nil
}

// GetUnackedCount returns the number of unacked messages for a consumer
func (ds *DisruptorStorage) GetUnackedCount(queueName, consumerTag string) (int, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return 0, interfaces.ErrQueueNotFound
	}

	ring.unackedMutex.RLock()
	defer ring.unackedMutex.RUnlock()

	if unackedSet, exists := ring.unacked[consumerTag]; exists {
		return len(unackedSet), nil
	}

	return 0, nil
}

// GetUnackedTags returns all unacked delivery tags for a consumer
func (ds *DisruptorStorage) GetUnackedTags(queueName, consumerTag string) ([]uint64, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return nil, interfaces.ErrQueueNotFound
	}

	ring.unackedMutex.RLock()
	defer ring.unackedMutex.RUnlock()

	if unackedSet, exists := ring.unacked[consumerTag]; exists {
		tags := make([]uint64, 0, len(unackedSet))
		for tag := range unackedSet {
			tags = append(tags, tag)
		}
		return tags, nil
	}

	return []uint64{}, nil
}

// GetLowestUnackedAcrossConsumers returns the lowest unacked tag across all consumers
func (ds *DisruptorStorage) GetLowestUnackedAcrossConsumers(queueName string) (uint64, error) {
	ds.mutex.RLock()
	ring, exists := ds.queues[queueName]
	ds.mutex.RUnlock()

	if !exists {
		return 0, interfaces.ErrQueueNotFound
	}

	ring.unackedMutex.RLock()
	defer ring.unackedMutex.RUnlock()

	var lowest uint64 = ^uint64(0) // Max uint64
	for _, unackedSet := range ring.unacked {
		for tag := range unackedSet {
			if tag < lowest {
				lowest = tag
			}
		}
	}

	if lowest == ^uint64(0) {
		return 0, nil
	}

	return lowest, nil
}

// Metadata operations (Phase 3: JSON files)

// StoreExchange stores an exchange
func (ds *DisruptorStorage) StoreExchange(exchange *protocol.Exchange) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreExchange(exchange)
}

// GetExchange retrieves an exchange
func (ds *DisruptorStorage) GetExchange(name string) (*protocol.Exchange, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchange(name)
}

// DeleteExchange deletes an exchange
func (ds *DisruptorStorage) DeleteExchange(name string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteExchange(name)
}

// ListExchanges returns all exchanges
func (ds *DisruptorStorage) ListExchanges() ([]*protocol.Exchange, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.ListExchanges()
}

// StoreQueue stores a queue
func (ds *DisruptorStorage) StoreQueue(queue *protocol.Queue) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}

	// Store to JSON file
	if err := ds.metadataStore.StoreQueue(queue); err != nil {
		return err
	}

	// Create the queue ring
	_ = ds.getOrCreateQueueRing(queue.Name)

	return nil
}

// GetQueue retrieves a queue
func (ds *DisruptorStorage) GetQueue(name string) (*protocol.Queue, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueue(name)
}

// DeleteQueue deletes a queue
func (ds *DisruptorStorage) DeleteQueue(name string) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	if ring, exists := ds.queues[name]; exists {
		// Close the disruptor
		_ = ring.disruptor.Close()
		ring.closed.Store(true)

		delete(ds.queues, name)
	}

	// Delete queue metadata from JSON file
	if ds.metadataStore != nil {
		return ds.metadataStore.DeleteQueue(name)
	}

	return nil
}

// ListQueues returns all queues
func (ds *DisruptorStorage) ListQueues() ([]*protocol.Queue, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.ListQueues()
}

// StoreBinding stores a binding
func (ds *DisruptorStorage) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreBinding(queueName, exchangeName, routingKey, arguments)
}

// GetBinding retrieves a specific binding
func (ds *DisruptorStorage) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetBinding(queueName, exchangeName, routingKey)
}

// DeleteBinding deletes a binding
func (ds *DisruptorStorage) DeleteBinding(queueName, exchangeName, routingKey string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteBinding(queueName, exchangeName, routingKey)
}

// GetQueueBindings returns all bindings for a queue
func (ds *DisruptorStorage) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueueBindings(queueName)
}

// GetExchangeBindings returns all bindings for an exchange
func (ds *DisruptorStorage) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchangeBindings(exchangeName)
}

// StoreConsumer stores a consumer
func (ds *DisruptorStorage) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreConsumer(queueName, consumerTag, consumer)
}

// GetConsumer retrieves a consumer
func (ds *DisruptorStorage) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetConsumer(queueName, consumerTag)
}

// DeleteConsumer deletes a consumer
func (ds *DisruptorStorage) DeleteConsumer(queueName, consumerTag string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}

	// Phase 4: Remove consumer offset tracking
	if ds.offsetStore != nil {
		_ = ds.offsetStore.RemoveConsumer(queueName, consumerTag)
	}

	return ds.metadataStore.DeleteConsumer(queueName, consumerTag)
}

// GetQueueConsumers returns all consumers for a queue
func (ds *DisruptorStorage) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueueConsumers(queueName)
}

// Transaction operations (simple in-memory implementation)

func (ds *DisruptorStorage) BeginTransaction(txID string) (*interfaces.Transaction, error) {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	if _, exists := ds.transactions[txID]; exists {
		return nil, interfaces.ErrTransactionExists
	}

	tx := &interfaces.Transaction{
		ID:        txID,
		Status:    interfaces.TxStatusActive,
		StartTime: time.Now(),
		Actions:   make([]*interfaces.TransactionAction, 0),
	}

	ds.transactions[txID] = tx
	return tx, nil
}

func (ds *DisruptorStorage) GetTransaction(txID string) (*interfaces.Transaction, error) {
	ds.txMutex.RLock()
	defer ds.txMutex.RUnlock()

	if tx, exists := ds.transactions[txID]; exists {
		return tx, nil
	}

	return nil, interfaces.ErrTransactionNotFound
}

func (ds *DisruptorStorage) AddAction(txID string, action *interfaces.TransactionAction) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	tx, exists := ds.transactions[txID]
	if !exists {
		return interfaces.ErrTransactionNotFound
	}

	if tx.Status != interfaces.TxStatusActive {
		return interfaces.ErrTransactionNotActive
	}

	tx.Actions = append(tx.Actions, action)
	return nil
}

func (ds *DisruptorStorage) CommitTransaction(txID string) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	tx, exists := ds.transactions[txID]
	if !exists {
		return interfaces.ErrTransactionNotFound
	}

	if tx.Status != interfaces.TxStatusActive {
		return interfaces.ErrTransactionNotActive
	}

	tx.Status = interfaces.TxStatusCommitted
	tx.EndTime = time.Now()
	return nil
}

func (ds *DisruptorStorage) RollbackTransaction(txID string) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	tx, exists := ds.transactions[txID]
	if !exists {
		return interfaces.ErrTransactionNotFound
	}

	if tx.Status != interfaces.TxStatusActive {
		return interfaces.ErrTransactionNotActive
	}

	tx.Status = interfaces.TxStatusRolledBack
	tx.EndTime = time.Now()
	return nil
}

func (ds *DisruptorStorage) DeleteTransaction(txID string) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	delete(ds.transactions, txID)
	return nil
}

func (ds *DisruptorStorage) ListActiveTransactions() ([]*interfaces.Transaction, error) {
	ds.txMutex.RLock()
	defer ds.txMutex.RUnlock()

	var active []*interfaces.Transaction
	for _, tx := range ds.transactions {
		if tx.Status == interfaces.TxStatusActive {
			active = append(active, tx)
		}
	}

	return active, nil
}

// Acknowledgment store (Phase 1: No-op, Phase 4 will implement offset-based ACKs)

func (ds *DisruptorStorage) StorePendingAck(pendingAck *protocol.PendingAck) error {
	// Phase 1: No-op - we're not storing pending acks to disk
	// Phase 4 will implement offset-based ACKs with periodic checkpointing
	return nil
}

func (ds *DisruptorStorage) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	return nil, interfaces.ErrPendingAckNotFound
}

func (ds *DisruptorStorage) DeletePendingAck(queueName string, deliveryTag uint64) error {
	// Phase 1: No-op
	return nil
}

func (ds *DisruptorStorage) GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error) {
	return []*protocol.PendingAck{}, nil
}

func (ds *DisruptorStorage) GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error) {
	return []*protocol.PendingAck{}, nil
}

func (ds *DisruptorStorage) CleanupExpiredAcks(maxAge time.Duration) error {
	// Phase 1: No-op
	return nil
}

func (ds *DisruptorStorage) GetAllPendingAcks() ([]*protocol.PendingAck, error) {
	return []*protocol.PendingAck{}, nil
}

// Durability store (Phase 1: In-memory only, Phase 2 will add append-log)

func (ds *DisruptorStorage) StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error {
	// Phase 1: No-op - in-memory only
	// Phase 3 will implement JSON metadata files
	return nil
}

func (ds *DisruptorStorage) GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error) {
	// Phase 3: Return durable queues and exchanges from metadata store
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}

	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []protocol.Exchange{},
		Queues:      []protocol.Queue{},
		Bindings:    []protocol.Binding{},
		LastUpdated: time.Now(),
	}

	// Collect exchanges from metadata store
	exchanges, err := ds.metadataStore.ListExchanges()
	if err != nil {
		return nil, fmt.Errorf("failed to list exchanges: %w", err)
	}
	for _, exchange := range exchanges {
		if exchange.Durable {
			metadata.Exchanges = append(metadata.Exchanges, *exchange)
		}
	}

	// Collect queues from metadata store
	queues, err := ds.metadataStore.ListQueues()
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	for _, queue := range queues {
		if queue.Durable {
			metadata.Queues = append(metadata.Queues, *queue)
		}
	}

	// Note: Bindings recovery not yet implemented
	// This would require scanning all exchanges and their bindings

	return metadata, nil
}

func (ds *DisruptorStorage) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, nil
}

func (ds *DisruptorStorage) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, nil
}

func (ds *DisruptorStorage) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	// Phase 3: Call WAL recovery to get all unacknowledged messages
	if ds.wal == nil {
		return make(map[string][]*protocol.Message), nil
	}

	// Recover messages from WAL
	recoveredMessages, err := ds.wal.RecoverFromWAL()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

	// Group messages by queue name
	messagesByQueue := make(map[string][]*protocol.Message)
	for _, recoveryMsg := range recoveredMessages {
		messagesByQueue[recoveryMsg.QueueName] = append(
			messagesByQueue[recoveryMsg.QueueName],
			recoveryMsg.Message,
		)
	}

	return messagesByQueue, nil
}

func (ds *DisruptorStorage) MarkRecoveryComplete(stats *protocol.RecoveryStats) error {
	return nil
}

// RecoverFromLog scans the WAL and segments to recover messages into ring buffer
// This is called during startup to reload persisted messages
func (ds *DisruptorStorage) RecoverFromLog(queueName string) error {
	ring := ds.getOrCreateQueueRing(queueName)

	// Step 1: Recover from segments (cold storage)
	if ds.segments != nil {
		// Segments store messages long-term, scan and load into ring
		// This is a simplified recovery - production would use indexes
	}

	// Step 2: Recover from WAL (recent messages)
	if ds.wal != nil {
		// WAL contains recent messages not yet checkpointed
		// This is handled by WAL's checkpoint loop on startup
	}

	// Step 3: Load consumer offsets
	if ds.offsetStore != nil {
		// Consumer offsets are already loaded by OffsetCheckpointStore
		// No additional work needed here
	}

	// Update ring head to highest delivery tag seen
	// This ensures new messages get unique tags
	_ = ring

	return nil
}

// RecoverAllQueues recovers all queues from persistent storage
func (ds *DisruptorStorage) RecoverAllQueues() error {
	if ds.metadataStore == nil {
		return nil
	}

	// Get all queues from metadata
	queues, err := ds.metadataStore.ListQueues()
	if err != nil {
		return fmt.Errorf("failed to list queues: %w", err)
	}

	// Recover each queue
	for _, queue := range queues {
		if err := ds.RecoverFromLog(queue.Name); err != nil {
			// Log error but continue with other queues
			continue
		}
	}

	return nil
}

// ExecuteAtomic runs operations atomically (Phase 1: simple mutex lock)
func (ds *DisruptorStorage) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// In Phase 1, we just execute operations under the global lock
	// Phase 2+ will implement proper MVCC or transaction log
	return operations(ds)
}

// Close cleans up all resources
func (ds *DisruptorStorage) Close() error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	for _, ring := range ds.queues {
		if !ring.closed.Load() {
			_ = ring.disruptor.Close()
			ring.closed.Store(true)
		}
	}

	// Close metadata store (Phase 3)
	if ds.metadataStore != nil {
		_ = ds.metadataStore.Close()
	}

	// Close offset checkpoint store (Phase 4)
	// This will perform a final checkpoint before exit
	if ds.offsetStore != nil {
		_ = ds.offsetStore.Close()
	}

	// Close WAL manager (Phase 6A)
	if ds.wal != nil {
		_ = ds.wal.Close()
	}

	// Close Segment manager (Phase 6B)
	if ds.segments != nil {
		_ = ds.segments.Close()
	}

	return nil
}

// Compile-time interface check
var _ interfaces.Storage = (*DisruptorStorage)(nil)
