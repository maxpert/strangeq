package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// BadgerTransactionWrapper provides atomic operations across multiple Badger stores
type BadgerTransactionWrapper struct {
	messageStore    *BadgerMessageStore
	metadataStore   *BadgerMetadataStore
	ackStore        *BadgerAckStore
	durabilityStore *BadgerDurabilityStore
	mutex          sync.RWMutex
}

// NewBadgerTransactionWrapper creates a new transaction wrapper
func NewBadgerTransactionWrapper(
	messageStore *BadgerMessageStore,
	metadataStore *BadgerMetadataStore,
	ackStore *BadgerAckStore,
	durabilityStore *BadgerDurabilityStore,
) *BadgerTransactionWrapper {
	return &BadgerTransactionWrapper{
		messageStore:    messageStore,
		metadataStore:   metadataStore,
		ackStore:        ackStore,
		durabilityStore: durabilityStore,
	}
}

// ExecuteAtomic runs operations atomically across multiple Badger stores
// NOTE: This is a simplified approach. For true ACID properties across multiple Badger databases,
// you would need a more sophisticated two-phase commit protocol.
func (w *BadgerTransactionWrapper) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Create a transaction storage wrapper that uses transactions for all operations
	txnStorage := &BadgerTransactionStorage{
		wrapper: w,
	}

	// Execute the operations
	return operations(txnStorage)
}

// BadgerTransactionStorage wraps operations in transactions
type BadgerTransactionStorage struct {
	wrapper *BadgerTransactionWrapper
}

// Implement interfaces.Storage interface with transaction support

// MessageStore methods
func (t *BadgerTransactionStorage) StoreMessage(queueName string, message *protocol.Message) error {
	if t.wrapper.messageStore == nil {
		return fmt.Errorf("message store not available")
	}
	return t.wrapper.messageStore.StoreMessage(queueName, message)
}

func (t *BadgerTransactionStorage) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	if t.wrapper.messageStore == nil {
		return nil, fmt.Errorf("message store not available")
	}
	return t.wrapper.messageStore.GetMessage(queueName, deliveryTag)
}

func (t *BadgerTransactionStorage) DeleteMessage(queueName string, deliveryTag uint64) error {
	if t.wrapper.messageStore == nil {
		return fmt.Errorf("message store not available")
	}
	return t.wrapper.messageStore.DeleteMessage(queueName, deliveryTag)
}

func (t *BadgerTransactionStorage) GetQueueMessages(queueName string) ([]*protocol.Message, error) {
	if t.wrapper.messageStore == nil {
		return nil, fmt.Errorf("message store not available")
	}
	return t.wrapper.messageStore.GetQueueMessages(queueName)
}

func (t *BadgerTransactionStorage) GetQueueMessageCount(queueName string) (int, error) {
	if t.wrapper.messageStore == nil {
		return 0, fmt.Errorf("message store not available")
	}
	return t.wrapper.messageStore.GetQueueMessageCount(queueName)
}

func (t *BadgerTransactionStorage) PurgeQueue(queueName string) (int, error) {
	if t.wrapper.messageStore == nil {
		return 0, fmt.Errorf("message store not available")
	}
	return t.wrapper.messageStore.PurgeQueue(queueName)
}

// MetadataStore methods - delegate to the underlying metadata store
func (t *BadgerTransactionStorage) StoreExchange(exchange *protocol.Exchange) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.StoreExchange(exchange)
}

func (t *BadgerTransactionStorage) GetExchange(name string) (*protocol.Exchange, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetExchange(name)
}

func (t *BadgerTransactionStorage) DeleteExchange(name string) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.DeleteExchange(name)
}

func (t *BadgerTransactionStorage) ListExchanges() ([]*protocol.Exchange, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.ListExchanges()
}

func (t *BadgerTransactionStorage) StoreQueue(queue *protocol.Queue) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.StoreQueue(queue)
}

func (t *BadgerTransactionStorage) GetQueue(name string) (*protocol.Queue, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetQueue(name)
}

func (t *BadgerTransactionStorage) DeleteQueue(name string) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.DeleteQueue(name)
}

func (t *BadgerTransactionStorage) ListQueues() ([]*protocol.Queue, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.ListQueues()
}

func (t *BadgerTransactionStorage) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.StoreBinding(queueName, exchangeName, routingKey, arguments)
}

func (t *BadgerTransactionStorage) DeleteBinding(queueName, exchangeName, routingKey string) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.DeleteBinding(queueName, exchangeName, routingKey)
}

func (t *BadgerTransactionStorage) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetExchangeBindings(exchangeName)
}

func (t *BadgerTransactionStorage) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetBinding(queueName, exchangeName, routingKey)
}

func (t *BadgerTransactionStorage) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetQueueBindings(queueName)
}

func (t *BadgerTransactionStorage) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.StoreConsumer(queueName, consumerTag, consumer)
}

func (t *BadgerTransactionStorage) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetConsumer(queueName, consumerTag)
}

func (t *BadgerTransactionStorage) DeleteConsumer(queueName, consumerTag string) error {
	if t.wrapper.metadataStore == nil {
		return fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.DeleteConsumer(queueName, consumerTag)
}

func (t *BadgerTransactionStorage) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	if t.wrapper.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not available")
	}
	return t.wrapper.metadataStore.GetQueueConsumers(queueName)
}

// TransactionStore methods - for now, delegate to memory implementation
func (t *BadgerTransactionStorage) BeginTransaction(txID string) (*interfaces.Transaction, error) {
	return nil, fmt.Errorf("transactions not yet implemented")
}

func (t *BadgerTransactionStorage) GetTransaction(txID string) (*interfaces.Transaction, error) {
	return nil, fmt.Errorf("transactions not yet implemented")
}

func (t *BadgerTransactionStorage) AddAction(txID string, action *interfaces.TransactionAction) error {
	return fmt.Errorf("transactions not yet implemented")
}

func (t *BadgerTransactionStorage) CommitTransaction(txID string) error {
	return fmt.Errorf("transactions not yet implemented")
}

func (t *BadgerTransactionStorage) RollbackTransaction(txID string) error {
	return fmt.Errorf("transactions not yet implemented")
}

func (t *BadgerTransactionStorage) DeleteTransaction(txID string) error {
	return fmt.Errorf("transactions not yet implemented")
}

func (t *BadgerTransactionStorage) ListActiveTransactions() ([]*interfaces.Transaction, error) {
	return nil, fmt.Errorf("transactions not yet implemented")
}

// AcknowledgmentStore methods
func (t *BadgerTransactionStorage) StorePendingAck(pendingAck *protocol.PendingAck) error {
	if t.wrapper.ackStore == nil {
		return fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.StorePendingAck(pendingAck)
}

func (t *BadgerTransactionStorage) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	if t.wrapper.ackStore == nil {
		return nil, fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.GetPendingAck(queueName, deliveryTag)
}

func (t *BadgerTransactionStorage) DeletePendingAck(queueName string, deliveryTag uint64) error {
	if t.wrapper.ackStore == nil {
		return fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.DeletePendingAck(queueName, deliveryTag)
}

func (t *BadgerTransactionStorage) GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error) {
	if t.wrapper.ackStore == nil {
		return nil, fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.GetQueuePendingAcks(queueName)
}

func (t *BadgerTransactionStorage) GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error) {
	if t.wrapper.ackStore == nil {
		return nil, fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.GetConsumerPendingAcks(consumerTag)
}

func (t *BadgerTransactionStorage) CleanupExpiredAcks(maxAge time.Duration) error {
	if t.wrapper.ackStore == nil {
		return fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.CleanupExpiredAcks(maxAge)
}

func (t *BadgerTransactionStorage) GetAllPendingAcks() ([]*protocol.PendingAck, error) {
	if t.wrapper.ackStore == nil {
		return nil, fmt.Errorf("acknowledgment store not available")
	}
	return t.wrapper.ackStore.GetAllPendingAcks()
}

// DurabilityStore methods
func (t *BadgerTransactionStorage) StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error {
	if t.wrapper.durabilityStore == nil {
		return fmt.Errorf("durability store not available")
	}
	return t.wrapper.durabilityStore.StoreDurableEntityMetadata(metadata)
}

func (t *BadgerTransactionStorage) GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error) {
	if t.wrapper.durabilityStore == nil {
		return nil, fmt.Errorf("durability store not available")
	}
	return t.wrapper.durabilityStore.GetDurableEntityMetadata()
}

func (t *BadgerTransactionStorage) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	if t.wrapper.durabilityStore == nil {
		return nil, fmt.Errorf("durability store not available")
	}
	return t.wrapper.durabilityStore.ValidateStorageIntegrity()
}

func (t *BadgerTransactionStorage) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	if t.wrapper.durabilityStore == nil {
		return nil, fmt.Errorf("durability store not available")
	}
	return t.wrapper.durabilityStore.RepairCorruption(autoRepair)
}

func (t *BadgerTransactionStorage) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	if t.wrapper.durabilityStore == nil {
		return nil, fmt.Errorf("durability store not available")
	}
	return t.wrapper.durabilityStore.GetRecoverableMessages()
}

func (t *BadgerTransactionStorage) MarkRecoveryComplete(stats *protocol.RecoveryStats) error {
	if t.wrapper.durabilityStore == nil {
		return fmt.Errorf("durability store not available")
	}
	return t.wrapper.durabilityStore.MarkRecoveryComplete(stats)
}

// AtomicStorage method - this allows nested transactions
func (t *BadgerTransactionStorage) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	// For nested transactions, just execute directly
	return operations(t)
}