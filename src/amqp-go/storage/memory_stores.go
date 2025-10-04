package storage

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// MemoryMessageStore implements MessageStore interface using in-memory storage
type MemoryMessageStore struct {
	messages map[string]*protocol.Message // key format: "queue_name:delivery_tag"
	mutex    sync.RWMutex
	ttl      time.Duration
}

// NewMemoryMessageStore creates a new in-memory message store
func NewMemoryMessageStore() *MemoryMessageStore {
	return &MemoryMessageStore{
		messages: make(map[string]*protocol.Message),
		ttl:      24 * time.Hour, // Default 24 hour TTL
	}
}

func (m *MemoryMessageStore) StoreMessage(queueName string, message *protocol.Message) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := m.messageKey(queueName, message.DeliveryTag)
	// Make a copy to avoid external modifications
	msgCopy := *message
	if message.Body != nil {
		msgCopy.Body = make([]byte, len(message.Body))
		copy(msgCopy.Body, message.Body)
	}
	m.messages[key] = &msgCopy
	
	return nil
}

func (m *MemoryMessageStore) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	key := m.messageKey(queueName, deliveryTag)
	message, exists := m.messages[key]
	if !exists {
		return nil, interfaces.ErrMessageNotFound
	}
	
	// Return a copy to avoid external modifications
	msgCopy := *message
	if message.Body != nil {
		msgCopy.Body = make([]byte, len(message.Body))
		copy(msgCopy.Body, message.Body)
	}
	
	return &msgCopy, nil
}

func (m *MemoryMessageStore) DeleteMessage(queueName string, deliveryTag uint64) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := m.messageKey(queueName, deliveryTag)
	if _, exists := m.messages[key]; !exists {
		return interfaces.ErrMessageNotFound
	}
	
	delete(m.messages, key)
	return nil
}

func (m *MemoryMessageStore) GetQueueMessages(queueName string) ([]*protocol.Message, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	prefix := queueName + ":"
	var messages []*protocol.Message
	
	for key, message := range m.messages {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			// Return a copy
			msgCopy := *message
			if message.Body != nil {
				msgCopy.Body = make([]byte, len(message.Body))
				copy(msgCopy.Body, message.Body)
			}
			messages = append(messages, &msgCopy)
		}
	}
	
	return messages, nil
}

func (m *MemoryMessageStore) GetQueueMessageCount(queueName string) (int, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	prefix := queueName + ":"
	count := 0
	
	for key := range m.messages {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			count++
		}
	}
	
	return count, nil
}

func (m *MemoryMessageStore) PurgeQueue(queueName string) (int, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	prefix := queueName + ":"
	count := 0
	
	for key := range m.messages {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			delete(m.messages, key)
			count++
		}
	}
	
	return count, nil
}

func (m *MemoryMessageStore) messageKey(queueName string, deliveryTag uint64) string {
	return queueName + ":" + string(rune(deliveryTag))
}

// MemoryMetadataStore implements MetadataStore interface using in-memory storage
type MemoryMetadataStore struct {
	exchanges map[string]*protocol.Exchange
	queues    map[string]*protocol.Queue
	bindings  map[string]*interfaces.QueueBinding // key format: "queue:exchange:routing_key"
	consumers map[string]*protocol.Consumer      // key format: "queue:consumer_tag"
	mutex     sync.RWMutex
}

// NewMemoryMetadataStore creates a new in-memory metadata store
func NewMemoryMetadataStore() *MemoryMetadataStore {
	return &MemoryMetadataStore{
		exchanges: make(map[string]*protocol.Exchange),
		queues:    make(map[string]*protocol.Queue),
		bindings:  make(map[string]*interfaces.QueueBinding),
		consumers: make(map[string]*protocol.Consumer),
	}
}

// Exchange operations
func (m *MemoryMetadataStore) StoreExchange(exchange *protocol.Exchange) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Make a copy to avoid external modifications
	exchCopy := *exchange
	m.exchanges[exchange.Name] = &exchCopy
	
	return nil
}

func (m *MemoryMetadataStore) GetExchange(name string) (*protocol.Exchange, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	exchange, exists := m.exchanges[name]
	if !exists {
		return nil, interfaces.ErrExchangeNotFound
	}
	
	// Return a copy
	exchCopy := *exchange
	return &exchCopy, nil
}

func (m *MemoryMetadataStore) DeleteExchange(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if _, exists := m.exchanges[name]; !exists {
		return interfaces.ErrExchangeNotFound
	}
	
	delete(m.exchanges, name)
	return nil
}

func (m *MemoryMetadataStore) ListExchanges() ([]*protocol.Exchange, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	exchanges := make([]*protocol.Exchange, 0, len(m.exchanges))
	for _, exchange := range m.exchanges {
		exchCopy := *exchange
		exchanges = append(exchanges, &exchCopy)
	}
	
	return exchanges, nil
}

// Queue operations
func (m *MemoryMetadataStore) StoreQueue(queue *protocol.Queue) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Make a copy
	queueCopy := *queue
	m.queues[queue.Name] = &queueCopy
	
	return nil
}

func (m *MemoryMetadataStore) GetQueue(name string) (*protocol.Queue, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	queue, exists := m.queues[name]
	if !exists {
		return nil, interfaces.ErrQueueNotFound
	}
	
	// Return a copy
	queueCopy := *queue
	return &queueCopy, nil
}

func (m *MemoryMetadataStore) DeleteQueue(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if _, exists := m.queues[name]; !exists {
		return interfaces.ErrQueueNotFound
	}
	
	delete(m.queues, name)
	return nil
}

func (m *MemoryMetadataStore) ListQueues() ([]*protocol.Queue, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	queues := make([]*protocol.Queue, 0, len(m.queues))
	for _, queue := range m.queues {
		queueCopy := *queue
		queues = append(queues, &queueCopy)
	}
	
	return queues, nil
}

// Binding operations
func (m *MemoryMetadataStore) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := m.bindingKey(queueName, exchangeName, routingKey)
	
	// Copy arguments map
	argsCopy := make(map[string]interface{})
	for k, v := range arguments {
		argsCopy[k] = v
	}
	
	m.bindings[key] = &interfaces.QueueBinding{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Arguments:    argsCopy,
	}
	
	return nil
}

func (m *MemoryMetadataStore) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	key := m.bindingKey(queueName, exchangeName, routingKey)
	binding, exists := m.bindings[key]
	if !exists {
		return nil, interfaces.ErrBindingNotFound
	}
	
	// Return a copy
	argsCopy := make(map[string]interface{})
	for k, v := range binding.Arguments {
		argsCopy[k] = v
	}
	
	return &interfaces.QueueBinding{
		QueueName:    binding.QueueName,
		ExchangeName: binding.ExchangeName,
		RoutingKey:   binding.RoutingKey,
		Arguments:    argsCopy,
	}, nil
}

func (m *MemoryMetadataStore) DeleteBinding(queueName, exchangeName, routingKey string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := m.bindingKey(queueName, exchangeName, routingKey)
	if _, exists := m.bindings[key]; !exists {
		return interfaces.ErrBindingNotFound
	}
	
	delete(m.bindings, key)
	return nil
}

func (m *MemoryMetadataStore) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var bindings []*interfaces.QueueBinding
	prefix := queueName + ":"
	
	for key, binding := range m.bindings {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			// Return a copy
			argsCopy := make(map[string]interface{})
			for k, v := range binding.Arguments {
				argsCopy[k] = v
			}
			
			bindings = append(bindings, &interfaces.QueueBinding{
				QueueName:    binding.QueueName,
				ExchangeName: binding.ExchangeName,
				RoutingKey:   binding.RoutingKey,
				Arguments:    argsCopy,
			})
		}
	}
	
	return bindings, nil
}

func (m *MemoryMetadataStore) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var bindings []*interfaces.QueueBinding
	
	for _, binding := range m.bindings {
		if binding.ExchangeName == exchangeName {
			// Return a copy
			argsCopy := make(map[string]interface{})
			for k, v := range binding.Arguments {
				argsCopy[k] = v
			}
			
			bindings = append(bindings, &interfaces.QueueBinding{
				QueueName:    binding.QueueName,
				ExchangeName: binding.ExchangeName,
				RoutingKey:   binding.RoutingKey,
				Arguments:    argsCopy,
			})
		}
	}
	
	return bindings, nil
}

// Consumer operations
func (m *MemoryMetadataStore) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := m.consumerKey(queueName, consumerTag)
	// Make a copy
	consumerCopy := *consumer
	m.consumers[key] = &consumerCopy
	
	return nil
}

func (m *MemoryMetadataStore) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	key := m.consumerKey(queueName, consumerTag)
	consumer, exists := m.consumers[key]
	if !exists {
		return nil, interfaces.ErrConsumerNotFound
	}
	
	// Return a copy
	consumerCopy := *consumer
	return &consumerCopy, nil
}

func (m *MemoryMetadataStore) DeleteConsumer(queueName, consumerTag string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := m.consumerKey(queueName, consumerTag)
	if _, exists := m.consumers[key]; !exists {
		return interfaces.ErrConsumerNotFound
	}
	
	delete(m.consumers, key)
	return nil
}

func (m *MemoryMetadataStore) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	prefix := queueName + ":"
	var consumers []*protocol.Consumer
	
	for key, consumer := range m.consumers {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			// Return a copy
			consumerCopy := *consumer
			consumers = append(consumers, &consumerCopy)
		}
	}
	
	return consumers, nil
}

func (m *MemoryMetadataStore) bindingKey(queueName, exchangeName, routingKey string) string {
	return queueName + ":" + exchangeName + ":" + routingKey
}

func (m *MemoryMetadataStore) consumerKey(queueName, consumerTag string) string {
	return queueName + ":" + consumerTag
}

// MemoryTransactionStore implements TransactionStore interface using in-memory storage
type MemoryTransactionStore struct {
	transactions map[string]*interfaces.Transaction
	mutex        sync.RWMutex
}

// NewMemoryTransactionStore creates a new in-memory transaction store
func NewMemoryTransactionStore() *MemoryTransactionStore {
	return &MemoryTransactionStore{
		transactions: make(map[string]*interfaces.Transaction),
	}
}

// MemoryAckStore implements AcknowledgmentStore in memory
type MemoryAckStore struct {
	pendingAcks map[string]*protocol.PendingAck
	mutex       sync.RWMutex
}

// NewMemoryAckStore creates a new in-memory acknowledgment store
func NewMemoryAckStore() *MemoryAckStore {
	return &MemoryAckStore{
		pendingAcks: make(map[string]*protocol.PendingAck),
	}
}

// StorePendingAck stores a pending acknowledgment in memory
func (s *MemoryAckStore) StorePendingAck(pendingAck *protocol.PendingAck) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	key := fmt.Sprintf("%s:%d", pendingAck.QueueName, pendingAck.DeliveryTag)
	s.pendingAcks[key] = pendingAck
	return nil
}

// GetPendingAck retrieves a pending acknowledgment
func (s *MemoryAckStore) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	key := fmt.Sprintf("%s:%d", queueName, deliveryTag)
	if ack, exists := s.pendingAcks[key]; exists {
		return ack, nil
	}
	
	return nil, interfaces.ErrPendingAckNotFound
}

// DeletePendingAck removes a pending acknowledgment
func (s *MemoryAckStore) DeletePendingAck(queueName string, deliveryTag uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	key := fmt.Sprintf("%s:%d", queueName, deliveryTag)
	delete(s.pendingAcks, key)
	return nil
}

// GetQueuePendingAcks retrieves all pending acknowledgments for a queue
func (s *MemoryAckStore) GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	var acks []*protocol.PendingAck
	for key, ack := range s.pendingAcks {
		if strings.HasPrefix(key, queueName+":") {
			acks = append(acks, ack)
		}
	}
	
	return acks, nil
}

// GetConsumerPendingAcks retrieves all pending acknowledgments for a consumer
func (s *MemoryAckStore) GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	var acks []*protocol.PendingAck
	for _, ack := range s.pendingAcks {
		if ack.ConsumerTag == consumerTag {
			acks = append(acks, ack)
		}
	}
	
	return acks, nil
}

// CleanupExpiredAcks removes acknowledgments older than maxAge
func (s *MemoryAckStore) CleanupExpiredAcks(maxAge time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	cutoff := time.Now().Add(-maxAge)
	
	for key, ack := range s.pendingAcks {
		if ack.DeliveredAt.Before(cutoff) {
			delete(s.pendingAcks, key)
		}
	}
	
	return nil
}

// GetAllPendingAcks retrieves all pending acknowledgments
func (s *MemoryAckStore) GetAllPendingAcks() ([]*protocol.PendingAck, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	var acks []*protocol.PendingAck
	for _, ack := range s.pendingAcks {
		acks = append(acks, ack)
	}
	
	return acks, nil
}

// MemoryDurabilityStore implements DurabilityStore in memory
type MemoryDurabilityStore struct {
	metadata *protocol.DurableEntityMetadata
	mutex    sync.RWMutex
}

// NewMemoryDurabilityStore creates a new in-memory durability store
func NewMemoryDurabilityStore() *MemoryDurabilityStore {
	return &MemoryDurabilityStore{
		metadata: &protocol.DurableEntityMetadata{
			Exchanges:   []protocol.Exchange{},
			Queues:      []protocol.Queue{},
			Bindings:    []protocol.Binding{},
			LastUpdated: time.Now(),
		},
	}
}

// StoreDurableEntityMetadata stores durable entity metadata
func (s *MemoryDurabilityStore) StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	metadata.LastUpdated = time.Now()
	s.metadata = metadata
	return nil
}

// GetDurableEntityMetadata retrieves durable entity metadata
func (s *MemoryDurabilityStore) GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	
	return s.metadata, nil
}

// ValidateStorageIntegrity validates memory storage (always returns success for memory)
func (s *MemoryDurabilityStore) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{
		ValidationErrors: []string{},
		RecoveryDuration: 0,
	}, nil
}

// RepairCorruption repairs memory storage (no-op for memory)
func (s *MemoryDurabilityStore) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	return s.ValidateStorageIntegrity()
}

// GetRecoverableMessages returns empty map for memory storage
func (s *MemoryDurabilityStore) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	return make(map[string][]*protocol.Message), nil
}

// MarkRecoveryComplete marks recovery as complete (no-op for memory)
func (s *MemoryDurabilityStore) MarkRecoveryComplete(stats *protocol.RecoveryStats) error {
	return nil
}

func (m *MemoryTransactionStore) BeginTransaction(txID string) (*interfaces.Transaction, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if _, exists := m.transactions[txID]; exists {
		return nil, interfaces.ErrTransactionExists
	}
	
	tx := &interfaces.Transaction{
		ID:        txID,
		Status:    interfaces.TxStatusActive,
		StartTime: time.Now(),
		Actions:   make([]*interfaces.TransactionAction, 0),
	}
	
	m.transactions[txID] = tx
	return tx, nil
}

func (m *MemoryTransactionStore) GetTransaction(txID string) (*interfaces.Transaction, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	tx, exists := m.transactions[txID]
	if !exists {
		return nil, interfaces.ErrTransactionNotFound
	}
	
	return tx, nil
}

func (m *MemoryTransactionStore) AddAction(txID string, action *interfaces.TransactionAction) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	tx, exists := m.transactions[txID]
	if !exists {
		return interfaces.ErrTransactionNotFound
	}
	
	if tx.Status != interfaces.TxStatusActive {
		return interfaces.ErrTransactionNotActive
	}
	
	tx.Actions = append(tx.Actions, action)
	return nil
}

func (m *MemoryTransactionStore) CommitTransaction(txID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	tx, exists := m.transactions[txID]
	if !exists {
		return interfaces.ErrTransactionNotFound
	}
	
	if tx.Status != interfaces.TxStatusActive {
		return interfaces.ErrTransactionNotActive
	}
	
	tx.Status = interfaces.TxStatusCommitted
	tx.EndTime = time.Now()
	
	// In a real implementation, we would apply all actions here
	// For now, just mark as committed
	
	return nil
}

func (m *MemoryTransactionStore) RollbackTransaction(txID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	tx, exists := m.transactions[txID]
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

func (m *MemoryTransactionStore) DeleteTransaction(txID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if _, exists := m.transactions[txID]; !exists {
		return interfaces.ErrTransactionNotFound
	}
	
	delete(m.transactions, txID)
	return nil
}

func (m *MemoryTransactionStore) ListActiveTransactions() ([]*interfaces.Transaction, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var active []*interfaces.Transaction
	for _, tx := range m.transactions {
		if tx.Status == interfaces.TxStatusActive {
			active = append(active, tx)
		}
	}
	
	return active, nil
}