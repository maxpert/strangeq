package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	DefaultRingBufferSize        = 1024 * 256
	DefaultSpillThresholdPercent = 80
)

type StorageMetrics interface {
	UpdateDiskMetrics(freeBytes, usedBytes float64)
	UpdateRingBufferUtilization(queueName string, utilization float64)
	WALMetrics
	SegmentMetrics
}

type DisruptorStorage struct {
	queues   sync.Map
	queuesMu sync.Mutex

	ringBufferSize int
	spillThreshold uint64

	metadataStore *PersistentMetadataStore
	offsetStore   *OffsetCheckpointStore
	wal           *WALManager
	segments      *SegmentManager

	transactions map[string]*interfaces.Transaction
	txMutex      sync.RWMutex

	pendingAcks  sync.Map
	consumerAcks sync.Map

	metrics StorageMetrics
	dataDir string
}

type QueueRing struct {
	name string
	ring *AtomicRing
	ack  *AckCursor

	consumerPositions map[string]*atomic.Uint64
	consumerMutex     sync.RWMutex

	closed atomic.Bool
}

func NewDisruptorStorage() *DisruptorStorage {
	return NewDisruptorStorageWithDataDir("./data")
}

func NewDisruptorStorageWithDataDir(dataDir string) *DisruptorStorage {
	return NewDisruptorStorageWithCheckpointInterval(dataDir, DefaultCheckpointInterval)
}

func NewDisruptorStorageWithCheckpointInterval(dataDir string, checkpointInterval time.Duration) *DisruptorStorage {
	return NewDisruptorStorageWithEngineConfig(dataDir, checkpointInterval, interfaces.EngineConfig{})
}

func WALConfigFromEngine(ec interfaces.EngineConfig) WALConfig {
	cfg := DefaultWALConfig()
	if ec.WALBatchSize > 0 {
		cfg.BatchSize = ec.WALBatchSize
	}
	if ec.WALBatchTimeoutMS > 0 {
		cfg.BatchTimeout = time.Duration(ec.WALBatchTimeoutMS) * time.Millisecond
	}
	if ec.WALFileSize > 0 {
		cfg.FileSize = ec.WALFileSize
	}
	if ec.WALChannelBuffer > 0 {
		cfg.ChannelBuffer = ec.WALChannelBuffer
	}
	if ec.WALCleanupCheckIntervalMS > 0 {
		cfg.CleanupInterval = time.Duration(ec.WALCleanupCheckIntervalMS) * time.Millisecond
	}
	return cfg
}

func SegmentConfigFromEngine(ec interfaces.EngineConfig) SegmentConfig {
	cfg := DefaultSegmentConfig()
	if ec.SegmentSize > 0 {
		cfg.SegmentSize = ec.SegmentSize
	}
	if ec.CompactionThreshold > 0 {
		cfg.CompactionThreshold = ec.CompactionThreshold
	}
	if ec.CompactionIntervalMS > 0 {
		cfg.CompactionInterval = time.Duration(ec.CompactionIntervalMS) * time.Millisecond
	}
	if ec.SegmentCheckpointIntervalMS > 0 {
		cfg.CheckpointInterval = time.Duration(ec.SegmentCheckpointIntervalMS) * time.Millisecond
	}
	return cfg
}

func NewDisruptorStorageWithEngineConfig(dataDir string, checkpointInterval time.Duration, engineCfg interfaces.EngineConfig) *DisruptorStorage {
	ringBufferSize := engineCfg.RingBufferSize
	if ringBufferSize <= 0 {
		ringBufferSize = DefaultRingBufferSize
	}
	spillPercent := engineCfg.SpillThresholdPercent
	if spillPercent <= 0 {
		spillPercent = DefaultSpillThresholdPercent
	}

	if ringBufferSize <= 0 || (ringBufferSize&(ringBufferSize-1)) != 0 {
		panic(fmt.Sprintf("ring buffer size must be a positive power of 2, got %d", ringBufferSize))
	}

	metadataStore, err := NewPersistentMetadataStore(dataDir)
	if err != nil {
		metadataStore = nil
	}

	offsetStore, err := NewOffsetCheckpointStoreWithInterval(dataDir, checkpointInterval)
	if err != nil {
		offsetStore = nil
	}

	walCfg := WALConfigFromEngine(engineCfg)
	segCfg := SegmentConfigFromEngine(engineCfg)

	walManager, err := NewWALManagerWithConfig(dataDir, walCfg)
	if err != nil {
		walManager = nil
	}

	segmentManager, err := NewSegmentManagerWithConfig(dataDir, segCfg)
	if err != nil {
		segmentManager = nil
	}

	if walManager != nil && segmentManager != nil {
		walManager.SetSegmentManager(segmentManager)
	}

	return &DisruptorStorage{
		ringBufferSize: ringBufferSize,
		spillThreshold: uint64(ringBufferSize) * uint64(spillPercent) / 100,
		metadataStore:  metadataStore,
		offsetStore:    offsetStore,
		wal:            walManager,
		segments:       segmentManager,
		transactions:   make(map[string]*interfaces.Transaction),
		dataDir:        dataDir,
	}
}

func (ds *DisruptorStorage) SetMetrics(metrics StorageMetrics) {
	ds.metrics = metrics
	if ds.wal != nil {
		ds.wal.SetMetrics(metrics)
	}
	if ds.segments != nil {
		ds.segments.SetMetrics(metrics)
	}
}

func (ds *DisruptorStorage) getQueueRing(queueName string) *QueueRing {
	if val, ok := ds.queues.Load(queueName); ok {
		return val.(*QueueRing)
	}
	return nil
}

func (ds *DisruptorStorage) getOrCreateQueueRing(queueName string) *QueueRing {
	if ring, ok := ds.queues.Load(queueName); ok {
		return ring.(*QueueRing)
	}

	ds.queuesMu.Lock()
	defer ds.queuesMu.Unlock()

	if ring, ok := ds.queues.Load(queueName); ok {
		return ring.(*QueueRing)
	}

	ring := &QueueRing{
		name:              queueName,
		ring:              NewAtomicRing(ds.ringBufferSize),
		ack:               NewAckCursor(),
		consumerPositions: make(map[string]*atomic.Uint64),
	}

	ds.queues.Store(queueName, ring)
	return ring
}

func (ds *DisruptorStorage) LoadMessageFromRecovery(queueName string, message *protocol.Message) error {
	ring := ds.getOrCreateQueueRing(queueName)

	if ring.ring.Count() >= ds.spillThreshold {
		return nil
	}

	_, spilled, err := ring.ring.Store(message.DeliveryTag, message)
	if err != nil {
		return err
	}
	if spilled {
		return nil
	}

	ring.ack.OnPublish(message.DeliveryTag)
	return nil
}

func (ds *DisruptorStorage) StoreMessage(queueName string, message *protocol.Message) error {
	ring := ds.getOrCreateQueueRing(queueName)

	if ds.wal != nil && message.DeliveryMode == 2 {
		if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
			return fmt.Errorf("failed to write durable message to WAL: %w", err)
		}
	}

	if ds.wal != nil && ring.ring.Count() > ds.spillThreshold {
		if message.DeliveryMode != 2 {
			if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
				return fmt.Errorf("failed to spill transient message to WAL: %w", err)
			}
		}
		return nil
	}

	_, spilled, err := ring.ring.Store(message.DeliveryTag, message)
	if err != nil {
		return fmt.Errorf("failed to store message in ring: %w", err)
	}
	if spilled {
		if message.DeliveryMode != 2 && ds.wal != nil {
			if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
				return fmt.Errorf("failed to spill transient message to WAL: %w", err)
			}
		}
		return nil
	}

	ring.ack.OnPublish(message.DeliveryTag)
	return nil
}

func (ds *DisruptorStorage) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}

	if msg, ok := ring.ring.LoadByTag(deliveryTag); ok {
		return msg, nil
	}

	if ds.wal != nil {
		if msg, err := ds.wal.Read(queueName, deliveryTag); err == nil {
			return msg, nil
		}
	}

	if ds.segments != nil {
		if msg, err := ds.segments.Read(queueName, deliveryTag); err == nil {
			return msg, nil
		}
	}

	return nil, interfaces.ErrMessageNotFound
}

func (ds *DisruptorStorage) LoadMessageBySeq(queueName string, seq uint64) (*protocol.Message, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}

	if msg, ok := ring.ring.LoadBySeq(seq); ok {
		return msg, nil
	}

	return nil, interfaces.ErrMessageNotFound
}

func (ds *DisruptorStorage) DeleteMessage(queueName string, deliveryTag uint64) error {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return interfaces.ErrQueueNotFound
	}

	ring.ring.Delete(deliveryTag)

	if ds.wal != nil {
		ds.wal.Acknowledge(queueName, deliveryTag)
	}

	if ds.segments != nil {
		ds.segments.Acknowledge(queueName, deliveryTag)
	}

	return nil
}

func (ds *DisruptorStorage) RegisterConsumerCursor(queueName string, consumerTag string) {
	ring := ds.getOrCreateQueueRing(queueName)
	ring.ack.OnConsumerRegister(consumerTag)
}

func (ds *DisruptorStorage) UnregisterConsumerCursor(queueName string, consumerTag string) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnConsumerUnregister(consumerTag)
}

func (ds *DisruptorStorage) DeliverToConsumer(queueName string, consumerTag string, deliveryTag uint64) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnDeliver(deliveryTag, consumerTag)
}

func (ds *DisruptorStorage) AckFromConsumer(queueName string, consumerTag string, deliveryTag uint64) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnAck(deliveryTag, consumerTag)
}

func (ds *DisruptorStorage) NackFromConsumer(queueName string, consumerTag string, deliveryTag uint64) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnNack(deliveryTag, consumerTag)
}

func (ds *DisruptorStorage) GetMinAckCursor(queueName string) uint64 {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0
	}
	return ring.ack.MinAckCursor()
}

func (ds *DisruptorStorage) IsSafeToOverwrite(queueName string, oldTag, newTag uint64) bool {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return true
	}
	return ring.ack.IsSafeToOverwrite(oldTag, newTag, ds.ringBufferSize)
}

func (ds *DisruptorStorage) GetQueueMessages(queueName string) ([]*protocol.Message, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	return ring.ring.GetAll(), nil
}

func (ds *DisruptorStorage) GetQueueMessageCount(queueName string) (int, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return int(ring.ring.Count()), nil
}

func (ds *DisruptorStorage) PurgeQueue(queueName string) (int, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ring.ring.Purge(), nil
}

func (ds *DisruptorStorage) GetMessageRange(queueName string, startTag, endTag uint64) ([]*protocol.Message, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	return ring.ring.GetRange(startTag, endTag), nil
}

func (ds *DisruptorStorage) DeleteMessageRange(queueName string, startTag, endTag uint64) error {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return interfaces.ErrQueueNotFound
	}
	ring.ring.DeleteRange(startTag, endTag)
	return nil
}

func (ds *DisruptorStorage) GetQueueHead(queueName string) (uint64, error) {
	ring := ds.getOrCreateQueueRing(queueName)
	return ring.ack.Head(), nil
}

func (ds *DisruptorStorage) SetQueueHead(queueName string, head uint64) error {
	ring := ds.getOrCreateQueueRing(queueName)
	ring.ack.OnPublish(head)
	return nil
}

func (ds *DisruptorStorage) IncrementQueueHead(queueName string) (uint64, error) {
	ring := ds.getOrCreateQueueRing(queueName)
	head := ring.ack.Head() + 1
	ring.ack.OnPublish(head)
	return head, nil
}

func (ds *DisruptorStorage) GetConsumerPosition(queueName, consumerTag string) (uint64, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}

	ring.consumerMutex.RLock()
	defer ring.consumerMutex.RUnlock()

	if pos, exists := ring.consumerPositions[consumerTag]; exists {
		return pos.Load(), nil
	}
	return 0, nil
}

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

func (ds *DisruptorStorage) AddUnacked(queueName, consumerTag string, deliveryTag uint64) error {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return interfaces.ErrQueueNotFound
	}
	ring.ack.OnDeliver(deliveryTag, consumerTag)
	return nil
}

func (ds *DisruptorStorage) RemoveUnacked(queueName, consumerTag string, deliveryTag uint64) error {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return interfaces.ErrQueueNotFound
	}
	ring.ack.OnAck(deliveryTag, consumerTag)

	if ds.offsetStore != nil {
		ds.offsetStore.UpdateOffset(queueName, consumerTag, deliveryTag)
	}
	return nil
}

func (ds *DisruptorStorage) GetUnackedCount(queueName, consumerTag string) (int, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ring.ack.GetUnackedCount(consumerTag), nil
}

func (ds *DisruptorStorage) GetUnackedTags(queueName, consumerTag string) ([]uint64, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}

	return ring.ack.GetUnackedTags(consumerTag), nil
}

func (ds *DisruptorStorage) GetLowestUnackedAcrossConsumers(queueName string) (uint64, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ring.ack.MinAckCursor(), nil
}

func (ds *DisruptorStorage) StoreExchange(exchange *protocol.Exchange) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreExchange(exchange)
}

func (ds *DisruptorStorage) GetExchange(name string) (*protocol.Exchange, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchange(name)
}

func (ds *DisruptorStorage) DeleteExchange(name string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteExchange(name)
}

func (ds *DisruptorStorage) ListExchanges() ([]*protocol.Exchange, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.ListExchanges()
}

func (ds *DisruptorStorage) StoreQueue(queue *protocol.Queue) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	if err := ds.metadataStore.StoreQueue(queue); err != nil {
		return err
	}
	_ = ds.getOrCreateQueueRing(queue.Name)
	return nil
}

func (ds *DisruptorStorage) GetQueue(name string) (*protocol.Queue, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueue(name)
}

func (ds *DisruptorStorage) DeleteQueue(name string) error {
	if val, ok := ds.queues.LoadAndDelete(name); ok {
		ring := val.(*QueueRing)
		ring.ring.Close()
		ring.closed.Store(true)
	}

	if ds.metadataStore != nil {
		return ds.metadataStore.DeleteQueue(name)
	}
	return nil
}

func (ds *DisruptorStorage) ListQueues() ([]*protocol.Queue, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.ListQueues()
}

func (ds *DisruptorStorage) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreBinding(queueName, exchangeName, routingKey, arguments)
}

func (ds *DisruptorStorage) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetBinding(queueName, exchangeName, routingKey)
}

func (ds *DisruptorStorage) DeleteBinding(queueName, exchangeName, routingKey string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteBinding(queueName, exchangeName, routingKey)
}

func (ds *DisruptorStorage) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueueBindings(queueName)
}

func (ds *DisruptorStorage) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchangeBindings(exchangeName)
}

func (ds *DisruptorStorage) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreConsumer(queueName, consumerTag, consumer)
}

func (ds *DisruptorStorage) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetConsumer(queueName, consumerTag)
}

func (ds *DisruptorStorage) DeleteConsumer(queueName, consumerTag string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	if ds.offsetStore != nil {
		_ = ds.offsetStore.RemoveConsumer(queueName, consumerTag)
	}
	return ds.metadataStore.DeleteConsumer(queueName, consumerTag)
}

func (ds *DisruptorStorage) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueueConsumers(queueName)
}

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

	if tx, ok := ds.transactions[txID]; ok {
		return tx, nil
	}
	return nil, interfaces.ErrTransactionNotFound
}

func (ds *DisruptorStorage) AddAction(txID string, action *interfaces.TransactionAction) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	tx, ok := ds.transactions[txID]
	if !ok {
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

	tx, ok := ds.transactions[txID]
	if !ok {
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

	tx, ok := ds.transactions[txID]
	if !ok {
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

func (ds *DisruptorStorage) StorePendingAck(pendingAck *protocol.PendingAck) error {
	if pendingAck == nil {
		return nil
	}
	ds.pendingAcks.Store(pendingAck.DeliveryTag, pendingAck)

	var consumerList []*protocol.PendingAck
	if val, ok := ds.consumerAcks.Load(pendingAck.ConsumerTag); ok {
		consumerList = val.([]*protocol.PendingAck)
	}
	consumerList = append(consumerList, pendingAck)
	ds.consumerAcks.Store(pendingAck.ConsumerTag, consumerList)

	return nil
}

func (ds *DisruptorStorage) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	if val, ok := ds.pendingAcks.Load(deliveryTag); ok {
		return val.(*protocol.PendingAck), nil
	}
	return nil, interfaces.ErrPendingAckNotFound
}

func (ds *DisruptorStorage) DeletePendingAck(queueName string, deliveryTag uint64) error {
	if val, ok := ds.pendingAcks.LoadAndDelete(deliveryTag); ok {
		pa := val.(*protocol.PendingAck)
		if consumerVal, cok := ds.consumerAcks.Load(pa.ConsumerTag); cok {
			list := consumerVal.([]*protocol.PendingAck)
			newList := make([]*protocol.PendingAck, 0, len(list))
			for _, item := range list {
				if item.DeliveryTag != deliveryTag {
					newList = append(newList, item)
				}
			}
			if len(newList) == 0 {
				ds.consumerAcks.Delete(pa.ConsumerTag)
			} else {
				ds.consumerAcks.Store(pa.ConsumerTag, newList)
			}
		}
	}
	return nil
}

func (ds *DisruptorStorage) GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error) {
	var result []*protocol.PendingAck
	ds.pendingAcks.Range(func(_, val interface{}) bool {
		pa := val.(*protocol.PendingAck)
		if pa.QueueName == queueName {
			result = append(result, pa)
		}
		return true
	})
	return result, nil
}

func (ds *DisruptorStorage) GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error) {
	if val, ok := ds.consumerAcks.Load(consumerTag); ok {
		return val.([]*protocol.PendingAck), nil
	}
	return []*protocol.PendingAck{}, nil
}

func (ds *DisruptorStorage) CleanupExpiredAcks(maxAge time.Duration) error {
	return nil
}

func (ds *DisruptorStorage) GetAllPendingAcks() ([]*protocol.PendingAck, error) {
	var result []*protocol.PendingAck
	ds.pendingAcks.Range(func(_, val interface{}) bool {
		result = append(result, val.(*protocol.PendingAck))
		return true
	})
	return result, nil
}

func (ds *DisruptorStorage) StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error {
	return nil
}

func (ds *DisruptorStorage) GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}

	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []*protocol.Exchange{},
		Queues:      []*protocol.Queue{},
		Bindings:    []protocol.Binding{},
		LastUpdated: time.Now(),
	}

	exchanges, err := ds.metadataStore.ListExchanges()
	if err != nil {
		return nil, fmt.Errorf("failed to list exchanges: %w", err)
	}
	for _, exchange := range exchanges {
		if exchange.Durable {
			metadata.Exchanges = append(metadata.Exchanges, exchange)
		}
	}

	queues, err := ds.metadataStore.ListQueues()
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	for _, queue := range queues {
		if queue.Durable {
			metadata.Queues = append(metadata.Queues, queue)
		}
	}

	return metadata, nil
}

func (ds *DisruptorStorage) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, nil
}

func (ds *DisruptorStorage) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, nil
}

func (ds *DisruptorStorage) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	if ds.wal == nil {
		return make(map[string][]*protocol.Message), nil
	}

	recoveredMessages, err := ds.wal.RecoverFromWAL()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

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

func (ds *DisruptorStorage) RecoverFromLog(queueName string) error {
	_ = ds.getOrCreateQueueRing(queueName)
	return nil
}

func (ds *DisruptorStorage) RecoverAllQueues() error {
	if ds.metadataStore == nil {
		return nil
	}

	queues, err := ds.metadataStore.ListQueues()
	if err != nil {
		return fmt.Errorf("failed to list queues: %w", err)
	}

	for _, queue := range queues {
		if err := ds.RecoverFromLog(queue.Name); err != nil {
			continue
		}
	}
	return nil
}

func (ds *DisruptorStorage) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()
	return operations(ds)
}

func (ds *DisruptorStorage) Close() error {
	ds.queues.Range(func(_, value interface{}) bool {
		ring := value.(*QueueRing)
		if !ring.closed.Load() {
			ring.ring.Close()
			ring.closed.Store(true)
		}
		return true
	})

	if ds.metadataStore != nil {
		_ = ds.metadataStore.Close()
	}

	if ds.offsetStore != nil {
		_ = ds.offsetStore.Close()
	}

	if ds.wal != nil {
		_ = ds.wal.Close()
	}

	if ds.segments != nil {
		_ = ds.segments.Close()
	}

	return nil
}

var _ interfaces.Storage = (*DisruptorStorage)(nil)
