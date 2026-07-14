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

	// pendingAckShardCount is the number of striped locks used to make
	// StorePendingAck and DeletePendingAck mutually exclusive for a given
	// delivery tag. It is a power of two so deliveryTag&mask selects a shard.
	pendingAckShardCount = 256
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

	pendingAcks sync.Map

	// consumerPendingAcks is a secondary index over pendingAcks keyed by
	// consumerTag -> *sync.Map{ deliveryTag -> *PendingAck }.
	//
	// Safety invariant: at all times the index is a SUPERSET of pendingAcks for
	// every consumer, so GetConsumerPendingAcks can transiently observe an
	// extra (already-acked) entry but can never drop a tag that is still
	// pending. This keeps GetConsumerPendingAcks lock-free and O(M) in the
	// consumer's own pending set instead of O(N) over the global map.
	//
	// The invariant is enforced by making StorePendingAck and DeletePendingAck
	// mutually exclusive FOR A GIVEN DELIVERY TAG. Each tag selects one shard of
	// pendingAckShards (a striped lock set); both methods hold that shard's
	// mutex across their entire two-map update, so the update is atomic with
	// respect to any other Store/Delete of the SAME tag:
	//   - StorePendingAck writes the index BEFORE the primary (under the shard
	//     lock), so the primary only gains a tag after the index already has it.
	//   - DeletePendingAck deletes from the primary BEFORE the index (under the
	//     shard lock), so the index only loses a tag after the primary lost it.
	//
	// Same-tag atomicity rules out the lost-tag interleaving that a non-atomic
	// two-map index suffers under re-delivery: a re-Store's index write can no
	// longer be undone by a concurrent Delete's index delete while the
	// re-Store's primary write lands after that Delete's primary delete. This
	// holds even when a re-Store of a previously-stored tag (re-delivery after
	// requeue) races the ack's Delete -- the two are serialized on the tag's
	// shard, leaving the tag present in both maps or absent from both.
	//
	// Distinct tags may map to different shards and run concurrently, even when
	// they share a consumer's inner *sync.Map; sync.Map handles concurrent
	// distinct-key access and the per-tag invariant is independent. Reads
	// (GetConsumerPendingAcks) only Range the inner sync.Map, which is safe
	// concurrent with those writers and observes the superset at every instant.
	consumerPendingAcks sync.Map

	// pendingAckShards striped-locks the per-delivery-tag pending-ack updates
	// (see consumerPendingAcks). Sharded by deliveryTag to bound contention
	// while still serializing all operations on the same tag.
	pendingAckShards [pendingAckShardCount]sync.Mutex

	metrics StorageMetrics
	dataDir string
}

type QueueRing struct {
	name      string
	ring      *AtomicRing
	ack       *AckCursor
	readAhead *readAheadBuffer
	closed    atomic.Bool
}

func NewDisruptorStorage() (*DisruptorStorage, error) {
	return NewDisruptorStorageWithDataDir("./data")
}

func NewDisruptorStorageWithDataDir(dataDir string) (*DisruptorStorage, error) {
	return NewDisruptorStorageWithCheckpointInterval(dataDir, DefaultCheckpointInterval)
}

func NewDisruptorStorageWithCheckpointInterval(dataDir string, checkpointInterval time.Duration) (*DisruptorStorage, error) {
	return NewDisruptorStorageWithEngineConfig(dataDir, checkpointInterval, interfaces.EngineConfig{})
}

func WALConfigFromEngine(ec interfaces.EngineConfig) WALConfig {
	cfg := DefaultWALConfig()
	// Unconditional copy is safe: both zero values mean fsync ON (durable) and
	// CRC ON (safe).
	cfg.SyncDisabled = ec.WALSyncDisabled
	cfg.CRCDisabled = ec.WALCRCDisabled
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
	if ec.SharedBodyMaxPinAgeMS > 0 {
		cfg.SharedBodyMaxPinAge = time.Duration(ec.SharedBodyMaxPinAgeMS) * time.Millisecond
	}
	return cfg
}

func SegmentConfigFromEngine(ec interfaces.EngineConfig) SegmentConfig {
	cfg := DefaultSegmentConfig()
	cfg.CRCDisabled = ec.WALCRCDisabled
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

func NewDisruptorStorageWithEngineConfig(dataDir string, checkpointInterval time.Duration, engineCfg interfaces.EngineConfig) (*DisruptorStorage, error) {
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
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	offsetStore, err := NewOffsetCheckpointStoreWithInterval(dataDir, checkpointInterval)
	if err != nil {
		offsetStore = nil
	}

	walCfg := WALConfigFromEngine(engineCfg)
	segCfg := SegmentConfigFromEngine(engineCfg)

	walManager, err := NewWALManagerWithConfig(dataDir, walCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}

	segmentManager, err := NewSegmentManagerWithConfig(dataDir, segCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment manager: %w", err)
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
	}, nil
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
		name:      queueName,
		ring:      NewAtomicRing(ds.ringBufferSize),
		ack:       NewAckCursor(),
		readAhead: newReadAheadBuffer(),
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
	if ds.wal == nil && message.DeliveryMode == 2 {
		return fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)
	}

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
			return nil
		}
		if ds.wal == nil {
			return fmt.Errorf("ring full and WAL unavailable for queue %s", queueName)
		}
		return nil
	}

	ring.ack.OnPublish(message.DeliveryTag)
	return nil
}

// StoreMessageAsync is the async-completion sibling of StoreMessage for the
// durable publish path (DeliveryMode==2). It enqueues the durable WAL write via
// WriteAsync (never blocking the caller on the fsync) and reports the fsync
// completion through onDurable.
//
// ring.Store is DEFERRED into the WAL completion (iteration 2, frontier design):
// on fsync SUCCESS the message is made ring-resident just before onDurable runs
// — and the broker advances consumer-visibility (its per-queue frontier) only
// from onDurable, AFTER this ring store, so a consumer that claims the tag the
// instant it becomes visible always finds it (no stranding). On fsync ERROR the
// ring store is skipped, so a non-durable tag is NEVER ring-resident: when the
// frontier advances past it, its claim GetMessage-misses and it is gap-skipped,
// never delivered (no stale-slot delivery of a nacked message). Deferral is safe
// precisely because the frontier never exposes a tag before its own completion,
// which is why the earlier "ring.Store synchronous" condition is no longer
// needed. Over the spill threshold a durable message is WAL-only, as in
// StoreMessage. onDurable fires exactly once, after the fsync.
func (ds *DisruptorStorage) StoreMessageAsync(queueName string, message *protocol.Message, onDurable func(error)) error {
	if ds.wal == nil {
		return fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)
	}

	ring := ds.getOrCreateQueueRing(queueName)

	ds.wal.WriteAsync(queueName, message, message.DeliveryTag, func(err error) {
		if err == nil {
			// Durable: make the message ring-resident BEFORE onDurable advances
			// the frontier. Spill decision as in StoreMessage (over the threshold
			// the durable message is WAL-only and read back via wal.Read). ring
			// ops are lock-free CAS — safe on the WAL batch-writer goroutine.
			if ring.ring.Count() <= ds.spillThreshold {
				if _, spilled, serr := ring.ring.Store(message.DeliveryTag, message); serr == nil && !spilled {
					ring.ack.OnPublish(message.DeliveryTag)
				}
			}
		}
		onDurable(err)
	})
	return nil
}

// StoreSharedAsync is the ITER5 fused sibling of StoreMessageAsync for a durable
// fan-out of one large body to N>=2 queues. It routes the N durable writes
// through a SINGLE WAL group-commit unit (one BodyBlock carrying the body once +
// N reference records, one fsync), instead of N full-body StoreMessageAsync
// writes. Each sub's per-copy completion performs THAT queue's ring-residency +
// spill decision exactly as StoreMessageAsync's completion does, then invokes the
// sub's OnDurable (the broker's per-copy visibility/confirm step) — so a fan-out
// of N copies drives N ordinary completions, and correctness/visibility are
// identical to the per-queue path; only the durable byte layout differs.
//
// The shared body is every sub's Message.Body (the broker shares one Body slice
// across fan-out copies), taken from the first sub. Completions run on the shared
// WAL batch-writer goroutine and must be non-blocking (invariant A4), which the
// broker's completions honor.
func (ds *DisruptorStorage) StoreSharedAsync(subs []interfaces.SharedDurableSub) error {
	if ds.wal == nil {
		return fmt.Errorf("cannot persist durable message: WAL unavailable")
	}
	if len(subs) == 0 {
		return nil
	}

	body := subs[0].Message.Body
	walSubs := make([]sharedSub, len(subs))
	for i := range subs {
		ring := ds.getOrCreateQueueRing(subs[i].QueueName)
		msg := subs[i].Message
		onDurable := subs[i].OnDurable
		walSubs[i] = sharedSub{
			queueName: subs[i].QueueName,
			offset:    msg.DeliveryTag,
			message:   msg,
			onDone: func(err error) {
				if err == nil {
					// Ring-residency BEFORE the broker advances this queue's
					// visibility, mirroring StoreMessageAsync exactly.
					if ring.ring.Count() <= ds.spillThreshold {
						if _, spilled, serr := ring.ring.Store(msg.DeliveryTag, msg); serr == nil && !spilled {
							ring.ack.OnPublish(msg.DeliveryTag)
						}
					}
				}
				onDurable(err)
			},
		}
	}
	return ds.wal.WriteSharedAsync(walSubs, body, uint32(len(walSubs)))
}

func (ds *DisruptorStorage) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}

	if msg, ok := ring.ring.LoadByTag(deliveryTag); ok {
		return msg, nil
	}

	if ring.readAhead != nil {
		if msg, ok := ring.readAhead.get(deliveryTag); ok {
			return msg, nil
		}
	}

	if ds.wal != nil {
		if ring.readAhead != nil {
			if batch, err := ds.wal.ReadBatch(queueName, deliveryTag); err == nil && len(batch) > 0 {
				ring.readAhead.put(batch)
				if msg, ok := batch[deliveryTag]; ok {
					return msg, nil
				}
			}
		}
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

func (ds *DisruptorStorage) DeleteMessage(queueName string, deliveryTag uint64) error {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return interfaces.ErrQueueNotFound
	}

	// Check if message is in the ring and whether it's durable.
	// Durable messages (DeliveryMode=2) are always written to WAL on publish,
	// so they need WAL ack regardless. Transient messages only need WAL ack
	// if they were spilled (not in ring).
	msg, foundInRing := ring.ring.LoadByTag(deliveryTag)
	isDurable := foundInRing && msg.DeliveryMode == 2

	ring.ring.Delete(deliveryTag)

	if !foundInRing || isDurable {
		if ds.wal != nil {
			ds.wal.Acknowledge(queueName, deliveryTag)
		}
		if ds.segments != nil {
			ds.segments.Acknowledge(queueName, deliveryTag)
		}
	}

	return nil
}

// DeleteMessageIfPresent removes a message and reports whether THIS call won the
// ring removal — AtomicRing.Delete's CAS result. It underpins the SQ-9 TTL
// reaper / delivery head-check linearization: for a ring-resident tag exactly
// one of many concurrent callers observes true, so queue depth is decremented
// exactly once even when the reaper and a consumer race the same waiting
// message. WAL/segment acknowledgement mirrors DeleteMessage.
//
// A tag that is NOT ring-resident (a rare spilled/WAL-only message) reports
// false: there is no ring CAS to arbitrate. Callers gate their depth decrement
// on true, so such a message is removed from durable storage but its depth is
// reconciled by the storage-synced min-ack path rather than the reaper — a
// bounded divergence documented for the >64K-unacked spill edge in W4.
func (ds *DisruptorStorage) DeleteMessageIfPresent(queueName string, deliveryTag uint64) (bool, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return false, interfaces.ErrQueueNotFound
	}

	msg, foundInRing := ring.ring.LoadByTag(deliveryTag)
	isDurable := foundInRing && msg.DeliveryMode == 2

	removed := ring.ring.Delete(deliveryTag)

	if !foundInRing || isDurable {
		if ds.wal != nil {
			ds.wal.Acknowledge(queueName, deliveryTag)
		}
		if ds.segments != nil {
			ds.segments.Acknowledge(queueName, deliveryTag)
		}
	}

	return removed, nil
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

func (ds *DisruptorStorage) StoreExchangeBinding(source, destination, routingKey string, arguments map[string]interface{}) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreExchangeBinding(source, destination, routingKey, arguments)
}

func (ds *DisruptorStorage) DeleteExchangeBinding(source, destination, routingKey string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteExchangeBinding(source, destination, routingKey)
}

func (ds *DisruptorStorage) GetExchangeBindingsFrom(source string) ([]*interfaces.ExchangeBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchangeBindingsFrom(source)
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
	// Hold the tag's shard across both map updates so the two-phase write is
	// atomic w.r.t. any DeletePendingAck of the same tag. Index first, then
	// primary: under the lock this preserves the index-superset invariant.
	lock := ds.pendingAckLock(pendingAck.DeliveryTag)
	lock.Lock()
	defer lock.Unlock()
	inner, _ := ds.consumerPendingAcks.LoadOrStore(pendingAck.ConsumerTag, &sync.Map{})
	inner.(*sync.Map).Store(pendingAck.DeliveryTag, pendingAck)
	ds.pendingAcks.Store(pendingAck.DeliveryTag, pendingAck)
	return nil
}

func (ds *DisruptorStorage) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	if val, ok := ds.pendingAcks.Load(deliveryTag); ok {
		return val.(*protocol.PendingAck), nil
	}
	return nil, interfaces.ErrPendingAckNotFound
}

func (ds *DisruptorStorage) DeletePendingAck(queueName string, deliveryTag uint64) error {
	// Hold the tag's shard across both map updates so the two-phase delete is
	// atomic w.r.t. any StorePendingAck of the same tag. Primary first, then
	// index: LoadAndDelete atomically retrieves the PendingAck (to learn its
	// consumer tag) and removes it from the primary; only then do we delete
	// from that consumer's index, keeping the index a superset at all times.
	// If the primary entry is already gone, this is a no-op and the index is
	// left untouched (a stale extra entry is benign and reaped on a later
	// delete of a re-stored tag, which is serialized on the same shard).
	lock := ds.pendingAckLock(deliveryTag)
	lock.Lock()
	defer lock.Unlock()
	val, ok := ds.pendingAcks.LoadAndDelete(deliveryTag)
	if !ok {
		return nil
	}
	pa := val.(*protocol.PendingAck)
	if inner, ok := ds.consumerPendingAcks.Load(pa.ConsumerTag); ok {
		inner.(*sync.Map).Delete(deliveryTag)
	}
	return nil
}

// pendingAckLock returns the striped mutex that serializes all pending-ack
// index updates for a given delivery tag (see consumerPendingAcks doc).
func (ds *DisruptorStorage) pendingAckLock(deliveryTag uint64) *sync.Mutex {
	return &ds.pendingAckShards[deliveryTag&(pendingAckShardCount-1)]
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
	var result []*protocol.PendingAck
	if inner, ok := ds.consumerPendingAcks.Load(consumerTag); ok {
		inner.(*sync.Map).Range(func(_, val interface{}) bool {
			result = append(result, val.(*protocol.PendingAck))
			return true
		})
	}
	return result, nil
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
	messagesByQueue := make(map[string][]*protocol.Message)

	if ds.wal != nil {
		recoveredMessages, err := ds.wal.RecoverFromWAL()
		if err != nil {
			return nil, fmt.Errorf("WAL recovery failed: %w", err)
		}

		for _, recoveryMsg := range recoveredMessages {
			messagesByQueue[recoveryMsg.QueueName] = append(
				messagesByQueue[recoveryMsg.QueueName],
				recoveryMsg.Message,
			)
		}
	}

	if ds.segments != nil {
		segmentMessages, err := ds.segments.RecoverFromSegments()
		if err != nil {
			return nil, fmt.Errorf("segment recovery failed: %w", err)
		}

		walKeys := make(map[string]map[uint64]bool)
		for queueName, msgs := range messagesByQueue {
			if walKeys[queueName] == nil {
				walKeys[queueName] = make(map[uint64]bool)
			}
			for _, msg := range msgs {
				walKeys[queueName][msg.DeliveryTag] = true
			}
		}

		for queueName, segMsgs := range segmentMessages {
			for _, rm := range segMsgs {
				if walKeys[queueName] != nil && walKeys[queueName][rm.Offset] {
					continue
				}
				messagesByQueue[queueName] = append(
					messagesByQueue[queueName],
					rm.Message,
				)
			}
		}
	}

	return messagesByQueue, nil
}

func (ds *DisruptorStorage) MarkRecoveryComplete(stats *protocol.RecoveryStats) error {
	return nil
}

// ExecuteAtomic runs operations as one all-or-nothing unit (SQ-8).
//
// The callback is handed a staging storage view that BUFFERS every StoreMessage
// (publish) and DeleteMessage (settlement) rather than applying it. If the
// callback returns an error, the buffer is dropped and nothing is applied — no
// partial publishes land in a queue, no partial settlements take effect. If the
// callback succeeds, the buffered set is committed atomically: durable publishes
// are written to the WAL bracketed by transaction-boundary markers with a single
// fsync (so recovery is all-or-nothing across a crash), then ring state and
// settlements are applied.
//
// Commits are serialized on txMutex; this is the deliberate slow path and does
// not touch the WAL group-commit hot path used by ordinary publishes.
func (ds *DisruptorStorage) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	staging := &txStagingStorage{DisruptorStorage: ds}
	if err := operations(staging); err != nil {
		// Roll back: nothing buffered has been applied.
		return err
	}
	return staging.commit()
}

func (ds *DisruptorStorage) SaveDeliveryTagCounter(tag uint64) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.SaveDeliveryTagCounter(tag)
}

func (ds *DisruptorStorage) LoadDeliveryTagCounter() (uint64, error) {
	if ds.metadataStore == nil {
		return 0, nil
	}
	return ds.metadataStore.LoadDeliveryTagCounter()
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
