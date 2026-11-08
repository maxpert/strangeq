package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	// pendingAckShardCount is the number of striped locks used to make
	// StorePendingAck and DeletePendingAck mutually exclusive for a given
	// delivery tag. It is a power of two so deliveryTag&mask selects a shard.
	pendingAckShardCount = 256
)

type StorageMetrics interface {
	UpdateDiskMetrics(freeBytes, usedBytes float64)
	// RecordHotCacheDeliveryHit counts ONE GetMessageForDelivery served from
	// the hot cache (no cold-tier read).
	RecordHotCacheDeliveryHit(queue string)
	// RecordHotCacheDeliveryColdRead counts ONE GetMessageForDelivery that
	// missed the hot cache and took a cold-tier read (WAL or segment).
	RecordHotCacheDeliveryColdRead(queue string)
	// RecordHotCacheDeliveryPread counts positional reads issued by the
	// delivery cold path (the WAL parse uses one header read plus one payload
	// read per record, plus optionally one shared-body-block read). The
	// per-delivered-message ratio is RecordHotCacheDeliveryPread / deliveries.
	RecordHotCacheDeliveryPread(queue string, n int)
	WALMetrics
	SegmentMetrics
}

// recordHotCacheDeliveryHit / ...ColdRead / ...Pread update the in-process
// counters (readable by tests and by DeliveryCacheStats) and, when a collector
// is bound, the exported Prometheus series. The in-process atomics are always
// maintained so the classification is observable without a metrics collector.
func (ds *StorageEngine) recordHotCacheDeliveryHit(queue string) {
	ds.deliveryHits.Add(1)
	if ds.metrics != nil {
		ds.metrics.RecordHotCacheDeliveryHit(queue)
	}
}

func (ds *StorageEngine) recordHotCacheDeliveryColdRead(queue string) {
	ds.deliveryColdReads.Add(1)
	if ds.metrics != nil {
		ds.metrics.RecordHotCacheDeliveryColdRead(queue)
	}
}

func (ds *StorageEngine) recordHotCacheDeliveryPread(queue string, n int) {
	if n <= 0 {
		return
	}
	ds.deliveryPreads.Add(uint64(n))
	if ds.metrics != nil {
		ds.metrics.RecordHotCacheDeliveryPread(queue, n)
	}
}

// DeliveryCacheStats returns the delivery-path classification counters since
// process start: (hot-cache hits, cold reads, cold-path positional reads).
// DIAGNOSTIC ONLY (Task 2): the hot-hit ratio is hits/(hits+coldReads) and
// preads per delivered message is preads/deliveries.
func (ds *StorageEngine) DeliveryCacheStats() (hits, coldReads, preads uint64) {
	return ds.deliveryHits.Load(), ds.deliveryColdReads.Load(), ds.deliveryPreads.Load()
}

type StorageEngine struct {
	queues   sync.Map
	queuesMu sync.Mutex

	hot           *hotCache
	metadataStore *PersistentMetadataStore
	wal           *WALManager
	segments      *SegmentManager

	transactions map[string]*interfaces.Transaction
	txMutex      sync.RWMutex

	pendingAcks sync.Map

	// consumerPendingAcks is a secondary index over pendingAcks keyed by the
	// broker-internal consumer identity (protocol.Consumer.ID, carried in the
	// PendingAck.ConsumerTag field, which keeps its name for on-disk
	// compatibility) -> *sync.Map{ deliveryTag -> *PendingAck }. It is NOT
	// keyed by the client-visible wire tag: AMQP 0-9-1 scopes that tag per
	// channel, so two consumers may legitimately share one.
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

	// Delivery-path classification counters (Task 2; diagnostic). Always
	// maintained so the cold-read fix is observable without a collector bound.
	deliveryHits      atomic.Uint64
	deliveryColdReads atomic.Uint64
	deliveryPreads    atomic.Uint64
}

type QueueHot struct {
	name   string
	ack    *AckCursor
	closed atomic.Bool
}

func NewStorageEngine() (*StorageEngine, error) {
	return NewStorageEngineWithDataDir("./data")
}

func NewStorageEngineWithDataDir(dataDir string) (*StorageEngine, error) {
	return NewStorageEngineWithConfig(dataDir, interfaces.EngineConfig{})
}

func WALConfigFromEngine(ec interfaces.EngineConfig) WALConfig {
	cfg := DefaultWALConfig()
	// Unconditional copy is safe: both zero values mean fsync ON (durable) and
	// CRC ON (safe).
	cfg.SyncDisabled = ec.WALSyncDisabled
	cfg.CRCDisabled = ec.WALCRCDisabled
	cfg.UnsafeRecovery = ec.UnsafeRecovery
	// ConfirmSync: the zero value (walConfirmBarrier) is the default, so any
	// value other than the explicit "force_flush" — including the empty string
	// of a zero-value EngineConfig{} — keeps the barrier. This mirrors the
	// zero-value-means-safe contract of the fields above.
	if ec.CrashModel == interfaces.CrashModelForceFlush {
		cfg.ConfirmSync = walConfirmForceFlush
	}
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
	if ec.CompactionIntervalMS > 0 {
		cfg.CompactionInterval = time.Duration(ec.CompactionIntervalMS) * time.Millisecond
	}
	if ec.SharedBodyMaxPinAgeMS > 0 {
		cfg.SharedBodyMaxPinAge = time.Duration(ec.SharedBodyMaxPinAgeMS) * time.Millisecond
	}
	// Only a POSITIVE engine value overrides the default: 0 would mean
	// "compact as soon as anything is dead", which is not what an unset
	// engine threshold means.
	if ec.CompactionThreshold > 0 {
		cfg.CompactionThreshold = ec.CompactionThreshold
	}
	return cfg
}

func SegmentConfigFromEngine(ec interfaces.EngineConfig) SegmentConfig {
	cfg := DefaultSegmentConfig()
	cfg.CRCDisabled = ec.WALCRCDisabled
	// ConfirmSync: the zero value (walConfirmBarrier) is the default, so any
	// value other than the explicit "force_flush" keeps the barrier. Mirrors
	// WALConfigFromEngine.
	cfg.ConfirmSync = confirmSyncFromCrashModel(ec.CrashModel)
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

func NewStorageEngineWithConfig(dataDir string, engineCfg interfaces.EngineConfig) (*StorageEngine, error) {
	// review-4 N-4. Every recovery fault names its artifact by path, and the
	// refusal message instructs the operator to "move the listed files OUT OF
	// THE DIRECTORY" — advice that is only actionable if the path is absolute.
	// The tests asserted filepath.IsAbs and passed because t.TempDir() is
	// absolute; there was no filepath.Abs anywhere in production code, and
	// config.DefaultConfig() ships Storage.Path: "./data". So at SHIPPED
	// DEFAULTS every artifact in a refusal message was relative to whatever
	// directory the process happened to start in. Absolutised ONCE, here, at the
	// single point every tier's dataDir flows from (canon rule 12).
	//
	// A failure here is REPORTED, not swallowed. filepath.Abs fails only when
	// the process has no working directory (its cwd was removed), and the old
	// no-else form fell back silently to the relative path — reinstating, in
	// the one place that fixes it, exactly the condition the paragraph above
	// says makes every refusal message unactionable (review-5 MINOR-10).
	abs, aerr := filepath.Abs(dataDir)
	if aerr != nil {
		return nil, fmt.Errorf("the storage data directory %q could not be resolved to an absolute path, "+
			"so no recovery fault could name its artifacts actionably: %w", dataDir, aerr)
	}
	dataDir = abs

	hotCacheQueueBytes := engineCfg.HotCacheQueueBytes
	if hotCacheQueueBytes <= 0 {
		// The shipped default. The per-queue ready set must not be SMALLER
		// than the 52,428-message ring it replaces: an LRU evicts the OLDEST
		// entry, which is exactly a FIFO consumer's next target, so a
		// too-small ready set turns nearly every read into a cold read.
		//
		// THIS IS THE EFFECTIVE VALUE the configuration validation compares
		// the global budget against, so the two live in one place.
		hotCacheQueueBytes = DefaultHotCacheQueueBytes
	}
	hotCacheGlobalBytes := engineCfg.HotCacheGlobalBytes
	if hotCacheGlobalBytes <= 0 {
		hotCacheGlobalBytes = DefaultHotCacheGlobalBytes
	}
	metadataStore, err := NewPersistentMetadataStoreWithCrashModel(dataDir, engineCfg.CrashModel)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
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

	return &StorageEngine{
		hot:           newHotCacheWithExemption(hotCacheQueueBytes, hotCacheGlobalBytes, hotCachePaceExemptsPendingDurable(walCfg, engineCfg)),
		metadataStore: metadataStore,
		wal:           walManager,
		segments:      segmentManager,
		transactions:  make(map[string]*interfaces.Transaction),
		dataDir:       dataDir,
	}, nil
}

// hotCachePaceExemptsPendingDurable reports whether the hot cache exempts
// NOT-YET-DURABLE bodies (up to queueBytes/8) from the pace budget.
//
// IT IS TRUE ONLY IN THE BARRIER CONFIGURATION: the exemption exists because in
// that mode a body whose covering fence has not returned occupies budget that
// the deliverable backlog needs. With storage.fsync DISABLED there is no
// barrier and every admitted body is durable at once, so the exemption is OFF
// and the charging is byte-identical to before. Under crash_model: force_flush
// it is OFF too -- the pace gate is not wired there at all (the full-flush
// makes batch size the limiter), and the charging must stay unchanged.
func hotCachePaceExemptsPendingDurable(walCfg WALConfig, ec interfaces.EngineConfig) bool {
	return !walCfg.SyncDisabled && ec.CrashModel != interfaces.CrashModelForceFlush
}

func (ds *StorageEngine) SetMetrics(metrics StorageMetrics) {
	ds.metrics = metrics
	if ds.wal != nil {
		ds.wal.SetMetrics(metrics)
	}
	if ds.segments != nil {
		ds.segments.SetMetrics(metrics)
	}
}

// HotCachePaceGate returns the publisher backpressure gate the broker arms to
// pace a producer to this queue's hot-cache BYTE budget, or nil when the queue
// has room for one more message the size of m.
//
// IT IS THE D-B CREDIT SIGNAL (Task 2). The returned gate reports
// AtHighWaterMark while the queue's resident bytes plus this message's charge
// exceed the per-queue budget; the broker's reader then stops pulling the
// producer's publishes off the socket (TCP backpressure) until an
// acknowledgement frees the room, keeping the backlog inside the hot cache so
// the delivered head is served from memory rather than by a cold WAL pread.
//
// A message that is LARGER than the whole per-queue budget can never be
// admitted (the documented "oversized durable publish succeeds disk-only"
// path); such a message must NOT arm a gate, because that gate would never
// clear and the producer would park forever.
func (ds *StorageEngine) HotCachePaceGate(queue string, m *protocol.Message) protocol.DepthGate {
	if ds.hot == nil || m == nil {
		return nil
	}
	charge := cacheCharge(queue, m)
	if charge <= 0 || charge > ds.hot.queueBytes {
		return nil
	}
	if ds.hot.paceEffectiveResident(queue)+charge <= ds.hot.queueBytes {
		return nil // room: no gate, and no allocation on the common fast path
	}
	return &hotCacheByteGate{c: ds.hot, queue: queue, charge: charge}
}

// HotCacheResidentBytes reports the queue's resident hot-cache bytes (the
// observable projection of the queue's own accounting). Diagnostic only.
func (ds *StorageEngine) HotCacheResidentBytes(queue string) int64 {
	if ds.hot == nil {
		return 0
	}
	return ds.hot.residentBytes(queue)
}

// HotCacheQueueByteBudget reports the effective per-queue hot-cache byte budget.
// Diagnostic only.
func (ds *StorageEngine) HotCacheQueueByteBudget() int64 {
	if ds.hot == nil {
		return 0
	}
	return ds.hot.queueBytes
}

// WALBookkeepingStats exposes the WAL tier's in-memory bookkeeping for the
// telemetry scrape (WS-MEMORY-CUT §8). Diagnostic only; never gated. Returns
// zeroes when the WAL tier is not built.
func (ds *StorageEngine) WALBookkeepingStats() (inventoryRecords, indexCopies int, drained uint64) {
	if ds.wal == nil {
		return 0, 0, 0
	}
	return ds.wal.WALBookkeepingStats()
}

func (ds *StorageEngine) getQueueHot(queueName string) *QueueHot {
	if val, ok := ds.queues.Load(queueName); ok {
		return val.(*QueueHot)
	}
	return nil
}

func (ds *StorageEngine) getOrCreateQueueHot(queueName string) *QueueHot {
	if ring, ok := ds.queues.Load(queueName); ok {
		return ring.(*QueueHot)
	}

	ds.queuesMu.Lock()
	defer ds.queuesMu.Unlock()

	if ring, ok := ds.queues.Load(queueName); ok {
		return ring.(*QueueHot)
	}

	ring := &QueueHot{
		name: queueName,
		ack:  NewAckCursor(),
	}

	ds.queues.Store(queueName, ring)
	return ring
}

func (ds *StorageEngine) LoadMessageFromRecovery(queueName string, message *protocol.Message) error {
	queue := ds.getOrCreateQueueHot(queueName)
	if ds.hot.Put(queueName, message, true) {
		queue.ack.OnPublish(message.DeliveryTag)
	}
	return nil
}

func (ds *StorageEngine) StoreMessage(queueName string, message *protocol.Message) error {
	if ds.wal == nil && message.DeliveryMode == 2 {
		return fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)
	}

	queue := ds.getOrCreateQueueHot(queueName)
	if message.DeliveryMode == 2 {
		if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
			return fmt.Errorf("failed to write durable message to WAL: %w", err)
		}
		if ds.hot.Put(queueName, message, true) {
			queue.ack.OnPublish(message.DeliveryTag)
		}
		return nil
	}

	if ds.hot.Put(queueName, message, false) {
		queue.ack.OnPublish(message.DeliveryTag)
		return nil
	}
	if ds.wal == nil {
		return fmt.Errorf("hot cache admission failed and WAL unavailable for queue %s", queueName)
	}
	if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
		return fmt.Errorf("failed to spill transient message to WAL: %w", err)
	}
	return nil
}

// StoreMessageAsync durably writes before making the message visible.
//
// THE BODY IS ADMITTED TO THE HOT CACHE AT THE WRITE, NOT AT THE COMPLETION.
// The hot cache is a RAM HOLDER, not the visibility gate: the moment the WAL
// write returns, the record's bytes are in the file and are re-readable by
// this process, so the cache can take the body for delivery.
//
// THE ADMITTED ENTRY IS NOT YET DURABLE. The record's covering fdatasync has
// NOT returned at the write, so the entry is admitted NOT-DURABLE: it is
// backed (the body is in the file) but it must not be served, counted or
// listed, exactly like a message that is not in the WAL yet. The DURABILITY
// POINT is the completion (after the fdatasync): it marks the entry durable,
// and only then does the queue DEPTH — the gate every consumer's claim goes
// through — advance. So a consumer is never handed a message that is not yet
// durable.
//
// A record whose fdatasync FAILS is not durable: the completion removes the
// cache entry the write admitted, so a non-durable message is never
// delivered from RAM.
//
// onWritten fires at the write (the RAM-admission point) and onDurable fires
// after the covering fdatasync (the durability point).
func (ds *StorageEngine) StoreMessageAsync(queueName string, message *protocol.Message, onWritten func(), onDurable func(error)) error {
	if ds.wal == nil {
		return fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)
	}
	queue := ds.getOrCreateQueueHot(queueName)
	tag := message.DeliveryTag
	// admitted is written by the write hook and read by the completion
	// callback. Both run on the shared WAL committer goroutine, in that
	// order, so the handoff needs no synchronization.
	//
	// entry is the hot-cache entry the write admitted. The completion marks
	// IT durable, which takes no lock (the flag is an atomic): the
	// committer goroutine is the broker's serialisation point for every
	// publish, and a second queue-lock acquisition there would contend with
	// the consumers' delivery and acknowledgement paths.
	var entry *hotCacheEntry
	admitted := false
	ds.wal.WriteAsyncObserved(queueName, message, tag, func() {
		// NOT YET DURABLE: the record's bytes are in the file, but the
		// covering fdatasync has not returned. The entry is backed (the body
		// is re-readable from the file) but it must not be served until the
		// completion marks it durable.
		var ok bool
		entry, ok = ds.hot.PutBestEffortEntry(queueName, message, true, false)
		admitted = ok
		if onWritten != nil {
			onWritten()
		}
	}, func(err error) {
		if err == nil {
			if admitted {
				// THE DURABILITY POINT: the covering fdatasync returned.
				// The entry becomes servable, and only now does the queue
				// depth advance.
				ds.hot.MarkDurableEntry(entry)
				queue.ack.OnPublish(tag)
			}
		} else {
			// The record is NOT durable: the body must not stay in the hot
			// cache, or a consumer could be handed a message the broker
			// never made durable.
			ds.hot.Delete(queueName, tag)
		}
		if onDurable != nil {
			onDurable(err)
		}
	})
	return nil
}

// StoreSharedAsync shares one durable body write across a fan-out. Each
// completion performs only cache admission before reporting durability.
func (ds *StorageEngine) StoreSharedAsync(subs []interfaces.SharedDurableSub) error {
	if ds.wal == nil {
		return fmt.Errorf("cannot persist durable message: WAL unavailable")
	}
	if len(subs) == 0 {
		return nil
	}

	body := subs[0].Message.Body
	walSubs := make([]sharedSub, len(subs))
	for i := range subs {
		queue := ds.getOrCreateQueueHot(subs[i].QueueName)
		msg := subs[i].Message
		onDurable := subs[i].OnDurable
		walSubs[i] = sharedSub{
			queueName: subs[i].QueueName,
			offset:    msg.DeliveryTag,
			message:   msg,
			onDone: func(err error) {
				if err == nil && ds.hot.Put(queue.name, msg, true) {
					queue.ack.OnPublish(msg.DeliveryTag)
				}
				onDurable(err)
			},
		}
	}
	return ds.wal.WriteSharedAsync(walSubs, body, uint32(len(walSubs)))
}

// GetMessage reads a message WITHOUT admitting it into the hot cache.
//
// IT IS A PEEK. The hot cache holds PUBLISHED messages, and
// GetQueueMessages / GetQueueMessageCount are hot-resident operations over
// exactly that set (WI3: "Range/count methods remain hot-resident operations
// AS TODAY" — the legacy ring only ever held published records). A read that
// admitted what it read would turn a READ into a PUBLISH as far as the count
// was concerned: the TTL reaper probes every ready tag on every sweep
// (broker/queue_reaper.go, reapTTLSweep), and each probe would insert a
// durable record into the cache and inflate the count it then reports.
//
// The delivery path uses GetMessageForDelivery, which DOES admit what it
// read (the write-through admission is attributable to a CONSUMER DELIVERY,
// not to a probe).
//
// The acknowledgement re-check is unaffected by the missing admission: it
// asks the WAL whether the tag was acknowledged between the tier read and
// now, and needs no cache state to do it.
func (ds *StorageEngine) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	queue := ds.getQueueHot(queueName)
	if queue == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	if msg, _, ok := ds.hot.Get(queueName, deliveryTag); ok {
		return msg, nil
	}
	if ds.wal != nil && ds.wal.IsAcknowledged(queueName, deliveryTag) {
		return nil, interfaces.ErrMessageNotFound
	}

	msg, err := ds.readColdMessageCounted(queueName, deliveryTag, false)
	if err != nil {
		return nil, err
	}

	// The acknowledgement re-check: the tag may have been acknowledged
	// between the tier read and now. It asks the WAL whether the tag was
	// acknowledged and needs no cache state to do it.
	if ds.wal != nil && ds.wal.IsAcknowledged(queueName, deliveryTag) {
		return nil, interfaces.ErrMessageNotFound
	}
	return msg, nil
}

// GetMessageForDelivery reads a message for a CONSUMER DELIVERY and admits
// what it read into the hot cache (the write-through admission).
//
// THE ADMISSION IS ATTRIBUTABLE TO A DELIVERY. A read that is not a delivery
// (a probe, a peek, an existence check) must not admit: the hot cache's
// budget is spent on the working set the consumers are actually reading, and
// the reported message count must not count a message that no consumer asked
// for. The non-delivery reads use GetMessage, which does not admit.
//
// THE ADMISSION IS ATOMIC WITH THE ACKNOWLEDGEMENT. The reader takes the
// same per-tag shard lock the acknowledgement paths take
// (coldRemovalShards), and holds it across
//
//	(acknowledgement re-check + admission)
//
// so an acknowledgement that lands while the reader is between its read and
// its admission can never leave an ACKNOWLEDGED tag resident in the hot
// cache (see the ack paths: DeleteMessage and DeleteMessageIfPresentDetailed).
//
// NO LOCK IS HELD ACROSS THE COLD READ: the disk read happens first, and the
// shard lock is taken only for the (check + admit).
//
// THE ADMISSION IS ALSO GUARDED AGAINST THE RANGE REMOVALS (the range purge
// and the queue deletion). Those settle a BAND of tags in the COLD tiers and
// they do NOT take the per-tag shard lock — they cannot, they settle many
// tags at once — so a reader parked between its cold read and its admission
// could re-admit a tag the range removal had just settled.
//
// So the reader CAPTURES the queue's incarnation (its QueueHot handle) and
// the hot cache's ADMISSION EPOCH immediately before the cold read, and the
// admission validates BOTH under the queue's own lock (see
// putBestEffortIfCurrent). A reader whose capture is stale admits NOTHING:
// a settled message cannot come back to the hot cache, be counted by the
// queue's reported message count, or be served from there again.
func (ds *StorageEngine) GetMessageForDelivery(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	queue := ds.getQueueHot(queueName)
	if queue == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	if msg, _, ok := ds.hot.Get(queueName, deliveryTag); ok {
		ds.recordHotCacheDeliveryHit(queueName)
		return msg, nil
	}
	if ds.wal != nil && ds.wal.IsAcknowledged(queueName, deliveryTag) {
		return nil, interfaces.ErrMessageNotFound
	}

	// THE CAPTURE. The queue's incarnation and the hot cache's admission
	// epoch are captured IMMEDIATELY BEFORE the cold read, so the admission
	// that follows the read can tell whether the queue was swept or purged
	// while the read was in flight.
	//
	// The handle is re-read here (it was already read above for the
	// existence check) so the capture is as close to the cold read as the
	// code allows: a queue deleted between the two reads is caught here.
	captured := ds.getQueueHot(queueName)
	if captured == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	capturedEpoch, hotQueue := ds.hot.captureAdmission(queueName)

	// THE COLD READ. No lock is held across it. This is a DELIVERY, so the
	// positional reads it issues are attributed to the delivery metric.
	ds.recordHotCacheDeliveryColdRead(queueName)
	msg, err := ds.readColdMessageCounted(queueName, deliveryTag, true)
	if err != nil {
		return nil, err
	}

	// THE ADMISSION. The per-tag shard lock makes the (acknowledgement
	// re-check + admission) atomic with respect to the acknowledgement
	// paths, which take the same lock.
	mu := &coldRemovalShards[deliveryTag&63]
	mu.Lock()
	defer mu.Unlock()

	// The acknowledgement re-check, UNDER THE LOCK: an acknowledgement that
	// landed while the cold read was in flight must refuse the message, and
	// it must NOT be admitted.
	if ds.wal != nil && ds.wal.IsAcknowledged(queueName, deliveryTag) {
		return nil, interfaces.ErrMessageNotFound
	}

	// THE SEAM BETWEEN THE RE-CHECK AND THE ADMISSION. TEST-ONLY observer:
	// it lets a test force an acknowledgement into exactly this window.
	//
	// IT RUNS WHILE THE PER-TAG SHARD LOCK IS HELD, so a hook must not take
	// that lock itself (it would deadlock); issue the acknowledgement from
	// another goroutine instead.
	coldReadAdmissionFire(queueName, deliveryTag)

	// THE ADMISSION IS VALIDATED BEFORE IT RESERVES ANYTHING. The admission
	// is BEST-EFFORT: it never evicts anything to make room for itself, so
	// a full cache simply leaves the message disk-only (the next read is a
	// cold read again) and nothing else changes.
	//
	// It is backed=true: the body is BACKED BY A READABLE COLD COPY IN THE
	// WAL OR THE SEGMENT TIER, so evicting the entry later is safe (the body
	// can be read back from the cold tiers). A segment-only cold read is
	// admitted with backed=true as well, even though its WAL copy may
	// already have been reclaimed: the segment tier is a cold copy too, and
	// that is SAFE for the current eviction, readback and acknowledgement
	// behaviour.
	//
	// It is durable=TRUE: the WAL's index is published only after the
	// covering fdatasync, so a WAL read can only ever find durable
	// messages.
	if err := ds.putBestEffortIfCurrent(queueName, captured, hotQueue, capturedEpoch, msg); err != nil {
		// AN EPOCH MISMATCH IS NOT AN ERROR THE CALLER SEES: the message was
		// read successfully, and the purge that moved the epoch may have been
		// aimed at a DIFFERENT band, so failing the delivery would be a
		// spurious delivery error. The message is returned, unadmitted.
		if errors.Is(err, errStaleAdmissionEpoch) {
			return msg, nil
		}
		return nil, err
	}
	return msg, nil
}

// errStaleAdmissionEpoch reports that the hot cache's admission epoch for the
// queue MOVED between the delivery reader's capture and its admission.
//
// IT IS NOT AN ERROR THE CALLER SEES: the reader returns the message it read
// and admits nothing (see GetMessageForDelivery).
var errStaleAdmissionEpoch = errors.New("storage: the hot cache's admission epoch moved")

// putBestEffortIfCurrent admits msg into the hot cache for queueName, but
// ONLY IF the reader's capture is still CURRENT.
//
// THE CAPTURE IS (the queue's incarnation, the hot cache's admission epoch).
// The incarnation is the queue's QueueHot handle: the hot cache keys its
// per-queue state by NAME and never removes it, so a deleted queue's
// successor inherits the deleted queue's state, and only the handle tells
// the two apart. The epoch is the hot cache's per-queue admission counter,
// which the range removals advance (see hotCacheQueue.admissionEpoch).
//
// THE VALIDATION AND THE ADMISSION ARE ATOMIC. The queue's lock is held
// across both, so a range removal that starts after the validation waits for
// the admission and then removes it (that ordering is acceptable and is
// documented on hotCache.DeleteRange), and a range removal that finished
// before the validation has already advanced the epoch, so the validation
// refuses the admission.
//
// EVERY VALIDATION HAPPENS BEFORE ANY MUTATION: before the global
// reservation, before the map/LRU mutation and before the accounting. A
// refused admission therefore needs NO ROLLBACK.
//
// THE OUTCOMES:
//
//   - the epoch moved (errStaleAdmissionEpoch): the message is NOT admitted
//     and the caller returns it to its caller WITHOUT an error — the purge
//     may have been aimed at a different band, and failing the delivery
//     would be a spurious delivery error;
//   - the incarnation moved, or the queue is closed
//     (interfaces.ErrQueueNotFound): the queue is gone (or is a different
//     incarnation), so the delivery fails and nothing is admitted.
func (ds *StorageEngine) putBestEffortIfCurrent(queueName string, captured *QueueHot, q *hotCacheQueue, epoch uint64, msg *protocol.Message) error {
	if msg == nil {
		return nil
	}

	// THE CHARGE IS COMPUTED OUTSIDE THE LOCK: it walks the message's
	// headers, and it does not depend on the cache's state.
	charge := cacheCharge(queueName, msg)

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	// THE INCARNATION CHECK COMES FIRST. A queue that was deleted (and
	// possibly redeclared) while the read was in flight must NOT deliver the
	// old message: the old queue's messages are settled, and the successor
	// is a different queue.
	//
	// IT COMES FIRST BECAUSE A DELETION MOVES BOTH. The deletion advances
	// the epoch (its DeleteRange) AND replaces the incarnation, so the two
	// checks would both fail and the two outcomes would contradict each
	// other. The incarnation is the STRONGER statement: the queue is gone,
	// so the delivery fails with ErrQueueNotFound, while a pure purge (the
	// incarnation is intact) is the epoch mismatch that returns the message
	// without an error.
	//
	// The check is cheap (a sync.Map load) and it is done under the queue's
	// lock, so it cannot be invalidated before the admission lands.
	if ds.getQueueHot(queueName) != captured || captured.closed.Load() {
		return interfaces.ErrQueueNotFound
	}

	// THE EPOCH CHECK. A range removal (the range purge, the queue deletion)
	// advances the epoch under this very lock, so the check and the
	// admission below are atomic with respect to it.
	if q.admissionEpoch != epoch {
		return errStaleAdmissionEpoch
	}

	// THE ADMISSION. Nothing was reserved and nothing was mutated yet, so a
	// refusal above needs no rollback.
	ds.hot.putBestEffortLocked(q, queueName, msg, charge, true, true)
	return nil
}

// readColdMessageCounted reads a message from the cold tiers (the WAL, then the
// segment tier). It NEVER admits and it NEVER acknowledges: it is the read half
// of both the peek and the delivery path.
//
// countPreads attributes the WAL's positional reads to the DELIVERY metric. It
// is true only on the delivery path: the peek (GetMessage) is used by the TTL
// reaper's per-tag probes, and counting those would inflate preads per
// delivered message with reads no consumer asked for.
func (ds *StorageEngine) readColdMessageCounted(queueName string, deliveryTag uint64, countPreads bool) (*protocol.Message, error) {
	var msg *protocol.Message
	if ds.wal != nil {
		if m, err := ds.wal.Read(queueName, deliveryTag); err == nil {
			msg = m
			if countPreads {
				// The WAL delivery parse issues one positional read for the
				// record header and one for its payload (parseMessageAt); a
				// shared-body record adds one more for the co-located body
				// block. They are counted here, at the boundary, so the metric
				// attributes them to the queue without threading a counter
				// through the WAL.
				ds.recordHotCacheDeliveryPread(queueName, 2)
			}
		}
	}
	if msg == nil && ds.segments != nil {
		if m, err := ds.segments.Read(queueName, deliveryTag); err == nil {
			msg = m
		}
	}
	if msg == nil {
		return nil, interfaces.ErrMessageNotFound
	}
	return msg, nil
}

// DeleteMessage removes a message from the hot cache and acknowledges it in
// the cold tiers.
//
// IT TAKES THE PER-TAG SHARD LOCK, so the (hot-cache removal + cold-tier
// acknowledgement) is ATOMIC with respect to the delivery reader's
// (acknowledgement re-check + admission). Without it, an acknowledgement
// could land between the reader's re-check and its admission, and the
// reader would admit an ALREADY-ACKNOWLEDGED tag into the hot cache: it
// would then be served from there, counted by the queue's message count,
// and its later removal would report a win — decrementing the queue depth a
// SECOND time for one message.
//
// It never holds a WAL/segment lock while taking the shard lock, and it is
// never called with the shard lock already held (see
// DeleteMessageIfPresentDetailed, which calls ackColdTiers directly).
func (ds *StorageEngine) DeleteMessage(queueName string, deliveryTag uint64) error {
	queue := ds.getQueueHot(queueName)
	if queue == nil {
		return interfaces.ErrQueueNotFound
	}
	mu := &coldRemovalShards[deliveryTag&63]
	mu.Lock()
	defer mu.Unlock()

	// GetResident, not Get: a settlement must find an entry that is
	// resident even when it is not yet durable.
	msg, backed, foundHot := ds.hot.GetResident(queueName, deliveryTag)
	ds.hot.Delete(queueName, deliveryTag)
	if !foundHot || msg.DeliveryMode == 2 || backed {
		ds.ackColdTiers(queueName, deliveryTag)
	}
	return nil
}

// DeleteMessageIfPresent removes a message and reports whether THIS call won
// the hot-cache removal — hotCache.Delete's unique-winner result only (the
// legacy resident-only contract). New callers that account queue depth MUST use
// DeleteMessageIfPresentDetailed: a spilled (WAL/segment-only) tag wins its
// removal through the detailed cold path but reports false here.
func (ds *StorageEngine) DeleteMessageIfPresent(queueName string, deliveryTag uint64) (bool, error) {
	removed, resident, err := ds.DeleteMessageIfPresentDetailed(queueName, deliveryTag)
	return removed && resident, err // legacy contract: resident removal only
}

// coldRemovalShards serialises the per-tag removal and admission so that
// concurrent removers of the same tag — hot-resident OR cold — observe
// exactly one winner, AND so that an acknowledgement can never race an
// admission into the hot cache.
//
// It is taken by:
//
//   - DeleteMessageIfPresentDetailed (the removal + the cold-tier
//     acknowledgement),
//   - DeleteMessage (the removal + the cold-tier acknowledgement), and
//   - GetMessageForDelivery (the acknowledgement re-check + the hot-cache
//     admission).
//
// The last two are what close the acknowledgement/admission window: the
// reader's (check + admit) and the remover's (remove + acknowledge) are
// mutually exclusive per tag, so an acknowledged tag can never be admitted.
//
// Package-level so no StorageEngine field (and no layout pin) changes.
// Taken only here; never while holding any WAL/segment lock.
var coldRemovalShards [64]sync.Mutex

// DeleteMessageIfPresentDetailed removes a ready message and reports whether
// THIS call is the unique remover of a message that was actually present.
//
//	removed=true,  wasResident=true : won the hot-cache delete
//	removed=false, wasResident=true : lost the hot-cache delete to a concurrent remover
//	removed=true,  wasResident=false: message was WAL/segment-only (spilled or
//	                                  recovered), present and not yet acked;
//	                                  this call acknowledged it
//	removed=false, wasResident=false: absent (gap tag, never stored, or
//	                                  already acknowledged)
//
// Callers decrement queue depth iff removed. Acknowledgement of WAL/segment
// tiers mirrors DeleteMessage.
func (ds *StorageEngine) DeleteMessageIfPresentDetailed(queueName string, deliveryTag uint64) (removed, wasResident bool, err error) {
	queue := ds.getQueueHot(queueName)
	if queue == nil {
		return false, false, interfaces.ErrQueueNotFound
	}
	mu := &coldRemovalShards[deliveryTag&63]
	mu.Lock()
	defer mu.Unlock()

	if msg, backed, ok := ds.hot.GetResident(queueName, deliveryTag); ok {
		won := ds.hot.Delete(queueName, deliveryTag)
		if won && (msg.DeliveryMode == 2 || backed) {
			ds.ackColdTiers(queueName, deliveryTag)
		}
		return won, true, nil
	}
	if ds.wal != nil && ds.wal.IsAcknowledged(queueName, deliveryTag) {
		return false, false, nil
	}
	// The cold read: GetMessage is a PEEK, so it does not admit anything —
	// this path settles the message itself.
	m, gerr := ds.GetMessage(queueName, deliveryTag)
	if gerr != nil || m == nil {
		return false, false, nil
	}
	ds.hot.Delete(queueName, deliveryTag)
	ds.ackColdTiers(queueName, deliveryTag)
	return true, false, nil
}

// ackColdTiers acknowledges a tag in the WAL and segment tiers, mirroring the
// acknowledgement half of DeleteMessage.
func (ds *StorageEngine) ackColdTiers(queueName string, deliveryTag uint64) {
	if ds.wal != nil {
		ds.wal.Acknowledge(queueName, deliveryTag)
	}
	if ds.segments != nil {
		ds.segments.Acknowledge(queueName, deliveryTag)
	}
}

func (ds *StorageEngine) RegisterConsumerCursor(queueName string, consumerTag string) {
	ring := ds.getOrCreateQueueHot(queueName)
	ring.ack.OnConsumerRegister(consumerTag)
}

func (ds *StorageEngine) UnregisterConsumerCursor(queueName string, consumerTag string) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnConsumerUnregister(consumerTag)
}

func (ds *StorageEngine) DeliverToConsumer(queueName string, consumerTag string, deliveryTag uint64) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnDeliver(deliveryTag, consumerTag)
}

func (ds *StorageEngine) AckFromConsumer(queueName string, consumerTag string, deliveryTag uint64) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnAck(deliveryTag, consumerTag)
}

func (ds *StorageEngine) NackFromConsumer(queueName string, consumerTag string, deliveryTag uint64) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return
	}
	ring.ack.OnNack(deliveryTag, consumerTag)
}

func (ds *StorageEngine) GetMinAckCursor(queueName string) uint64 {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return 0
	}
	return ring.ack.MinAckCursor()
}

func (ds *StorageEngine) GetQueueMessages(queueName string) ([]*protocol.Message, error) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	return ds.hot.Range(queueName, 0, ^uint64(0)), nil
}

func (ds *StorageEngine) GetQueueMessageCount(queueName string) (int, error) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ds.hot.Count(queueName), nil
}

// purgeQueueRange settles the hot and both cold tiers and counts each logical
// delivery tag once, even when it has multiple physical copies or resides in
// more than one tier. Caller has verified that the queue exists.
//
// IT INVALIDATES THE IN-FLIGHT DELIVERIES OF THE BAND. The purge settles a
// BAND of tags in the cold tiers, and it does not take the per-tag shard
// lock, so a delivery reader that is between its cold read and its admission
// could re-admit a tag the purge has just settled. The queue's admission
// epoch is advanced FIRST — before the cold tiers are settled — so such a
// reader is refused, and the hot tier's DeleteRange advances it again when
// it removes the band's entries.
func (ds *StorageEngine) purgeQueueRange(queueName string, minTag, maxTag uint64) int {
	if minTag > maxTag {
		return 0
	}
	// THE EPOCH IS ADVANCED BEFORE THE COLD TIERS ARE SETTLED, so a reader
	// that captured it before this purge cannot admit a tag this purge is
	// about to settle — not even in the window between the cold-tier
	// settlement and the hot tier's DeleteRange below.
	ds.hot.bumpAdmissionEpoch(queueName)
	unique := make(map[uint64]struct{})
	if ds.wal != nil {
		for _, tag := range ds.wal.PurgeQueue(queueName, minTag, maxTag) {
			unique[tag] = struct{}{}
		}
	}
	if ds.segments != nil {
		for _, tag := range ds.segments.PurgeQueue(queueName, minTag, maxTag) {
			unique[tag] = struct{}{}
		}
	}
	// The hot tier is settled by the ENTRY'S OWNED TAG. The message the cache
	// hands out is the live pointer, so its DeliveryTag field can be mutated
	// after admission; settling by that field would delete the wrong key and
	// report the wrong tag set.
	for _, tag := range ds.hot.TagsInRange(queueName, minTag, maxTag) {
		unique[tag] = struct{}{}
	}
	ds.hot.DeleteRange(queueName, minTag, maxTag)
	return len(unique)
}

func (ds *StorageEngine) PurgeQueue(queueName string, minTag, maxTag uint64) (int, error) {
	if ds.getQueueHot(queueName) == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ds.purgeQueueRange(queueName, minTag, maxTag), nil
}

func (ds *StorageEngine) GetMessageRange(queueName string, startTag, endTag uint64) ([]*protocol.Message, error) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}
	return ds.hot.Range(queueName, startTag, endTag), nil
}

func (ds *StorageEngine) DeleteMessageRange(queueName string, startTag, endTag uint64) error {
	if ds.getQueueHot(queueName) == nil {
		return interfaces.ErrQueueNotFound
	}
	ds.purgeQueueRange(queueName, startTag, endTag)
	return nil
}

func (ds *StorageEngine) GetUnackedCount(queueName, consumerTag string) (int, error) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ring.ack.GetUnackedCount(consumerTag), nil
}

func (ds *StorageEngine) GetUnackedTags(queueName, consumerTag string) ([]uint64, error) {
	ring := ds.getQueueHot(queueName)
	if ring == nil {
		return nil, interfaces.ErrQueueNotFound
	}

	return ring.ack.GetUnackedTags(consumerTag), nil
}

func (ds *StorageEngine) StoreExchange(exchange *protocol.Exchange) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreExchange(exchange)
}

func (ds *StorageEngine) GetExchange(name string) (*protocol.Exchange, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchange(name)
}

func (ds *StorageEngine) DeleteExchange(name string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteExchange(name)
}

func (ds *StorageEngine) ListExchanges() ([]*protocol.Exchange, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.ListExchanges()
}

func (ds *StorageEngine) StoreQueue(queue *protocol.Queue) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	if err := ds.metadataStore.StoreQueue(queue); err != nil {
		return err
	}
	_ = ds.getOrCreateQueueHot(queue.Name)
	return nil
}

func (ds *StorageEngine) GetQueue(name string) (*protocol.Queue, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueue(name)
}

func (ds *StorageEngine) DeleteQueue(name string) error {
	if val, ok := ds.queues.LoadAndDelete(name); ok {
		ring := val.(*QueueHot)
		ring.closed.Store(true)
		ds.hot.DeleteRange(name, 0, ^uint64(0))
	}

	if ds.metadataStore != nil {
		return ds.metadataStore.DeleteQueue(name)
	}
	return nil
}

// ListQueues enumerates persisted queue metadata records.
//
// Its errors are CLASSIFIED HERE, at the site that knows both the semantics and
// the absolute path (review-3 H-3). broker.initOrdinalAllocator used to build
// the fault itself and could only name the artifact `metadata/queues` — a
// RELATIVE path, in a refusal message an operator with several brokers has to
// act on. RecoveryFault.Artifact's own doc requires an absolute path when a
// file is involved.
func (ds *StorageEngine) ListQueues() ([]*protocol.Queue, error) {
	queuesDir := filepath.Join(ds.dataDir, MetadataDir, QueuesDir)
	if ds.metadataStore == nil {
		return nil, interfaces.FatalFault("metadata", queuesDir,
			"the metadata store was never initialized, so no queue record can be read",
			"the delivery-tag ordinal high-water mark cannot be established; a newly declared queue may be handed an ordinal whose records are still on disk, colliding with them in a WAL keyed by a bare delivery tag",
			nil)
	}
	queues, err := ds.metadataStore.ListQueues()
	if err != nil {
		return queues, interfaces.FatalFault("metadata", queuesDir,
			"the queue metadata directory could not be enumerated, so recovery cannot know which queues exist or what delivery-tag ordinal each owns",
			"the delivery-tag ordinal high-water mark cannot be established; a newly declared queue may be handed an ordinal whose records are still on disk, colliding with them in a WAL keyed by a bare delivery tag",
			err)
	}
	return queues, nil
}

func (ds *StorageEngine) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreBinding(queueName, exchangeName, routingKey, arguments)
}

func (ds *StorageEngine) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetBinding(queueName, exchangeName, routingKey)
}

func (ds *StorageEngine) DeleteBinding(queueName, exchangeName, routingKey string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteBinding(queueName, exchangeName, routingKey)
}

func (ds *StorageEngine) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueueBindings(queueName)
}

// SnapshotBindings forwards to the metadata store (BOB-4).
func (ds *StorageEngine) SnapshotBindings() ([]*interfaces.QueueBinding, []*interfaces.ExchangeBinding, error) {
	if ds.metadataStore == nil {
		return nil, nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.SnapshotBindings()
}

func (ds *StorageEngine) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchangeBindings(exchangeName)
}

func (ds *StorageEngine) StoreExchangeBinding(source, destination, routingKey string, arguments map[string]interface{}) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreExchangeBinding(source, destination, routingKey, arguments)
}

func (ds *StorageEngine) DeleteExchangeBinding(source, destination, routingKey string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteExchangeBinding(source, destination, routingKey)
}

func (ds *StorageEngine) GetExchangeBindingsFrom(source string) ([]*interfaces.ExchangeBinding, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetExchangeBindingsFrom(source)
}

// SetLogger wires a structured logger into the storage tiers' cold paths.
//
// Before this existed the storage package had NO logger, so three durability
// conditions were completely invisible: a checkpoint that could not run (the
// queue's WAL then grows every 5 minutes forever — review-4 B-2), a segment
// withdrawn from compaction (review-4 B-4), and a segment fault discovered on
// the write path rather than at boot (review-4 N-7). nil disables logging.
func (ds *StorageEngine) SetLogger(l interfaces.Logger) {
	if ds.wal != nil {
		ds.wal.SetLogger(l)
	}
	if ds.segments != nil {
		ds.segments.SetLogger(l)
	}
}

func (ds *StorageEngine) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.StoreConsumer(queueName, consumerTag, consumer)
}

func (ds *StorageEngine) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetConsumer(queueName, consumerTag)
}

func (ds *StorageEngine) DeleteConsumer(queueName, consumerTag string) error {
	if ds.metadataStore == nil {
		return fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.DeleteConsumer(queueName, consumerTag)
}

func (ds *StorageEngine) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	if ds.metadataStore == nil {
		return nil, fmt.Errorf("metadata store not initialized")
	}
	return ds.metadataStore.GetQueueConsumers(queueName)
}

func (ds *StorageEngine) BeginTransaction(txID string) (*interfaces.Transaction, error) {
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

func (ds *StorageEngine) GetTransaction(txID string) (*interfaces.Transaction, error) {
	ds.txMutex.RLock()
	defer ds.txMutex.RUnlock()

	if tx, ok := ds.transactions[txID]; ok {
		return tx, nil
	}
	return nil, interfaces.ErrTransactionNotFound
}

func (ds *StorageEngine) AddAction(txID string, action *interfaces.TransactionAction) error {
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

func (ds *StorageEngine) CommitTransaction(txID string) error {
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

func (ds *StorageEngine) RollbackTransaction(txID string) error {
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

func (ds *StorageEngine) DeleteTransaction(txID string) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()
	delete(ds.transactions, txID)
	return nil
}

func (ds *StorageEngine) ListActiveTransactions() ([]*interfaces.Transaction, error) {
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

func (ds *StorageEngine) StorePendingAck(pendingAck *protocol.PendingAck) error {
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

func (ds *StorageEngine) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	if val, ok := ds.pendingAcks.Load(deliveryTag); ok {
		return val.(*protocol.PendingAck), nil
	}
	return nil, interfaces.ErrPendingAckNotFound
}

func (ds *StorageEngine) DeletePendingAck(queueName string, deliveryTag uint64) error {
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
func (ds *StorageEngine) pendingAckLock(deliveryTag uint64) *sync.Mutex {
	return &ds.pendingAckShards[deliveryTag&(pendingAckShardCount-1)]
}

func (ds *StorageEngine) GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error) {
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

func (ds *StorageEngine) GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error) {
	var result []*protocol.PendingAck
	if inner, ok := ds.consumerPendingAcks.Load(consumerTag); ok {
		inner.(*sync.Map).Range(func(_, val interface{}) bool {
			result = append(result, val.(*protocol.PendingAck))
			return true
		})
	}
	return result, nil
}

func (ds *StorageEngine) CleanupExpiredAcks(maxAge time.Duration) error {
	return nil
}

func (ds *StorageEngine) GetAllPendingAcks() ([]*protocol.PendingAck, error) {
	var result []*protocol.PendingAck
	ds.pendingAcks.Range(func(_, val interface{}) bool {
		result = append(result, val.(*protocol.PendingAck))
		return true
	})
	return result, nil
}

func (ds *StorageEngine) StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error {
	return nil
}

func (ds *StorageEngine) GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error) {
	if ds.metadataStore == nil {
		return nil, interfaces.FatalFault("metadata", filepath.Join(ds.dataDir, MetadataDir),
			"the metadata store was never initialized, so no exchange, queue or ordinal can be read",
			"every durable entity in this data directory is abandoned and every recovered record becomes an orphan",
			nil)
	}

	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []*protocol.Exchange{},
		Queues:      []*protocol.Queue{},
		LastUpdated: time.Now(),
	}

	exchanges, err := ds.metadataStore.ListExchanges()
	if err != nil {
		return nil, interfaces.FatalFault("metadata", filepath.Join(ds.dataDir, MetadataDir, ExchangesDir),
			"the exchange metadata directory could not be enumerated, so recovery cannot know which durable exchanges exist",
			"every durable exchange and every binding through it is abandoned; messages published to them would be silently unroutable",
			err)
	}
	for _, exchange := range exchanges {
		if exchange.Durable {
			metadata.Exchanges = append(metadata.Exchanges, exchange)
		}
	}

	queues, err := ds.metadataStore.ListQueues()
	if err != nil {
		return nil, interfaces.FatalFault("metadata", filepath.Join(ds.dataDir, MetadataDir, QueuesDir),
			"the queue metadata directory could not be enumerated, so recovery cannot know which durable queues exist or what delivery-tag ordinal each owns",
			"every durable queue is abandoned and its confirmed durable records become unattributable",
			err)
	}
	for _, queue := range queues {
		if queue.Durable {
			metadata.Queues = append(metadata.Queues, queue)
		}
	}

	return metadata, nil
}

// ValidateStorageIntegrity is not implemented: integrity is enforced
// fail-closed by the classified recovery scan (GetRecoverableMessages /
// RecoverFromWAL / rebuildBootState), which runs right after this call and
// already refuses the boot on interior corruption. It returns
// ErrNotImplemented rather than an empty success, which would claim a
// validation that never ran. The stats pointer is non-nil so a caller that
// ignores the error cannot crash on a nil dereference.
func (ds *StorageEngine) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, fmt.Errorf("storage: ValidateStorageIntegrity: %w", ErrNotImplemented)
}

// RepairCorruption is not implemented (see ValidateStorageIntegrity).
func (ds *StorageEngine) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, fmt.Errorf("storage: RepairCorruption: %w", ErrNotImplemented)
}

// GetRecoverableMessages returns everything recovery should consider, together
// with a CLASSIFIED fault (interfaces.RecoveryOutcome) describing anything that
// went wrong reading it.
//
// It returns partial data alongside a non-fatal fault deliberately: a torn tail
// or one quarantined file must not cost the caller the records it DID read, and
// the boot decision belongs to the caller, not here. A fatal fault means the
// map cannot be trusted as an inventory of the directory.
func (ds *StorageEngine) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	messagesByQueue := make(map[string][]*protocol.Message)
	var faults []error

	if ds.wal != nil {
		// PARTIAL DATA IS KEPT EVEN WHEN THE FAULT IS FATAL. RecoverFromWAL
		// confines a fatal fault to the file that produced it and scans the
		// rest of the directory anyway, so what comes back is the complete
		// inventory of everything that COULD be read. Discarding it here — the
		// previous behaviour — is half of review-3's B-1: five healthy 59 MB
		// files' worth of confirmed durable messages were thrown away because
		// a sixth file had one flipped bit.
		recoveredMessages, err := ds.wal.RecoverFromWAL()
		if err != nil {
			faults = append(faults, err)
		}

		// walKeys is the WAL's recovery inventory keyed EXPLICITLY by
		// RecoveryMessage.Offset — the same key segments use (C8), rather than
		// the reconstructed DeliveryTag (the two are equal today; this makes
		// the key the deliberate one).
		walKeys := make(map[string]map[uint64]bool)
		for _, recoveryMsg := range recoveredMessages {
			messagesByQueue[recoveryMsg.QueueName] = append(
				messagesByQueue[recoveryMsg.QueueName],
				recoveryMsg.Message,
			)
			if walKeys[recoveryMsg.QueueName] == nil {
				walKeys[recoveryMsg.QueueName] = make(map[uint64]bool)
			}
			walKeys[recoveryMsg.QueueName][recoveryMsg.Offset] = true
		}

		if ds.segments != nil {
			// STEP 4 SLOT. RecoverFromSegments cannot currently return a non-nil
			// error (it swallows its own ReadDir failure and every per-segment
			// read error), so this branch is unreachable today. Step 4 makes the
			// segment tier authoritative — performCheckpoint unlinks the source WAL
			// file, so the segment becomes the only copy — and its error
			// classification lands HERE, in exactly this shape: a segment fault is
			// constructed with interfaces.FatalFault/DegradedFault at the site that
			// knows the semantics and appended to `faults`. Nothing above or below
			// needs restructuring; the outcome type, the join, the refusal message
			// and the --unsafe-recovery gate are all tier-agnostic.
			//
			// STEP 3-FIX CHANGED THIS SLOT'S CONTRACT, and Step 4 must honour it:
			// a fatal fault no longer returns early and no longer discards the map.
			// RecoverFromSegments must therefore confine a fatal segment fault to
			// the segment file that produced it — quarantine it, keep scanning, and
			// return everything else — exactly as RecoverFromWAL now does.
			segmentMessages, err := ds.segments.RecoverFromSegments()
			if err != nil {
				faults = append(faults, err)
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

		return messagesByQueue, joinFaults(faults)
	}

	if ds.segments != nil {
		segmentMessages, err := ds.segments.RecoverFromSegments()
		if err != nil {
			faults = append(faults, err)
		}

		for queueName, segMsgs := range segmentMessages {
			for _, rm := range segMsgs {
				messagesByQueue[queueName] = append(
					messagesByQueue[queueName],
					rm.Message,
				)
			}
		}
	}

	return messagesByQueue, joinFaults(faults)
}

func (ds *StorageEngine) MarkRecoveryComplete(stats *protocol.RecoveryStats) error {
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
func (ds *StorageEngine) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()

	staging := &txStagingStorage{StorageEngine: ds}
	if err := operations(staging); err != nil {
		// Roll back: nothing buffered has been applied.
		return err
	}
	return staging.commit()
}

func (ds *StorageEngine) Close() error {
	ds.queues.Range(func(_, value interface{}) bool {
		ring := value.(*QueueHot)
		if !ring.closed.Load() {
			ring.closed.Store(true)
		}
		return true
	})

	if ds.metadataStore != nil {
		_ = ds.metadataStore.Close()
	}

	if ds.wal != nil {
		_ = ds.wal.Close()
	}

	if ds.segments != nil {
		_ = ds.segments.Close()
	}

	return nil
}

var _ interfaces.Storage = (*StorageEngine)(nil)
