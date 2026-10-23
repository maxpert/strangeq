package storage

import (
	"fmt"
	"path/filepath"
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
	return NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
}

func WALConfigFromEngine(ec interfaces.EngineConfig) WALConfig {
	cfg := DefaultWALConfig()
	// Unconditional copy is safe: both zero values mean fsync ON (durable) and
	// CRC ON (safe).
	cfg.SyncDisabled = ec.WALSyncDisabled
	cfg.CRCDisabled = ec.WALCRCDisabled
	cfg.UnsafeRecovery = ec.UnsafeRecovery
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

func NewDisruptorStorageWithEngineConfig(dataDir string, engineCfg interfaces.EngineConfig) (*DisruptorStorage, error) {
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

	// The RING is self-gating and is therefore read first, UNGATED: DeleteMessage
	// and DeleteMessageIfPresent remove the tag from the ring BEFORE they
	// acknowledge either durable tier, so a ring hit means no acknowledgement has
	// been issued through this layer. Keeping the gate off the warm path is
	// deliberate — the ring is the only tier a warm read touches.
	if msg, ok := ring.ring.LoadByTag(deliveryTag); ok {
		return msg, nil
	}

	// THE ACKNOWLEDGEMENT GATE, covering every COLD tier at once.
	//
	// Below this line the answer can come from the read-ahead buffer, the WAL's
	// O(1) index, the WAL's sequential scan, or a segment. All four are gated
	// here, once, because this is the only layer that sees all four and the only
	// layer that holds NO tier lock while asking.
	//
	// Per-tier gating cannot do this job. The tiers disagree about what "acked"
	// means: WALManager.Acknowledge records synchronously, while the segment
	// tier's bitmap is written by batchAckLoop on a 10ms ticker, so a segment
	// gate is blind for up to one tick after every ack — and queue_reaper's
	// reapTTLSweep acknowledges a tag and re-probes it inside the same sweep,
	// landing in that window by construction. Worse, a WAL gate that correctly
	// refuses is precisely what ROUTES the probe on to the segment tier, because
	// a tier error here means "try the next one". One gate above all four
	// consults ONE set with ONE latency and never asks which tier is answering.
	if ds.wal != nil && ds.wal.IsAcknowledged(deliveryTag) {
		return nil, interfaces.ErrMessageNotFound
	}

	var msg *protocol.Message

	if ring.readAhead != nil {
		if m, ok := ring.readAhead.get(deliveryTag); ok {
			msg = m
		}
	}

	if msg == nil && ds.wal != nil {
		if ring.readAhead != nil {
			if batch, err := ds.wal.ReadBatch(queueName, deliveryTag); err == nil && len(batch) > 0 {
				// Take our own answer BEFORE publishing the map. put() hands
				// ownership of `batch` to the buffer, after which it may only be
				// touched under the buffer's lock — remove() deletes from it on
				// every acknowledgement. Reading batch[...] after put() is an
				// unsynchronised access to a map another goroutine is mutating.
				m, ok := batch[deliveryTag]
				ring.readAhead.put(batch)
				if ok {
					msg = m
				}
			}
		}
		if msg == nil {
			if m, err := ds.wal.Read(queueName, deliveryTag); err == nil {
				msg = m
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

	// RE-VALIDATE. The gate above and the tier read below it are not atomic, so
	// an acknowledgement landing while the cold read was in flight would
	// otherwise be served. Re-asking narrows that window to the bitmap lookup
	// itself, and costs one uncontended RLock on the cold path only.
	if ds.wal != nil && ds.wal.IsAcknowledged(deliveryTag) {
		return nil, interfaces.ErrMessageNotFound
	}

	return msg, nil
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

	// Invalidate the read-ahead cache unconditionally. GetMessage consults it
	// before the WAL, so leaving an acknowledged tag there keeps serving it no
	// matter what the WAL decides.
	if ring.readAhead != nil {
		ring.readAhead.remove(deliveryTag)
	}

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

// DeleteMessageIfPresent removes a message and reports whether THIS call won
// the RING removal — AtomicRing.Delete's CAS result only (the legacy
// contract). New callers that account queue depth MUST use
// DeleteMessageIfPresentDetailed: a spilled (WAL/segment-only) tag wins its
// removal through the detailed cold path but reports false here.
func (ds *DisruptorStorage) DeleteMessageIfPresent(queueName string, deliveryTag uint64) (bool, error) {
	removed, resident, err := ds.DeleteMessageIfPresentDetailed(queueName, deliveryTag)
	return removed && resident, err // legacy contract: ring-CAS result only
}

// coldRemovalShards serialises DeleteMessageIfPresentDetailed per tag so that
// concurrent removers of the same tag — ring-resident OR spilled — observe
// exactly one winner. Package-level so no DisruptorStorage field (and no
// layout pin) changes. Taken only here; never while holding any WAL/segment lock.
var coldRemovalShards [64]sync.Mutex

// DeleteMessageIfPresentDetailed removes a ready message and reports whether
// THIS call is the unique remover of a message that was actually present.
//
//	removed=true,  wasResident=true : won the ring CAS
//	removed=false, wasResident=true : lost the ring CAS to a concurrent remover
//	removed=true,  wasResident=false: message was WAL/segment-only (spilled or
//	                                  recovered), present and not yet acked;
//	                                  this call acknowledged it
//	removed=false, wasResident=false: absent (gap tag, never stored, or
//	                                  already acknowledged)
//
// Callers decrement queue depth iff removed. Acknowledgement of WAL/segment
// tiers mirrors DeleteMessage.
func (ds *DisruptorStorage) DeleteMessageIfPresentDetailed(queueName string, deliveryTag uint64) (removed, wasResident bool, err error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return false, false, interfaces.ErrQueueNotFound
	}
	// The stripe lock spans the ring path too: a durable ring winner deletes
	// the slot and then acks the WAL; without the lock, a loser could reach
	// the cold path in between, find the WAL record unacked, and double-remove.
	mu := &coldRemovalShards[deliveryTag&63]
	mu.Lock()
	defer mu.Unlock()

	if msg, ok := ring.ring.LoadByTag(deliveryTag); ok {
		won := ring.ring.Delete(deliveryTag)
		if ring.readAhead != nil {
			ring.readAhead.remove(deliveryTag)
		}
		if won && msg.DeliveryMode == 2 {
			ds.ackColdTiers(queueName, deliveryTag)
		}
		return won, true, nil
	}
	if ds.wal != nil && ds.wal.IsAcknowledged(deliveryTag) {
		return false, false, nil
	}
	m, gerr := ds.GetMessage(queueName, deliveryTag) // ring miss → cold tiers, ack-gated
	if gerr != nil || m == nil {
		return false, false, nil
	}
	if ring.readAhead != nil {
		ring.readAhead.remove(deliveryTag)
	}
	ds.ackColdTiers(queueName, deliveryTag)
	return true, false, nil
}

// ackColdTiers acknowledges a tag in the WAL and segment tiers, mirroring the
// acknowledgement half of DeleteMessage.
func (ds *DisruptorStorage) ackColdTiers(queueName string, deliveryTag uint64) {
	if ds.wal != nil {
		ds.wal.Acknowledge(queueName, deliveryTag)
	}
	if ds.segments != nil {
		ds.segments.Acknowledge(queueName, deliveryTag)
	}
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

func (ds *DisruptorStorage) PurgeQueue(queueName string, minTag, maxTag uint64) (int, error) {
	ring := ds.getQueueRing(queueName)
	if ring == nil {
		return 0, interfaces.ErrQueueNotFound
	}
	return ring.ring.DeleteRange(minTag, maxTag), nil
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

// ListQueues enumerates persisted queue metadata records.
//
// Its errors are CLASSIFIED HERE, at the site that knows both the semantics and
// the absolute path (review-3 H-3). broker.initOrdinalAllocator used to build
// the fault itself and could only name the artifact `metadata/queues` — a
// RELATIVE path, in a refusal message an operator with several brokers has to
// act on. RecoveryFault.Artifact's own doc requires an absolute path when a
// file is involved.
func (ds *DisruptorStorage) ListQueues() ([]*protocol.Queue, error) {
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

// SetLogger wires a structured logger into the storage tiers' cold paths.
//
// Before this existed the storage package had NO logger, so three durability
// conditions were completely invisible: a checkpoint that could not run (the
// queue's WAL then grows every 5 minutes forever — review-4 B-2), a segment
// withdrawn from compaction (review-4 B-4), and a segment fault discovered on
// the write path rather than at boot (review-4 N-7). nil disables logging.
func (ds *DisruptorStorage) SetLogger(l interfaces.Logger) {
	if ds.segments != nil {
		ds.segments.SetLogger(l)
	}
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

func (ds *DisruptorStorage) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, nil
}

func (ds *DisruptorStorage) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	return &protocol.RecoveryStats{}, nil
}

// GetRecoverableMessages returns everything recovery should consider, together
// with a CLASSIFIED fault (interfaces.RecoveryOutcome) describing anything that
// went wrong reading it.
//
// It returns partial data alongside a non-fatal fault deliberately: a torn tail
// or one quarantined file must not cost the caller the records it DID read, and
// the boot decision belongs to the caller, not here. A fatal fault means the
// map cannot be trusted as an inventory of the directory.
func (ds *DisruptorStorage) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
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

		for _, recoveryMsg := range recoveredMessages {
			messagesByQueue[recoveryMsg.QueueName] = append(
				messagesByQueue[recoveryMsg.QueueName],
				recoveryMsg.Message,
			)
		}
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

	return messagesByQueue, joinFaults(faults)
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

	if ds.wal != nil {
		_ = ds.wal.Close()
	}

	if ds.segments != nil {
		_ = ds.segments.Close()
	}

	return nil
}

var _ interfaces.Storage = (*DisruptorStorage)(nil)
