package broker

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// ErrConsumerChannelFull is returned when a consumer channel is full and cannot accept more messages
var ErrConsumerChannelFull = errors.New("consumer channel full")

// ErrNoRoute is returned when a message published with mandatory=true
// cannot be routed to any queue.
var ErrNoRoute = errors.New("no route to destination queue")

// ErrQueueClosed is returned when a publish is routed to a queue whose runtime
// state was closed underneath it — a concurrent queue.delete tore the queue
// down after routing resolved its binding. The message was neither stored nor
// dead-lettered, so the publisher must be told (nack / basic.return), but the
// connection is healthy and must NOT be torn down.
var ErrQueueClosed = errors.New("queue closed during publish")

// ErrExchangeTypeMismatch is returned when a re-declare of an existing
// exchange specifies a different type. Callers should handle this as a
// channel-level 406 PreconditionFailed per AMQP 0.9.1 spec.
var ErrExchangeTypeMismatch = errors.New("exchange type mismatch")

// defaultUnlimitedPrefetchCap is the fallback finite gate cap for a prefetch-0
// manual-ack consumer when EngineConfig.UnlimitedPrefetchCap is unset (0). It
// must exceed the coarsest supported client ack cadence (see the config field
// doc): the head-to-head MultiAck1000 benchmark multi-acks every 1000 messages
// and needs a window strictly greater than 1000 to make progress.
const defaultUnlimitedPrefetchCap = 2000

// unlimitedPrefetchCap returns the configured finite cap applied to a prefetch-0
// manual-ack consumer, falling back to defaultUnlimitedPrefetchCap when the
// engine config leaves it unset (e.g. a manually-constructed test EngineConfig).
func (b *StorageBroker) unlimitedPrefetchCap() int64 {
	if b.engineConfig.UnlimitedPrefetchCap > 0 {
		return int64(b.engineConfig.UnlimitedPrefetchCap)
	}
	return defaultUnlimitedPrefetchCap
}

func isSupportedExchangeType(kind string) bool {
	switch kind {
	case "direct", "fanout", "topic", "headers":
		return true
	default:
		return false
	}
}

// QueueState and its constructor NewQueueState now live in queue_dispatch.go,
// replacing the old `available chan uint64` with a lock-free tail cursor +
// condvar park/wake + bounded requeue ring. See queue_dispatch.go for the
// full design.

// ConsumerState tracks the state of an active consumer for pull-based delivery
type prefetchGate struct {
	limit    atomic.Int64
	inflight atomic.Int64
	wake     chan struct{}
	stopped  atomic.Bool
}

func newPrefetchGate(limit int64) *prefetchGate {
	g := &prefetchGate{wake: make(chan struct{}, 1)}
	g.limit.Store(limit)
	return g
}

func (g *prefetchGate) acquire(stop <-chan struct{}) bool {
	for {
		if g.stopped.Load() {
			return false
		}
		lim := g.limit.Load()
		inf := g.inflight.Load()
		if inf < lim {
			if g.inflight.CompareAndSwap(inf, inf+1) {
				return true
			}
			continue
		}
		select {
		case <-g.wake:
		case <-stop:
			return false
		}
	}
}

func (g *prefetchGate) release(n int64) {
	g.inflight.Add(-n)
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *prefetchGate) updateLimit(newLimit int64) {
	g.limit.Store(newLimit)
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *prefetchGate) stop() {
	g.stopped.Store(true)
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// releaseGate returns prefetch credit for n acknowledged/rejected/requeued
// messages, unless the consumer bypasses the gate (no-ack or prefetch 0), in
// which case no credit was ever taken and releasing would drive the gate's
// inflight counter negative.
func (s *ConsumerState) releaseGate(n int64) {
	if !s.bypassGate.Load() {
		s.gate.release(n)
	}
}

type ConsumerState struct {
	consumer  *protocol.Consumer
	queueName string
	gate      *prefetchGate
	// bypassGate is set when the consumer must not be prefetch-gated: either it
	// is no-ack (per AMQP 0.9.1 prefetch is ignored under no-ack) or its
	// prefetch-count is 0 (which the spec defines as "no limit" / unlimited).
	// When set, the poll loop delivers without acquiring gate credit, and the
	// ack/nack/reject paths must not release credit they never took. Gating a
	// prefetch-0 consumer with a fixed cap deadlocks a client whose ack cadence
	// is coarser than that cap (e.g. multi-ack every N > cap): it can never
	// receive enough messages to emit the ack that would reopen the gate.
	bypassGate atomic.Bool
	stopCh     chan struct{}
	stopOnce   sync.Once
	done       chan struct{}
}

// StorageBroker manages exchanges, queues, and routing using persistent storage
// All fields use lock-free data structures for maximum concurrency
type MetricsCollector interface {
	RecordMessageRedelivered()
}

type StorageBroker struct {
	storage           interfaces.Storage
	engineConfig      interfaces.EngineConfig
	queueStates       sync.Map
	activeQueues      sync.Map
	activeConsumers   sync.Map
	queueConsumers    sync.Map
	queueConsumersMu  sync.Map
	queueOwners       sync.Map
	deliveryIndex     sync.Map
	getDeliveryQueues sync.Map
	metricsCollector  MetricsCollector

	// queueCreateMuByName holds one *sync.Mutex per queue name (LoadOrStore,
	// same pattern as queueConsumersMu / getQueueConsumersMutex). It backs
	// FIVE distinct per-name critical sections. The mutex is NOT reentrant and
	// must never be held twice by the same goroutine; within a section, call
	// createQueueStateLocked, never getOrCreateQueueState. Sections 1-4 HOLD
	// the mutex across their QueueState work rather than unlocking first — see
	// getQueueCreateMutex's doc comment for why that is the load-bearing
	// invariant and what breaks if any of them unlocks early. Section 5 holds
	// no QueueState at all and takes the mutex only to serialize against
	// section 4:
	//
	//  1. DeclareQueue's new-queue branch: serializes the whole
	//     check-allocate-persist sequence for a queue's metadata record, so
	//     only ONE ordinal is ever allocated and persisted for a given name
	//     even when several connections race to idempotently declare the
	//     same brand-new queue (the most common AMQP client pattern). See
	//     storage/persistent_metadata.go StoreQueue's write-once-ordinal
	//     invariant for the second, independent layer of defense.
	//
	//  2. DeclareQueue's existing-queue (redeclare) branch: the lock-free
	//     record read that selects this branch is only a pre-check, so the
	//     record is re-read under this mutex and the mutex is then held across
	//     the whole redeclare, including the QueueState creation inside
	//     declareExistingQueueLocked. Without that span, a concurrent
	//     DeleteQueue landing between validating the record and creating the
	//     state leaves a live, publishable, record-less queue whose confirmed
	//     durable publishes are discarded at the next restart. If the re-read
	//     finds the record already deleted, this branch unlocks and falls
	//     through to case 1, which persists a fresh record and ordinal band
	//     for the new incarnation.
	//
	//  3. getOrCreateQueueState's COLD path — the first-ever creation of a
	//     QueueState for a given queue name in this process. Held while the
	//     queue's composite-tag ordinal (broker/tag_packing.go) is resolved
	//     and SetOrdinal is called, BEFORE the new QueueState is published
	//     via queueStates.Store: without this, a plain LoadOrStore would let
	//     a concurrent publisher observe the QueueState (and mint a tag)
	//     before SetOrdinal ran, minting with ordinalBase still at its zero
	//     value — colliding with ordinal-0's tag space.
	//
	//  4. DeleteQueue's teardown span: held from the incarnation revalidation
	//     through storage.DeleteQueue (inclusive), so it covers the ring
	//     purge, the binding sweep and the QueueState removal. Two properties
	//     depend on that width. First, the revalidated ordinal cannot go stale
	//     under the caller, because DeclareQueue — the only way a successor
	//     comes into existence — holds this same mutex across its whole body.
	//     Second, a concurrent getOrCreateQueueState cold path cannot observe
	//     the metadata record after the QueueState has been removed but before
	//     the record is deleted — a window in which it would reuse the dying
	//     queue's ordinal without persisting it. Blocking here instead forces
	//     that concurrent create to wait until the record is genuinely gone
	//     and allocate a FRESH ordinal. This is a SIBLING critical section to
	//     getQueueConsumersMutex — DeleteQueue releases that mutex before
	//     acquiring this one — never nested inside it.
	//
	//  5. BindQueue's check→store pair: holds the mutex across the
	//     queue-exists check and StoreBinding, and nothing else. A binding row
	//     carries no identity to revalidate against, so mutual exclusion
	//     against section 4's binding sweep is the only mechanism available;
	//     see BindQueue for what a row that outlives its queue costs. This
	//     section is the reason a queue.bind can now wait on a concurrent
	//     first-ever queue.declare of the same name (~18ms, two fsyncs) instead
	//     of proceeding lock-free. Only the pair is held —
	//     updateDurableMetadata runs outside it.
	//
	// Locking per NAME rather than broker-wide means one queue's first-ever
	// creation (StoreQueue's atomicWrite fsyncs twice, ~18ms on macOS) never
	// blocks a different queue's concurrent first creation — only concurrent
	// creators of the SAME name contend. The hot path (queue already known)
	// in getOrCreateQueueState never touches this map; it is a single
	// lock-free sync.Map Load.
	queueCreateMuByName sync.Map

	// ordinalAlloc is the broker-wide allocator for queue composite-tag
	// ordinals (broker/tag_packing.go). Initialized at construction from the
	// max Ordinal already persisted across all known queues (via
	// ListQueues), so restart never reissues an ordinal a still-live durable
	// queue already owns. allocateOrdinal() is the only writer.
	ordinalAlloc atomic.Uint64

	// logger is used only on cold paths (e.g. a dropped dead-letter). nil is
	// treated as "no logging" so callers that never wire one pay nothing; the
	// server builder injects its interfaces.Logger via SetLogger.
	logger interfaces.Logger

	// initErr carries a construction failure NewStorageBroker's signature
	// cannot return. See InitError.
	initErr error

	// ttlNow is the wall clock (Unix milliseconds) used by every SQ-9 TTL time
	// read — publish stamping, the delivery head-check, the reaper, x-expires,
	// and recovery replay. It is a field (not a package global) so parallel tests
	// each get an isolated, overridable clock and BenchmarkDeliver_NoTTL_ZeroCost
	// can install a counting clock to assert the no-TTL delivery path performs
	// zero clock reads.
	ttlNow func() int64

	// reaperWG tracks the live per-queue SQ-9 reaper goroutines so Close() can
	// wait for them to exit before the caller tears down storage. Without the
	// wait, a reaper mid-sweep could touch a closed store (a data race under
	// -race and the reaper/claim tests run there).
	reaperWG sync.WaitGroup

	// Deferred drop-head max-length trim worker (C1). A durable async publish's
	// completion runs on the shared WAL batch-writer goroutine; drop-head
	// eviction can dead-letter/spill via a SYNCHRONOUS StoreMessage -> wal.Write
	// -> <-doneChan, and that doneChan is only signalled by the batch-writer
	// goroutine — so calling it FROM a completion deadlocks the durable pipeline
	// (A4). Completions therefore hand eviction to this worker, which may block
	// freely. Started lazily on first use; Close stops it. The worker is only
	// ever engaged by queues with a drop-head max-length policy — a plain queue
	// (the throughput benchmark) never touches it.
	maxLenTrimMu     sync.Mutex
	maxLenTrimCh     chan maxLenTrimReq
	maxLenTrimStop   chan struct{}
	maxLenTrimWG     sync.WaitGroup
	maxLenTrimClosed bool
}

// maxLenTrimReq is a deferred drop-head trim handed from an async publish
// completion (on the WAL batch-writer goroutine) to the max-length trim worker.
type maxLenTrimReq struct {
	qs        *QueueState
	queueName string
	policy    *QueuePolicy
}

// NewStorageBroker creates a new storage-backed broker instance
// Phase 6G: Now accepts EngineConfig for tunable parameters
func NewStorageBroker(storage interfaces.Storage, engineConfig interfaces.EngineConfig) *StorageBroker {
	broker := &StorageBroker{
		storage:      storage,
		engineConfig: engineConfig,
		ttlNow:       func() int64 { return time.Now().UnixMilli() },
	}

	broker.initErr = broker.initOrdinalAllocator()
	broker.initializeDefaultExchanges()

	return broker
}

// InitError reports a failure that occurred while constructing the broker and
// that the constructor's signature cannot return.
//
// It exists for exactly one fact: initOrdinalAllocator's delivery-tag ordinal
// high-water mark. That mark is what stops a fresh declare being handed an
// ordinal whose records are still physically present in the shared WAL, which
// is a cross-queue delivery-tag collision in a store keyed by a bare uint64.
// The function used to write `if recoverable, err := ...; err == nil` — so it
// dropped the WAL-derived half of the mark EXACTLY when the WAL was unreadable,
// which is precisely the collision its own doc comment exists to prevent.
//
// server/builder.go checks this immediately after NewStorageBroker, before
// recovery runs. NewStorageBroker's own signature is not changed because it has
// ~20 call sites, almost all of them tests with their own temp directories.
func (b *StorageBroker) InitError() error { return b.initErr }

// initOrdinalAllocator seeds ordinalAlloc from the highest queue ordinal
// already persisted (via ListQueues), so a fresh process never reissues an
// ordinal a still-live durable queue already owns after restart. Queues
// declared before this field existed (or never yet resolved) carry
// Ordinal==0 and are ignored by the max. A ListQueues error is tolerated
// (best-effort, mirrors updateDurableMetadata's "log but don't fail" style
// elsewhere in this file): ordinalAlloc simply starts at 0, so the first
// allocation is ordinal 1, same as a brand-new broker.
//
// ListQueues alone under-reports the true high-water mark: a queue's
// ordinal is RETIRED, never reclaimed, when its metadata record is deleted
// (queue.delete purges only the in-memory ring — the shared WAL is never
// purged by queue name; see recovery_manager.go's discard-on-recovery
// cases B/C). So a name that was declared, published to, and deleted still
// has physical records sitting in the WAL tagged with its old ordinal, with
// no metadata trace for ListQueues to see. Left unaccounted for, a fresh
// declare of a new name (or a redeclare of the same name) after restart
// could be handed that same retired ordinal — and the moment recovery next
// runs, records from the dead incarnation and the new one would carry
// identical composite tags, an unrecoverable collision in the shared WAL's
// bare-uint64-keyed offsetIndex/ackBitmap. So every recoverable record's own
// composite-tag ordinal is folded into the high-water mark too, not just
// live queues' persisted Ordinal fields.
//
// BOUNDARY OF THAT GUARD, stated because it was nearly violated: it is keyed on
// PHYSICAL PRESENCE of records. Once a dead incarnation's records are reclaimed
// there is nothing left to fold, and the ordinal is legitimately handed to a
// brand-new queue — which is correct only while no durable, tag-keyed state
// survives the records. Today none does; the durable acknowledgement snapshot
// that would have been the first such state was withdrawn, in part because this
// fold only sees DeliveryMode==2 records, so a non-durable queue that spilled
// and acked transient messages left no ordinal trace at all and its whole acked
// tag band would have been inherited by whichever new queue got its ordinal.
// Any future durable state keyed by delivery tag must be attributed to the
// RECORD it refers to rather than to the tag value, or a reused ordinal
// destroys the new queue's confirmed messages. See
// .notes/loop-2/deferred-ack-durability.md.
// STEP 3 — BOTH READS NOW PROPAGATE THEIR ERRORS. Each used to be wrapped in
// `if ..., err := ...; err == nil`, which silently produced a high-water mark
// of 0 in exactly the situations that make the mark matter: an unreadable
// queue-metadata directory, or an unreadable WAL. The GetRecoverableMessages
// case is the sharper one — it dropped the WAL-derived mark precisely when the
// WAL could not be read, i.e. when nothing else could vouch for which ordinals
// are in use. Whatever partial inventory came back is still folded in before
// the error is returned, and the mark is stored even on the error path, so the
// allocator is never LESS informed than it was.
func (b *StorageBroker) initOrdinalAllocator() error {
	var maxOrdinal uint64
	var faults []error

	queues, qerr := b.storage.ListQueues()
	for _, q := range queues {
		if q.Ordinal > maxOrdinal {
			maxOrdinal = q.Ordinal
		}
	}
	if qerr != nil {
		// Pass a classified error straight through: the storage layer knows the
		// absolute path of the directory it failed to read and this package
		// does not. Only an UNCLASSIFIED error (a storage implementation that
		// does not classify) is wrapped here, and it names no path rather than
		// naming a relative one — review-3 H-3.
		if len(interfaces.FaultsOf(qerr)) > 0 {
			faults = append(faults, qerr)
		} else {
			faults = append(faults, interfaces.FatalFault("ordinal-allocator", "the queue metadata store",
				"the queue metadata records could not be listed, so the delivery-tag ordinal high-water mark cannot be established",
				"a newly declared queue may be handed an ordinal whose records are still on disk, colliding with them in a WAL keyed by a bare delivery tag",
				qerr))
		}
	}

	recoverable, rerr := b.storage.GetRecoverableMessages()
	for _, messages := range recoverable {
		for _, m := range messages {
			if m.DeliveryMode != 2 {
				continue
			}
			if ord := TagOrdinal(m.DeliveryTag); ord > maxOrdinal {
				maxOrdinal = ord
			}
		}
	}
	if rerr != nil {
		faults = append(faults, rerr)
	}

	// Store BEFORE returning: a partial mark is strictly safer than zero.
	b.ordinalAlloc.Store(maxOrdinal)

	if len(faults) == 0 {
		return nil
	}
	return errors.Join(faults...)
}

// allocateOrdinal hands out the next composite-tag queue ordinal
// (broker/tag_packing.go), starting from 1 (0 means "unassigned"). Fails
// loudly — never silently wraps — if the 20-bit ordinal space is exhausted;
// see tag_packing.go for why that is unreachable in any real deployment
// (1,048,575 queue lifetimes).
func (b *StorageBroker) allocateOrdinal() (uint64, error) {
	ord := b.ordinalAlloc.Add(1)
	if ord > MaxQueueOrdinal {
		return 0, fmt.Errorf("queue ordinal space exhausted: cannot allocate ordinal %d (max %d)", ord, MaxQueueOrdinal)
	}
	return ord, nil
}

// resolveQueueOrdinal returns the composite-tag ordinal for the queue whose
// metadata record is `existing`, reusing an already-persisted one (redeclare,
// or restart recovery) or allocating and persisting a fresh one. In the common
// case DeclareQueue's new-queue branch has already resolved and persisted the
// ordinal before the queue's QueueState is ever created, so this is just a
// fast reuse; the allocate-and-persist branch below remains for legacy/edge
// records (e.g. a pre-ordinal-field record).
//
// It takes the RECORD, not a queue name, and `existing` must be non-nil. That
// is the whole point of the signature: a QueueState must never be created for
// a name with no record (resolving an ordinal that cannot be persisted leaves
// a live, record-less queue whose confirmed durable publishes are discarded at
// the next restart), and passing the record makes that a precondition the
// compiler helps enforce rather than a comment a future caller can miss. The
// sole caller, createQueueStateLocked, reads the record to make exactly that
// decision and hands the same pointer down, so there is no second read here.
// That caller holds getQueueCreateMutex for the queue name, so there is no
// concurrent-allocation race for it.
//
// PERSISTENCE FAILURE IS FATAL TO THE DECLARE, and the comment that used to
// sit here saying otherwise was false. It read:
//
//	"Persistence failure is tolerated (best-effort): a freshly allocated
//	 ordinal is always new and unique, so a failed write here cannot cause a
//	 collision — worst case the queue re-allocates a different, still-safe
//	 ordinal on next restart"
//
// The moment a message is CONFIRMED under the unpersisted ordinal, the next
// restart reads the record back with Ordinal==0, recovery takes Case D, and
// ErrLegacyDataDirectory makes the broker refuse to boot — permanently, until
// an operator destroys the data directory. The tolerated failure was not
// "re-allocate on next restart"; it was "brick the broker", written down as
// reassurance. The behaviour is fixed here, not the comment.
//
// It also no longer panics on ordinal-space exhaustion. That was the only panic
// reachable while a per-name create mutex was held with a bare Lock/Unlock, and
// the OTHER allocation site (DeclareQueue) already returned a clean error for
// the identical condition. The caller turns either error into a CLOSED queue
// state, so publishes are refused rather than confirmed and then discarded.
func (b *StorageBroker) resolveQueueOrdinal(existing *protocol.Queue) (uint64, error) {
	if existing.Ordinal != 0 {
		return existing.Ordinal, nil
	}

	ordinal, aerr := b.allocateOrdinal()
	if aerr != nil {
		return 0, aerr
	}

	// existing is storage's SHARED CACHED POINTER (see
	// PersistentMetadataStore.GetQueue/loadQueueFromDisk) — concurrent GetQueue
	// callers read the SAME object, so it must never be mutated in place.
	//
	// Do not weaken this to "the callers are serialized now": DeclareQueue,
	// deleteQueueIncarnation and BindQueue do all hold this queue's create
	// mutex across their GetQueue, but eight other call sites do NOT — grep
	// b.storage.GetQueue / metadata GetQueue — and one of them is in
	// server/recovery_manager.go, a different package. Serialization of some
	// readers is not a licence to mutate a pointer the rest still share.
	//
	// Clone before setting the ordinal. Field-by-field (not
	// `cp := *existing`): protocol.Queue embeds an atomic.Uint64
	// (MessageCount), which must not be copied by value (go vet: "assignment
	// copies lock value") — round-trip its value through Load/Store instead.
	cp := protocol.Queue{
		Name:        existing.Name,
		Durable:     existing.Durable,
		AutoDelete:  existing.AutoDelete,
		Exclusive:   existing.Exclusive,
		Arguments:   existing.Arguments,
		Channel:     existing.Channel,
		OwnerConnID: existing.OwnerConnID,
		Ordinal:     ordinal,
	}
	cp.MessageCount.Store(existing.MessageCount.Load())
	if serr := b.storage.StoreQueue(&cp); serr != nil {
		return 0, fmt.Errorf("failed to persist delivery-tag ordinal %d for queue %q: %w",
			ordinal, existing.Name, serr)
	}

	return ordinal, nil
}

// ttlNowMillis returns the current wall clock in Unix milliseconds through the
// broker's (test-overridable) SQ-9 clock.
func (b *StorageBroker) ttlNowMillis() int64 { return b.ttlNow() }

// SetTTLClock overrides the SQ-9 wall clock. TEST/RECOVERY-simulation use only —
// it lets a test advance time to expire messages or simulate downtime without
// real sleeps. Not safe to call concurrently with TTL activity.
func (b *StorageBroker) SetTTLClock(now func() int64) { b.ttlNow = now }

// Close stops all per-queue background goroutines (the SQ-9 TTL/x-expires
// reaper) by closing every queue state, then WAITS for every reaper goroutine
// to exit. It is idempotent and does NOT close the underlying storage — the
// caller owns that. Production servers should call this on shutdown for a clean
// stop; tests call it before closing storage so a reaper never touches a closed
// store (the synchronous wait is what makes that guarantee hold under -race).
func (b *StorageBroker) Close() {
	b.queueStates.Range(func(_, v interface{}) bool {
		v.(*QueueState).Close()
		return true
	})
	b.reaperWG.Wait()

	// Stop the deferred max-length trim worker (if it was started) and wait for
	// it to drain, so no trim touches storage after the caller closes it.
	b.maxLenTrimMu.Lock()
	b.maxLenTrimClosed = true
	stop := b.maxLenTrimStop
	b.maxLenTrimMu.Unlock()
	if stop != nil {
		close(stop)
		b.maxLenTrimWG.Wait()
	}
}

// enqueueMaxLenTrim hands a drop-head max-length trim to the trim worker instead
// of running it inline. It is called from an async publish completion (which runs
// on the shared WAL batch-writer goroutine), where a blocking eviction would
// deadlock the durable pipeline (C1/A4). Non-blocking: lazily starts the worker,
// then does a non-blocking send — if the worker is behind, the trim is skipped
// (drop-head tolerates bounded transient overshoot; the next completion's trim
// re-converges the queue under its limit).
func (b *StorageBroker) enqueueMaxLenTrim(qs *QueueState, queueName string, policy *QueuePolicy) {
	b.maxLenTrimMu.Lock()
	if b.maxLenTrimClosed {
		b.maxLenTrimMu.Unlock()
		return
	}
	if b.maxLenTrimCh == nil {
		b.maxLenTrimCh = make(chan maxLenTrimReq, 4096)
		b.maxLenTrimStop = make(chan struct{})
		b.maxLenTrimWG.Add(1)
		go b.maxLenTrimLoop(b.maxLenTrimCh, b.maxLenTrimStop)
	}
	ch := b.maxLenTrimCh
	b.maxLenTrimMu.Unlock()

	select {
	case ch <- maxLenTrimReq{qs: qs, queueName: queueName, policy: policy}:
	default:
	}
}

// maxLenTrimLoop runs deferred drop-head trims off the WAL batch-writer
// goroutine, so an eviction's synchronous dead-letter/spill StoreMessage can
// block here without wedging the durable pipeline. On stop it best-effort drains
// queued trims so the final queue state is converged before storage is closed.
func (b *StorageBroker) maxLenTrimLoop(ch chan maxLenTrimReq, stop chan struct{}) {
	defer b.maxLenTrimWG.Done()
	for {
		select {
		case req := <-ch:
			b.dropHeadTrim(req.qs, req.queueName, req.policy)
		case <-stop:
			for {
				select {
				case req := <-ch:
					b.dropHeadTrim(req.qs, req.queueName, req.policy)
				default:
					return
				}
			}
		}
	}
}

// ringDeleter is the optional storage capability the SQ-9 TTL reaper and
// delivery head-check use to atomically claim a message removal (the
// linearization token). DisruptorStorage implements it; a backend that does not
// falls back to the unconditional DeleteMessage (treated as a win), which is
// safe because such a backend is used single-writer in the paths that need it.
type ringDeleter interface {
	DeleteMessageIfPresent(queueName string, deliveryTag uint64) (bool, error)
}

// ringDeleterDetailed is the §8 capability that also reports removals of
// SPILLED (WAL/segment-only) messages, so depth accounting is correct for a
// non-ring-resident ready message.
type ringDeleterDetailed interface {
	DeleteMessageIfPresentDetailed(queueName string, deliveryTag uint64) (removed, wasResident bool, err error)
}

// asyncDurableWriter is the optional storage capability that decouples a durable
// publish's fsync from the caller: the message is ring-stored synchronously and
// its WAL write is enqueued for group commit, with the fsync completion reported
// via onDurable. DisruptorStorage implements it; PublishMessageAsyncConfirm
// falls back to the synchronous PublishMessage for any backend that does not.
type asyncDurableWriter interface {
	StoreMessageAsync(queueName string, message *protocol.Message, onDurable func(error)) error
}

// DefaultSharedBodyThreshold is the body size (bytes) at/above which a durable
// fan-out to >=2 queues writes the body once (shared) instead of per queue, when
// EngineConfig.SharedBodyThreshold is unset (0). Matches the WAL default.
const DefaultSharedBodyThreshold = 4096

// sharedDurableWriter is the optional ITER5 storage capability that fuses a
// durable fan-out of one large body to N>=2 queues into a single group-commit
// unit (one shared BodyBlock + N reference records, one fsync) — writing the body
// ONCE instead of N times. Each sub's OnDurable fires after that copy is durable,
// exactly like StoreMessageAsync's completion, so per-queue visibility/confirm is
// unchanged. DisruptorStorage implements it; a backend that does not simply keeps
// the per-queue StoreMessageAsync loop (correct, just N body copies on disk).
type sharedDurableWriter interface {
	StoreSharedAsync(subs []interfaces.SharedDurableSub) error
}

// sharedBodyThreshold returns the configured shared-body size gate, defaulting to
// DefaultSharedBodyThreshold when unset. A publish shares its body only for a
// durable fan-out to >=2 queues whose body is at least this large.
func (b *StorageBroker) sharedBodyThreshold() int {
	if b.engineConfig.SharedBodyThreshold > 0 {
		return b.engineConfig.SharedBodyThreshold
	}
	return DefaultSharedBodyThreshold
}

// deleteIfPresent removes a message from storage and reports whether THIS call
// removed a message that was PRESENT — the ring CAS win OR the first
// acknowledgement of a spilled (WAL/segment-only) record (§8). Depth is
// decremented only by the winner, so at most one of {reaper drop, consumer
// head-check, drop-head evict, no-ack get, purge} decrements depth per tag,
// and a spilled message's removal now decrements depth exactly once (the old
// ring-CAS-only contract leaked depth for spilled tags at all five call sites).
func (b *StorageBroker) deleteIfPresent(queueName string, tag uint64) bool {
	if rd, ok := b.storage.(ringDeleterDetailed); ok {
		removed, _, _ := rd.DeleteMessageIfPresentDetailed(queueName, tag)
		return removed
	}
	if rd, ok := b.storage.(ringDeleter); ok {
		removed, _ := rd.DeleteMessageIfPresent(queueName, tag)
		return removed
	}
	_ = b.storage.DeleteMessage(queueName, tag)
	return true
}

// queueHasConsumers reports whether the queue currently has any registered
// consumer. Lock-free: queueConsumers stores an immutable slice replaced
// atomically on register/unregister, so reading its length never races.
func (b *StorageBroker) queueHasConsumers(queueName string) bool {
	if val, ok := b.queueConsumers.Load(queueName); ok {
		return len(val.([]*ConsumerState)) > 0
	}
	return false
}

func (b *StorageBroker) SetMetricsCollector(mc MetricsCollector) {
	b.metricsCollector = mc
}

// SetLogger wires a structured logger for the broker's cold-path warnings
// (e.g. a dead-letter dropped because its target is full or its store failed).
// nil disables broker logging. Not called on any hot path.
func (b *StorageBroker) SetLogger(l interfaces.Logger) {
	b.logger = l
}

// UpdateConsumerPrefetch re-gates one consumer, addressed by its
// broker-internal identity (Consumer.ID).
func (b *StorageBroker) UpdateConsumerPrefetch(consumerID string, prefetchCount uint16) {
	val, ok := b.activeConsumers.Load(consumerID)
	if !ok {
		return
	}
	state := val.(*ConsumerState)
	// prefetch-count 0 keeps the large finite cap; a non-zero value re-gates at
	// that limit. No-ack consumers always bypass the gate regardless.
	var limit int64
	if prefetchCount == 0 {
		limit = b.unlimitedPrefetchCap()
	} else {
		limit = int64(prefetchCount)
	}
	state.gate.updateLimit(limit)
	state.bypassGate.Store(state.consumer.NoAck)
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

// getQueueCreateMutex returns the per-queue-name mutex that serializes every
// operation which creates, re-validates, or destroys a queue's identity for
// one name. Same LoadOrStore-one-mutex-per-key pattern as
// getQueueConsumersMutex above.
//
// FIVE acquirers. The first four HOLD IT ACROSS the QueueState work rather than
// unlocking first — that is the invariant, and it is the opposite of what this
// comment said before the C1 fixes. The fifth, BindQueue, holds no QueueState
// and takes it purely to serialize against the fourth:
//   - getOrCreateQueueState — cold-path creation; defer-unlocks, so it is held
//     across createQueueStateLocked.
//   - DeclareQueue, existing-queue branch — re-reads the record under the lock
//     and holds it across all of declareExistingQueueLocked. The one path that
//     unlocks early is the deliberate fallthrough when the re-read finds the
//     record deleted; that path then re-acquires below.
//   - DeclareQueue, new-queue branch — holds it across the
//     ordinal-allocate-then-persist sequence AND the activeQueues.Store plus
//     createQueueStateLocked pair, unlocking only after the state is published.
//   - deleteQueueIncarnation's teardown — holds it from the incarnation
//     revalidation (its first act under the lock) through storage.DeleteQueue
//     inclusive, so every destructive step runs under the ordinal that was
//     validated, and the validation cannot go stale beneath them. That span
//     includes the binding sweep, which is what BindQueue below serializes
//     against.
//   - BindQueue — holds it across its queue-exists check and StoreBinding, so
//     a binding cannot be written for a queue whose teardown has already swept
//     its bindings. Held only across that pair, not across
//     updateDurableMetadata.
//
// WHY they hold rather than unlock: a delete landing between "validate the
// record" and "create the state" produced a live, publishable, record-less
// queue whose confirmed durable publishes were discarded at the next restart.
// Unlocking early anywhere above reopens that. See createQueueStateLocked.
//
// NON-REENTRANT. While holding it, call createQueueStateLocked, NEVER
// getOrCreateQueueState — the latter takes this same mutex and self-deadlocks.
//
// LOCK ORDERING RULE — this mutex is a LEAF among broker locks. While it is
// held you may descend only into the storage/metadata layer (GetQueue /
// StoreQueue, i.e. PersistentMetadataStore.mutex). You must NEVER acquire
// another broker-level lock beneath it — in particular getQueueConsumersMutex —
// and nothing may acquire this one while holding that. Two per-name mutexes
// taken in opposite orders on two queue names is a textbook deadlock, and the
// storage layer never calls back up into the broker, so keeping this a leaf is
// what makes the ordering trivially acyclic.
//
// SIBLING, NOT NESTED, with the consumers mutex: DeleteQueue takes the
// consumers mutex to stop and unregister consumers, and RELEASES it before
// acquiring this one for the teardown span. The two spans are strictly
// disjoint. Preserve that if you reorder teardown.
//
// If you add a call under this lock, re-verify the leaf rule and the
// sibling-not-nested property — and if you change who holds it across what,
// UPDATE THIS COMMENT. A lock-discipline comment that no longer matches the
// code is the same defect class as an unverified safety invariant: it is read
// precisely by the maintainer about to add a call under the lock.
//
// Acquirers are named by SYMBOL, deliberately, with no line numbers: grep for
// getQueueCreateMutex. This comment has twice shipped carrying references that
// had drifted under edits made above them — once while it was itself being
// fixed for being wrong. A stale line number here misdirects exactly the
// maintainer the comment exists to warn, so keep symbol names when you extend
// it; they are less precise but edits cannot rot them.
func (b *StorageBroker) getQueueCreateMutex(queueName string) *sync.Mutex {
	val, _ := b.queueCreateMuByName.LoadOrStore(queueName, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// SetQueueOwnerIfFree atomically claims ownership of a queue for a connection.
// Returns true if the caller is now the owner (either it was unowned or already
// owned by the same connection). Returns false if another connection already
// owns the queue.
func (b *StorageBroker) SetQueueOwnerIfFree(queueName, connID string) bool {
	val, loaded := b.queueOwners.LoadOrStore(queueName, connID)
	if !loaded {
		return true
	}
	return val.(string) == connID
}

// GetQueueOwner returns the connection ID that owns the given queue, or
// ("", false) if no owner is recorded.
func (b *StorageBroker) GetQueueOwner(queueName string) (string, bool) {
	val, ok := b.queueOwners.Load(queueName)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// GetQueuesOwnedByConnection returns the names of all queues owned by the
// given connection ID. Used during connection-close teardown to delete
// exclusive queues.
func (b *StorageBroker) GetQueuesOwnedByConnection(connID string) []string {
	var queues []string
	b.queueOwners.Range(func(key, value interface{}) bool {
		if value.(string) == connID {
			queues = append(queues, key.(string))
		}
		return true
	})
	return queues
}

// getOrCreateQueueState returns the queue state, creating it if needed. The
// fast path (queue already known) is a single lock-free sync.Map load — this
// is called on every publish, so it must stay cheap. Only the COLD path
// (first-ever access to this queue name in this process) takes the per-name
// mutex from getQueueCreateMutex: it must resolve the queue's composite-tag
// ordinal (broker/tag_packing.go) and call SetOrdinal BEFORE the QueueState
// is published to other goroutines. A plain LoadOrStore would publish the
// new QueueState first, letting a concurrent publisher mint a tag with
// ordinalBase still at its zero value — colliding with ordinal-0's tag space
// (see queueCreateMuByName's field doc). The re-check after acquiring the
// mutex handles two goroutines racing to create the SAME queue name; other
// queue names creating concurrently only contend briefly on the (distinct,
// per-name) mutex, not on each other's ordinal resolution.
func (b *StorageBroker) getOrCreateQueueState(queueName string) *QueueState {
	if val, ok := b.queueStates.Load(queueName); ok {
		return val.(*QueueState)
	}

	mu := b.getQueueCreateMutex(queueName)
	mu.Lock()
	defer mu.Unlock()

	return b.createQueueStateLocked(queueName)
}

// createQueueStateLocked creates and publishes the QueueState for queueName.
// The caller MUST already hold getQueueCreateMutex(queueName): the ordinal is
// resolved and SetOrdinal is called BEFORE the state is published via
// queueStates.Store, so a concurrent publisher can never observe it with
// ordinalBase still at its zero value. It re-checks queueStates first, so it
// is safe to call when another goroutine created the state while the caller
// was waiting on the mutex.
func (b *StorageBroker) createQueueStateLocked(queueName string) *QueueState {
	if val, ok := b.queueStates.Load(queueName); ok {
		return val.(*QueueState)
	}

	// A QueueState must never come into existence for a name that has no
	// metadata record. THIS nil-check is that guard — resolveQueueOrdinal
	// below persists unconditionally and takes the record as a non-nil
	// precondition, so without this check we would allocate an ordinal for a
	// name with nothing to write it to, leaving a live, publishable,
	// record-less queue. Recovery classifies records with no metadata as a
	// deleted queue's and discards them, so any durable publish confirmed
	// against such a state is lost at the next restart.
	//
	// In production the only way to reach here without a record is that a
	// concurrent DeleteQueue removed it (routing can resolve a binding just
	// before the delete lands). Return a CLOSED state rather than a live one:
	// every publish path bails on StopCh() — PublishMessage,
	// PublishMessageAsyncConfirm, fanoutSharedAsyncConfirm, fanoutSharedSync
	// and PublishMessageTx all check it — so the publish is REFUSED instead of
	// being confirmed and then silently discarded. Any new publish path MUST
	// keep that StopCh check or this guarantee is lost.
	//
	// It is deliberately NOT stored in queueStates and burns no ordinal, so a
	// later legitimate declare still creates a real state for this name.
	//
	// A read FAILURE is deliberately treated the same as absence here, unlike
	// DeclareQueue's re-check which fails the declare instead. The asymmetry is
	// intentional: this function cannot report an error (it returns only a
	// *QueueState, and giving it an error return would push a decision onto
	// ~19 call sites that mostly cannot act on it), and refusing is the safe
	// direction — a spurious ErrQueueClosed nack costs a publisher a retry,
	// whereas guessing "present" on an unreadable record risks the silent
	// data loss above. The unreadable case is logged so it cannot be mistaken
	// for a routine delete race, since the two are indistinguishable here.
	record, gerr := b.storage.GetQueue(queueName)
	if gerr != nil || record == nil {
		if gerr != nil && !errors.Is(gerr, interfaces.ErrQueueNotFound) && b.logger != nil {
			b.logger.Warn("queue metadata unreadable; refusing to create queue state (publishes will be refused until it reads cleanly)",
				interfaces.LogField{Key: "queue", Value: queueName},
				interfaces.LogField{Key: "error", Value: gerr})
		}
		dead := NewQueueState(b.computeDepthHighWM())
		dead.Close()
		return dead
	}

	// Create new queue state with a backpressure high-water mark derived from
	// the ring-buffer spill threshold. The depth gate (head - minAckCursor)
	// coordinates broker-level publisher backpressure with storage-level
	// spilling: once unacked depth reaches the spill threshold, publishers
	// block in WaitForCapacity until consumers ack and drain.
	//
	// The record read above is handed straight to resolveQueueOrdinal: it is
	// the same record that decision needs, and re-reading it there would be
	// both redundant and a second chance to observe a different value.
	ordinal, oerr := b.resolveQueueOrdinal(record)
	if oerr != nil {
		// Same safe direction as the unreadable-record branch above, and for a
		// stronger reason: a live QueueState whose ordinal was never persisted
		// confirms durable publishes that the NEXT restart reads back with
		// Ordinal==0, which is recovery Case D — a permanent refusal to boot.
		// Refusing the publish now costs a retry; accepting it bricks the
		// broker later.
		if b.logger != nil {
			b.logger.Error("could not resolve a delivery-tag ordinal for queue; refusing to create queue state (publishes will be refused)",
				interfaces.LogField{Key: "queue", Value: queueName},
				interfaces.LogField{Key: "error", Value: oerr})
		}
		dead := NewQueueState(b.computeDepthHighWM())
		dead.Close()
		return dead
	}

	newState := NewQueueState(b.computeDepthHighWM())
	newState.SetOrdinal(ordinal)
	b.queueStates.Store(queueName, newState)
	return newState
}

// GetQueuePolicy returns the resolved x-argument policy for a queue, or nil
// when the queue has no policy or no runtime state. Intended for inspection
// (tests, management) — hot paths should use QueueState.Policy() on a
// QueueState they already hold.
func (b *StorageBroker) GetQueuePolicy(queueName string) *QueuePolicy {
	val, ok := b.queueStates.Load(queueName)
	if !ok {
		return nil
	}
	return val.(*QueueState).Policy()
}

// computeDepthHighWM derives the per-queue backpressure high-water mark from
// the engine config: ring buffer size × spill threshold %. This is the depth
// (head - minAckCursor) at which publishers block or spill. Falls back to
// sensible defaults when config fields are unset.
func (b *StorageBroker) computeDepthHighWM() uint64 {
	if b.engineConfig.DepthHighWMOverride > 0 {
		return b.engineConfig.DepthHighWMOverride
	}
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
//
// The prefetch gate is bypassed entirely for no-ack consumers. Per AMQP 0.9.1,
// QoS prefetch limits are IGNORED when no-ack is set: a no-ack delivery is an
// implicit acknowledgement, so there is no outstanding-unacked window to bound.
// Gating a no-ack consumer would deadlock — it never acks, so gate credit would
// never be returned (SQ-0).
func (b *StorageBroker) consumerPollLoop(state *ConsumerState, queueState *QueueState) {
	defer close(state.done)
	parkTimer := time.NewTimer(queueState.parkTimeout)
	defer parkTimer.Stop()
	for {
		// Capture the bypass decision once per iteration so the acquire and any
		// matching release stay paired even if a concurrent basic.qos flips it.
		bypass := state.bypassGate.Load()
		if !bypass {
			if !state.gate.acquire(state.stopCh) {
				return
			}
		}

		tag, redelivered, ok := queueState.Claim(state.stopCh, parkTimer)
		if !ok {
			if !bypass {
				state.gate.release(1)
			}
			return
		}

		b.deliverMessage(queueState, state, tag, redelivered)
	}
}

// deliver is the single pre-send bookkeeping funnel (Hardening 2). It records
// EVERY structure that represents a live delivery, and NOTHING may reach the
// consumer before it returns — anything that sends before OnDeliver runs would
// be invisible to a concurrent multi-ack's unacked-set enumeration and would
// silently under-ack. In one place it:
//   - Stores the deliveryIndex ledger entry — the settle() key that every
//     terminal transition claims via LoadAndDelete, the credit-exactness
//     invariant's source of truth, AND the per-consumer ownership record that
//     consumer-cancel cleanup scans (deliveryIndex maps msgID -> Consumer.ID,
//     the broker-internal consumer identity — NOT the client-visible tag,
//     which is only channel-scoped and so cannot key a broker map).
//   - Claims queue-depth inflight (waiting-1, inflight+1).
//   - For manual-ack ONLY: marks the tag delivered on the AckCursor (OnDeliver,
//     which multi-ack enumerates) and stores its pending ack. No-ack deliveries
//     take no gate credit, join no unacked window, and get none of this
//     machinery (§4).
//
// The gate credit itself was already reserved by the poll loop's acquire; deliver
// does not touch it. It runs only after GetMessage has succeeded, so a gap tag
// never reaches here (its reserved credit is released tagless in deliverMessage).
func (b *StorageBroker) deliver(queueState *QueueState, state *ConsumerState, msgID uint64, redelivered bool) {
	b.deliveryIndex.Store(msgID, state.consumer.ID)
	queueState.ClaimInflight(msgID)
	if !state.consumer.NoAck {
		b.storage.DeliverToConsumer(state.queueName, state.consumer.ID, msgID)
		b.storage.StorePendingAck(&protocol.PendingAck{
			QueueName:   state.queueName,
			DeliveryTag: msgID,
			ConsumerTag: state.consumer.ID,
			Redelivered: redelivered,
		})
	}
}

// deliverMessage delivers a message to a consumer with blocking send
// This provides natural TCP backpressure when consumer is slow
// Messages are never put back in normal operation, preserving FIFO order
// Put-backs only occur on shutdown/cancellation (rare events) via Requeue
func (b *StorageBroker) deliverMessage(queueState *QueueState, state *ConsumerState, msgID uint64, redelivered bool) {
	noAck := state.consumer.NoAck

	message, err := b.storage.GetMessage(state.queueName, msgID)
	if err != nil {
		// Gap tag (recovered/acked message no longer in storage). deliver() —
		// which Stores the ledger entry — has not run, so no tag exists for any
		// terminal path to claim. The poll loop's reserved credit is therefore
		// tagless and released directly (it cannot race any owner, exactly like
		// the Claim-failure path); only the depth-frontier advance is
		// path-specific.
		state.releaseGate(1)
		queueState.GapSkipAdvance(msgID)
		return
	}

	// W4 SQ-9 delivery-time TTL head-check. The single shared Policy() load is
	// reused by TTL (here) and dead-lettering. Zero-cost when no TTL applies: the
	// EnqueueUnixMilli==0 gate (an anchor is stamped at publish iff a TTL applied)
	// short-circuits before any wall-clock read — asserted by
	// BenchmarkDeliver_NoTTL_ZeroCost. An expired message is dead-lettered
	// (reason=expired) or dropped instead of delivered.
	p := queueState.Policy()
	if message.EnqueueUnixMilli != 0 && messageExpired(message, p, b.ttlNowMillis()) {
		b.dropExpiredOnDelivery(queueState, state, message, msgID, p)
		return
	}

	delivery := &protocol.Delivery{
		Message:     message,
		DeliveryTag: msgID,
		Redelivered: redelivered,
		Exchange:    message.Exchange,
		RoutingKey:  message.RoutingKey,
		ConsumerID:  state.consumer.ID,
		NoAck:       noAck,
	}

	if redelivered && b.metricsCollector != nil {
		b.metricsCollector.RecordMessageRedelivered()
	}

	// Record all pre-send bookkeeping in one place before the message can reach
	// the consumer (see deliver()).
	b.deliver(queueState, state, msgID, redelivered)

	// SQ-11 max-length: the claimed message left the ready set (deliver ->
	// ClaimInflight). Subtract its bytes when the queue enforces
	// x-max-length-bytes. Reuses the single shared Policy() load `p` from the
	// SQ-9 TTL head-check above; one atomic load + a not-taken branch when
	// neither feature is set.
	subReadyBytesOnClaim(queueState, p, len(message.Body))

	select {
	case state.consumer.Messages <- delivery:
		if noAck {
			// No-ack fast path (§4): a successful send is an implicit ack. The
			// delivery never joined the unacked window — deliver() stored no
			// pending ack and never called OnDeliver — so settle here does the
			// minimum: claim the ledger entry (releaseGate is a no-op under
			// bypassGate) then drop the message from storage and decrement queue
			// depth. Deliberately NO DeletePendingAck, NO AckFromConsumer, and NO
			// min-ack recompute: that AckCursor machinery is pure overhead on the
			// throughput-critical no-ack path (the 2x-goal scenario). The
			// LoadAndDelete guard keeps this race-free against a concurrent
			// UnregisterConsumer requeue: whichever side wins owns the tag.
			if _, won := b.settle(msgID); won {
				b.storage.DeleteMessage(state.queueName, msgID)
				queueState.AckAdvance(msgID)
			}
		}
		// Manual-ack bookkeeping was already recorded in deliver().

	case <-state.stopCh:
		// The hand-off never happened (consumer cancelled/shutting down): undo
		// the pre-send bookkeeping and requeue for another consumer. Settle
		// funnels the LoadAndDelete + gate release; guarding on the win makes
		// this structurally safe against a concurrent ack/cancel that may have
		// already claimed the same tag (Hardening 1) — only the winner requeues.
		// finishNack's manual-ack cursor / pending-ack cleanup is a harmless
		// no-op for a no-ack delivery (which recorded neither).
		if _, won := b.settle(msgID); won {
			b.finishNack(queueState, state.queueName, state.consumer.ID, msgID, true)
		}
	}
}

// dropExpiredOnDelivery removes a message that failed the delivery-time TTL
// head-check instead of delivering it: it dead-letters the message
// (reason=expired) through the frozen W3 seam when a DLX is configured, then
// removes it and accounts queue depth. It runs BEFORE deliver()/ClaimInflight,
// so the message is still counted in `waiting`.
//
// Depth accounting is gated on winning the ring removal (deleteIfPresent==true),
// so it reconciles with the reaper: at most one of {reaper drop, this
// head-check} decrements depth per tag. On a win it does ReapDrop (waiting-1) —
// the tag was claimed off the tail but never marked inflight, so only the ready
// count moves. On a loss (the reaper already reaped this tag), it mirrors the gap
// path (release the reserved gate credit + GapSkipAdvance) and touches no depth
// counter — the reaper owns the decrement.
//
// Dead-lettering precedes removal (at-least-once, matching finishNack): a rare
// duplicate dead-letter is possible only in the consumer-registration transition
// where the reaper and head-check both act on the same queue, which RabbitMQ's
// at-least-once dead-letter semantics permit.
func (b *StorageBroker) dropExpiredOnDelivery(qs *QueueState, state *ConsumerState, msg *protocol.Message, tag uint64, p *QueuePolicy) {
	if p != nil && p.HasDeadLetterExchange {
		_ = b.deadLetter(p, state.queueName, msg, DeadLetterExpired)
	}
	won := b.deleteIfPresent(state.queueName, tag)
	state.releaseGate(1)
	if won {
		qs.ReapDrop()
		// SQ-11: the expired message left the ready set — account its bytes when
		// the queue enforces x-max-length-bytes, gated on the same ring-removal win
		// as the `waiting` decrement so the byte counter never drifts up.
		subReadyBytesOnClaim(qs, p, len(msg.Body))
	} else {
		qs.GapSkipAdvance(tag)
	}
}

// dropExpiredOnGet is the basic.get sibling of dropExpiredOnDelivery: it
// dead-letters (reason=expired) or drops a message that failed the TTL
// head-check during GetMessageForGet, gating the depth decrement on winning the
// ring removal so it reconciles with the reaper. basic.get holds no prefetch
// gate, so there is nothing to release.
func (b *StorageBroker) dropExpiredOnGet(qs *QueueState, queueName string, msg *protocol.Message, tag uint64, p *QueuePolicy) {
	if p != nil && p.HasDeadLetterExchange {
		_ = b.deadLetter(p, queueName, msg, DeadLetterExpired)
	}
	if b.deleteIfPresent(queueName, tag) {
		qs.ReapDrop()
		// SQ-11: the expired message left the ready set — account its bytes when
		// the queue enforces x-max-length-bytes, gated on the same ring-removal win
		// as the `waiting` decrement so the byte counter never drifts up.
		subReadyBytesOnClaim(qs, p, len(msg.Body))
	} else {
		qs.GapSkipAdvance(tag)
	}
}

// ackDelivered performs the storage and queue bookkeeping to acknowledge a
// single already-delivered message: delete it from storage, clear any pending
// ack, notify the ack cursor, sync the min-ack cursor for ring-overwrite safety
// and backpressure, and advance the queue depth frontier (inflight−1). The
// caller is responsible for the deliveryIndex LoadAndDelete guard and for
// releasing the prefetch gate (no-ack deliveries hold no gate credit). Shared
// by the manual single-message ack path and the no-ack implicit-ack path so the
// two stay in lockstep.
func (b *StorageBroker) ackDelivered(queueState *QueueState, queueName, consumerID string, deliveryTag uint64) {
	b.finishAck(queueState, queueName, consumerID, deliveryTag)
	queueState.SetMinAckCursor(b.storage.GetMinAckCursor(queueName))
}

// finishAck performs the per-tag storage and queue bookkeeping to positively
// acknowledge one already-settled delivery: delete the message, clear its
// pending ack, notify the AckCursor, and advance the queue depth frontier. It
// does NOT release the prefetch gate (the
// settle() winner already did) and does NOT sync the min-ack cursor — callers
// sync it once (per-ack for single ack via ackDelivered, once-after-the-loop for
// multi-ack) to keep the O(consumers) recompute off the per-tag path. The tag
// must already have been claimed via settle().
func (b *StorageBroker) finishAck(queueState *QueueState, queueName, consumerID string, deliveryTag uint64) {
	b.storage.DeleteMessage(queueName, deliveryTag)
	b.storage.DeletePendingAck(queueName, deliveryTag)
	b.storage.AckFromConsumer(queueName, consumerID, deliveryTag)
	queueState.AckAdvance(deliveryTag)
}

// finishNack performs the per-tag storage and queue bookkeeping to negatively
// acknowledge one already-settled delivery (basic.reject, basic.nack, and
// basic.recover requeue-all share it): remove it from the AckCursor unacked
// set, clear its pending ack, then either
// requeue it for redelivery or discard it and advance the depth frontier. Like
// finishAck it does NOT release the prefetch gate (settle() owns that) and does
// NOT sync the min-ack cursor. The tag must already have been claimed via
// settle().
func (b *StorageBroker) finishNack(queueState *QueueState, queueName, consumerID string, deliveryTag uint64, requeue bool) {
	b.storage.NackFromConsumer(queueName, consumerID, deliveryTag)
	b.storage.DeletePendingAck(queueName, deliveryTag)
	if requeue {
		// SQ-11: the message re-enters the ready set — restore its bytes when
		// the queue enforces x-max-length-bytes. Divergence (documented, W7):
		// unlike RabbitMQ we do NOT drop-head a requeue that pushes the queue
		// back over the cap; the transient overshoot is bounded by the requeued
		// set and re-trimmed on the next publish.
		b.restoreReadyBytesOnRequeue(queueState, queueName, deliveryTag)
		queueState.Requeue(deliveryTag)
		return
	}
	// requeue==false discard. Dead-letter (reason=rejected) BEFORE deleting the
	// source: republish StoreMessage precedes DeleteMessage so a crash between
	// them re-dead-letters on recovery (at-least-once; duplicates OK, matches
	// RabbitMQ). qs.Policy() is loaded STRICTLY here, inside the requeue==false
	// branch, so the requeue==true redelivery hot path (and ack, and publish)
	// pays zero: no load, no map lookup, one not-taken branch.
	b.deadLetterOnDiscard(queueState, queueName, deliveryTag)
	b.storage.DeleteMessage(queueName, deliveryTag)
	queueState.AckAdvance(deliveryTag)
}

// deadLetterOnDiscard dead-letters a single about-to-be-discarded message with
// reason=rejected when the queue has a dead-letter exchange configured. It is
// the shared reject/nack/basic.get-reject hook, invoked only on the cold
// requeue==false path. The Policy() guard short-circuits before any storage
// read when no DLX is set, so a queue without dead-lettering pays only one
// atomic load plus a not-taken branch.
func (b *StorageBroker) deadLetterOnDiscard(queueState *QueueState, queueName string, deliveryTag uint64) {
	p := queueState.Policy()
	if p == nil || !p.HasDeadLetterExchange {
		return
	}
	msg, err := b.storage.GetMessage(queueName, deliveryTag)
	if err != nil || msg == nil {
		return
	}
	_ = b.deadLetter(p, queueName, msg, DeadLetterRejected)
}

// discardNackedBatch settles and discards every unacked tag <= upTo for a
// multi-nack/reject with requeue=false. When the queue has a dead-letter
// exchange, the per-message DLX republishes are FANNED OUT concurrently and
// awaited before ANY source message is deleted. This:
//   - avoids serializing N synchronous WAL group-commit fsyncs on the
//     connection's frame goroutine — the concurrent StoreMessages coalesce into
//     shared group commits instead of N sequential fsync latencies; and
//   - preserves the at-least-once ordering: every republish is durably stored
//     before its source message is removed, so a crash re-dead-letters on
//     recovery (duplicates OK, matches RabbitMQ).
//
// A single Policy() load is hoisted before the loop (the hot-path zero-cost
// contract for multi-nack). The gate-credit release still funnels through
// settle() exactly once per tag.
func (b *StorageBroker) discardNackedBatch(queueState *QueueState, queueName, consumerID string, tags []uint64, upTo uint64) {
	p := queueState.Policy()
	if p == nil || !p.HasDeadLetterExchange {
		// No dead-lettering: discard inline in a single pass — no fan-out, no
		// scratch slice — so a multi-nack on a queue without a DLX pays only the
		// one hoisted Policy() load (the unset-feature zero-cost contract).
		for _, tag := range tags {
			if tag > upTo {
				continue
			}
			if _, won := b.settle(tag); !won {
				continue
			}
			b.storage.NackFromConsumer(queueName, consumerID, tag)
			b.storage.DeletePendingAck(queueName, tag)
			b.storage.DeleteMessage(queueName, tag)
			queueState.AckAdvance(tag)
		}
		return
	}

	// DLX configured: fan the per-message republishes out concurrently and await
	// them before deleting any source, so we neither serialize N synchronous WAL
	// fsyncs nor violate at-least-once ordering. The fan-out is BOUNDED by a
	// semaphore so a large unacked set (e.g. prefetch-0) cannot spawn thousands of
	// concurrent storage-I/O goroutines; the cap still lets enough writes overlap
	// to coalesce into shared WAL group commits.
	settled := make([]uint64, 0, len(tags))
	sem := make(chan struct{}, deadLetterFanoutLimit)
	var wg sync.WaitGroup
	for _, tag := range tags {
		if tag > upTo {
			continue
		}
		if _, won := b.settle(tag); !won {
			continue
		}
		b.storage.NackFromConsumer(queueName, consumerID, tag)
		b.storage.DeletePendingAck(queueName, tag)
		if msg, err := b.storage.GetMessage(queueName, tag); err == nil && msg != nil {
			wg.Add(1)
			sem <- struct{}{} // bounds in-flight republish goroutines
			go func(m *protocol.Message) {
				defer wg.Done()
				defer func() { <-sem }()
				_ = b.deadLetter(p, queueName, m, DeadLetterRejected)
			}(msg)
		}
		settled = append(settled, tag)
	}

	// Ordering barrier: all DLX republishes are durably stored before any source
	// message is deleted.
	wg.Wait()
	for _, tag := range settled {
		b.storage.DeleteMessage(queueName, tag)
		queueState.AckAdvance(tag)
	}
}

// settle removes a tag from the single delivery ledger (deliveryIndex) and
// releases exactly the one prefetch credit that its delivery consumed. It is the
// ONLY place manual-ack prefetch credit is returned. The LoadAndDelete is the
// linearization point: exactly one caller wins per tag, so the release is neither
// doubled, skipped, nor computed from a scanned count. Every terminal transition
// (single/multi ack, reject, single/multi nack, requeue, and the poll-loop
// cleanup paths) funnels through here so that gate.inflight is exact by
// construction — equal to the number of live deliveryIndex entries for the
// consumer at every quiescent point (design §2).
//
// Returns the owning ConsumerState (nil for a basic.get delivery, whose consumer
// tag is "", or a consumer that has since been unregistered) and whether this
// caller won the LoadAndDelete. A false return means the tag was already settled
// by a concurrent ack/cancel/requeue and the caller must treat it as a no-op.
//
// The one legitimate release that does NOT go through settle is the poll loop's
// Claim-failure path: it holds a reserved credit for which no tag was ever
// Stored, so it releases directly and cannot race any tag owner.
func (b *StorageBroker) settle(tag uint64) (*ConsumerState, bool) {
	v, won := b.deliveryIndex.LoadAndDelete(tag)
	if !won {
		return nil, false // lost to a concurrent ack / cancel / requeue — no-op
	}
	st, _ := b.activeConsumers.Load(v.(string))
	// Safe comma-ok assertion: when the consumer is absent (basic.get "" tag or a
	// consumer that was unregistered) st is a nil interface, so cs is a typed nil
	// and we must NOT dereference it. releaseGate is a no-op under bypassGate
	// (no-ack / prefetch-0), so no credit is returned where none was taken.
	cs, _ := st.(*ConsumerState)
	if cs != nil {
		cs.releaseGate(1)
	}
	return cs, true
}

// DeclareExchange creates or updates an exchange
func (b *StorageBroker) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	// Check if exchange already exists (lock-free)
	existing, err := b.storage.GetExchange(name)
	if err != nil && !errors.Is(err, interfaces.ErrExchangeNotFound) {
		return fmt.Errorf("failed to check existing exchange: %w", err)
	}

	// If exists, validate properties match (R8 assert_equivalence), in order:
	// type, durable, auto_delete, internal, then ONLY the alternate-exchange
	// argument (D3); other argument keys are ignored. The type check keeps
	// ErrExchangeTypeMismatch errors.Is-reachable (§2.3).
	if existing != nil {
		if existing.Kind != exchangeType {
			return inequivalent("exchange", name, "type", fmtBareVal(exchangeType), fmtBareVal(existing.Kind), ErrExchangeTypeMismatch)
		}
		if existing.Durable != durable {
			return inequivalent("exchange", name, "durable", fmtBool(durable), fmtBool(existing.Durable))
		}
		if existing.AutoDelete != autoDelete {
			return inequivalent("exchange", name, "auto_delete", fmtBool(autoDelete), fmtBool(existing.AutoDelete))
		}
		if existing.Internal != internal {
			return inequivalent("exchange", name, "internal", fmtBool(internal), fmtBool(existing.Internal))
		}
		o, oOK := existing.Arguments["alternate-exchange"]
		n, nOK := arguments["alternate-exchange"]
		if oOK != nOK {
			return inequivalent("exchange", name, "alternate-exchange", fmtArgVal(n, nOK), fmtArgVal(o, oOK))
		}
		if oOK && nOK && !argEquivalent(o, n) {
			return inequivalent("exchange", name, "alternate-exchange", fmtArgVal(n, nOK), fmtArgVal(o, oOK))
		}
		// Exchange already exists with matching properties
		return nil
	}

	if !isSupportedExchangeType(exchangeType) {
		return fmt.Errorf("unsupported exchange type: %s", exchangeType)
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

// ExchangeDeclarePassive returns the existing exchange record for name, never
// creating anything. Missing → errNoExchange(name) (R1 with_or_die → 404).
// Type/durable/auto-delete/internal/arguments in a passive frame are IGNORED:
// R1's passive clause never calls assert_equivalence.
func (b *StorageBroker) ExchangeDeclarePassive(name string) (*protocol.Exchange, error) {
	ex, err := b.storage.GetExchange(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return nil, errNoExchange(name)
		}
		return nil, fmt.Errorf("failed to check exchange '%s': %w", name, err)
	}
	return ex, nil
}

// DeleteExchange removes an exchange
func (b *StorageBroker) DeleteExchange(name string, ifUnused bool) error {
	_, err := b.storage.GetExchange(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			// D2/R4: deleting a missing exchange is delete-ok
			// (rabbit_exchange:ensure_deleted → ok).
			return nil
		}
		return err
	}

	// R4: in_use means "has bindings where this exchange is the SOURCE" —
	// queue bindings (GetExchangeBindings) OR exchange-to-exchange source
	// bindings (GetExchangeBindingsFrom).
	if ifUnused {
		qb, err := b.storage.GetExchangeBindings(name)
		if err != nil {
			return fmt.Errorf("failed to check exchange bindings: %w", err)
		}
		eb, err := b.storage.GetExchangeBindingsFrom(name)
		if err != nil {
			return fmt.Errorf("failed to check exchange bindings: %w", err)
		}
		if len(qb) > 0 || len(eb) > 0 {
			return semErr(exchangeRS(name)+" in use", ErrExchangeInUse)
		}
	}

	queueBindings, _ := b.storage.GetExchangeBindings(name)
	for _, bnd := range queueBindings {
		b.storage.DeleteBinding(bnd.QueueName, bnd.ExchangeName, bnd.RoutingKey)
	}

	fromBindings, _ := b.storage.GetExchangeBindingsFrom(name)
	for _, eb := range fromBindings {
		b.storage.DeleteExchangeBinding(eb.Source, eb.Destination, eb.RoutingKey)
	}

	allExchanges, _ := b.storage.ListExchanges()
	for _, exch := range allExchanges {
		exchBindings, _ := b.storage.GetExchangeBindingsFrom(exch.Name)
		for _, eb := range exchBindings {
			if eb.Destination == name {
				b.storage.DeleteExchangeBinding(eb.Source, eb.Destination, eb.RoutingKey)
				// §12: the deleted exchange was the DESTINATION of this e2e
				// binding; removing it may orphan an auto-delete SOURCE.
				b.maybeAutoDeleteExchange(eb.Source)
			}
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
	if existing != nil {
		// The read above is lock-free and therefore only a fast pre-check: a
		// concurrent DeleteQueue may remove the record before we act on it.
		// Re-read under the per-name create mutex and hold it across the whole
		// redeclare, so the record cannot vanish between validating it and
		// creating the QueueState. Acting on the stale pointer would re-install
		// a deleted queue and then mint a QueueState whose freshly allocated
		// ordinal is never persisted — a live, publishable, record-less queue
		// whose confirmed durable publishes are discarded at the next restart.
		mu := b.getQueueCreateMutex(name)
		mu.Lock()
		fresh, ferr := b.storage.GetQueue(name)
		if ferr == nil && fresh != nil {
			q, derr := b.declareExistingQueueLocked(fresh, durable, autoDelete, exclusive, arguments)
			mu.Unlock()
			return q, derr
		}
		// Only a genuine "no such record" may fall through to the new-queue
		// path. That path allocates and PERSISTS a fresh ordinal band, so
		// treating a transient read failure as "deleted" would rewrite the
		// ordinal of a queue that is still very much alive — re-banding its
		// delivery tags and orphaning every record already written under the
		// old band. Fail the declare instead; a redeclare is safe to retry.
		if ferr != nil && !errors.Is(ferr, interfaces.ErrQueueNotFound) {
			mu.Unlock()
			return nil, fmt.Errorf("failed to re-check existing queue '%s': %w", name, ferr)
		}
		// The record was deleted out from under us. Fall through to the
		// new-queue path, which re-acquires the mutex and persists a fresh
		// record + ordinal (a new incarnation, with its own ordinal band).
		mu.Unlock()
	}

	// Resolve the typed queue policy from x-arguments ONCE, at declare time
	// (SQ-7). Invalid known arguments fail the declare before anything is
	// stored; the server layer maps ErrInvalidQueueArgument to a channel-level
	// 406 PreconditionFailed. This is also the recovery path: durable queues
	// are re-declared through this method on restart, so the policy is
	// re-resolved from the persisted arguments by the same function.
	policy, err := ResolveQueuePolicy(arguments)
	if err != nil {
		return nil, fmt.Errorf("queue '%s': %w", name, err)
	}

	// New queue: serialize the whole check-allocate-persist sequence per
	// queue NAME. Two connections idempotently declaring the SAME brand-new
	// queue (the most common AMQP client pattern) is completely ordinary; the
	// lock-free check above is only a fast pre-check, not a guarantee — both
	// callers can reach here concurrently for the same name having each seen
	// "not found". Without this lock, each would build its own queue object
	// (Ordinal 0) and call StoreQueue independently: even with StoreQueue's
	// write-once-ordinal invariant (storage/persistent_metadata.go) closing
	// the specific "second write reverts Ordinal to 0" failure, the loser's
	// StoreQueue call would still be redundant and racy against the winner's
	// ordinal resolution below. Locking per name means only concurrent
	// creators of the SAME name ever contend; other names' first-ever
	// creation (StoreQueue's atomicWrite fsyncs twice, ~18ms on macOS)
	// proceeds independently. See queueCreateMuByName's field doc.
	mu := b.getQueueCreateMutex(name)
	mu.Lock()

	// Re-check under the lock: another goroutine may have declared (and
	// released the lock for) this exact queue name while this one was
	// waiting on it. declareExistingQueueLocked requires the create mutex to
	// stay held across the whole redeclare (see its doc comment), so unlock
	// AFTER the call, not before — unlocking first would let a concurrent
	// DeleteQueue land between the read above and the QueueState creation
	// inside declareExistingQueueLocked, which is the exact race this whole
	// change eliminates.
	if existing, gerr := b.storage.GetQueue(name); gerr == nil && existing != nil {
		q, derr := b.declareExistingQueueLocked(existing, durable, autoDelete, exclusive, arguments)
		mu.Unlock()
		return q, derr
	}

	// Create new queue and resolve its composite-tag ordinal (see
	// broker/tag_packing.go) BEFORE the first persist, so the record is
	// written to disk exactly once, already carrying its final ordinal —
	// never Ordinal:0 followed by a later repair write that a concurrent
	// reader could observe mid-transition.
	queue := protocol.NewQueue(name, durable, autoDelete, exclusive, arguments)

	ordinal, aerr := b.allocateOrdinal()
	if aerr != nil {
		mu.Unlock()
		// Unreachable in any real deployment (see tag_packing.go); a silent
		// wrap here would be cross-queue delivery-tag collision, so fail the
		// declare outright instead of proceeding with an invalid ordinal.
		return nil, fmt.Errorf("queue '%s': %w", name, aerr)
	}
	queue.Ordinal = ordinal

	// Store queue. A failed persist here is a HARD failure (not
	// best-effort): the ordinal was just allocated for this queue and must
	// not be silently lost, so the declare fails rather than proceeding with
	// an unpersisted identity.
	if serr := b.storage.StoreQueue(queue); serr != nil {
		mu.Unlock()
		return nil, serr
	}

	// Publish the routing-cache entry and create the QueueState while STILL
	// holding the per-name create mutex. Unlocking first would let a concurrent
	// DeleteQueue's activeQueues.Delete be overwritten by the Store below,
	// leaving a permanent phantom routing entry with no metadata record and no
	// QueueState that the default exchange keeps resolving to.
	// createQueueStateLocked assumes the lock is held (getOrCreateQueueState
	// would retake it and self-deadlock — the mutex is not reentrant).
	b.activeQueues.Store(name, queue)
	qs := b.createQueueStateLocked(name)
	mu.Unlock()

	qs.SetPolicy(policy)
	// W4 SQ-9: start the per-queue TTL/x-expires reaper (no-op unless the policy
	// needs it) and seed the x-expires idle clock from declare time.
	b.maybeStartReaper(name, qs)

	// Update durable entity metadata if this is a durable queue
	if queue.Durable {
		err = b.updateDurableMetadata()
		if err != nil {
			// Log but don't fail - metadata update is not critical for operation
		}
	}

	return queue, nil
}

// declareExistingQueueLocked handles the redeclare path for a queue name
// that already has a persisted record — reached either from the lock-free
// fast check at the top of DeclareQueue, or from the re-check inside the
// per-name create lock after losing a race to create the same brand-new
// queue name (see DeclareQueue's new-queue branch). The caller MUST already
// hold getQueueCreateMutex(existing.Name) and must keep holding it for the
// duration of this call: that is what makes "the record still exists" and
// "the QueueState now exists" atomic against a concurrent DeleteQueue, which
// is why this function calls createQueueStateLocked (which assumes the lock
// is held) rather than getOrCreateQueueState (which would try to take it
// again and self-deadlock).
func (b *StorageBroker) declareExistingQueueLocked(existing *protocol.Queue, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	// RabbitMQ redeclare equivalence (R6/R7), checked in order; the first
	// failure wins. Equivalence runs BEFORE any side effect (activeQueues,
	// QueueState, reaper), so a refused redeclare changes nothing.
	//
	// 1. exclusive is the strict 405 check (D4); cross-CONNECTION ownership
	//    stays in the server (SetQueueOwnerIfFree).
	if existing.Exclusive != exclusive {
		return nil, semErr("cannot obtain exclusive access to locked "+queueRS(existing.Name)+
			". It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
			ErrQueueLocked)
	}
	// 2/3. durable, then auto_delete.
	if existing.Durable != durable {
		return nil, inequivalent("queue", existing.Name, "durable", fmtBool(durable), fmtBool(existing.Durable))
	}
	if existing.AutoDelete != autoDelete {
		return nil, inequivalent("queue", existing.Name, "auto_delete", fmtBool(autoDelete), fmtBool(existing.AutoDelete))
	}
	// 4. arguments: ONLY the queue type's declared argument keys are compared
	// (D3, R6 classic queue_arguments); keys outside the list are ignored.
	for _, k := range classicQueueArgumentKeys {
		o, oOK := existing.Arguments[k]
		n, nOK := arguments[k]
		if !oOK && !nOK {
			continue
		}
		// R7 assert_field_equivalence: x-queue-type "classic" vs absent is ok.
		if k == "x-queue-type" && (!oOK || !nOK) {
			present, presentOK := o, oOK
			if !presentOK {
				present = n
			}
			if s, isStr := present.(string); isStr && s == "classic" {
				continue
			}
		}
		if oOK != nOK {
			return nil, inequivalent("queue", existing.Name, k, fmtArgVal(n, nOK), fmtArgVal(o, oOK))
		}
		if !argEquivalent(o, n) {
			return nil, inequivalent("queue", existing.Name, k, fmtArgVal(n, nOK), fmtArgVal(o, oOK))
		}
	}

	// Add to active cache (lock-free)
	b.activeQueues.Store(existing.Name, existing)

	// Ensure queue state exists and carries the resolved policy. The stored
	// (original) arguments win: equivalence above guarantees the redeclare
	// carries nothing new, so they must not overwrite the policy either. This
	// branch also covers recovery when the storage backend already holds the
	// durable queue across restarts. Resolution errors are tolerated here
	// (nil policy) so queues persisted before validation existed remain
	// usable.
	qs := b.createQueueStateLocked(existing.Name)
	if policy, perr := ResolveQueuePolicy(existing.Arguments); perr == nil {
		qs.SetPolicy(policy)
	}
	// W4 SQ-9: a (re)declare is queue "use" (resets the x-expires idle clock)
	// and must ensure the reaper is running (e.g. on durable recovery, whose
	// queues re-enter through this branch).
	qs.MarkActivity(b.ttlNowMillis())
	b.maybeStartReaper(existing.Name, qs)

	return existing, nil
}

// classicQueueArgumentKeys is RabbitMQ's classic-queue argument list (R6,
// rabbit_classic_queue.erl capabilities/0 queue_arguments), in the fixed
// comparison order of §2.2. Redeclare equivalence compares ONLY these keys;
// plugin/client keys outside the list are ignored (D3).
var classicQueueArgumentKeys = []string{
	"x-expires",
	"x-message-ttl",
	"x-dead-letter-exchange",
	"x-dead-letter-routing-key",
	"x-max-length",
	"x-max-length-bytes",
	"x-max-priority",
	"x-overflow",
	"x-queue-mode",
	"x-queue-version",
	"x-single-active-consumer",
	"x-queue-type",
	"x-queue-master-locator",
	"x-queue-leader-locator",
}

// QueueDeclarePassive returns the existing queue record for name, never
// creating anything (no record, no QueueState, no ordinal, no reaper).
// Missing → errNoQueue(name) (R1 with_or_die → 404). It resets the x-expires
// idle clock when a QueueState already exists and the policy has x-expires (a
// declare renews the lease; RabbitMQ's stat path re-arms the expiry timer).
// Durable/exclusive/auto-delete/arguments in a passive frame are IGNORED
// (R1's passive clause never calls assert_equivalence).
func (b *StorageBroker) QueueDeclarePassive(name string) (*protocol.Queue, error) {
	q, err := b.storage.GetQueue(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return nil, errNoQueue(name)
		}
		return nil, fmt.Errorf("failed to check queue '%s': %w", name, err)
	}
	// Deliberately NOT getOrCreateQueueState: a passive declare creates no
	// runtime state. An existing QueueState just gets its lease renewed.
	if qsVal, ok := b.queueStates.Load(name); ok {
		qs := qsVal.(*QueueState)
		if p := qs.Policy(); p != nil && p.HasQueueExpires {
			qs.MarkActivity(b.ttlNowMillis())
		}
	}
	return q, nil
}

// DeleteQueue removes a queue. It captures the identity (Ordinal) of the
// incarnation that currently holds `name` and delegates to
// deleteQueueIncarnation; see that function's doc comment for why the
// teardown is scoped to an incarnation rather than to the bare name.
//
// Note for callers holding an identity captured EARLIER: calling THIS function
// re-resolves the identity fresh, right here, which defeats exactly the
// protection those callers need against acting on a stale name. They must call
// deleteQueueIncarnation directly with their own previously-captured ordinal.
// Two such callers were converted — the reaper's idle decision
// (broker/queue_reaper.go) and the last-consumer-cancel auto-delete in this
// file. That list is NOT exhaustive and must not be read as a closed set: at
// least one more survives unconverted, server.handleConnectionClose's
// exclusive-queue teardown (server/server.go), which captures a list of names
// and then deletes them one at a time by name. It is pre-existing rather than
// introduced here, and is on the Step 6 backlog.
//
// Client-visible semantic, since no other comment says it: because the capture
// here is outside the create mutex and the revalidation is inside it, a
// delete+redeclare landing in that gap makes this function a silent no-op —
// it returns (0, nil) and the caller replies delete-ok while a queue of that
// name still exists. That is the correct safety trade, but queue.delete is no
// longer guaranteed to have deleted anything.
func (b *StorageBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error) {
	record, err := b.storage.GetQueue(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			// D2/R3: deleting a missing queue is delete-ok with count 0
			// (rabbit_amqqueue:delete_with/6 maps not_found → {ok, 0}).
			return 0, nil
		}
		return 0, err
	}
	return b.deleteQueueIncarnation(name, record.Ordinal, ifUnused, ifEmpty)
}

// deleteQueueIncarnation tears down the queue incarnation identified by
// (name, wantOrdinal) — never just "whatever currently answers to name".
// DeleteQueue destroys the metadata record, the ring and the bindings, so
// acting on a successor incarnation (one that took the same name after a
// delete+redeclare race) would annihilate a live queue whose durable
// publishes were already confirmed. See H1/H2 in the Step 6 teardown-identity
// design: Queue.Ordinal is a per-incarnation identity (monotone, never reused
// within a run — allocateOrdinal), so pairing every destructive call with the
// ordinal its caller actually intends to destroy is what makes a stale
// caller's delete a safe no-op instead of a silent cross-incarnation strand.
func (b *StorageBroker) deleteQueueIncarnation(name string, wantOrdinal uint64, ifUnused, ifEmpty bool) (int, error) {
	// R5 order: if-empty is checked BEFORE if-unused. is_empty means
	// len == 0 where len counts READY messages only (depth = len +
	// pending_acks): delivered-unacked messages do NOT block if-empty.
	// WaitingCount is the ready set (the same count declare-ok reports).
	if ifEmpty {
		if qv, ok := b.queueStates.Load(name); ok && qv.(*QueueState).WaitingCount() > 0 {
			return 0, semErr(queueRS(name)+" not empty", ErrQueueNotEmpty)
		}
	}

	// The consumer sweep runs BEFORE the incarnation is revalidated, and is
	// deliberately left that way.
	//
	// Stated precisely, because the accurate reason and the convenient one
	// differ: the leaf rule FORBIDS nesting the consumers mutex under the
	// create mutex (see getQueueCreateMutex), and that rule exists so the
	// ordering stays trivially acyclic without anyone re-deriving it. It is not
	// that a cycle exists today — no consumers-mutex holder currently acquires
	// the create mutex, so the nesting would deadlock nothing right now. It is
	// that re-deriving a lock order is exactly the kind of change that must not
	// ride along inside a correctness fix. A second unsynchronized check ahead
	// of the sweep was tried and removed instead: it narrows the window without
	// closing it, and no test can attribute it.
	//
	// So a superseded caller still stops the successor's consumers on its way
	// to being refused. That is disruption, not loss: the successor's confirmed
	// messages and its dispatch cursors are untouched, and consumers reconnect.
	// Unchanged from before this fix; logged as a follow-up.
	// if-unused consults the LIVE consumer registry (R5 is_unused == consumer
	// count 0). The check runs under the SAME consumers-mutex hold that
	// performs the sweep, so a consumer registering concurrently either lands
	// before the check (refused) or after the sweep (the pre-existing
	// register-after-delete window, spec §16).
	mu := b.getQueueConsumersMutex(name)
	mu.Lock()
	if ifUnused {
		if val, ok := b.queueConsumers.Load(name); ok && len(val.([]*ConsumerState)) > 0 {
			mu.Unlock()
			return 0, semErr(queueRS(name)+" in use", ErrQueueInUse)
		}
	}
	if val, ok := b.queueConsumers.Load(name); ok {
		consumers := val.([]*ConsumerState)
		for _, state := range consumers {
			state.gate.stop()
			state.stopOnce.Do(func() { close(state.stopCh) })
			b.activeConsumers.Delete(state.consumer.ID)
		}
	}
	b.queueConsumers.Delete(name)
	mu.Unlock()

	// Hold the per-name create mutex across EVERY destructive step, and
	// revalidate the incarnation as the first thing under it.
	//
	// The lock is what makes the check load-bearing rather than another TOCTOU:
	// a successor can only come into existence through DeclareQueue, which
	// holds this same mutex across its whole body, so no redeclare can
	// interleave between the check and the last destructive step below. Without
	// the lock, two concurrent deletes both capturing ordinal N would both
	// validate N and both proceed, and the loser would run its teardown by name
	// against the N+1 that a redeclare had meanwhile installed.
	//
	// It is also what closes the older, narrower window this span was
	// originally opened for: a concurrent getOrCreateQueueState cold path
	// observing the metadata record AFTER the QueueState has been removed but
	// BEFORE the record is deleted would reuse the dying queue's ordinal
	// without persisting it, leaving a live publishable queue with no metadata
	// record — whose durable publishes are confirmed and then discarded at the
	// next restart as a deleted queue.
	//
	// §12: exchanges whose bindings are swept below are collected for
	// auto-delete AFTER the create mutex is released (defers run LIFO, so this
	// runs after cmu.Unlock): the leaf-lock section stays free of the extra
	// DeleteExchange work (leaf rule, see getQueueCreateMutex).
	var touched []string
	defer func() {
		for _, x := range touched {
			b.maybeAutoDeleteExchange(x)
		}
	}()
	cmu := b.getQueueCreateMutex(name)
	cmu.Lock()
	defer cmu.Unlock()

	record, err := b.storage.GetQueue(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if record.Ordinal != wantOrdinal {
		return 0, nil
	}

	// The purge band comes from wantOrdinal, not from a name lookup: it is a
	// total function of the identity this call was given, so it cannot resolve
	// to a successor's ring even if every assumption above were wrong.
	//
	// Defence-in-depth, and no test attributes it HERE: storage.DeleteQueue
	// four lines below drops the whole ring anyway, so widening this band to
	// the entire tag space leaves the suite green (measured). What the M2
	// mutation arm reddens on is a COUNT — DeleteQueue must return the number
	// purged — which proves the band is a live function of wantOrdinal, not
	// that band-scoping prevents anything at this call site. The band that
	// carries the safety property is purgeIncarnation's, on the client-facing
	// queue.purge path.
	purgedCount, _ := b.storage.PurgeQueue(name, PackTag(wantOrdinal, 0), PackTag(wantOrdinal, SeqMask))

	bindings, err := b.storage.GetQueueBindings(name)
	if err == nil {
		for _, binding := range bindings {
			b.storage.DeleteBinding(binding.QueueName, binding.ExchangeName, binding.RoutingKey)
			touched = append(touched, binding.ExchangeName)
		}
	}

	// Persisted-consumer sweep. The live consumers were already stopped and
	// removed above; this covers records in the metadata store. Keyed by
	// Consumer.ID, matching how RegisterConsumer stored them.
	//
	// NOTE, verified not assumed: with the current metadata store this loop
	// never executes. PersistentMetadataStore.StoreConsumer is a documented
	// no-op ("Consumer registration is ephemeral"), so nothing ever writes the
	// consumers/<queue>/ directory that GetQueueConsumers reads, and it always
	// returns an empty slice. That fact no longer disables anything: WS-D §3
	// moved the ifUnused check onto the LIVE consumer registry (queueConsumers)
	// above, which is authoritative for running consumers.
	consumers, err := b.storage.GetQueueConsumers(name)
	if err == nil {
		for _, consumer := range consumers {
			b.storage.DeleteConsumer(name, consumer.ID)
			b.activeConsumers.Delete(consumer.ID)
		}
	}

	b.activeQueues.Delete(name)
	b.queueOwners.Delete(name)
	if qval, qok := b.queueStates.LoadAndDelete(name); qok {
		qval.(*QueueState).Close()
	}
	if err := b.storage.DeleteQueue(name); err != nil {
		return purgedCount, err
	}
	return purgedCount, nil
}

// BindQueue binds a queue to an exchange
func (b *StorageBroker) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	// Validate exchange exists (lock-free)
	bindEx, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return errNoExchange(exchangeName)
		}
		return err
	}

	// Headers-exchange bindings validate x-match at bind time (R9
	// validate_binding), BEFORE anything is stored.
	if bindEx.Kind == "headers" {
		if verr := validateHeadersBinding(arguments); verr != nil {
			return verr
		}
	}

	// Validate the queue exists and store the binding under the queue's create
	// mutex, so the check and the store observe the same incarnation.
	//
	// A binding row carries no identity of its own to revalidate against — it
	// is name-keyed by construction and dies with the queue — so mutual
	// exclusion is the only available mechanism here. Lock-free, a delete
	// landing between the two lines left a binding row addressed to a queue
	// that no longer exists: deleteQueueIncarnation's binding sweep runs inside
	// this same mutex, so it had already passed by the time StoreBinding wrote.
	// No later delete of that name removes the row.
	//
	// That row is an availability defect, not accounting residue. A publish is
	// all-or-nothing across its routed target set — PublishMessage returns
	// ErrQueueClosed for the WHOLE publish as soon as any one routed target's
	// QueueState is closed — so one ghost row permanently poisons its
	// (exchange, routing key) for every healthy queue bound to it.
	//
	// Only the check→store pair is held. updateDurableMetadata below re-reads
	// the whole queue and exchange lists and rewrites the metadata file; it
	// needs no incarnation guarantee, and holding this mutex across it would
	// serialize every declare of this name behind a full metadata rewrite.
	cmu := b.getQueueCreateMutex(queueName)
	cmu.Lock()
	if _, err = b.storage.GetQueue(queueName); err != nil {
		cmu.Unlock()
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return errNoQueue(queueName)
		}
		return err
	}
	err = b.storage.StoreBinding(queueName, exchangeName, routingKey, arguments)
	cmu.Unlock()
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

// maybeAutoDeleteExchange deletes exchange `name` if it is auto-delete and
// has no remaining bindings where it is the SOURCE (queue bindings + e2e) —
// RabbitMQ's maybe_auto_delete on the source (§12). Storage-only; takes no
// broker lock. Centralised so every binding-removal path (unbind, queue
// delete, destination-exchange delete) gets the same cleanup.
func (b *StorageBroker) maybeAutoDeleteExchange(name string) {
	if name == "" {
		return
	}
	ex, err := b.storage.GetExchange(name)
	if err != nil || !ex.AutoDelete {
		return
	}
	qb, _ := b.storage.GetExchangeBindings(name)
	eb, _ := b.storage.GetExchangeBindingsFrom(name)
	if len(qb) == 0 && len(eb) == 0 {
		_ = b.DeleteExchange(name, false)
	}
}

// UnbindQueue removes a binding between queue and exchange
func (b *StorageBroker) UnbindQueue(queueName, exchangeName, routingKey string) error {
	if err := b.storage.DeleteBinding(queueName, exchangeName, routingKey); err != nil {
		return err
	}

	b.maybeAutoDeleteExchange(exchangeName)

	return nil
}

// BindExchange binds a destination exchange to a source exchange with a routing key.
// Messages published to the source exchange with a matching routing key are routed
// to the destination exchange, which then routes to its bound queues (or further
// exchanges). Exchange-to-exchange bindings can form chains but MUST NOT form cycles.
func (b *StorageBroker) BindExchange(destination, source, routingKey string, arguments map[string]interface{}) error {
	if source == "" {
		return fmt.Errorf("cannot bind from default exchange")
	}
	if destination == "" {
		return fmt.Errorf("cannot bind to default exchange")
	}

	srcExchange, err := b.storage.GetExchange(source)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return errNoExchange(source)
		}
		return err
	}

	// Headers-SOURCE e2e bindings validate x-match at bind time (R9), before
	// anything is stored.
	if srcExchange.Kind == "headers" {
		if verr := validateHeadersBinding(arguments); verr != nil {
			return verr
		}
	}

	_, err = b.storage.GetExchange(destination)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return errNoExchange(destination)
		}
		return err
	}

	// §11 (R11): binding TO an internal exchange is exactly the supported use
	// of internal exchanges — the old rejection here is removed. Publishing
	// DIRECTLY to an internal exchange is what RabbitMQ forbids, and that is
	// enforced at the publish entry points (ValidatePublishTarget et al).

	if source == destination {
		return fmt.Errorf("exchange binding cycle detected: %s -> %s", destination, source)
	}

	if b.wouldCreateCycle(destination, source) {
		return fmt.Errorf("exchange binding cycle detected: binding %s -> %s would create a cycle", destination, source)
	}

	return b.storage.StoreExchangeBinding(source, destination, routingKey, arguments)
}

// UnbindExchange removes an exchange-to-exchange binding.
func (b *StorageBroker) UnbindExchange(destination, source, routingKey string) error {
	if err := b.storage.DeleteExchangeBinding(source, destination, routingKey); err != nil {
		return err
	}

	b.maybeAutoDeleteExchange(source)

	return nil
}

// wouldCreateCycle checks if binding destination→source would create a cycle.
// It performs a DFS from source following exchange-to-exchange binding edges
// (source→destination) to see if destination is already reachable from source.
// If so, adding destination→source would close the cycle.
func (b *StorageBroker) wouldCreateCycle(destination, source string) bool {
	visited := make(map[string]bool)
	return b.reachableFrom(destination, source, visited)
}

func (b *StorageBroker) reachableFrom(current, target string, visited map[string]bool) bool {
	if current == target {
		return true
	}
	if visited[current] {
		return false
	}
	visited[current] = true

	bindings, err := b.storage.GetExchangeBindingsFrom(current)
	if err != nil {
		return false
	}
	for _, eb := range bindings {
		if b.reachableFrom(eb.Destination, target, visited) {
			return true
		}
	}
	return false
}

// RegisterConsumer registers a new consumer for a queue under the
// broker-internal consumer identity consumerID (see protocol.Consumer.ID) —
// NOT under the client-visible consumer tag, which AMQP 0-9-1 scopes per
// channel and which two clients may therefore legitimately share.
//
// consumerID must equal consumer.ID: there is then exactly one identity in play
// and every downstream map (activeConsumers, deliveryIndex, the storage ack
// cursors, the connection's delivery-routing table) is keyed by the same string.
// Disagreement is a programming error and is rejected rather than silently
// registering under two names.
//
// This function NEVER WRITES to *consumer. By the time the server calls it the
// consumer has already been published into channel.Consumers, where the
// connection's delivery loop reads consumer.ID under the channel mutex — a
// mutating fallback here would be a write to a shared struct outside that
// mutex. It was previously safe only because the branch happened to be dead on
// the server path, which is safety by accident. Callers that used to rely on
// the fallback (broker unit tests registering under a bare tag) set consumer.ID
// themselves; see registerConsumer in this package's tests.
func (b *StorageBroker) RegisterConsumer(queueName, consumerID string, consumer *protocol.Consumer) error {
	if consumerID == "" {
		return errors.New("consumer identity must not be empty")
	}
	if consumer.ID != consumerID {
		return fmt.Errorf("consumer identity mismatch: registering as %q but consumer.ID is %q",
			consumerID, consumer.ID)
	}

	_, err := b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return errNoQueue(queueName)
		}
		return err
	}

	if ownerVal, ok := b.queueOwners.Load(queueName); ok {
		ownerID := ownerVal.(string)
		if consumer.Channel != nil && consumer.Channel.Connection != nil && ownerID != consumer.Channel.Connection.ID {
			return errors.New("queue '" + queueName + "' is exclusive to another connection")
		}
	}

	mu := b.getQueueConsumersMutex(queueName)
	mu.Lock()
	if consumer.Exclusive {
		if val, ok := b.queueConsumers.Load(queueName); ok && len(val.([]*ConsumerState)) > 0 {
			mu.Unlock()
			return errors.New("exclusive consumer cannot join queue with existing consumers")
		}
	} else {
		if val, ok := b.queueConsumers.Load(queueName); ok {
			for _, c := range val.([]*ConsumerState) {
				if c.consumer.Exclusive {
					mu.Unlock()
					return errors.New("cannot add consumer to queue with exclusive consumer")
				}
			}
		}
	}
	mu.Unlock()

	// Persist BEFORE publishing any in-memory state. Everything below this
	// point — the ConsumerState, activeConsumers, the ack cursor, the
	// queueConsumers entry, the poll-loop goroutine — is unconditional and
	// cannot fail, so this is the last place registration can go wrong, and
	// failing here leaves nothing behind to clean up.
	//
	// The persist used to run LAST, after the goroutine was already spawned, so
	// an error returned a partially registered consumer: a live poll loop, a
	// ConsumerState in activeConsumers, a registered ack cursor and a
	// queueConsumers entry, none of which any caller could see to undo. That
	// was latent rather than live only because every StoreConsumer
	// implementation is currently a documented no-op returning nil — a
	// correctness bug held harmless by dead code. Ordering it write-ahead
	// removes the failure window instead of compensating for it after the fact.
	if err := b.storage.StoreConsumer(queueName, consumerID, consumer); err != nil {
		return fmt.Errorf("failed to store consumer: %w", err)
	}

	queueState := b.getOrCreateQueueState(queueName)

	// W4 SQ-9: registering a consumer is queue "use" — it resets the x-expires
	// idle clock so an about-to-expire queue is kept alive the instant a consumer
	// attaches, without waiting for the reaper's next periodic re-check. Guarded
	// by HasQueueExpires so a queue without x-expires pays only one atomic load.
	if p := queueState.Policy(); p != nil && p.HasQueueExpires {
		queueState.MarkActivity(b.ttlNowMillis())
	}

	// A no-ack consumer ignores prefetch entirely (per AMQP 0.9.1) and bypasses
	// the gate. For an explicit prefetch-count we gate at that limit. For
	// prefetch-count 0 the spec says "no limit"; we apply a large finite cap
	// rather than a true unbounded gate, because an unbounded unacked set makes
	// the ack cursor's lowest-unacked rescan O(n) per ack (O(n^2) overall). The
	// cap still bounds the set so per-ack work stays small.
	prefetchCount := consumer.PrefetchCount
	var gateLimit int64
	if prefetchCount == 0 {
		gateLimit = b.unlimitedPrefetchCap()
	} else {
		gateLimit = int64(prefetchCount)
	}

	state := &ConsumerState{
		consumer:  consumer,
		queueName: queueName,
		gate:      newPrefetchGate(gateLimit),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	state.bypassGate.Store(consumer.NoAck)

	b.activeConsumers.Store(consumerID, state)

	b.storage.RegisterConsumerCursor(queueName, consumerID)

	mu.Lock()
	defer mu.Unlock()
	val, _ := b.queueConsumers.LoadOrStore(queueName, []*ConsumerState{})
	consumers := val.([]*ConsumerState)
	consumers = append(consumers, state)
	b.queueConsumers.Store(queueName, consumers)

	go b.consumerPollLoop(state, queueState)

	return nil
}

// UnregisterConsumer removes a consumer and requeues all its in-flight
// messages (delivered but not yet ACKed) so they can be redelivered to other
// consumers. Without this cleanup, messages buffered in consumer.Messages or
// already pulled by the server's forwarder goroutine would be permanently
// orphaned: the deliveryIndex would still map their tags, the QueueState
// inflight counter would still count them, and pendingAck records would
// persist in storage — but no one would ever ACK or requeue them.
// The consumer is addressed by its broker-internal identity (Consumer.ID), not
// by its client-visible tag: the tag is channel-scoped, so cancelling by tag
// would tear down whichever same-tagged consumer happened to be registered last
// — one connection's basic.cancel killing another connection's consumer.
func (b *StorageBroker) UnregisterConsumer(consumerID string) error {
	// Load consumer state (lock-free)
	val, ok := b.activeConsumers.Load(consumerID)
	if !ok {
		return errors.New("consumer not found")
	}
	state := val.(*ConsumerState)

	state.gate.stop()

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
	b.activeConsumers.Delete(consumerID)

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

	// 7. Scan the deliveryIndex ledger for remaining in-flight messages owned by
	//    this consumer. These are deliveries that already LEFT consumer.Messages
	//    (pulled by the server's forwarder goroutine, sitting in the fanIn
	//    channel buffer, or already sent to the client over TCP) but were never
	//    ACKed. Without this scan, they would be permanently orphaned. The
	//    deliveryIndex maps msgID -> Consumer.ID and is the single ownership
	//    record (step 5 deleted the duplicate QueueState.inflightOwners map).
	//    Messages already claimed by deliverMessage's stopCh path, the drain
	//    above, or a concurrent ack are gone from the ledger (LoadAndDelete), so
	//    requeueInflightDelivery's own LoadAndDelete guard makes this a no-op for
	//    them — no double-requeue. basic.get deliveries carry an empty consumer
	//    identity and are correctly skipped by the identity filter. Tags are
	//    collected first so the requeue's LoadAndDelete does not mutate the map
	//    mid-Range.
	//
	//    The scan is O(total in-flight deliveries broker-wide) — acceptable
	//    because consumer cancellation is a rare event.
	var orphanedTags []uint64
	b.deliveryIndex.Range(func(key, value interface{}) bool {
		if value.(string) == consumerID {
			orphanedTags = append(orphanedTags, key.(uint64))
		}
		return true
	})
	for _, deliveryTag := range orphanedTags {
		b.requeueInflightDelivery(queueState, state.queueName, deliveryTag)
	}

	// 8. Unregister from AckCursor — safe now: all in-flight messages have
	//    been requeued, so minAckCursor recomputation won't skip them. If
	//    this were done before the requeue, minAckCursor would jump forward
	//    past the consumer's unacked tags, making the queue appear emptier
	//    than it actually is and potentially failing to apply backpressure.
	b.storage.UnregisterConsumerCursor(state.queueName, consumerID)

	mu := b.getQueueConsumersMutex(state.queueName)
	mu.Lock()
	lastConsumer := false
	if val, ok := b.queueConsumers.Load(state.queueName); ok {
		consumers := val.([]*ConsumerState)
		newConsumers := make([]*ConsumerState, 0, len(consumers)-1)
		for _, c := range consumers {
			if c.consumer.ID != consumerID {
				newConsumers = append(newConsumers, c)
			}
		}
		if len(newConsumers) > 0 {
			b.queueConsumers.Store(state.queueName, newConsumers)
		} else {
			b.queueConsumers.Delete(state.queueName)
			lastConsumer = true
		}
	}
	mu.Unlock()

	if lastConsumer {
		// Scoped to the incarnation whose AutoDelete flag was just read, for the
		// same reason as the reaper's idle delete (broker/queue_reaper.go): the
		// decision to delete is made from `q`, and a queue.declare landing
		// between that read and the delete would otherwise destroy a successor
		// that never lost a consumer, along with any durable publishes the
		// broker had already confirmed on it.
		//
		// GATED, both ways. Passing the WRONG ordinal reddens
		// TestAutoDeleteQueueDeletedWhenLastConsumerLeaves; reverting this line
		// to DeleteQueue(name) reddens
		// TestLastConsumerAutoDeleteCannotDeleteSuccessor
		// (broker/autodelete_incarnation_test.go), 5 of 5 under -race.
		//
		// That fixture exists because the window here cannot be reached by
		// racing: the capture and the act are two adjacent calls, so landing a
		// delete+redeclare between them means completing ~4 metadata fsyncs
		// inside a gap of microseconds — measured at 0 detections in 200
		// iterations under -race and 200 without. It is reached by VALUE
		// instead, poisoning the capture through a test-only storage
		// decorator. Unreachable by timing is not the same as ungatable, and
		// an earlier version of this comment conflated the two.
		if q, err := b.storage.GetQueue(state.queueName); err == nil && q.AutoDelete {
			b.deleteQueueIncarnation(state.queueName, q.Ordinal, false, false)
		}
	}

	err := b.storage.DeleteConsumer(state.queueName, consumerID)
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
	// SQ-11: consumer-cancel drain re-enters the message into the ready set —
	// restore its bytes when the queue enforces x-max-length-bytes.
	b.restoreReadyBytesOnRequeue(qs, queueName, deliveryTag)
	qs.Requeue(deliveryTag)
	b.storage.DeletePendingAck(queueName, deliveryTag)
}

// PublishMessage publishes a message to an exchange.
//
// Ownership: the broker takes ownership of message — it mutates
// message.DeliveryTag and stores the pointer directly in the first target
// queue's ring. Callers must pass a fresh Message per publish and must not
// reuse or mutate it afterwards (sharing the Body slice is fine; bodies are
// treated as immutable after publish). Reusing one Message object across
// publishes aliases the same pointer in multiple ring slots, which breaks the
// ring's tag-identity checks and permanently leaks ring count.
// ValidatePublishTarget reports the errors basic.publish must raise
// synchronously even in tx mode (R11, rabbit_channel.erl
// check_internal_exchange runs before tx buffering): errNoExchange (404) and
// ErrExchangeInternal (403).
func (b *StorageBroker) ValidatePublishTarget(exchangeName string) error {
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return errNoExchange(exchangeName)
		}
		return err
	}
	if exchange.Internal {
		return semErr("cannot publish to internal "+exchangeRS(exchangeName), ErrExchangeInternal)
	}
	return nil
}

func (b *StorageBroker) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	// Get exchange (lock-free)
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return errNoExchange(exchangeName)
		}
		return err
	}
	// §11 (R11): publishing DIRECTLY to an internal exchange is refused
	// (403); routing INTO one via e2e bindings remains allowed.
	if exchange.Internal {
		return semErr("cannot publish to internal "+exchangeRS(exchangeName), ErrExchangeInternal)
	}

	// W4 SQ-9: stamp the durable TTL anchor (enqueue instant) up front for a
	// VALID per-message expiration, shared by every fanout copy. Localized here
	// to keep the W5 (max-length) merge surface minimal; the queue-level
	// x-message-ttl anchor is stamped per copy in the enqueue loop below (it
	// needs the target queue's policy). A malformed/negative expiration is NOT
	// stamped — it has no usable deadline, so stamping an anchor would only make
	// the delivery head-check recompute a never-expiring deadline forever. No
	// clock read when no per-message TTL.
	if _, ok := parsePerMessageTTLMillis(message.Expiration); ok && message.EnqueueUnixMilli == 0 {
		message.EnqueueUnixMilli = b.ttlNowMillis()
	}

	// Find target queues based on exchange type and routing. The pooled
	// scratch keeps this allocation-free; it is owned by this goroutine for
	// the duration of the call and nothing below retains rs.queues.
	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)
	if err := b.routeMessage(exchange, routingKey, message, rs); err != nil {
		return err
	}
	targetQueues := rs.queues

	if len(targetQueues) == 0 {
		if message.Mandatory {
			return ErrNoRoute
		}
		return nil
	}

	// ITER5 shared-body gate (synchronous path). DeliveryMode==2 is checked FIRST
	// so the transient hot path evaluates one comparison and stops (no regression).
	// The server routes every durable publish through PublishMessageAsyncConfirm,
	// so this synchronous durable-fanout branch is reached only by non-async
	// callers/tests; it keeps the feature consistent regardless of entry point.
	if message.DeliveryMode == 2 && len(targetQueues) >= 2 && len(message.Body) >= b.sharedBodyThreshold() {
		if sdw, ok := b.storage.(sharedDurableWriter); ok {
			return b.fanoutSharedSync(sdw, message, targetQueues)
		}
	}

	// Enqueue to all target queues (lock-free, no consumer iteration)
	rejected := false
	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		if !queueState.WaitForCapacity(queueState.StopCh()) {
			return fmt.Errorf("queue '%s' closed during backpressure wait: %w", queueName, ErrQueueClosed)
		}

		// SQ-11 max-length: one policy load per target queue (nil => no policy
		// => every branch below is not taken). For x-overflow=reject-publish,
		// refuse the incoming message BEFORE any store/enqueue when the queue is
		// AT OR OVER its limit; the message is not enqueued to this target and
		// not dead-lettered. drop-head is applied AFTER enqueue (below).
		p := queueState.Policy()
		// For reject-publish, the AT-OR-OVER check and the matching enqueue must
		// be atomic w.r.t. other publishers to this queue: two publishers both
		// observing count == limit-1 would otherwise both accept and push the
		// ready set permanently past the cap. maxLenMu serializes only this
		// admission path; drop-head and no-policy queues stay lock-free.
		rejectPublish := p != nil && p.Overflow == OverflowRejectPublish &&
			(p.HasMaxLength || p.HasMaxLengthBytes)
		if rejectPublish {
			queueState.maxLenMu.Lock()
			if maxLenRejectsPublish(queueState, p) {
				queueState.maxLenMu.Unlock()
				rejected = true
				continue
			}
		}

		// Assign the delivery tag. Every publish — transient or durable —
		// reserves the tag as pending on the contiguous frontier UNDER the
		// frontier lock AT MINT TIME. This closes the transient-vs-transient
		// visibility race: a neighbor's FrontierComplete can never advance head
		// past this still-pending tag, so the consumer can never claim (and
		// gap-skip) a tag whose message is not yet ring-resident. The frontier
		// is activated on the first publish to the queue (monotonically, never
		// reset), so pure-transient queues also go through the frontier after
		// their first publish. The lock is held for O(1) work (append + atomic
		// store), never across I/O.
		msgID := queueState.FrontierReserve()

		var storeMsg *protocol.Message
		if i == 0 {
			message.DeliveryTag = msgID
			storeMsg = message
		} else {
			// FANOUT INVARIANT: For fanout (and any multi-queue routing), tail
			// deliveries (i > 0) MUST deep-copy the Headers map. All queue
			// copies would otherwise share the same map pointer; a consumer
			// or internal mutation of one copy's Headers would silently
			// corrupt every other queue's delivery. The Body slice is not
			// deep-copied because the AMQP protocol treats message bodies as
			// immutable after publish — consumers read but never mutate Body.
			msgCopy := *message
			msgCopy.DeliveryTag = msgID
			if message.Headers != nil {
				h := make(map[string]interface{}, len(message.Headers))
				for k, v := range message.Headers {
					h[k] = v
				}
				msgCopy.Headers = h
			}
			storeMsg = &msgCopy
		}

		// W4 SQ-9: stamp the TTL anchor for a queue-level x-message-ttl target
		// that carried no per-message expiration (that case was stamped up
		// front). Reuses the single shared Policy() load `p` from the SQ-11
		// admission check above, so a queue without TTL pays no extra work; no
		// clock read when no TTL applies.
		if storeMsg.EnqueueUnixMilli == 0 && p != nil && p.HasMessageTTL {
			storeMsg.EnqueueUnixMilli = b.ttlNowMillis()
		}

		// Store to persistent storage
		err := b.storage.StoreMessage(queueName, storeMsg)
		if err != nil {
			queueState.FrontierComplete(msgID, false)
			if rejectPublish {
				queueState.maxLenMu.Unlock()
			}
			return fmt.Errorf("failed to store message to queue '%s': %w", queueName, err)
		}

		queueState.FrontierComplete(msgID, true)

		// SQ-11 max-length: byte accounting + drop-head eviction AFTER the
		// enqueue, so the just-published newest message is retained and the
		// globally-oldest ready message is evicted first.
		if p != nil {
			addReadyBytesOnEnqueue(queueState, p, len(storeMsg.Body))
			if p.Overflow == OverflowDropHead && (p.HasMaxLength || p.HasMaxLengthBytes) {
				b.dropHeadTrim(queueState, queueName, p)
			}
		}
		if rejectPublish {
			// Admission (check + Publish + AddReadyBytes) is complete and now
			// visible under the cap; release so the next publisher re-checks.
			queueState.maxLenMu.Unlock()
		}

		// Done - polling loops will discover it (NO consumer iteration needed)
	}

	if rejected {
		// At least one reject-publish target refused the message; the server
		// layer nacks (confirm) / returns (mandatory) / drops the publisher.
		return ErrMaxLengthExceeded
	}
	return nil
}

// publishConfirmAgg is the per-publish fan-in that fires onDurable EXACTLY ONCE,
// after every durable copy of a message has crossed the WAL group-commit fsync
// barrier. remaining starts at 1 (a setup guard dropped after the routing loop)
// plus 1 per enqueued durable copy; the transition-to-zero fires onDurable with
// the first error seen (nil on full success). The guard prevents a premature
// fire if an early copy's fsync completes before later copies are enqueued.
type publishConfirmAgg struct {
	remaining atomic.Int32
	onDurable func(error)
	errOnce   sync.Once
	err       error
}

func newPublishConfirmAgg(onDurable func(error)) *publishConfirmAgg {
	a := &publishConfirmAgg{onDurable: onDurable}
	a.remaining.Store(1) // setup guard
	return a
}

// addCopy registers one durable copy. MUST be called BEFORE that copy's WAL
// write is enqueued, so a completion that fires immediately can never drive
// remaining to zero before every copy has been counted.
func (a *publishConfirmAgg) addCopy() { a.remaining.Add(1) }

// report records one settled copy (or the setup-guard drop). The first non-nil
// error wins. When the last outstanding count clears, onDurable fires once (if
// non-nil). errOnce provides the happens-before for reading a.err on the
// transition-to-zero decrement (the atomic total order links each report's
// error write to the final decrement).
func (a *publishConfirmAgg) report(err error) {
	if err != nil {
		a.errOnce.Do(func() { a.err = err })
	}
	if a.remaining.Add(-1) == 0 && a.onDurable != nil {
		a.onDurable(a.err)
	}
}

// PublishMessageAsyncConfirm is the async-completion publish path for durable
// and/or publisher-confirm messages. It mirrors PublishMessage's routing,
// delivery-tag assignment, no-route, and SQ-11 reject-publish admission — all
// SYNCHRONOUS, so ErrNoRoute / ErrMaxLengthExceeded are returned inline — but
// for a durable (DeliveryMode==2) message it enqueues each copy's WAL write via
// StoreMessageAsync and DEFERS that copy's consumer-visibility (queueState.
// Publish + max-length accounting) into the copy's fsync completion, exactly as
// PublishMessageTx defers visibility until after the atomic commit. A per-publish
// fan-in aggregator fires onDurable once every durable copy is fsynced.
//
// It returns durableInflight=true iff at least one durable copy was enqueued and
// onDurable will fire from the async completion (the caller must NOT settle the
// confirm itself — the completion drives it). It returns durableInflight=false
// for: a transient publish (identical to PublishMessage, visible synchronously),
// and a no-route non-mandatory success — in both cases onDurable is NOT invoked
// here and the caller settles the confirm synchronously. On ErrNoRoute /
// ErrMaxLengthExceeded, onDurable is never invoked (the caller settles via its
// existing error handling); durable copies already enqueued to accepted targets
// still become visible on their fsync, but the confirm is nacked.
func (b *StorageBroker) PublishMessageAsyncConfirm(exchangeName, routingKey string, message *protocol.Message, onDurable func(error)) (durableInflight bool, backpressure protocol.DepthGate, err error) {
	// Transient publishes take the byte-for-byte-unchanged synchronous path.
	// There is no durability barrier, so the confirm (if any) is settled
	// synchronously by the caller (durableInflight=false).
	if message.DeliveryMode != 2 {
		return false, nil, b.PublishMessage(exchangeName, routingKey, message)
	}

	// A backend without async durable support falls back to the synchronous
	// publish (blocking fsync + immediate visibility); the caller settles the
	// confirm synchronously. Only the storage backend that decouples the fsync
	// gets the pipelined async path below.
	adw, ok := b.storage.(asyncDurableWriter)
	if !ok {
		return false, nil, b.PublishMessage(exchangeName, routingKey, message)
	}

	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return false, nil, errNoExchange(exchangeName)
		}
		return false, nil, err
	}
	// §11 (R11): publishing DIRECTLY to an internal exchange is refused (403).
	if exchange.Internal {
		return false, nil, semErr("cannot publish to internal "+exchangeRS(exchangeName), ErrExchangeInternal)
	}

	// W4 SQ-9: stamp the durable TTL anchor for a valid per-message expiration
	// up front (shared by every fanout copy), exactly as PublishMessage.
	if _, ok := parsePerMessageTTLMillis(message.Expiration); ok && message.EnqueueUnixMilli == 0 {
		message.EnqueueUnixMilli = b.ttlNowMillis()
	}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)
	if err := b.routeMessage(exchange, routingKey, message, rs); err != nil {
		return false, nil, err
	}
	targetQueues := rs.queues

	if len(targetQueues) == 0 {
		if message.Mandatory {
			return false, nil, ErrNoRoute
		}
		// No route, non-mandatory: the caller confirms synchronously.
		return false, nil, nil
	}

	agg := newPublishConfirmAgg(onDurable)
	rejected := false

	// ITER5 shared-body gate: a durable fan-out to >=2 queues with a large body
	// routes its N durable writes through ONE fused BodyBlock+refs unit (body on
	// disk once) when the backend supports it. Evaluated with values in hand — no
	// hash, no lock, no alloc. Every winning single-queue durable cell has
	// len(targetQueues)==1, so this is false there and the loop below runs
	// byte-for-byte as today.
	if len(targetQueues) >= 2 && len(message.Body) >= b.sharedBodyThreshold() {
		if sdw, ok := b.storage.(sharedDurableWriter); ok {
			return b.fanoutSharedAsyncConfirm(sdw, agg, message, targetQueues)
		}
	}

	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		// Iteration 2 (option A): the durable+confirm admission path does NOT park
		// the frame PROCESSOR on queue depth. The confirm tag was already minted
		// (server side) before this call, so parking here until a consumer drains —
		// as WaitForCapacity did — would STRAND that tag whenever the consumer stops
		// (benchmark-teardown deadlock). Instead the publish is admitted →
		// persisted(WAL) → confirmed, and depth backpressure is applied UPSTREAM at
		// the READER: if a routed target is at/over its high-water mark we report it
		// as `backpressure` so the caller pauses this producer-only connection's
		// reader (TCP backpressure), pacing the producer to the consumer's drain rate
		// and keeping the queue small + ring-resident (high consume throughput)
		// WITHOUT ever stranding a tag on the processor side. Still bail if the queue
		// was deleted out from under us.
		select {
		case <-queueState.StopCh():
			return false, nil, fmt.Errorf("queue '%s': %w", queueName, ErrQueueClosed)
		default:
		}
		if queueState.AtHighWaterMark() {
			backpressure = queueState
		}

		// SQ-11 reject-publish admission (synchronous), identical to PublishMessage.
		p := queueState.Policy()
		rejectPublish := p != nil && p.Overflow == OverflowRejectPublish &&
			(p.HasMaxLength || p.HasMaxLengthBytes)
		if rejectPublish {
			queueState.maxLenMu.Lock()
			if maxLenRejectsPublish(queueState, p) {
				queueState.maxLenMu.Unlock()
				rejected = true
				continue
			}
		}

		// Assign the delivery tag AND register it pending on this queue's
		// contiguous durable frontier atomically, holding the queue's frontier
		// lock across the global-counter Add so the queue's durable tags register
		// in tag order and any concurrent transient publisher that mints a higher
		// tag observes the queue frontier-active and routes through it (A3
		// race-safety — see QueueState.FrontierReserve).
		msgID := queueState.FrontierReserve()

		var storeMsg *protocol.Message
		if i == 0 {
			message.DeliveryTag = msgID
			storeMsg = message
		} else {
			// FANOUT INVARIANT (see PublishMessage): deep-copy the Headers map for
			// tail deliveries so queue copies never share a map pointer.
			msgCopy := *message
			msgCopy.DeliveryTag = msgID
			if message.Headers != nil {
				h := make(map[string]interface{}, len(message.Headers))
				for k, v := range message.Headers {
					h[k] = v
				}
				msgCopy.Headers = h
			}
			storeMsg = &msgCopy
		}

		if storeMsg.EnqueueUnixMilli == 0 && p != nil && p.HasMessageTTL {
			storeMsg.EnqueueUnixMilli = b.ttlNowMillis()
		}

		// Deferred per-copy completion: consumer-visibility (the frontier head
		// advance) and max-length accounting run ONLY after THIS copy's WAL record
		// is fsynced, so no consumer can claim the message before it is durable
		// (A3). The frontier advances head to the lowest still-pending routed tag,
		// never past this tag before it is done, even under out-of-order
		// cross-connection completion. On fsync error the copy is advanced past
		// (real=false) but never made ring-resident, so it is gap-skipped, never
		// delivered. Runs on the shared WAL batch-writer goroutine — non-blocking
		// (frontier ops are O(1) under a per-queue lock, no I/O; A4).
		qs := queueState
		id := msgID
		qn := queueName
		pol := p
		bodyLen := len(storeMsg.Body)
		completion := func(cerr error) {
			qs.FrontierComplete(id, cerr == nil)
			if cerr == nil && pol != nil {
				addReadyBytesOnEnqueue(qs, pol, bodyLen)
				if pol.Overflow == OverflowDropHead && (pol.HasMaxLength || pol.HasMaxLengthBytes) {
					// Runs on the WAL batch-writer goroutine — eviction can block on
					// a synchronous dead-letter/spill StoreMessage, so it MUST be
					// handed to the trim worker, never run inline (C1/A4 deadlock).
					b.enqueueMaxLenTrim(qs, qn, pol)
				}
			}
			agg.report(cerr)
		}

		agg.addCopy() // count BEFORE enqueue so an immediate completion can't underflow
		if serr := adw.StoreMessageAsync(queueName, storeMsg, completion); serr != nil {
			// Enqueue failed (e.g. WAL unavailable): the completion will NOT fire,
			// so RELEASE the frontier slot we reserved for this tag — otherwise it
			// stays pending forever and wedges the per-queue frontier, blocking all
			// later publishes to this queue (M1). real=false advances head past it
			// without counting it ready. The aggregator is abandoned (guard never
			// dropped => onDurable never fires). Propagate the failure synchronously.
			queueState.FrontierComplete(msgID, false)
			if rejectPublish {
				queueState.maxLenMu.Unlock()
			}
			return false, nil, fmt.Errorf("failed to store durable message to queue '%s': %w", queueName, serr)
		}

		if rejectPublish {
			queueState.maxLenMu.Unlock()
		}
	}

	if rejected {
		// A reject-publish target refused the message. Any durable copies to
		// accepted targets are enqueued and will become visible on their fsync,
		// but the publisher confirm is nacked by the caller. The setup guard is
		// deliberately NOT dropped, so onDurable never fires for a nacked publish.
		return false, nil, ErrMaxLengthExceeded
	}

	// Drop the setup guard. remaining is now (#durable copies) >= 1, so onDurable
	// fires only once the last copy's fsync completes (asynchronously). Never
	// fires synchronously here.
	agg.report(nil)
	return true, backpressure, nil
}

// fanoutSharedAsyncConfirm is the ITER5 fused fan-out for PublishMessageAsyncConfirm:
// it performs the SAME per-queue in-memory bookkeeping as the per-queue loop (tag
// mint via FrontierReserve, Headers deep-copy for tail copies, TTL stamp,
// reject-publish admission, HWM backpressure) but collects the N copies and routes
// their durable writes through ONE StoreSharedAsync (one shared BodyBlock + N
// reference records, one fsync). Each copy's completion advances THAT queue's
// frontier + max-length accounting exactly like the per-queue completion, so
// consumer-visibility/confirm semantics are identical; only the durable byte
// layout differs. The publishConfirmAgg fans the N completions into one confirm.
func (b *StorageBroker) fanoutSharedAsyncConfirm(sdw sharedDurableWriter, agg *publishConfirmAgg, message *protocol.Message, targetQueues []string) (bool, protocol.DepthGate, error) {
	var backpressure protocol.DepthGate
	rejected := false

	// reserved tracks (queueState, tag) for every copy whose frontier slot we
	// reserved, so a StoreSharedAsync enqueue failure can release them all (the
	// completions will not fire) — mirroring the per-queue enqueue-error path.
	type reservedSlot struct {
		qs  *QueueState
		tag uint64
	}
	reserved := make([]reservedSlot, 0, len(targetQueues))
	subs := make([]interfaces.SharedDurableSub, 0, len(targetQueues))

	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		select {
		case <-queueState.StopCh():
			// Release any slots reserved so far before bailing.
			for _, r := range reserved {
				r.qs.FrontierComplete(r.tag, false)
			}
			return false, nil, fmt.Errorf("queue '%s': %w", queueName, ErrQueueClosed)
		default:
		}
		if queueState.AtHighWaterMark() {
			backpressure = queueState
		}

		p := queueState.Policy()
		rejectPublish := p != nil && p.Overflow == OverflowRejectPublish &&
			(p.HasMaxLength || p.HasMaxLengthBytes)
		if rejectPublish {
			queueState.maxLenMu.Lock()
			if maxLenRejectsPublish(queueState, p) {
				queueState.maxLenMu.Unlock()
				rejected = true
				continue
			}
		}

		msgID := queueState.FrontierReserve()

		var storeMsg *protocol.Message
		if i == 0 {
			message.DeliveryTag = msgID
			storeMsg = message
		} else {
			// FANOUT INVARIANT (see PublishMessage): deep-copy the Headers map for
			// tail copies so queue copies never share a map pointer. Body IS shared
			// (immutable after publish) — it is the single body the BodyBlock holds.
			msgCopy := *message
			msgCopy.DeliveryTag = msgID
			if message.Headers != nil {
				h := make(map[string]interface{}, len(message.Headers))
				for k, v := range message.Headers {
					h[k] = v
				}
				msgCopy.Headers = h
			}
			storeMsg = &msgCopy
		}

		if storeMsg.EnqueueUnixMilli == 0 && p != nil && p.HasMessageTTL {
			storeMsg.EnqueueUnixMilli = b.ttlNowMillis()
		}

		// Per-copy deferred completion, identical to the per-queue path (§3.5):
		// visibility + max-length accounting run ONLY after THIS copy is durable.
		qs := queueState
		id := msgID
		qn := queueName
		pol := p
		bodyLen := len(storeMsg.Body)
		completion := func(cerr error) {
			qs.FrontierComplete(id, cerr == nil)
			if cerr == nil && pol != nil {
				addReadyBytesOnEnqueue(qs, pol, bodyLen)
				if pol.Overflow == OverflowDropHead && (pol.HasMaxLength || pol.HasMaxLengthBytes) {
					// Runs on the WAL batch-writer goroutine — hand drop-head trim to
					// the worker (never inline; C1/A4 deadlock), as the per-queue path.
					b.enqueueMaxLenTrim(qs, qn, pol)
				}
			}
			agg.report(cerr)
		}

		agg.addCopy() // count BEFORE enqueue so an immediate completion can't underflow
		reserved = append(reserved, reservedSlot{qs: queueState, tag: msgID})
		subs = append(subs, interfaces.SharedDurableSub{
			QueueName: queueName,
			Message:   storeMsg,
			OnDurable: completion,
		})

		if rejectPublish {
			queueState.maxLenMu.Unlock()
		}
	}

	if len(subs) > 0 {
		if serr := sdw.StoreSharedAsync(subs); serr != nil {
			// Enqueue failed: no completion will fire, so release every reserved
			// frontier slot (else the per-queue frontiers wedge, M1). The aggregator
			// is abandoned (guard never dropped => onDurable never fires).
			for _, r := range reserved {
				r.qs.FrontierComplete(r.tag, false)
			}
			return false, nil, fmt.Errorf("failed to store shared durable message: %w", serr)
		}
	}

	if rejected {
		// A reject-publish target refused the message. Accepted copies are enqueued
		// and will become visible on their fsync, but the confirm is nacked by the
		// caller. The setup guard is deliberately NOT dropped (onDurable never fires).
		return false, nil, ErrMaxLengthExceeded
	}

	// Drop the setup guard: onDurable fires once the last copy's fsync completes.
	agg.report(nil)
	return true, backpressure, nil
}

// fanoutSharedSync is the ITER5 fused fan-out for the synchronous PublishMessage:
// it mirrors PublishMessage's per-queue bookkeeping (WaitForCapacity, tag mint,
// Headers deep-copy, TTL stamp, reject-publish admission) but routes the N durable
// writes through ONE StoreSharedAsync and BLOCKS until every copy is durable
// (preserving PublishMessage's synchronous "durable + visible on return"
// contract). Each copy's visibility (frontier head advance + max-length
// accounting) runs in its own durable completion, exactly after that copy is
// durable, so a consumer can never observe a message before it is durable.
//
// I1-safety: the tag is minted with FrontierReserve (reserve-at-mint) on EVERY
// target — identical to the async path fanoutSharedAsyncConfirm — rather than
// raw-minting a tag on a not-yet-frontier-active queue and deferring a CAS-max
// Publish. Reserving pending on the contiguous frontier UNDER the frontier lock at
// mint time closes the I1 window: a concurrent durable-confirm publish on the same
// queue that FrontierReserves a higher tag can never advance head PAST our
// lower, still-pending tag before it is stored (publish-order FIFO, A3). Keeping
// the sync and async fan-out paths symmetric in their frontier/tag handling.
func (b *StorageBroker) fanoutSharedSync(sdw sharedDurableWriter, message *protocol.Message, targetQueues []string) error {
	type reservedSlot struct {
		qs  *QueueState
		tag uint64
	}
	reserved := make([]reservedSlot, 0, len(targetQueues))
	subs := make([]interfaces.SharedDurableSub, 0, len(targetQueues))

	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	rejected := false
	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		if !queueState.WaitForCapacity(queueState.StopCh()) {
			for _, r := range reserved {
				r.qs.FrontierComplete(r.tag, false)
			}
			return fmt.Errorf("queue '%s' closed during backpressure wait: %w", queueName, ErrQueueClosed)
		}

		p := queueState.Policy()
		rejectPublish := p != nil && p.Overflow == OverflowRejectPublish &&
			(p.HasMaxLength || p.HasMaxLengthBytes)
		if rejectPublish {
			queueState.maxLenMu.Lock()
			if maxLenRejectsPublish(queueState, p) {
				queueState.maxLenMu.Unlock()
				rejected = true
				continue
			}
		}

		// Reserve-at-mint on every target (I1-safe, symmetric with the async path):
		// the tag registers pending on the contiguous durable frontier under the
		// frontier lock, so a concurrent durable publisher minting a higher tag
		// observes the queue frontier-active and can never advance head past this
		// still-pending lower tag before its completion below marks it done.
		msgID := queueState.FrontierReserve()

		var storeMsg *protocol.Message
		if i == 0 {
			message.DeliveryTag = msgID
			storeMsg = message
		} else {
			msgCopy := *message
			msgCopy.DeliveryTag = msgID
			if message.Headers != nil {
				h := make(map[string]interface{}, len(message.Headers))
				for k, v := range message.Headers {
					h[k] = v
				}
				msgCopy.Headers = h
			}
			storeMsg = &msgCopy
		}

		if storeMsg.EnqueueUnixMilli == 0 && p != nil && p.HasMessageTTL {
			storeMsg.EnqueueUnixMilli = b.ttlNowMillis()
		}

		qs := queueState
		id := msgID
		qn := queueName
		pol := p
		bodyLen := len(storeMsg.Body)
		wg.Add(1)
		completion := func(cerr error) {
			defer wg.Done()
			// Advance the reserved frontier slot: on success head advances across the
			// contiguous done-prefix (making the tag visible only now that it is
			// durable); on error real=false advances head past it without counting it
			// ready. Identical to the async path's per-copy completion.
			qs.FrontierComplete(id, cerr == nil)
			if cerr != nil {
				errOnce.Do(func() { firstErr = cerr })
				return
			}
			if pol != nil {
				addReadyBytesOnEnqueue(qs, pol, bodyLen)
				if pol.Overflow == OverflowDropHead && (pol.HasMaxLength || pol.HasMaxLengthBytes) {
					// Completion runs on the WAL batch-writer goroutine — hand drop-head
					// trim to the worker (never inline; C1/A4 deadlock).
					b.enqueueMaxLenTrim(qs, qn, pol)
				}
			}
		}

		reserved = append(reserved, reservedSlot{qs: queueState, tag: msgID})
		subs = append(subs, interfaces.SharedDurableSub{
			QueueName: queueName,
			Message:   storeMsg,
			OnDurable: completion,
		})

		if rejectPublish {
			queueState.maxLenMu.Unlock()
		}
	}

	if len(subs) > 0 {
		if serr := sdw.StoreSharedAsync(subs); serr != nil {
			// No completion fires: undo the WaitGroup adds and release the reserved
			// frontier slots (else the per-queue frontiers wedge, M1).
			for range subs {
				wg.Done()
			}
			for _, r := range reserved {
				r.qs.FrontierComplete(r.tag, false)
			}
			return fmt.Errorf("failed to store shared durable message: %w", serr)
		}
		wg.Wait()
	}

	if firstErr != nil {
		return firstErr
	}
	if rejected {
		return ErrMaxLengthExceeded
	}
	return nil
}

// PublishMessageTx is the transactional (SQ-8) sibling of PublishMessage. It
// performs identical routing and delivery-tag assignment, but writes the message
// to the supplied transactional storage view (txnStore) instead of the live
// broker storage, and DEFERS making the message visible to consumers.
//
// The tag is reserved on the per-queue contiguous frontier at STAGE TIME via
// FrontierReserve (minting inside the callback, same as PublishMessage). This
// closes the transient-vs-tx visibility race: a concurrent non-tx publish's
// FrontierComplete can never advance head past this still-pending tag, so the
// consumer can never claim (and gap-skip) a tx tag before its message is
// committed to real storage. The deferred closure calls FrontierComplete(tag,
// true) after the atomic commit (the message is durable/stored), or
// FrontierComplete(tag, false) on abort (releasing the frontier slot so the tag
// is gap-skipped, never stranded).
//
// The returned closures are run by the transaction manager: apply(true) after a
// successful commit, apply(false) after a rollback. On a mid-fanout failure
// inside this function, all already-reserved frontier slots are released before
// returning the error, so no slot is stranded.
//
// This is the slow path; it deliberately duplicates PublishMessage rather than
// refactoring it, to leave the non-transactional publish hot path untouched.
func (b *StorageBroker) PublishMessageTx(txnStore interfaces.Storage, exchangeName, routingKey string, message *protocol.Message) ([]func(bool), error) {
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return nil, errNoExchange(exchangeName)
		}
		return nil, err
	}
	// §11 (R11): check_internal_exchange runs at publish time in tx and
	// non-tx mode alike; the server also pre-checks via ValidatePublishTarget
	// before buffering (spec §11.2).
	if exchange.Internal {
		return nil, semErr("cannot publish to internal "+exchangeRS(exchangeName), ErrExchangeInternal)
	}

	// Same pooled-scratch routing as PublishMessage. The deferred visibility
	// closures below capture only the QueueState and message ID — never
	// rs.queues — so releasing the scratch on return is safe.
	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)
	if err := b.routeMessage(exchange, routingKey, message, rs); err != nil {
		return nil, err
	}
	targetQueues := rs.queues

	if len(targetQueues) == 0 {
		if message.Mandatory {
			return nil, ErrNoRoute
		}
		return nil, nil
	}

	// W4 SQ-9: stamp the durable TTL anchor for a VALID per-message expiration
	// HERE, in PublishMessageTx, which the tx manager runs at COMMIT time (it
	// buffers publish operations and executes them inside ExecuteAtomic). Stamping
	// before txnStore.StoreMessage — NOT in the post-commit visibility closure —
	// means the anchor is part of the durable WAL write, so a durable tx+TTL
	// message recovers with its deadline intact and still expires. The stamp
	// instant is the commit instant (PublishMessageTx runs at commit), so a
	// tx-published TTL counts from commit. Malformed/negative expiration is not
	// stamped (see PublishMessage).
	if _, ok := parsePerMessageTTLMillis(message.Expiration); ok && message.EnqueueUnixMilli == 0 {
		message.EnqueueUnixMilli = b.ttlNowMillis()
	}

	deferred := make([]func(bool), 0, len(targetQueues))

	type reservedSlot struct {
		qs  *QueueState
		tag uint64
	}
	reserved := make([]reservedSlot, 0, len(targetQueues))
	releaseReserved := func() {
		for _, r := range reserved {
			r.qs.FrontierComplete(r.tag, false)
		}
	}

	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		if !queueState.WaitForCapacity(queueState.StopCh()) {
			releaseReserved()
			return nil, fmt.Errorf("queue '%s' closed during backpressure wait: %w", queueName, ErrQueueClosed)
		}

		// Reserve the tag on the frontier at stage time, minting inside the
		// callback so this queue's frontier tags register in tag order (same
		// pattern as PublishMessage). The tag stays pending on the frontier
		// until the deferred closure calls FrontierComplete after commit/abort.
		msgID := queueState.FrontierReserve()
		reserved = append(reserved, reservedSlot{queueState, msgID})

		var storeMsg *protocol.Message
		if i == 0 {
			message.DeliveryTag = msgID
			storeMsg = message
		} else {
			// Same FANOUT INVARIANT as PublishMessage: deep-copy the Headers map
			// for tail deliveries so queue copies never share a map pointer.
			msgCopy := *message
			msgCopy.DeliveryTag = msgID
			if message.Headers != nil {
				h := make(map[string]interface{}, len(message.Headers))
				for k, v := range message.Headers {
					h[k] = v
				}
				msgCopy.Headers = h
			}
			storeMsg = &msgCopy
		}

		// W4 SQ-9 + SQ-11: one shared Policy() load for this target — used by the
		// TTL anchor stamp (below) and the deferred max-length accounting closure.
		p := queueState.Policy()

		// W4 SQ-9: stamp the TTL anchor for a queue-level x-message-ttl target that
		// carried no per-message expiration, per copy. Like the top-of-function
		// stamp this runs at commit time and BEFORE the durable stage below, so the
		// anchor is persisted.
		if storeMsg.EnqueueUnixMilli == 0 && p != nil && p.HasMessageTTL {
			storeMsg.EnqueueUnixMilli = b.ttlNowMillis()
		}

		// Stage the store into the transactional view (buffered until commit).
		if err := txnStore.StoreMessage(queueName, storeMsg); err != nil {
			releaseReserved()
			return nil, fmt.Errorf("failed to stage message to queue '%s': %w", queueName, err)
		}

		// Defer consumer visibility until after the atomic commit succeeds. On
		// commit, FrontierComplete(tag, true) makes the message visible (head
		// advances, waiting incremented). On abort, FrontierComplete(tag, false)
		// releases the frontier slot (head advances past the tag, never counted
		// ready) so the consumer gap-skips it — no strand, no leak.
		qs := queueState
		id := msgID
		qn := queueName
		pol := p
		bodyLen := len(storeMsg.Body)
		deferred = append(deferred, func(commit bool) {
			qs.FrontierComplete(id, commit)
			if commit && pol != nil {
				addReadyBytesOnEnqueue(qs, pol, bodyLen)
				if pol.Overflow == OverflowDropHead && (pol.HasMaxLength || pol.HasMaxLengthBytes) {
					b.dropHeadTrim(qs, qn, pol)
				}
			}
		})
	}

	return deferred, nil
}

// routeScratch holds per-publish routing state. Instances are pooled so the
// publish hot path performs zero steady-state heap allocations: the queues
// slice, seen set, and visited set are all reused across publishes.
//
// Ownership contract (race safety without locks): a scratch obtained from
// acquireRouteScratch is owned exclusively by the calling goroutine until
// releaseRouteScratch returns it to the pool. Neither the scratch nor its
// queues slice may be retained, returned, or shared past the release — copy
// anything that must outlive the publish (see findTargetQueues).
type routeScratch struct {
	// queues accumulates target queue names in routing order.
	queues []string
	// seen deduplicates queues across multiple matching bindings; visited
	// provides cycle protection for exchange-to-exchange bindings. Both are
	// lazily allocated: the default-exchange and direct-no-e2e fast paths in
	// routeMessage never touch them.
	seen    map[string]struct{}
	visited map[string]struct{}
}

var routeScratchPool = sync.Pool{
	New: func() interface{} { return &routeScratch{} },
}

func acquireRouteScratch() *routeScratch {
	return routeScratchPool.Get().(*routeScratch)
}

func releaseRouteScratch(rs *routeScratch) {
	routeScratchPool.Put(rs)
}

// resetMaps prepares the dedup/cycle sets for the recursive routing path,
// allocating them on first use of a pooled scratch and clearing them after.
func (rs *routeScratch) resetMaps() {
	if rs.seen == nil {
		rs.seen = make(map[string]struct{}, 8)
		rs.visited = make(map[string]struct{}, 4)
		return
	}
	clear(rs.seen)
	clear(rs.visited)
}

// routeMessage is the single routing entry point: it resolves the set of
// queues that should receive a message published to exchange with routingKey
// and stores them in rs.queues (resetting any previous contents). It is
// shared by PublishMessage and PublishMessageTx, and is the re-entry point
// for future dead-letter-exchange routing (SQ-10).
//
// Routing is read-only and lock-free. Two allocation fast paths cover the
// overwhelmingly common cases without touching the dedup/visited maps:
//
//  1. Default exchange: routes to at most one queue named by the routing key.
//  2. Direct exchange with no exchange-to-exchange bindings: a linear scan of
//     the exchange's bindings, deduplicated by scanning the (small) result
//     slice instead of a map.
//
// Fanout, topic, and headers exchanges — and any exchange with
// exchange-to-exchange bindings — use the recursive map-based path
// (collectTargetQueues), which preserves cycle protection and dedup semantics.
func (b *StorageBroker) routeMessage(exchange *protocol.Exchange, routingKey string, message *protocol.Message, rs *routeScratch) error {
	rs.queues = rs.queues[:0]

	// Fast path 1: the default exchange routes directly to the queue named by
	// the routing key. No bindings, no recursion, no maps.
	if exchange.Name == "" {
		if _, ok := b.activeQueues.Load(routingKey); ok {
			rs.queues = append(rs.queues, routingKey)
			return nil
		}
		if _, err := b.storage.GetQueue(routingKey); err == nil {
			rs.queues = append(rs.queues, routingKey)
		}
		return nil
	}

	// Fast path 2: direct exchange with no exchange-to-exchange bindings.
	if exchange.Kind == "direct" {
		exchBindings, err := b.storage.GetExchangeBindingsFrom(exchange.Name)
		if err != nil {
			return fmt.Errorf("failed to get exchange-to-exchange bindings: %w", err)
		}
		if len(exchBindings) == 0 {
			bindings, err := b.storage.GetExchangeBindings(exchange.Name)
			if err != nil {
				return fmt.Errorf("failed to get exchange bindings: %w", err)
			}
			for _, binding := range bindings {
				if binding.RoutingKey == routingKey && !slices.Contains(rs.queues, binding.QueueName) {
					rs.queues = append(rs.queues, binding.QueueName)
				}
			}
			return nil
		}
	}

	// General path: recursive traversal with map-based dedup and cycle
	// protection. The maps come from the pooled scratch, so this path is
	// also allocation-free in steady state.
	rs.resetMaps()
	return b.collectTargetQueues(exchange, routingKey, message, rs)
}

// findTargetQueues resolves target queues into a freshly allocated slice.
// It is a convenience wrapper over routeMessage for callers (tests, future
// dead-letter handling) that need a result outliving the routing call; the
// hot publish paths use routeMessage with a pooled scratch directly.
func (b *StorageBroker) findTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message) ([]string, error) {
	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)
	if err := b.routeMessage(exchange, routingKey, message, rs); err != nil {
		return nil, err
	}
	if len(rs.queues) == 0 {
		return nil, nil
	}
	// Copy out: rs.queues is pooled and must not escape this call.
	return slices.Clone(rs.queues), nil
}

func (b *StorageBroker) collectTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message, rs *routeScratch) error {
	if _, ok := rs.visited[exchange.Name]; ok {
		return nil
	}
	rs.visited[exchange.Name] = struct{}{}

	if exchange.Name == "" {
		if _, ok := rs.seen[routingKey]; ok {
			return nil
		}
		if _, ok := b.activeQueues.Load(routingKey); ok {
			rs.queues = append(rs.queues, routingKey)
			rs.seen[routingKey] = struct{}{}
			return nil
		}
		_, err := b.storage.GetQueue(routingKey)
		if err == nil {
			rs.queues = append(rs.queues, routingKey)
			rs.seen[routingKey] = struct{}{}
		}
		return nil
	}

	bindings, err := b.storage.GetExchangeBindings(exchange.Name)
	if err != nil {
		return fmt.Errorf("failed to get exchange bindings: %w", err)
	}

	switch exchange.Kind {
	case "direct":
		for _, binding := range bindings {
			if binding.RoutingKey == routingKey {
				if _, ok := rs.seen[binding.QueueName]; !ok {
					rs.queues = append(rs.queues, binding.QueueName)
					rs.seen[binding.QueueName] = struct{}{}
				}
			}
		}
	case "fanout":
		for _, binding := range bindings {
			if _, ok := rs.seen[binding.QueueName]; !ok {
				rs.queues = append(rs.queues, binding.QueueName)
				rs.seen[binding.QueueName] = struct{}{}
			}
		}
	case "topic":
		for _, binding := range bindings {
			if b.matchTopicPattern(binding.RoutingKey, routingKey) {
				if _, ok := rs.seen[binding.QueueName]; !ok {
					rs.queues = append(rs.queues, binding.QueueName)
					rs.seen[binding.QueueName] = struct{}{}
				}
			}
		}
	case "headers":
		for _, binding := range bindings {
			if b.matchHeaders(binding.Arguments, message.Headers) {
				if _, ok := rs.seen[binding.QueueName]; !ok {
					rs.queues = append(rs.queues, binding.QueueName)
					rs.seen[binding.QueueName] = struct{}{}
				}
			}
		}
	default:
	}

	exchBindings, err := b.storage.GetExchangeBindingsFrom(exchange.Name)
	if err != nil {
		return fmt.Errorf("failed to get exchange-to-exchange bindings: %w", err)
	}

	for _, eb := range exchBindings {
		match := false
		switch exchange.Kind {
		case "direct":
			match = eb.RoutingKey == routingKey
		case "fanout":
			match = true
		case "topic":
			match = b.matchTopicPattern(eb.RoutingKey, routingKey)
		case "headers":
			match = b.matchHeaders(eb.Arguments, message.Headers)
		default:
		}
		if !match {
			continue
		}
		destExchange, err := b.storage.GetExchange(eb.Destination)
		if err != nil {
			continue
		}
		if err := b.collectTargetQueues(destExchange, routingKey, message, rs); err != nil {
			return err
		}
	}

	return nil
}

// Helper methods for routing

// matchHeaders implements RabbitMQ's headers-exchange match (R9,
// rabbit_exchange_type_headers.erl): every x-* binding key is skipped except
// under the *-with-x modes; a void (nil) binding value is a PRESENCE check
// (D5); typed values compare with headerValueMatches (integer widths equal,
// integer vs float not, longstr ≡ binary by bytes). match_all([]) = true,
// match_any([]) = false — a nil/empty binding therefore matches every
// message. x-match itself is validated at bind time (validateHeadersBinding).
func (b *StorageBroker) matchHeaders(bindingArgs map[string]interface{}, messageHeaders map[string]interface{}) bool {
	mode, _ := bindingArgs["x-match"].(string) // validated at bind time
	withX := mode == "all-with-x" || mode == "any-with-x"
	any := mode == "any" || mode == "any-with-x"
	for k, bv := range bindingArgs {
		if k == "x-match" || (!withX && strings.HasPrefix(k, "x-")) {
			continue
		}
		hv, present := messageHeaders[k] // nil map read is safe
		var ok bool
		if bv == nil {
			ok = present // void binding value = presence check
		} else {
			ok = present && headerValueMatches(bv, hv)
		}
		if any && ok {
			return true
		}
		if !any && !ok {
			return false
		}
	}
	return !any // match_all([]) = true, match_any([]) = false
}

// validateHeadersBinding implements RabbitMQ's validate_binding for a headers
// exchange (R9): x-match must be absent or a longstr naming one of the four
// modes; anything else is binding_invalid (406).
func validateHeadersBinding(args map[string]interface{}) error {
	v, present := args["x-match"]
	if !present {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return semErr(fmt.Sprintf("Invalid x-match field type %T (value %v); expected longstr", v, v), ErrInvalidBindingArgument)
	}
	switch s {
	case "all", "any", "all-with-x", "any-with-x":
		return nil
	}
	return semErr(fmt.Sprintf("Invalid x-match field value %s; expected all, any, all-with-x, or any-with-x", s), ErrInvalidBindingArgument)
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

// GetConsumers returns all active consumers (for management interface), keyed
// by their broker-internal identity (Consumer.ID). It is keyed by identity and
// not by consumer tag because tags are only channel-scoped: keying by tag would
// silently collapse two legitimately-same-tagged consumers into one entry and
// under-report the consumer count. Each value's Tag field carries the
// client-visible tag for display.
func (b *StorageBroker) GetConsumers() map[string]*protocol.Consumer {
	result := make(map[string]*protocol.Consumer)
	b.activeConsumers.Range(func(key, value interface{}) bool {
		consumerID := key.(string)
		state := value.(*ConsumerState)
		result[consumerID] = state.consumer
		return true
	})

	return result
}

func (b *StorageBroker) GetQueueConsumerCount(queueName string) int {
	if val, ok := b.queueConsumers.Load(queueName); ok {
		return len(val.([]*ConsumerState))
	}
	return 0
}

// GetQueueReadyCount returns the number of READY messages in a queue — published
// and requeued messages that are available for delivery, EXCLUDING messages that
// are delivered-but-unacked (inflight). This is RabbitMQ's queue.declare-ok
// message_count semantics. Used to populate queue.declare-ok /
// queue.declare-passive-ok (the protocol.Queue.MessageCount runtime counter is
// never maintained, so reading it always yielded 0 — a wire-fidelity bug caught
// by the W7 conformance suite).
//
// The storage ring count (basic.get's source) is deliberately NOT used here: the
// ring still holds inflight-unacked messages until they are acked, so it counts
// ready+unacked and over-reports vs RabbitMQ. QueueState.waiting is decremented
// on ClaimInflight and re-incremented on Requeue, so WaitingCount is the true
// ready depth. Returns 0 for an unknown / uninitialised queue (read-only load;
// never creates a queue state).
func (b *StorageBroker) GetQueueReadyCount(queueName string) uint32 {
	val, ok := b.queueStates.Load(queueName)
	if !ok {
		return 0
	}
	ready := val.(*QueueState).WaitingCount()
	if ready < 0 {
		return 0
	}
	return uint32(ready)
}

// GetQueueConsumerIDs returns the broker-internal identities (Consumer.ID) of
// every consumer currently attached to a queue.
//
// Identities, not tags: the caller (queue.delete's consumer-cancel notification)
// has to find each consumer's channel across every open connection, and a bare
// tag does not identify one — the same tag may legitimately be in use on other
// channels, so a tag-keyed search cancels whichever consumer it happens to find
// first. The wire tag to put in the basic.cancel is read off the consumer that
// the identity resolves to.
func (b *StorageBroker) GetQueueConsumerIDs(queueName string) []string {
	mu := b.getQueueConsumersMutex(queueName)
	mu.Lock()
	defer mu.Unlock()
	if val, ok := b.queueConsumers.Load(queueName); ok {
		consumers := val.([]*ConsumerState)
		ids := make([]string, 0, len(consumers))
		for _, state := range consumers {
			ids = append(ids, state.consumer.ID)
		}
		return ids
	}
	return nil
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

	// Create metadata structure
	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []*protocol.Exchange{},
		Queues:      []*protocol.Queue{},
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

// AcknowledgeMessage handles message acknowledgment (lock-free hot path).
// consumerID is the broker-internal consumer identity (Consumer.ID) recorded
// on the delivery when it was sent; the hot path is a single string-keyed
// sync.Map load, exactly as it was when this map was (incorrectly) keyed by
// the channel-scoped consumer tag.
func (b *StorageBroker) AcknowledgeMessage(consumerID string, deliveryTag uint64, multiple bool) error {
	val, ok := b.activeConsumers.Load(consumerID)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	if multiple {
		// Enumerate the consumer's authoritative unacked set (the AckCursor's
		// unacked map, gate-bounded so this is O(prefetch), not O(queue)) and
		// route every tag <= deliveryTag through settle(). Each settle win IS the
		// one gate-credit release for that tag, so the total release equals the
		// number of tags actually settled — never a separately-computed count
		// that can skew from the acquired credits (design §3). Stale or
		// already-settled tags lose the LoadAndDelete and are skipped with no
		// release. The enumeration is a complete-at-ack-time superset: OnDeliver
		// runs before the channel send, and deliveries to one consumer are FIFO,
		// so any tag the client could have received (and thus multi-acked) is
		// present here.
		tags, _ := b.storage.GetUnackedTags(state.queueName, consumerID)
		for _, tag := range tags {
			if tag > deliveryTag {
				continue
			}
			if _, won := b.settle(tag); !won {
				continue
			}
			b.finishAck(queueState, state.queueName, consumerID, tag)
		}
		queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))
	} else {
		// Acknowledge single message. Settle funnels the LoadAndDelete guard
		// (against duplicate ACK or concurrent UnregisterConsumer cleanup) and
		// the single gate-credit release. Only the winner runs the path-specific
		// bookkeeping below.
		if _, won := b.settle(deliveryTag); !won {
			return nil
		}
		// Delete the message, clear its pending ack, notify the AckCursor,
		// sync the min-ack cursor for backpressure, and advance the depth
		// frontier (may jump minAckCursor to head if the queue drained).
		b.ackDelivered(queueState, state.queueName, state.consumer.ID, deliveryTag)
	}

	// Done - polling loop will acquire permit and pull next message
	return nil
}

// RejectMessage handles message rejection (lock-free hot path). consumerID is
// the broker-internal consumer identity (Consumer.ID).
func (b *StorageBroker) RejectMessage(consumerID string, deliveryTag uint64, requeue bool) error {
	val, ok := b.activeConsumers.Load(consumerID)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	// Settle funnels the LoadAndDelete guard (against duplicate reject or
	// concurrent UnregisterConsumer cleanup) and the single gate-credit release.
	if _, won := b.settle(deliveryTag); !won {
		return nil
	}
	b.finishNack(queueState, state.queueName, state.consumer.ID, deliveryTag, requeue)

	return nil
}

// NacknowledgeMessage handles negative acknowledgment (lock-free hot path).
// consumerID is the broker-internal consumer identity (Consumer.ID).
func (b *StorageBroker) NacknowledgeMessage(consumerID string, deliveryTag uint64, multiple, requeue bool) error {
	val, ok := b.activeConsumers.Load(consumerID)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	// Get queue state
	queueState := b.getOrCreateQueueState(state.queueName)

	if multiple {
		// Enumerate-and-settle over the consumer's authoritative unacked set,
		// mirroring multi-ack (design §3): every tag <= deliveryTag is claimed
		// via settle() (its LoadAndDelete win is the one gate-credit release),
		// then requeued or discarded. No separately-computed release count.
		tags, _ := b.storage.GetUnackedTags(state.queueName, consumerID)
		if requeue {
			// Requeue is in-memory and DLX-free; the per-tag finishNack path is
			// cheap. (Its requeue==false branch, and its Policy() load, are never
			// reached here.)
			for _, tag := range tags {
				if tag > deliveryTag {
					continue
				}
				if _, won := b.settle(tag); !won {
					continue
				}
				b.finishNack(queueState, state.queueName, consumerID, tag, true)
			}
		} else {
			// requeue==false discard: batch the DLX republishes so we do not
			// serialize N synchronous WAL group-commit fsyncs on this frame
			// goroutine (a real latency cliff). One Policy() load is hoisted.
			b.discardNackedBatch(queueState, state.queueName, consumerID, tags, deliveryTag)
		}
		queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))
	} else {
		// Nacknowledge single message. Settle funnels the LoadAndDelete guard
		// (against duplicate nack or concurrent UnregisterConsumer cleanup) and
		// the single gate-credit release.
		if _, won := b.settle(deliveryTag); !won {
			return nil
		}
		b.finishNack(queueState, state.queueName, state.consumer.ID, deliveryTag, requeue)
	}

	return nil
}

// GetConsumerForDelivery returns the broker-internal consumer identity
// (Consumer.ID) that owns a given delivery tag, or "" for a basic.get delivery.
// This provides O(1) lookup for ACK routing using globally unique delivery tags.
func (b *StorageBroker) GetConsumerForDelivery(deliveryTag uint64) (string, bool) {
	val, ok := b.deliveryIndex.Load(deliveryTag)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// RebuildDeliveryIndex rebuilds a single delivery index entry (used during crash
// recovery). consumerID is the identity recorded on the pending-ack record; no
// consumer by that identity exists after a restart, so the entry exists only so
// the tag is claimable for requeue.
func (b *StorageBroker) RebuildDeliveryIndex(deliveryTag uint64, consumerID string) {
	b.deliveryIndex.Store(deliveryTag, consumerID)
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
func (b *StorageBroker) RecoverQueue(queueName string, minTag, maxTag, count uint64) {
	queueState := b.getOrCreateQueueState(queueName)
	queueState.Recover(minTag, maxTag, count)

	// SQ-11: reconstruct the ready-bytes counter for a durable queue that
	// enforces x-max-length-bytes so the byte limit is enforced correctly after
	// a restart. Queue metadata (and thus the policy) is recovered BEFORE
	// messages (recovery steps 3 then 5), so the policy is already attached.
	// One-time, cold, and only for byte-limited queues; gaps (tags acked before
	// restart) return not-found and are skipped.
	if p := queueState.Policy(); p != nil && p.HasMaxLengthBytes && count > 0 {
		var total int64
		for tag := minTag; tag <= maxTag; tag++ {
			if msg, err := b.storage.GetMessage(queueName, tag); err == nil && msg != nil {
				total += int64(len(msg.Body))
			}
		}
		if total > 0 {
			queueState.AddReadyBytes(total)
		}
	}
}

// GetMessageForGet attempts to synchronously retrieve the next message from a
// queue for a basic.get operation. If no message is available, returns
// (nil, 0, 0, nil). When noAck is false, the delivery is tracked for
// ack/nack/reject using an empty consumer identity ("") in the deliveryIndex
// — basic.get has no consumer, so it owns no Consumer.ID.
// Returns the message, its delivery tag, the remaining message count, and any
// error.
func (b *StorageBroker) GetMessageForGet(queueName string, noAck bool) (*protocol.Message, uint64, uint32, error) {
	_, err := b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return nil, 0, 0, errNoQueue(queueName)
		}
		return nil, 0, 0, err
	}

	queueState := b.getOrCreateQueueState(queueName)

	// W4 SQ-9: load the policy once (shared by the x-expires idle-clock reset and
	// the TTL head-check below). A basic.get is queue "use", so it resets the
	// x-expires idle timer.
	p := queueState.Policy()
	if p != nil && p.HasQueueExpires {
		queueState.MarkActivity(b.ttlNowMillis())
	}

	// Non-blocking claim loop: try requeue ring first, then walk the
	// tail→head cursor. basic.get is synchronous and must return immediately
	// if no message is available. Gap tags (from recovered/acked messages)
	// are skipped via the GetMessage-not-found path, mirroring deliverMessage.
	var tag uint64
	var message *protocol.Message

	if t, r, ok := queueState.tryPopRequeue(); ok {
		tag = t
		message, _ = b.storage.GetMessage(queueName, tag)
		if message != nil {
			// W4 SQ-9: drop an expired requeued message instead of returning it;
			// fall through to the tail cursor for the next candidate.
			if message.EnqueueUnixMilli != 0 && messageExpired(message, p, b.ttlNowMillis()) {
				b.dropExpiredOnGet(queueState, queueName, message, tag, p)
				message = nil
			}
			if message != nil {
				message.Redelivered = r
			}
		}
	}

	if message == nil {
		for {
			t := queueState.tail.Load()
			h := queueState.head.Load()
			if t >= h {
				break
			}
			if !queueState.tail.CompareAndSwap(t, t+1) {
				continue
			}
			tag = t
			msg, err := b.storage.GetMessage(queueName, tag)
			if err != nil {
				queueState.GapSkipAdvance(tag)
				continue
			}
			// W4 SQ-9 delivery-time (basic.get) TTL head-check: dead-letter
			// (reason=expired) or drop an expired message rather than returning
			// it, then keep walking the cursor. Depth accounting is gated on the
			// ring-delete win (see dropExpiredOnGet), reconciling with the reaper.
			if msg.EnqueueUnixMilli != 0 && messageExpired(msg, p, b.ttlNowMillis()) {
				b.dropExpiredOnGet(queueState, queueName, msg, tag, p)
				continue
			}
			message = msg
			break
		}
	}

	if message == nil {
		return nil, 0, 0, nil
	}

	// basic.get-ok message-count is RabbitMQ's "messages remaining": the READY
	// depth EXCLUDING the message being returned. It is symmetric with the
	// queue.declare-ok ready-count (GetQueueReadyCount == WaitingCount); the
	// storage ring count would include delivered-but-unacked messages and
	// over-report. The selected message is still counted in `waiting` here
	// (ClaimInflight runs below, only for manual-ack gets), so subtract one for it.
	remaining := b.GetQueueReadyCount(queueName)
	if remaining > 0 {
		remaining--
	}

	if !noAck {
		queueState.ClaimInflight(tag)
		// SQ-11 max-length: claimed message left the ready set. Reuses the shared
		// Policy() load `p` from the top of GetMessageForGet.
		subReadyBytesOnClaim(queueState, p, len(message.Body))
		b.deliveryIndex.Store(tag, "")
		b.getDeliveryQueues.Store(tag, queueName)
		b.storage.StorePendingAck(&protocol.PendingAck{
			QueueName:   queueName,
			DeliveryTag: tag,
			ConsumerTag: "",
		})
	} else {
		// No-ack basic.get is an implicit ack (§4): the message must be settled
		// HERE, mirroring the push no-ack settle branch. Remove it from storage and
		// account queue depth, gated on winning the ring removal so it reconciles
		// with the SQ-9 reaper (a consumer-less TTL queue's reaper can target the
		// same tag, exactly like the head-check drop paths). On a win the message is
		// already removed by deleteIfPresent, so we only settle the counters —
		// ClaimInflight (waiting-1, inflight+1) + subReadyBytesOnClaim + AckAdvance
		// (inflight-1) — matching how the reaper/head-check remove-then-account. On
		// a loss the reaper already removed and accounted the tag, or the tag was
		// absent, so we advance the
		// depth frontier without touching the counters (GapSkipAdvance). Without
		// this branch a no-ack basic.get leaked the message (durable redelivery
		// after restart) and drifted both `waiting` and `readyBytes` upward.
		if b.deleteIfPresent(queueName, tag) {
			queueState.ClaimInflight(tag)
			subReadyBytesOnClaim(queueState, p, len(message.Body))
			queueState.AckAdvance(tag)
		} else {
			queueState.GapSkipAdvance(tag)
		}
	}

	return message, tag, remaining, nil
}

// PurgeQueue removes all waiting messages from a queue while keeping the
// queue itself and its bindings intact. Returns the number of messages purged.
//
// It resolves the queue name to an incarnation EXACTLY ONCE and hands that
// incarnation to purgeIncarnation. HEAD resolved the name three times over
// (storage ring lookup, then queueStates lookup) with nothing serializing them,
// so a concurrent delete+redeclare could apply the ring purge to one
// incarnation and the cursor reset to its successor — stranding the
// successor's already-confirmed durable messages as undispatchable while they
// sat intact on disk.
func (b *StorageBroker) PurgeQueue(name string) (int, error) {
	return b.purgeIncarnation(name, b.getOrCreateQueueState(name))
}

// purgeIncarnation applies a purge to the ONE incarnation it is handed,
// removing READY messages ONLY (WS-D §4, RabbitMQ parity). A message is ready
// iff it is (a) in the requeue ring or (b) an unclaimed tag in [tail, head);
// both sets are popped through the SAME claim mechanisms consumers use
// (requeueMu snapshot / tail CAS), so an in-flight (delivered-unacked) tag is
// never touched: its body survives for a later requeue, `inflight` stays
// correct for AckAdvance, and spilled ready messages are WAL-acked through the
// §8 cold path (they no longer resurrect on restart, D-NEW-3). purge-ok's
// message-count is the number of READY messages removed.
//
// Concurrency: consumers' Claim/tryPopRequeue and drop-head's PopOldestReady
// arbitrate with purge on the same CAS and mutex, so each tag has exactly one
// owner; the TTL reaper's deleteIfPresent and §8 arbitration ensure exactly
// one of {reaper, purge} decrements depth per tag. No new lock is taken:
// maxLenMu is not needed because purge only LOWERS waiting/readyBytes, which
// can never cause a reject-publish overshoot. Cost is O(ready) per purge.
//
// DELIBERATE BEHAVIOUR CHANGE (documented, was :3977-4003): ring residents
// that are neither ready nor in-flight (the B-2 out-of-band leak) are no
// longer swept by purge. storage.PurgeQueue is still used by
// deleteQueueIncarnation, unchanged.
func (b *StorageBroker) purgeIncarnation(name string, qs *QueueState) (int, error) {
	if qs.closed.Load() {
		return 0, errNoQueue(name)
	}
	p := qs.Policy()
	purged := 0

	// (a) Snapshot-and-empty the requeue ring under requeueMu. Requeue() after
	// this point re-adds to the (now empty) ring; those are post-purge readies.
	qs.requeueMu.Lock()
	snap := make([]uint64, 0, qs.requeueLen)
	bufCap := cap(qs.requeueBuf)
	for i := 0; i < qs.requeueLen; i++ {
		snap = append(snap, qs.requeueBuf[(qs.requeueHead+i)%bufCap].tag)
	}
	qs.requeueHead, qs.requeueLen = 0, 0
	qs.requeueCount.Store(0)
	qs.requeueMu.Unlock()
	for _, tag := range snap {
		if b.purgeOne(qs, name, tag, p) {
			purged++
		}
	}

	// (b) Pop [tail, h0) by CAS, exactly like PopOldestReady. h0 is captured
	// once: messages that become visible after the purge started are retained.
	h0 := qs.head.Load()
	for {
		t := qs.tail.Load()
		if t >= h0 {
			break
		}
		if !qs.tail.CompareAndSwap(t, t+1) {
			continue
		}
		if b.purgeOne(qs, name, t, p) {
			purged++
		}
	}
	qs.SetMinAckCursor(b.storage.GetMinAckCursor(name))
	qs.WakeAll()
	return purged, nil
}

// purgeOne removes one popped ready tag. Mirrors dropHeadEvictOne
// (max_length.go) minus dead-lettering (purge never dead-letters).
func (b *StorageBroker) purgeOne(qs *QueueState, name string, tag uint64, p *QueuePolicy) bool {
	var bodyLen int
	if p != nil && p.HasMaxLengthBytes {
		m, err := b.storage.GetMessage(name, tag)
		if err != nil || m == nil {
			qs.GapSkipAdvance(tag)
			return false
		}
		bodyLen = len(m.Body)
	}
	if !b.deleteIfPresent(name, tag) { // §8 semantics: true iff THIS call removed a present message
		qs.GapSkipAdvance(tag) // gap tag, or the reaper won the race
		return false
	}
	qs.ClaimInflight(tag)
	subReadyBytesOnClaim(qs, p, bodyLen)
	qs.AckAdvance(tag)
	return true
}

// QueueCounters reports (ready, inflight, pending) for name without creating
// any state — a test-only hook (spec §17). pending is the §7 reject-publish
// reservation count (0 until that lands).
func (b *StorageBroker) QueueCounters(name string) (ready, inflight, pending int64) {
	if qv, ok := b.queueStates.Load(name); ok {
		qs := qv.(*QueueState)
		return qs.WaitingCount(), qs.InflightCount(), qs.pendingCount.Load()
	}
	return 0, 0, 0
}

// AcknowledgeGetDelivery handles acknowledgment of a basic.get delivery
// (identified by an empty consumer identity in the deliveryIndex).
func (b *StorageBroker) AcknowledgeGetDelivery(deliveryTag uint64) error {
	if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
		return nil
	}

	val, ok := b.getDeliveryQueues.LoadAndDelete(deliveryTag)
	if !ok {
		return nil
	}
	queueName := val.(string)

	queueState := b.getOrCreateQueueState(queueName)
	b.storage.DeleteMessage(queueName, deliveryTag)
	b.storage.DeletePendingAck(queueName, deliveryTag)
	queueState.AckAdvance(deliveryTag)
	queueState.SetMinAckCursor(b.storage.GetMinAckCursor(queueName))

	return nil
}

// RejectGetDelivery handles rejection of a basic.get delivery.
func (b *StorageBroker) RejectGetDelivery(deliveryTag uint64, requeue bool) error {
	if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
		return nil
	}

	val, ok := b.getDeliveryQueues.LoadAndDelete(deliveryTag)
	if !ok {
		return nil
	}
	queueName := val.(string)

	queueState := b.getOrCreateQueueState(queueName)
	b.storage.DeletePendingAck(queueName, deliveryTag)

	if requeue {
		// SQ-11: restore ready bytes when the queue enforces x-max-length-bytes.
		b.restoreReadyBytesOnRequeue(queueState, queueName, deliveryTag)
		queueState.Requeue(deliveryTag)
	} else {
		// Dead-letter (reason=rejected) before discarding, mirroring finishNack:
		// republish precedes DeleteMessage (at-least-once); Policy() loaded only
		// on this requeue==false path.
		b.deadLetterOnDiscard(queueState, queueName, deliveryTag)
		b.storage.DeleteMessage(queueName, deliveryTag)
		queueState.AckAdvance(deliveryTag)
	}

	return nil
}

// NackGetDelivery handles negative acknowledgment of a basic.get delivery.
func (b *StorageBroker) NackGetDelivery(deliveryTag uint64, requeue bool) error {
	return b.RejectGetDelivery(deliveryTag, requeue)
}

// RequeueAllForConsumer requeues all unacknowledged (in-flight) messages
// for the given consumer. This is used by basic.recover to redeliver
// unacked messages. The requeued messages become available for delivery
// to any consumer on the same queue (including the original consumer).
// consumerID is the broker-internal consumer identity (Consumer.ID).
func (b *StorageBroker) RequeueAllForConsumer(consumerID string) error {
	val, ok := b.activeConsumers.Load(consumerID)
	if !ok {
		return nil
	}
	state := val.(*ConsumerState)

	queueState := b.getOrCreateQueueState(state.queueName)

	// Enumerate-and-settle the consumer's entire unacked set (no deliveryTag
	// bound: basic.recover requeues everything). settle() funnels the
	// LoadAndDelete guard and the one gate-credit release per tag; finishNack
	// with requeue=true removes each tag from the AckCursor unacked set (so a
	// later redelivery's OnDeliver re-adds it cleanly) before requeuing it.
	tags, _ := b.storage.GetUnackedTags(state.queueName, consumerID)
	for _, tag := range tags {
		if _, won := b.settle(tag); !won {
			continue
		}
		b.finishNack(queueState, state.queueName, consumerID, tag, true)
	}

	queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))

	return nil
}

// RequeueAllGetDeliveries requeues all in-flight basic.get deliveries
// (identified by an empty consumer identity). Called during connection close
// to prevent messages from being permanently orphaned when a client
// disconnects without acking basic.get deliveries.
func (b *StorageBroker) RequeueAllGetDeliveries() {
	b.getDeliveryQueues.Range(func(key, value interface{}) bool {
		deliveryTag := key.(uint64)
		queueName := value.(string)

		if _, loaded := b.deliveryIndex.LoadAndDelete(deliveryTag); !loaded {
			return true
		}
		b.getDeliveryQueues.Delete(deliveryTag)

		queueState := b.getOrCreateQueueState(queueName)
		b.storage.DeletePendingAck(queueName, deliveryTag)
		// SQ-11: restore ready bytes when the queue enforces x-max-length-bytes.
		b.restoreReadyBytesOnRequeue(queueState, queueName, deliveryTag)
		queueState.Requeue(deliveryTag)

		return true
	})
}
