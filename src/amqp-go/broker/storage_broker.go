package broker

import (
	"errors"
	"fmt"
	"reflect"
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
	globalDeliveryTag atomic.Uint64
	metricsCollector  MetricsCollector
	// logger is used only on cold paths (e.g. a dropped dead-letter). nil is
	// treated as "no logging" so callers that never wire one pay nothing; the
	// server builder injects its interfaces.Logger via SetLogger.
	logger interfaces.Logger

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
}

// NewStorageBroker creates a new storage-backed broker instance
// Phase 6G: Now accepts EngineConfig for tunable parameters
func NewStorageBroker(storage interfaces.Storage, engineConfig interfaces.EngineConfig) *StorageBroker {
	broker := &StorageBroker{
		storage:      storage,
		engineConfig: engineConfig,
		ttlNow:       func() int64 { return time.Now().UnixMilli() },
	}

	broker.initializeDefaultExchanges()

	return broker
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
}

// ringDeleter is the optional storage capability the SQ-9 TTL reaper and
// delivery head-check use to atomically claim a message removal (the
// linearization token). DisruptorStorage implements it; a backend that does not
// falls back to the unconditional DeleteMessage (treated as a win), which is
// safe because such a backend is used single-writer in the paths that need it.
type ringDeleter interface {
	DeleteMessageIfPresent(queueName string, deliveryTag uint64) (bool, error)
}

// deleteIfPresent removes a message from storage and reports whether THIS call
// won the removal (the ring CAS). Depth is decremented only by the winner, so at
// most one of {reaper drop, consumer head-check} decrements depth per tag.
func (b *StorageBroker) deleteIfPresent(queueName string, tag uint64) bool {
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

func (b *StorageBroker) UpdateConsumerPrefetch(consumerTag string, prefetchCount uint16) {
	val, ok := b.activeConsumers.Load(consumerTag)
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
//     consumer-cancel cleanup scans (deliveryIndex maps tag -> consumer tag).
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
	b.deliveryIndex.Store(msgID, state.consumer.Tag)
	queueState.ClaimInflight(msgID)
	if !state.consumer.NoAck {
		b.storage.DeliverToConsumer(state.queueName, state.consumer.Tag, msgID)
		b.storage.StorePendingAck(&protocol.PendingAck{
			QueueName:   state.queueName,
			DeliveryTag: msgID,
			ConsumerTag: state.consumer.Tag,
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
		ConsumerTag: state.consumer.Tag,
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
			b.finishNack(queueState, state.queueName, state.consumer.Tag, msgID, true)
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
func (b *StorageBroker) ackDelivered(queueState *QueueState, queueName, consumerTag string, deliveryTag uint64) {
	b.finishAck(queueState, queueName, consumerTag, deliveryTag)
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
func (b *StorageBroker) finishAck(queueState *QueueState, queueName, consumerTag string, deliveryTag uint64) {
	b.storage.DeleteMessage(queueName, deliveryTag)
	b.storage.DeletePendingAck(queueName, deliveryTag)
	b.storage.AckFromConsumer(queueName, consumerTag, deliveryTag)
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
func (b *StorageBroker) finishNack(queueState *QueueState, queueName, consumerTag string, deliveryTag uint64, requeue bool) {
	b.storage.NackFromConsumer(queueName, consumerTag, deliveryTag)
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
func (b *StorageBroker) discardNackedBatch(queueState *QueueState, queueName, consumerTag string, tags []uint64, upTo uint64) {
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
			b.storage.NackFromConsumer(queueName, consumerTag, tag)
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
		b.storage.NackFromConsumer(queueName, consumerTag, tag)
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

	// If exists, validate properties match
	if existing != nil {
		if existing.Kind != exchangeType {
			return fmt.Errorf("%w: exchange '%s' expected %s, got %s", ErrExchangeTypeMismatch, name, existing.Kind, exchangeType)
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

// DeleteExchange removes an exchange
func (b *StorageBroker) DeleteExchange(name string, ifUnused bool) error {
	_, err := b.storage.GetExchange(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return nil
		}
		return err
	}

	if ifUnused {
		bindings, err := b.storage.GetExchangeBindings(name)
		if err != nil {
			return fmt.Errorf("failed to check exchange bindings: %w", err)
		}
		if len(bindings) > 0 {
			return fmt.Errorf("exchange '%s' in use", name)
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

	// If exists, validate properties match
	if existing != nil {
		if existing.Durable != durable || existing.AutoDelete != autoDelete || existing.Exclusive != exclusive {
			return nil, fmt.Errorf("queue '%s' properties mismatch", name)
		}

		// Add to active cache (lock-free)
		b.activeQueues.Store(name, existing)

		// Ensure queue state exists and carries the resolved policy. The
		// stored (original) arguments win: redeclare arguments are not
		// compared today (known gap — AMQP 0.9.1 says differing args should
		// be a 406), so they must not overwrite the policy either. This
		// branch also covers recovery when the storage backend already holds
		// the durable queue across restarts. Resolution errors are tolerated
		// here (nil policy) so queues persisted before validation existed
		// remain usable.
		qs := b.getOrCreateQueueState(name)
		if policy, perr := ResolveQueuePolicy(existing.Arguments); perr == nil {
			qs.SetPolicy(policy)
		}
		// W4 SQ-9: a (re)declare is queue "use" (resets the x-expires idle clock)
		// and must ensure the reaper is running (e.g. on durable recovery, whose
		// queues re-enter through this branch).
		qs.MarkActivity(b.ttlNowMillis())
		b.maybeStartReaper(name, qs)

		return existing, nil
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

	// Create new queue
	queue := protocol.NewQueue(name, durable, autoDelete, exclusive, arguments)

	// Store queue
	err = b.storage.StoreQueue(queue)
	if err != nil {
		return nil, err
	}

	// Add to active cache (lock-free)
	b.activeQueues.Store(name, queue)

	// Ensure queue state exists and attach the resolved policy (nil when no
	// known x-arguments were supplied).
	qs := b.getOrCreateQueueState(name)
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

// DeleteQueue removes a queue
func (b *StorageBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error) {
	_, err := b.storage.GetQueue(name)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return 0, nil
		}
		return 0, err
	}

	if ifUnused {
		consumers, err := b.storage.GetQueueConsumers(name)
		if err != nil {
			return 0, fmt.Errorf("failed to check queue consumers: %w", err)
		}
		if len(consumers) > 0 {
			return 0, fmt.Errorf("queue '%s' in use", name)
		}
	}

	if ifEmpty {
		count, err := b.storage.GetQueueMessageCount(name)
		if err != nil {
			return 0, fmt.Errorf("failed to check queue message count: %w", err)
		}
		if count > 0 {
			return 0, fmt.Errorf("queue '%s' not empty", name)
		}
	}

	mu := b.getQueueConsumersMutex(name)
	mu.Lock()
	if val, ok := b.queueConsumers.Load(name); ok {
		consumers := val.([]*ConsumerState)
		for _, state := range consumers {
			state.gate.stop()
			state.stopOnce.Do(func() { close(state.stopCh) })
			b.activeConsumers.Delete(state.consumer.Tag)
		}
	}
	b.queueConsumers.Delete(name)
	mu.Unlock()

	purgedCount, _ := b.storage.PurgeQueue(name)

	bindings, err := b.storage.GetQueueBindings(name)
	if err == nil {
		for _, binding := range bindings {
			b.storage.DeleteBinding(binding.QueueName, binding.ExchangeName, binding.RoutingKey)
		}
	}

	consumers, err := b.storage.GetQueueConsumers(name)
	if err == nil {
		for _, consumer := range consumers {
			b.storage.DeleteConsumer(name, consumer.Tag)
			b.activeConsumers.Delete(consumer.Tag)
		}
	}

	b.activeQueues.Delete(name)
	b.queueOwners.Delete(name)
	if qval, qok := b.queueStates.LoadAndDelete(name); qok {
		qval.(*QueueState).Close()
	}

	err = b.storage.DeleteQueue(name)
	if err != nil {
		return purgedCount, err
	}
	return purgedCount, nil
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
	if err := b.storage.DeleteBinding(queueName, exchangeName, routingKey); err != nil {
		return err
	}

	if exch, err := b.storage.GetExchange(exchangeName); err == nil && exch.AutoDelete {
		bindings, _ := b.storage.GetExchangeBindings(exchangeName)
		fromBindings, _ := b.storage.GetExchangeBindingsFrom(exchangeName)
		if len(bindings) == 0 && len(fromBindings) == 0 {
			b.DeleteExchange(exchangeName, false)
		}
	}

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

	_, err := b.storage.GetExchange(source)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("source exchange '%s' not found", source)
		}
		return err
	}

	destExchange, err := b.storage.GetExchange(destination)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("destination exchange '%s' not found", destination)
		}
		return err
	}

	if destExchange.Internal {
		return fmt.Errorf("cannot bind to internal exchange '%s'", destination)
	}

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

	if exch, err := b.storage.GetExchange(source); err == nil && exch.AutoDelete {
		bindings, _ := b.storage.GetExchangeBindings(source)
		fromBindings, _ := b.storage.GetExchangeBindingsFrom(source)
		if len(bindings) == 0 && len(fromBindings) == 0 {
			b.DeleteExchange(source, false)
		}
	}

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

// RegisterConsumer registers a new consumer for a queue
func (b *StorageBroker) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	_, err := b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return errors.New("queue does not exist")
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

	b.activeConsumers.Store(consumerTag, state)

	b.storage.RegisterConsumerCursor(queueName, consumerTag)

	mu.Lock()
	defer mu.Unlock()
	val, _ := b.queueConsumers.LoadOrStore(queueName, []*ConsumerState{})
	consumers := val.([]*ConsumerState)
	consumers = append(consumers, state)
	b.queueConsumers.Store(queueName, consumers)

	go b.consumerPollLoop(state, queueState)

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

	// 7. Scan the deliveryIndex ledger for remaining in-flight messages owned by
	//    this consumer. These are deliveries that already LEFT consumer.Messages
	//    (pulled by the server's forwarder goroutine, sitting in the fanIn
	//    channel buffer, or already sent to the client over TCP) but were never
	//    ACKed. Without this scan, they would be permanently orphaned. The
	//    deliveryIndex maps tag -> consumer tag and is the single ownership
	//    record (step 5 deleted the duplicate QueueState.inflightOwners map).
	//    Messages already claimed by deliverMessage's stopCh path, the drain
	//    above, or a concurrent ack are gone from the ledger (LoadAndDelete), so
	//    requeueInflightDelivery's own LoadAndDelete guard makes this a no-op for
	//    them — no double-requeue. basic.get deliveries carry an empty consumer
	//    tag and are correctly skipped by the consumer-tag filter. Tags are
	//    collected first so the requeue's LoadAndDelete does not mutate the map
	//    mid-Range.
	//
	//    The scan is O(total in-flight deliveries broker-wide) — acceptable
	//    because consumer cancellation is a rare event.
	var orphanedTags []uint64
	b.deliveryIndex.Range(func(key, value interface{}) bool {
		if value.(string) == consumerTag {
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
	b.storage.UnregisterConsumerCursor(state.queueName, consumerTag)

	mu := b.getQueueConsumersMutex(state.queueName)
	mu.Lock()
	lastConsumer := false
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
			lastConsumer = true
		}
	}
	mu.Unlock()

	if lastConsumer {
		if q, err := b.storage.GetQueue(state.queueName); err == nil && q.AutoDelete {
			b.DeleteQueue(state.queueName, false, false)
		}
	}

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
func (b *StorageBroker) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	// Get exchange (lock-free)
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return fmt.Errorf("exchange '%s' not found", exchangeName)
		}
		return err
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

	// Enqueue to all target queues (lock-free, no consumer iteration)
	rejected := false
	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		if !queueState.WaitForCapacity(queueState.StopCh()) {
			return fmt.Errorf("queue '%s' closed during backpressure wait", queueName)
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

		msgID := b.globalDeliveryTag.Add(1)

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
			if rejectPublish {
				queueState.maxLenMu.Unlock()
			}
			return fmt.Errorf("failed to store message to queue '%s': %w", queueName, err)
		}

		// Advance the per-queue head cursor and wake parked consumers.
		// Replaces the old `available <- msgID` channel send.
		queueState.Publish(msgID)

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

// PublishMessageTx is the transactional (SQ-8) sibling of PublishMessage. It
// performs identical routing and delivery-tag assignment, but writes the message
// to the supplied transactional storage view (txnStore) instead of the live
// broker storage, and DEFERS making the message visible to consumers.
//
// The returned closures perform the "make visible" step (advance the per-queue
// head cursor and wake consumers). The caller runs them only AFTER the atomic
// storage commit has succeeded, so a transaction that aborts (a later operation
// fails) leaves nothing durable AND nothing visible — no consumer can observe a
// message from a transaction that never committed. This preserves the invariant
// that a message becomes visible only after it is durable.
//
// This is the slow path; it deliberately duplicates PublishMessage rather than
// refactoring it, to leave the non-transactional publish hot path untouched.
func (b *StorageBroker) PublishMessageTx(txnStore interfaces.Storage, exchangeName, routingKey string, message *protocol.Message) ([]func(), error) {
	exchange, err := b.storage.GetExchange(exchangeName)
	if err != nil {
		if errors.Is(err, interfaces.ErrExchangeNotFound) {
			return nil, fmt.Errorf("exchange '%s' not found", exchangeName)
		}
		return nil, err
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

	deferred := make([]func(), 0, len(targetQueues))

	for i, queueName := range targetQueues {
		queueState := b.getOrCreateQueueState(queueName)

		if !queueState.WaitForCapacity(queueState.StopCh()) {
			return nil, fmt.Errorf("queue '%s' closed during backpressure wait", queueName)
		}

		msgID := b.globalDeliveryTag.Add(1)

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
			return nil, fmt.Errorf("failed to stage message to queue '%s': %w", queueName, err)
		}

		// Defer ONLY consumer visibility until after the atomic commit succeeds.
		// The TTL anchor is already stamped above (part of the durable write), so
		// the closure does no TTL work.
		qs := queueState
		id := msgID
		qn := queueName
		bodyLen := len(storeMsg.Body)
		deferred = append(deferred, func() {
			qs.Publish(id)
			// SQ-11 max-length for transactional publishes: byte accounting +
			// drop-head eviction run AFTER the post-commit visibility. Since the
			// message is already durably committed, reject-publish is NOT
			// enforced here (refusing it would strand a durable message) — a
			// reject-publish queue accepts tx publishes (documented divergence;
			// W7 locks the exact tx-vs-reject-publish semantics).
			if p != nil {
				addReadyBytesOnEnqueue(qs, p, bodyLen)
				if p.Overflow == OverflowDropHead && (p.HasMaxLength || p.HasMaxLengthBytes) {
					b.dropHeadTrim(qs, qn, p)
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
	result := make(map[string]*protocol.Consumer)
	b.activeConsumers.Range(func(key, value interface{}) bool {
		tag := key.(string)
		state := value.(*ConsumerState)
		result[tag] = state.consumer
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

func (b *StorageBroker) GetQueueConsumerTags(queueName string) []string {
	mu := b.getQueueConsumersMutex(queueName)
	mu.Lock()
	defer mu.Unlock()
	if val, ok := b.queueConsumers.Load(queueName); ok {
		consumers := val.([]*ConsumerState)
		tags := make([]string, 0, len(consumers))
		for _, state := range consumers {
			tags = append(tags, state.consumer.Tag)
		}
		return tags
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
		tags, _ := b.storage.GetUnackedTags(state.queueName, consumerTag)
		for _, tag := range tags {
			if tag > deliveryTag {
				continue
			}
			if _, won := b.settle(tag); !won {
				continue
			}
			b.finishAck(queueState, state.queueName, consumerTag, tag)
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
		b.ackDelivered(queueState, state.queueName, state.consumer.Tag, deliveryTag)
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

	// Settle funnels the LoadAndDelete guard (against duplicate reject or
	// concurrent UnregisterConsumer cleanup) and the single gate-credit release.
	if _, won := b.settle(deliveryTag); !won {
		return nil
	}
	b.finishNack(queueState, state.queueName, state.consumer.Tag, deliveryTag, requeue)

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
		// Enumerate-and-settle over the consumer's authoritative unacked set,
		// mirroring multi-ack (design §3): every tag <= deliveryTag is claimed
		// via settle() (its LoadAndDelete win is the one gate-credit release),
		// then requeued or discarded. No separately-computed release count.
		tags, _ := b.storage.GetUnackedTags(state.queueName, consumerTag)
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
				b.finishNack(queueState, state.queueName, consumerTag, tag, true)
			}
		} else {
			// requeue==false discard: batch the DLX republishes so we do not
			// serialize N synchronous WAL group-commit fsyncs on this frame
			// goroutine (a real latency cliff). One Policy() load is hoisted.
			b.discardNackedBatch(queueState, state.queueName, consumerTag, tags, deliveryTag)
		}
		queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))
	} else {
		// Nacknowledge single message. Settle funnels the LoadAndDelete guard
		// (against duplicate nack or concurrent UnregisterConsumer cleanup) and
		// the single gate-credit release.
		if _, won := b.settle(deliveryTag); !won {
			return nil
		}
		b.finishNack(queueState, state.queueName, state.consumer.Tag, deliveryTag, requeue)
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
// ack/nack/reject using an empty consumer tag ("") in the deliveryIndex.
// Returns the message, its delivery tag, the remaining message count, and any
// error.
func (b *StorageBroker) GetMessageForGet(queueName string, noAck bool) (*protocol.Message, uint64, uint32, error) {
	_, err := b.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return nil, 0, 0, fmt.Errorf("queue '%s' not found", queueName)
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
		// a loss the reaper already removed and accounted the tag, so we advance the
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
func (b *StorageBroker) PurgeQueue(name string) (int, error) {
	count, err := b.storage.PurgeQueue(name)
	if err != nil {
		return 0, err
	}

	if val, ok := b.queueStates.Load(name); ok {
		qs := val.(*QueueState)
		qs.tail.Store(qs.head.Load())
		qs.waiting.Store(0)
		qs.inflight.Store(0)
		qs.readyBytes.Store(0) // SQ-11: purge clears the ready-bytes counter
		qs.requeueMu.Lock()
		qs.requeueBuf = make([]requeueEntry, requeueInitialCap)
		qs.requeueHead = 0
		qs.requeueLen = 0
		qs.requeueCount.Store(0)
		qs.requeueMu.Unlock()
		qs.WakeAll()
	}

	return count, nil
}

// AcknowledgeGetDelivery handles acknowledgment of a basic.get delivery
// (identified by an empty consumer tag in the deliveryIndex).
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
func (b *StorageBroker) RequeueAllForConsumer(consumerTag string) error {
	val, ok := b.activeConsumers.Load(consumerTag)
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
	tags, _ := b.storage.GetUnackedTags(state.queueName, consumerTag)
	for _, tag := range tags {
		if _, won := b.settle(tag); !won {
			continue
		}
		b.finishNack(queueState, state.queueName, consumerTag, tag, true)
	}

	queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))

	return nil
}

// RequeueAllGetDeliveries requeues all in-flight basic.get deliveries
// (identified by empty consumer tag). Called during connection close
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
