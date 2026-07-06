package broker

import (
	"errors"
	"fmt"
	"reflect"
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
}

// NewStorageBroker creates a new storage-backed broker instance
// Phase 6G: Now accepts EngineConfig for tunable parameters
func NewStorageBroker(storage interfaces.Storage, engineConfig interfaces.EngineConfig) *StorageBroker {
	broker := &StorageBroker{
		storage:      storage,
		engineConfig: engineConfig,
	}

	broker.initializeDefaultExchanges()

	return broker
}

func (b *StorageBroker) SetMetricsCollector(mc MetricsCollector) {
	b.metricsCollector = mc
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
		queueState.Requeue(deliveryTag)
	} else {
		b.storage.DeleteMessage(queueName, deliveryTag)
		queueState.AckAdvance(deliveryTag)
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
	b.getOrCreateQueueState(name).SetPolicy(policy)

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

	if len(targetQueues) == 0 {
		if message.Mandatory {
			return ErrNoRoute
		}
		return nil
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

	targetQueues, err := b.findTargetQueues(exchange, routingKey, message)
	if err != nil {
		return nil, err
	}

	if len(targetQueues) == 0 {
		if message.Mandatory {
			return nil, ErrNoRoute
		}
		return nil, nil
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

		// Stage the store into the transactional view (buffered until commit).
		if err := txnStore.StoreMessage(queueName, storeMsg); err != nil {
			return nil, fmt.Errorf("failed to stage message to queue '%s': %w", queueName, err)
		}

		// Defer visibility until after the atomic commit succeeds.
		qs := queueState
		id := msgID
		deferred = append(deferred, func() { qs.Publish(id) })
	}

	return deferred, nil
}

// findTargetQueues determines which queues should receive the message.
// It follows both queue bindings and exchange-to-exchange bindings recursively,
// using a visited set for cycle protection and a seen set for queue deduplication.
func (b *StorageBroker) findTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message) ([]string, error) {
	seen := make(map[string]struct{})
	visited := make(map[string]struct{})
	var queues []string
	if err := b.collectTargetQueues(exchange, routingKey, message, seen, visited, &queues); err != nil {
		return nil, err
	}
	return queues, nil
}

func (b *StorageBroker) collectTargetQueues(exchange *protocol.Exchange, routingKey string, message *protocol.Message, seen map[string]struct{}, visited map[string]struct{}, queues *[]string) error {
	if _, ok := visited[exchange.Name]; ok {
		return nil
	}
	visited[exchange.Name] = struct{}{}

	if exchange.Name == "" {
		if _, ok := seen[routingKey]; ok {
			return nil
		}
		if _, ok := b.activeQueues.Load(routingKey); ok {
			*queues = append(*queues, routingKey)
			seen[routingKey] = struct{}{}
			return nil
		}
		_, err := b.storage.GetQueue(routingKey)
		if err == nil {
			*queues = append(*queues, routingKey)
			seen[routingKey] = struct{}{}
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
				if _, ok := seen[binding.QueueName]; !ok {
					*queues = append(*queues, binding.QueueName)
					seen[binding.QueueName] = struct{}{}
				}
			}
		}
	case "fanout":
		for _, binding := range bindings {
			if _, ok := seen[binding.QueueName]; !ok {
				*queues = append(*queues, binding.QueueName)
				seen[binding.QueueName] = struct{}{}
			}
		}
	case "topic":
		for _, binding := range bindings {
			if b.matchTopicPattern(binding.RoutingKey, routingKey) {
				if _, ok := seen[binding.QueueName]; !ok {
					*queues = append(*queues, binding.QueueName)
					seen[binding.QueueName] = struct{}{}
				}
			}
		}
	case "headers":
		for _, binding := range bindings {
			if b.matchHeaders(binding.Arguments, message.Headers) {
				if _, ok := seen[binding.QueueName]; !ok {
					*queues = append(*queues, binding.QueueName)
					seen[binding.QueueName] = struct{}{}
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
		if err := b.collectTargetQueues(destExchange, routingKey, message, seen, visited, queues); err != nil {
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
		for _, tag := range tags {
			if tag > deliveryTag {
				continue
			}
			if _, won := b.settle(tag); !won {
				continue
			}
			b.finishNack(queueState, state.queueName, consumerTag, tag, requeue)
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
			message.Redelivered = r
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
			message = msg
			break
		}
	}

	if message == nil {
		return nil, 0, 0, nil
	}

	countBefore, _ := b.storage.GetQueueMessageCount(queueName)
	remaining := uint32(countBefore)
	if remaining > 0 {
		remaining--
	}

	if !noAck {
		queueState.ClaimInflight(tag)
		b.deliveryIndex.Store(tag, "")
		b.getDeliveryQueues.Store(tag, queueName)
		b.storage.StorePendingAck(&protocol.PendingAck{
			QueueName:   queueName,
			DeliveryTag: tag,
			ConsumerTag: "",
		})
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
		queueState.Requeue(deliveryTag)
	} else {
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
		queueState.Requeue(deliveryTag)

		return true
	})
}
