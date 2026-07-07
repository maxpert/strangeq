package broker

import (
	"log"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// DeadLetterReason is the wire value recorded in the x-death "reason" field of
// a dead-lettered message. The three emitted reasons match RabbitMQ exactly and
// are part of the Wave 2 frozen contract; quorum-queue delivery_limit dead-
// lettering is out of scope for this wave and is deliberately NOT defined here
// (reserved, never emitted).
type DeadLetterReason string

const (
	// DeadLetterRejected: a consumer rejected/nacked the message with
	// requeue=false. Cycle detection is skipped for this reason (see deadLetter).
	DeadLetterRejected DeadLetterReason = "rejected"
	// DeadLetterExpired: the message's per-message or queue TTL elapsed. The
	// clone's Expiration is stripped and recorded as original-expiration.
	DeadLetterExpired DeadLetterReason = "expired"
	// DeadLetterMaxLen: the message was evicted to satisfy an x-max-length /
	// x-max-length-bytes limit (drop-head overflow).
	DeadLetterMaxLen DeadLetterReason = "maxlen"
)

// deadLetterFanoutLimit bounds how many dead-letter republishes run concurrently
// on the batched multi-nack/reject cold path (discardNackedBatch). It caps the
// goroutine + storage-I/O fan-out for a large unacked set (e.g. a prefetch-0
// consumer's window) while still letting enough durable writes overlap to
// coalesce into shared WAL group commits.
const deadLetterFanoutLimit = 64

// x-death / x-first-death-* header keys (RabbitMQ dead-letter extension).
const (
	xDeathHeader              = "x-death"
	xFirstDeathReasonHeader   = "x-first-death-reason"
	xFirstDeathQueueHeader    = "x-first-death-queue"
	xFirstDeathExchangeHeader = "x-first-death-exchange"
)

// x-death entry field keys.
const (
	xDeathCount              = "count"
	xDeathReason             = "reason"
	xDeathQueue              = "queue"
	xDeathTime               = "time"
	xDeathExchange           = "exchange"
	xDeathRoutingKeys        = "routing-keys"
	xDeathOriginalExpiration = "original-expiration"
)

// xDeathEvent describes a single dead-lettering occurrence to record in the
// message headers. It is grouped into a struct (rather than a long parameter
// list) so callers read at the call site and the annotation stays gofmt-clean.
type xDeathEvent struct {
	// queue is the source queue the message was dead-lettered FROM.
	queue string
	// reason is the wire reason (rejected/expired/maxlen).
	reason DeadLetterReason
	// exchange is the exchange the message carried when it entered the source
	// queue (recorded verbatim; RabbitMQ preserves it).
	exchange string
	// routingKeys are the routing key(s) the message carried; recorded as an
	// AMQP field array of longstr.
	routingKeys []string
	// deathTime is the time of this dead-lettering (recorded to second
	// resolution on the wire).
	deathTime time.Time
	// originalExpiration is the message's original expiration property. Recorded
	// as original-expiration ONLY when reason==expired and it is non-empty.
	originalExpiration string
}

// annotateXDeath records a dead-lettering event in headers, maintaining the
// x-death field array and the x-first-death-* headers exactly as RabbitMQ does.
// It mutates and returns headers (allocating a fresh map when headers is nil).
//
// The x-death header is an AMQP field array ([]interface{}) of field tables
// (map[string]interface{}) with these exact field names/types:
//
//	count               int64
//	reason              longstr (string)
//	queue               longstr (string)
//	time                timestamp (time.Time, second resolution)
//	exchange            longstr (string)
//	routing-keys        field array of longstr ([]interface{} of string)
//	original-expiration longstr (string) — present ONLY when reason==expired
//
// Aggregation is keyed by the (queue,reason) pair: a repeat of an existing pair
// increments that entry's count and moves it to the FRONT (head-insertion), so
// x-death[0] is always the most recent death; a new pair is prepended with
// count=1. x-first-death-{reason,queue,exchange} are set exactly once, gated on
// the absence of x-first-death-reason (so a message recovered with a prior
// first-death keeps it).
//
// CONTRACT: annotateXDeath MUST be called on a message clone's Headers, never on
// the live message pointer — see deadLetter.
func annotateXDeath(headers map[string]interface{}, ev xDeathEvent) map[string]interface{} {
	if headers == nil {
		headers = make(map[string]interface{})
	}

	entry := map[string]interface{}{
		xDeathCount:       int64(1),
		xDeathReason:      string(ev.reason),
		xDeathQueue:       ev.queue,
		xDeathTime:        ev.deathTime,
		xDeathExchange:    ev.exchange,
		xDeathRoutingKeys: routingKeysToField(ev.routingKeys),
	}
	// original-expiration is recorded only for expired deaths that carried an
	// expiration property (queue-TTL-only expiry has no original value).
	if ev.reason == DeadLetterExpired && ev.originalExpiration != "" {
		entry[xDeathOriginalExpiration] = ev.originalExpiration
	}

	headers[xDeathHeader] = mergeXDeath(headers[xDeathHeader], entry, ev.queue, string(ev.reason))

	// x-first-death-* is written once, on the first death this message ever
	// experiences (gated on absence of the reason key).
	if _, ok := headers[xFirstDeathReasonHeader]; !ok {
		headers[xFirstDeathReasonHeader] = string(ev.reason)
		headers[xFirstDeathQueueHeader] = ev.queue
		headers[xFirstDeathExchangeHeader] = ev.exchange
	}

	return headers
}

// mergeXDeath folds entry into the existing x-death array (existing may be nil
// or of an unexpected type, in which case it is treated as empty). If an entry
// for (queue,reason) already exists, its count is carried forward+1 into the
// new entry and the old slot is dropped; the (new or updated) entry is placed at
// the head so x-death[0] is the most recent death.
func mergeXDeath(existing interface{}, entry map[string]interface{}, queue, reason string) []interface{} {
	prev, _ := existing.([]interface{})

	out := make([]interface{}, 0, len(prev)+1)
	out = append(out, entry) // head-insertion: newest first
	for _, e := range prev {
		tbl, ok := e.(map[string]interface{})
		if !ok {
			// Preserve unexpected/foreign entries verbatim rather than dropping.
			out = append(out, e)
			continue
		}
		if xDeathMatches(tbl, queue, reason) {
			// Same (queue,reason): carry its count forward and drop the old slot.
			entry[xDeathCount] = asInt64(tbl[xDeathCount]) + 1
			continue
		}
		out = append(out, tbl)
	}
	return out
}

// xDeathMatches reports whether an existing x-death entry is for (queue,reason).
func xDeathMatches(entry map[string]interface{}, queue, reason string) bool {
	q, _ := entry[xDeathQueue].(string)
	r, _ := entry[xDeathReason].(string)
	return q == queue && r == reason
}

// routingKeysToField converts routing keys to an AMQP field array of longstr.
// A nil/empty input yields an empty (non-nil) field array, matching RabbitMQ
// which always emits the routing-keys field.
func routingKeysToField(keys []string) []interface{} {
	arr := make([]interface{}, len(keys))
	for i, k := range keys {
		arr[i] = k
	}
	return arr
}

// asInt64 coerces a decoded x-death count to int64. The count is written as
// int64, but a value recovered through a JSON durable-metadata path (W2) may
// arrive as float64 or a differently-sized integer; coercing defensively keeps
// aggregation correct across the storage boundary.
func asInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case uint64:
		return int64(n)
	case uint32:
		return int64(n)
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	default:
		return 0
	}
}

// deadLetter republishes msg to its source queue's configured dead-letter
// exchange. This is the single frozen DLX seam every Wave 2 consumer (SQ-9 TTL,
// SQ-10 reject/nack, SQ-11 max-length) routes through; the signature must NOT
// change once parallel work starts.
//
// FROZEN CONTRACT (spec §3, W1):
//   - policy is passed IN and NEVER re-loaded inside. The caller does the single
//     qs.Policy() load and the `if p != nil && p.HasDeadLetterExchange` guard
//     before fetching the message body, so paths where dead-lettering does not
//     apply (ack, requeue=true redelivery, publish) pay zero.
//   - Returns nil on drop (no DLX / absent or misconfigured DLX / cycle / zero
//     routed queues). Dropping is normal, not an error — the caller still
//     deletes the source message.
//   - Re-enters ONLY through routeMessage via a NON-BLOCKING republish. It must
//     NOT call queueState.WaitForCapacity: a DLX target at its high-water mark
//     (dead-letter queues typically have no consumers) must never block
//     basic.reject/nack on the connection's frame goroutine. Drop-on-full (or
//     the target's own SQ-11 overflow policy) is applied at this seam so all
//     three consumers inherit it.
//   - Cycle detection is REASON-CONDITIONAL: skipped entirely when
//     reason==rejected (an A<->B reject loop cycles forever and does NOT drop,
//     matching rabbit_dead_letter.erl); for expired/maxlen it matches on QUEUE
//     NAME ONLY, applied as post-routeMessage per-target filtering so a fanout
//     DLX with one cyclic + one non-cyclic bound queue delivers to the
//     non-cyclic and drops only the cyclic.
//   - Clones the message (deep-copying Headers, resetting DeliveryTag to 0) and
//     annotates x-death on the CLONE, never the live pointer. Strips Expiration
//     from the clone ONLY when reason==expired (recording original-expiration);
//     preserves it for rejected/maxlen.
//
// SQ-10 (W3) fills the body. It is best-effort: every path that cannot deliver
// (no/absent DLX, zero routed queues, all targets cyclic, a target at capacity,
// a storage error on republish) returns nil and lets the caller delete the
// source. The only durable state it mutates is the DLX target queues.
func (b *StorageBroker) deadLetter(policy *QueuePolicy, srcQueueName string, msg *protocol.Message, reason DeadLetterReason) error {
	// Defensive no-op: the caller guards with (p != nil && p.HasDeadLetterExchange),
	// but the seam must never deref a nil policy or route without a DLX.
	if policy == nil || !policy.HasDeadLetterExchange {
		return nil
	}

	// Resolve the DLX. An absent or otherwise unreadable DLX is a silent drop
	// (RabbitMQ drops the message; the caller still deletes the source).
	exchange, err := b.storage.GetExchange(policy.DeadLetterExchange)
	if err != nil || exchange == nil {
		return nil
	}

	// Routing key: the configured x-dead-letter-routing-key when set, else the
	// message's CURRENT routing key (RabbitMQ preserves the original RK).
	routingKey := msg.RoutingKey
	if policy.HasDeadLetterRoutingKey {
		routingKey = policy.DeadLetterRoutingKey
	}

	// Clone the message (deep-copy Headers, reset DeliveryTag) and annotate
	// x-death on the CLONE — never the live pointer. The original routing key
	// is captured for the x-death routing-keys field BEFORE any rewrite.
	clone := cloneForDeadLetter(msg)

	originalExpiration := ""
	if reason == DeadLetterExpired {
		// Strip Expiration only for expired deaths, recording the original so a
		// consumer can see the TTL that fired. Rejected/maxlen preserve it.
		originalExpiration = clone.Expiration
		clone.Expiration = ""
	}

	clone.Headers = annotateXDeath(clone.Headers, xDeathEvent{
		queue:              srcQueueName,
		reason:             reason,
		exchange:           msg.Exchange,
		routingKeys:        []string{msg.RoutingKey},
		deathTime:          time.Now(),
		originalExpiration: originalExpiration,
	})

	// Re-enter routing ONLY through routeMessage (never a second router). The
	// scratch is pooled and owned by this goroutine until releaseRouteScratch;
	// republish happens before that release, so iterating rs.queues is safe.
	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)
	if err := b.routeMessage(exchange, routingKey, clone, rs); err != nil {
		return nil
	}
	if len(rs.queues) == 0 {
		return nil
	}

	// Reason-conditional cycle detection (spec §3 item 2), applied as
	// post-routeMessage PER-TARGET filtering:
	//   - rejected: skip entirely. RabbitMQ's detect_cycles returns all targets
	//     for a reject, so an A<->B reject loop cycles forever and must NOT drop.
	//   - expired/maxlen: drop any target whose queue NAME already appears in the
	//     message's x-death (which now includes srcQueueName). A fanout DLX with
	//     one cyclic and one non-cyclic bound queue delivers to the non-cyclic and
	//     drops only the cyclic one.
	var cyclic map[string]struct{}
	if reason != DeadLetterRejected {
		cyclic = xDeathQueueSet(clone.Headers)
	}

	// RabbitMQ rewrites the dead-lettered message's exchange to the DLX and its
	// routing key to the resolved key (dead-letter-routing-key when set, else the
	// original). A DLQ consumer must see the rewritten values. This runs AFTER
	// annotateXDeath (which recorded the ORIGINAL exchange + routing key from msg,
	// not the clone) and AFTER routeMessage (which routed on the resolved key
	// argument, not clone.RoutingKey), so on a chained A→B→C dead-letter each
	// x-death entry records that hop's own exchange (the intermediate DLX), not
	// the original exchange forever.
	clone.Exchange = policy.DeadLetterExchange
	clone.RoutingKey = routingKey

	b.republishToTargets(srcQueueName, rs.queues, clone, cyclic)
	return nil
}

// republishToTargets enqueues the dead-letter clone into each (non-cyclic)
// target queue using a NON-BLOCKING enqueue path — the frozen seam requirement
// so all three Wave 2 consumers (SQ-9 TTL, SQ-10 reject/nack, SQ-11 max-length)
// inherit it. Unlike the publish hot path (PublishMessage), it MUST NOT call
// QueueState.WaitForCapacity: a consumer-less dead-letter queue sitting at its
// high-water mark would otherwise stall basic.reject/nack on the connection's
// frame goroutine.
//
// Baseline overflow behavior is drop-on-full (documented divergence to lock
// against RabbitMQ in W7, which applies the TARGET queue's own x-overflow).
// SQ-11 (W5) hooks in at the AtHighWaterMark check below to consult the target's
// x-overflow policy (drop-head vs reject) instead of the unconditional drop.
//
// The per-target copy mirrors the PublishMessage fanout invariant: the first
// successfully-enqueued target reuses the clone directly; subsequent targets
// deep-copy Headers so queue copies never share a map pointer.
func (b *StorageBroker) republishToTargets(srcQueueName string, targets []string, clone *protocol.Message, cyclic map[string]struct{}) {
	first := true
	for _, target := range targets {
		if cyclic != nil {
			if _, isCyclic := cyclic[target]; isCyclic {
				continue // per-target cycle drop (expired/maxlen only)
			}
		}

		qs := b.getOrCreateQueueState(target)

		// Non-blocking overflow gate (SQ-11 hook). Baseline: drop-on-full.
		if qs.AtHighWaterMark() {
			continue
		}

		msgID := b.globalDeliveryTag.Add(1)

		var storeMsg *protocol.Message
		if first {
			clone.DeliveryTag = msgID
			storeMsg = clone
		} else {
			c := *clone
			c.DeliveryTag = msgID
			if clone.Headers != nil {
				h := make(map[string]interface{}, len(clone.Headers))
				for k, v := range clone.Headers {
					h[k] = v
				}
				c.Headers = h
			}
			storeMsg = &c
		}

		// At-least-once: StoreMessage (durable persistence for a persistent
		// message) happens here, BEFORE the caller removes the source. A store
		// failure is a genuine message LOSS (the caller still deletes the source),
		// so it is made observable — logged and counted via the optional metrics
		// hook — rather than dropped silently. Behavior is unchanged: drop and
		// continue.
		if err := b.storage.StoreMessage(target, storeMsg); err != nil {
			b.recordDeadLetterDrop(srcQueueName, target, err)
			continue
		}
		qs.Publish(msgID)
		// SQ-11: keep the DLX target's ready-bytes counter consistent when the
		// target itself enforces x-max-length-bytes.
		addReadyBytesOnEnqueue(qs, qs.Policy(), len(storeMsg.Body))
		first = false
	}
}

// deadLetterDropRecorder is an OPTIONAL metrics hook. A MetricsCollector may
// additionally implement it to count dead-letter republishes dropped because the
// target store failed (a genuine message loss). It is discovered via a type
// assertion, so existing collectors need not implement it.
type deadLetterDropRecorder interface {
	RecordDeadLetterDropped()
}

// recordDeadLetterDrop surfaces a store-failure drop: a warning log (this cold
// path is the only logging in the otherwise silent broker package, and a lost
// message warrants it) plus the optional metrics counter when the configured
// collector supports it.
func (b *StorageBroker) recordDeadLetterDrop(srcQueueName, target string, cause error) {
	log.Printf("WARN: dead-letter republish dropped a message from queue %q to target %q: store failed: %v",
		srcQueueName, target, cause)
	if rec, ok := b.metricsCollector.(deadLetterDropRecorder); ok {
		rec.RecordDeadLetterDropped()
	}
}

// cloneForDeadLetter produces a dead-letter copy of msg: a shallow struct copy
// with a deep-copied Headers map and DeliveryTag reset to 0. Annotation and any
// Expiration strip happen on this copy so the caller's live message is never
// mutated. Allocation here is acceptable — this is the cold dead-letter path.
func cloneForDeadLetter(msg *protocol.Message) *protocol.Message {
	clone := *msg
	clone.DeliveryTag = 0
	if msg.Headers != nil {
		h := make(map[string]interface{}, len(msg.Headers))
		for k, v := range msg.Headers {
			h[k] = v
		}
		clone.Headers = h
	}
	return &clone
}

// xDeathQueueSet returns the set of queue names recorded in the message's
// x-death header. It is the queue-name-only cycle key for expired/maxlen deaths
// (spec §3 item 2): a target queue is cyclic iff its name appears here. Because
// annotateXDeath runs before this, the set includes the current source queue, so
// a DLX routing straight back to the source is correctly detected.
func xDeathQueueSet(headers map[string]interface{}) map[string]struct{} {
	set := make(map[string]struct{})
	arr, ok := headers[xDeathHeader].([]interface{})
	if !ok {
		return set
	}
	for _, e := range arr {
		tbl, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		if q, ok := tbl[xDeathQueue].(string); ok {
			set[q] = struct{}{}
		}
	}
	return set
}
