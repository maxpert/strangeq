package broker

import (
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
// W1 ships this as a documented stub returning nil; SQ-10 (W3) fills the body.
func (b *StorageBroker) deadLetter(policy *QueuePolicy, srcQueueName string, msg *protocol.Message, reason DeadLetterReason) error {
	// STUB (SQ-10/W3): the body — clone, annotate x-death, reason-conditional
	// cycle filter, non-blocking republish through routeMessage — is filled by
	// W3. Returning nil is the correct drop behavior until then.
	_ = policy
	_ = srcQueueName
	_ = msg
	_ = reason
	return nil
}
