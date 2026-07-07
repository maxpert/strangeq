package broker

// SQ-9 (W4) message & queue TTL.
//
// Effective TTL for a message is min(per-message x-expiration, queue
// x-message-ttl). At publish we stamp the enqueue instant into the durable
// protocol.Message.EnqueueUnixMilli field (W2 made it durable); the effective
// deadline is (EnqueueUnixMilli + effectiveTTL) recomputed at every check
// against the (test-overridable) wall clock. Storing the enqueue instant rather
// than a pre-computed deadline keeps the anchor honest, lets a fanned-out
// message share one anchor across queues with different queue-level TTLs, and
// makes recovery replay a plain wall-clock re-evaluation.
//
// Expired messages are dead-lettered (reason=expired) through the frozen W3 seam
// when a DLX is configured, else dropped — at the delivery-time head-check
// (deliverMessage + GetMessageForGet) and by a bounded per-queue background
// reaper (see queue_reaper.go). The single QueueState.Policy() load in
// deliverMessage plus the EnqueueUnixMilli==0 gate keep the no-TTL delivery path
// free of any wall-clock read (BenchmarkDeliver_NoTTL_ZeroCost).

import (
	"strconv"

	"github.com/maxpert/amqp-go/protocol"
)

// effectiveTTLMillis returns the effective time-to-live for msg on a queue with
// policy p, in milliseconds, and whether any TTL applies at all. It is
// min(per-message expiration, queue x-message-ttl) per RabbitMQ. A malformed or
// negative per-message expiration is treated as "no per-message TTL" (the exact
// behavior is locked against real RabbitMQ in W7); a queue TTL still applies.
func effectiveTTLMillis(msg *protocol.Message, p *QueuePolicy) (int64, bool) {
	var ttl int64
	has := false
	if p != nil && p.HasMessageTTL {
		ttl = p.MessageTTL.Milliseconds()
		has = true
	}
	if msg.Expiration != "" {
		if ms, err := strconv.ParseInt(msg.Expiration, 10, 64); err == nil && ms >= 0 {
			if !has || ms < ttl {
				ttl = ms
			}
			has = true
		}
		// Malformed/non-numeric/negative per-message expiration → ignored.
	}
	return ttl, has
}

// messageDeadlineMillis returns the absolute effective-TTL deadline (Unix
// milliseconds) for msg on a queue with policy p, and whether a deadline exists.
// A message with no stamped anchor (EnqueueUnixMilli==0) or no applicable TTL
// has no deadline. This is the single deadline computation shared by the
// head-check, the reaper, and recovery replay.
func messageDeadlineMillis(msg *protocol.Message, p *QueuePolicy) (int64, bool) {
	if msg.EnqueueUnixMilli == 0 {
		return 0, false
	}
	ttl, ok := effectiveTTLMillis(msg, p)
	if !ok {
		return 0, false
	}
	return msg.EnqueueUnixMilli + ttl, true
}

// messageExpired reports whether msg has passed its effective-TTL deadline as of
// nowMillis. The EnqueueUnixMilli==0 fast path means a message published without
// any TTL is never expired and — because callers gate the wall-clock read on the
// same condition — costs no clock read. ttl=0 yields deadline==enqueue, so a
// zero-TTL message is expired at the first check after publish (see the ttl=0
// divergence note in the W4 report).
func messageExpired(msg *protocol.Message, p *QueuePolicy, nowMillis int64) bool {
	deadline, ok := messageDeadlineMillis(msg, p)
	if !ok {
		return false
	}
	return nowMillis >= deadline
}
