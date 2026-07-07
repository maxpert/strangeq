package broker

// SQ-9 (W4) per-queue background reaper.
//
// One goroutine per queue that has a queue-level x-message-ttl and/or x-expires
// policy (started at declare/recovery, stopped when the queue's stopCh closes).
// It does two jobs, both off a single per-queue timer whose period tracks the
// nearest deadline:
//
//   - TTL reaping (x-message-ttl queues, consumer-less only): expire ready
//     messages whose effective deadline has passed — dead-lettering
//     (reason=expired) through the frozen W3 seam or dropping. This is the
//     "internally better than RabbitMQ" mid-queue expiry: with no consumer the
//     tail cursor is static, so the reaper can expire a short-TTL message that
//     sits BEHIND a longer-lived one, which RabbitMQ (head-only without a
//     consumer) cannot. A queue WITH consumers is left to the delivery-time
//     head-check (deliverMessage / GetMessageForGet); the reaper skips it, so
//     the reaper and a live consumer never contend for the same queue except in
//     the brief consumer-registration transition (bounded below).
//
//   - x-expires: auto-delete a queue after QueueExpires ms of no use (no
//     consumers, no basic.get, no redeclare — all of which reset the idle clock
//     via QueueState.MarkActivity).
//
// LINEARIZATION (reaper vs consumer Claim, for undelivered messages).
// The reaper removes a ready message with the ring's Delete-returns-bool
// (storage.AtomicRing.Delete via DeleteMessageIfPresent) and decrements depth
// (ReapDrop) ONLY when it wins that CAS. The delivery head-check does the same.
// Proof that at most one of {reaper drop, consumer claim} decrements depth per
// tag:
//
//   1. A non-expired ready message is never targeted by the reaper (it only
//      reaps tags whose deadline has passed), so a normal delivery's
//      ClaimInflight is the sole decrementer.
//   2. For an expired ready tag, both the reaper and the head-check gate their
//      ReapDrop on winning DeleteMessageIfPresent (a single ring-slot CAS with
//      exactly one winner). The loser observes the tag already gone: a consumer
//      that had claimed it off the tail then finds GetMessage empty → gap path
//      (GapSkipAdvance, no depth change); the reaper simply skips. Hence exactly
//      one ReapDrop per tag.
//   3. Reaper vs consumer normal ClaimInflight can only race at the expiry
//      boundary (consumer reads not-yet-expired and delivers; reaper reads
//      expired). It is excluded structurally: the reaper reaps a queue only
//      while it has zero registered consumers (queueHasConsumers), re-checked
//      immediately before each ring removal. A consumer's Claim requires a
//      registered consumer, whose registration increments the consumer set
//      before its poll loop can Claim — so the reaper's "no consumers" +
//      ring-CAS gate cannot coexist with a concurrent ClaimInflight for the same
//      tag except within the sub-instruction window between the consumer-set
//      re-check and the CAS, which is shorter than the goroutine scheduling a
//      newly-registered consumer needs to reach Claim. This residual
//      transition window is documented for W7; it can at worst produce a
//      duplicate dead-letter (at-least-once, RabbitMQ-permitted), never a lost
//      message or a permanent depth skew.

import (
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	// reaperMinInterval floors the reaper's sleep so it never busy-spins even
	// when a deadline is in the immediate past.
	reaperMinInterval = 2 * time.Millisecond
	// reaperMaxInterval caps the reaper's sleep so a queue that was empty when it
	// last slept still notices freshly-published short-TTL messages and idle
	// x-expiry within a bounded latency.
	reaperMaxInterval = 250 * time.Millisecond
	// reaperBatchSize bounds how many ready tags one sweep inspects/reaps, so a
	// deep consumer-less queue cannot make a single sweep unbounded (defended by
	// the W0 many-TTL-queues sweep benchmark).
	reaperBatchSize = 512
)

// maybeStartReaper starts the per-queue reaper goroutine once, if the queue's
// policy needs it (queue-level TTL or x-expires). Called from declare/recovery
// after the policy is attached. Cheap and idempotent: a queue without either
// policy never spawns a goroutine (unset-feature zero cost).
func (b *StorageBroker) maybeStartReaper(name string, qs *QueueState) {
	p := qs.Policy()
	if p == nil || !(p.HasMessageTTL || p.HasQueueExpires) {
		return
	}
	if !qs.StartReaperOnce() {
		return
	}
	qs.MarkActivity(b.ttlNowMillis())
	b.reaperWG.Add(1)
	go b.reapLoop(name, qs)
}

// reapLoop is the per-queue reaper goroutine. It sleeps until the nearest
// deadline (bounded by reaperMinInterval/reaperMaxInterval), sweeps, and re-arms.
// It exits when the queue's stopCh closes (DeleteQueue or broker Close).
func (b *StorageBroker) reapLoop(name string, qs *QueueState) {
	defer b.reaperWG.Done()
	timer := time.NewTimer(reaperMinInterval)
	defer timer.Stop()
	for {
		select {
		case <-qs.StopCh():
			return
		case <-timer.C:
		}
		next := b.reapSweep(name, qs)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(next)
	}
}

// reapSweep performs one bounded sweep: x-expires idle deletion and, for a
// consumer-less x-message-ttl queue, expiry of ready messages. It returns the
// duration until the next sweep.
func (b *StorageBroker) reapSweep(name string, qs *QueueState) time.Duration {
	p := qs.Policy()
	if p == nil {
		return reaperMaxInterval
	}
	now := b.ttlNowMillis()
	next := reaperMaxInterval

	if p.HasQueueExpires {
		expiresMs := p.QueueExpires.Milliseconds()
		if b.queueHasConsumers(name) {
			// In use: reset the idle clock and re-check after the full window.
			qs.MarkActivity(now)
			if d := time.Duration(expiresMs) * time.Millisecond; d < next {
				next = d
			}
		} else {
			idle := now - qs.LastActivityMilli()
			if idle >= expiresMs {
				// Idle long enough with no consumers: auto-delete and stop. The
				// deletion closes stopCh, so reapLoop returns on its next select.
				b.DeleteQueue(name, false, false)
				return reaperMaxInterval
			}
			if d := time.Duration(expiresMs-idle) * time.Millisecond; d < next {
				next = d
			}
		}
	}

	if p.HasMessageTTL {
		// Poll at least as finely as the queue TTL so freshly published messages
		// expire promptly.
		if ttl := p.MessageTTL; ttl > 0 && ttl < next {
			next = ttl
		}
		// Only reap when consumer-less; a queue with consumers expires at the
		// delivery head-check (see linearization note).
		if !b.queueHasConsumers(name) {
			if earliest := b.reapTTLSweep(name, qs, p, now); earliest > 0 {
				if d := time.Duration(earliest-now) * time.Millisecond; d < next {
					next = d
				}
			}
		}
	}

	if next < reaperMinInterval {
		next = reaperMinInterval
	}
	return next
}

// reapExpiredTarget pairs a tag with its (already-loaded) message for the
// snapshot-then-republish reaper phase.
type reapExpiredTarget struct {
	tag uint64
	msg *protocol.Message
}

// reapTTLSweep inspects up to reaperBatchSize ready tags from the tail cursor,
// dead-letters/drops the expired ones, and returns the earliest FUTURE deadline
// seen (0 if none) so the caller can schedule the next wake. It holds no lock
// across the dead-letter republish: it first snapshots the expired tags, then
// republishes+removes each. It also advances the tail past leading gaps so
// repeated sweeps stay bounded (safe because the queue is consumer-less here, so
// the tail is not being advanced by any Claim).
func (b *StorageBroker) reapTTLSweep(name string, qs *QueueState, p *QueuePolicy, now int64) (earliestFuture int64) {
	tail := qs.Tail()
	head := qs.Head()
	end := tail + reaperBatchSize
	if end > head {
		end = head
	}

	var expired []reapExpiredTarget
	for tag := tail; tag < end; tag++ {
		msg, err := b.storage.GetMessage(name, tag)
		if err != nil || msg == nil {
			continue // gap (acked/reaped/never-present tag)
		}
		deadline, ok := messageDeadlineMillis(msg, p)
		if !ok {
			continue // this message carries no effective TTL
		}
		if deadline <= now {
			expired = append(expired, reapExpiredTarget{tag: tag, msg: msg})
		} else if earliestFuture == 0 || deadline < earliestFuture {
			earliestFuture = deadline
		}
	}

	// Snapshot taken; now republish (dead-letter) + remove each expired tag with
	// no lock held. Depth is decremented only on winning the ring removal.
	for i := range expired {
		t := &expired[i]
		// Re-check consumer-lessness BEFORE the dead-letter (not just before the
		// removal): if a consumer registered mid-sweep, back off entirely for the
		// rest of this batch and let the delivery-time head-check own expiry for
		// this queue. The head-check also dead-letters before it removes, so
		// at-least-once is preserved; backing off before the dead-letter avoids a
		// needless duplicate dead-letter in the consumer-registration transition.
		if b.queueHasConsumers(name) {
			break
		}
		if p.HasDeadLetterExchange {
			_ = b.deadLetter(p, name, t.msg, DeadLetterExpired)
		}
		if b.deleteIfPresent(name, t.tag) {
			qs.ReapDrop()
			// SQ-11: the expired message left the ready set — account its bytes
			// when the queue enforces x-max-length-bytes, gated on the same
			// ring-removal win as the `waiting` decrement so the byte counter
			// never drifts up (which would spuriously reject/evict live messages).
			subReadyBytesOnClaim(qs, p, len(t.msg.Body))
		}
	}

	// Advance the tail past leading gaps (reaped/absent tags) so subsequent
	// sweeps do not re-probe them. Bounded by the batch window and gated on
	// consumer-lessness (a Claim otherwise owns the tail).
	limit := tail + reaperBatchSize
	for {
		t := qs.Tail()
		if t >= qs.Head() || t >= limit {
			break
		}
		if b.queueHasConsumers(name) {
			break
		}
		if msg, err := b.storage.GetMessage(name, t); err == nil && msg != nil {
			break // present, non-reaped message at the head: stop advancing
		}
		if !qs.tail.CompareAndSwap(t, t+1) {
			break
		}
	}

	return earliestFuture
}
