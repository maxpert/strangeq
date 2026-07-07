package broker

import "errors"

// SQ-11 — queue length / byte limits (x-max-length, x-max-length-bytes) with
// x-overflow = drop-head (default) or reject-publish.
//
// Enforcement is on the READY set only (WaitingCount / ReadyBytes), matching
// RabbitMQ, which counts ready messages and ignores delivered-unacknowledged
// ones. The ready-bytes counter (QueueState.readyBytes, a W1 atomic) is
// maintained ONLY for queues whose policy sets x-max-length-bytes, so a queue
// without a byte limit pays nothing beyond a single policy load + a not-taken
// branch. The counter mirrors `waiting` exactly: it is added/subtracted at the
// same transitions (enqueue, claim, requeue, evict), guaranteeing the byte check
// and the count check see a consistent ready set.
//
// Evicted (drop-head) messages dead-letter through the frozen W3 seam with
// reason=maxlen. A reject-publish refusal does NOT dead-letter the incoming
// message (RabbitMQ parity).

// ErrMaxLengthExceeded is returned by PublishMessage when a target queue's
// x-overflow=reject-publish policy refuses the incoming message because the
// queue is AT OR OVER its x-max-length / x-max-length-bytes limit. The server
// layer turns it into a publisher-confirm basic.nack (via
// protocol.Channel.SettleConfirmNack), a basic.return on a mandatory publish, or
// a silent drop otherwise.
var ErrMaxLengthExceeded = errors.New("max-length exceeded (reject-publish)")

// maxDropHeadEvictionsPerPublish bounds the drop-head trim loop so a pathological
// policy can never spin forever. Real limits converge in one (steady state) or a
// few evictions per publish.
const maxDropHeadEvictionsPerPublish = 1 << 20

// maxLenRejectsPublish reports whether a reject-publish queue is AT OR OVER its
// limit BEFORE adding the incoming message (spec W5 criterion 1): reject iff
// WaitingCount() >= MaxLength OR ReadyBytes() >= MaxLengthBytes. This is NOT a
// would-exceed test, so a single body larger than the byte limit is accepted
// into an empty queue. The caller has already loaded p (non-nil) and confirmed
// Overflow == OverflowRejectPublish.
func maxLenRejectsPublish(qs *QueueState, p *QueuePolicy) bool {
	if p.HasMaxLength && qs.WaitingCount() >= p.MaxLength {
		return true
	}
	if p.HasMaxLengthBytes && qs.ReadyBytes() >= p.MaxLengthBytes {
		return true
	}
	return false
}

// dropHeadTrim evicts the globally-oldest ready messages (requeue ring first,
// then tail cursor) until the queue is back within its x-max-length /
// x-max-length-bytes limits, dead-lettering each (reason=maxlen). It runs AFTER
// the triggering publish has been enqueued, so the just-published newest message
// sits at the tail end and is evicted last; an older requeued message is always
// evicted first. For x-max-length-bytes it keeps at least one ready message so a
// single body larger than the byte limit is retained (RabbitMQ parity). policy
// is passed in (already loaded); it enforces only the limits that are set.
func (b *StorageBroker) dropHeadTrim(qs *QueueState, queueName string, policy *QueuePolicy) {
	for i := 0; i < maxDropHeadEvictionsPerPublish; i++ {
		overCount := policy.HasMaxLength && qs.WaitingCount() > policy.MaxLength
		overBytes := policy.HasMaxLengthBytes && qs.ReadyBytes() > policy.MaxLengthBytes && qs.WaitingCount() > 1
		if !overCount && !overBytes {
			return
		}
		if !b.dropHeadEvictOne(qs, queueName, policy) {
			return
		}
	}
}

// dropHeadEvictOne evicts a single globally-oldest REAL ready message and
// returns true, or false when the queue has no ready message left to evict. Gap
// tags (never stored / already acked — a fresh queue's cursor starts below the
// first real tag) are skipped WITHOUT counting an eviction and WITHOUT touching
// `waiting`, exactly mirroring the deliver/basic.get gap path, so a gap can never
// masquerade as a dropped message. A real message is dead-lettered (reason=maxlen)
// when a DLX is configured, dropped from storage, byte-accounted, and settled via
// ClaimInflight()+AckAdvance().
func (b *StorageBroker) dropHeadEvictOne(qs *QueueState, queueName string, policy *QueuePolicy) bool {
	for {
		tag, ok := qs.PopOldestReady()
		if !ok {
			return false
		}
		// Read the body BEFORE deleting, for dead-lettering + byte accounting.
		msg, err := b.storage.GetMessage(queueName, tag)
		if err != nil || msg == nil {
			// Gap tag: advance the depth frontier and keep looking — it was
			// never a ready message, so it must not count toward the limit.
			qs.GapSkipAdvance(tag)
			continue
		}
		bodyLen := int64(len(msg.Body))
		if policy.HasDeadLetterExchange {
			// maxlen-evicted messages dead-letter through the frozen W3 seam.
			// deadLetter never touches the live pointer and returns nil on drop.
			_ = b.deadLetter(policy, queueName, msg, DeadLetterMaxLen)
		}
		// Gate the depth accounting on winning the ring removal so it reconciles
		// with the SQ-9 reaper (see the queue_reaper.go linearization note): on a
		// consumer-less queue with BOTH x-message-ttl and drop-head, the reaper and
		// this eviction can select the same tag. Exactly one wins deleteIfPresent
		// and decrements depth; the loser advances the frontier (GapSkipAdvance) and
		// tries the next oldest — never a double `waiting` decrement that
		// under-counts the ready set and lets depth exceed x-max-length. For a
		// no-TTL drop-head queue (the common case) deleteIfPresent always wins, so
		// this path stays allocation/latency-equivalent to an unconditional delete.
		if !b.deleteIfPresent(queueName, tag) {
			qs.GapSkipAdvance(tag)
			continue
		}
		b.storage.DeletePendingAck(queueName, tag)
		// Ready -> gone for a real message: ClaimInflight (waiting-1, inflight+1)
		// then AckAdvance (inflight-1) reuses the delivery bookkeeping and keeps
		// minAckCursor safe against concurrent real deliveries.
		qs.ClaimInflight(tag)
		if policy.HasMaxLengthBytes && bodyLen > 0 {
			qs.SubReadyBytes(bodyLen)
		}
		qs.AckAdvance(tag)
		return true
	}
}

// addReadyBytesOnEnqueue adds body bytes to the ready-bytes counter when the
// queue enforces x-max-length-bytes. Called at each enqueue site (publish, tx
// visibility, dead-letter republish) right after QueueState.Publish. One atomic
// load + a not-taken branch when the queue has no byte limit.
func addReadyBytesOnEnqueue(qs *QueueState, p *QueuePolicy, bodyLen int) {
	if p != nil && p.HasMaxLengthBytes {
		qs.AddReadyBytes(int64(bodyLen))
	}
}

// subReadyBytesOnClaim subtracts body bytes from the ready-bytes counter when
// the queue enforces x-max-length-bytes. Called at EVERY ready-set removal site —
// consumer delivery, manual-ack and no-ack basic.get, and the SQ-9 TTL-expiry
// drops (delivery/get head-check and reaper) — alongside the `waiting` decrement,
// so the byte counter tracks exactly the ready set and never drifts. p may be nil
// (per-message TTL with no queue policy): the guard handles it.
func subReadyBytesOnClaim(qs *QueueState, p *QueuePolicy, bodyLen int) {
	if p != nil && p.HasMaxLengthBytes {
		qs.SubReadyBytes(int64(bodyLen))
	}
}

// restoreReadyBytesOnRequeue adds a requeued message's bytes back to the
// ready-bytes counter when the queue enforces x-max-length-bytes. Cold path
// (nack/reject requeue, consumer-cancel drain, basic.get requeue): the message
// is still in storage, so its body is read directly — no per-delivery persisted
// size, which keeps the delivery hot path free of any storage-codec change.
func (b *StorageBroker) restoreReadyBytesOnRequeue(qs *QueueState, queueName string, tag uint64) {
	p := qs.Policy()
	if p == nil || !p.HasMaxLengthBytes {
		return
	}
	if msg, err := b.storage.GetMessage(queueName, tag); err == nil && msg != nil {
		qs.AddReadyBytes(int64(len(msg.Body)))
	}
}
