package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
)

// ErrLegacyDataDirectory is returned by PerformRecovery when a queue has
// recoverable durable messages but no composite-tag ordinal
// (broker/tag_packing.go) was ever persisted for it — protocol.Queue.Ordinal
// reads 0 as this restart's recovery begins. That means the data directory
// predates per-queue delivery-tag sequencing: its recovered delivery tags are
// raw, unpacked legacy values whose high bits are not a valid queue ordinal
// at all. There is no safe in-place migration. server/builder.go treats this
// as fatal (the server refuses to start) rather than silently discarding the
// directory's data the way most other recovery errors are handled — see the
// comment there.
var ErrLegacyDataDirectory = errors.New("data directory predates per-queue delivery-tag sequencing; drain and recreate the affected queue(s), or the data directory, before restarting on this build")

// ErrOrdinalMismatch is returned by PerformRecovery when a queue's recovered
// delivery tags carry a composite-tag ordinal (broker/tag_packing.go)
// different from the ordinal assigned to that queue. Since two queues must
// never mint the same delivery tag (the shared WAL's offsetIndex/ackBitmap
// and the broker's deliveryIndex are keyed by a bare uint64 with no queue
// discriminator), a mismatch here means recovery is about to hand this
// queue's consumers tags that collide with another queue's tag space —
// corruption, or a legacy directory ErrLegacyDataDirectory's own check
// didn't already catch. Fatal for the same reason as ErrLegacyDataDirectory.
var ErrOrdinalMismatch = errors.New("recovered delivery tag ordinal does not match the queue's assigned ordinal")

// RecoveryManager handles server startup recovery of durable entities
type RecoveryManager struct {
	storage interfaces.Storage
	broker  UnifiedBroker
	logger  *zap.Logger

	// queueOrdinalsAtDeclare captures each durable queue's persisted
	// composite-tag ordinal (broker/tag_packing.go) as read from metadata
	// BEFORE recoverDurableQueues (re)declares it. DeclareQueue ->
	// resolveQueueOrdinal (broker/storage_broker.go) silently allocates and
	// persists a fresh ordinal for any queue whose stored record still has
	// Ordinal==0, so by the time recoverPersistentMessages runs the
	// persisted value is never 0 anymore for a queue that went through that
	// path. This map is recovery's only remaining visibility into the
	// PRE-recovery value, which is exactly what legacy-directory detection
	// needs (see ErrLegacyDataDirectory).
	queueOrdinalsAtDeclare map[string]uint64

	// faults accumulates every classified thing that went wrong, in the order
	// recovery hit it. PerformRecovery returns them joined; server/builder.go
	// decides the boot from their worst outcome. They are accumulated rather
	// than returned one at a time so a degraded boot can NAME EVERY discarded
	// artifact instead of the first one.
	faults []*interfaces.RecoveryFault
}

// addFault records a classified fault.
func (r *RecoveryManager) addFault(f *interfaces.RecoveryFault) {
	r.faults = append(r.faults, f)
}

// absorb records whatever classification an error carries and reports the
// resulting outcome.
//
// AN ERROR CARRYING NO CLASSIFICATION IS RECORDED AS FATAL. That is the whole
// inversion: before Step 3 an unclassified recovery error fell through to
// "boot with an empty broker and start confirming durable publishes", so the
// default for the error nobody had thought about yet was the most dangerous
// one available. Now the default is the safest one, and it is a property of
// the type rather than of anybody remembering to extend an allow-list.
func (r *RecoveryManager) absorb(stage string, err error) interfaces.RecoveryOutcome {
	if err == nil {
		return interfaces.RecoveryBenign
	}
	r.faults = append(r.faults, interfaces.ExplainedFaults(stage, err)...)
	return interfaces.OutcomeOf(err)
}

// result joins the accumulated faults, or returns nil when there are none.
func (r *RecoveryManager) result() error {
	if len(r.faults) == 0 {
		return nil
	}
	errs := make([]error, 0, len(r.faults))
	for _, f := range r.faults {
		errs = append(errs, f)
	}
	return errors.Join(errs...)
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(storage interfaces.Storage, broker UnifiedBroker, logger *zap.Logger) *RecoveryManager {
	return &RecoveryManager{
		storage:                storage,
		broker:                 broker,
		logger:                 logger,
		queueOrdinalsAtDeclare: make(map[string]uint64),
	}
}

// PerformRecovery performs complete server startup recovery.
//
// It returns a CLASSIFIED error (interfaces.RecoveryOutcome) rather than a bare
// one. A non-nil return does NOT mean "recovery failed" — it means "recovery
// has something to report", and the worst outcome among the reported faults is
// what server/builder.go gates the boot on. A fatal fault stops recovery where
// it happened: nothing after it is loaded, because nothing after it can be
// trusted.
func (r *RecoveryManager) PerformRecovery() (*protocol.RecoveryStats, error) {
	startTime := time.Now()

	r.logger.Info("Starting server recovery process...")

	// Step 1: Validate storage integrity and repair if needed
	stats, err := r.validateAndRepairStorage()
	if err != nil {
		r.absorb("storage-validation", err)
		return stats, r.result()
	}

	// Durable entity metadata is read ONCE. It used to be read three times —
	// once each by the exchange, queue and binding steps — from one function
	// that always loads both directories, so a QUEUE directory failure could
	// surface as "exchange recovery failed" and vice versa. One read, one
	// classification, and two fewer full metadata directory walks per boot.
	metadata, merr := r.storage.GetDurableEntityMetadata()
	if merr != nil {
		r.absorb("metadata", merr)
		return stats, r.result()
	}

	// Step 2: Recover durable exchanges
	r.recoverDurableExchanges(stats, metadata)

	// Step 3: Recover durable queues
	r.recoverDurableQueues(stats, metadata)

	// Step 5: Recover persistent messages
	r.recoverPersistentMessages(stats)

	// Step 6: Recover pending acknowledgments
	if err := r.recoverPendingAcknowledgments(stats); err != nil {
		if r.absorb("pending-acks", err) == interfaces.RecoveryFatal {
			return stats, r.result()
		}
	}

	stats.RecoveryDuration = time.Since(startTime)

	// Step 7: Mark recovery as complete
	if err := r.storage.MarkRecoveryComplete(stats); err != nil {
		r.logger.Warn("Failed to mark recovery complete", zap.Error(err))
	}

	r.logger.Info("Server recovery completed",
		zap.Duration("duration", stats.RecoveryDuration),
		zap.Int("exchanges_recovered", stats.DurableExchangesRecovered),
		zap.Int("queues_recovered", stats.DurableQueuesRecovered),
		zap.Int("messages_recovered", stats.PersistentMessagesRecovered),
		zap.Int("pending_acks_recovered", stats.PendingAcksRecovered),
		zap.Int("corrupted_entries_repaired", stats.CorruptedEntriesRepaired),
		zap.Strings("validation_errors", stats.ValidationErrors),
	)

	return stats, r.result()
}

// validateAndRepairStorage validates storage integrity and repairs corruption
//
// WS-C C6 seam: storage.ValidateStorageIntegrity/RepairCorruption are
// intentionally NOT implemented by the disruptor backend (an empty success
// would claim a validation that never ran; the classified recovery scan that
// follows already enforces integrity fail-closed). ErrNotImplemented from
// validation therefore means "capability unavailable" — log once and proceed
// to real recovery — never a boot refusal. Any other error still ends
// recovery early (absorbed Fatal, fail closed).
func (r *RecoveryManager) validateAndRepairStorage() (*protocol.RecoveryStats, error) {
	r.logger.Info("Validating storage integrity...")

	// First validate storage integrity
	stats, err := r.storage.ValidateStorageIntegrity()
	if errors.Is(err, storage.ErrNotImplemented) {
		r.logger.Warn("Storage integrity pre-validation is not implemented by this storage backend; " +
			"integrity is enforced by the classified recovery scan that follows")
		return &protocol.RecoveryStats{}, nil
	}
	if err != nil {
		return stats, err
	}
	if stats == nil {
		stats = &protocol.RecoveryStats{}
	}

	// If validation errors were found, attempt auto-repair
	if len(stats.ValidationErrors) > 0 {
		r.logger.Warn("Storage validation errors found, attempting auto-repair",
			zap.Int("error_count", len(stats.ValidationErrors)))

		repairStats, err := r.storage.RepairCorruption(true) // auto-repair enabled
		if err != nil {
			return stats, err
		}

		// Merge repair stats
		stats.CorruptedEntriesRepaired = repairStats.CorruptedEntriesRepaired
		if len(repairStats.ValidationErrors) > 0 {
			stats.ValidationErrors = append(stats.ValidationErrors, repairStats.ValidationErrors...)
		}
	}

	return stats, nil
}

// recoverDurableExchanges recovers all durable exchanges from storage.
// A per-exchange declare failure is DEGRADED, not silent: the exchange is
// missing from the running broker and anything bound through it is unroutable.
func (r *RecoveryManager) recoverDurableExchanges(stats *protocol.RecoveryStats, metadata *protocol.DurableEntityMetadata) {
	r.logger.Info("Recovering durable exchanges...")

	for i := range metadata.Exchanges {
		exchange := metadata.Exchanges[i]
		if exchange.Durable {
			err := r.broker.DeclareExchange(
				exchange.Name,
				exchange.Kind,
				exchange.Durable,
				exchange.AutoDelete,
				exchange.Internal,
				exchange.Arguments,
			)
			if err != nil {
				r.logger.Error("Failed to recover durable exchange",
					zap.String("exchange", exchange.Name),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to recover exchange %s: %v", exchange.Name, err))
				r.addFault(interfaces.DegradedFault("exchange-recovery", "exchange "+exchange.Name,
					"a durable exchange that exists on disk could not be redeclared into the running broker",
					"this exchange is absent from the running broker; publishes to it are unroutable and every queue bound through it receives nothing",
					err))
			} else {
				stats.DurableExchangesRecovered++
				r.logger.Debug("Recovered durable exchange",
					zap.String("exchange", exchange.Name),
					zap.String("type", exchange.Kind))
			}
		}
	}
}

// recoverDurableQueues recovers all durable queues from storage.
// A per-queue declare failure is DEGRADED: that queue's confirmed durable
// records are on disk with nothing to load them into.
func (r *RecoveryManager) recoverDurableQueues(stats *protocol.RecoveryStats, metadata *protocol.DurableEntityMetadata) {
	r.logger.Info("Recovering durable queues...")

	for i := range metadata.Queues {
		queue := metadata.Queues[i]
		if queue.Durable {
			// Capture the ordinal EXACTLY as persisted, before DeclareQueue's
			// resolveQueueOrdinal cold path can silently allocate and persist a
			// fresh one for a record that still reads 0 — see
			// queueOrdinalsAtDeclare's field doc.
			r.queueOrdinalsAtDeclare[queue.Name] = queue.Ordinal

			_, err := r.broker.DeclareQueue(
				queue.Name,
				queue.Durable,
				queue.AutoDelete,
				queue.Exclusive,
				queue.Arguments,
			)
			if err != nil {
				r.logger.Error("Failed to recover durable queue",
					zap.String("queue", queue.Name),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to recover queue %s: %v", queue.Name, err))
				r.addFault(interfaces.DegradedFault("queue-recovery", "queue "+queue.Name,
					"a durable queue that exists on disk could not be redeclared into the running broker",
					"this queue is absent from the running broker; its confirmed durable records stay on disk with nothing to load them into, and a client redeclaring the name may be handed a different incarnation",
					err))
			} else {
				stats.DurableQueuesRecovered++
				r.logger.Debug("Recovered durable queue",
					zap.String("queue", queue.Name))
			}
		}
	}
}

// recoverPersistentMessages recovers all persistent messages from storage.
//
// The shared WAL is never purged when a queue is deleted (DeleteQueue only
// tears down the in-memory ring), and recoverable records are gathered by
// queue NAME. So records from several past "incarnations" of the same name —
// each minted under a different ordinal — can all surface here, and records
// can survive for a queue that no longer exists at all. The ordinal and the
// existence of a metadata record for queueName MUST be resolved BEFORE any
// message is loaded into the ring, so records belonging to dead incarnations
// or deleted queues are never loaded in the first place — loading them and
// then discarding would still let the ordinal-mismatch panic below trip on
// perfectly ordinary dead-incarnation records.
// EVERY FAULT IS ENUMERATED AND NOTHING ALREADY READ IS THROWN AWAY. A fatal
// fault is confined to the QUEUE (or, upstream, the WAL file) that produced it:
// that queue's records are quarantined — named, counted, not loaded — and every
// other queue is still recovered. It used to `return true` on the first fatal
// fault, and the wal-scan branch below used to DISCARD the recoverableMessages
// map it had just been handed. Measured consequence (review-3 B-1): with
// --unsafe-recovery, one flipped bit in one of six WAL files cost all 271,080
// confirmed durable messages in the directory while the operator-facing cost
// statement named one file.
func (r *RecoveryManager) recoverPersistentMessages(stats *protocol.RecoveryStats) {
	r.logger.Info("Recovering persistent messages...")

	// Partial data is returned alongside the fault deliberately — a quarantined
	// file must not cost us the records we DID read, whatever its class.
	recoverableMessages, err := r.storage.GetRecoverableMessages()
	if err != nil {
		r.absorb("wal-scan", err)
	}

	for queueName, messages := range recoverableMessages {
		queueOrdinal, hasRecord, durable, oerr := r.resolveRecoveredQueueOrdinal(queueName)
		if oerr != nil {
			r.addFault(interfaces.FatalFault("queue-recovery", "queue "+queueName,
				"the metadata record for a queue that has recoverable records could not be read, so its delivery-tag ordinal is unknown and its records cannot be attributed to an incarnation",
				"every confirmed durable record for this queue is abandoned, and a later declare of the same name may be handed a colliding ordinal; every OTHER queue in this data directory is recovered normally",
				oerr))
			continue
		}

		if !hasRecord {
			// Case C: no metadata record exists for this queue name.
			//
			// The inference "absent record ⇒ deleted queue" is unsound in
			// general, and the recon named three other producers of absence.
			// The DANGEROUS one — a metadata record that is PRESENT but
			// unreadable, which ListQueues silently skips and GetQueue then
			// fails on — is now fatal, and it is fatal on the branch above
			// (oerr != nil), not here: existingOrdinalLocked and
			// loadQueueFromDisk were made to distinguish "absent" from
			// "present but unreadable", which is exactly the distinction that
			// was missing.
			//
			// What is left here is GENUINE ABSENCE, and genuine absence is what
			// queue.delete produces. Classifying it fatal would mean that
			// deleting a durable queue that still had a backlog — an ordinary
			// AMQP operation — bricks the next restart, because DeleteQueue
			// tears down the ring but never purges the shared WAL, so leftover
			// records are the NORM after any such delete. Two existing tests
			// assert that boot explicitly. This is therefore BENIGN-but-counted
			// rather than fatal, and it stays that way until a queue-delete
			// tombstone (recon-recovery S3) makes "deleted" a fact rather than
			// an inference. See .notes/loop-2/step3.md for the full argument;
			// the tombstone is the one change that lets this become fatal
			// without breaking delete.
			//
			// Non-durable leftovers are likewise discarded: AMQP 0-9-1
			// §1.7.2.1 says a non-durable queue must not survive a restart, so
			// there is nothing here a client was ever promised.
			var lo, hi uint64
			var durableCount int
			for _, message := range messages {
				if message.DeliveryMode != 2 {
					continue
				}
				if durableCount == 0 || message.DeliveryTag < lo {
					lo = message.DeliveryTag
				}
				if message.DeliveryTag > hi {
					hi = message.DeliveryTag
				}
				durableCount++
			}
			if durableCount == 0 {
				r.addFault(interfaces.BenignFault("queue-recovery", "queue "+queueName,
					fmt.Sprintf("discarded %d non-durable record(s) for a queue with no metadata record", len(messages))))
				continue
			}
			r.logger.Warn("discarding recovered records for a queue with no metadata record",
				zap.String("queue", queueName),
				zap.Int("durable_records", durableCount),
				zap.Uint64("ordinal_span_min", broker.TagOrdinal(lo)),
				zap.Uint64("ordinal_span_max", broker.TagOrdinal(hi)))
			r.addFault(interfaces.BenignFault("queue-recovery", "queue "+queueName,
				fmt.Sprintf("discarded %d durable record(s) (tags %d..%d) for a queue whose metadata record is ABSENT (AMQP 0-9-1 §1.7.2.10, queue.delete removes the queue and its messages)",
					durableCount, lo, hi)))
			continue
		}

		if !durable {
			// AMQP 0-9-1 §1.7.2.1: a non-durable queue does not survive a
			// server restart. DeclareQueue persists a metadata record for
			// EVERY queue (the Durable check only gates updateDurableMetadata),
			// so a transient queue leaves a permanent record with an allocated
			// ordinal, and any DeliveryMode==2 message published to it used to
			// take Case A here — loaded into a live QueueState that a client
			// redeclaring durable=false then inherited, messages and all.
			//
			// The records are reaped rather than loaded. StoreQueue is
			// deliberately NOT gated instead: createQueueStateLocked guards on
			// record existence, so refusing to persist transient queues would
			// break them outright.
			r.addFault(interfaces.BenignFault("queue-recovery", "queue "+queueName,
				fmt.Sprintf("reaped %d record(s) belonging to a NON-DURABLE queue; AMQP 0-9-1 §1.7.2.1 forbids it surviving a restart", len(messages))))
			r.logger.Warn("reaping recovered records for a non-durable queue",
				zap.String("queue", queueName),
				zap.Int("reaped", len(messages)))
			continue
		}

		if queueOrdinal == 0 {
			// Case D: metadata record exists but carries Ordinal==0 while
			// records exist for it — a genuine pre-packing data directory.
			// This is the ONLY case allowed to report ErrLegacyDataDirectory.
			var count uint64
			var minTag, maxTag uint64
			has := false
			for _, message := range messages {
				if message.DeliveryMode != 2 {
					continue
				}
				if !has || message.DeliveryTag < minTag {
					minTag = message.DeliveryTag
				}
				if message.DeliveryTag > maxTag {
					maxTag = message.DeliveryTag
				}
				has = true
				count++
			}
			if has {
				r.addFault(interfaces.FatalFault("queue-recovery", "queue "+queueName,
					fmt.Sprintf("%s: %d recovered durable message(s) (tags %d..%d) but no delivery-tag ordinal was ever persisted for this queue",
						ErrLegacyDataDirectory.Error(), count, minTag, maxTag),
					"the recovered delivery tags are raw legacy values whose high bits are not a valid queue ordinal; starting anyway abandons every one of these confirmed durable messages, and the tags they carry may collide with another queue's tag space; every OTHER queue in this data directory is recovered normally",
					ErrLegacyDataDirectory))
				continue
			}
			// No DeliveryMode==2 records at all — nothing to recover, nothing
			// to refuse over.
			continue
		}

		// Case E is checked in a PRE-PASS, before a single record of this queue
		// is loaded. A record whose tag ordinal EXCEEDS the queue's assigned
		// ordinal is never a dead incarnation — it is metadata corruption (the
		// persisted ordinal regressed) — and it means this queue's whole
		// recovered tag space is untrustworthy. Detecting it mid-load, which is
		// what the loop below used to do, left however many records had already
		// been loaded sitting in a ring whose sequence was never restored
		// (RecoverQueue is not reached), so "abandoned" was not what actually
		// happened. Pre-passing makes the quarantine total and the cost
		// statement true.
		mismatched := false
		for _, message := range messages {
			if message.DeliveryMode != 2 {
				continue
			}
			if tagOrdinal := broker.TagOrdinal(message.DeliveryTag); tagOrdinal > queueOrdinal {
				r.addFault(interfaces.FatalFault("queue-recovery", "queue "+queueName,
					fmt.Sprintf("%s: queue is assigned ordinal %d, but a recovered tag carries ordinal %d (tag=%d)",
						ErrOrdinalMismatch.Error(), queueOrdinal, tagOrdinal, message.DeliveryTag),
					"recovery is about to hand this queue's consumers delivery tags that collide with another queue's tag space in a WAL keyed by a bare uint64; starting anyway risks cross-queue message corruption, not just loss. Every record of this queue is quarantined and none is loaded; every OTHER queue in this data directory is recovered normally",
					ErrOrdinalMismatch))
				mismatched = true
				break
			}
		}
		if mismatched {
			continue
		}

		// Cases A and B: a real ordinal is assigned to this queue name.
		// Partition records into ones that belong to this incarnation
		// (ordinal == queueOrdinal, case A) versus dead older incarnations
		// of the same name (ordinal < queueOrdinal, case B).
		var minTag, maxTag uint64
		var queueCount uint64
		hasRecovered := false
		var discardedCount int
		var discardedLo, discardedHi uint64
		hasDiscarded := false
		for _, message := range messages {
			if message.DeliveryMode != 2 {
				continue
			}
			tagOrdinal := broker.TagOrdinal(message.DeliveryTag)
			if tagOrdinal < queueOrdinal {
				// Case B: a dead incarnation's record. Discard — do not load,
				// do not count.
				discardedCount++
				if !hasDiscarded || message.DeliveryTag < discardedLo {
					discardedLo = message.DeliveryTag
				}
				if message.DeliveryTag > discardedHi {
					discardedHi = message.DeliveryTag
				}
				hasDiscarded = true
				continue
			}

			// tagOrdinal == queueOrdinal: belongs to the current incarnation.
			//
			// The tag SPAN is widened for EVERY physically-present record, and
			// nothing may ever be filtered out of it. That is load-bearing, not
			// incidental: broker/queue_dispatch.go RecoverSeq restores this
			// queue's next delivery-tag sequence from maxTag, and its safety
			// argument is that any record still on disk comes back inside
			// [minTag, maxTag] so a resumed sequence cannot mint a tag whose
			// bytes are still present. Narrowing the span to any subset — the
			// unacknowledged one was the concrete proposal — reintroduces
			// exactly that collision against a globally tag-keyed shared WAL,
			// where it silently destroys the newly published messages. Marked
			// never-resurrect; see .notes/loop-2/deferred-ack-durability.md.
			if !hasRecovered || message.DeliveryTag < minTag {
				minTag = message.DeliveryTag
			}
			if message.DeliveryTag > maxTag {
				maxTag = message.DeliveryTag
			}
			hasRecovered = true

			err := r.storage.LoadMessageFromRecovery(queueName, message)
			if err != nil {
				r.logger.Error("Failed to load recovered message into ring buffer",
					zap.String("queue", queueName),
					zap.Uint64("delivery_tag", message.DeliveryTag),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to load message in queue %s: %v", queueName, err))
				r.addFault(interfaces.DegradedFault("message-recovery", "queue "+queueName,
					fmt.Sprintf("a confirmed durable record (delivery tag %d) could not be loaded back into the queue", message.DeliveryTag),
					"this confirmed durable message is not delivered after the restart; its delivery tag stays inside the recovered span, so it is skipped as a gap",
					err))
				continue
			}
			stats.PersistentMessagesRecovered++
			queueCount++
			r.logger.Debug("Loaded recovered message into ring buffer",
				zap.String("queue", queueName),
				zap.Uint64("delivery_tag", message.DeliveryTag))
		}

		if hasDiscarded {
			r.logger.Warn("discarding records from a dead incarnation of a queue",
				zap.String("queue", queueName),
				zap.Int("discarded", discardedCount),
				zap.Uint64("ordinal_span_min", broker.TagOrdinal(discardedLo)),
				zap.Uint64("ordinal_span_max", broker.TagOrdinal(discardedHi)))
			// BENIGN, and counted. Case B is the one discard in this function
			// that rests on a fact rather than an inference: the tag's ordinal
			// is strictly below the queue's CURRENT ordinal, and an ordinal is
			// write-once per incarnation and never reclaimed, so the record
			// provably belongs to an incarnation that was deleted. AMQP
			// §1.7.2.10 says its messages went with it.
			r.addFault(interfaces.BenignFault("queue-recovery", "queue "+queueName,
				fmt.Sprintf("discarded %d record(s) from a dead incarnation (tag ordinals %d..%d, current ordinal %d)",
					discardedCount, broker.TagOrdinal(discardedLo), broker.TagOrdinal(discardedHi), queueOrdinal)))
		}

		if hasRecovered {
			r.broker.RecoverQueue(queueName, minTag, maxTag, queueCount)
		}
	}
}

// resolveRecoveredQueueOrdinal returns the composite-tag ordinal
// (broker/tag_packing.go) recovery should expect a queue's recovered
// delivery tags to carry, and whether a metadata record exists for
// queueName at all. The two are reported separately (tri-state) because
// they demand opposite responses at recovery time:
//
//   - hasRecord == false means no metadata record exists for this name. That
//     is NOT proof the queue was deleted — see the Case C comment in
//     recoverPersistentMessages for the three other ways absence is produced —
//     which is why it is now fatal for durable records rather than a silent
//     discard.
//   - hasRecord == true, ordinal == 0 means a metadata record exists but
//     never had a composite-tag ordinal assigned: a genuine pre-packing
//     data directory, which IS fatal (ErrLegacyDataDirectory).
//   - durable reports the record's Durable flag, so recovery can reap the
//     records of a NON-durable queue instead of resurrecting it
//     (AMQP 0-9-1 §1.7.2.1). Anything in queueOrdinalsAtDeclare is durable by
//     construction: recoverDurableQueues only populates it inside its
//     `if queue.Durable` branch.
//
// The ordinal comes from the value captured before recoverDurableQueues
// (re)declared the queue (the normal case for any durable queue — see
// queueOrdinalsAtDeclare's field doc), or from the currently persisted
// record for a queue that path never touched, e.g. a non-durable queue that
// nonetheless holds recoverable persistent messages.
func (r *RecoveryManager) resolveRecoveredQueueOrdinal(queueName string) (ordinal uint64, hasRecord bool, durable bool, err error) {
	if ordinal, ok := r.queueOrdinalsAtDeclare[queueName]; ok {
		return ordinal, true, true, nil
	}
	q, err := r.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return 0, false, false, nil
		}
		return 0, false, false, err
	}
	if q == nil {
		return 0, false, false, nil
	}
	return q.Ordinal, true, q.Durable, nil
}

// recoverPendingAcknowledgments recovers all pending acknowledgments from storage
func (r *RecoveryManager) recoverPendingAcknowledgments(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering pending acknowledgments...")

	pendingAcks, err := r.storage.GetAllPendingAcks()
	if err != nil {
		return err
	}

	// Clean up expired acknowledgments (older than 1 hour)
	expiredCutoff := 1 * time.Hour
	err = r.storage.CleanupExpiredAcks(expiredCutoff)
	if err != nil {
		r.logger.Warn("Failed to cleanup expired acknowledgments", zap.Error(err))
	}

	// CRITICAL FIX: Rebuild global delivery index during crash recovery
	// This ensures ACKs can be routed correctly after server restart
	for _, pendingAck := range pendingAcks {
		if !pendingAck.DeliveredAt.IsZero() {
			age := time.Since(pendingAck.DeliveredAt)
			if age > expiredCutoff {
				err := r.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
				if err != nil {
					r.logger.Warn("Failed to delete expired pending ack",
						zap.String("queue", pendingAck.QueueName),
						zap.Uint64("delivery_tag", pendingAck.DeliveryTag),
						zap.Error(err))
				}
				continue
			}
		}

		r.broker.RebuildDeliveryIndex(pendingAck.DeliveryTag, pendingAck.ConsumerTag)

		stats.PendingAcksRecovered++
		r.logger.Debug("Recovered pending acknowledgment",
			zap.String("queue", pendingAck.QueueName),
			// PendingAck.ConsumerTag carries the broker-internal Consumer.ID, not
			// the client-visible tag (see protocol.PendingAck). Logged under its
			// real name so an operator does not read "cid-7" as a client's tag.
			zap.String("consumer_id", pendingAck.ConsumerTag),
			zap.Uint64("delivery_tag", pendingAck.DeliveryTag))
	}

	return nil
}
