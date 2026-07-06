package storage

import (
	"fmt"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// txStagingStorage is the all-or-nothing storage view handed to the callback of
// DisruptorStorage.ExecuteAtomic (SQ-8). It embeds the real *DisruptorStorage so
// every read/metadata method is delegated unchanged, but it intercepts the two
// mutating message operations — StoreMessage and DeleteMessage — and BUFFERS
// them instead of applying them immediately.
//
// If the callback returns an error, the buffer is simply dropped: nothing was
// ever written to a queue ring or to the WAL, so the transaction rolls back
// cleanly with no partial effects. If the callback succeeds, commit() applies
// the whole buffer as one durable unit: the durable publishes are written to
// the WAL bracketed by transaction-boundary markers (a single fsync), then the
// in-memory ring state and any settlements are applied.
//
// Before SQ-8, ExecuteAtomic simply ran the callback against the live storage,
// so a failure partway through a transaction's operations left the earlier
// operations applied — the atomicity guarantee was a lie on partial failure.
type txStagingStorage struct {
	*DisruptorStorage

	stores  []stagedStore
	deletes []stagedDelete
}

type stagedStore struct {
	queueName string
	message   *protocol.Message
}

type stagedDelete struct {
	queueName   string
	deliveryTag uint64
}

// StoreMessage buffers a publish. It performs the same up-front durability
// validation as DisruptorStorage.StoreMessage so an impossible operation (a
// durable message with no WAL) fails the callback and rolls the whole
// transaction back, rather than being discovered mid-commit.
func (s *txStagingStorage) StoreMessage(queueName string, message *protocol.Message) error {
	if s.wal == nil && message.DeliveryMode == 2 {
		return fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)
	}
	s.stores = append(s.stores, stagedStore{queueName: queueName, message: message})
	return nil
}

// DeleteMessage buffers a settlement (ack/reject-drop). Applied only on commit.
func (s *txStagingStorage) DeleteMessage(queueName string, deliveryTag uint64) error {
	s.deletes = append(s.deletes, stagedDelete{queueName: queueName, deliveryTag: deliveryTag})
	return nil
}

// commit durably applies the whole staged operation set as one atomic unit.
func (s *txStagingStorage) commit() error {
	ds := s.DisruptorStorage

	// Phase 1 — durability. Collect the durable publishes and write them to the
	// WAL as a single all-or-nothing transaction unit bracketed by begin/commit
	// markers. A crash before this returns leaves NO commit marker on disk, so
	// recovery discards every record in the unit (recovery atomicity). If this
	// fails, no ring state has been touched yet, so the transaction aborts clean.
	var durableRecords []*RecoveryMessage
	for i := range s.stores {
		st := &s.stores[i]
		if st.message.DeliveryMode == 2 && ds.wal != nil {
			durableRecords = append(durableRecords, &RecoveryMessage{
				QueueName: st.queueName,
				Message:   st.message,
				Offset:    st.message.DeliveryTag,
			})
		}
	}
	if len(durableRecords) > 0 {
		if err := ds.wal.WriteTxAtomic(durableRecords); err != nil {
			return fmt.Errorf("atomic transaction commit failed writing WAL: %w", err)
		}
	}

	// Phase 2 — in-memory ring state. Durable messages are already in the WAL
	// (via WriteTxAtomic above), so they only need to be inserted into the ring
	// (mirroring StoreMessage minus its per-message WAL write). Transient
	// messages take the normal StoreMessage path (ring, with spill-to-WAL).
	for i := range s.stores {
		st := &s.stores[i]
		if st.message.DeliveryMode == 2 && ds.wal != nil {
			if err := ds.insertDurableIntoRing(st.queueName, st.message); err != nil {
				return fmt.Errorf("atomic transaction commit failed applying ring state: %w", err)
			}
			continue
		}
		if err := ds.StoreMessage(st.queueName, st.message); err != nil {
			return fmt.Errorf("atomic transaction commit failed storing message: %w", err)
		}
	}

	// Phase 3 — settlements (acks / dropped rejects).
	for _, d := range s.deletes {
		if err := ds.DeleteMessage(d.queueName, d.deliveryTag); err != nil {
			return fmt.Errorf("atomic transaction commit failed settling delivery: %w", err)
		}
	}

	return nil
}

// insertDurableIntoRing places an already-WAL-durable message into its queue
// ring. It mirrors the ring-handling half of StoreMessage (spill awareness
// included) but performs NO WAL write, since the message was already persisted
// as part of the atomic transaction unit.
func (ds *DisruptorStorage) insertDurableIntoRing(queueName string, message *protocol.Message) error {
	ring := ds.getOrCreateQueueRing(queueName)

	// Ring already over the spill threshold: leave the message in the WAL only
	// (it is read on demand), exactly as StoreMessage would.
	if ring.ring.Count() > ds.spillThreshold {
		return nil
	}

	_, spilled, err := ring.ring.Store(message.DeliveryTag, message)
	if err != nil {
		return fmt.Errorf("failed to store message in ring: %w", err)
	}
	if spilled {
		// Spilled out of the ring; the WAL copy is authoritative.
		return nil
	}

	ring.ack.OnPublish(message.DeliveryTag)
	return nil
}

// ensure the staging view satisfies the full Storage contract.
var _ interfaces.Storage = (*txStagingStorage)(nil)
