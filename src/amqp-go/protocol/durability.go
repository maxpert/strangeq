package protocol

import (
	"time"
)

// PendingAck represents a message that has been delivered but not yet acknowledged
type PendingAck struct {
	QueueName   string `json:"queue_name"`
	DeliveryTag uint64 `json:"delivery_tag"`
	// ConsumerTag holds the broker-internal consumer identity (Consumer.ID),
	// not the client-visible tag — that is the key the ack ledger is addressed
	// by. The struct field and its serialized name are kept for on-disk
	// compatibility with existing pending-ack records. The value is meaningless
	// after a restart (consumer identities are process-scoped and no client's
	// consumer survives a restart), which is already true of the tag it
	// replaced: RecoveryManager only reinstates the delivery-index entry so the
	// tag is owned by nobody and can be requeued.
	ConsumerTag     string    `json:"consumer_tag"`
	MessageID       string    `json:"message_id"` // For correlation with stored message
	DeliveredAt     time.Time `json:"delivered_at"`
	RedeliveryCount int       `json:"redelivery_count"` // Track redeliveries
	Redelivered     bool      `json:"redelivered"`      // Whether this is a redelivery
}

// DurableEntityMetadata contains metadata about durable entities for recovery
type DurableEntityMetadata struct {
	// Exchange metadata (pointers to avoid copying sync primitives)
	Exchanges []*Exchange `json:"exchanges"`

	// Queue metadata (pointers to avoid copying sync primitives)
	Queues []*Queue `json:"queues"`

	// NO BINDING FIELD. It existed, was initialised to an empty slice by
	// StorageEngine.GetDurableEntityMetadata, and was never appended to, so
	// RecoveryManager.recoverBindings' loop body never executed once.
	//
	// Durable bindings DO survive a restart, and not through recovery: the
	// routing path re-reads them from storage on every publish
	// (broker/storage_broker.go calls GetExchangeBindings /
	// GetExchangeBindingsFrom from its routing functions, ~14 sites), backed by
	// PersistentMetadataStore.ListBindings and its caches. The CBOR files are
	// the authority; the in-memory exchange is not. Re-establishing bindings at
	// boot would therefore create a SECOND source of truth for a fact that is
	// already correct, so this was deleted rather than wired (canon rule 6).
	// Verified by experiment in .notes/loop-2/review-3.md ("THE LEAD") and
	// re-verified in .notes/loop-2/step3-fix.md.

	// Timestamp for recovery validation
	LastUpdated time.Time `json:"last_updated"`
}

// RecoveryStats tracks statistics during server startup recovery
type RecoveryStats struct {
	DurableExchangesRecovered   int           `json:"durable_exchanges_recovered"`
	DurableQueuesRecovered      int           `json:"durable_queues_recovered"`
	PersistentMessagesRecovered int           `json:"persistent_messages_recovered"`
	PendingAcksRecovered        int           `json:"pending_acks_recovered"`
	CorruptedEntriesRepaired    int           `json:"corrupted_entries_repaired"`
	RecoveryDuration            time.Duration `json:"recovery_duration"`
	ValidationErrors            []string      `json:"validation_errors,omitempty"`
}
