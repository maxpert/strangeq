package protocol

import (
	"time"
)

// PendingAck represents a message that has been delivered but not yet acknowledged
type PendingAck struct {
	QueueName       string    `json:"queue_name"`
	DeliveryTag     uint64    `json:"delivery_tag"`
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

	// Binding metadata
	Bindings []Binding `json:"bindings"`

	// Timestamp for recovery validation
	LastUpdated time.Time `json:"last_updated"`
}

// RecoveryStats tracks statistics during server startup recovery
type RecoveryStats struct {
	DurableExchangesRecovered   int           `json:"durable_exchanges_recovered"`
	DurableQueuesRecovered      int           `json:"durable_queues_recovered"`
	BindingsRecovered           int           `json:"bindings_recovered"`
	PersistentMessagesRecovered int           `json:"persistent_messages_recovered"`
	PendingAcksRecovered        int           `json:"pending_acks_recovered"`
	CorruptedEntriesRepaired    int           `json:"corrupted_entries_repaired"`
	RecoveryDuration            time.Duration `json:"recovery_duration"`
	ValidationErrors            []string      `json:"validation_errors,omitempty"`
}
