package server

// recovery_metrics.go — the CLOSED label domains for the spec 06 §3.2–§3.5
// broker families. Go cannot restrict string values, so these const blocks are
// the guard: every recorder call site passes one of these values, and the
// metrics inventory test pins the family set so a new family cannot appear
// unnoticed. No value is a queue name, connection id, user name, routing key or
// exchange name.

// Recovery stages (spec 06 §3.2). These mirror the stage strings recovery
// uses in its faults and are the CLOSED metric label domain for
// amqp_recovery_faults_total{stage} / amqp_recovery_discarded_records_total{stage}.
// Every recording site passes one of these consts — a string literal here would
// silently widen the domain past the guard.
//
// The former `recovery` arm is deliberately absent: PerformRecovery classified
// and recorded every fault at the site that created it, so the builder's
// aggregate classifier could never mint a fault of that stage. It was a
// permanently-zero series (see builder.go's classifier).
const (
	RecoveryStageStorageValidation = "storage-validation"
	RecoveryStageMetadata          = "metadata"
	RecoveryStageExchange          = "exchange-recovery"
	RecoveryStageQueue             = "queue-recovery"
	RecoveryStageMessage           = "message-recovery"
	RecoveryStageWALScan           = "wal-scan"
	RecoveryStagePendingAcks       = "pending-acks"
	RecoveryStageOrdinalAllocator  = "ordinal-allocator"
)

// Recovery outcomes (spec 06 §3.2), matching interfaces.RecoveryOutcome.String().
const (
	RecoveryOutcomeDegraded = "degraded"
)

// Publish-refusal reasons (spec 06 §3.4). Unroutable is deliberately absent:
// amqp_messages_unroutable_total carries that signal.
const (
	PublishRefusedMaxLength   = "x_max_length"
	PublishRefusedQueueClosed = "queue_closed"
)

// Delivery requeue / drop reasons (spec 06 §3.4).
const (
	DeliveryRequeueWriteError   = "write_error"
	DeliveryDropConsumerRemoved = "consumer_removed"
)

// Connection close causes (spec 06 §3.5).
const (
	ConnectionCloseClientEOF        = "client_eof"
	ConnectionCloseProtocolError    = "protocol_error"
	ConnectionCloseHeartbeatTimeout = "heartbeat_timeout"
	ConnectionCloseForcedShutdown   = "forced_shutdown"
	ConnectionCloseReaderOverflow   = "reader_overflow"
	ConnectionCloseOversizedAck     = "oversized_ack"
	ConnectionCloseBodyStall        = "body_stall"
	ConnectionCloseHandshakeBacklog = "handshake_backlog"
	ConnectionCloseWriteError       = "write_error"
	ConnectionCloseLeaseClose       = "lease_close"
)

// Shutdown results (spec 06 §3.5).
const (
	ShutdownResultClean      = "clean"
	ShutdownResultForced     = "forced"
	ShutdownResultIncomplete = "incomplete"
)

// Resource-alarm resources (spec 06 §3.5).
const (
	AlarmResourceMemory = "memory"
	AlarmResourceDisk   = "disk"
)

// Panic components (spec 06 §3.5). The spec's seven names plus "other" for the
// remaining per-connection goroutine names (ack-processor, confirm-flusher,
// flow-sender, lease-close, handleConnection); bounding them all to "other"
// keeps the label domain closed rather than letting goroutine names leak into
// the series.
const (
	PanicComponentReader    = "reader"
	PanicComponentProcessor = "processor"
	PanicComponentDelivery  = "delivery"
	PanicComponentForwarder = "forwarder"
	PanicComponentHeartbeat = "heartbeat"
	PanicComponentCleanup   = "cleanup"
	PanicComponentOther     = "other"
)

// panicComponent maps a per-connection goroutine name to the bounded component
// enum.
func panicComponent(where string) string {
	switch where {
	case PanicComponentReader:
		return PanicComponentReader
	case PanicComponentProcessor:
		return PanicComponentProcessor
	case PanicComponentDelivery:
		return PanicComponentDelivery
	case PanicComponentForwarder:
		return PanicComponentForwarder
	case PanicComponentHeartbeat:
		return PanicComponentHeartbeat
	case PanicComponentCleanup:
		return PanicComponentCleanup
	default:
		return PanicComponentOther
	}
}

// RecoveryFaultSink is the optional metrics surface RecoveryManager writes its
// classified faults and discarded-record counts through. It is a narrow
// interface (not MetricsCollector) so the recovery manager has no other
// dependency; the production *metrics.Collector implements it and a NoOp
// collector simply does not (every call is then a no-op).
type RecoveryFaultSink interface {
	RecordRecoveryFault(stage, outcome string)
	RecordRecoveryDiscardedRecords(stage string, n int)
}
