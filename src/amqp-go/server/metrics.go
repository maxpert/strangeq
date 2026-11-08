package server

// MetricsCollector defines the interface for metrics collection
type MetricsCollector interface {
	// Connection metrics
	RecordConnectionCreated()
	RecordConnectionClosed()

	// Channel metrics
	RecordChannelCreated()
	RecordChannelClosed()

	// Queue metrics
	RecordQueueDeclared()
	RecordQueueDeleted()
	UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int)
	DeleteQueueMetrics(queueName, vhost string)

	// Exchange metrics
	RecordExchangeDeclared()
	RecordExchangeDeleted()

	// Message metrics
	RecordMessagePublished(size int)
	RecordMessageDelivered(size int)
	RecordMessageAcknowledged()
	// RecordMessagesAcknowledged counts n deliveries settled by ONE cumulative
	// (multiple=true) basic.ack. The singular form above is one settled
	// delivery; this form exists because a cumulative ack settles len(refs)
	// deliveries but used to be counted as one (finding 5).
	RecordMessagesAcknowledged(n int)
	RecordMessageRedelivered()
	RecordMessageRejected()
	// RecordMessagesRejected counts n deliveries settled by ONE cumulative
	// (multiple=true) basic.nack (finding 5).
	RecordMessagesRejected(n int)
	RecordMessageUnroutable()

	// Consumer metrics
	SetConsumersTotal(count int)

	// Transaction metrics
	RecordTransactionStarted()
	RecordTransactionCommitted()
	RecordTransactionRolledback()

	// Server metrics
	UpdateServerUptime(seconds float64)

	// SetUnsafeRecoveryArtifacts pins, for the lifetime of the process, how
	// many artifacts this broker DISCARDED at boot because --unsafe-recovery
	// was set. Zero on a clean boot. It is a gauge rather than a counter
	// deliberately: the fact an operator needs is "this process is running on
	// data it could not fully recover", which is true for as long as the
	// process lives, not an event that scrolls out of a log.
	SetUnsafeRecoveryArtifacts(count int)

	// Latency metrics
	RecordPublishLatency(duration float64)
	RecordDeliveryLatency(duration float64)

	// Disk metrics
	UpdateDiskMetrics(freeBytes, usedBytes float64)

	// WAL metrics
	UpdateWALSize(bytes float64)
	RecordWALWrite()
	RecordWALFsync(duration float64)
	RecordWALWriteError()

	// Segment metrics
	UpdateSegmentMetrics(queueName string, count, sizeBytes float64)
	DeleteSegmentMetrics(queueName string)
	RecordSegmentCompaction()
	RecordSegmentCompactionFailure()
	RecordSegmentReadError()

	// Hot-cache delivery-path metrics (Task 2). These mirror
	// storage.StorageMetrics so the collector stays assignable to it, and they
	// are what make the cold-read fix provable: the delivery hot-hit ratio is
	// hits / (hits + cold_reads), and preads per delivered message is
	// preads / deliveries.
	RecordHotCacheDeliveryHit(queue string)
	RecordHotCacheDeliveryColdRead(queue string)
	RecordHotCacheDeliveryPread(queue string, n int)

	// Publisher-park metrics (Task 2): one call per completed queue-depth
	// reader park, so the pacing is visible rather than inferred.
	RecordPublisherPark(seconds float64)

	// Memory metrics
	UpdateMemoryMetrics(usedBytes, heapBytes float64)
	RecordGCPause(duration float64)

	// System resource metrics
	UpdateGoroutines(count float64)
	UpdateFileDescriptors(count float64)

	// Error metrics
	RecordConnectionError(errorType string)
	RecordChannelError(errorType string)
	RecordMessageDropped(queueName, reason string)

	// --- Spec 06 broker outage telemetry (bounded-label families) ---

	// Recovery (spec 06 §3.2). outcome is one of the closed
	// recoveryOutcome* values; stage one of the bounded recoveryStage* values.
	RecordRecoveryFault(stage, outcome string)
	RecordRecoveryDiscardedRecords(stage string, n int)
	RecordRecoveryDuration(seconds float64)
	SetRecoveryMessagesRecovered(n int)

	// WAL file-count gauge (spec 06 §3.3).
	UpdateWALFileCount(n int)

	// Publish / back-pressure / delivery (spec 06 §3.4). Reasons/causes are
	// closed enums (publishRefused*, deliveryRequeue*, deliveryDrop*,
	// connectionCloseCause*).
	RecordPublishRefused(reason string)
	RecordConsumerRegisterError()
	RecordDepthGateArmed()
	RecordDeliverySendError()
	RecordDeliveriesRequeued(reason string, n int)
	RecordDeliveriesDropped(reason string, n int)
	RecordReaderPaused()

	// Connections / shutdown / alarms / panics (spec 06 §3.5).
	RecordConnectionClosedByCause(cause string)
	RecordShutdown(result string, seconds float64)
	RecordShutdownConnectionForced()
	RecordStorageCloseError()
	RecordResourceAlarmTrip(resource string)
	RecordPanic(component string)
}

// DepthGateStatsBinder is an OPTIONAL extension implemented by the production
// collector: the server binds the live gated-connection counter so
// amqp_depth_gated_current is read AT SCRAPE TIME (spec 06 §3.4).
type DepthGateStatsBinder interface {
	BindDepthGateStats(stats func() int64)
}

// ReaderPausedStatsBinder is an OPTIONAL extension implemented by the
// production collector for amqp_reader_paused_current (spec 06 §3.4).
type ReaderPausedStatsBinder interface {
	BindReaderPausedStats(stats func() int64)
}

// MessagesInflightStatsBinder is an OPTIONAL extension implemented by the
// production collector for amqp_messages_inflight (spec 06 §3.5).
type MessagesInflightStatsBinder interface {
	BindMessagesInflightStats(stats func() int64)
}

// LeaseMetrics is an OPTIONAL extension of MetricsCollector (WS-I Phase B):
// the publish-lease subsystem type-asserts for it, so custom collectors keep
// working unchanged. Implemented by metrics.Collector. Wait counts are
// diagnostic only (never a gate); the leaked counter must stay zero.
type LeaseMetrics interface {
	// RecordPublishLeaseWait observes one completed blocking lease wait.
	RecordPublishLeaseWait(seconds float64)
	// RecordPublishLeaseOversized counts an oversized (>G) lease grant.
	RecordPublishLeaseOversized()
	// RecordPublishLeaseOverbudgetClose counts a 506 lease-budget close.
	RecordPublishLeaseOverbudgetClose()
	// RecordPublishLeaseBodyStallClose counts a leased-body-stall close.
	RecordPublishLeaseBodyStallClose()
	// RecordPublishLeaseLeaked counts a lease recovered by PutPendingMessage.
	RecordPublishLeaseLeaked()
}

// LeaseGaugeObserver is an OPTIONAL hook-driven gauge sink (WS-I review):
// called on every grant/release transition with the fresh values. Test probes
// implement it for exact per-step assertions; the production collector uses
// scrape-time GaugeFuncs instead (see LeaseStatsBinder).
type LeaseGaugeObserver interface {
	UpdatePublishLeaseGauges(usedBytes, budgetBytes int64, waiters int)
}

// LeaseStatsBinder is implemented by the production collector: the server
// binds the limiter's Stats function at limiter creation so the
// amqp_publish_lease_* gauges are read AT SCRAPE TIME (never stale) and the
// budget gauge initializes immediately.
type LeaseStatsBinder interface {
	BindPublishLeaseStats(stats func() (used, budget int64, waiters int))
}

// leaseMetrics returns the MetricsCollector as a LeaseMetrics, or nil.
func (s *Server) leaseMetrics() LeaseMetrics {
	lm, _ := s.MetricsCollector.(LeaseMetrics)
	return lm
}

// WALBookkeepingStatsBinder is implemented by the production collector: the
// server binds the storage tier's bookkeeping accessor at build time so the
// amqp_wal_bookkeeping_* gauges are read AT SCRAPE TIME (never stale).
// DIAGNOSTIC ONLY; never gated (WS-MEMORY-CUT §8).
type WALBookkeepingStatsBinder interface {
	BindWALBookkeepingStats(stats func() (inventoryRecords, indexCopies int, drained uint64))
}

// PublisherParkStatsBinder is implemented by the production collector: the
// server binds its publisher-park accessor at build time so the
// amqp_publisher_parked_* gauges are read AT SCRAPE TIME (never stale).
// DIAGNOSTIC ONLY; never gated (Task 2).
type PublisherParkStatsBinder interface {
	BindPublisherParkStats(stats func() (count uint64, seconds float64, current int64))
}

// WALFullFlushSinkBinder is implemented by the production collector: the server
// wires the storage tier's full-flush counter into the
// amqp_wal_full_flush_total{site} metric at build time. GATED on the barrier
// arm: it must stay 0 across the measured window.
type WALFullFlushSinkBinder interface {
	RecordWALFullFlush(site string)
	InitWALFullFlushSites(sites []string)
}

// NoOpMetricsCollector is a metrics collector that does nothing
type NoOpMetricsCollector struct{}

func (n *NoOpMetricsCollector) RecordConnectionCreated() {}
func (n *NoOpMetricsCollector) RecordConnectionClosed()  {}
func (n *NoOpMetricsCollector) RecordChannelCreated()    {}
func (n *NoOpMetricsCollector) RecordChannelClosed()     {}
func (n *NoOpMetricsCollector) RecordQueueDeclared()     {}
func (n *NoOpMetricsCollector) RecordQueueDeleted()      {}
func (n *NoOpMetricsCollector) UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int) {
}
func (n *NoOpMetricsCollector) DeleteQueueMetrics(queueName, vhost string) {}
func (n *NoOpMetricsCollector) RecordExchangeDeclared()                    {}
func (n *NoOpMetricsCollector) RecordExchangeDeleted()                     {}
func (n *NoOpMetricsCollector) RecordMessagePublished(size int)            {}
func (n *NoOpMetricsCollector) RecordMessageDelivered(size int)            {}
func (n *NoOpMetricsCollector) RecordMessageAcknowledged()                 {}
func (n *NoOpMetricsCollector) RecordMessagesAcknowledged(count int)       {}
func (n *NoOpMetricsCollector) RecordMessageRedelivered()                  {}
func (n *NoOpMetricsCollector) RecordMessageRejected()                     {}
func (n *NoOpMetricsCollector) RecordMessagesRejected(count int)           {}
func (n *NoOpMetricsCollector) RecordMessageUnroutable()                   {}
func (n *NoOpMetricsCollector) SetConsumersTotal(count int)                {}
func (n *NoOpMetricsCollector) RecordTransactionStarted()                  {}
func (n *NoOpMetricsCollector) RecordTransactionCommitted()                {}
func (n *NoOpMetricsCollector) RecordTransactionRolledback()               {}
func (n *NoOpMetricsCollector) UpdateServerUptime(seconds float64)         {}
func (n *NoOpMetricsCollector) SetUnsafeRecoveryArtifacts(count int)       {}

// Latency metrics
func (n *NoOpMetricsCollector) RecordPublishLatency(duration float64)  {}
func (n *NoOpMetricsCollector) RecordDeliveryLatency(duration float64) {}

// Disk metrics
func (n *NoOpMetricsCollector) UpdateDiskMetrics(freeBytes, usedBytes float64) {}

// WAL metrics
func (n *NoOpMetricsCollector) UpdateWALSize(bytes float64)     {}
func (n *NoOpMetricsCollector) RecordWALWrite()                 {}
func (n *NoOpMetricsCollector) RecordWALFsync(duration float64) {}
func (n *NoOpMetricsCollector) RecordWALWriteError()            {}

// Segment metrics
func (n *NoOpMetricsCollector) UpdateSegmentMetrics(queueName string, count, sizeBytes float64) {}
func (n *NoOpMetricsCollector) DeleteSegmentMetrics(queueName string)                           {}
func (n *NoOpMetricsCollector) RecordSegmentCompaction()                                        {}
func (n *NoOpMetricsCollector) RecordSegmentCompactionFailure()                                 {}
func (n *NoOpMetricsCollector) RecordSegmentReadError()                                         {}

// Hot-cache delivery-path metrics (Task 2)
func (n *NoOpMetricsCollector) RecordHotCacheDeliveryHit(queue string)      {}
func (n *NoOpMetricsCollector) RecordHotCacheDeliveryColdRead(queue string) {}
func (n *NoOpMetricsCollector) RecordHotCacheDeliveryPread(queue string, count int) {
}

// Publisher-park metrics (Task 2)
func (n *NoOpMetricsCollector) RecordPublisherPark(seconds float64) {}

// Memory metrics
func (n *NoOpMetricsCollector) UpdateMemoryMetrics(usedBytes, heapBytes float64) {}
func (n *NoOpMetricsCollector) RecordGCPause(duration float64)                   {}

// System resource metrics
func (n *NoOpMetricsCollector) UpdateGoroutines(count float64)      {}
func (n *NoOpMetricsCollector) UpdateFileDescriptors(count float64) {}

// Error metrics
func (n *NoOpMetricsCollector) RecordConnectionError(errorType string)        {}
func (n *NoOpMetricsCollector) RecordChannelError(errorType string)           {}
func (n *NoOpMetricsCollector) RecordMessageDropped(queueName, reason string) {}

// Spec 06 broker outage telemetry
func (n *NoOpMetricsCollector) RecordRecoveryFault(stage, outcome string)            {}
func (n *NoOpMetricsCollector) RecordRecoveryDiscardedRecords(stage string, cnt int) {}
func (n *NoOpMetricsCollector) RecordRecoveryDuration(seconds float64)               {}
func (n *NoOpMetricsCollector) SetRecoveryMessagesRecovered(count int)               {}
func (n *NoOpMetricsCollector) UpdateWALFileCount(count int)                         {}
func (n *NoOpMetricsCollector) RecordPublishRefused(reason string)                   {}
func (n *NoOpMetricsCollector) RecordConsumerRegisterError()                         {}
func (n *NoOpMetricsCollector) RecordDepthGateArmed()                                {}
func (n *NoOpMetricsCollector) RecordDeliverySendError()                             {}
func (n *NoOpMetricsCollector) RecordDeliveriesRequeued(reason string, cnt int)      {}
func (n *NoOpMetricsCollector) RecordDeliveriesDropped(reason string, cnt int)       {}
func (n *NoOpMetricsCollector) RecordReaderPaused()                                  {}
func (n *NoOpMetricsCollector) RecordConnectionClosedByCause(cause string)           {}
func (n *NoOpMetricsCollector) RecordShutdown(result string, seconds float64)        {}
func (n *NoOpMetricsCollector) RecordShutdownConnectionForced()                      {}
func (n *NoOpMetricsCollector) RecordStorageCloseError()                             {}
func (n *NoOpMetricsCollector) RecordResourceAlarmTrip(resource string)              {}
func (n *NoOpMetricsCollector) RecordPanic(component string)                         {}
