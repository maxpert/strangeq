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
	RecordMessageRedelivered()
	RecordMessageRejected()
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
	UpdateMessageAge(queueName, vhost string, ageSeconds float64)
	DeleteMessageAge(queueName, vhost string)

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

	// Ring buffer metrics
	UpdateRingBufferUtilization(queueName string, utilization float64)
	DeleteRingBufferUtilization(queueName string)

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
func (n *NoOpMetricsCollector) RecordMessageRedelivered()                  {}
func (n *NoOpMetricsCollector) RecordMessageRejected()                     {}
func (n *NoOpMetricsCollector) RecordMessageUnroutable()                   {}
func (n *NoOpMetricsCollector) SetConsumersTotal(count int)                {}
func (n *NoOpMetricsCollector) RecordTransactionStarted()                  {}
func (n *NoOpMetricsCollector) RecordTransactionCommitted()                {}
func (n *NoOpMetricsCollector) RecordTransactionRolledback()               {}
func (n *NoOpMetricsCollector) UpdateServerUptime(seconds float64)         {}
func (n *NoOpMetricsCollector) SetUnsafeRecoveryArtifacts(count int)       {}

// Latency metrics
func (n *NoOpMetricsCollector) RecordPublishLatency(duration float64)                        {}
func (n *NoOpMetricsCollector) RecordDeliveryLatency(duration float64)                       {}
func (n *NoOpMetricsCollector) UpdateMessageAge(queueName, vhost string, ageSeconds float64) {}
func (n *NoOpMetricsCollector) DeleteMessageAge(queueName, vhost string)                     {}

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

// Ring buffer metrics
func (n *NoOpMetricsCollector) UpdateRingBufferUtilization(queueName string, utilization float64) {}
func (n *NoOpMetricsCollector) DeleteRingBufferUtilization(queueName string)                      {}

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
