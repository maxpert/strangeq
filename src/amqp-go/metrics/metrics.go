package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector holds all Prometheus metrics for the AMQP server.
//
// Cardinality: per-queue series = 4 gauges (ready, unacknowledged, total,
// consumers) plus message_age when wired, times the number of queues. The
// queue poller caps per-queue series at maxQueueSeries (2000) and keeps
// publishing only the authoritative queues_total above that; segment, ring
// and dropped series carry no vhost label because the broker is not
// vhost-scoped yet (the label would be the constant "/").
type Collector struct {
	// Connection metrics
	ConnectionsTotal   prometheus.Gauge
	ConnectionsCreated prometheus.Counter
	ConnectionsClosed  prometheus.Counter

	// Channel metrics
	ChannelsTotal   prometheus.Gauge
	ChannelsCreated prometheus.Counter
	ChannelsClosed  prometheus.Counter

	// Queue metrics
	QueuesTotal          prometheus.Gauge
	QueuesDeclared       prometheus.Counter
	QueuesDeleted        prometheus.Counter
	QueueMessagesReady   *prometheus.GaugeVec
	QueueMessagesUnacked *prometheus.GaugeVec
	QueueMessagesTotal   *prometheus.GaugeVec
	QueueConsumers       *prometheus.GaugeVec

	// Exchange metrics
	ExchangesTotal    prometheus.Gauge
	ExchangesDeclared prometheus.Counter
	ExchangesDeleted  prometheus.Counter

	// Message metrics
	MessagesPublished      prometheus.Counter
	MessagesPublishedBytes prometheus.Counter
	MessagesDelivered      prometheus.Counter
	MessagesDeliveredBytes prometheus.Counter
	MessagesAcknowledged   prometheus.Counter
	MessagesRedelivered    prometheus.Counter
	MessagesRejected       prometheus.Counter
	MessagesUnroutable     prometheus.Counter

	// Consumer metrics
	ConsumersTotal prometheus.Gauge

	// Transaction metrics
	TransactionsStarted    prometheus.Counter
	TransactionsCommitted  prometheus.Counter
	TransactionsRolledback prometheus.Counter

	// Server metrics
	ServerUptime prometheus.Gauge

	// UnsafeRecoveryArtifacts is pinned for the lifetime of the process to the
	// number of artifacts recovery DISCARDED because --unsafe-recovery was
	// set. 0 on a clean boot. Alert on > 0.
	UnsafeRecoveryArtifacts prometheus.Gauge

	// Latency metrics
	MessagePublishDuration  prometheus.Histogram
	MessageDeliveryDuration prometheus.Histogram
	MessageAge              *prometheus.GaugeVec

	// Disk metrics
	DiskFreeBytes  prometheus.Gauge
	DiskUsageBytes prometheus.Gauge

	// WAL metrics
	WALSizeBytes        prometheus.Gauge
	WALWriteTotal       prometheus.Counter
	WALFsyncTotal       prometheus.Counter
	WALFsyncDuration    prometheus.Histogram
	WALWriteErrorsTotal prometheus.Counter

	// Segment metrics
	SegmentCount           *prometheus.GaugeVec
	SegmentSizeBytes       *prometheus.GaugeVec
	SegmentCompactionTotal prometheus.Counter
	// SegmentCompactionFailuresTotal counts segments WITHDRAWN from compaction
	// because their bytes cannot be rewritten. It is monotonic and bounded by
	// the number of damaged segments, not by the number of ticks.
	SegmentCompactionFailuresTotal prometheus.Counter
	SegmentReadErrorsTotal         prometheus.Counter

	// Ring buffer metrics
	RingBufferUtilization *prometheus.GaugeVec

	// Memory metrics
	MemoryUsedBytes prometheus.Gauge
	MemoryHeapBytes prometheus.Gauge
	MemoryGCPause   prometheus.Summary

	// System resource metrics
	GoroutinesTotal     prometheus.Gauge
	FileDescriptorsOpen prometheus.Gauge

	// Error metrics
	ConnectionErrorsTotal *prometheus.CounterVec
	ChannelErrorsTotal    *prometheus.CounterVec
	MessagesDroppedTotal  *prometheus.CounterVec

	// AuthFailuresTotal counts refused authentication attempts by reason:
	// host_blocked, host_not_allowed, user_blocked, user_not_allowed,
	// rate_limited, bad_credentials, unsupported_mechanism.
	AuthFailuresTotal *prometheus.CounterVec

	// ResourceAlarmActive is 1 while the named resource alarm is tripped.
	ResourceAlarmActive *prometheus.GaugeVec

	// BuildInfo is the constant-1 build identity gauge.
	BuildInfo *prometheus.GaugeVec

	// WS-I Phase B publish-lease metrics (amqp_publish_lease_*).
	PublishLeaseUsedBytes             prometheus.Gauge
	PublishLeaseBudgetBytes           prometheus.Gauge
	PublishLeaseWaiters               prometheus.Gauge
	PublishLeaseWaitsTotal            prometheus.Counter
	PublishLeaseOversizedTotal        prometheus.Counter
	PublishLeaseOverbudgetClosesTotal prometheus.Counter
	PublishLeaseBodyStallClosesTotal  prometheus.Counter
	PublishLeaseLeakedTotal           prometheus.Counter
	PublishLeaseWaitSeconds           prometheus.Histogram
}

// NewCollector creates a new metrics collector with all Prometheus metrics
func NewCollector(namespace string) *Collector {
	if namespace == "" {
		namespace = "amqp"
	}

	return &Collector{
		// Connection metrics
		ConnectionsTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connections_total",
			Help:      "Current number of active connections",
		}),
		ConnectionsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_created_total",
			Help:      "Total number of connections created since server start",
		}),
		ConnectionsClosed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_closed_total",
			Help:      "Total number of connections closed since server start",
		}),

		// Channel metrics
		ChannelsTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "channels_total",
			Help:      "Current number of active channels",
		}),
		ChannelsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "channels_created_total",
			Help:      "Total number of channels created since server start",
		}),
		ChannelsClosed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "channels_closed_total",
			Help:      "Total number of channels closed since server start",
		}),

		// Queue metrics
		QueuesTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queues_total",
			Help:      "Current number of queues",
		}),
		QueuesDeclared: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queues_declared_total",
			Help:      "Total number of queues declared since server start",
		}),
		QueuesDeleted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queues_deleted_total",
			Help:      "Total number of queues deleted since server start",
		}),
		QueueMessagesReady: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_messages_ready",
			Help:      "Number of messages ready to be delivered in queue",
		}, []string{"queue", "vhost"}),
		QueueMessagesUnacked: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_messages_unacknowledged",
			Help:      "Number of messages delivered but not yet acknowledged in queue",
		}, []string{"queue", "vhost"}),
		QueueMessagesTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_messages_total",
			Help:      "Total number of messages in queue (ready + unacknowledged)",
		}, []string{"queue", "vhost"}),
		QueueConsumers: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_consumers",
			Help:      "Number of consumers on queue",
		}, []string{"queue", "vhost"}),

		// Exchange metrics
		ExchangesTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exchanges_total",
			Help:      "Current number of exchanges",
		}),
		ExchangesDeclared: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exchanges_declared_total",
			Help:      "Total number of exchanges declared since server start",
		}),
		ExchangesDeleted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exchanges_deleted_total",
			Help:      "Total number of exchanges deleted since server start",
		}),

		// Message metrics
		MessagesPublished: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_total",
			Help:      "Total number of messages published since server start",
		}),
		MessagesPublishedBytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_bytes_total",
			Help:      "Total bytes of messages published since server start",
		}),
		MessagesDelivered: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_delivered_total",
			Help:      "Total number of messages delivered to consumers since server start",
		}),
		MessagesDeliveredBytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_delivered_bytes_total",
			Help:      "Total bytes of messages delivered to consumers since server start",
		}),
		MessagesAcknowledged: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_acknowledged_total",
			Help:      "Total number of messages acknowledged since server start",
		}),
		MessagesRedelivered: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_redelivered_total",
			Help:      "Total number of messages redelivered since server start",
		}),
		MessagesRejected: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_rejected_total",
			Help:      "Total number of messages rejected since server start",
		}),
		MessagesUnroutable: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_unroutable_total",
			Help:      "Total number of unroutable messages since server start",
		}),

		// Consumer metrics
		ConsumersTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumers_total",
			Help:      "Current number of active consumers",
		}),

		// Transaction metrics
		TransactionsStarted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transactions_started_total",
			Help:      "Total number of transactions started since server start",
		}),
		TransactionsCommitted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transactions_committed_total",
			Help:      "Total number of transactions committed since server start",
		}),
		TransactionsRolledback: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transactions_rolledback_total",
			Help:      "Total number of transactions rolled back since server start",
		}),

		// Server metrics
		ServerUptime: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "server_uptime_seconds",
			Help:      "Server uptime in seconds",
		}),

		UnsafeRecoveryArtifacts: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "unsafe_recovery_discarded_artifacts",
			Help:      "Number of data artifacts discarded at boot because --unsafe-recovery was set; 0 on a clean boot. Non-zero means this broker is serving from data it could not fully recover.",
		}),

		// Latency metrics
		MessagePublishDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "message_publish_duration_seconds",
			Help:      "Time taken to publish a message",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3s
		}),
		MessageDeliveryDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "message_delivery_duration_seconds",
			Help:      "Time taken to deliver a message to consumer",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3s
		}),
		MessageAge: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "message_age_seconds",
			Help:      "Age of oldest message in queue",
		}, []string{"queue", "vhost"}),

		// Disk metrics
		DiskFreeBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "disk_free_bytes",
			Help:      "Free disk space in data directory",
		}),
		DiskUsageBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "disk_usage_bytes",
			Help:      "Total disk used by server data",
		}),

		// WAL metrics
		WALSizeBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "wal_size_bytes",
			Help:      "Total size of WAL files",
		}),
		WALWriteTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "wal_write_total",
			Help:      "Total number of messages written to WAL",
		}),
		WALFsyncTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "wal_fsync_total",
			Help:      "Total number of fsync operations on WAL",
		}),
		WALFsyncDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "wal_fsync_duration_seconds",
			Help:      "Time taken for WAL fsync operations",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3s
		}),
		WALWriteErrorsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "wal_write_errors_total",
			Help:      "Total number of WAL write failures",
		}),

		// Segment metrics
		SegmentCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "segment_count",
			Help:      "Number of segment files per queue",
		}, []string{"queue"}),
		SegmentSizeBytes: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "segment_size_bytes",
			Help:      "Total size of segment files per queue",
		}, []string{"queue"}),
		SegmentCompactionTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "segment_compaction_total",
			Help:      "Total number of segment compactions performed",
		}),
		SegmentCompactionFailuresTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "segment_compaction_failures_total",
			Help:      "Segments withdrawn from compaction because they cannot be rewritten (no data is lost; acked records are not reclaimed)",
		}),
		SegmentReadErrorsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "segment_read_errors_total",
			Help:      "Total number of segment read failures",
		}),

		// Ring buffer metrics
		RingBufferUtilization: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ring_buffer_utilization_ratio",
			Help:      "Ring buffer utilization ratio (0-1)",
		}, []string{"queue"}),

		// Memory metrics
		MemoryUsedBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "memory_used_bytes",
			Help:      "Total memory used by Go runtime",
		}),
		MemoryHeapBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "memory_heap_bytes",
			Help:      "Heap memory in use",
		}),
		MemoryGCPause: promauto.NewSummary(prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "memory_gc_pause_seconds",
			Help:      "GC pause duration",
			Objectives: map[float64]float64{
				0.5:  0.05,  // 50th percentile with 5% error
				0.9:  0.01,  // 90th percentile with 1% error
				0.99: 0.001, // 99th percentile with 0.1% error
			},
		}),

		// System resource metrics
		GoroutinesTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "goroutines_total",
			Help:      "Number of active goroutines",
		}),
		FileDescriptorsOpen: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "file_descriptors_open",
			Help:      "Number of open file descriptors",
		}),

		// Error metrics
		ConnectionErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connection_errors_total",
			Help:      "Total number of connection errors by type",
		}, []string{"error_type"}),
		ChannelErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "channel_errors_total",
			Help:      "Total number of channel errors by type",
		}, []string{"error_type"}),
		MessagesDroppedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_dropped_total",
			Help:      "Total number of messages dropped by reason",
		}, []string{"queue", "reason"}),

		AuthFailuresTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "auth_failures_total",
			Help:      "Total number of refused authentication attempts by reason",
		}, []string{"reason"}),

		ResourceAlarmActive: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "resource_alarm_active",
			Help:      "1 while the named resource alarm (memory|disk) is tripped",
		}, []string{"resource"}),

		BuildInfo: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "build_info",
			Help:      "Build identity; the value is always 1",
		}, []string{"version", "commit", "go_version"}),

		PublishLeaseUsedBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "publish_lease_used_bytes",
			Help:      "Declared publish body bytes (+framing) currently leased (WS-I)",
		}),
		PublishLeaseBudgetBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "publish_lease_budget_bytes",
			Help:      "Publish-inflight lease budget G (WS-I)",
		}),
		PublishLeaseWaiters: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "publish_lease_waiters",
			Help:      "Connections currently queued for a publish lease (WS-I)",
		}),
		PublishLeaseWaitsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_lease_waits_total",
			Help:      "Blocking publish-lease waits (WS-I; diagnostic, never gated)",
		}),
		PublishLeaseOversizedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_lease_oversized_total",
			Help:      "Oversized (>G) publish leases granted at used==0 (WS-I)",
		}),
		PublishLeaseOverbudgetClosesTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_lease_overbudget_closes_total",
			Help:      "Connections closed with 506 because a nested header could not be leased (WS-I)",
		}),
		PublishLeaseBodyStallClosesTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_lease_body_stall_closes_total",
			Help:      "Connections closed for no leased-body progress within writeTimeout (WS-I)",
		}),
		PublishLeaseLeakedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_lease_leaked_total",
			Help:      "Publish leases recovered by PutPendingMessage instead of a proper release (WS-I; must stay 0)",
		}),
		PublishLeaseWaitSeconds: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "publish_lease_wait_seconds",
			Help:      "Time spent blocked waiting for a publish lease (WS-I)",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 7),
		}),
	}
}

// RecordPublishLeaseWait observes one completed blocking lease wait.
func (c *Collector) RecordPublishLeaseWait(seconds float64) {
	c.PublishLeaseWaitsTotal.Inc()
	c.PublishLeaseWaitSeconds.Observe(seconds)
}

// RecordPublishLeaseOversized counts one oversized (>G) lease grant.
func (c *Collector) RecordPublishLeaseOversized() { c.PublishLeaseOversizedTotal.Inc() }

// RecordPublishLeaseOverbudgetClose counts one 506 lease-budget close.
func (c *Collector) RecordPublishLeaseOverbudgetClose() { c.PublishLeaseOverbudgetClosesTotal.Inc() }

// RecordPublishLeaseBodyStallClose counts one leased-body-stall close.
func (c *Collector) RecordPublishLeaseBodyStallClose() { c.PublishLeaseBodyStallClosesTotal.Inc() }

// RecordPublishLeaseLeaked counts one lease recovered by PutPendingMessage.
func (c *Collector) RecordPublishLeaseLeaked() { c.PublishLeaseLeakedTotal.Inc() }

// UpdatePublishLeaseGauges publishes the current lease gauges.
func (c *Collector) UpdatePublishLeaseGauges(usedBytes, budgetBytes int64, waiters int) {
	c.PublishLeaseUsedBytes.Set(float64(usedBytes))
	c.PublishLeaseBudgetBytes.Set(float64(budgetBytes))
	c.PublishLeaseWaiters.Set(float64(waiters))
}

// RecordAuthFailure increments the failed-authentication counter for reason.
func (c *Collector) RecordAuthFailure(reason string) {
	c.AuthFailuresTotal.WithLabelValues(reason).Inc()
}

// SetBuildInfo publishes the constant-1 build identity gauge.
func (c *Collector) SetBuildInfo(version, commit, goVersion string) {
	c.BuildInfo.WithLabelValues(version, commit, goVersion).Set(1)
}

// UpdateQueueReadyAndConsumers sets the ready depth (only when known) and the
// consumer count for one queue. Unacked/total are deliberately not written:
// no cheap unacked accessor exists, and total = ready + 0 would mislead.
func (c *Collector) UpdateQueueReadyAndConsumers(queue, vhost string, ready int, readyKnown bool, consumers int) {
	labels := prometheus.Labels{"queue": queue, "vhost": vhost}
	if readyKnown {
		c.QueueMessagesReady.With(labels).Set(float64(ready))
	}
	c.QueueConsumers.With(labels).Set(float64(consumers))
}

// RecordConnectionCreated increments connection creation counter and total
func (c *Collector) RecordConnectionCreated() {
	c.ConnectionsCreated.Inc()
	c.ConnectionsTotal.Inc()
}

// RecordConnectionClosed increments connection close counter and decrements total
func (c *Collector) RecordConnectionClosed() {
	c.ConnectionsClosed.Inc()
	c.ConnectionsTotal.Dec()
}

// RecordChannelCreated increments channel creation counter and total
func (c *Collector) RecordChannelCreated() {
	c.ChannelsCreated.Inc()
	c.ChannelsTotal.Inc()
}

// RecordChannelClosed increments channel close counter and decrements total
func (c *Collector) RecordChannelClosed() {
	c.ChannelsClosed.Inc()
	c.ChannelsTotal.Dec()
}

// RecordQueueDeclared increments queue declaration counter and total
func (c *Collector) RecordQueueDeclared() {
	c.QueuesDeclared.Inc()
	c.QueuesTotal.Inc()
}

// RecordQueueDeleted increments queue deletion counter and decrements total
func (c *Collector) RecordQueueDeleted() {
	c.QueuesDeleted.Inc()
	c.QueuesTotal.Dec()
}

// RecordExchangeDeclared increments exchange declaration counter and total
func (c *Collector) RecordExchangeDeclared() {
	c.ExchangesDeclared.Inc()
	c.ExchangesTotal.Inc()
}

// RecordExchangeDeleted increments exchange deletion counter and decrements total
func (c *Collector) RecordExchangeDeleted() {
	c.ExchangesDeleted.Inc()
	c.ExchangesTotal.Dec()
}

// RecordMessagePublished records a published message
func (c *Collector) RecordMessagePublished(size int) {
	c.MessagesPublished.Inc()
	c.MessagesPublishedBytes.Add(float64(size))
}

// RecordMessageDelivered records a delivered message
func (c *Collector) RecordMessageDelivered(size int) {
	c.MessagesDelivered.Inc()
	c.MessagesDeliveredBytes.Add(float64(size))
}

// RecordMessageAcknowledged records an acknowledged message
func (c *Collector) RecordMessageAcknowledged() {
	c.MessagesAcknowledged.Inc()
}

// RecordMessageRedelivered records a redelivered message
func (c *Collector) RecordMessageRedelivered() {
	c.MessagesRedelivered.Inc()
}

// RecordMessageRejected records a rejected message
func (c *Collector) RecordMessageRejected() {
	c.MessagesRejected.Inc()
}

// RecordMessageUnroutable records an unroutable message
func (c *Collector) RecordMessageUnroutable() {
	c.MessagesUnroutable.Inc()
}

// UpdateQueueMetrics updates all metrics for a specific queue
func (c *Collector) UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int) {
	labels := prometheus.Labels{"queue": queueName, "vhost": vhost}
	c.QueueMessagesReady.With(labels).Set(float64(ready))
	c.QueueMessagesUnacked.With(labels).Set(float64(unacked))
	c.QueueMessagesTotal.With(labels).Set(float64(ready + unacked))
	c.QueueConsumers.With(labels).Set(float64(consumers))
}

// DeleteQueueMetrics removes metrics for a deleted queue
func (c *Collector) DeleteQueueMetrics(queueName, vhost string) {
	labels := prometheus.Labels{"queue": queueName, "vhost": vhost}
	c.QueueMessagesReady.Delete(labels)
	c.QueueMessagesUnacked.Delete(labels)
	c.QueueMessagesTotal.Delete(labels)
	c.QueueConsumers.Delete(labels)
}

// SetConsumersTotal sets the total number of consumers
func (c *Collector) SetConsumersTotal(count int) {
	c.ConsumersTotal.Set(float64(count))
}

// RecordTransactionStarted increments transaction started counter
func (c *Collector) RecordTransactionStarted() {
	c.TransactionsStarted.Inc()
}

// RecordTransactionCommitted increments transaction committed counter
func (c *Collector) RecordTransactionCommitted() {
	c.TransactionsCommitted.Inc()
}

// RecordTransactionRolledback increments transaction rolled back counter
func (c *Collector) RecordTransactionRolledback() {
	c.TransactionsRolledback.Inc()
}

// UpdateServerUptime updates the server uptime metric
func (c *Collector) UpdateServerUptime(seconds float64) {
	c.ServerUptime.Set(seconds)
}

// SetUnsafeRecoveryArtifacts pins the degraded-boot gauge for the lifetime of
// the process. See MetricsCollector.SetUnsafeRecoveryArtifacts.
func (c *Collector) SetUnsafeRecoveryArtifacts(count int) {
	c.UnsafeRecoveryArtifacts.Set(float64(count))
}

// RecordPublishLatency records the time taken to publish a message
func (c *Collector) RecordPublishLatency(duration float64) {
	c.MessagePublishDuration.Observe(duration)
}

// RecordDeliveryLatency records the time taken to deliver a message
func (c *Collector) RecordDeliveryLatency(duration float64) {
	c.MessageDeliveryDuration.Observe(duration)
}

// UpdateMessageAge updates the age of the oldest message in a queue
func (c *Collector) UpdateMessageAge(queueName, vhost string, ageSeconds float64) {
	labels := prometheus.Labels{"queue": queueName, "vhost": vhost}
	c.MessageAge.With(labels).Set(ageSeconds)
}

// DeleteMessageAge removes message age metric for a deleted queue
func (c *Collector) DeleteMessageAge(queueName, vhost string) {
	labels := prometheus.Labels{"queue": queueName, "vhost": vhost}
	c.MessageAge.Delete(labels)
}

// UpdateDiskMetrics updates disk usage metrics
func (c *Collector) UpdateDiskMetrics(freeBytes, usedBytes float64) {
	c.DiskFreeBytes.Set(freeBytes)
	c.DiskUsageBytes.Set(usedBytes)
}

// UpdateWALSize updates the total WAL file size
func (c *Collector) UpdateWALSize(bytes float64) {
	c.WALSizeBytes.Set(bytes)
}

// RecordWALWrite records a write to the WAL
func (c *Collector) RecordWALWrite() {
	c.WALWriteTotal.Inc()
}

// RecordWALFsync records an fsync operation
func (c *Collector) RecordWALFsync(duration float64) {
	c.WALFsyncTotal.Inc()
	c.WALFsyncDuration.Observe(duration)
}

// RecordWALWriteError records a WAL write error
func (c *Collector) RecordWALWriteError() {
	c.WALWriteErrorsTotal.Inc()
}

// UpdateSegmentMetrics updates segment count and size for a queue
func (c *Collector) UpdateSegmentMetrics(queueName string, count, sizeBytes float64) {
	c.SegmentCount.With(prometheus.Labels{"queue": queueName}).Set(count)
	c.SegmentSizeBytes.With(prometheus.Labels{"queue": queueName}).Set(sizeBytes)
}

// DeleteSegmentMetrics removes segment metrics for a deleted queue
func (c *Collector) DeleteSegmentMetrics(queueName string) {
	c.SegmentCount.Delete(prometheus.Labels{"queue": queueName})
	c.SegmentSizeBytes.Delete(prometheus.Labels{"queue": queueName})
}

// RecordSegmentCompaction records a segment compaction
func (c *Collector) RecordSegmentCompaction() {
	c.SegmentCompactionTotal.Inc()
}

// RecordSegmentCompactionFailure records a segment withdrawn from compaction
func (c *Collector) RecordSegmentCompactionFailure() {
	c.SegmentCompactionFailuresTotal.Inc()
}

// RecordSegmentReadError records a segment read error
func (c *Collector) RecordSegmentReadError() {
	c.SegmentReadErrorsTotal.Inc()
}

// UpdateRingBufferUtilization updates ring buffer utilization for a queue
func (c *Collector) UpdateRingBufferUtilization(queueName string, utilization float64) {
	c.RingBufferUtilization.With(prometheus.Labels{"queue": queueName}).Set(utilization)
}

// DeleteRingBufferUtilization removes ring buffer metric for a deleted queue
func (c *Collector) DeleteRingBufferUtilization(queueName string) {
	c.RingBufferUtilization.Delete(prometheus.Labels{"queue": queueName})
}

// UpdateMemoryMetrics updates memory usage metrics
func (c *Collector) UpdateMemoryMetrics(usedBytes, heapBytes float64) {
	c.MemoryUsedBytes.Set(usedBytes)
	c.MemoryHeapBytes.Set(heapBytes)
}

// RecordGCPause records a GC pause duration
func (c *Collector) RecordGCPause(duration float64) {
	c.MemoryGCPause.Observe(duration)
}

// UpdateGoroutines updates the goroutine count
func (c *Collector) UpdateGoroutines(count float64) {
	c.GoroutinesTotal.Set(count)
}

// UpdateFileDescriptors updates the file descriptor count
func (c *Collector) UpdateFileDescriptors(count float64) {
	c.FileDescriptorsOpen.Set(count)
}

// RecordConnectionError records a connection error
func (c *Collector) RecordConnectionError(errorType string) {
	c.ConnectionErrorsTotal.With(prometheus.Labels{"error_type": errorType}).Inc()
}

// RecordChannelError records a channel error
func (c *Collector) RecordChannelError(errorType string) {
	c.ChannelErrorsTotal.With(prometheus.Labels{"error_type": errorType}).Inc()
}

// RecordMessageDropped records a dropped message
func (c *Collector) RecordMessageDropped(queueName, reason string) {
	labels := prometheus.Labels{"queue": queueName, "reason": reason}
	c.MessagesDroppedTotal.With(labels).Inc()
}
