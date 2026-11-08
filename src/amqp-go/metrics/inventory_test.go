package metrics

import (
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// primeForInventory touches ONE child of every label-bearing Vec and every
// scalar/gauge/histogram family so a family that has no series yet still
// appears in the gather output. Test #24 of spec 06 §5 pins REGISTRATION, not
// just emission: without this a zero-child CounterVec is invisible to Gather
// and a deletion would go unnoticed.
func primeForInventory(c *Collector) {
	// Base families with labels.
	c.UpdateQueueReadyAndConsumers("q", "/", 1, true, 1, true, 1)
	c.QueueDepthHighWater.WithLabelValues("q", "/").Set(1)
	c.RecordWALFullFlush("group_commit")
	c.UpdateSegmentMetrics("q", 1, 1)
	c.RecordConnectionError("read")
	c.RecordChannelError("x")
	c.RecordMessageDropped("q", "consumer_removed")
	c.RecordAuthFailure("bad_credentials")
	c.ResourceAlarmActive.WithLabelValues("memory").Set(1)
	c.SetBuildInfo("v", "c", "g")
	c.RecordHotCacheDeliveryHit("q")
	c.RecordHotCacheDeliveryColdRead("q")
	c.RecordHotCacheDeliveryPread("q", 1)

	// Spec 06 §3.2–§3.5 families with labels.
	c.RecordRecoveryFault("queue-recovery", "benign")
	c.RecordRecoveryDiscardedRecords("queue-recovery", 1)
	c.RecordRecoveryDuration(0.1)
	c.SetRecoveryMessagesRecovered(1)
	c.RecordPublishRefused("x_max_length")
	c.RecordDeliveriesRequeued("write_error", 1)
	c.RecordDeliveriesDropped("consumer_removed", 1)
	c.RecordConnectionClosedByCause("client_eof")
	c.RecordShutdown("clean", 0.1)
	c.RecordResourceAlarmTrip("memory")
	c.RecordPanic("reader")

	// Management families (registered lazily): touch one child of each Vec.
	c.RegisterManagement()
	c.ManagementRequests("/api/overview", "GET", 2)
	c.ManagementDuration("/api/overview", 0.1)
	c.ManagementSessions(1)
	c.ManagementAuthFailure("bad_credentials")
	c.ManagementSnapshotPartial("/api/overview")
	c.ManagementSnapshotDuration("/api/overview", 0.1)
	c.ManagementSnapshotLockWait(0.1)
	c.ManagementForbiddenLock("Server.Mutex")
	c.ManagementJobs("queued", 1)
	c.ManagementCapabilityDenied("capView")
	c.ManagementUIRequests(2)
	c.ManagementPanic("/api/overview")
}

// gatheredFamilyNames returns the sorted family names on a registry.
func gatheredFamilyNames(t *testing.T, g prometheus.Gatherer) []string {
	t.Helper()
	families, err := g.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	sort.Strings(names)
	return names
}

// TestMetricInventory_PinsFullSeriesSet is spec 06 §5 test #24, the "a metric
// cannot silently disappear" gate. It pins the FULL amqp_* family set gathered
// from a PRIVATE registry after a prime pass that touches every public
// recorder. Any removal, rename, or new family without a matching spec edit
// fails it.
//
// DELIBERATE DIVERGENCE FROM THE SPEC'S 47-FAMILY TALLY: spec 06 §3.3 names
// amqp_segment_checkpoint_errors_total, but the segstore-v1 rework removed the
// WAL->segment checkpoint COPY path (performCheckpoint is now a no-op; there is
// no CheckpointBatch). No surviving code path can ever increment it, so
// registering it would be exactly the "permanently-zero series" the spec's
// §3.6 forbids. It is therefore NOT registered; the segment durability signal
// lives in segment_fsync_errors_total and segment_roll_errors_total, which DO
// have live sites. See the final report.
func TestMetricInventory_PinsFullSeriesSet(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := newCollector(reg, "amqp")
	primeForInventory(c)

	got := gatheredFamilyNames(t, reg)

	want := append([]string(nil), amqpFamilyGolden...)
	sort.Strings(want)

	require.Equal(t, want, got,
		"the gathered amqp_* family set must equal the pinned golden set; a "+
			"removed, renamed or new family must be reflected in the spec and "+
			"this list")
}

// amqpFamilyGolden is the pinned set of amqp_* families, shared with the
// Grafana dashboard gate (grafana_dashboard_test.go): every family a dashboard
// query names must be in this set, or the dashboard is plotting a series that
// this broker can never emit.
var amqpFamilyGolden = []string{
	// --- base families (77) ---
	"amqp_auth_failures_total",
	"amqp_build_info",
	"amqp_channel_errors_total",
	"amqp_channels_closed_total",
	"amqp_channels_created_total",
	"amqp_channels_total",
	"amqp_connection_errors_total",
	"amqp_connections_closed_total",
	"amqp_connections_created_total",
	"amqp_connections_total",
	"amqp_consumers_total",
	"amqp_disk_free_bytes",
	"amqp_disk_usage_bytes",
	"amqp_exchanges_declared_total",
	"amqp_exchanges_deleted_total",
	"amqp_exchanges_total",
	"amqp_file_descriptors_open",
	"amqp_goroutines_total",
	"amqp_hot_cache_delivery_cold_reads_total",
	"amqp_hot_cache_delivery_hits_total",
	"amqp_hot_cache_delivery_preads_total",
	"amqp_memory_gc_pause_seconds",
	"amqp_memory_heap_bytes",
	"amqp_memory_used_bytes",
	"amqp_message_delivery_duration_seconds",
	"amqp_message_publish_duration_seconds",
	"amqp_messages_acknowledged_total",
	"amqp_messages_delivered_bytes_total",
	"amqp_messages_delivered_total",
	"amqp_messages_dropped_total",
	"amqp_messages_published_bytes_total",
	"amqp_messages_published_total",
	"amqp_messages_redelivered_total",
	"amqp_messages_rejected_total",
	"amqp_messages_unroutable_total",
	"amqp_publish_lease_body_stall_closes_total",
	"amqp_publish_lease_budget_bytes",
	"amqp_publish_lease_leaked_total",
	"amqp_publish_lease_overbudget_closes_total",
	"amqp_publish_lease_oversized_total",
	"amqp_publish_lease_used_bytes",
	"amqp_publish_lease_waits_total",
	"amqp_publish_lease_wait_seconds",
	"amqp_publish_lease_waiters",
	"amqp_publisher_park_duration_seconds",
	"amqp_publisher_park_seconds_total",
	"amqp_publisher_parked_current",
	"amqp_publisher_parked_total",
	"amqp_queue_consumers",
	"amqp_queue_depth_high_water",
	"amqp_queue_messages_ready",
	"amqp_queue_messages_total",
	"amqp_queue_messages_unacknowledged",
	"amqp_queues_declared_total",
	"amqp_queues_deleted_total",
	"amqp_queues_total",
	"amqp_resource_alarm_active",
	"amqp_segment_compaction_failures_total",
	"amqp_segment_compaction_total",
	"amqp_segment_count",
	"amqp_segment_read_errors_total",
	"amqp_segment_size_bytes",
	"amqp_server_uptime_seconds",
	"amqp_transactions_committed_total",
	"amqp_transactions_rolledback_total",
	"amqp_transactions_started_total",
	"amqp_unsafe_recovery_discarded_artifacts",
	"amqp_wal_fsync_duration_seconds",
	"amqp_wal_fsync_total",
	"amqp_wal_full_flush_total",
	"amqp_wal_index_copies",
	"amqp_wal_inventory_records",
	"amqp_wal_settle_drained",
	"amqp_wal_size_bytes",
	"amqp_wal_write_errors_total",
	"amqp_wal_write_total",

	// --- spec 06 §3.2 recovery (4) ---
	"amqp_recovery_discarded_records_total",
	"amqp_recovery_duration_seconds",
	"amqp_recovery_faults_total",
	"amqp_recovery_messages_recovered",

	// --- spec 06 §3.3 WAL / segment (7; checkpoint omitted, see doc) ---
	"amqp_segment_fsync_errors_total",
	"amqp_segment_roll_errors_total",
	"amqp_wal_compaction_errors_total",
	"amqp_wal_files",
	"amqp_wal_fsync_errors_total",
	"amqp_wal_reclaim_fsync_errors_total",
	"amqp_wal_roll_errors_total",

	// --- spec 06 §3.4 publish / backpressure / delivery (10) ---
	"amqp_consumer_register_errors_total",
	"amqp_deliveries_dropped_total",
	"amqp_deliveries_requeued_total",
	"amqp_delivery_send_errors_total",
	"amqp_depth_gate_armed_total",
	"amqp_depth_gated_current",
	"amqp_messages_inflight",
	"amqp_publish_refused_total",
	"amqp_reader_paused_current",
	"amqp_reader_paused_total",

	// --- spec 06 §3.5 connections / shutdown / alarms / panics (7) ---
	"amqp_connections_closed_by_cause_total",
	"amqp_panics_total",
	"amqp_resource_alarm_trips_total",
	"amqp_shutdown_connections_forced_total",
	"amqp_shutdown_duration_seconds",
	"amqp_shutdown_total",
	"amqp_storage_close_errors_total",

	// --- management families (17) ---
	"amqp_management_audit_dropped_total",
	"amqp_management_auth_failures_total",
	"amqp_management_capability_denied_total",
	"amqp_management_forbidden_lock_acquired_total",
	"amqp_management_in_flight",
	"amqp_management_insecure_bind",
	"amqp_management_jobs",
	"amqp_management_panics_total",
	"amqp_management_publish_backpressure_total",
	"amqp_management_rate_limited_total",
	"amqp_management_request_duration_seconds",
	"amqp_management_requests_total",
	"amqp_management_sessions",
	"amqp_management_snapshot_duration_seconds",
	"amqp_management_snapshot_lock_wait_seconds",
	"amqp_management_snapshot_partial_total",
	"amqp_management_ui_requests_total",
	"amqp_management_unauthenticated",
}
