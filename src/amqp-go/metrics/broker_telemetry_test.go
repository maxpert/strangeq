package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A fresh private-registry collector per test avoids the process-global
// duplicate-registration panic and keeps the "amqp" namespace stable.
func newTestCollector(t *testing.T) *Collector {
	t.Helper()
	return newCollector(prometheus.NewRegistry(), "amqp")
}

// TestRecoveryMetrics_CountersAndGauges (spec 06 §3.2, tests #4/#5): the
// recovery families move by the called value.
func TestRecoveryMetrics_CountersAndGauges(t *testing.T) {
	c := newTestCollector(t)
	c.RecordRecoveryFault("queue-recovery", "degraded")
	c.RecordRecoveryFault("queue-recovery", "degraded")
	c.RecordRecoveryFault("queue-recovery", "fatal")
	c.RecordRecoveryFault("wal-scan", "benign")
	c.RecordRecoveryDiscardedRecords("queue-recovery", 3)
	c.RecordRecoveryDuration(0.25)
	c.SetRecoveryMessagesRecovered(7)

	assert.Equal(t, 2.0, testutil.ToFloat64(c.RecoveryFaultsTotal.WithLabelValues("queue-recovery", "degraded")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.RecoveryFaultsTotal.WithLabelValues("queue-recovery", "fatal")))
	assert.Equal(t, 3.0, testutil.ToFloat64(c.RecoveryDiscardedRecordsTotal.WithLabelValues("queue-recovery")))
	assert.Equal(t, 1, testutil.CollectAndCount(c.RecoveryDurationSeconds))
	assert.Equal(t, 7.0, testutil.ToFloat64(c.RecoveryMessagesRecovered))
}

// TestWALSegmentErrorCountersAreDistinct (spec 06 §3.3, test #6): the fsync
// error series and the generic write-error series are disjoint.
func TestWALSegmentErrorCountersAreDistinct(t *testing.T) {
	c := newTestCollector(t)
	c.RecordWALFsyncError()
	c.RecordWALRollError()
	c.RecordWALCompactionError()
	c.RecordWALReclaimFsyncError()
	c.UpdateWALFileCount(4)
	c.RecordSegmentRollError()
	c.RecordSegmentFsyncError()

	assert.Equal(t, 1.0, testutil.ToFloat64(c.WALFsyncErrorsTotal))
	assert.Equal(t, 0.0, testutil.ToFloat64(c.WALWriteErrorsTotal),
		"an fsync error must NOT increment the write-error series")
	assert.Equal(t, 1.0, testutil.ToFloat64(c.WALRollErrorsTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.WALCompactionErrorsTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.WALReclaimFsyncErrorsTotal))
	assert.Equal(t, 4.0, testutil.ToFloat64(c.WALFiles))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.SegmentRollErrorsTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.SegmentFsyncErrorsTotal))

	c.RecordWALWriteError()
	assert.Equal(t, 1.0, testutil.ToFloat64(c.WALWriteErrorsTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.WALFsyncErrorsTotal),
		"a write error must NOT increment the fsync-error series")
}

// TestPublishDeliveryCounters (spec 06 §3.4): the bounded-reason families move.
func TestPublishDeliveryCounters(t *testing.T) {
	c := newTestCollector(t)
	c.RecordPublishRefused("x_max_length")
	c.RecordPublishRefused("queue_closed")
	c.RecordPublishRefused("queue_closed")
	c.RecordConsumerRegisterError()
	c.RecordDepthGateArmed()
	c.RecordReaderPaused()
	c.RecordDeliverySendError()
	c.RecordDeliveriesRequeued("write_error", 3)
	c.RecordDeliveriesDropped("consumer_removed", 1)

	assert.Equal(t, 1.0, testutil.ToFloat64(c.PublishRefusedTotal.WithLabelValues("x_max_length")))
	assert.Equal(t, 2.0, testutil.ToFloat64(c.PublishRefusedTotal.WithLabelValues("queue_closed")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ConsumerRegisterErrorsTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.DepthGateArmedTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ReaderPausedTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.DeliverySendErrorsTotal))
	assert.Equal(t, 3.0, testutil.ToFloat64(c.DeliveriesRequeuedTotal.WithLabelValues("write_error")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.DeliveriesDroppedTotal.WithLabelValues("consumer_removed")))
}

// TestLifecycleCounters (spec 06 §3.5): connection/shutdown/alarm/panic.
func TestLifecycleCounters(t *testing.T) {
	c := newTestCollector(t)
	c.RecordConnectionClosedByCause("client_eof")
	c.RecordConnectionClosedByCause("heartbeat_timeout")
	c.RecordShutdown("clean", 0.5)
	c.RecordShutdownConnectionForced()
	c.RecordShutdownConnectionForced()
	c.RecordStorageCloseError()
	c.RecordResourceAlarmTrip("memory")
	c.RecordPanic("reader")

	assert.Equal(t, 1.0, testutil.ToFloat64(c.ConnectionsClosedByCauseTotal.WithLabelValues("client_eof")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ConnectionsClosedByCauseTotal.WithLabelValues("heartbeat_timeout")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ShutdownTotal.WithLabelValues("clean")))
	assert.Equal(t, 1, testutil.CollectAndCount(c.ShutdownDurationSeconds))
	assert.Equal(t, 2.0, testutil.ToFloat64(c.ShutdownConnectionsForcedTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.StorageCloseErrorsTotal))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ResourceAlarmTripsTotal.WithLabelValues("memory")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.PanicsTotal.WithLabelValues("reader")))
}

// TestLiveGauges_ReadAtScrape (spec 06 §5 test #17): the three gauges track the
// bound accessor with no Set call.
func TestLiveGauges_ReadAtScrape(t *testing.T) {
	c := newTestCollector(t)
	var depth, paused, inflight int64
	c.BindDepthGateStats(func() int64 { return depth })
	c.BindReaderPausedStats(func() int64 { return paused })
	c.BindMessagesInflightStats(func() int64 { return inflight })

	assert.Equal(t, 0.0, testutil.ToFloat64(c.DepthGatedCurrent))
	assert.Equal(t, 0.0, testutil.ToFloat64(c.ReaderPausedCurrent))
	assert.Equal(t, 0.0, testutil.ToFloat64(c.MessagesInflight))

	depth, paused, inflight = 2, 1, 42
	assert.Equal(t, 2.0, testutil.ToFloat64(c.DepthGatedCurrent))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ReaderPausedCurrent))
	assert.Equal(t, 42.0, testutil.ToFloat64(c.MessagesInflight))

	// Unbound collectors read 0 rather than panicking.
	bare := newTestCollector(t)
	assert.Equal(t, 0.0, testutil.ToFloat64(bare.DepthGatedCurrent))
}

// TestQueueUnackedWiredFromPoller (spec 06 §5 test #18): a known unacked depth
// is published; an unknown one creates no series.
func TestQueueUnackedWiredFromPoller(t *testing.T) {
	c := newTestCollector(t)
	p := &queuePoller{collector: c}
	p.pollOnce(fakeQueueStats{stats: []QueueStat{
		{Name: "q", VHost: "/", Ready: 5, ReadyKnown: true, Unacked: 2, UnackedKnown: true, Consumers: 1},
		{Name: "u", VHost: "/", Ready: 0, ReadyKnown: false, UnackedKnown: false},
	}}, nil)

	assert.Equal(t, 2.0, testutil.ToFloat64(c.QueueMessagesUnacked.WithLabelValues("q", "/")))
	assert.Equal(t, 7.0, testutil.ToFloat64(c.QueueMessagesTotal.WithLabelValues("q", "/")))

	// The unknown queue emits no unacked/total series.
	require.Equal(t, 1, testutil.CollectAndCount(c.QueueMessagesUnacked),
		"only the known queue has an unacked series")
	require.Equal(t, 1, testutil.CollectAndCount(c.QueueMessagesTotal))
}
