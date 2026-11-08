package metrics

// broker_telemetry.go — the spec 06 §3.2–§3.5 broker outage families.
//
// Every label domain is a CLOSED enum (the caller passes one of the bounded
// const strings declared in server/recovery_metrics.go and server/metrics.go);
// no family carries a queue name, connection id, user name, routing key or
// exchange name. The three live gauges (depth_gated_current,
// reader_paused_current, messages_inflight) are GaugeFuncs over accessors the
// server binds once, so the publish/deliver hot path pays nothing.

// --- Recovery (spec 06 §3.2) ---

// RecordRecoveryFault counts one classified recovery fault by stage and
// bounded outcome (benign|degraded|fatal).
func (c *Collector) RecordRecoveryFault(stage, outcome string) {
	c.RecoveryFaultsTotal.WithLabelValues(stage, outcome).Inc()
}

// RecordRecoveryDiscardedRecords counts n records discarded or quarantined at
// boot by recovery stage.
func (c *Collector) RecordRecoveryDiscardedRecords(stage string, n int) {
	if n <= 0 {
		return
	}
	c.RecoveryDiscardedRecordsTotal.WithLabelValues(stage).Add(float64(n))
}

// RecordRecoveryDuration observes the boot recovery pass duration.
func (c *Collector) RecordRecoveryDuration(seconds float64) {
	c.RecoveryDurationSeconds.Observe(seconds)
}

// SetRecoveryMessagesRecovered pins the number of persistent messages loaded
// back into the running broker at boot.
func (c *Collector) SetRecoveryMessagesRecovered(n int) {
	c.RecoveryMessagesRecovered.Set(float64(n))
}

// --- WAL / segment durability (spec 06 §3.3) ---

// RecordWALFsyncError counts one failed WAL durability barrier. SEPARATE from
// RecordWALWriteError: a barrier failure and a write failure are different
// outages.
func (c *Collector) RecordWALFsyncError() { c.WALFsyncErrorsTotal.Inc() }

// RecordWALRollError counts one failed WAL roll.
func (c *Collector) RecordWALRollError() { c.WALRollErrorsTotal.Inc() }

// RecordWALCompactionError counts one failed WAL compaction.
func (c *Collector) RecordWALCompactionError() { c.WALCompactionErrorsTotal.Inc() }

// RecordWALReclaimFsyncError counts one failed directory fsync after reclaim.
func (c *Collector) RecordWALReclaimFsyncError() { c.WALReclaimFsyncErrorsTotal.Inc() }

// UpdateWALFileCount sets the number of .wal files in the shared WAL directory.
func (c *Collector) UpdateWALFileCount(n int) { c.WALFiles.Set(float64(n)) }

// RecordSegmentRollError counts one failed segment file create/open.
func (c *Collector) RecordSegmentRollError() { c.SegmentRollErrorsTotal.Inc() }

// RecordSegmentFsyncError counts one failed segment fsync barrier.
func (c *Collector) RecordSegmentFsyncError() { c.SegmentFsyncErrorsTotal.Inc() }

// --- Publish / back-pressure / delivery (spec 06 §3.4) ---

// RecordPublishRefused counts one publish refused after routing by bounded
// reason (x_max_length|queue_closed). Unroutable is deliberately NOT re-counted
// here; amqp_messages_unroutable_total carries that signal.
func (c *Collector) RecordPublishRefused(reason string) {
	c.PublishRefusedTotal.WithLabelValues(reason).Inc()
}

// RecordConsumerRegisterError counts one refused basic.consume registration.
func (c *Collector) RecordConsumerRegisterError() { c.ConsumerRegisterErrorsTotal.Inc() }

// RecordDepthGateArmed counts one queue-depth reader gate armed.
func (c *Collector) RecordDepthGateArmed() { c.DepthGateArmedTotal.Inc() }

// RecordDeliverySendError counts one failed basic.deliver batch write.
func (c *Collector) RecordDeliverySendError() { c.DeliverySendErrorsTotal.Inc() }

// RecordDeliveriesRequeued counts n deliveries requeued after a delivery-path
// failure, by bounded reason (write_error).
func (c *Collector) RecordDeliveriesRequeued(reason string, n int) {
	if n <= 0 {
		return
	}
	c.DeliveriesRequeuedTotal.WithLabelValues(reason).Add(float64(n))
}

// RecordDeliveriesDropped counts n deliveries dropped without requeue, by
// bounded reason (consumer_removed). This is the silent message-loss signal.
func (c *Collector) RecordDeliveriesDropped(reason string, n int) {
	if n <= 0 {
		return
	}
	c.DeliveriesDroppedTotal.WithLabelValues(reason).Add(float64(n))
}

// RecordReaderPaused counts one reader park on the frame-budget hand-off.
func (c *Collector) RecordReaderPaused() { c.ReaderPausedTotal.Inc() }

// --- Connections / shutdown / alarms / panics (spec 06 §3.5) ---

// RecordConnectionClosedByCause counts one connection close by bounded cause.
func (c *Collector) RecordConnectionClosedByCause(cause string) {
	c.ConnectionsClosedByCauseTotal.WithLabelValues(cause).Inc()
}

// RecordShutdown counts one broker shutdown by bounded result
// (clean|forced|incomplete) and observes its duration.
func (c *Collector) RecordShutdown(result string, seconds float64) {
	c.ShutdownTotal.WithLabelValues(result).Inc()
	c.ShutdownDurationSeconds.Observe(seconds)
}

// RecordShutdownConnectionForced counts one connection told to close by
// broker shutdown.
func (c *Collector) RecordShutdownConnectionForced() {
	c.ShutdownConnectionsForcedTotal.Inc()
}

// RecordStorageCloseError counts one failure closing the storage tier.
func (c *Collector) RecordStorageCloseError() { c.StorageCloseErrorsTotal.Inc() }

// RecordResourceAlarmTrip counts one resource alarm raised (0 -> non-zero edge)
// by bounded resource (memory|disk).
func (c *Collector) RecordResourceAlarmTrip(resource string) {
	c.ResourceAlarmTripsTotal.WithLabelValues(resource).Inc()
}

// RecordPanic counts one recovered panic by bounded component.
func (c *Collector) RecordPanic(component string) {
	c.PanicsTotal.WithLabelValues(component).Inc()
}

// --- live gauge binders (spec 06 §3.4/§3.5) ---

// BindDepthGateStats binds the accessor the amqp_depth_gated_current gauge
// reads at scrape time. Called once by the server builder.
func (c *Collector) BindDepthGateStats(stats func() int64) { c.depthGateStats.Store(stats) }

// BindReaderPausedStats binds the accessor the amqp_reader_paused_current gauge
// reads at scrape time. Called once by the server builder.
func (c *Collector) BindReaderPausedStats(stats func() int64) { c.readerPausedStats.Store(stats) }

// BindMessagesInflightStats binds the accessor the amqp_messages_inflight gauge
// reads at scrape time. Called once by the server builder.
func (c *Collector) BindMessagesInflightStats(stats func() int64) { c.inflightStats.Store(stats) }
