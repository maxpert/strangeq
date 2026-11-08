package management

// ManagementMetricsSink is the optional metrics surface discovered on
// Options.Metrics (OPS-001…005). When the injected collector does not implement
// it every call is a no-op, so the surface is zero-cost with telemetry disabled
// (SEC-001). Labels are bounded route templates, never raw paths (OPS-004).
type ManagementMetricsSink interface {
	ManagementRequests(endpoint, method string, statusClass int)
	ManagementDuration(endpoint string, seconds float64)
	ManagementInFlight(n int)
	ManagementSessions(n int)
	ManagementAuthFailure(reason string)
	ManagementRateLimited()
	ManagementSnapshotPartial(endpoint string)
	ManagementSnapshotDuration(endpoint string, seconds float64)
	ManagementSnapshotLockWait(seconds float64)
	// ManagementForbiddenLock counts an acquisition of a forbidden hot-path
	// lock by a management request. It is NOT runtime-reachable: no management
	// request path calls it. The AST gate (TestForbiddenLockGate_ASTScan) and
	// the hold-the-lock deadline test (TestForbiddenLock_OverviewDoesNotBlock)
	// are the real barrier (NFR-011); the family exists only so a future
	// regression has a name to fire. See
	// TestForbiddenLockMetric_NotRuntimeReachable.
	ManagementForbiddenLock(lock string)
	ManagementPublishBackpressure()
	ManagementJobs(state string, n int)
	ManagementAuditDropped()
	ManagementCapabilityDenied(capability string)
	ManagementUnauthenticated(v bool)
	ManagementInsecureBind(v bool)
	ManagementUIRequests(statusClass int)
	// ManagementPanic counts one recovered management-handler panic by the
	// bounded route TEMPLATE (OPS-060). The endpoint is never a raw path.
	ManagementPanic(endpoint string)
}

// noopSink implements ManagementMetricsSink as a no-op.
type noopSink struct{}

func (noopSink) ManagementRequests(string, string, int)     {}
func (noopSink) ManagementDuration(string, float64)         {}
func (noopSink) ManagementInFlight(int)                     {}
func (noopSink) ManagementSessions(int)                     {}
func (noopSink) ManagementAuthFailure(string)               {}
func (noopSink) ManagementRateLimited()                     {}
func (noopSink) ManagementSnapshotPartial(string)           {}
func (noopSink) ManagementSnapshotDuration(string, float64) {}
func (noopSink) ManagementSnapshotLockWait(float64)         {}
func (noopSink) ManagementForbiddenLock(string)             {}
func (noopSink) ManagementPublishBackpressure()             {}
func (noopSink) ManagementJobs(string, int)                 {}
func (noopSink) ManagementAuditDropped()                    {}
func (noopSink) ManagementCapabilityDenied(string)          {}
func (noopSink) ManagementUnauthenticated(bool)             {}
func (noopSink) ManagementInsecureBind(bool)                {}
func (noopSink) ManagementUIRequests(int)                   {}
func (noopSink) ManagementPanic(string)                     {}

// sinkOrNoop returns the injected sink or a shared no-op.
func sinkOrNoop(base any) ManagementMetricsSink {
	if s, ok := base.(ManagementMetricsSink); ok && s != nil {
		return s
	}
	return noopSink{}
}

// statusClass returns the metric class for an HTTP status (2/3/4/5).
func statusClass(status int) int {
	switch {
	case status >= 500:
		return 5
	case status >= 400:
		return 4
	case status >= 300:
		return 3
	case status >= 200:
		return 2
	default:
		return 0
	}
}
