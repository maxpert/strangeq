package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// managementMetrics holds the amqp_management_* families of mgmt-01 §9 /
// 14-observability-and-operability.md §14.1. They are created ONLY by
// RegisterManagement, so with the management surface disabled (or telemetry
// off) none of them is registered (OPS-002/SEC-001).
//
// Cardinality discipline (OPS-004/005): every label is bounded — `endpoint` is
// the route TEMPLATE (e.g. "/api/queues/{vhost}/{name}"), `method` is the HTTP
// verb, `status_class` is 2/3/4/5, `reason`/`capability`/`state`/`lock` come
// from closed enums in the management package. No entity name, user, vhost, or
// message content is ever a label.
type managementMetrics struct {
	requests            *prometheus.CounterVec
	requestDuration     *prometheus.HistogramVec
	inFlight            prometheus.Gauge
	sessions            prometheus.Gauge
	authFailures        *prometheus.CounterVec
	rateLimited         prometheus.Counter
	snapshotPartial     *prometheus.CounterVec
	snapshotDuration    *prometheus.HistogramVec
	snapshotLockWait    prometheus.Histogram
	forbiddenLock       *prometheus.CounterVec
	publishBackpressure prometheus.Counter
	jobs                *prometheus.GaugeVec
	auditDropped        prometheus.Counter
	capabilityDenied    *prometheus.CounterVec
	unauthenticated     prometheus.Gauge
	insecureBind        prometheus.Gauge
	uiRequests          *prometheus.CounterVec
}

// RegisterManagement creates and registers the amqp_management_* families on
// the telemetry registry (mgmt-01 §9). It is idempotent (sync.Once): a second
// call is a no-op, so a re-entrant Start cannot panic on duplicate
// registration.
//
// It MUST be called only when the management surface is enabled: until it runs
// every ManagementMetricsSink method is a no-op, and OPS-002 requires that a
// disabled surface register no amqp_management_* series at all.
func (c *Collector) RegisterManagement() {
	c.mgmtOnce.Do(func() {
		ns := c.namespace
		reg := c.mgmtRegistry
		if reg == nil {
			reg = prometheus.DefaultRegisterer
		}
		m := &managementMetrics{
			requests: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_requests_total",
				Help:      "Management API requests by bounded route template, method and status class",
			}, []string{"endpoint", "method", "status_class"}),
			requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "management_request_duration_seconds",
				Help:      "Management API request latency by bounded route template",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
			}, []string{"endpoint"}),
			inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "management_in_flight",
				Help:      "Management API requests currently being served (SEC-061)",
			}),
			sessions: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "management_sessions",
				Help:      "Management session-store usage (SEC-064)",
			}),
			authFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_auth_failures_total",
				Help:      "Refused management authentication attempts by reason (SEC-050)",
			}, []string{"reason"}),
			rateLimited: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_rate_limited_total",
				Help:      "Management requests refused by a rate or in-flight limit",
			}),
			snapshotPartial: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_snapshot_partial_total",
				Help:      "Degraded management snapshots by bounded route template (DATA-053)",
			}, []string{"endpoint"}),
			snapshotDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "management_snapshot_duration_seconds",
				Help:      "Management snapshot cost by bounded route template",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
			}, []string{"endpoint"}),
			snapshotLockWait: prometheus.NewHistogram(prometheus.HistogramOpts{
				Namespace: ns,
				Name:      "management_snapshot_lock_wait_seconds",
				Help:      "Wait on the permitted Server.Mutex read lock during a snapshot (NFR-011)",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
			}),
			forbiddenLock: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_forbidden_lock_acquired_total",
				Help:      "Acquisitions of a forbidden hot-path lock by a management request; MUST stay 0 (NFR-011)",
			}, []string{"lock"}),
			publishBackpressure: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_publish_backpressure_total",
				Help:      "HTTP publishes refused 503 by a queue high-water gate (FR-API-058)",
			}),
			jobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "management_jobs",
				Help:      "Management async job pool by state (FR-API-142)",
			}, []string{"state"}),
			auditDropped: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_audit_dropped_total",
				Help:      "Non-destructive audit records dropped; destructive records never drop (SEC-084)",
			}),
			capabilityDenied: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_capability_denied_total",
				Help:      "Management requests denied by the capability gate, by capability (AUTH-010/023)",
			}, []string{"capability"}),
			unauthenticated: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "management_unauthenticated",
				Help:      "1 when the management surface runs with authentication disabled (AUTH-004)",
			}),
			insecureBind: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: ns,
				Name:      "management_insecure_bind",
				Help:      "1 when the management listener is non-loopback without TLS (SEC-110)",
			}),
			uiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: ns,
				Name:      "management_ui_requests_total",
				Help:      "Management UI asset/route requests by status class",
			}, []string{"status_class"}),
		}
		reg.MustRegister(
			m.requests, m.requestDuration, m.inFlight, m.sessions, m.authFailures,
			m.rateLimited, m.snapshotPartial, m.snapshotDuration, m.snapshotLockWait,
			m.forbiddenLock, m.publishBackpressure, m.jobs, m.auditDropped,
			m.capabilityDenied, m.unauthenticated, m.insecureBind, m.uiRequests,
		)
		c.mgmt.Store(m)
	})
}

// mgmtOrNil returns the registered management families, or nil when
// RegisterManagement has not run (every sink method is then a no-op).
func (c *Collector) mgmtOrNil() *managementMetrics { return c.mgmt.Load() }

func statusClassLabel(statusClass int) string { return strconv.Itoa(statusClass) }

// ManagementRequests counts one management request by bounded route template,
// method and status class (OPS-001/004).
func (c *Collector) ManagementRequests(endpoint, method string, statusClass int) {
	if m := c.mgmtOrNil(); m != nil {
		m.requests.WithLabelValues(endpoint, method, statusClassLabel(statusClass)).Inc()
	}
}

// ManagementDuration observes one management request's latency.
func (c *Collector) ManagementDuration(endpoint string, seconds float64) {
	if m := c.mgmtOrNil(); m != nil {
		m.requestDuration.WithLabelValues(endpoint).Observe(seconds)
	}
}

// ManagementInFlight sets the current in-flight management request count.
func (c *Collector) ManagementInFlight(n int) {
	if m := c.mgmtOrNil(); m != nil {
		m.inFlight.Set(float64(n))
	}
}

// ManagementSessions sets the current session-store usage.
func (c *Collector) ManagementSessions(n int) {
	if m := c.mgmtOrNil(); m != nil {
		m.sessions.Set(float64(n))
	}
}

// ManagementAuthFailure counts one refused authentication attempt by reason.
func (c *Collector) ManagementAuthFailure(reason string) {
	if m := c.mgmtOrNil(); m != nil {
		m.authFailures.WithLabelValues(reason).Inc()
	}
}

// ManagementRateLimited counts one rate/in-flight refusal.
func (c *Collector) ManagementRateLimited() {
	if m := c.mgmtOrNil(); m != nil {
		m.rateLimited.Inc()
	}
}

// ManagementSnapshotPartial counts one degraded snapshot by route template.
func (c *Collector) ManagementSnapshotPartial(endpoint string) {
	if m := c.mgmtOrNil(); m != nil {
		m.snapshotPartial.WithLabelValues(endpoint).Inc()
	}
}

// ManagementSnapshotDuration observes one snapshot's cost by route template.
func (c *Collector) ManagementSnapshotDuration(endpoint string, seconds float64) {
	if m := c.mgmtOrNil(); m != nil {
		m.snapshotDuration.WithLabelValues(endpoint).Observe(seconds)
	}
}

// ManagementSnapshotLockWait observes the time a snapshot spent waiting on the
// permitted Server.Mutex read lock.
func (c *Collector) ManagementSnapshotLockWait(seconds float64) {
	if m := c.mgmtOrNil(); m != nil {
		m.snapshotLockWait.Observe(seconds)
	}
}

// ManagementForbiddenLock counts an acquisition of a forbidden hot-path lock by
// a management request. It is NOT runtime-reachable: no management code path
// calls it, and the AST gate plus the hold-the-lock deadline tests are the real
// barrier (NFR-011). It exists so a future regression has a name to fire.
func (c *Collector) ManagementForbiddenLock(lock string) {
	if m := c.mgmtOrNil(); m != nil {
		m.forbiddenLock.WithLabelValues(lock).Inc()
	}
}

// ManagementPublishBackpressure counts an HTTP publish refused by the
// high-water gate.
func (c *Collector) ManagementPublishBackpressure() {
	if m := c.mgmtOrNil(); m != nil {
		m.publishBackpressure.Inc()
	}
}

// ManagementJobs sets the job-pool count for a state.
func (c *Collector) ManagementJobs(state string, n int) {
	if m := c.mgmtOrNil(); m != nil {
		m.jobs.WithLabelValues(state).Set(float64(n))
	}
}

// ManagementAuditDropped counts one non-destructive audit record dropped.
func (c *Collector) ManagementAuditDropped() {
	if m := c.mgmtOrNil(); m != nil {
		m.auditDropped.Inc()
	}
}

// ManagementCapabilityDenied counts one capability-gate denial.
func (c *Collector) ManagementCapabilityDenied(capability string) {
	if m := c.mgmtOrNil(); m != nil {
		m.capabilityDenied.WithLabelValues(capability).Inc()
	}
}

// ManagementUnauthenticated pins the auth-disabled gauge (1 when disabled).
func (c *Collector) ManagementUnauthenticated(v bool) {
	if m := c.mgmtOrNil(); m != nil {
		m.unauthenticated.Set(boolGauge(v))
	}
}

// ManagementInsecureBind pins the insecure-bind gauge (1 when tripped).
func (c *Collector) ManagementInsecureBind(v bool) {
	if m := c.mgmtOrNil(); m != nil {
		m.insecureBind.Set(boolGauge(v))
	}
}

// ManagementUIRequests counts one UI asset/route request by status class.
func (c *Collector) ManagementUIRequests(statusClass int) {
	if m := c.mgmtOrNil(); m != nil {
		m.uiRequests.WithLabelValues(statusClassLabel(statusClass)).Inc()
	}
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
