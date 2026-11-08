package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server/management"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

// mgmtCollector is the single "amqp"-namespace collector in this test binary:
// NewCollector registers its fixed families on the default registry, so a second
// "amqp" collector would panic. The management-family tests redirect its
// mgmtRegistry to a private registry (mgmtRegistry) so they never contaminate
// the default registry that TestCollector_ManagementDisabledNoSeries inspects.
var mgmtCollector = NewCollector("amqp")

// mgmtRegistry is the private registry the enabled-path test registers the
// amqp_management_* families on. It is package-scoped so `-count=N` reuses the
// same series instead of re-registering (which RegisterManagement's sync.Once
// would make a no-op anyway) and the delta assertions keep holding.
var mgmtRegistry = prometheus.NewRegistry()

// registerManagementMetrics points the shared collector at the private registry
// and registers the management families (idempotent).
func registerManagementMetrics() {
	mgmtCollector.mgmtRegistry = mgmtRegistry
	mgmtCollector.RegisterManagement()
}

// managementTestServer builds a management Server in "none" auth mode with the
// shared collector as its sink. The surface is enabled but never started: the
// test drives Handler() directly.
func managementTestServer(t *testing.T) *management.Server {
	t.Helper()
	return managementTestServerWithSink(t, mgmtCollector)
}

// managementTestServerWithSink builds a management Server whose metrics sink is
// explicitly provided (the shared collector, or a test wrapper around it).
func managementTestServerWithSink(t *testing.T, sink management.ManagementMetricsSink) *management.Server {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Management.Enabled = true
	cfg.Management.Authentication = config.ManagementAuthenticationNone
	cfg.Management.Address = "127.0.0.1"
	m, err := management.New(management.Options{
		Config:  cfg,
		Logger:  zap.NewNop(),
		Metrics: sink,
	})
	if err != nil {
		t.Fatalf("management.New: %v", err)
	}
	return m
}

// doOverview drives one GET /api/overview through the real management handler.
func doOverview(t *testing.T, m *management.Server) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	req.RemoteAddr = "127.0.0.1:43210"
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	return rec
}

// inflightBarrierSink is a test-only ManagementMetricsSink: it forwards every
// call to the real collector and, while armed, holds the first `pending`
// ManagementInFlight calls (the ENTRY calls of the N barrier requests) until the
// test releases them. It is the sync.WaitGroup-driven seam the barrier test
// uses to hold N handlers inside serveHTTP.
type inflightBarrierSink struct {
	*Collector
	mu      sync.Mutex
	pending int
	held    chan struct{}
	release chan struct{}
}

func newInflightBarrierSink(c *Collector, n int) *inflightBarrierSink {
	return &inflightBarrierSink{
		Collector: c,
		pending:   n,
		held:      make(chan struct{}, n),
		release:   make(chan struct{}),
	}
}

func (b *inflightBarrierSink) ManagementInFlight(n int) {
	b.Collector.ManagementInFlight(n)
	b.mu.Lock()
	hold := b.pending > 0
	if hold {
		b.pending--
	}
	b.mu.Unlock()
	if hold {
		b.held <- struct{}{}
		<-b.release
	}
}

// scrapeCounter sums a named counter family across a registry for the exact
// label set.
func scrapeCounter(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range f.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				total += m.GetCounter().GetValue()
			}
		}
		return total
	}
	return 0
}

// familyPresent reports whether a metric family is registered on a registry.
func familyPresent(t *testing.T, g prometheus.Gatherer, name string) bool {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return true
		}
	}
	return false
}

// scrapeGauge sums a named gauge family across a registry for the exact label
// set (an absent family or label set reads 0).
func scrapeGauge(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range f.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				total += m.GetGauge().GetValue()
			}
		}
		return total
	}
	return 0
}

// scrapeHistogramCount returns the total sample count of a named histogram
// family across a registry for the exact label set.
func scrapeHistogramCount(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) uint64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range f.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				total += m.GetHistogram().GetSampleCount()
			}
		}
		return total
	}
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(pairs))
	for _, p := range pairs {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// TestCollector_ManagementDisabledNoSeries (OPS-002/SEC-001): with the
// management surface disabled RegisterManagement is never called, so the
// telemetry registry carries NO amqp_management_* family. The enabled-path test
// registers into a private registry, so this assertion is order-independent.
func TestCollector_ManagementDisabledNoSeries(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "amqp_management_") {
			t.Fatalf("management disabled must register no amqp_management_* series, found %q", f.GetName())
		}
	}
}

// TestCollector_ManagementEnabledRecordsFamilies (OPS-001/003/004/005,
// NFR-050): with the surface enabled, RegisterManagement registers the mgmt-01
// §9 families and one GET /api/overview is counted under the bounded route
// template label, with the in-flight gauge present.
func TestCollector_ManagementEnabledRecordsFamilies(t *testing.T) {
	registerManagementMetrics()
	m := managementTestServer(t)

	labels := map[string]string{"endpoint": "/api/overview", "method": "GET", "status_class": "2"}
	before := scrapeCounter(t, mgmtRegistry, "amqp_management_requests_total", labels)

	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	req.RemoteAddr = "127.0.0.1:43210"
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/overview = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}

	after := scrapeCounter(t, mgmtRegistry, "amqp_management_requests_total", labels)
	if after-before != 1 {
		t.Fatalf("amqp_management_requests_total{endpoint=/api/overview,method=GET,status_class=2} delta = %v, want 1", after-before)
	}
	if !familyPresent(t, mgmtRegistry, "amqp_management_in_flight") {
		t.Fatal("amqp_management_in_flight must be present after a request")
	}
}

// TestManagementInFlightGauge_ReturnsToZero (MEDIUM-1): the in-flight gauge is
// set at request ENTRY and MUST be decremented at completion. Before the fix it
// stuck at >= 1 forever.
func TestManagementInFlightGauge_ReturnsToZero(t *testing.T) {
	registerManagementMetrics()
	m := managementTestServer(t)

	if rec := doOverview(t, m); rec.Code != http.StatusOK {
		t.Fatalf("GET /api/overview = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := scrapeGauge(t, mgmtRegistry, "amqp_management_in_flight", nil); got != 0 {
		t.Fatalf("amqp_management_in_flight = %v after the request completed, want 0", got)
	}
}

// TestManagementInFlightGauge_Barrier (MEDIUM-1): with N handlers held inside
// serveHTTP the gauge reads exactly N, and after they complete one at a time it
// returns to 0. The barrier is a test-only sink wrapper (no sleeps).
func TestManagementInFlightGauge_Barrier(t *testing.T) {
	registerManagementMetrics()
	const n = 3
	sink := newInflightBarrierSink(mgmtCollector, n)
	m := managementTestServerWithSink(t, sink)

	done := make(chan struct{}, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
			req.RemoteAddr = "127.0.0.1:43210"
			m.Handler().ServeHTTP(httptest.NewRecorder(), req)
			done <- struct{}{}
		}()
		// Start the next handler only once this one is held at the barrier, so
		// the entry Sets are ordered 1..N (a deterministic gauge, not a race).
		select {
		case <-sink.held:
		case <-time.After(3 * time.Second):
			t.Fatal("handler did not reach the in-flight barrier within 3s")
		}
	}

	if got := scrapeGauge(t, mgmtRegistry, "amqp_management_in_flight", nil); got != n {
		t.Fatalf("amqp_management_in_flight = %v with %d requests held in flight, want %d", got, n, n)
	}

	// Release one handler at a time, so the completion Sets are ordered and the
	// final value is exactly 0.
	for i := 0; i < n; i++ {
		sink.release <- struct{}{}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("held handler did not complete within 3s")
		}
	}
	wg.Wait()

	if got := scrapeGauge(t, mgmtRegistry, "amqp_management_in_flight", nil); got != 0 {
		t.Fatalf("amqp_management_in_flight = %v after all requests completed, want 0", got)
	}
}

// TestManagementSnapshotDuration_RecordedOnOverview (MEDIUM-2): one GET
// /api/overview observes the snapshot duration on the bounded route template.
func TestManagementSnapshotDuration_RecordedOnOverview(t *testing.T) {
	registerManagementMetrics()
	m := managementTestServer(t)

	labels := map[string]string{"endpoint": "/api/overview"}
	before := scrapeHistogramCount(t, mgmtRegistry, "amqp_management_snapshot_duration_seconds", labels)

	if rec := doOverview(t, m); rec.Code != http.StatusOK {
		t.Fatalf("GET /api/overview = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}

	after := scrapeHistogramCount(t, mgmtRegistry, "amqp_management_snapshot_duration_seconds", labels)
	if after-before < 1 {
		t.Fatalf("amqp_management_snapshot_duration_seconds_count{endpoint=/api/overview} delta = %d, want >= 1", after-before)
	}
}

// TestCollector_ImplementsManagementSink is the contract pin: *metrics.Collector
// satisfies management.ManagementMetricsSink.
func TestCollector_ImplementsManagementSink(t *testing.T) {
	var sink any = &Collector{}
	if _, ok := sink.(management.ManagementMetricsSink); !ok {
		t.Fatal("*metrics.Collector must implement management.ManagementMetricsSink")
	}
}
