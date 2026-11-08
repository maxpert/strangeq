package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	cfg := config.DefaultConfig()
	cfg.Management.Enabled = true
	cfg.Management.Authentication = config.ManagementAuthenticationNone
	cfg.Management.Address = "127.0.0.1"
	m, err := management.New(management.Options{
		Config:  cfg,
		Logger:  zap.NewNop(),
		Metrics: mgmtCollector,
	})
	if err != nil {
		t.Fatalf("management.New: %v", err)
	}
	return m
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

// TestCollector_ImplementsManagementSink is the contract pin: *metrics.Collector
// satisfies management.ManagementMetricsSink.
func TestCollector_ImplementsManagementSink(t *testing.T) {
	var sink any = &Collector{}
	if _, ok := sink.(management.ManagementMetricsSink); !ok {
		t.Fatal("*metrics.Collector must implement management.ManagementMetricsSink")
	}
}
