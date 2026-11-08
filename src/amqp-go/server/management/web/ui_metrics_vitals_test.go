package web

import (
	"testing"
)

// TestUI_MetricsManagement5xxTile (BUG 1): amqp_management_requests_total is
// labelled with the literal status classes "2".."5" (metrics/management.go
// statusClassLabel = strconv.Itoa). The mgmt_5xx tile selects status_class="5",
// so a present 5-class series must render its number with a bad severity — never
// a permanent em-dash — and the 2-class series must not bleed into it.
func TestUI_MetricsManagement5xxTile(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_management_requests_total Management requests",
			"# TYPE amqp_management_requests_total counter",
			`amqp_management_requests_total{endpoint="/api/overview",method="GET",status_class="2"} 40`,
			`amqp_management_requests_total{endpoint="/api/overview",method="GET",status_class="5"} 4`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if got := tileText(t, ctx, "mgmt_5xx"); got != "4" {
		t.Fatalf("mgmt_5xx tile = %q, want 4 (a present status_class=\"5\" series)", got)
	}
	if got := tileSeverity(t, ctx, "mgmt_5xx"); got != "bad" {
		t.Fatalf("mgmt_5xx severity = %q, want bad", got)
	}
	// The tile must reflect only the 5xx class; an absent family stays unknown.
	if got := tileText(t, ctx, "mgmt_rate_limited"); got != "\u2014" {
		t.Fatalf("absent mgmt_rate_limited tile = %q, want an em-dash", got)
	}
	if got := evalStr(t, ctx, `document.getElementById('metrics-tile-mgmt_5xx').title`); got != "" {
		t.Fatalf("present mgmt_5xx tile title = %q, want empty", got)
	}
}
