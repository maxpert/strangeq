package management

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
)

// scrapeEnv builds a broker-less management env whose MetricsScrape seam is fn.
// mutate, when non-nil, adjusts the config last.
func scrapeEnv(t *testing.T, mutate func(*config.AMQPConfig), fn func() ([]byte, error)) *env {
	t.Helper()
	cfg := baseEnvConfig(t, mutate)
	e := &env{t: t, cfg: cfg}
	e.mgmt = newManagementWithScrape(t, nil, cfg, fn)
	return e
}

const fakeScrape = "# HELP x Total messages\n# TYPE x_total counter\nx_total 5\n"

// TestMetrics_ServesScrapeTextJSON: telemetry enabled, scrape ok ⇒ 200 with the
// JSON {text} wrapper, an ETag, no-store, and Vary: Authorization (spec mgmt-07
// §3).
func TestMetrics_ServesScrapeTextJSON(t *testing.T) {
	e := scrapeEnv(t, nil, func() ([]byte, error) { return []byte(fakeScrape), nil })
	rec := e.get("/api/metrics")
	wantStatus(t, rec, http.StatusOK)
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	if v := rec.Header().Get("Vary"); !strings.Contains(v, "Authorization") {
		t.Errorf("Vary = %q, want it to contain Authorization", v)
	}
	if rec.Header().Get("ETag") == "" {
		t.Error("a 200 metrics response must carry an ETag")
	}
	out := decode[metricsResponse](t, rec)
	if out.Text != fakeScrape {
		t.Errorf("text = %q, want %q", out.Text, fakeScrape)
	}
}

// TestMetrics_ETag304: replaying the ETag as If-None-Match ⇒ 304, empty body,
// same validator.
func TestMetrics_ETag304(t *testing.T) {
	e := scrapeEnv(t, nil, func() ([]byte, error) { return []byte(fakeScrape), nil })
	first := e.get("/api/metrics")
	wantStatus(t, first, http.StatusOK)
	etag := first.Header().Get("ETag")

	second := e.do(http.MethodGet, "/api/metrics", nil, map[string]string{"If-None-Match": etag})
	wantStatus(t, second, http.StatusNotModified)
	if second.Body.Len() != 0 {
		t.Errorf("304 carried a body of %d bytes", second.Body.Len())
	}
	if got := second.Header().Get("ETag"); got != etag {
		t.Errorf("304 ETag = %q, want %q", got, etag)
	}
}

// TestMetrics_TelemetryDisabled501: a nil MetricsScrape (telemetry disabled) ⇒
// 501 not_implemented naming telemetry.enabled, never a fabricated empty matrix
// (spec §2.2 / R10).
func TestMetrics_TelemetryDisabled501(t *testing.T) {
	e := newEnv(t, false, nil) // no scrape seam → telemetry disabled
	rec := e.get("/api/metrics")
	wantStatus(t, rec, http.StatusNotImplemented)
	out := decode[errorEnvelope](t, rec)
	if out.Error != errNotImplemented {
		t.Errorf("error = %q, want %q", out.Error, errNotImplemented)
	}
	if !strings.Contains(out.Reason, "telemetry is disabled") {
		t.Errorf("reason must name the telemetry switch, got %q", out.Reason)
	}
}

// TestMetrics_ScrapeError503: a failing scrape ⇒ 503 unavailable with a fixed
// reason, Retry-After: 1, and the injected error text never reaches the client
// (spec §2.2).
func TestMetrics_ScrapeError503(t *testing.T) {
	e := scrapeEnv(t, nil, func() ([]byte, error) { return nil, errors.New("boom-secret-detail") })
	rec := e.get("/api/metrics")
	wantStatus(t, rec, http.StatusServiceUnavailable)
	out := decode[errorEnvelope](t, rec)
	if out.Error != errUnavailable {
		t.Errorf("error = %q, want %q", out.Error, errUnavailable)
	}
	if out.Reason != "metrics scrape failed" {
		t.Errorf("reason = %q, want the fixed %q", out.Reason, "metrics scrape failed")
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
	if strings.Contains(rec.Body.String(), "boom-secret-detail") {
		t.Error("the injected scrape error text leaked into the response body")
	}
}

// TestMetrics_ResponseCapHonest: a scrape larger than
// limits.max_response_bytes ⇒ the standard over-cap 503 envelope, never a
// silently truncated JSON body (spec §3 / R12).
func TestMetrics_ResponseCapHonest(t *testing.T) {
	big := strings.Repeat("amqp_alpha_total 1\n", 200) // > 64 bytes once wrapped
	e := scrapeEnv(t, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.MaxResponseBytes = 64
	}, func() ([]byte, error) { return []byte(big), nil })
	rec := e.get("/api/metrics")
	wantStatus(t, rec, http.StatusServiceUnavailable)
	out := decode[errorEnvelope](t, rec)
	if out.Error != errUnavailable {
		t.Errorf("error = %q, want %q", out.Error, errUnavailable)
	}
	if !strings.Contains(out.Reason, "max_response_bytes") {
		t.Errorf("reason = %q, want it to name the response cap", out.Reason)
	}
	if strings.HasPrefix(strings.TrimSpace(rec.Body.String()), `{"text"`) {
		t.Error("over-cap response was a truncated JSON matrix, not an error envelope")
	}
}

// TestMetrics_AuthRequired401: no credential ⇒ 401 with a Basic challenge.
func TestMetrics_AuthRequired401(t *testing.T) {
	e := basicEnv(t, true, nil)
	// basicEnv does not inject a scrape seam, but the auth gate fires first.
	rec := e.get("/api/metrics")
	wantStatus(t, rec, http.StatusUnauthorized)
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Errorf("WWW-Authenticate = %q, want a Basic challenge", got)
	}
}

// TestMetrics_RequiresReadTopology403: a user whose tags grant nothing is
// refused 403 access_refused on the metrics read too (spec §2.2 auth model).
func TestMetrics_RequiresReadTopology403(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"plain","password_hash":"`+secretHash(t)+`",
		"tags":[],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	rec := e.do(http.MethodGet, "/api/metrics", nil, map[string]string{
		"Authorization": "Basic " + basicToken("plain", "secret"),
	})
	wantStatus(t, rec, http.StatusForbidden)
	out := decode[errorEnvelope](t, rec)
	if out.Error != errAccessRefused {
		t.Errorf("error = %q, want %q", out.Error, errAccessRefused)
	}
}

// TestMetrics_NotVhostScoped: a principal holding read_topology but NO
// VHostPermissions entry for "/" still gets 200 from /api/metrics, while an
// entity route (/api/queues) is 403. Pins that isEntityReadRoute was NOT
// extended (spec §2.2 / test 10).
func TestMetrics_NotVhostScoped(t *testing.T) {
	// An inherit-mode user holding "management" (read_topology) and no vhost
	// permissions.
	inherit := inheritEnv(t, `{"users":[{"username":"mon","password_hash":"`+secretHash(t)+`",
		"tags":["management"],"vhost_permissions":[]}]}`, nil)
	// The inherit env has no scrape seam, so install one directly.
	inherit.mgmt.opts.MetricsScrape = func() ([]byte, error) { return []byte(fakeScrape), nil }

	hdr := map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")}
	if rec := inherit.do(http.MethodGet, "/api/metrics", nil, hdr); rec.Code != http.StatusOK {
		t.Fatalf("read_topology without a vhost permission must still read metrics: got %d (%s)", rec.Code, rec.Body.String())
	}
	if rec := inherit.do(http.MethodGet, "/api/queues", nil, hdr); rec.Code != http.StatusForbidden {
		t.Fatalf("an entity read without a vhost permission must be 403, got %d (%s)", rec.Code, rec.Body.String())
	}
}

// TestMetrics_MethodNotAllowed: any verb but GET ⇒ 405 with Allow: GET.
func TestMetrics_MethodNotAllowed(t *testing.T) {
	e := scrapeEnv(t, nil, func() ([]byte, error) { return []byte(fakeScrape), nil })
	rec := e.do(http.MethodPost, "/api/metrics", nil, nil)
	wantStatus(t, rec, http.StatusMethodNotAllowed)
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow = %q, want GET", got)
	}
}

// TestMetrics_ListedInVersionEndpoints: the route is discoverable from
// GET /api/strangeq/version (which needs a real broker for its capability
// report).
func TestMetrics_ListedInVersionEndpoints(t *testing.T) {
	e := newEnv(t, true, nil)
	e.mgmt.opts.MetricsScrape = func() ([]byte, error) { return []byte(fakeScrape), nil }
	rec := e.get("/api/strangeq/version")
	wantStatus(t, rec, http.StatusOK)
	out := decode[versionResponse](t, rec)
	found := false
	for _, ep := range out.Endpoints {
		if ep == "GET /api/metrics" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("endpoints %v does not list GET /api/metrics", out.Endpoints)
	}
}

// TestMetrics_IsolationGateStillPasses pins that the new handler runs on the
// forbidden-lock-free path: it returns within a bounded deadline while the
// mechanical AST gate (TestForbiddenLockGate_ASTScan) covers the static side
// (spec §8.2 test 13).
func TestMetrics_IsolationGateStillPasses(t *testing.T) {
	e := scrapeEnv(t, nil, func() ([]byte, error) { return []byte(fakeScrape), nil })
	done := make(chan int, 1)
	go func() { done <- e.get("/api/metrics").Code }()
	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Fatalf("GET /api/metrics = %d, want 200", code)
		}
	case <-time.After(time.Second):
		t.Fatal("GET /api/metrics did not return within 1s")
	}
}
