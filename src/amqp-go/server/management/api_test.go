package management

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// --- lifecycle / zero cost ---

// TestDisabled_ZeroCost: a disabled surface binds nothing, starts nothing, and
// costs nothing (SEC-001/014, spec §11 test 6).
func TestDisabled_ZeroCost(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Management.Enabled = false
	m, err := New(Options{Config: cfg, Logger: nil})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if m.Enabled() {
		t.Fatalf("Enabled() must be false")
	}
	if err := m.Start(); err != nil {
		t.Fatalf("disabled Start must be a no-op, got %v", err)
	}
	if m.Addr() != nil {
		t.Fatalf("disabled surface must have no address")
	}
	if err := m.Shutdown(contextBackground()); err != nil {
		t.Fatalf("disabled Shutdown: %v", err)
	}
}

// TestStartStop_Repeated: N cycles leave the goroutine count stable
// (NFR-040/OPS-044, spec §11 test 8).
func TestStartStop_Repeated(t *testing.T) {
	e := newEnv(t, false, func(cfg *config.AMQPConfig) {
		cfg.Management.Port = freePort(t)
	})
	base := settleGoroutines(t)
	for i := 0; i < 5; i++ {
		if err := e.mgmt.Start(); err != nil {
			t.Fatalf("cycle %d Start: %v", i, err)
		}
		if e.mgmt.Addr() == nil {
			t.Fatalf("cycle %d: no bound address", i)
		}
		if err := e.mgmt.Shutdown(contextWithTimeout(5 * time.Second)); err != nil {
			t.Fatalf("cycle %d Shutdown: %v", i, err)
		}
	}
	if got := settleGoroutines(t); got > base+3 {
		t.Fatalf("goroutine leak: base=%d after=%d", base, got)
	}
}

// TestShutdown_Bounded: Shutdown returns well within the 5 s budget
// (OPS-043/NFR-024, spec §11 test 7).
func TestShutdown_Bounded(t *testing.T) {
	e := newEnv(t, false, func(cfg *config.AMQPConfig) {
		cfg.Management.Port = freePort(t)
	})
	if err := e.mgmt.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- e.mgmt.Shutdown(contextWithTimeout(5 * time.Second)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Shutdown took %s, want < 5s", elapsed)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("Shutdown did not return within 6s")
	}
}

// TestStartup_SecurityWarnings: a non-loopback bind without TLS warns and pins
// amqp_management_insecure_bind (SEC-110, spec §11 test 5).
func TestStartup_SecurityWarnings(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Management.Enabled = true
	cfg.Management.Address = "0.0.0.0"
	cfg.Management.Authentication = config.ManagementAuthenticationNone
	warns := cfg.ManagementSecurityWarnings()
	found := false
	for _, w := range warns {
		if strings.Contains(w, "non-loopback") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a non-loopback warning, got %v", warns)
	}

	// "none" on a non-loopback bind is REFUSED before serving (SEC-111).
	if _, err := New(Options{Config: cfg}); err == nil {
		t.Fatalf("New must refuse none-on-non-loopback")
	}

	// With auth inherited and the broker auth disabled, the allow flag is
	// required on loopback; the insecure-bind gauge is then set for a
	// non-loopback basic bind.
	cfg2 := config.DefaultConfig()
	cfg2.Management.Enabled = true
	cfg2.Management.Address = "0.0.0.0"
	cfg2.Management.Authentication = config.ManagementAuthenticationBasic
	cfg2.Management.Basic.Username = "a"
	cfg2.Management.Basic.PasswordHash = secretHash(t)
	m, err := New(Options{Config: cfg2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !m.insecureBind.Load() {
		t.Fatalf("insecure bind gauge must be set for a non-loopback plaintext bind")
	}
}

// --- conventions ---

// TestEnvelope_PageVsBare (FR-CONV-052, spec §11 test 22).
func TestEnvelope_PageVsBare(t *testing.T) {
	e := basicEnv(t, true, nil)
	for i := 0; i < 3; i++ {
		rec := e.do(http.MethodPut, "/api/queues/%2F/env-"+string(rune('a'+i)),
			e.jsonBody(t, queueDeclareRequest{Durable: true, AutoDelete: true, Exclusive: true}), basicAuthHeader())
		wantStatus(t, rec, http.StatusCreated)
	}
	bare := e.do(http.MethodGet, "/api/queues", nil, basicAuthHeader())
	wantStatus(t, bare, http.StatusOK)
	if !strings.HasPrefix(strings.TrimSpace(bare.Body.String()), "[") {
		t.Fatalf("page absent must yield a bare array, got %s", truncate(bare.Body.String()))
	}
	envRec := e.do(http.MethodGet, "/api/queues?page=1&page_size=2", nil, basicAuthHeader())
	wantStatus(t, envRec, http.StatusOK)
	env := decode[pageEnvelope](t, envRec)
	if env.TotalCount < 3 || env.PageCount < 2 || env.Page != 1 || env.PageSize != 2 {
		t.Fatalf("envelope = %+v", env)
	}
}

// TestEnvelope_LinkHeaders (FR-CONV-053, spec §11 test 23).
func TestEnvelope_LinkHeaders(t *testing.T) {
	e := basicEnv(t, true, nil)
	for i := 0; i < 3; i++ {
		rec := e.do(http.MethodPut, "/api/queues/%2F/link-"+string(rune('a'+i)),
			e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
		wantStatus(t, rec, http.StatusCreated)
	}
	rec := e.do(http.MethodGet, "/api/queues?page=1&page_size=1", nil, basicAuthHeader())
	link := rec.Header().Get("Link")
	if !strings.Contains(link, `rel="next"`) {
		t.Fatalf("Link = %q, want a next relation", link)
	}
	if strings.Contains(link, `rel="prev"`) {
		t.Fatalf("page 1 must not advertise prev: %q", link)
	}
}

// TestPagination_PageSizeOverMax400 (FR-CONV-050, spec §11 test 24).
func TestPagination_PageSizeOverMax400(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/queues?page=1&page_size=99999", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusBadRequest)
	if rec := e.do(http.MethodGet, "/api/queues?page=0", nil, basicAuthHeader()); rec.Code != http.StatusBadRequest {
		t.Fatalf("page=0 must be 400, got %d", rec.Code)
	}
}

// TestSort_UnknownField400 and TestSort_MessageStats400 (FR-CONV-054).
func TestSort_UnknownField400(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/queues?sort=nonsense", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusBadRequest)

	msg := e.do(http.MethodGet, "/api/queues?sort=message_stats", nil, basicAuthHeader())
	wantStatus(t, msg, http.StatusBadRequest)
	env := decode[errorEnvelope](t, msg)
	if !strings.Contains(env.Reason, "message_stats") {
		t.Fatalf("reason must mention message_stats, got %q", env.Reason)
	}
	// A known field sorts.
	if ok := e.do(http.MethodGet, "/api/queues?sort=name&sort_reverse=true", nil, basicAuthHeader()); ok.Code != http.StatusOK {
		t.Fatalf("sort=name must be accepted, got %d", ok.Code)
	}
}

// TestColumns_Unknown400 (FR-CONV-055, spec §11 test 26).
func TestColumns_Unknown400(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/queues?columns=not_a_field", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusBadRequest)
	ok := e.do(http.MethodGet, "/api/queues?columns=name,vhost", nil, basicAuthHeader())
	wantStatus(t, ok, http.StatusOK)
}

// TestErrorEnvelope_HasRequestID (SEC-081, spec §11 test 28).
func TestErrorEnvelope_HasRequestID(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/queues/other", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusNotFound)
	var env errorEnvelope
	if err := jsonUnmarshalStrict(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error != errNotFound || env.Reason != "unknown vhost" {
		t.Fatalf("envelope = %+v", env)
	}
	if env.RequestID == "" {
		t.Fatalf("error envelope must carry request_id")
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatalf("X-Request-Id header must be present")
	}
}

// TestStatus405HasAllow (spec §11 test 29).
func TestStatus405HasAllow(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPatch, "/api/overview", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusMethodNotAllowed)
	if !strings.Contains(rec.Header().Get("Allow"), http.MethodGet) {
		t.Fatalf("Allow = %q, want GET", rec.Header().Get("Allow"))
	}
}

// TestAccept406 / TestContentType415 (FR-CONV-010/011, spec §11 test 30).
func TestAccept406(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/overview", nil, map[string]string{"Accept": "text/html"})
	wantStatus(t, rec, http.StatusNotAcceptable)
}

func TestContentType415(t *testing.T) {
	e := basicEnv(t, true, nil)
	req := e.do(http.MethodPut, "/api/queues/%2F/ct-q", []byte(`{"durable":true}`), map[string]string{
		"Authorization": basicAuthHeader()["Authorization"],
		"Content-Type":  "text/plain",
	})
	wantStatus(t, req, http.StatusUnsupportedMediaType)
}

// TestVhost_OnlyPercent2F (FR-CONV-003, spec §11 test 31).
func TestVhost_OnlyPercent2F(t *testing.T) {
	e := basicEnv(t, true, nil)
	if ok := e.do(http.MethodGet, "/api/queues/%2F", nil, basicAuthHeader()); ok.Code != http.StatusOK {
		t.Fatalf("vhost %%2F must be accepted, got %d (%s)", ok.Code, ok.Body.String())
	}
	bad := e.do(http.MethodGet, "/api/queues/other", nil, basicAuthHeader())
	wantStatus(t, bad, http.StatusNotFound)
	var env errorEnvelope
	if err := jsonUnmarshalStrict(bad.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Reason != "unknown vhost" {
		t.Fatalf("reason = %q, want unknown vhost", env.Reason)
	}
}

// TestCacheHeaders_Sec045 (MIL-015, spec §11 test 32).
func TestCacheHeaders_Sec045(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := rec.Header().Get("Vary"); !strings.Contains(got, "Authorization") {
		t.Fatalf("Vary = %q, want Authorization", got)
	}
}

// TestSecurityHeaders_OnAPIAndUI (SEC-031/032/035, spec §11 test 33).
func TestSecurityHeaders_OnAPIAndUI(t *testing.T) {
	e := basicEnv(t, true, nil)
	api := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	for _, h := range []string{"Content-Security-Policy", "X-Content-Type-Options", "Referrer-Policy", "X-Frame-Options"} {
		if api.Header().Get(h) == "" {
			t.Fatalf("API response missing %s", h)
		}
	}
	ui := e.get("/")
	wantStatus(t, ui, http.StatusOK)
	for _, h := range []string{"Content-Security-Policy", "X-Content-Type-Options", "Referrer-Policy", "X-Frame-Options"} {
		if ui.Header().Get(h) == "" {
			t.Fatalf("UI response missing %s", h)
		}
	}
	if !strings.Contains(ui.Header().Get("Content-Security-Policy"), "default-src 'self'") {
		t.Fatalf("CSP = %q", ui.Header().Get("Content-Security-Policy"))
	}
	if e.get("/does-not-exist").Code != http.StatusNotFound {
		t.Fatalf("unknown UI path must be 404")
	}
}

// TestETag_DeterministicAnd304 (FR-CONV-064, spec §11 test 34).
func TestETag_DeterministicAnd304(t *testing.T) {
	e := basicEnv(t, true, nil)
	a := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	b := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	tag := a.Header().Get("ETag")
	if tag == "" || tag != b.Header().Get("ETag") {
		t.Fatalf("ETag must be present and stable: %q vs %q", tag, b.Header().Get("ETag"))
	}
	if a.Body.String() != b.Body.String() {
		t.Fatalf("same logical response must serialise identically")
	}
	cond := e.do(http.MethodGet, "/api/overview", nil, map[string]string{
		"Authorization": basicAuthHeader()["Authorization"],
		"If-None-Match": tag,
	})
	wantStatus(t, cond, http.StatusNotModified)
	if cond.Body.Len() != 0 {
		t.Fatalf("304 must have an empty body, got %q", cond.Body.String())
	}
}

// TestETag_ListChangesWithPageContent (spec §11 test 35).
func TestETag_ListChangesWithPageContent(t *testing.T) {
	e := basicEnv(t, true, nil)
	before := e.do(http.MethodGet, "/api/queues", nil, basicAuthHeader()).Header().Get("ETag")
	rec := e.do(http.MethodPut, "/api/queues/%2F/etag-q", e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	after := e.do(http.MethodGet, "/api/queues", nil, basicAuthHeader()).Header().Get("ETag")
	if before == after {
		t.Fatalf("ETag must change when the list content changes: %q", before)
	}
}

// noTotalsBroker embeds a UnifiedBroker so it satisfies the interface but
// deliberately does NOT promote the optional BOB-1/BOB-8 accessor seams
// (QueueMessageCounts / ConsumerViews / QueueReadyCount), none of which are part
// of UnifiedBroker. It models a custom broker on which message totals are
// genuinely unknowable, so the management layer must render null (never 0) with
// stats_partial:true.
type noTotalsBroker struct {
	server.UnifiedBroker
}

// TestNullDiscipline_NeverZero (FR-CONV-041, spec §11 test 37).
func TestNullDiscipline_NeverZero(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/queues/%2F/null-q", e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)

	// Known path: BOB-1 is built, so a queue with a live QueueState reports a
	// real 0 (Known=true) with stats_partial:false — null would be a lie here.
	known := e.do(http.MethodGet, "/api/queues?page=1", nil, basicAuthHeader()).Body.String()
	if !strings.Contains(known, `"messages_unacknowledged":0`) || !strings.Contains(known, `"stats_partial":false`) {
		t.Fatalf("a live queue's known totals must be 0/false, got %s", truncate(known))
	}

	// Unknown path: strip the optional BOB-1/BOB-8 accessor seams so the totals
	// are genuinely unknowable. The null discipline must hold — an unknown stat
	// is null, never 0 — and stats_partial must be true.
	e.srv.Broker = noTotalsBroker{UnifiedBroker: e.srv.Broker}

	body := e.do(http.MethodGet, "/api/queues?page=1", nil, basicAuthHeader()).Body.String()
	if !strings.Contains(body, `"messages_unacknowledged":null`) {
		t.Fatalf("messages_unacknowledged must be null when unknown, got %s", truncate(body))
	}
	if !strings.Contains(body, `"stats_partial":true`) {
		t.Fatalf("stats_partial must be true when a stat is unknown, got %s", truncate(body))
	}
	if !strings.Contains(body, `"messages_ready":`) {
		t.Fatalf("messages_ready must be present, got %s", truncate(body))
	}
}

// TestDisableStats_RetainsReadyDropsRates (FR-CONV-056, spec §11 test 27).
func TestDisableStats_RetainsReadyDropsRates(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/queues/%2F/ds-q", e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	body := e.do(http.MethodGet, "/api/queues?page=1&disable_stats=true", nil, basicAuthHeader()).Body.String()
	for _, must := range []string{`"messages_ready"`, `"messages_unacknowledged"`, `"messages"`, `"consumers"`} {
		if !strings.Contains(body, must) {
			t.Fatalf("disable_stats must retain %s, got %s", must, truncate(body))
		}
	}
	for _, mustNot := range []string{`"message_stats"`, `"consumer_utilisation"`, `"messages_persistent"`, `"idle_since"`} {
		if strings.Contains(body, mustNot) {
			t.Fatalf("disable_stats must drop %s, got %s", mustNot, truncate(body))
		}
	}
}

// TestLocation_ControlCharRejected (SEC-085, spec §11 test 36).
func TestLocation_ControlCharRejected(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/queues/%2F/cr%0Alf", e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
	if rec.Code != http.StatusCreated {
		t.Fatalf("declare with a control-char name: got %d (%s)", rec.Code, rec.Body.String())
	}
	loc := rec.Header().Get("Location")
	if strings.ContainsAny(loc, "\r\n") {
		t.Fatalf("Location must not contain CR/LF, got %q", loc)
	}
}

// --- reads ---

// TestVersion_CapabilitiesKeysAndEndpoints (FR-API-006, spec §11 test 40).
func TestVersion_CapabilitiesKeysAndEndpoints(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/strangeq/version", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	body := rec.Body.String()
	for _, key := range []string{
		"queue_message_totals", "channels", "bindings", "close_connection", "user_crud",
		"tag_capabilities", "consumer_counts", "atomic_import", "message_rates",
		"vhost_scoping", "coherent_snapshot", "unauthenticated",
	} {
		if !strings.Contains(body, `"`+key+`"`) {
			t.Fatalf("capabilities must always carry %q, got %s", key, truncate(body))
		}
	}
	out := decode[versionResponse](t, rec)
	if out.Capabilities.Bindings || out.Capabilities.UserCRUD || out.Capabilities.MessageRates {
		t.Fatalf("BOB-2/4/6 capabilities must report false: %+v", out.Capabilities)
	}
	if !out.Capabilities.CloseConnection {
		t.Fatalf("close_connection must be true (the operation exists)")
	}
	if len(out.Endpoints) == 0 {
		t.Fatalf("endpoints must be a non-empty sibling of capabilities")
	}
}

// TestHealth_OkAlarmNotReady (OPS-010, spec §11 test 41).
func TestHealth_OkAlarmNotReady(t *testing.T) {
	e := basicEnv(t, true, nil)
	// Not listening yet.
	notReady := e.do(http.MethodGet, "/api/health", nil, basicAuthHeader())
	wantStatus(t, notReady, http.StatusServiceUnavailable)
	if out := decode[healthResponse](t, notReady); out.Status != "not_ready" {
		t.Fatalf("status = %q, want not_ready", out.Status)
	}
	// Pretend the broker is listening.
	ln := &stubListener{addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}, closed: make(chan struct{})}
	e.srv.Listener = ln
	t.Cleanup(func() { _ = ln.Close() })
	ok := e.do(http.MethodGet, "/api/health", nil, basicAuthHeader())
	wantStatus(t, ok, http.StatusOK)
	if out := decode[healthResponse](t, ok); out.Status != "ok" {
		t.Fatalf("status = %q, want ok", out.Status)
	}
}

// TestHealthChecks_NamesAndUnanswerable501 (OPS-012/DIV-15, spec §11 test 42).
func TestHealthChecks_NamesAndUnanswerable501(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/health/checks", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	out := decode[checksResponse](t, rec)
	names := map[string]bool{}
	for _, c := range out.Checks {
		names[c.Name] = true
		if c.Status == "" {
			t.Fatalf("check %q has an empty status", c.Name)
		}
	}
	for _, want := range []string{"is-in-service", "local-alarms", "below-node-connection-limit", "aliveness", "disk-space"} {
		if !names[want] {
			t.Fatalf("missing health check %q (got %v)", want, names)
		}
	}
	unanswerable := e.do(http.MethodGet, "/api/health/checks/virtual-hosts", nil, basicAuthHeader())
	wantStatus(t, unanswerable, http.StatusNotImplemented)
	if e.do(http.MethodGet, "/api/health/checks/nope", nil, basicAuthHeader()).Code != http.StatusNotFound {
		t.Fatalf("unknown check must be 404")
	}
}

// TestNodes_SyntheticAnd404 (D-2, spec §11 test 43).
func TestNodes_SyntheticAnd404(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/nodes", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	nodes := decode[[]nodeInfo](t, rec)
	if len(nodes) != 1 || nodes[0].Name != e.mgmt.nodeName || nodes[0].Type != "disc" || !nodes[0].Running {
		t.Fatalf("nodes = %+v", nodes)
	}
	if got := e.do(http.MethodGet, "/api/nodes/"+e.mgmt.nodeName, nil, basicAuthHeader()); got.Code != http.StatusOK {
		t.Fatalf("known node must be 200, got %d", got.Code)
	}
	if got := e.do(http.MethodGet, "/api/nodes/nope@host", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("unknown node must be 404, got %d", got.Code)
	}
}

// TestVhosts_SyntheticOneElement (D-1, spec §11 test 50).
func TestVhosts_SyntheticOneElement(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/vhosts", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	vs := decode[[]vhostInfo](t, rec)
	if len(vs) != 1 || vs[0].Name != "/" || !vs[0].Synthetic {
		t.Fatalf("vhosts = %+v", vs)
	}
	if got := e.do(http.MethodGet, "/api/vhosts/%2F", nil, basicAuthHeader()); got.Code != http.StatusOK {
		t.Fatalf("GET /api/vhosts/%%2F must be 200, got %d", got.Code)
	}
}

// TestUsers_NoHashes (SEC-070, spec §11 test 51) and empty source (§11 test 52).
func TestUsers_NoHashes(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	rec := e.do(http.MethodGet, "/api/users", nil, map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")})
	wantStatus(t, rec, http.StatusOK)
	body := rec.Body.String()
	if strings.Contains(body, "password_hash") || strings.Contains(body, "secretHash") {
		t.Fatalf("the user list must never expose a hash: %s", truncate(body))
	}
	users := decode[[]userInfo](t, rec)
	if len(users) != 1 || users[0].Name != "adm" {
		t.Fatalf("users = %+v", users)
	}
}

func TestUsers_AuthDisabledEmpty(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthenticationFilePath = ""
	})
	rec := e.do(http.MethodGet, "/api/users", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
		t.Fatalf("an absent user store must yield [], got %s", body)
	}
}

// TestPermissions_FromAuthFile (FR-API-113, spec §11 test 53).
func TestPermissions_FromAuthFile(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":"^q.*$","write":"^w.*$","read":"^r.*$"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")}
	rec := e.do(http.MethodGet, "/api/permissions", nil, hdr)
	wantStatus(t, rec, http.StatusOK)
	perms := decode[[]permissionInfo](t, rec)
	if len(perms) != 1 {
		t.Fatalf("permissions = %+v", perms)
	}
	got := perms[0]
	if got.User != "adm" || got.VHost != "/" || got.Configure != "^q.*$" || got.Write != "^w.*$" || got.Read != "^r.*$" {
		t.Fatalf("permission triple = %+v", got)
	}
	single := e.do(http.MethodGet, "/api/permissions/%2F/adm", nil, hdr)
	wantStatus(t, single, http.StatusOK)
}

// TestHonestEmpties_200Empty (FR-API-130, spec §11 test 55).
func TestHonestEmpties_200Empty(t *testing.T) {
	e := basicEnv(t, true, nil)
	for _, p := range []string{
		"/api/policies", "/api/operator-policies", "/api/parameters",
		"/api/global-parameters", "/api/feature-flags", "/api/deprecated-features",
		"/api/deprecated-features/used", "/api/vhost-limits", "/api/user-limits",
		"/api/federation-links", "/api/topic-permissions", "/api/shovels",
	} {
		rec := e.do(http.MethodGet, p, nil, basicAuthHeader())
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", p, rec.Code)
		}
		if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
			t.Fatalf("GET %s = %s, want []", p, truncate(body))
		}
	}
}

// TestHonestWrites_501 (FR-API-131, spec §11 test 56) and bindings 501
// (spec §11 test 72).
func TestHonestWrites_501(t *testing.T) {
	e := basicEnv(t, true, nil)
	cases := []struct {
		method, path string
	}{
		{http.MethodPut, "/api/policies"},
		{http.MethodPut, "/api/parameters"},
		{http.MethodDelete, "/api/feature-flags"},
		{http.MethodPut, "/api/users/someone"},
		{http.MethodDelete, "/api/permissions/%2F/someone"},
		{http.MethodPost, "/api/strangeq/rebalance"},
		{http.MethodPut, "/api/vhosts/other"},
		{http.MethodPost, "/api/nodes/" + "n@h"},
		{http.MethodGet, "/api/bindings"},
		{http.MethodGet, "/api/bindings/%2F"},
		{http.MethodGet, "/api/queues/%2F/q/bindings"},
	}
	for _, c := range cases {
		rec := e.do(c.method, c.path, []byte(`{}`), basicAuthHeader())
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("%s %s = %d, want 501 (%s)", c.method, c.path, rec.Code, truncate(rec.Body.String()))
		}
		var env errorEnvelope
		if err := jsonUnmarshalStrict(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if env.Error != errNotImplemented {
			t.Fatalf("%s %s token = %q, want not_implemented", c.method, c.path, env.Error)
		}
	}
	// DELETE /api/vhosts/%2F is 409 (the default vhost cannot be deleted).
	del := e.do(http.MethodDelete, "/api/vhosts/%2F", nil, basicAuthHeader())
	wantStatus(t, del, http.StatusConflict)
}

// TestDefinitionsExport_NoHashesAndSubset (MIL-040, spec §11 test 73) and the
// manage_users gate on the user section (AUTH-024, spec §11 test 74).
func TestDefinitionsExport_NoHashesAndSubset(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]},
		{"username":"mon","password_hash":"`+secretHash(t)+`","tags":["monitoring"],
		"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)

	adm := e.do(http.MethodGet, "/api/definitions", nil, map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")})
	wantStatus(t, adm, http.StatusOK)
	body := adm.Body.String()
	if strings.Contains(body, "password_hash") || strings.Contains(body, "$2") {
		t.Fatalf("definitions export must never carry a hash: %s", truncate(body))
	}
	if !strings.Contains(body, `"subset":true`) {
		t.Fatalf("export must declare subset:true: %s", truncate(body))
	}
	var admDoc map[string]json.RawMessage
	if err := jsonUnmarshalStrict(adm.Body.Bytes(), &admDoc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := admDoc["users"]; !ok {
		t.Fatalf("administrator export must include the users key: %s", truncate(body))
	}

	mon := e.do(http.MethodGet, "/api/definitions", nil, map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")})
	wantStatus(t, mon, http.StatusOK)
	var monDoc map[string]json.RawMessage
	if err := jsonUnmarshalStrict(mon.Body.Bytes(), &monDoc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := monDoc["users"]; ok {
		t.Fatalf("a principal without manage_users must not receive the user section: %s", truncate(mon.Body.String()))
	}
	if string(monDoc["bindings"]) != "[]" {
		t.Fatalf("export must include an empty bindings list, got %s", string(monDoc["bindings"]))
	}
}

// helpers ---------------------------------------------------------------

type stubListener struct {
	addr   net.Addr
	closed chan struct{}
}

func (l *stubListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, errors.New("closed")
}
func (l *stubListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (l *stubListener) Addr() net.Addr { return l.addr }

func truncate(s string) string {
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}
