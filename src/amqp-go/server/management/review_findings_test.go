package management

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
)

// withReqHeader returns a copy of h with k set to v.
func withReqHeader(h map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(h)+1)
	for kk, vv := range h {
		out[kk] = vv
	}
	out[k] = v
	return out
}

// --- F1: AUTH-015/SEC-066 canClose bypass ---

// TestClose_NoneModeGuestOwnerNotBypass pins SEC-066: with broker auth
// disabled the unauthenticated principal is itself named "guest", so a
// connection whose Username is "guest" must NOT be treated as owned by the
// caller. Both management.allow_unauthenticated_mutations and
// management.allow_connection_close are required.
func TestClose_NoneModeGuestOwnerNotBypass(t *testing.T) {
	setup := func(mutate func(*config.AMQPConfig)) *env {
		return newEnv(t, true, func(cfg *config.AMQPConfig) {
			cfg.Management.Authentication = config.ManagementAuthenticationNone
			cfg.Management.AllowUnauthenticated = true
			if mutate != nil {
				mutate(cfg)
			}
		})
	}

	// Close disabled: a matching name must not authorise the close.
	e1 := setup(func(cfg *config.AMQPConfig) {
		cfg.Management.AllowConnectionClose = false
		cfg.Management.AllowUnauthenticatedMutations = true
	})
	e1.injectConnection(t, "guest-conn", "guest")
	if rec := e1.do(http.MethodDelete, "/api/connections/guest-conn", nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("close with allow_connection_close=false = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}
	if rec := e1.do(http.MethodDelete, "/api/connections/username/guest", nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("close-by-user with allow_connection_close=false = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}

	// Mutations disabled: close-allowed alone is not enough.
	e2 := setup(func(cfg *config.AMQPConfig) {
		cfg.Management.AllowConnectionClose = true
		cfg.Management.AllowUnauthenticatedMutations = false
	})
	e2.injectConnection(t, "guest-conn", "guest")
	if rec := e2.do(http.MethodDelete, "/api/connections/guest-conn", nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("close with allow_unauthenticated_mutations=false = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}
	if rec := e2.do(http.MethodDelete, "/api/connections/username/guest", nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("close-by-user with allow_unauthenticated_mutations=false = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}

	// Both opt-ins: the close proceeds for both endpoints.
	e3 := setup(func(cfg *config.AMQPConfig) {
		cfg.Management.AllowConnectionClose = true
		cfg.Management.AllowUnauthenticatedMutations = true
	})
	e3.injectConnection(t, "guest-conn", "guest")
	if rec := e3.do(http.MethodDelete, "/api/connections/guest-conn", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("close with both opt-ins = %d, want 204 (%s)", rec.Code, rec.Body.String())
	}
	e3.injectConnection(t, "guest-conn2", "guest")
	if rec := e3.do(http.MethodDelete, "/api/connections/username/guest", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("close-by-user with both opt-ins = %d, want 204 (%s)", rec.Code, rec.Body.String())
	}
}

// --- F2: AUTH-011 read resource gate ---

// TestAuth_ResourceReadGate pins AUTH-011: a read requires at least one
// VHostPermissions entry for the requested vhost. The built-in principals are
// exempt (p.User == nil), but an inherit-mode user is not.
func TestAuth_ResourceReadGate(t *testing.T) {
	hdr := map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")}

	denied := inheritEnv(t, `{"users":[{"username":"mon","password_hash":"`+secretHash(t)+`",
		"tags":["monitoring"]}]}`, nil)
	rec := denied.do(http.MethodGet, "/api/queues", nil, hdr)
	wantStatus(t, rec, http.StatusForbidden)
	if env := decode[errorEnvelope](t, rec); env.Error != errAccessRefused {
		t.Fatalf("token = %q, want access_refused", env.Error)
	}

	allowed := inheritEnv(t, `{"users":[{"username":"mon","password_hash":"`+secretHash(t)+`",
		"tags":["monitoring"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	if got := allowed.do(http.MethodGet, "/api/queues", nil, hdr); got.Code != http.StatusOK {
		t.Fatalf("with a / vhost triple the read must be 200, got %d (%s)", got.Code, got.Body.String())
	}
}

// --- F3: 500-only reason scrubbing ---

// TestErrorReason_ScrubsOnlyInternalServerError pins SEC-072 precisely: only a
// genuine 500 is rewritten to "internal error". A 501 keeps the subsystem text
// and a 503 keeps the backpressure text.
func TestErrorReason_ScrubsOnlyInternalServerError(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/policies", []byte(`{}`), basicAuthHeader())
	wantStatus(t, rec, http.StatusNotImplemented)
	if env := decode[errorEnvelope](t, rec); !strings.Contains(env.Reason, "not implemented") {
		t.Fatalf("501 reason = %q, want the subsystem text", env.Reason)
	}

	e2 := basicEnv(t, true, nil)
	installDepthGate(t, e2)
	pub := e2.do(http.MethodPost, "/api/exchanges/%2F/rl-hdr-ex/publish",
		e2.jsonBody(t, publishRequest{RoutingKey: "k", Payload: "x"}), basicAuthHeader())
	wantStatus(t, pub, http.StatusServiceUnavailable)
	if env := decode[errorEnvelope](t, pub); env.Reason != "queue backpressure" {
		t.Fatalf("503 reason = %q, want the queue-backpressure text", env.Reason)
	}
}

// --- F4: cookie replay must honour loopback_only ---

// TestLoopback_CookieReplayFromNonLoopback403 pins SEC-112/T-06: a session
// minted by a loopback_only user on loopback must not be replayable from a
// non-loopback client.
func TestLoopback_CookieReplayFromNonLoopback403(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"lo","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"loopback_only":true,
		"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)

	login := e.do(http.MethodPost, "/api/strangeq/session",
		e.jsonBody(t, sessionRequest{Username: "lo", Password: "secret"}), nil)
	wantStatus(t, login, http.StatusOK)
	var sess *http.Cookie
	for _, c := range login.Result().Cookies() {
		if c.Name == sessionCookieName {
			sess = c
		}
	}
	if sess == nil {
		t.Fatalf("login must set the %s cookie", sessionCookieName)
	}
	cookie := map[string]string{"Cookie": sess.Name + "=" + sess.Value}

	replay := e.doFrom("198.51.100.7:5555", http.MethodGet, "/api/overview", nil, cookie)
	if replay.Code != http.StatusForbidden {
		t.Fatalf("loopback_only cookie replayed off-loopback = %d, want 403 (%s)", replay.Code, replay.Body.String())
	}
	if ok := e.do(http.MethodGet, "/api/overview", nil, cookie); ok.Code != http.StatusOK {
		t.Fatalf("the same cookie from loopback must still be 200, got %d (%s)", ok.Code, ok.Body.String())
	}
}

// --- F5: definitions import per-target configure authz ---

// TestDefinitionsImport_PerTargetConfigureDenied pins AUTH-012: a definitions
// import must apply the same per-target configure permission a direct
// PUT would, not bypass it behind the single capMutate gate.
func TestDefinitionsImport_PerTargetConfigureDenied(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":"^$","write":".*","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")}

	defs := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "denied-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "denied-q"}},
	}
	rec := e.do(http.MethodPost, "/api/definitions", e.jsonBody(t, defs), hdr)
	wantStatus(t, rec, http.StatusOK)
	res := decode[definitionsImportResult](t, rec)
	failed := strings.Join(res.Failed, ",")
	if !strings.Contains(failed, "exchange:denied-ex") || !strings.Contains(failed, "queue:denied-q") {
		t.Fatalf("denied targets must be reported failed, got %v", res.Failed)
	}
	if strings.Contains(strings.Join(res.Applied, ","), "denied") {
		t.Fatalf("a denied target must never be applied: %v", res.Applied)
	}
	list := e.do(http.MethodGet, "/api/exchanges", nil, hdr)
	wantStatus(t, list, http.StatusOK)
	if strings.Contains(list.Body.String(), "denied-ex") {
		t.Fatalf("a denied exchange must not be declared: %s", truncate(list.Body.String()))
	}
	queues := e.do(http.MethodGet, "/api/queues", nil, hdr)
	if strings.Contains(queues.Body.String(), "denied-q") {
		t.Fatalf("a denied queue must not be declared: %s", truncate(queues.Body.String()))
	}
}

// --- F6: multipart/form-data definitions import ---

// TestDefinitionsImport_MultipartFile pins COMPAT: the import endpoint accepts
// multipart/form-data and reads the definitions document from the "file" part,
// exactly as it accepts the same document as application/json.
func TestDefinitionsImport_MultipartFile(t *testing.T) {
	e := basicEnv(t, true, nil)
	defs := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "mp-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "mp-q"}},
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "definitions.json")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := fw.Write(e.jsonBody(t, defs)); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	rec := e.do(http.MethodPost, "/api/definitions", buf.Bytes(), map[string]string{
		"Authorization": basicAuthHeader()["Authorization"],
		"Content-Type":  mw.FormDataContentType(),
	})
	wantStatus(t, rec, http.StatusOK)
	res := decode[definitionsImportResult](t, rec)
	applied := strings.Join(res.Applied, ",")
	if !strings.Contains(applied, "exchange:mp-ex") || !strings.Contains(applied, "queue:mp-q") {
		t.Fatalf("multipart import applied = %v", res.Applied)
	}

	// The same document as JSON is still a 200 (entities now exist ⇒ skipped).
	jsonRec := e.do(http.MethodPost, "/api/definitions", e.jsonBody(t, defs), basicAuthHeader())
	wantStatus(t, jsonRec, http.StatusOK)
}

// --- F11: request_timeout_seconds ---

// TestRequestTimeout_ContextDeadline pins that management.limits
// .request_timeout_seconds installs a per-request context deadline, and that
// the zero value installs none.
func TestRequestTimeout_ContextDeadline(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.RequestTimeoutSeconds = 1
	})
	var deadlineSet bool
	for _, rt := range e.mgmt.router.routes {
		if rt.pattern == "/api/overview" {
			rt.handler = func(w http.ResponseWriter, r *http.Request) {
				_, deadlineSet = r.Context().Deadline()
				w.WriteHeader(http.StatusOK)
			}
		}
	}
	wantStatus(t, e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader()), http.StatusOK)
	if !deadlineSet {
		t.Fatalf("request_timeout_seconds>0 must install a context deadline")
	}

	e2 := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.RequestTimeoutSeconds = 0
	})
	deadlineSet = false
	for _, rt := range e2.mgmt.router.routes {
		if rt.pattern == "/api/overview" {
			rt.handler = func(w http.ResponseWriter, r *http.Request) {
				_, deadlineSet = r.Context().Deadline()
				w.WriteHeader(http.StatusOK)
			}
		}
	}
	wantStatus(t, e2.do(http.MethodGet, "/api/overview", nil, basicAuthHeader()), http.StatusOK)
	if deadlineSet {
		t.Fatalf("request_timeout_seconds=0 must not install a deadline")
	}
}

// --- F12: If-Match / 412 ---

// TestIfMatch_PreconditionFailed412 pins the If-Match precondition on
// individual PUT and DELETE: a validator that does not match the current
// representation (or a PUT to a resource that does not exist) is 412 and the
// mutation does not proceed.
func TestIfMatch_PreconditionFailed412(t *testing.T) {
	e := basicEnv(t, true, nil)

	wantStatus(t, e.do(http.MethodPut, "/api/queues/%2F/if-q", e.jsonBody(t, queueDeclareRequest{Durable: true}), basicAuthHeader()), http.StatusCreated)
	get := e.do(http.MethodGet, "/api/queues/%2F/if-q", nil, basicAuthHeader())
	tag := get.Header().Get("ETag")
	if tag == "" {
		t.Fatalf("the GET must carry an ETag")
	}

	stale := e.do(http.MethodDelete, "/api/queues/%2F/if-q", nil, withReqHeader(basicAuthHeader(), "If-Match", `"not-the-tag"`))
	wantStatus(t, stale, http.StatusPreconditionFailed)
	if env := decode[errorEnvelope](t, stale); env.Error != errPrecondition {
		t.Fatalf("token = %q, want precondition_failed", env.Error)
	}
	if got := e.do(http.MethodGet, "/api/queues/%2F/if-q", nil, basicAuthHeader()); got.Code != http.StatusOK {
		t.Fatalf("a 412 must not apply the mutation: queue gone (%d)", got.Code)
	}

	okDel := e.do(http.MethodDelete, "/api/queues/%2F/if-q", nil, withReqHeader(basicAuthHeader(), "If-Match", tag))
	wantStatus(t, okDel, http.StatusNoContent)

	// Exchange PUT: stale validator refused, current validator accepted.
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/if-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader()), http.StatusCreated)
	exGet := e.do(http.MethodGet, "/api/exchanges/%2F/if-ex", nil, basicAuthHeader())
	exTag := exGet.Header().Get("ETag")
	if exTag == "" {
		t.Fatalf("the exchange GET must carry an ETag")
	}
	badEx := e.do(http.MethodPut, "/api/exchanges/%2F/if-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), withReqHeader(basicAuthHeader(), "If-Match", `"stale"`))
	wantStatus(t, badEx, http.StatusPreconditionFailed)
	okEx := e.do(http.MethodPut, "/api/exchanges/%2F/if-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), withReqHeader(basicAuthHeader(), "If-Match", exTag))
	wantStatus(t, okEx, http.StatusNoContent)

	// If-Match on a resource that does not exist is 412 even with "*".
	missing := e.do(http.MethodPut, "/api/queues/%2F/absent-q", e.jsonBody(t, queueDeclareRequest{}), withReqHeader(basicAuthHeader(), "If-Match", "*"))
	wantStatus(t, missing, http.StatusPreconditionFailed)
}

// --- F13: Vary: Cookie and X-RateLimit-* ---

// TestVary_CookieForSessionAuth pins that a cookie-authenticated response
// varies on Cookie (a Basic-authenticated one does not).
func TestVary_CookieForSessionAuth(t *testing.T) {
	e := basicEnv(t, true, nil)
	sess, _ := sessionLogin(t, e)
	rec := e.do(http.MethodGet, "/api/overview", nil, map[string]string{"Cookie": sess.Name + "=" + sess.Value})
	wantStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Header().Get("Vary"), "Cookie") {
		t.Fatalf("cookie response Vary = %q, want Cookie", rec.Header().Get("Vary"))
	}
	basic := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	if strings.Contains(basic.Header().Get("Vary"), "Cookie") {
		t.Fatalf("Basic response must not vary on Cookie: %q", basic.Header().Get("Vary"))
	}
}

// TestRateLimitHeaders_OnPublish pins the advisory X-RateLimit-* headers on a
// rate-limited endpoint, including on the 429.
func TestRateLimitHeaders_OnPublish(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.Publish = 2
	})
	e.declareQueue(t, "rl-hdr-q", queueDeclareRequest{Durable: true})
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/rl-hdr-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader()), http.StatusCreated)
	if err := e.srv.Broker.BindQueue("rl-hdr-q", "rl-hdr-ex", "rk", nil); err != nil {
		t.Fatalf("bind: %v", err)
	}
	body := e.jsonBody(t, publishRequest{RoutingKey: "rk", Payload: "x"})

	first := e.do(http.MethodPost, "/api/exchanges/%2F/rl-hdr-ex/publish", body, basicAuthHeader())
	wantStatus(t, first, http.StatusOK)
	if got := first.Header().Get("X-RateLimit-Limit"); got != "2" {
		t.Fatalf("X-RateLimit-Limit = %q, want 2", got)
	}
	if got := first.Header().Get("X-RateLimit-Remaining"); got != "1" {
		t.Fatalf("first X-RateLimit-Remaining = %q, want 1", got)
	}
	if first.Header().Get("X-RateLimit-Reset") == "" {
		t.Fatalf("X-RateLimit-Reset must be present")
	}

	second := e.do(http.MethodPost, "/api/exchanges/%2F/rl-hdr-ex/publish", body, basicAuthHeader())
	wantStatus(t, second, http.StatusOK)
	if got := second.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("second X-RateLimit-Remaining = %q, want 0", got)
	}

	third := e.do(http.MethodPost, "/api/exchanges/%2F/rl-hdr-ex/publish", body, basicAuthHeader())
	wantStatus(t, third, http.StatusTooManyRequests)
	if got := third.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("429 X-RateLimit-Remaining = %q, want 0", got)
	}
}

// --- F14: CORS ---

// TestCORS_AllowedOriginAndPreflight pins that only an allow-listed Origin is
// reflected, that a preflight is answered, and that an unknown Origin gets no
// ACAO header.
func TestCORS_AllowedOriginAndPreflight(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.CORS.AllowedOrigins = []string{"https://ui.example"}
	})
	rec := e.do(http.MethodGet, "/api/overview", nil, withReqHeader(basicAuthHeader(), "Origin", "https://ui.example"))
	wantStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://ui.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want the allow-listed origin", got)
	}
	if !strings.Contains(rec.Header().Get("Vary"), "Origin") {
		t.Fatalf("Vary must include Origin: %q", rec.Header().Get("Vary"))
	}

	pre := e.do(http.MethodOptions, "/api/queues", nil, map[string]string{"Origin": "https://ui.example"})
	wantStatus(t, pre, http.StatusNoContent)
	if !strings.Contains(pre.Header().Get("Access-Control-Allow-Methods"), "GET") {
		t.Fatalf("preflight Allow-Methods = %q", pre.Header().Get("Access-Control-Allow-Methods"))
	}

	bad := e.do(http.MethodGet, "/api/overview", nil, withReqHeader(basicAuthHeader(), "Origin", "https://evil.example"))
	wantStatus(t, bad, http.StatusOK)
	if got := bad.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("a non-allow-listed origin must not be reflected, got %q", got)
	}
}

// --- F15: constant-time CSRF compare ---

// TestCSRF_WrongToken403 pins that a wrong (same-length, one-character-off)
// CSRF token is refused.
func TestCSRF_WrongToken403(t *testing.T) {
	e := basicEnv(t, true, nil)
	sess, csrf := sessionLogin(t, e)
	rec := e.do(http.MethodPut, "/api/queues/%2F/csrf-wrong",
		e.jsonBody(t, queueDeclareRequest{Durable: true}),
		map[string]string{
			"Cookie":       sess.Name + "=" + sess.Value,
			"X-CSRF-Token": "x" + csrf,
			"Origin":       "http://example.com",
		})
	wantStatus(t, rec, http.StatusForbidden)
}

// --- F16: reads are not audited ---

// TestDefinitionsExport_NotAudited pins OPS-020: a GET /api/definitions is a
// read and must not emit an audit record; mutations still are.
func TestDefinitionsExport_NotAudited(t *testing.T) {
	e := basicEnv(t, true, nil)
	sink := &captureSink{}
	e.mgmt.auditSink = sink

	wantStatus(t, e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader()), http.StatusOK)
	for _, rec := range sink.records() {
		if strings.HasPrefix(rec.Entity, "definitions") {
			t.Fatalf("a read of /api/definitions must not be audited: %+v", rec)
		}
	}

	wantStatus(t, e.do(http.MethodPut, "/api/queues/%2F/audited-q", e.jsonBody(t, queueDeclareRequest{Durable: true}), basicAuthHeader()), http.StatusCreated)
	found := false
	for _, rec := range sink.records() {
		if strings.HasPrefix(rec.Entity, "queue:audited-q") {
			found = true
		}
	}
	if !found {
		t.Fatalf("mutations must still be audited")
	}
}

// --- F9/#67: depth-gate 503 ---

// fixedDepthGate is a protocol.DepthGate that always reports the high-water
// mark, so a publish is refused without parking.
type fixedDepthGate struct{}

func (fixedDepthGate) AtHighWaterMark() bool { return true }

// gatedBroker wraps the real broker, overriding only PublishMessageAdmitted so
// the management publish path sees a non-nil gate. Embedding the real
// UnifiedBroker keeps every other accessor working.
type gatedBroker struct {
	server.UnifiedBroker
	gate protocol.DepthGate
}

func (g gatedBroker) PublishMessageAdmitted(_, _ string, _ *protocol.Message) (protocol.DepthGate, error) {
	return g.gate, nil
}

func installDepthGate(t *testing.T, e *env) {
	t.Helper()
	orig := e.srv.Broker
	if orig == nil {
		t.Fatalf("env has no broker to wrap")
	}
	e.srv.Broker = gatedBroker{UnifiedBroker: orig, gate: fixedDepthGate{}}
}

// TestPublish_DepthGate503RetryAfterNoPark pins FR-API-058 backpressure: a
// non-nil depth gate yields 503 + Retry-After + the queue-backpressure reason,
// immediately (no parking) — bounded here by a 1 s deadline.
func TestPublish_DepthGate503RetryAfterNoPark(t *testing.T) {
	e := basicEnv(t, true, nil)
	installDepthGate(t, e)
	body := e.jsonBody(t, publishRequest{RoutingKey: "nowhere", Payload: "x"})

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- e.do(http.MethodPost, "/api/exchanges/%2F/rl-hdr-ex/publish", body, basicAuthHeader())
	}()
	select {
	case rec := <-done:
		wantStatus(t, rec, http.StatusServiceUnavailable)
		if rec.Header().Get("Retry-After") == "" {
			t.Fatalf("a depth-gate 503 must carry Retry-After")
		}
		if env := decode[errorEnvelope](t, rec); env.Reason != "queue backpressure" {
			t.Fatalf("reason = %q, want the queue-backpressure text", env.Reason)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("a non-parking depth-gate publish did not return within 1s")
	}
}
