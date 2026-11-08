package management

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
)

// TestAuth_MissingCredential401 (AUTH-006, spec §11 test 9).
func TestAuth_MissingCredential401(t *testing.T) {
	e := inheritEnv(t, `{"users":[]}`, nil)
	rec := e.get("/api/overview")
	wantStatus(t, rec, http.StatusUnauthorized)
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Fatalf("WWW-Authenticate = %q, want a Basic challenge", got)
	}
	env := decode[errorEnvelope](t, rec)
	if env.Error != errUnauthorized {
		t.Fatalf("error token = %q, want %q", env.Error, errUnauthorized)
	}
	if !strings.Contains(env.Reason, "authentication required") {
		t.Fatalf("reason = %q", env.Reason)
	}
	if env.RequestID == "" {
		t.Fatalf("401 must carry a request_id")
	}
}

// TestAuth_NoTagDenied403OnReads: a user with a "/" permission but NO tag gets
// 403 on every /api/*, reads included (AUTH-010/023/MIL-013, spec §11 test 10).
func TestAuth_NoTagDenied403OnReads(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"plain","password_hash":"`+secretHash(t)+`",
		"tags":[],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	rec := e.do(http.MethodGet, "/api/queues", nil, map[string]string{
		"Authorization": "Basic " + basicToken("plain", "secret"),
	})
	wantStatus(t, rec, http.StatusForbidden)
	env := decode[errorEnvelope](t, rec)
	if env.Error != errAccessRefused {
		t.Fatalf("error = %q, want access_refused", env.Error)
	}
}

// TestAuth_MonitoringCanReadCannotMutate (MIL-026, spec §11 test 11).
func TestAuth_MonitoringCanReadCannotMutate(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"mon","password_hash":"`+secretHash(t)+`",
		"tags":["monitoring"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")}

	if rec := e.do(http.MethodGet, "/api/queues", nil, hdr); rec.Code != http.StatusOK {
		t.Fatalf("monitoring must read queues: got %d (%s)", rec.Code, rec.Body.String())
	}
	rec := e.do(http.MethodPut, "/api/queues/%2F/mon-q", e.jsonBody(t, queueDeclareRequest{Durable: true}), hdr)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("monitoring must NOT mutate: got %d (%s)", rec.Code, rec.Body.String())
	}
}

// TestAuth_ResourcePermissionGate: a configure-permission miss is 403 and the
// reason names action+resource, never whether the resource exists
// (AUTH-012/014, spec §11 test 12).
func TestAuth_ResourcePermissionGate(t *testing.T) {
	// Full administrator CAPABILITIES, but the vhost permission denies
	// configure for every name. Gate 1 passes; gate 2 (the resource gate) must
	// refuse, and the reason must name the action and the resource.
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":"^$","write":".*","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")}
	rec := e.do(http.MethodPut, "/api/queues/%2F/q1", e.jsonBody(t, queueDeclareRequest{Durable: true}), hdr)
	wantStatus(t, rec, http.StatusForbidden)
	env := decode[errorEnvelope](t, rec)
	if !strings.Contains(env.Reason, "configure") || !strings.Contains(env.Reason, "q1") {
		t.Fatalf("reason must name the action and resource, got %q", env.Reason)
	}
	if strings.Contains(env.Reason, "exists") || strings.Contains(env.Reason, "not found") {
		t.Fatalf("reason must not reveal existence, got %q", env.Reason)
	}
}

// TestAuth_BasicMutationAccepted: a Basic-authenticated PUT succeeds (SEC-024,
// spec §11 test 13).
func TestAuth_BasicMutationAccepted(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/queues/%2F/basic-q", e.jsonBody(t, queueDeclareRequest{Durable: true, Exclusive: true, AutoDelete: true}), basicAuthHeader())
	if rec.Code != http.StatusCreated {
		t.Fatalf("basic-admin PUT must create: got %d (%s)", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); !strings.Contains(loc, "basic-q") {
		t.Fatalf("Location = %q", loc)
	}
}

// TestAuth_BasicNonAdminCannotMutate.
func TestAuth_BasicNonAdminCannotMutate(t *testing.T) {
	e := basicEnv(t, false, nil)
	rec := e.do(http.MethodPut, "/api/queues/%2F/x", e.jsonBody(t, queueDeclareRequest{}), basicAuthHeader())
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin basic PUT must be 403, got %d (%s)", rec.Code, rec.Body.String())
	}
}

// sessionLogin logs in over the session endpoint and returns the session
// cookie and CSRF token.
func sessionLogin(t *testing.T, e *env) (*http.Cookie, string) {
	t.Helper()
	rec := e.do(http.MethodPost, "/api/strangeq/session",
		e.jsonBody(t, sessionRequest{Username: "admin", Password: "secret"}), nil)
	wantStatus(t, rec, http.StatusOK)
	var sess, csrf *http.Cookie
	for _, c := range rec.Result().Cookies() {
		switch c.Name {
		case sessionCookieName:
			sess = c
		case csrfCookieName:
			csrf = c
		}
	}
	if sess == nil || csrf == nil {
		t.Fatalf("login must set both cookies, got %v", rec.Result().Cookies())
	}
	return sess, csrf.Value
}

// TestCSRF_CookieMutationWithoutToken403 (MIL-012, spec §11 test 14).
func TestCSRF_CookieMutationWithoutToken403(t *testing.T) {
	e := basicEnv(t, true, nil)
	sess, csrf := sessionLogin(t, e)
	if !sess.HttpOnly {
		t.Fatalf("SQ_SESSION must be HttpOnly")
	}
	if len(csrf) < 43 {
		t.Fatalf("CSRF token must be >=43 chars base64url, got %d", len(csrf))
	}

	// Cookie-authenticated mutation without X-CSRF-Token → 403.
	noToken := e.do(http.MethodPut, "/api/queues/%2F/csrf-q",
		e.jsonBody(t, queueDeclareRequest{Durable: true}),
		map[string]string{"Cookie": sess.Name + "=" + sess.Value})
	wantStatus(t, noToken, http.StatusForbidden)

	// With the token AND a matching Origin it succeeds.
	withToken := e.do(http.MethodPut, "/api/queues/%2F/csrf-q",
		e.jsonBody(t, queueDeclareRequest{Durable: true}),
		map[string]string{
			"Cookie":       sess.Name + "=" + sess.Value,
			"X-CSRF-Token": csrf,
			"Origin":       "http://example.com",
		})
	if withToken.Code != http.StatusCreated {
		t.Fatalf("cookie mutation with CSRF + Origin must succeed: got %d (%s)", withToken.Code, withToken.Body.String())
	}
}

// TestCSRF_BadOrigin403 (SEC-022, spec §11 test 15).
func TestCSRF_BadOrigin403(t *testing.T) {
	e := basicEnv(t, true, nil)
	sess, csrf := sessionLogin(t, e)
	bad := e.do(http.MethodPut, "/api/queues/%2F/origin-q",
		e.jsonBody(t, queueDeclareRequest{Durable: true}),
		map[string]string{
			"Cookie":       sess.Name + "=" + sess.Value,
			"X-CSRF-Token": csrf,
			"Origin":       "http://evil.example",
		})
	wantStatus(t, bad, http.StatusForbidden)
}

// TestSession_Logout204AndCookieCleared (AUTH-031, spec §11 test 16). Logout is
// a cookie-authenticated DELETE, so it carries the CSRF token (§5.4).
func TestSession_Logout204AndCookieCleared(t *testing.T) {
	e := basicEnv(t, true, nil)
	sess, csrf := sessionLogin(t, e)

	out := e.do(http.MethodDelete, "/api/strangeq/session", nil,
		map[string]string{
			"Cookie":       sess.Name + "=" + sess.Value,
			"X-CSRF-Token": csrf,
			"Origin":       "http://example.com",
		})
	wantStatus(t, out, http.StatusNoContent)
	cleared := false
	for _, c := range out.Result().Cookies() {
		if c.Name == sessionCookieName && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatalf("logout must clear SQ_SESSION (MaxAge<0)")
	}
	// The old cookie is now invalid.
	after := e.do(http.MethodGet, "/api/overview", nil, map[string]string{"Cookie": sess.Name + "=" + sess.Value})
	if after.Code != http.StatusUnauthorized {
		t.Fatalf("a deleted session must be 401, got %d", after.Code)
	}
}

// TestSession_IdleExpiry injects a clock so expiry is deterministic
// (spec §11 test 17).
func TestSession_IdleExpiry(t *testing.T) {
	store := newSessionStore(16, 30*time.Second)
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	sess, err := store.create(guestPrincipal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, ok := store.get(sess.id); !ok {
		t.Fatalf("fresh session must be present")
	}
	now = now.Add(31 * time.Second)
	if _, ok := store.get(sess.id); ok {
		t.Fatalf("idle-expired session must be invalid")
	}
}

// TestSession_Bounded: over max, the oldest session is evicted (AUTH-032,
// spec §11 test 18).
func TestSession_Bounded(t *testing.T) {
	store := newSessionStore(3, time.Hour)
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	var ids []string
	for i := 0; i < 5; i++ {
		now = now.Add(time.Second)
		s, err := store.create(guestPrincipal())
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		ids = append(ids, s.id)
	}
	if store.count() != 3 {
		t.Fatalf("session count = %d, want 3", store.count())
	}
	if _, ok := store.get(ids[0]); ok {
		t.Fatalf("oldest session must be evicted")
	}
	if _, ok := store.get(ids[4]); !ok {
		t.Fatalf("newest session must survive")
	}
	if store.evictedTotal() < 2 {
		t.Fatalf("evicted = %d, want >=2", store.evictedTotal())
	}
}

// TestLoopback_EffectiveClientBehindProxy (SEC-112/MIL-016, spec §11 test 19).
// loopback_only is evaluated against the EFFECTIVE client: a loopback socket
// with an untrusted X-Forwarded-For does NOT get the loopback exemption.
func TestLoopback_EffectiveClientBehindProxy(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"lo","password_hash":"`+secretHash(t)+`",
		"tags":["management"],"loopback_only":true,
		"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)

	// No proxy header: socket peer is loopback → allowed.
	ok := e.do(http.MethodGet, "/api/overview", nil, map[string]string{
		"Authorization": "Basic " + basicToken("lo", "secret"),
	})
	if ok.Code != http.StatusOK {
		t.Fatalf("loopback_only user on a loopback socket must be allowed, got %d (%s)", ok.Code, ok.Body.String())
	}

	// Now make the socket peer a trusted proxy and forward a public client.
	proxyEnv := inheritEnv(t, `{"users":[{"username":"lo","password_hash":"`+secretHash(t)+`",
		"tags":["management"],"loopback_only":true,
		"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, func(cfg *config.AMQPConfig) {
		cfg.Management.TrustedProxies = []string{"127.0.0.1/32"}
	})
	req := proxyEnv.doFrom("127.0.0.1:5555", http.MethodGet, "/api/overview", nil, map[string]string{
		"Authorization":   "Basic " + basicToken("lo", "secret"),
		"X-Forwarded-For": "203.0.113.9",
	})
	if req.Code != http.StatusForbidden {
		t.Fatalf("loopback_only user behind a trusted proxy with a public client must be 403, got %d (%s)", req.Code, req.Body.String())
	}
}

// TestAuthzDisabled_NonLoopback403 (AUTH-018/MIL-016, spec §11 test 20).
func TestAuthzDisabled_NonLoopback403(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthorizationEnabled = false
	})
	rec := e.doFrom("198.51.100.7:5555", http.MethodGet, "/api/overview", nil, basicAuthHeader())
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-loopback client with authorization disabled must be 403, got %d (%s)", rec.Code, rec.Body.String())
	}
	// A loopback client with the same credentials is admitted.
	if ok := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader()); ok.Code != http.StatusOK {
		t.Fatalf("loopback client must still be served, got %d (%s)", ok.Code, ok.Body.String())
	}
}

// TestReloadAuth_SIGHUPInvalidatesSession (AUTH-051, spec §11 test 21).
func TestReloadAuth_SIGHUPInvalidatesSession(t *testing.T) {
	path := writeAuthFile(t, `{"users":[{"username":"u1","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`)
	e := newEnv(t, false, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthenticationEnabled = true
		cfg.Security.AuthorizationEnabled = true
		cfg.Security.AuthenticationFilePath = path
		cfg.Management.Authentication = config.ManagementAuthenticationInherit
	})
	sess, err := e.mgmt.sessions.create(&Principal{Name: "u1", Caps: map[string]bool{capReadTopology: true}})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	// Rewrite the file without u1.
	if err := os.WriteFile(path, []byte(`{"users":[]}`), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if n := e.mgmt.ReloadUsers(); n != 1 {
		t.Fatalf("ReloadUsers invalidated %d sessions, want 1", n)
	}
	if _, ok := e.mgmt.sessions.get(sess.id); ok {
		t.Fatalf("session for a removed user must be invalidated")
	}
}

// TestWhoami_CapabilitiesAndTriples (AUTH-033, spec §11 test 54).
func TestWhoami_CapabilitiesAndTriples(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	rec := e.do(http.MethodGet, "/api/whoami", nil, map[string]string{
		"Authorization": "Basic " + basicToken("adm", "secret"),
	})
	wantStatus(t, rec, http.StatusOK)
	out := decode[whoamiResponse](t, rec)
	if out.Name != "adm" || out.VHost != "/" {
		t.Fatalf("whoami = %+v", out)
	}
	if len(out.Capabilities) != len(administratorCapabilities) {
		t.Fatalf("administrator capabilities = %v", out.Capabilities)
	}
	if len(out.VHostPermissions) != 1 || out.VHostPermissions[0].VHost != "/" {
		t.Fatalf("vhost_permissions = %+v", out.VHostPermissions)
	}
}
