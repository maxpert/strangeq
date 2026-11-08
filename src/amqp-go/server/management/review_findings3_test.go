package management

import (
	"net/http"
	"testing"

	"github.com/maxpert/amqp-go/config"
)

// --- WS1 round-3 F1: trusted proxy with no forwarded client ---

// TestTrustedProxy_MissingXFFIsNotLoopback pins round-3 F1: when the socket
// peer IS a trusted proxy but sets neither X-Forwarded-For nor X-Real-IP, the
// peer is the PROXY — not the client. The effective client must stay unknown
// (never the loopback socket address), so a guest identity cannot be treated
// as loopback by a loopback-resident proxy that fails to forward the client IP
// (SEC-112).
func TestTrustedProxy_MissingXFFIsNotLoopback(t *testing.T) {
	e := newEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationNone
		cfg.Management.AllowUnauthenticated = true
		cfg.Management.TrustedProxies = []string{"127.0.0.1/32"}
	})
	sess, err := e.mgmt.sessions.create(guestPrincipal())
	if err != nil {
		t.Fatalf("create guest session: %v", err)
	}
	hdr := map[string]string{"Cookie": sessionCookieName + "=" + sess.id}

	// Trusted proxy, loopback socket, no forwarded client: 403.
	rec := e.doFrom("127.0.0.1:5555", http.MethodGet, "/api/whoami", nil, hdr)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("trusted proxy with no forwarded client = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}

	// A verbatim XFF naming the proxy itself is skipped as a trusted hop, so
	// `X-Forwarded-For: 127.0.0.1` cannot spoof a loopback client: 403.
	forged := e.doFrom("127.0.0.1:5555", http.MethodGet, "/api/whoami", nil,
		withReqHeader(hdr, "X-Forwarded-For", "127.0.0.1"))
	if forged.Code != http.StatusForbidden {
		t.Fatalf("forged XFF naming the proxy = %d, want 403 (%s)", forged.Code, forged.Body.String())
	}

	// A forwarded non-loopback client is refused on the loopback-only guest rule.
	remote := e.doFrom("127.0.0.1:5555", http.MethodGet, "/api/whoami", nil,
		withReqHeader(hdr, "X-Forwarded-For", "203.0.113.9"))
	if remote.Code != http.StatusForbidden {
		t.Fatalf("forwarded non-loopback client = %d, want 403 (%s)", remote.Code, remote.Body.String())
	}

	// A forwarded loopback client (127.0.0.2, not the proxy address) is
	// admitted: the decision follows the forwarded IP, not the socket peer.
	local := e.doFrom("127.0.0.1:5555", http.MethodGet, "/api/whoami", nil,
		withReqHeader(hdr, "X-Forwarded-For", "127.0.0.2"))
	if local.Code != http.StatusOK {
		t.Fatalf("forwarded loopback client = %d, want 200 (%s)", local.Code, local.Body.String())
	}
}

// --- WS1 round-3 F3: host ACL on the guest and session-create paths ---

// TestHostACL_GuestAndSessionCreate pins round-3 F3: security.blockedhosts
// (AUTH-009) is enforced on the no-credential guest path and on the
// session-create endpoint, not only on the Basic request path.
func TestHostACL_GuestAndSessionCreate(t *testing.T) {
	t.Run("guest", func(t *testing.T) {
		e := newEnv(t, false, func(cfg *config.AMQPConfig) {
			cfg.Management.Authentication = config.ManagementAuthenticationNone
			cfg.Management.AllowUnauthenticated = true
			cfg.Security.BlockedHosts = []string{"127.0.0.1/32"}
		})
		rec := e.do(http.MethodGet, "/api/health", nil, nil)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("guest from a blocked host = %d, want 403 (%s)", rec.Code, rec.Body.String())
		}
		if env := decode[errorEnvelope](t, rec); env.Error != "access_refused" {
			t.Fatalf("guest from a blocked host error = %q, want access_refused (%s)", env.Error, rec.Body.String())
		}
	})

	t.Run("session-create", func(t *testing.T) {
		e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
			cfg.Security.BlockedHosts = []string{"127.0.0.1/32"}
		})
		body := e.jsonBody(t, sessionRequest{Username: "admin", Password: "secret"})
		rec := e.do(http.MethodPost, "/api/strangeq/session", body, nil)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("session-create from a blocked host = %d, want 403 (%s)", rec.Code, rec.Body.String())
		}
		for _, c := range rec.Result().Cookies() {
			if c.Name == sessionCookieName {
				t.Fatalf("a blocked host must not receive a %s cookie", sessionCookieName)
			}
		}
	})
}

// --- WS1 round-3 F4: /api/permissions is an entity read route ---

// TestEntityReadRoute_PermissionsSymmetry pins round-3 F4: /api/permissions
// addresses permission records and must pass resourceReadGate exactly like
// /api/vhosts/{vhost}/permissions (AUTH-011).
func TestEntityReadRoute_PermissionsSymmetry(t *testing.T) {
	hdr := map[string]string{"Authorization": "Basic " + basicToken("perm", "secret")}

	noPerms := inheritEnv(t, `{"users":[{"username":"perm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"]}]}`, nil)
	rec := noPerms.do(http.MethodGet, "/api/permissions", nil, hdr)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("GET /api/permissions without a / permission = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}
	if env := decode[errorEnvelope](t, rec); env.Error != "access_refused" {
		t.Fatalf("error = %q, want access_refused (%s)", env.Error, rec.Body.String())
	}

	withPerms := inheritEnv(t, `{"users":[{"username":"perm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],
		"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	if got := withPerms.do(http.MethodGet, "/api/permissions", nil, hdr); got.Code != http.StatusOK {
		t.Fatalf("GET /api/permissions with a / permission = %d, want 200 (%s)", got.Code, got.Body.String())
	}
}
