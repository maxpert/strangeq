package management

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
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

// --- WS1 round-3 F2: multipart import with a non-positive limit ---

// TestDefinitionsImport_ZeroImportLimitDefaults pins round-3 F2: a directly
// constructed Options with a zero MaxImportBodyBytes must fall back to the same
// 2 MiB default readBodyLimited uses, not 413 every multipart import.
func TestDefinitionsImport_ZeroImportLimitDefaults(t *testing.T) {
	// The broker builder validates the config it is handed, so build the
	// broker with a valid config and hand the management server a copy whose
	// import limit is zero — the "embedder constructed Options directly" case.
	srvCfg := config.DefaultConfig()
	srvCfg.Storage.Path = t.TempDir()
	srvCfg.Server.LogLevel = "silent"
	srvCfg.Management.Enabled = true
	srvCfg.Management.Address = "127.0.0.1"
	srvCfg.Management.Authentication = config.ManagementAuthenticationBasic
	srvCfg.Management.Basic.Username = "admin"
	srvCfg.Management.Basic.PasswordHash = secretHash(t)
	srvCfg.Management.Basic.Admin = true

	srv, err := server.NewServerBuilder().WithConfig(srvCfg).WithZapLogger("silent").Build()
	if err != nil {
		t.Fatalf("build broker: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	mcfg := *srvCfg
	mcfg.Management.Limits.MaxImportBodyBytes = 0
	m, err := New(Options{
		Server:        srv,
		Config:        &mcfg,
		AuthFile:      mcfg.Security.AuthenticationFilePath,
		Authenticator: srv.Authenticator,
		Logger:        zap.NewNop(),
		Version:       "9.9.9",
	})
	if err != nil {
		t.Fatalf("management.New: %v", err)
	}
	e := &env{t: t, cfg: &mcfg, srv: srv, mgmt: m}

	defs := definitionsImportRequest{Exchanges: []map[string]any{{"name": "small-ex", "type": "direct"}}}
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

	req := httptest.NewRequest(http.MethodPost, "/api/definitions", bytes.NewReader(buf.Bytes()))
	req.RemoteAddr = "127.0.0.1:43210"
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", basicAuthHeader()["Authorization"])
	rec := httptest.NewRecorder()
	e.mgmt.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusRequestEntityTooLarge {
		t.Fatalf("a zero import limit must default, not 413 every import (%s)", rec.Body.String())
	}
	wantStatus(t, rec, http.StatusOK)
	if ex := e.do(http.MethodGet, "/api/exchanges/%2F/small-ex", nil, basicAuthHeader()); ex.Code != http.StatusOK {
		t.Fatalf("small import must apply its exchange, got %d (%s)", ex.Code, ex.Body.String())
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
