package management

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/server/management/web"
	"go.uber.org/zap"
)

// TestComposedServer_UIAndAPIOnOneListener is the integration seam: it builds
// the SHIPPED composition (a real server.Server + management.New with the
// embedded UI injected, exactly as cmd/amqp-server wires it), starts the
// management listener on 127.0.0.1:0, and proves the management middleware
// routes the SPA, the API, and the static assets correctly on ONE listener.
//
// It is the e2e counterpart of the web package's unit tests and the management
// package's handler-level tests: nothing is stubbed, the real mux serves.
func TestComposedServer_UIAndAPIOnOneListener(t *testing.T) {
	cfg := baseEnvConfig(t, func(c *config.AMQPConfig) {
		// An ephemeral loopback port: config validation rejects port 0, so
		// reserve one and let Start bind it (the package's established
		// pattern). Addr() then reports the real listener.
		c.Management.Port = freePort(t)
	})

	srv, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("silent").
		Build()
	if err != nil {
		t.Fatalf("build broker: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	m, err := New(Options{
		Server:        srv,
		Config:        cfg,
		Authenticator: srv.Authenticator,
		AuthFile:      cfg.Security.AuthenticationFilePath,
		Logger:        zap.NewNop(),
		Version:       "9.9.9",
		Commit:        "deadbeef",
		BuildDate:     "2026-11-08",
		GoVersion:     "go1.27",
		UIHandler:     web.Handler(),
	})
	if err != nil {
		t.Fatalf("management.New: %v", err)
	}
	if err := m.Start(); err != nil {
		t.Fatalf("management.Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("management.Shutdown: %v", err)
		}
	})

	addr := m.Addr()
	if addr == nil {
		t.Fatal("management listener has no bound address")
	}
	base := "http://" + addr.String()
	client := &http.Client{Timeout: 5 * time.Second}

	// Readiness: the socket is bound synchronously by Start, but bound it what
	// the deadline is for. No sleep-only wait.
	waitReady(t, client, base+"/")

	// 1. GET / serves the SPA shell with the security headers.
	shellStatus, shellHdr, shellBody := integrationGet(t, client, base+"/", nil)
	if shellStatus != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", shellStatus)
	}
	if ct := shellHdr.Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("GET / Content-Type = %q, want text/html; charset=utf-8", ct)
	}
	if csp := shellHdr.Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") {
		t.Errorf("GET / CSP = %q, want script-src 'self'", csp)
	}
	if got := shellHdr.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("GET / X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := shellHdr.Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("GET / X-Frame-Options = %q, want DENY", got)
	}
	for _, marker := range []string{`id="content"`, "/assets/app.js", "StrangeQ Management"} {
		if !strings.Contains(shellBody, marker) {
			t.Errorf("GET / shell body missing marker %q", marker)
		}
	}

	// 2. An authenticated API read answers JSON. The surface runs in the
	// none-authentication test mode, so there is no credential to attach.
	apiStatus, apiHdr, apiBody := integrationGet(t, client, base+"/api/overview", nil)
	if apiStatus != http.StatusOK {
		t.Fatalf("GET /api/overview = %d, want 200; body=%s", apiStatus, apiBody)
	}
	if ct := apiHdr.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("GET /api/overview Content-Type = %q, want application/json", ct)
	}
	var overview map[string]any
	if err := json.Unmarshal([]byte(apiBody), &overview); err != nil {
		t.Fatalf("GET /api/overview is not JSON: %v; body=%s", err, apiBody)
	}
	if _, ok := overview["object_totals"]; !ok {
		t.Errorf("GET /api/overview missing object_totals: %s", apiBody)
	}

	// 3. An unknown NON-API path is an SPA deep link: the shell, not a 404.
	spaStatus, spaHdr, spaBody := integrationGet(t, client, base+"/does-not-exist", nil)
	if spaStatus != http.StatusOK {
		t.Errorf("GET /does-not-exist = %d, want 200 (SPA fallback)", spaStatus)
	}
	if ct := spaHdr.Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("GET /does-not-exist Content-Type = %q, want text/html", ct)
	}
	if spaBody != shellBody {
		t.Error("GET /does-not-exist did not serve the same SPA shell as GET /")
	}

	// 4. An unknown /api/ path is the JSON error envelope, never the shell.
	missingStatus, missingHdr, missingBody := integrationGet(t, client, base+"/api/does-not-exist", nil)
	if missingStatus != http.StatusNotFound {
		t.Errorf("GET /api/does-not-exist = %d, want 404", missingStatus)
	}
	if ct := missingHdr.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("GET /api/does-not-exist Content-Type = %q, want application/json", ct)
	}
	if strings.Contains(missingBody, "<html") || strings.Contains(missingBody, `id="content"`) {
		t.Errorf("GET /api/does-not-exist leaked the SPA shell: %s", missingBody)
	}
	var envelope struct {
		Error     string `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal([]byte(missingBody), &envelope); err != nil {
		t.Fatalf("GET /api/does-not-exist is not a JSON envelope: %v; body=%s", err, missingBody)
	}
	if envelope.Error != "not_found" {
		t.Errorf("GET /api/does-not-exist error = %q, want not_found", envelope.Error)
	}

	// 5. The UI static assets carry a strong ETag and revalidate to 304.
	assetStatus, assetHdr, assetBody := integrationGet(t, client, base+"/assets/app.js", nil)
	if assetStatus != http.StatusOK {
		t.Fatalf("GET /assets/app.js = %d, want 200", assetStatus)
	}
	etag := assetHdr.Get("ETag")
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Fatalf("GET /assets/app.js ETag = %q, want a strong quoted validator", etag)
	}
	if !strings.Contains(assetBody, "sq-config") {
		t.Error("app.js does not read the injected sq-config meta")
	}
	notModifiedStatus, _, notModifiedBody := integrationGet(t, client, base+"/assets/app.js",
		map[string]string{"If-None-Match": etag})
	if notModifiedStatus != http.StatusNotModified {
		t.Errorf("GET /assets/app.js with If-None-Match = %d, want 304", notModifiedStatus)
	}
	if len(notModifiedBody) != 0 {
		t.Errorf("304 carried %d body bytes", len(notModifiedBody))
	}
}

// waitReady polls GET url until the listener answers (any HTTP status) or the
// deadline expires. It bounds the wait so the test can never hang.
func waitReady(t *testing.T, client *http.Client, url string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("management listener at %s never became ready within 3s: %v", url, lastErr)
}

// integrationGet issues a GET and returns the status, headers, and body.
func integrationGet(t *testing.T, client *http.Client, url string, hdr map[string]string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, resp.Header, string(body)
}
