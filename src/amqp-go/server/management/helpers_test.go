package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// testHash is a bcrypt hash of "secret" computed once (bcrypt is deliberately
// slow; tests must not pay it per case).
var (
	testHashOnce sync.Once
	testHash     string
)

func secretHash(t *testing.T) string {
	t.Helper()
	testHashOnce.Do(func() {
		h, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
		if err != nil {
			panic(err)
		}
		testHash = string(h)
	})
	return testHash
}

// env is one management server plus the config it was built from.
type env struct {
	t    *testing.T
	cfg  *config.AMQPConfig
	srv  *server.Server
	mgmt *Server
}

// newEnv builds a management server. withBroker controls whether a real
// in-process broker is constructed (slower, needed for topology endpoints).
func newEnv(t *testing.T, withBroker bool, mutate func(*config.AMQPConfig)) *env {
	t.Helper()
	cfg := baseEnvConfig(t, mutate)

	e := &env{t: t, cfg: cfg}
	if withBroker {
		srv, err := server.NewServerBuilder().
			WithConfig(cfg).
			WithZapLogger("silent").
			Build()
		if err != nil {
			t.Fatalf("build broker: %v", err)
		}
		e.srv = srv
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
		})
	}

	e.mgmt = newManagement(t, e.srv, cfg)
	return e
}

// baseEnvConfig returns the shared test config — a temp storage dir, silent
// logging, and management enabled in "none" auth mode. mutate, when non-nil, is
// applied last.
func baseEnvConfig(t *testing.T, mutate func(*config.AMQPConfig)) *config.AMQPConfig {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"
	cfg.Management.Enabled = true
	cfg.Management.Authentication = config.ManagementAuthenticationNone
	cfg.Management.Address = "127.0.0.1"
	if mutate != nil {
		mutate(cfg)
	}
	return cfg
}

// newManagement constructs the management Server around srv (which may be nil).
func newManagement(t *testing.T, srv *server.Server, cfg *config.AMQPConfig) *Server {
	t.Helper()
	opts := Options{
		Server:    srv,
		Config:    cfg,
		AuthFile:  cfg.Security.AuthenticationFilePath,
		Logger:    zap.NewNop(),
		Version:   "9.9.9",
		Commit:    "deadbeef",
		BuildDate: "2026-11-08",
		GoVersion: "go1.27",
	}
	if srv != nil {
		opts.Authenticator = srv.Authenticator
	}
	m, err := New(opts)
	if err != nil {
		t.Fatalf("management.New: %v", err)
	}
	return m
}

// do performs a request through the full management pipeline with a loopback
// peer (so the loopback-only rules are satisfied).
func (e *env) do(method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	e.t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, target, rdr)
	req.RemoteAddr = "127.0.0.1:43210"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.mgmt.Handler().ServeHTTP(rec, req)
	return rec
}

func (e *env) get(target string) *httptest.ResponseRecorder {
	return e.do(http.MethodGet, target, nil, nil)
}

// doFrom performs a request with an explicit socket peer address.
func (e *env) doFrom(remoteAddr, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	e.t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, target, rdr)
	req.RemoteAddr = remoteAddr
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.mgmt.Handler().ServeHTTP(rec, req)
	return rec
}

func (e *env) jsonBody(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %T from %q: %v", out, rec.Body.String(), err)
	}
	return out
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, want, rec.Body.String())
	}
}

// writeAuthFile writes an auth.json with the given users and returns its path.
func writeAuthFile(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/auth.json"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	return path
}

// inheritEnv builds an env in inherit mode with the given auth.json users.
func inheritEnv(t *testing.T, authJSON string, mutate func(*config.AMQPConfig)) *env {
	t.Helper()
	path := writeAuthFile(t, authJSON)
	return newEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthenticationEnabled = true
		cfg.Security.AuthorizationEnabled = true
		cfg.Security.AuthenticationBackend = "file"
		cfg.Security.AuthenticationFilePath = path
		cfg.Management.Authentication = config.ManagementAuthenticationInherit
		if mutate != nil {
			mutate(cfg)
		}
	})
}

// basicEnv builds an env in basic mode with an admin principal.
func basicEnv(t *testing.T, admin bool, mutate func(*config.AMQPConfig)) *env {
	t.Helper()
	hash := secretHash(t)
	return newEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationBasic
		cfg.Management.Basic.Username = "admin"
		cfg.Management.Basic.PasswordHash = hash
		cfg.Management.Basic.Admin = admin
		if mutate != nil {
			mutate(cfg)
		}
	})
}

func basicAuthHeader() map[string]string {
	return map[string]string{"Authorization": "Basic " + basicToken("admin", "secret")}
}

func basicToken(user, pass string) string {
	req, _ := http.NewRequest(http.MethodGet, "http://x/", nil)
	req.SetBasicAuth(user, pass)
	return strings.TrimPrefix(req.Header.Get("Authorization"), "Basic ")
}

func contextBackground() context.Context { return context.Background() }

func contextWithTimeout(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	// The caller's request is short-lived; leaking the cancel func for the
	// duration of a test is harmless, but keeping it referenced keeps vet
	// quiet about lostcancel.
	_ = cancel
	return ctx
}

// settleGoroutines returns a stable goroutine count, polling with a bounded
// deadline.
func settleGoroutines(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	last := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		cur := runtime.NumGoroutine()
		if cur == last {
			return cur
		}
		last = cur
	}
	return last
}

// freePort reserves an ephemeral loopback port and returns it.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func jsonUnmarshalStrict(b []byte, v any) error { return json.Unmarshal(b, v) }

// observerlessBroker embeds a UnifiedBroker so it satisfies the interface but
// does NOT promote the optional SetEntityObserver seam. Build therefore leaves
// EntityObserverWired() false, and the management overview must honestly report
// snapshot_coherent:false rather than claim a coherent read it did not perform.
// Mirrors the wrapper in server/entities_build_test.go.
type observerlessBroker struct {
	server.UnifiedBroker
}

// PublishMessageAdmitted forwards to the wrapped broker so Build's
// lease-enabled NonBlockingPublisher check still passes.
func (b observerlessBroker) PublishMessageAdmitted(exchangeName, routingKey string, message *protocol.Message) (protocol.DepthGate, error) {
	return b.UnifiedBroker.(server.NonBlockingPublisher).PublishMessageAdmitted(exchangeName, routingKey, message)
}

// observerlessEnv builds a basic-auth management env whose Options.Server was
// built with a broker lacking the BOB-11 observer seam, so the coherence
// property is genuinely false end to end.
func observerlessEnv(t *testing.T, admin bool, mutate func(*config.AMQPConfig)) *env {
	t.Helper()
	hash := secretHash(t)
	cfg := baseEnvConfig(t, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationBasic
		cfg.Management.Basic.Username = "admin"
		cfg.Management.Basic.PasswordHash = hash
		cfg.Management.Basic.Admin = admin
		if mutate != nil {
			mutate(cfg)
		}
	})

	store, err := storage.NewStorageEngineWithDataDir(cfg.Storage.Path)
	if err != nil {
		t.Fatalf("storage engine: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	sb := broker.NewStorageBroker(store, cfg.GetEngine())
	t.Cleanup(sb.Close)

	fake := observerlessBroker{UnifiedBroker: server.NewStorageBrokerAdapter(sb)}
	srv, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("silent").
		WithStorage(store).
		WithUnifiedBroker(fake).
		Build()
	if err != nil {
		t.Fatalf("build observerless broker: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	e := &env{t: t, cfg: cfg, srv: srv}
	e.mgmt = newManagement(t, srv, cfg)
	return e
}

// consumerForTest builds a broker-registrable consumer with a unique identity.
func consumerForTest(tag, queue string) *protocol.Consumer {
	return &protocol.Consumer{
		ID:    "cid-" + tag + "-" + queue,
		Tag:   tag,
		Queue: queue,
	}
}
