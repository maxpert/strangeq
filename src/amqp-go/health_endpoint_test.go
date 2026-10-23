package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthHandler builds the exact /health handler main builds: readiness is
// listener state + the optional Server.Health() + the main-owned shutdown flag.
func healthHandler(srv interface {
	IsListening() bool
}) (http.Handler, *atomic.Bool) {
	var shutting atomic.Bool
	// Same logic as main.readyFunc: the read-only root test package cannot
	// import cmd/amqp-server, so both call the shared metrics helpers. S5:
	// WS-B's Health returns interfaces.HealthStatus, not error — assert the
	// real signature and adapt through the shared mapping.
	var health func() error
	if h, ok := any(srv).(interface {
		Health() interfaces.HealthStatus
	}); ok {
		health = metrics.HealthStatusFunc(h.Health)
	}
	telemetry := metrics.NewServerWithOptions(metrics.ServerOptions{
		Port:  9419,
		Ready: metrics.ReadinessFromServer(srv.IsListening, health, &shutting),
	})
	return telemetry.Handler(), &shutting
}

func getHealth(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	return rec
}

// TestHealth_503UntilReady pins that /health reports not-ready before the AMQP
// listener is bound and ready once it is.
//
// The "before Build" half of the lifecycle cannot be expressed by a unit test
// that receives an already-built server; main starts telemetry only after
// Build() returns. The systemd smoke script covers the from-process-start gap
// by polling /health while the broker recovers.
func TestHealth_503UntilReady(t *testing.T) {
	cfg := isolatedTestConfig(t, "127.0.0.1:0")
	srv := newIsolatedTestServer(t, cfg)

	h, _ := healthHandler(srv)

	rec := getHealth(t, h)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "NOT READY:")

	waitForListening(t, srv)
	t.Cleanup(func() { _ = srv.Stop() })

	rec = getHealth(t, h)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())
}

func TestHealth_503DuringShutdown(t *testing.T) {
	cfg := isolatedTestConfig(t, "127.0.0.1:0")
	srv := newIsolatedTestServer(t, cfg)
	waitForListening(t, srv)
	t.Cleanup(func() { _ = srv.Stop() })

	h, shutting := healthHandler(srv)

	rec := getHealth(t, h)
	require.Equal(t, http.StatusOK, rec.Code)

	// Exactly what main does first on SIGTERM.
	shutting.Store(true)
	rec = getHealth(t, h)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "shutting down")

	// Even with the flag cleared, a stopped listener must not report ready.
	shutting.Store(false)
	require.NoError(t, srv.Stop())
	rec = getHealth(t, h)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "not bound")
}
