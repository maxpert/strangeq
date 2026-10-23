package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestTelemetry_PprofDisabledByDefault(t *testing.T) {
	// Read-only query: -1 returns the current fraction without changing it.
	old := runtime.SetMutexProfileFraction(-1)
	runtime.SetMutexProfileFraction(old)
	t.Cleanup(func() { runtime.SetMutexProfileFraction(old) })
	if old != 0 {
		t.Skipf("another test left mutex profiling at %d", old)
	}

	s := NewServerWithOptions(ServerOptions{Port: 9419})
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1), "pprof disabled must not enable profiles")

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap"} {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusNotFound, rec.Code, path)
	}

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestTelemetry_PprofEnabled(t *testing.T) {
	s := NewServerWithOptions(ServerOptions{Port: 9419, EnablePprof: true})
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotZero(t, runtime.SetMutexProfileFraction(-1), "pprof enabled must turn on mutex profiling")
}

func TestTelemetry_LoopbackBind(t *testing.T) {
	s := NewServerWithOptions(ServerOptions{Port: freePort(t)})
	require.NoError(t, s.Listen())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	addr, ok := s.Addr().(*net.TCPAddr)
	require.True(t, ok)
	assert.True(t, addr.IP.IsLoopback(), "telemetry must bind loopback by default, got %s", s.Addr())

	t.Run("explicit 0.0.0.0", func(t *testing.T) {
		s := NewServerWithOptions(ServerOptions{Address: "0.0.0.0", Port: freePort(t)})
		require.NoError(t, s.Listen())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = s.Stop(ctx)
		})
		addr, ok := s.Addr().(*net.TCPAddr)
		require.True(t, ok)
		assert.True(t, addr.IP.IsUnspecified())
	})
}

func TestTelemetry_ProfileOutlivesWriteTimeout(t *testing.T) {
	s := NewServerWithOptions(ServerOptions{
		Port:         freePort(t),
		EnablePprof:  true,
		writeTimeout: 200 * time.Millisecond,
	})
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	})
	require.NoError(t, s.Listen())
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})

	url := "http://" + s.Addr().String() + "/debug/pprof/profile?seconds=1"
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.Greater(t, len(body), 2)
	assert.Equal(t, []byte{0x1f, 0x8b}, body[:2], "pprof profile must be a gzip stream")
}

func TestHealth_ReadyFuncAndAnnotate(t *testing.T) {
	// ready
	s := NewServerWithOptions(ServerOptions{Port: 9419})
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())
	assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))

	// not ready
	s = NewServerWithOptions(ServerOptions{
		Port:  9419,
		Ready: func() error { return assert.AnError },
	})
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "NOT READY: ")

	// annotated
	s = NewServerWithOptions(ServerOptions{
		Port:     9419,
		Annotate: func() string { return "resource alarm active: memory" },
	})
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK (resource alarm active: memory)", rec.Body.String())
}

func TestReadinessFromServer(t *testing.T) {
	var shutting atomic.Bool
	listening := true
	healthy := error(nil)

	ready := ReadinessFromServer(
		func() bool { return listening },
		func() error { return healthy },
		&shutting,
	)
	assert.NoError(t, ready())

	shutting.Store(true)
	assert.ErrorContains(t, ready(), "shutting down")
	shutting.Store(false)

	listening = false
	assert.ErrorContains(t, ready(), "not bound")
	listening = true

	healthy = assert.AnError
	assert.ErrorIs(t, ready(), assert.AnError)
}

func TestReadinessFromServer_NilHealth(t *testing.T) {
	ready := ReadinessFromServer(func() bool { return true }, nil, nil)
	assert.NoError(t, ready())
}

// TestHealthStatusFunc (S5): WS-B's Health() interfaces.HealthStatus feeds
// /health readiness through this single mapping — "healthy" is ready, any
// other status is not, and recorded Errors surface in the message.
func TestHealthStatusFunc(t *testing.T) {
	assert.Nil(t, HealthStatusFunc(nil))

	healthy := HealthStatusFunc(func() interfaces.HealthStatus {
		return interfaces.HealthStatus{Status: "healthy"}
	})
	assert.NoError(t, healthy())

	degraded := HealthStatusFunc(func() interfaces.HealthStatus {
		return interfaces.HealthStatus{Status: "degraded"}
	})
	assert.ErrorContains(t, degraded(), "degraded")

	withErrors := HealthStatusFunc(func() interfaces.HealthStatus {
		return interfaces.HealthStatus{Status: "unhealthy", Errors: []string{"wal replay incomplete"}}
	})
	err := withErrors()
	assert.ErrorContains(t, err, "unhealthy")
	assert.ErrorContains(t, err, "wal replay incomplete")
}

func TestCollector_RecordAuthFailure(t *testing.T) {
	c := NewCollector("wse_auth_" + sanitize(t.Name()))
	c.RecordAuthFailure("bad_credentials")
	c.RecordAuthFailure("bad_credentials")
	c.RecordAuthFailure("rate_limited")
	assert.Equal(t, 2.0, testutil.ToFloat64(c.AuthFailuresTotal.WithLabelValues("bad_credentials")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.AuthFailuresTotal.WithLabelValues("rate_limited")))
}

func TestCollector_BuildInfo(t *testing.T) {
	c := NewCollector("wse_build_" + sanitize(t.Name()))
	c.SetBuildInfo("0.1.0", "abc1234", "go1.25.1")
	assert.Equal(t, 1.0, testutil.ToFloat64(c.BuildInfo.WithLabelValues("0.1.0", "abc1234", "go1.25.1")))
}
