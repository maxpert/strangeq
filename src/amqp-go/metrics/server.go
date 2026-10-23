package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServerOptions configures the telemetry HTTP server.
type ServerOptions struct {
	Address     string        // "" → "127.0.0.1" (loopback only)
	Port        int           // required, 1..65535 (no silent 9419 substitution; config defaults it)
	EnablePprof bool          // register /debug/pprof/* and enable mutex/block profiling
	Ready       func() error  // nil → always ready; non-nil error → 503
	Annotate    func() string // optional suffix for 200 bodies (alarms)

	// writeTimeout is a test hook. Production keeps 10s so /metrics cannot be
	// held open by a slow reader; Go 1.25's net/http/pprof extends the write
	// deadline for /profile, /trace and delta profiles.
	writeTimeout time.Duration
}

// Server provides an HTTP server for Prometheus metrics, health and profiling.
type Server struct {
	httpServer       *http.Server
	listener         net.Listener
	listenAddress    string
	port             int
	profilingEnabled bool
}

// NewServerWithOptions creates a telemetry server.
func NewServerWithOptions(o ServerOptions) *Server {
	address := o.Address
	if address == "" {
		address = "127.0.0.1"
	}

	writeTimeout := o.writeTimeout
	if writeTimeout == 0 {
		writeTimeout = 10 * time.Second
	}

	mux := http.NewServeMux()

	// Prometheus metrics endpoint (always enabled)
	mux.Handle("/metrics", promhttp.Handler())

	// Readiness endpoint: 200 only when Ready is nil or returns nil.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if o.Ready != nil {
			if err := o.Ready(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "NOT READY: %v", err)
				return
			}
		}
		body := "OK"
		if o.Annotate != nil {
			if annotation := o.Annotate(); annotation != "" {
				body += " (" + annotation + ")"
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})

	// Enable profiling endpoints only when explicitly requested.
	if o.EnablePprof {
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)

		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		mux.Handle("/debug/pprof/block", pprof.Handler("block"))
		mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	}

	// Root endpoint with documentation. 404 for every other path: the "/"
	// pattern is a catch-all, so without this guard a disabled pprof endpoint
	// would fall through to the HTML index with 200.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")

		pprofSection := `<p><em>Profiling endpoints are disabled. Start server with --enable-pprof to enable.</em></p>`
		if o.EnablePprof {
			pprofSection = `<h2>Profiling Endpoints (pprof)</h2>
<ul>
<li><a href="/debug/pprof/">/debug/pprof/</a> - Profiling index</li>
<li><a href="/debug/pprof/profile">/debug/pprof/profile</a> - CPU profile (30s by default)</li>
<li><a href="/debug/pprof/heap">/debug/pprof/heap</a> - Heap profile</li>
<li><a href="/debug/pprof/mutex">/debug/pprof/mutex</a> - Mutex contention profile</li>
<li><a href="/debug/pprof/block">/debug/pprof/block</a> - Blocking profile</li>
<li><a href="/debug/pprof/goroutine">/debug/pprof/goroutine</a> - Goroutine profile</li>
<li><a href="/debug/pprof/allocs">/debug/pprof/allocs</a> - Allocations profile</li>
</ul>
<h3>Usage Examples</h3>
<pre>
# Collect CPU profile (30 seconds)
curl -o cpu.prof http://localhost:` + strconv.Itoa(o.Port) + `/debug/pprof/profile?seconds=30

# Collect mutex profile
curl -o mutex.prof http://localhost:` + strconv.Itoa(o.Port) + `/debug/pprof/mutex

# Analyze with pprof
go tool pprof -http=:8080 cpu.prof
</pre>`
		}

		html := `<!DOCTYPE html>
<html>
<head><title>AMQP Server Telemetry</title></head>
<body>
<h1>AMQP Server Telemetry & Observability</h1>

<h2>Metrics Endpoints</h2>
<ul>
<li><a href="/metrics">/metrics</a> - Prometheus metrics in text format</li>
<li><a href="/health">/health</a> - Readiness check (503 until the broker is ready)</li>
</ul>

` + pprofSection + `

<p>For more information, see:</p>
<ul>
<li><a href="https://prometheus.io/">Prometheus documentation</a></li>
<li><a href="https://go.dev/blog/pprof">Go pprof profiling</a></li>
</ul>
</body>
</html>`
		_, _ = w.Write([]byte(html))
	})

	return &Server{
		httpServer: &http.Server{
			Handler:           mux,
			ReadTimeout:       5 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      writeTimeout,
			IdleTimeout:       120 * time.Second,
		},
		port:             o.Port,
		profilingEnabled: o.EnablePprof,
		// Remember the address for Listen (the http.Server no longer listens
		// through Addr so bind errors can be surfaced synchronously).
		listenAddress: address,
	}
}

// NewServer is the compatibility wrapper: loopback bind, explicit port.
func NewServer(port int, enableProfiling bool) *Server {
	return NewServerWithOptions(ServerOptions{Port: port, EnablePprof: enableProfiling})
}

// Handler returns the HTTP handler, for tests.
func (s *Server) Handler() http.Handler { return s.httpServer.Handler }

// Listen binds the telemetry socket synchronously so callers see bind errors
// before serving. The port never silently defaults: config supplies 9419.
func (s *Server) Listen() error {
	if s.port < 1 || s.port > 65535 {
		return fmt.Errorf("invalid telemetry port: %d", s.port)
	}
	addr := net.JoinHostPort(s.listenAddress, strconv.Itoa(s.port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("telemetry listen %s: %w", addr, err)
	}
	s.listener = listener
	return nil
}

// Serve serves on the listener created by Listen. http.ErrServerClosed is not
// an error: it is what a graceful Stop returns.
func (s *Server) Serve() error {
	if s.listener == nil {
		return errors.New("telemetry server: Serve called before Listen")
	}
	if err := s.httpServer.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Addr returns the bound address (nil before Listen).
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Start is the compatibility entry point used by examples: Listen + Serve.
func (s *Server) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve()
}

// Stop gracefully stops the metrics HTTP server
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Port returns the port the metrics server is listening on
func (s *Server) Port() int {
	return s.port
}

// ReadinessFromServer builds the /health readiness predicate used by both
// main and the module-root health test. health may be nil (no Server.Health
// yet); shutting may be nil. Everything here is cheap enough to run per
// request: no I/O and no caching.
func ReadinessFromServer(listening func() bool, health func() error, shutting *atomic.Bool) func() error {
	return func() error {
		if shutting != nil && shutting.Load() {
			return errors.New("shutting down")
		}
		if listening != nil && !listening() {
			return errors.New("AMQP listener not bound")
		}
		if health != nil {
			return health()
		}
		return nil
	}
}
