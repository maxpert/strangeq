package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides an HTTP server for Prometheus metrics and profiling
type Server struct {
	httpServer       *http.Server
	port             int
	profilingEnabled bool
}

// NewServer creates a new telemetry HTTP server
// If profiling is enabled, pprof endpoints are registered
func NewServer(port int, enableProfiling bool) *Server {
	if port == 0 {
		port = 9419 // Default AMQP exporter port (registered with Prometheus)
	}

	mux := http.NewServeMux()

	// Prometheus metrics endpoint (always enabled)
	mux.Handle("/metrics", promhttp.Handler())

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Enable profiling endpoints if requested
	if enableProfiling {
		// Enable mutex and block profiling
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)

		// Register pprof handlers
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		// Manually add the other pprof handlers
		mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		mux.Handle("/debug/pprof/block", pprof.Handler("block"))
		mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	}

	// Root endpoint with documentation
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")

		pprofSection := ""
		if enableProfiling {
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
curl -o cpu.prof http://localhost:` + fmt.Sprintf("%d", port) + `/debug/pprof/profile?seconds=30

# Collect mutex profile
curl -o mutex.prof http://localhost:` + fmt.Sprintf("%d", port) + `/debug/pprof/mutex

# Analyze with pprof
go tool pprof -http=:8080 cpu.prof
</pre>`
		} else {
			pprofSection = `<p><em>Profiling endpoints are disabled. Start server with --enable-telemetry to enable.</em></p>`
		}

		html := `<!DOCTYPE html>
<html>
<head><title>AMQP Server Telemetry</title></head>
<body>
<h1>AMQP Server Telemetry & Observability</h1>

<h2>Metrics Endpoints</h2>
<ul>
<li><a href="/metrics">/metrics</a> - Prometheus metrics in text format</li>
<li><a href="/health">/health</a> - Health check endpoint</li>
</ul>

` + pprofSection + `

<p>For more information, see:</p>
<ul>
<li><a href="https://prometheus.io/">Prometheus documentation</a></li>
<li><a href="https://go.dev/blog/pprof">Go pprof profiling</a></li>
</ul>
</body>
</html>`
		w.Write([]byte(html))
	})

	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		port:             port,
		profilingEnabled: enableProfiling,
	}
}

// Start starts the metrics HTTP server
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

// Stop gracefully stops the metrics HTTP server
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Port returns the port the metrics server is listening on
func (s *Server) Port() int {
	return s.port
}
