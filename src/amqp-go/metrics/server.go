package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides an HTTP server for Prometheus metrics
type Server struct {
	httpServer *http.Server
	port       int
}

// NewServer creates a new metrics HTTP server
func NewServer(port int) *Server {
	if port == 0 {
		port = 9419 // Default AMQP exporter port (registered with Prometheus)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Root endpoint with documentation
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>AMQP Server Metrics</title></head>
<body>
<h1>AMQP Server Prometheus Metrics</h1>
<p>Available endpoints:</p>
<ul>
<li><a href="/metrics">/metrics</a> - Prometheus metrics in text format</li>
<li><a href="/health">/health</a> - Health check endpoint</li>
</ul>
<p>For more information, see the <a href="https://prometheus.io/">Prometheus documentation</a>.</p>
</body>
</html>`))
	})

	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		port: port,
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
