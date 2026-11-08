package management

import (
	"net/http"

	"go.uber.org/zap"
)

// metricsResponse is the GET /api/metrics payload: the Prometheus text
// exposition wrapped in a JSON object so the same-origin management API can
// carry it under connect-src 'self' (spec mgmt-07 §3). The JSON wrapper is a
// StrangeQ extension with no RabbitMQ counterpart (DIV-metrics-2).
type metricsResponse struct {
	Text string `json:"text"`
}

// hMetrics serves GET /api/metrics. It re-serves the process registry's
// Prometheus text via the injected MetricsScrape seam (the same promhttp
// handler the telemetry listener serves, so the two never drift).
//
// When the seam is nil, telemetry is disabled and the route answers 501 naming
// telemetry.enabled: the screen renders a truthful notice rather than a
// fabricated empty matrix (spec mgmt-07 §2.2 / R10). A scrape failure is a 503
// with a fixed reason; the upstream error never reaches the client (it would
// merely be scrubbed for a 500) and goes to the log with the correlation id.
//
// Everything else — no-store, Vary: Authorization, security headers,
// authentication + read_topology, 405 + Allow, the request timeout, panic
// isolation, ETag/304 and the max_response_bytes cap — is inherited from
// serveHTTP/writeJSON with no code here.
func (s *Server) hMetrics(w http.ResponseWriter, r *http.Request) {
	if s.opts.MetricsScrape == nil {
		s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented,
			"telemetry is disabled; no broker metrics are collected (set telemetry.enabled, or start with --enable-telemetry)"))
		return
	}
	body, err := s.opts.MetricsScrape()
	if err != nil {
		s.log.Warn("metrics scrape failed", zap.Error(err), zap.String("request_id", requestID(r)))
		s.writeAPIError(w, r, apiErr(http.StatusServiceUnavailable, errUnavailable,
			"metrics scrape failed", withHeader("Retry-After", "1")))
		return
	}
	s.writeJSON(w, r, http.StatusOK, metricsResponse{Text: string(body)})
}
