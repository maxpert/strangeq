package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync/atomic"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
)

// S5 compile-time seams: the dynamic guards in serverHealth/shutdownServer
// fall back to non-graceful Stop() (or no health signal) when the methods are
// missing, which would silently turn a future signature drift into skipped
// drain/close-ok waits. These assertions make such drift a BUILD failure
// instead. (WS-B's Health returns interfaces.HealthStatus, NOT error — the
// pre-merge guard asserted Health() error and never matched.)
var (
	_ interface{ Shutdown(context.Context) error } = (*server.Server)(nil)
	_ interface {
		Health() interfaces.HealthStatus
	} = (*server.Server)(nil)
)

// queueLister is the narrow broker surface the queue-stats provider needs.
// server.UnifiedBroker satisfies it; tests can use a tiny fake.
type queueLister interface {
	GetQueues() map[string]*protocol.Queue
	GetConsumers() map[string]*protocol.Consumer
}

// queueReadyCounter is the optional seam (server/queue_stats.go) that exposes
// a queue's ready depth without touching broker/. When absent the ready series
// is omitted rather than fabricated.
type queueReadyCounter interface {
	QueueReadyCount(name string) uint32
}

// queueHighWaterCounter is the optional seam exposing a queue's maximum depth
// observed since creation (Task 2; diagnostic). When absent the series is
// omitted.
type queueHighWaterCounter interface {
	QueueDepthHighWater(name string) uint64
}

// queueMessageCounter is the optional seam exposing a queue's ready/unacked/
// total depth (BOB-1). It is what wires amqp_queue_messages_unacknowledged and
// amqp_queue_messages_total from the broker at poll time (spec 06 §3.5); when
// absent those series are omitted rather than fabricated.
type queueMessageCounter interface {
	QueueMessageCounts(name string) broker.QueueMessageCounts
}

// brokerQueueStats adapts the live broker to metrics.QueueStatsProvider.
type brokerQueueStats struct {
	b queueLister
}

// QueueStats returns one entry per queue with consumer counts, plus ready
// depth when the broker exposes it.
func (p brokerQueueStats) QueueStats() []metrics.QueueStat {
	return queueStatsFrom(p.b)
}

func queueStatsFrom(l queueLister) []metrics.QueueStat {
	if l == nil {
		return nil
	}
	consumers := make(map[string]int)
	for _, consumer := range l.GetConsumers() {
		if consumer != nil {
			consumers[consumer.Queue]++
		}
	}

	readyCounter, hasReady := l.(queueReadyCounter)
	hwCounter, hasHW := l.(queueHighWaterCounter)
	msgCounter, hasMsg := l.(queueMessageCounter)
	stats := make([]metrics.QueueStat, 0, len(l.GetQueues()))
	for name := range l.GetQueues() {
		stat := metrics.QueueStat{
			Name:      name,
			VHost:     "/",
			Consumers: consumers[name],
		}
		if hasReady {
			stat.Ready = int(readyCounter.QueueReadyCount(name))
			stat.ReadyKnown = true
		}
		if hasHW {
			stat.HighWater = int(hwCounter.QueueDepthHighWater(name))
			stat.HighWaterSet = true
		}
		if hasMsg {
			// The unacked/total families are written ONLY when the broker
			// reports a live QueueState (Known), so an unknown queue emits no
			// series rather than a fabricated 0.
			if mc := msgCounter.QueueMessageCounts(name); mc.Known {
				stat.Unacked = int(mc.Unacked)
				stat.UnackedKnown = true
				if !stat.ReadyKnown {
					stat.Ready = int(mc.Ready)
					stat.ReadyKnown = true
				}
			}
		}
		stats = append(stats, stat)
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	return stats
}

// emitSecurityWarnings writes each finding to stderr inside a banner and logs
// it at WARN with component=startup-security. Called after Validate and before
// daemonizing so it reaches the operator's terminal or journal.
func emitSecurityWarnings(stderr io.Writer, log *zap.Logger, warnings []string) {
	for _, warning := range warnings {
		fmt.Fprintf(stderr, "*** SECURITY WARNING *** %s\n", warning)
		if log != nil {
			log.Warn(warning, zap.String("component", "startup-security"))
		}
	}
}

// readyFunc is the /health readiness predicate. It flips to 503 first on
// SIGTERM (shuttingDown), then reports the listener and the optional
// Server.Health() (WS-B seam). Health is assumed cheap; no caching.
func readyFunc(listening func() bool, health func() error, shuttingDown *atomic.Bool) func() error {
	return metrics.ReadinessFromServer(listening, health, shuttingDown)
}

// serverHealth wires WS-B's Server.Health into the /health readiness
// predicate through the shared metrics adapter (S5). The compile-time
// assertion above guarantees the method exists with exactly this signature.
func serverHealth(s *server.Server) func() error {
	return metrics.HealthStatusFunc(s.Health)
}

// shutdownServer calls Server.Shutdown(ctx) when present (WS-B seam S-1) and
// falls back to the pre-existing Stop() on a tree without it. The 30s budget
// is supplied by the caller.
func shutdownServer(s any, ctx context.Context) error {
	if g, ok := s.(interface{ Shutdown(context.Context) error }); ok {
		return g.Shutdown(ctx)
	}
	if st, ok := s.(interface{ Stop() error }); ok {
		return st.Stop()
	}
	return nil
}

// shutdownWith is the testable core of shutdownServer.
func shutdownWith(ctx context.Context, shutdown func(context.Context) error) error {
	if shutdown == nil {
		return nil
	}
	return shutdown(ctx)
}

// alarmAnnotation renders active resource alarms for the /health body. An
// alarm keeps the endpoint at 200: it blocks publishers only, and pulling the
// node from rotation would also cut consumers and prolong the alarm.
func alarmAnnotation(state uint32) string {
	const (
		memoryBit = 1 << 0
		diskBit   = 1 << 1
	)
	var active []string
	if state&memoryBit != 0 {
		active = append(active, "memory")
	}
	if state&diskBit != 0 {
		active = append(active, "disk")
	}
	if len(active) == 0 {
		return ""
	}
	return "resource alarm active: " + joinComma(active)
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

// healthcheckURL builds the container probe URL. An unspecified bind
// (0.0.0.0/::/empty) is probed through loopback.
func healthcheckURL(address string, port int) string {
	host := address
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(port)) + "/health"
}

// metricsURLFor builds the /metrics URL advertised to the management SPA from
// the bound telemetry listener. A nil or unparseable address, or a wildcard
// bind (0.0.0.0/::), yields "" — a wildcard host is not connectable by a
// browser, so the SPA omits the link rather than advertise a dead address.
// There is no telemetry TLS setting, so the scheme tracks the deployment's
// control-plane TLS posture (management.tls.enabled).
func metricsURLFor(addr net.Addr, tlsEnabled bool) string {
	if addr == nil {
		return ""
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return ""
	}
	switch host {
	case "", "0.0.0.0", "::":
		return ""
	}
	scheme := "http"
	if tlsEnabled {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, port) + "/metrics"
}

// metricsScrapeFunc is the management surface's GET /api/metrics seam (spec
// mgmt-07 §2.3). It returns nil when telemetry is disabled (no telemetry
// server), which makes the route answer 501 naming telemetry.enabled; otherwise
// it returns the process registry scrape, so the SPA and the Prometheus
// listener serve byte-identical text.
func metricsScrapeFunc(telemetry *metrics.Server) func() ([]byte, error) {
	if telemetry == nil {
		return nil
	}
	return metrics.ScrapeText
}
