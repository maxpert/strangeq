package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync/atomic"

	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
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

// serverHealth returns Server.Health when the merged tree provides it. The
// worktree that owns this branch may not have WS-B's method yet, so the
// assertion is deliberately dynamic: main compiles and works standalone.
func serverHealth(s any) func() error {
	if h, ok := s.(interface{ Health() error }); ok {
		return h.Health
	}
	return nil
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
