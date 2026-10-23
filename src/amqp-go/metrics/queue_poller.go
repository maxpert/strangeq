package metrics

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// maxQueueSeries caps per-queue Prometheus series. Above it, only the
// authoritative queue total is published: an unbounded queue-name set would
// otherwise blow up the scrape payload and Prometheus memory.
const maxQueueSeries = 2000

// Resource-alarm bits, mirroring server.AlarmMemory/AlarmDisk. They are
// duplicated here because the metrics package cannot import server (server
// imports metrics).
const (
	alarmMemory uint32 = 1 << 0
	alarmDisk   uint32 = 1 << 1
)

// QueueStat is one queue's sampled state.
type QueueStat struct {
	Name       string
	VHost      string
	Ready      int
	ReadyKnown bool
	Consumers  int
}

// QueueStatsProvider supplies a point-in-time queue snapshot. Implementations
// must be cheap: the poller calls this once per interval (15s in main).
type QueueStatsProvider interface {
	QueueStats() []QueueStat
}

// AlarmStateProvider returns the current resource-alarm bitmask.
type AlarmStateProvider func() uint32

// PollerOptions configures RunPoller.
type PollerOptions struct {
	Interval time.Duration      // default 15s
	Queues   QueueStatsProvider // required
	Alarms   AlarmStateProvider // optional
	Log      *zap.Logger        // optional
}

type queueKey struct {
	name  string
	vhost string
}

type queuePoller struct {
	collector      *Collector
	log            *zap.Logger
	last           map[queueKey]struct{}
	cardinalityHit bool
}

// RunPoller blocks until ctx is done, ticking every interval. The first tick
// is immediate so a scrape right after startup does not wait a full interval.
func (c *Collector) RunPoller(ctx context.Context, opts PollerOptions) {
	interval := opts.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	p := &queuePoller{
		collector: c,
		log:       opts.Log,
		last:      make(map[queueKey]struct{}),
	}

	p.tick(opts)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(opts)
		}
	}
}

func (p *queuePoller) tick(opts PollerOptions) {
	p.pollOnce(opts.Queues, opts.Alarms)
}

// pollOnce performs a single poll. Exposed as a method for tests: no clock, no
// goroutine, no sleeps.
func (p *queuePoller) pollOnce(q QueueStatsProvider, alarms AlarmStateProvider) {
	if q == nil {
		p.collector.QueuesTotal.Set(0)
		return
	}
	stats := q.QueueStats()
	if len(stats) > maxQueueSeries {
		if !p.cardinalityHit {
			p.cardinalityHit = true
			if p.log != nil {
				p.log.Warn("too many queues for per-queue metrics; publishing only the total",
					zap.Int("queues", len(stats)),
					zap.Int("limit", maxQueueSeries))
			}
		}
		p.collector.QueuesTotal.Set(float64(len(stats)))
		return
	}
	p.cardinalityHit = false

	current := make(map[queueKey]struct{}, len(stats))
	for _, stat := range stats {
		vhost := stat.VHost
		if vhost == "" {
			vhost = "/"
		}
		key := queueKey{name: stat.Name, vhost: vhost}
		current[key] = struct{}{}
		p.collector.UpdateQueueReadyAndConsumers(stat.Name, vhost, stat.Ready, stat.ReadyKnown, stat.Consumers)
	}

	for key := range p.last {
		if _, ok := current[key]; ok {
			continue
		}
		p.collector.DeleteQueueMetrics(key.name, key.vhost)
		p.collector.DeleteMessageAge(key.name, key.vhost)
	}
	p.last = current

	p.collector.QueuesTotal.Set(float64(len(stats)))

	if alarms != nil {
		state := alarms()
		p.collector.ResourceAlarmActive.WithLabelValues("memory").Set(boolFloat(state&alarmMemory != 0))
		p.collector.ResourceAlarmActive.WithLabelValues("disk").Set(boolFloat(state&alarmDisk != 0))
	}
}

func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
