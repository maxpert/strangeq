package metrics

import (
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nonAlnum = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// sanitize makes a unique Prometheus namespace from a test name.
func sanitize(name string) string {
	return nonAlnum.ReplaceAllString(name, "_")
}

type fakeQueueStats struct {
	stats []QueueStat
}

func (f fakeQueueStats) QueueStats() []QueueStat { return f.stats }

func TestQueuePoller_PopulatesAndDeletes(t *testing.T) {
	c := NewCollector("wse_poller_" + sanitize(t.Name()))
	p := &queuePoller{collector: c}

	p.pollOnce(fakeQueueStats{stats: []QueueStat{
		{Name: "q1", VHost: "/", Ready: 5, ReadyKnown: true, Consumers: 2},
		{Name: "q2", VHost: "/", Ready: 0, ReadyKnown: false, Consumers: 1},
	}}, func() uint32 { return 1 }) // AlarmMemory

	assert.Equal(t, 5.0, testutil.ToFloat64(c.QueueMessagesReady.WithLabelValues("q1", "/")))
	assert.Equal(t, 2.0, testutil.ToFloat64(c.QueueConsumers.WithLabelValues("q1", "/")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.QueueConsumers.WithLabelValues("q2", "/")))
	assert.Equal(t, 2.0, testutil.ToFloat64(c.QueuesTotal))
	assert.Equal(t, 0.0, testutil.ToFloat64(c.QueueMessagesUnacked.WithLabelValues("q1", "/")),
		"unacked must not be fabricated")

	// q2 has no ready count: no series may be created at all.
	assert.Equal(t, 1, testutil.CollectAndCount(c.QueueMessagesReady),
		"only q1 has a known ready count")

	// Alarm memory set, disk clear.
	assert.Equal(t, 1.0, testutil.ToFloat64(c.ResourceAlarmActive.WithLabelValues("memory")))
	assert.Equal(t, 0.0, testutil.ToFloat64(c.ResourceAlarmActive.WithLabelValues("disk")))

	// Second tick: q2 vanished; its consumer series is deleted.
	p.pollOnce(fakeQueueStats{stats: []QueueStat{
		{Name: "q1", VHost: "/", Ready: 3, ReadyKnown: true, Consumers: 1},
	}}, func() uint32 { return 0 })
	assert.Equal(t, 3.0, testutil.ToFloat64(c.QueueMessagesReady.WithLabelValues("q1", "/")))
	assert.Equal(t, 1.0, testutil.ToFloat64(c.QueuesTotal))
	assert.Equal(t, 1, testutil.CollectAndCount(c.QueueConsumers),
		"q2 consumer series should be deleted, leaving only q1")
	assert.Equal(t, 0.0, testutil.ToFloat64(c.ResourceAlarmActive.WithLabelValues("memory")))
}

func TestQueuePoller_CardinalityLimit(t *testing.T) {
	c := NewCollector("wse_poller_limit_" + sanitize(t.Name()))
	p := &queuePoller{collector: c}

	stats := make([]QueueStat, maxQueueSeries+1)
	for i := range stats {
		stats[i] = QueueStat{Name: "q" + strings.Repeat("x", i+1), VHost: "/", Ready: 1, ReadyKnown: true}
	}
	p.pollOnce(fakeQueueStats{stats: stats}, nil)

	// Only the authoritative total is published; per-queue series are skipped.
	assert.Equal(t, float64(len(stats)), testutil.ToFloat64(c.QueuesTotal))
	assert.Equal(t, 0, testutil.CollectAndCount(c.QueueConsumers))
}

func TestQueuePoller_VHostLabel(t *testing.T) {
	c := NewCollector("wse_poller_vhost_" + sanitize(t.Name()))
	p := &queuePoller{collector: c}
	p.pollOnce(fakeQueueStats{stats: []QueueStat{
		{Name: "q", VHost: "", Ready: 1, ReadyKnown: true, Consumers: 1},
	}}, nil)
	// An empty vhost is normalized to "/" so the label is consistent with
	// the rest of the queue gauges.
	assert.Equal(t, 1.0, testutil.ToFloat64(c.QueueMessagesReady.WithLabelValues("q", "/")))
	require.Equal(t, 1, testutil.CollectAndCount(c.QueueMessagesReady))
}
