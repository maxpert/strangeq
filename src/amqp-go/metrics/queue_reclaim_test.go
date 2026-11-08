package metrics

// FINDING 7 — per-queue series must be reclaimed on queue delete.
//
// The poller-driven delete path (queue_poller.go) calls DeleteQueueMetrics when
// a queue vanishes. Before the fix that removed only the four queue gauges and
// the high-water gauge, leaving amqp_segment_count{queue}/amqp_segment_size_bytes{queue}
// and the three hot-cache {queue} counters behind forever. This test asserts
// that after one pollOnce that omits the queue, NO amqp_* series with
// queue="q" survives.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeleteQueueMetrics_ReclaimsEveryQueueSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := newCollector(reg, "amqp")

	// Establish one series in every per-queue family DeleteQueueMetrics must
	// now reclaim.
	c.UpdateQueueReadyAndConsumers("q", "/", 3, true, 1, true, 2)
	c.QueueDepthHighWater.WithLabelValues("q", "/").Set(9)
	c.UpdateSegmentMetrics("q", 4, 4096)
	c.RecordHotCacheDeliveryHit("q")
	c.RecordHotCacheDeliveryColdRead("q")
	c.RecordHotCacheDeliveryPread("q", 2)

	// Sanity: the segment and hot-cache series exist before the delete pass.
	require.Equal(t, 4.0, testutil.ToFloat64(c.SegmentCount.WithLabelValues("q")))
	require.Equal(t, 1.0, testutil.ToFloat64(c.HotCacheDeliveryHits.WithLabelValues("q")))

	// One poll that no longer reports q: p.last seeded with q, provider omits it.
	p := &queuePoller{collector: c, last: map[queueKey]struct{}{
		{name: "q", vhost: "/"}: {},
	}}
	p.pollOnce(fakeQueueStats{stats: nil}, nil)

	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "queue" && lp.GetValue() == "q" {
					t.Fatalf("series %s{queue=\"q\"} survived the queue-delete poll; per-queue series are not reclaimed", f.GetName())
				}
			}
		}
	}
}
