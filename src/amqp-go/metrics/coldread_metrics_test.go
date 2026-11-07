package metrics

// TASK 2 — the delivery-path classification and publisher-pacing series must be
// wired: a hot hit, a cold read and a positional read are counted
// independently, pacing counters accumulate, and the parked-current gauge is
// read at scrape time through the binder.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestColdReadAndPublisherParkMetrics(t *testing.T) {
	c := NewCollector("coldread_" + sanitize(t.Name()))

	c.RecordHotCacheDeliveryHit("q")
	c.RecordHotCacheDeliveryHit("q")
	c.RecordHotCacheDeliveryColdRead("q")
	c.RecordHotCacheDeliveryPread("q", 2)

	require.Equal(t, 2.0, testutil.ToFloat64(c.HotCacheDeliveryHits.WithLabelValues("q")))
	require.Equal(t, 1.0, testutil.ToFloat64(c.HotCacheDeliveryColdReads.WithLabelValues("q")))
	require.Equal(t, 2.0, testutil.ToFloat64(c.HotCacheDeliveryPreads.WithLabelValues("q")))

	c.RecordPublisherPark(0.5)
	c.RecordPublisherPark(0.25)
	require.Equal(t, 2.0, testutil.ToFloat64(c.PublisherParksTotal))
	require.InDelta(t, 0.75, testutil.ToFloat64(c.PublisherParkSecondsTotal), 1e-9)

	c.BindPublisherParkStats(func() (uint64, float64, int64) { return 7, 1.5, 3 })
	require.Equal(t, 3.0, testutil.ToFloat64(c.PublisherParkedCurrent))

	c.QueueDepthHighWater.WithLabelValues("q", "/").Set(1234)
	require.Equal(t, 1234.0, testutil.ToFloat64(c.QueueDepthHighWater.WithLabelValues("q", "/")))
}

func TestQueuePoller_PublishesDepthHighWater(t *testing.T) {
	c := NewCollector("hw_" + sanitize(t.Name()))
	p := &queuePoller{collector: c}

	p.pollOnce(fakeQueueStats{stats: []QueueStat{
		{Name: "q1", VHost: "/", Ready: 3, ReadyKnown: true, Consumers: 1, HighWater: 4242, HighWaterSet: true},
	}}, nil)
	require.Equal(t, 4242.0, testutil.ToFloat64(c.QueueDepthHighWater.WithLabelValues("q1", "/")))

	// The queue vanishes: its high-water series is deleted with the rest.
	p.pollOnce(fakeQueueStats{stats: nil}, nil)
	require.Equal(t, 0, testutil.CollectAndCount(c.QueueDepthHighWater))
}
