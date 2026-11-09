package storage

import (
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// hotCacheDeliveryResolverSpy implements the OPTIONAL
// hotCacheDeliveryMetricsResolver capability. It counts RESOLUTIONS and the
// increments that reach the resolved handles, and it counts the fallback
// RecordHotCacheDelivery* calls, so a test can prove:
//
//   - the resolver runs ONCE per queue (not once per delivered message), and
//   - the resolved path does NOT also call the per-call methods (which would
//     double-count).
type hotCacheDeliveryResolverSpy struct {
	durabilityMetricsSpy // the rest of the StorageMetrics surface
	resolves             atomic.Int64
	hits                 atomic.Int64
	coldReads            atomic.Int64
	preads               atomic.Int64
	fallbackCalls        atomic.Int64
}

func (m *hotCacheDeliveryResolverSpy) ResolveHotCacheDeliveryMetrics(string) (func(), func(), func(int)) {
	m.resolves.Add(1)
	return func() { m.hits.Add(1) },
		func() { m.coldReads.Add(1) },
		func(n int) { m.preads.Add(int64(n)) }
}

func (m *hotCacheDeliveryResolverSpy) RecordHotCacheDeliveryHit(string)      { m.fallbackCalls.Add(1) }
func (m *hotCacheDeliveryResolverSpy) RecordHotCacheDeliveryColdRead(string) { m.fallbackCalls.Add(1) }
func (m *hotCacheDeliveryResolverSpy) RecordHotCacheDeliveryPread(string, int) {
	m.fallbackCalls.Add(1)
}

// fallbackOnlySpy does NOT implement the resolver: the delivery path must fall
// back to the per-call interface methods.
type fallbackOnlySpy struct {
	durabilityMetricsSpy
	hits   atomic.Int64
	preads atomic.Int64
}

func (m *fallbackOnlySpy) RecordHotCacheDeliveryHit(string)        { m.hits.Add(1) }
func (m *fallbackOnlySpy) RecordHotCacheDeliveryPread(string, int) { m.preads.Add(1) }

func deliveryMetricsTestEngine(t *testing.T) *StorageEngine {
	t.Helper()
	ds, err := NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// TestDeliveryMetricsResolvedOncePerQueue is the falsifiable form of T4: the
// queue's Prometheus counters must be resolved ONCE and then incremented
// through the resolved handle — not re-resolved (label hash + vector RWMutex)
// on every delivered message.
func TestDeliveryMetricsResolvedOncePerQueue(t *testing.T) {
	ds := deliveryMetricsTestEngine(t)
	spy := &hotCacheDeliveryResolverSpy{}
	ds.SetMetrics(spy)

	const q = "resolve-q"
	msg := &protocol.Message{Exchange: "", RoutingKey: q, Body: []byte("body"), DeliveryMode: 1, DeliveryTag: 1}
	require.NoError(t, ds.StoreMessage(q, msg))

	const deliveries = 16
	for i := 0; i < deliveries; i++ {
		_, err := ds.GetMessageForDelivery(q, 1)
		require.NoError(t, err)
	}

	require.EqualValues(t, deliveries, ds.deliveryHits.Load(), "in-process hit counter")
	require.EqualValues(t, 1, spy.resolves.Load(),
		"the queue's delivery metrics must be resolved ONCE, not per delivery")
	require.EqualValues(t, deliveries, spy.hits.Load(),
		"every delivery must increment the RESOLVED hit handle")
	require.Zero(t, spy.fallbackCalls.Load(),
		"the resolved path must not also call the per-call methods (double count)")
}

// TestDeliveryMetricsColdReadResolvedHandle covers the cold arm: the cold read
// and its positional reads must go through the same resolved handles.
func TestDeliveryMetricsColdReadResolvedHandle(t *testing.T) {
	ds := deliveryMetricsTestEngine(t)
	spy := &hotCacheDeliveryResolverSpy{}
	ds.SetMetrics(spy)

	const q = "cold-q"
	msg := &protocol.Message{Exchange: "", RoutingKey: q, Body: []byte("body"), DeliveryMode: 2, DeliveryTag: 1}
	require.NoError(t, ds.StoreMessage(q, msg))
	// Evict it from the hot cache so the delivery must read the WAL.
	require.True(t, ds.hot.Delete(q, 1))

	_, err := ds.GetMessageForDelivery(q, 1)
	require.NoError(t, err)

	require.EqualValues(t, 1, ds.deliveryColdReads.Load())
	require.EqualValues(t, 1, spy.coldReads.Load(),
		"the cold read must increment the RESOLVED cold-read handle")
	require.EqualValues(t, 2, spy.preads.Load(),
		"the WAL parse's positional reads must go through the RESOLVED pread handle")
	require.EqualValues(t, 1, spy.resolves.Load(), "still resolved once per queue")
	require.Zero(t, spy.fallbackCalls.Load())
}

// TestDeliveryMetricsFallbackWithoutResolver pins that a collector which does
// NOT implement the optional resolver keeps working through the per-call
// methods, so the capability is truly optional.
func TestDeliveryMetricsFallbackWithoutResolver(t *testing.T) {
	ds := deliveryMetricsTestEngine(t)
	spy := &fallbackOnlySpy{}
	ds.SetMetrics(spy)

	const q = "fallback-q"
	msg := &protocol.Message{Exchange: "", RoutingKey: q, Body: []byte("body"), DeliveryMode: 1, DeliveryTag: 1}
	require.NoError(t, ds.StoreMessage(q, msg))

	for i := 0; i < 5; i++ {
		_, err := ds.GetMessageForDelivery(q, 1)
		require.NoError(t, err)
	}

	require.EqualValues(t, 5, spy.hits.Load(),
		"a collector without the resolver must still receive every hit via the per-call method")
}

// TestDeliveryMetricsResolvedForQueueCreatedBeforeSetMetrics pins the ordering
// hazard: a queue created BEFORE the collector is bound must still be resolved,
// not left on the fallback path forever.
func TestDeliveryMetricsResolvedForQueueCreatedBeforeSetMetrics(t *testing.T) {
	ds := deliveryMetricsTestEngine(t)

	const q = "late-collector-q"
	msg := &protocol.Message{Exchange: "", RoutingKey: q, Body: []byte("body"), DeliveryMode: 1, DeliveryTag: 1}
	require.NoError(t, ds.StoreMessage(q, msg)) // creates the queue with no collector bound

	spy := &hotCacheDeliveryResolverSpy{}
	ds.SetMetrics(spy)

	for i := 0; i < 4; i++ {
		_, err := ds.GetMessageForDelivery(q, 1)
		require.NoError(t, err)
	}

	require.EqualValues(t, 1, spy.resolves.Load(), "SetMetrics must resolve existing queues")
	require.EqualValues(t, 4, spy.hits.Load())
	require.Zero(t, spy.fallbackCalls.Load())
}
