//go:build !race

package broker

import (
	"fmt"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// These tests pin the SQ-6 zero-allocation guarantee of routeMessage with a
// reused scratch. They are excluded under -race because the race detector's
// instrumentation changes allocation behavior.

func TestRouteMessage_DefaultExchange_ZeroAlloc(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, broker, "alloc.default.q")
	broker.getOrCreateQueueState("alloc.default.q") // activeQueues hit path

	exchange := &protocol.Exchange{Name: "", Kind: "direct"}
	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "alloc.default.q"}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)

	allocs := testing.AllocsPerRun(200, func() {
		if err := broker.routeMessage(exchange, "alloc.default.q", msg, rs); err != nil {
			t.Fatal(err)
		}
		if len(rs.queues) != 1 {
			t.Fatalf("expected 1 target queue, got %d", len(rs.queues))
		}
	})
	require.Zero(t, allocs, "default-exchange routing must not allocate")
}

func TestRouteMessage_DirectExchangeNoE2E_ZeroAlloc(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("alloc.direct", "direct", false, false, false, nil))
	mustDeclareQueue(t, broker, "alloc.direct.q")
	require.NoError(t, broker.BindQueue("alloc.direct.q", "alloc.direct", "k", nil))

	exchange, err := broker.storage.GetExchange("alloc.direct")
	require.NoError(t, err)
	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)

	// Warm binding caches before measuring.
	require.NoError(t, broker.routeMessage(exchange, "k", msg, rs))

	allocs := testing.AllocsPerRun(200, func() {
		if err := broker.routeMessage(exchange, "k", msg, rs); err != nil {
			t.Fatal(err)
		}
		if len(rs.queues) != 1 {
			t.Fatalf("expected 1 target queue, got %d", len(rs.queues))
		}
	})
	require.Zero(t, allocs, "direct-exchange routing without e2e bindings must not allocate")
}

func TestRouteMessage_FanoutGeneralPath_SteadyStateZeroAlloc(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("alloc.fanout", "fanout", false, false, false, nil))
	for i := 0; i < 4; i++ {
		q := fmt.Sprintf("alloc.fanout.q%d", i)
		mustDeclareQueue(t, broker, q)
		require.NoError(t, broker.BindQueue(q, "alloc.fanout", "", nil))
	}

	exchange, err := broker.storage.GetExchange("alloc.fanout")
	require.NoError(t, err)
	msg := &protocol.Message{Body: []byte("x")}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)

	// Warm scratch maps, slice capacity, and binding caches.
	require.NoError(t, broker.routeMessage(exchange, "", msg, rs))

	allocs := testing.AllocsPerRun(200, func() {
		if err := broker.routeMessage(exchange, "", msg, rs); err != nil {
			t.Fatal(err)
		}
		if len(rs.queues) != 4 {
			t.Fatalf("expected 4 target queues, got %d", len(rs.queues))
		}
	})
	require.Zero(t, allocs, "map-based routing path must be allocation-free in steady state with a reused scratch")
}

// TestRouteMessage_TopicGeneralPath_SteadyStateZeroAlloc (WS-D T9.4) pins the
// topic general path: the iterative matcher (broker/topic_match.go) must stay
// allocation-free per binding, so routing through 4 topic bindings with
// '#' and '*' wildcards is zero-alloc in steady state.
// TestRouteMessage_DirectManyBindings_SteadyStateZeroAlloc (WS-D T14.2,
// !race): the >8-bindings direct fast path dedups through the POOLED scratch
// map — O(n) and zero-alloc in steady state.
func TestRouteMessage_DirectManyBindings_SteadyStateZeroAlloc(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("alloc.direct.many", "direct", false, false, false, nil))
	for i := 0; i < 100; i++ {
		q := fmt.Sprintf("alloc.direct.many.q%d", i)
		mustDeclareQueue(t, broker, q)
		require.NoError(t, broker.BindQueue(q, "alloc.direct.many", "k", nil))
	}

	exchange, err := broker.storage.GetExchange("alloc.direct.many")
	require.NoError(t, err)
	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)

	// Warm scratch maps, slice capacity, and binding caches.
	require.NoError(t, broker.routeMessage(exchange, "k", msg, rs))
	require.Len(t, rs.queues, 100)

	allocs := testing.AllocsPerRun(200, func() {
		if err := broker.routeMessage(exchange, "k", msg, rs); err != nil {
			t.Fatal(err)
		}
		if len(rs.queues) != 100 {
			t.Fatalf("expected 100 target queues, got %d", len(rs.queues))
		}
	})
	require.Zero(t, allocs, "direct many-bindings path must be allocation-free in steady state with a reused scratch")
}

// BenchmarkRouteDirect_1000Bindings (WS-D T14.3): old vs new ns/op for the
// direct fast path at 1000 same-key bindings.
func BenchmarkRouteDirect_1000Bindings(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	require.NoError(b, broker.DeclareExchange("bench.direct", "direct", false, false, false, nil))
	for i := 0; i < 1000; i++ {
		q := fmt.Sprintf("bench.direct.q%d", i)
		mustDeclareQueue(b, broker, q)
		require.NoError(b, broker.BindQueue(q, "bench.direct", "k", nil))
	}
	exchange, err := broker.storage.GetExchange("bench.direct")
	require.NoError(b, err)
	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.routeMessage(exchange, "k", msg, rs); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRouteMessage_TopicGeneralPath_SteadyStateZeroAlloc(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("alloc.topic", "topic", false, false, false, nil))
	patterns := []string{"a.#", "a.*.b", "#.b", "*.*.*"}
	for i, p := range patterns {
		q := fmt.Sprintf("alloc.topic.q%d", i)
		mustDeclareQueue(t, broker, q)
		require.NoError(t, broker.BindQueue(q, "alloc.topic", p, nil))
	}

	exchange, err := broker.storage.GetExchange("alloc.topic")
	require.NoError(t, err)
	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "a.x.b"}

	rs := acquireRouteScratch()
	defer releaseRouteScratch(rs)

	// Warm scratch maps, slice capacity, and binding caches.
	require.NoError(t, broker.routeMessage(exchange, "a.x.b", msg, rs))

	allocs := testing.AllocsPerRun(200, func() {
		if err := broker.routeMessage(exchange, "a.x.b", msg, rs); err != nil {
			t.Fatal(err)
		}
		if len(rs.queues) != 4 {
			t.Fatalf("expected 4 target queues, got %d", len(rs.queues))
		}
	})
	require.Zero(t, allocs, "topic general path must be allocation-free in steady state with a reused scratch")
}
