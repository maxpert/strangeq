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
