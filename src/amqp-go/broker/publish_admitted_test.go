package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WS-I closure (Sol HIGH): PublishMessageAdmitted synchronously STORES even
// when at HWM (bounded one-publish overshoot, exactly like the async durable
// path) AND returns the HWM gate for reader-level backpressure. StopCh and
// ALL targets are checked before any partial store. It NEVER parks in
// WaitForCapacity.
func TestPublishMessageAdmitted_StoresAtHWMWithGate(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState("q")
	qs.SetDepthHighWM(1)

	// Under HWM: admitted, stored, no gate.
	gate, err := b.PublishMessageAdmitted("", "q", &protocol.Message{
		Body: []byte("m1"), RoutingKey: "q", DeliveryMode: 1,
	})
	require.NoError(t, err)
	require.Nil(t, gate, "under HWM: no backpressure reported")
	require.Equal(t, uint32(1), b.GetQueueReadyCount("q"))
	require.True(t, qs.AtHighWaterMark())

	// AT HWM: the publish STILL STORES (bounded overshoot) AND returns the
	// gate — PROMPTLY (a WaitForCapacity park would block this goroutine
	// forever with no consumer).
	done := make(chan protocol.DepthGate, 1)
	go func() {
		gate, err := b.PublishMessageAdmitted("", "q", &protocol.Message{
			Body: []byte("m2"), RoutingKey: "q", DeliveryMode: 1,
		})
		require.NoError(t, err)
		done <- gate
	}()
	select {
	case gate := <-done:
		require.NotNil(t, gate, "at HWM the gate must be reported")
		require.True(t, gate.AtHighWaterMark())
	case <-time.After(3 * time.Second):
		t.Fatal("PublishMessageAdmitted parked at HWM")
	}
	assert.Equal(t, uint32(2), b.GetQueueReadyCount("q"), "stored even at HWM (bounded overshoot)")
}

// PublishMessageAdmitted: a fanout with one closed target errors BEFORE any
// partial store (StopCh checked across all targets first).
func TestPublishMessageAdmitted_NoPartialStoreOnClosedTarget(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}
	b.getOrCreateQueueState("q2").Close()

	_, err := b.PublishMessageAdmitted("fx", "", &protocol.Message{
		Body: []byte("m"), DeliveryMode: 1,
	})
	require.ErrorIs(t, err, ErrQueueClosed)
	assert.Equal(t, uint32(0), b.GetQueueReadyCount("q1"), "no partial store: q1 must hold nothing")
}

// PublishMessageAdmitted: no-route and closed-queue semantics mirror
// PublishMessage.
func TestPublishMessageAdmitted_NoRouteAndClosedQueue(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("ex0", "direct", true, false, false, nil))

	// No route, non-mandatory: nil, nil.
	gate, err := b.PublishMessageAdmitted("ex0", "nowhere", &protocol.Message{
		Body: []byte("m"), RoutingKey: "nowhere", DeliveryMode: 1,
	})
	require.NoError(t, err)
	assert.Nil(t, gate)

	// No route, mandatory: ErrNoRoute.
	_, err = b.PublishMessageAdmitted("ex0", "nowhere", &protocol.Message{
		Body: []byte("m"), RoutingKey: "nowhere", DeliveryMode: 1, Mandatory: true,
	})
	require.ErrorIs(t, err, ErrNoRoute)

	// Closed queue: ErrQueueClosed.
	_, err = b.DeclareQueue("cq", true, false, false, nil)
	require.NoError(t, err)
	b.getOrCreateQueueState("cq").Close()
	_, err = b.PublishMessageAdmitted("", "cq", &protocol.Message{
		Body: []byte("m"), RoutingKey: "cq", DeliveryMode: 1,
	})
	require.ErrorIs(t, err, ErrQueueClosed)
}
