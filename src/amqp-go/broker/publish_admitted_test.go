package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WS-I review (item 2): PublishMessageAdmitted never parks in WaitForCapacity:
// a target at its high-water mark is reported (gate) with NOTHING stored; an
// admitted publish stores synchronously.
func TestPublishMessageAdmitted_NeverParksAtHWM(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState("q")
	qs.SetDepthHighWM(2)

	// Fill to HWM (transient publishes: admitted + stored).
	for i := 0; i < 2; i++ {
		gate, err := b.PublishMessageAdmitted("", "q", &protocol.Message{
			Body: []byte("fill"), RoutingKey: "q", DeliveryMode: 1,
		})
		require.NoError(t, err)
		require.Nil(t, gate, "under HWM: admitted")
	}
	require.True(t, qs.AtHighWaterMark())
	require.Equal(t, uint32(2), b.GetQueueReadyCount("q"))

	// At HWM: NOT admitted, NOTHING stored, and the call returns PROMPTLY (no
	// WaitForCapacity park — it would block this goroutine forever, no consumer).
	done := make(chan protocol.DepthGate, 1)
	go func() {
		gate, err := b.PublishMessageAdmitted("", "q", &protocol.Message{
			Body: []byte("over"), RoutingKey: "q", DeliveryMode: 1,
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
	assert.Equal(t, uint32(2), b.GetQueueReadyCount("q"), "a non-admitted publish stores nothing")

	// Drain one (simulated consumer claim + ack), then admission succeeds.
	stop, cancel := makeStop()
	defer cancel()
	tag, _, ok := qs.Claim(stop, testTimer(qs))
	require.True(t, ok)
	qs.ClaimInflight(tag) // waiting → inflight
	qs.AckAdvance(tag)    // inflight → settled
	require.Eventually(t, func() bool { return !qs.AtHighWaterMark() }, 30*time.Second, time.Millisecond)

	gate, err := b.PublishMessageAdmitted("", "q", &protocol.Message{
		Body: []byte("after"), RoutingKey: "q", DeliveryMode: 1,
	})
	require.NoError(t, err)
	assert.Nil(t, gate)
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
