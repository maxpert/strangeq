package broker

// TASK 2 — the requeue-on-consumer-cancel path is unchanged by the hot-cache
// pacing fix.
//
// A tiny hot budget makes every durable publish disk-only, so every delivery
// and every REDELIVERY is a cold WAL read: exactly the path the pacing change
// could break if it had touched the settle/requeue machinery. Cancelling the
// consumer must still requeue its delivered-but-unacked messages, and another
// consumer must still receive them.

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

func TestRequeueStillRedeliversUnderHotCachePacing(t *testing.T) {
	const queue = "requeue-q"
	ec := interfaces.EngineConfig{
		// No message fits: every durable publish is WAL-only, so requeue
		// redelivery is a cold read.
		HotCacheQueueBytes:    1,
		DepthHighWMOverride:   1_000_000,
		RingBufferSize:        65536,
		SpillThresholdPercent: 80,
	}
	store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
	require.NoError(t, err)
	b := NewStorageBroker(store, ec)
	t.Cleanup(func() { b.Close(); _ = store.Close() })
	_, err = b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)

	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         queue,
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	require.NoError(t, registerConsumer(b, queue, "consumer-a", consumerA))

	const n = 5
	for i := 0; i < n; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("cold-%d", i)), Exchange: "", RoutingKey: queue}
		require.NoError(t, b.PublishMessage("", queue, msg))
	}
	waitForChannelDepth(t, consumerA.Messages, n, 5*time.Second)

	original := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		d := <-consumerA.Messages
		original[d.DeliveryTag] = true
	}
	for tag := range original {
		consumerA.Messages <- &protocol.Delivery{DeliveryTag: tag}
	}

	consumerB := &protocol.Consumer{
		Tag:           "consumer-b",
		Queue:         queue,
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	require.NoError(t, registerConsumer(b, queue, "consumer-b", consumerB))
	require.NoError(t, b.UnregisterConsumer("consumer-a"))

	requeued := receiveN(t, consumerB.Messages, n, 5*time.Second)
	for _, d := range requeued {
		require.True(t, original[d.DeliveryTag],
			"redelivered tag %d must be one of the requeued originals", d.DeliveryTag)
	}
	_ = b.UnregisterConsumer("consumer-b")
}
