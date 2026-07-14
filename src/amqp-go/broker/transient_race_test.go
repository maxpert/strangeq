package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestTransientRace_NoStrandUnderConcurrentPublishers proves that concurrent
// transient publishers to the same queue do not strand messages: every
// published message must be delivered, and WaitingCount must reconcile to 0
// after full drain. Without always-reserve-at-mint, a neighbor transient's
// casMaxHead can advance head past a still-unstored lower tag, causing a
// consumer gap-skip that strands the message and leaks the waiting counter.
func TestTransientRace_NoStrandUnderConcurrentPublishers(t *testing.T) {
	const numQueues = 100
	const publishersPerQueue = 8
	const msgsPerPublisher = 5

	var totalLeakedWaiting atomic.Int64
	var totalLeakedInflight atomic.Int64

	for q := 0; q < numQueues; q++ {
		b, cleanup := createTestBroker(t)

		const qname = "q"
		_, err := b.DeclareQueue(qname, false, false, false, nil)
		require.NoError(t, err)
		qs := b.getOrCreateQueueState(qname)

		msgs := make(chan *protocol.Delivery, 256)
		var delivered atomic.Int64
		drainerDone := make(chan struct{})
		go func() {
			defer close(drainerDone)
			for range msgs {
				delivered.Add(1)
			}
		}()
		cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
		require.NoError(t, b.RegisterConsumer(qname, "c", cons))

		var wg sync.WaitGroup
		start := make(chan struct{})
		for p := 0; p < publishersPerQueue; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < msgsPerPublisher; i++ {
					m := &protocol.Message{RoutingKey: qname, Body: []byte("x"), DeliveryMode: 1}
					_ = b.PublishMessage("", qname, m)
				}
			}()
		}
		close(start)
		wg.Wait()

		require.Eventually(t, func() bool {
			return qs.tail.Load() >= qs.head.Load()
		}, 10*time.Second, 100*time.Microsecond, "consumer did not drain")

		require.NoError(t, b.UnregisterConsumer("c"))
		close(msgs)
		<-drainerDone

		totalLeakedWaiting.Add(qs.WaitingCount())
		totalLeakedInflight.Add(qs.InflightCount())

		cleanup()
	}

	require.Zero(t, totalLeakedWaiting.Load(), "transient publish must not leak WaitingCount")
	require.Zero(t, totalLeakedInflight.Load(), "transient publish must not leak InflightCount")
}
