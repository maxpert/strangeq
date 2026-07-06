package broker

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// TestMultiAckHighVolumeRegime replicates the BenchmarkVersus_MultiAck1000
// regime at the broker level: a prefetch-0 (capped) manual-ack consumer, a
// publisher that floods far more messages than the depth high-water mark (so
// PublishMessage parks in WaitForCapacity, exactly like processFrames does),
// and a consumer goroutine that multi-acks every 1000th received message.
// Progress requires the gate window to stay exact: any lost multi-ack or credit
// leak collapses the window below the cadence and the flow wedges.
func TestMultiAckHighVolumeRegime(t *testing.T) {
	if testing.Short() {
		t.Skip("high-volume regime test")
	}
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("regime", false, false, false, nil)

	const (
		total   = 200000
		cadence = 1000
	)

	consumer := &protocol.Consumer{
		Tag:           "regime-consumer",
		Queue:         "regime",
		NoAck:         false,
		Messages:      make(chan *protocol.Delivery, 100), // server uses cap 100
		PrefetchCount: 0,                                  // "unlimited" -> capped at 2000
	}
	if err := broker.RegisterConsumer("regime", "regime-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	// Publisher goroutine — floods the queue; WaitForCapacity will park it at
	// the high-water mark just like the real frame processor.
	var published atomic.Int64
	pubDone := make(chan error, 1)
	go func() {
		for i := 0; i < total; i++ {
			msg := &protocol.Message{Body: []byte("m"), Exchange: "", RoutingKey: "regime"}
			if err := broker.PublishMessage("", "regime", msg); err != nil {
				pubDone <- fmt.Errorf("publish %d: %w", i, err)
				return
			}
			published.Add(1)
		}
		pubDone <- nil
	}()

	// Consumer goroutine — mirrors the benchmark client: receive, multi-ack
	// every 1000th.
	var received atomic.Int64
	consDone := make(chan struct{})
	go func() {
		defer close(consDone)
		var count int64
		for d := range consumer.Messages {
			count++
			received.Store(count)
			if count%cadence == 0 {
				broker.AcknowledgeMessage("regime-consumer", d.DeliveryTag, true)
			}
			if count == total {
				return
			}
		}
	}()

	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
	lastReceived := int64(-1)
	for {
		select {
		case <-consDone:
			if err := <-pubDone; err != nil {
				t.Fatal(err)
			}
			return // all received — regime flows
		case <-progress.C:
			r := received.Load()
			if r == lastReceived {
				// No progress in 5s — dump the accounting and fail.
				inflight := gateInflight(t, broker, "regime-consumer")
				unacked := unackedCount(t, broker, "regime", "regime-consumer")
				qs := broker.getOrCreateQueueState("regime")
				t.Fatalf("WEDGED at received=%d published=%d: gate.inflight=%d unacked=%d "+
					"depth=%d waiting=%d qinflight=%d chanBuf=%d",
					r, published.Load(), inflight, unacked,
					qs.Depth(), qs.WaitingCount(), qs.InflightCount(), len(consumer.Messages))
			}
			lastReceived = r
		case <-deadline.C:
			t.Fatalf("regime did not complete in 60s: received=%d published=%d",
				received.Load(), published.Load())
		}
	}
}
