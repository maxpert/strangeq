package broker

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

func startDrainer(qs *QueueState, broker *StorageBroker, queueName string, stop <-chan struct{}, workers int) {
	for w := 0; w < workers; w++ {
		go func() {
			for {
				tag, ok := qs.Claim(stop, testTimer(qs))
				if !ok {
					return
				}
				broker.storage.DeleteMessage(queueName, tag)
				qs.ClaimInflight(tag)
				qs.AckAdvance(tag)
			}
		}()
	}
}

func BenchmarkQueueDispatch_Claim(b *testing.B) {
	qs := NewQueueState(0)
	defer qs.Close()
	stop, cancel := makeStop()
	defer cancel()

	for i := uint64(0); i < uint64(b.N)+1; i++ {
		qs.Publish(i)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := qs.Claim(stop, testTimer(qs)); !ok {
			b.Fatal("claim failed")
		}
	}
}

func BenchmarkQueueDispatch_Publish(b *testing.B) {
	qs := NewQueueState(0)
	defer qs.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := uint64(0); i < uint64(b.N); i++ {
		qs.Publish(i)
	}
}

func BenchmarkQueueDispatch_PublishClaimRoundtrip(b *testing.B) {
	qs := NewQueueState(0)
	defer qs.Close()
	stop, cancel := makeStop()
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()

	for i := uint64(0); i < uint64(b.N); i++ {
		qs.Publish(i)
		if _, ok := qs.Claim(stop, testTimer(qs)); !ok {
			b.Fatal("claim failed")
		}
	}
}

func BenchmarkQueueDispatch_ConcurrentPublishClaim(b *testing.B) {
	qs := NewQueueState(0)
	defer qs.Close()
	stop, cancel := makeStop()
	defer cancel()

	var tagCounter atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tag := tagCounter.Add(1)
			qs.Publish(tag)
			if _, ok := qs.Claim(stop, testTimer(qs)); !ok {
				b.Fatal("claim failed")
			}
		}
	})
}

func BenchmarkQueueDispatch_Requeue(b *testing.B) {
	qs := NewQueueState(0)
	defer qs.Close()
	stop, cancel := makeStop()
	defer cancel()

	for i := uint64(0); i < uint64(b.N); i++ {
		qs.Publish(i)
		tag, ok := qs.Claim(stop, testTimer(qs))
		if !ok {
			b.Fatal("claim failed")
		}
		qs.ClaimInflight(tag)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := uint64(0); i < uint64(b.N); i++ {
		qs.Requeue(i)
	}
}

func BenchmarkQueueDispatch_AckAdvance(b *testing.B) {
	qs := NewQueueState(0)
	defer qs.Close()
	stop, cancel := makeStop()
	defer cancel()

	for i := uint64(0); i < uint64(b.N); i++ {
		qs.Publish(i)
		tag, ok := qs.Claim(stop, testTimer(qs))
		if !ok {
			b.Fatal("claim failed")
		}
		qs.ClaimInflight(tag)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := uint64(0); i < uint64(b.N); i++ {
		qs.AckAdvance(i)
	}
}

func BenchmarkPublishMessage_SingleQueue(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	broker.DeclareQueue("bench-pub", false, false, false, nil)
	qs := broker.getOrCreateQueueState("bench-pub")
	qs.SetDepthHighWM(0)

	stop, cancel := makeStop()
	defer cancel()
	startDrainer(qs, broker, "bench-pub", stop, 4)

	msg := &protocol.Message{
		Body:       make([]byte, 256),
		Exchange:   "",
		RoutingKey: "bench-pub",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := broker.PublishMessage("", "bench-pub", msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishMessage_MultiQueue(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	broker.DeclareExchange("bench-fanout", "fanout", false, false, false, nil)

	const numQueues = 10
	var qss [numQueues]*QueueState
	for i := 0; i < numQueues; i++ {
		qname := fmt.Sprintf("bench-fq-%d", i)
		broker.DeclareQueue(qname, false, false, false, nil)
		broker.BindQueue(qname, "bench-fanout", "", nil)
		qss[i] = broker.getOrCreateQueueState(qname)
		qss[i].SetDepthHighWM(0)
	}

	stop, cancel := makeStop()
	defer cancel()
	for i := 0; i < numQueues; i++ {
		qname := fmt.Sprintf("bench-fq-%d", i)
		startDrainer(qss[i], broker, qname, stop, 2)
	}

	msg := &protocol.Message{
		Body:       make([]byte, 256),
		Exchange:   "bench-fanout",
		RoutingKey: "",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := broker.PublishMessage("bench-fanout", "", msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAcknowledgeMessage(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	broker.DeclareQueue("bench-ack", false, false, false, nil)
	broker.storage.RegisterConsumerCursor("bench-ack", "bench-ack-consumer")
	defer broker.storage.UnregisterConsumerCursor("bench-ack", "bench-ack-consumer")

	state := &ConsumerState{
		consumer: &protocol.Consumer{
			Tag:   "bench-ack-consumer",
			Queue: "bench-ack",
		},
		queueName: "bench-ack",
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	broker.activeConsumers.Store("bench-ack-consumer", state)
	defer broker.activeConsumers.Delete("bench-ack-consumer")

	qs := broker.getOrCreateQueueState("bench-ack")
	qs.SetDepthHighWM(0)

	body := make([]byte, 256)
	tag := uint64(0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tag++
		msg := &protocol.Message{
			Body:        body,
			Exchange:    "",
			RoutingKey:  "bench-ack",
			DeliveryTag: tag,
		}
		broker.storage.StoreMessage("bench-ack", msg)
		broker.storage.StorePendingAck(&protocol.PendingAck{
			QueueName:   "bench-ack",
			DeliveryTag: tag,
			ConsumerTag: "bench-ack-consumer",
			MessageID:   fmt.Sprintf("bench-ack:%d", tag),
		})
		broker.storage.DeliverToConsumer("bench-ack", "bench-ack-consumer", tag)
		broker.deliveryIndex.Store(tag, "bench-ack-consumer")
		qs.StoreInflight(tag, "bench-ack-consumer")
		qs.ClaimInflight(tag)
		b.StartTimer()

		if err := broker.AcknowledgeMessage("bench-ack-consumer", tag, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishConsumeAck_Roundtrip(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	broker.DeclareQueue("bench-rt", false, false, false, nil)
	broker.storage.RegisterConsumerCursor("bench-rt", "bench-rt-consumer")
	qs := broker.getOrCreateQueueState("bench-rt")
	qs.SetDepthHighWM(0)

	stop, cancel := makeStop()
	defer cancel()

	msg := &protocol.Message{
		Body:       make([]byte, 256),
		Exchange:   "",
		RoutingKey: "bench-rt",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := broker.PublishMessage("", "bench-rt", msg); err != nil {
			b.Fatal(err)
		}
		tag, ok := qs.Claim(stop, testTimer(qs))
		if !ok {
			b.Fatal("claim failed")
		}
		if _, err := broker.storage.GetMessage("bench-rt", tag); err != nil {
			qs.AckAdvance(tag)
			continue
		}
		qs.ClaimInflight(tag)
		broker.storage.DeliverToConsumer("bench-rt", "bench-rt-consumer", tag)
		broker.storage.DeleteMessage("bench-rt", tag)
		broker.storage.AckFromConsumer("bench-rt", "bench-rt-consumer", tag)
		qs.SetMinAckCursor(broker.storage.GetMinAckCursor("bench-rt"))
		qs.DeleteInflight(tag)
		qs.AckAdvance(tag)
	}
}
