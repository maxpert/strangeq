package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// startDrainer registers workers goroutines against wg (Add before spawning,
// Done on exit) so callers can wait for every drainer to actually stop
// claiming/deleting before tearing down storage. Closing `stop` alone is not
// enough: a drainer can be mid-DeleteMessage, racing storage.Close() (which
// mutates WALManager state unsynchronized against Acknowledge) the instant
// the benchmark function returns — reproducible under -race and, more
// rarely, as an outright nil-pointer panic without -race. Callers must
// `defer wg.Wait()` AFTER `defer cancel()` (defers run LIFO, so cancel()
// fires first, then Wait() blocks for drainer exit, then cleanup() closes
// storage last).
func startDrainer(wg *sync.WaitGroup, qs *QueueState, broker *StorageBroker, queueName string, stop <-chan struct{}, workers int) {
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				tag, _, ok := qs.Claim(stop, testTimer(qs))
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
		if _, _, ok := qs.Claim(stop, testTimer(qs)); !ok {
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
		if _, _, ok := qs.Claim(stop, testTimer(qs)); !ok {
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
			if _, _, ok := qs.Claim(stop, testTimer(qs)); !ok {
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
		tag, _, ok := qs.Claim(stop, testTimer(qs))
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
		tag, _, ok := qs.Claim(stop, testTimer(qs))
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
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	startDrainer(&wg, qs, broker, "bench-pub", stop, 4)

	// PublishMessage takes ownership of the message (it mutates DeliveryTag
	// and stores the pointer in the queue ring), so each publish must use a
	// fresh Message. Reusing one Message across iterations aliased the same
	// pointer in many ring slots; every DeleteMessage then failed its
	// tag-identity check, the ring count never decreased, and past the spill
	// threshold every publish became a synchronous WAL write — presenting as
	// a benchmark wedge. The Body is immutable after publish and safe to share.
	body := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg := &protocol.Message{
			Body:       body,
			Exchange:   "",
			RoutingKey: "bench-pub",
		}
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
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for i := 0; i < numQueues; i++ {
		qname := fmt.Sprintf("bench-fq-%d", i)
		startDrainer(&wg, qss[i], broker, qname, stop, 2)
	}

	// Fresh Message per publish: PublishMessage takes ownership (see
	// BenchmarkPublishMessage_SingleQueue).
	body := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg := &protocol.Message{
			Body:       body,
			Exchange:   "bench-fanout",
			RoutingKey: "",
		}
		if err := broker.PublishMessage("bench-fanout", "", msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublishMessage_MultiQueueTopic is the topic-exchange counterpart
// to BenchmarkPublishMessage_MultiQueue. Fanout's collectTargetQueues match
// is unconditional (every binding matches); topic additionally pays for
// matchTopicPattern's segment-by-segment recursion on every one of the N
// bindings. Both matter as fanout/topic N-queue publish coverage: a future
// per-target Policy() load (W4/W5/W6) would otherwise be measured only by
// single-queue benches, hiding its N-multiplier.
func BenchmarkPublishMessage_MultiQueueTopic(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	broker.DeclareExchange("bench-topic", "topic", false, false, false, nil)

	const numQueues = 10
	var qss [numQueues]*QueueState
	for i := 0; i < numQueues; i++ {
		qname := fmt.Sprintf("bench-tq-%d", i)
		broker.DeclareQueue(qname, false, false, false, nil)
		// A wildcard segment forces matchTopicPattern's recursive
		// word-by-word comparison instead of the pattern==routingKey /
		// pattern=="#" fast-return, so this exercises real matching cost
		// per binding, not just a literal-equality check.
		broker.BindQueue(qname, "bench-topic", "bench.*.pub", nil)
		qss[i] = broker.getOrCreateQueueState(qname)
		qss[i].SetDepthHighWM(0)
	}

	stop, cancel := makeStop()
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for i := 0; i < numQueues; i++ {
		qname := fmt.Sprintf("bench-tq-%d", i)
		startDrainer(&wg, qss[i], broker, qname, stop, 2)
	}

	// Fresh Message per publish: PublishMessage takes ownership (see
	// BenchmarkPublishMessage_SingleQueue).
	body := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg := &protocol.Message{
			Body:       body,
			Exchange:   "bench-topic",
			RoutingKey: "bench.topic.pub",
		}
		if err := broker.PublishMessage("bench-topic", "bench.topic.pub", msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeliverMessage_ManualAck exercises the real deliverMessage path
// (the blocking select onto consumer.Messages, deliver()'s pre-send
// bookkeeping, and the manual-ack settle via AcknowledgeMessage) rather than
// inlining the storage calls directly the way
// BenchmarkPublishConsumeAck_Roundtrip does. It publishes with no per-message
// Expiration, so it stands in as the "no per-message expiration" manual-ack
// baseline W4's TTL delivery-time head-check will be compared against.
func BenchmarkDeliverMessage_ManualAck(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	const queueName = "bench-deliver-manualack"
	const consumerTag = "bench-deliver-manualack-consumer"
	broker.DeclareQueue(queueName, false, false, false, nil)
	broker.storage.RegisterConsumerCursor(queueName, consumerTag)
	defer broker.storage.UnregisterConsumerCursor(queueName, consumerTag)

	qs := broker.getOrCreateQueueState(queueName)
	qs.SetDepthHighWM(0)

	consumer := &protocol.Consumer{
		Tag:      consumerTag,
		Queue:    queueName,
		NoAck:    false,
		Messages: make(chan *protocol.Delivery, 64),
	}
	state := &ConsumerState{
		consumer:  consumer,
		queueName: queueName,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
		gate:      newPrefetchGate(2000),
	}
	broker.activeConsumers.Store(consumerTag, state)
	defer broker.activeConsumers.Delete(consumerTag)

	stop, cancel := makeStop()
	defer cancel()

	// Acker drains deliverMessage's channel send and settles each delivery
	// through the real AcknowledgeMessage path (single ack per message),
	// mirroring a manual-ack client. Runs concurrently with the timed loop;
	// completion is verified after b.StopTimer() via the acked counter.
	var acked atomic.Int64
	ackerDone := make(chan struct{})
	go func() {
		defer close(ackerDone)
		for d := range consumer.Messages {
			if err := broker.AcknowledgeMessage(consumerTag, d.DeliveryTag, false); err != nil {
				b.Error(err)
			}
			acked.Add(1)
		}
	}()

	body := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := uint64(0); i < uint64(b.N); i++ {
		b.StopTimer()
		// QueueState's tags are its own zero-based tail-cursor sequence, not
		// an identity the caller controls (Publish(tag) only advances the
		// head watermark to tag+1 — Claim then hands back whatever the tail
		// cursor is, e.g. Publish(1) on a fresh queue yields Claim()==0, not
		// 1). Storage must be keyed by the tag Claim actually returns, or
		// deliverMessage's GetMessage silently takes the gap-tag path and
		// never sends to the consumer (see BenchmarkQueueDispatch_Claim for
		// the same zero-based Publish(i) convention).
		qs.Publish(i)
		claimedTag, redelivered, ok := qs.Claim(stop, testTimer(qs))
		if !ok {
			b.Fatal("claim failed")
		}
		msg := &protocol.Message{
			Body:        body,
			Exchange:    "",
			RoutingKey:  queueName,
			DeliveryTag: claimedTag,
		}
		broker.storage.StoreMessage(queueName, msg)
		b.StartTimer()

		broker.deliverMessage(qs, state, claimedTag, redelivered)
	}

	b.StopTimer()
	close(consumer.Messages)
	<-ackerDone
	if got := acked.Load(); got != int64(b.N) {
		b.Fatalf("acked %d of %d deliveries", got, b.N)
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
		gate:      newPrefetchGate(2000),
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

	// Fresh Message per publish: PublishMessage takes ownership (see
	// BenchmarkPublishMessage_SingleQueue).
	body := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg := &protocol.Message{
			Body:       body,
			Exchange:   "",
			RoutingKey: "bench-rt",
		}
		if err := broker.PublishMessage("", "bench-rt", msg); err != nil {
			b.Fatal(err)
		}
		tag, _, ok := qs.Claim(stop, testTimer(qs))
		if !ok {
			b.Fatal("claim failed")
		}
		if _, err := broker.storage.GetMessage("bench-rt", tag); err != nil {
			qs.GapSkipAdvance(tag)
			continue
		}
		qs.ClaimInflight(tag)
		broker.storage.DeliverToConsumer("bench-rt", "bench-rt-consumer", tag)
		broker.storage.DeleteMessage("bench-rt", tag)
		broker.storage.AckFromConsumer("bench-rt", "bench-rt-consumer", tag)
		qs.SetMinAckCursor(broker.storage.GetMinAckCursor("bench-rt"))
		qs.AckAdvance(tag)
	}
}
