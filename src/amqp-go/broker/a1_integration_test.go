package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

func TestA1Integration_PublishConsumeAck(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("test-ack", false, false, false, nil)

	msg := &protocol.Message{
		Body:       []byte("hello-ack"),
		Exchange:   "",
		RoutingKey: "test-ack",
	}
	if err := broker.PublishMessage("", "test-ack", msg); err != nil {
		t.Fatalf("PublishMessage: %v", err)
	}

	queueState := broker.getOrCreateQueueState("test-ack")
	stop, cancel := makeStop()
	defer cancel()

	var tag uint64
	var ok bool
	for {
		tag, _, ok = queueState.Claim(stop, testTimer(queueState))
		if !ok {
			t.Fatal("Claim failed")
		}
		retrieved, err := broker.storage.GetMessage("test-ack", tag)
		if err == nil {
			if string(retrieved.Body) != "hello-ack" {
				t.Errorf("body: got %q, want %q", retrieved.Body, "hello-ack")
			}
			queueState.ClaimInflight(tag)
			broker.storage.DeliverToConsumer("test-ack", "test-consumer", tag)
			broker.storage.DeleteMessage("test-ack", tag)
			broker.storage.AckFromConsumer("test-ack", "test-consumer", tag)
			queueState.SetMinAckCursor(broker.storage.GetMinAckCursor("test-ack"))
			queueState.AckAdvance(tag)
			break
		}
		queueState.GapSkipAdvance(tag)
	}

	if d := queueState.Depth(); d != 0 {
		t.Errorf("Depth after ack: got %d, want 0", d)
	}
}

func TestA1Integration_FanoutMultipleQueues(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareExchange("fanout-ex", "fanout", false, false, false, nil)
	broker.DeclareQueue("fq1", false, false, false, nil)
	broker.DeclareQueue("fq2", false, false, false, nil)
	broker.DeclareQueue("fq3", false, false, false, nil)
	broker.BindQueue("fq1", "fanout-ex", "", nil)
	broker.BindQueue("fq2", "fanout-ex", "", nil)
	broker.BindQueue("fq3", "fanout-ex", "", nil)

	msg := &protocol.Message{
		Body:       []byte("fanout-msg"),
		Exchange:   "fanout-ex",
		RoutingKey: "",
	}
	if err := broker.PublishMessage("fanout-ex", "", msg); err != nil {
		t.Fatalf("PublishMessage: %v", err)
	}

	for _, queue := range []string{"fq1", "fq2", "fq3"} {
		qs := broker.getOrCreateQueueState(queue)
		stop, cancel := makeStop()

		var tag uint64
		var ok bool
		for {
			tag, _, ok = qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Errorf("Claim for %s failed", queue)
				break
			}
			retrieved, err := broker.storage.GetMessage(queue, tag)
			if err == nil {
				if string(retrieved.Body) != "fanout-msg" {
					t.Errorf("%s body: got %q, want %q", queue, retrieved.Body, "fanout-msg")
				}
				if retrieved.DeliveryTag != tag {
					t.Errorf("%s DeliveryTag: got %d, want %d", queue, retrieved.DeliveryTag, tag)
				}
				break
			}
		}
		cancel()
	}
}

func TestA1Integration_RejectRequeue(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("test-requeue", false, false, false, nil)

	msg := &protocol.Message{
		Body:       []byte("requeue-me"),
		Exchange:   "",
		RoutingKey: "test-requeue",
	}
	if err := broker.PublishMessage("", "test-requeue", msg); err != nil {
		t.Fatalf("PublishMessage: %v", err)
	}

	qs := broker.getOrCreateQueueState("test-requeue")
	stop, cancel := makeStop()
	defer cancel()

	var tag1 uint64
	var ok bool
	for {
		tag1, _, ok = qs.Claim(stop, testTimer(qs))
		if !ok {
			t.Fatal("first Claim failed")
		}
		if _, err := broker.storage.GetMessage("test-requeue", tag1); err == nil {
			break
		}
	}

	qs.ClaimInflight(tag1)
	qs.Requeue(tag1)

	var tag2 uint64
	for {
		tag2, _, ok = qs.Claim(stop, testTimer(qs))
		if !ok {
			t.Fatal("second Claim failed after requeue")
		}
		if tag2 != tag1 {
			t.Errorf("requeued tag: got %d, want %d (original tag preserved)", tag2, tag1)
		}
		retrieved, err := broker.storage.GetMessage("test-requeue", tag2)
		if err == nil {
			if string(retrieved.Body) != "requeue-me" {
				t.Errorf("body after requeue: got %q, want %q", retrieved.Body, "requeue-me")
			}
			break
		}
	}
}

func TestA1Integration_BackpressureBlocksPublisher(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("test-bp", false, false, false, nil)
	qs := broker.getOrCreateQueueState("test-bp")
	qs.SetDepthHighWM(3)

	stop := qs.StopCh()

	qs.Publish(1)
	qs.Publish(2)
	qs.Publish(3)
	qs.ClaimInflight(1)
	qs.ClaimInflight(2)
	qs.ClaimInflight(3)

	if d := qs.Depth(); d < 3 {
		t.Errorf("Depth: got %d, want >= 3", d)
	}

	blocked := make(chan struct{})
	go func() {
		qs.WaitForCapacity(stop)
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("WaitForCapacity returned before any ack")
	case <-time.After(50 * time.Millisecond):
	}

	broker.storage.RegisterConsumerCursor("test-bp", "bp-consumer")
	broker.storage.DeliverToConsumer("test-bp", "bp-consumer", 1)
	broker.storage.DeliverToConsumer("test-bp", "bp-consumer", 2)
	broker.storage.DeliverToConsumer("test-bp", "bp-consumer", 3)

	broker.storage.AckFromConsumer("test-bp", "bp-consumer", 1)
	qs.SetMinAckCursor(broker.storage.GetMinAckCursor("test-bp"))
	qs.AckAdvance(1)

	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("WaitForCapacity not released after ack")
	}
}

func TestA1Integration_ConcurrentPublishConsume(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("test-concurrent", false, false, false, nil)
	qs := broker.getOrCreateQueueState("test-concurrent")
	stop, cancel := makeStop()
	defer cancel()

	const total = 500
	var published atomic.Int64
	var consumed atomic.Int64

	var pubWG sync.WaitGroup
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		for i := 0; i < total; i++ {
			msg := &protocol.Message{
				Body:       []byte("concurrent"),
				Exchange:   "",
				RoutingKey: "test-concurrent",
			}
			if err := broker.PublishMessage("", "test-concurrent", msg); err != nil {
				return
			}
			published.Add(1)
		}
	}()

	var consWG sync.WaitGroup
	consWG.Add(4)
	for w := 0; w < 4; w++ {
		go func() {
			defer consWG.Done()
			for {
				tag, _, ok := qs.Claim(stop, testTimer(qs))
				if !ok {
					return
				}
				if _, err := broker.storage.GetMessage("test-concurrent", tag); err != nil {
					qs.GapSkipAdvance(tag)
					continue
				}
				qs.ClaimInflight(tag)
				consumed.Add(1)
				qs.AckAdvance(tag)
			}
		}()
	}

	go func() {
		for {
			if consumed.Load() >= int64(total) {
				cancel()
				qs.WakeAll()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	pubWG.Wait()
	consWG.Wait()

	if consumed.Load() != int64(total) {
		t.Errorf("consumed: got %d, want %d", consumed.Load(), total)
	}
}
