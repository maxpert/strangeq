package storage

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func ackCursorEq(t *testing.T, want, got uint64, msg string) {
	t.Helper()
	if want != got {
		t.Fatalf("%s: want %d, got %d", msg, want, got)
	}
}

func TestAckCursor_SingleConsumer_FIFO(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ackCursorEq(t, 0, ac.MinAckCursor(), "initial min")

	for tag := uint64(1); tag <= 5; tag++ {
		ac.OnPublish(tag)
		ac.OnDeliver(tag, "c1")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "after deliver 1-5")

	expected := []uint64{2, 3, 4, 5, 5}
	for i, tag := 0, uint64(1); tag <= 5; i, tag = i+1, tag+1 {
		ac.OnAck(tag, "c1")
		ackCursorEq(t, expected[i], ac.MinAckCursor(), fmt.Sprintf("after ack %d", tag))
	}
}

func TestAckCursor_SingleConsumer_OutOfOrder(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	for tag := uint64(1); tag <= 5; tag++ {
		ac.OnPublish(tag)
		ac.OnDeliver(tag, "c1")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "after deliver 1-5")

	for _, tag := range []uint64{3, 2, 1} {
		ac.OnAck(tag, "c1")
	}
	ackCursorEq(t, 4, ac.MinAckCursor(), "after acking 3,2,1, cursor jumps to next lowest unacked (4)")

	ac.OnAck(5, "c1")
	ackCursorEq(t, 4, ac.MinAckCursor(), "acking non-frontier 5 does not move cursor")

	ac.OnAck(4, "c1")
	ackCursorEq(t, 5, ac.MinAckCursor(), "all acked -> min = head = 5")
}

func TestAckCursor_MultiConsumer_GlobalMin(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("A")
	ac.OnConsumerRegister("B")
	for tag := uint64(1); tag <= 4; tag++ {
		ac.OnPublish(tag)
	}
	ac.OnDeliver(1, "A")
	ac.OnDeliver(3, "A")
	ac.OnDeliver(2, "B")
	ac.OnDeliver(4, "B")

	ackCursorEq(t, 1, ac.MinAckCursor(), "global min across A{1,3} and B{2,4}")

	if !ac.OnAck(1, "A") {
		t.Fatal("OnAck(1, A) should return true")
	}
	ackCursorEq(t, 2, ac.MinAckCursor(), "after ack 1 for A, min is B's 2")

	if !ac.OnAck(2, "B") {
		t.Fatal("OnAck(2, B) should return true")
	}
	ackCursorEq(t, 3, ac.MinAckCursor(), "after ack 2 for B, min is A's 3")
}

func TestAckCursor_NackSameAsAck(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	for tag := uint64(1); tag <= 3; tag++ {
		ac.OnPublish(tag)
		ac.OnDeliver(tag, "c1")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "after deliver 1-3")

	if !ac.OnNack(1, "c1") {
		t.Fatal("OnNack(1) should return true")
	}
	ackCursorEq(t, 2, ac.MinAckCursor(), "after nack 1")

	if !ac.OnNack(2, "c1") {
		t.Fatal("OnNack(2) should return true")
	}
	ackCursorEq(t, 3, ac.MinAckCursor(), "nack advances cursor past 2 to 3")
}

func TestAckCursor_RegisterUnregister(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("A")
	ac.OnConsumerRegister("B")
	if ac.ConsumerCount() != 2 {
		t.Fatalf("expected 2 consumers, got %d", ac.ConsumerCount())
	}

	ac.OnPublish(1)
	ac.OnDeliver(1, "A")
	ac.OnPublish(2)
	ac.OnDeliver(2, "B")
	ackCursorEq(t, 1, ac.MinAckCursor(), "min across A{1} and B{2}")

	ac.OnConsumerUnregister("A")
	ackCursorEq(t, 2, ac.MinAckCursor(), "after unregister A, min recomputed from B{2}")
	if ac.ConsumerCount() != 1 {
		t.Fatalf("expected 1 consumer, got %d", ac.ConsumerCount())
	}
}

func TestAckCursor_UnregisterAllConsumers(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(5)
	ac.OnDeliver(5, "c1")
	ackCursorEq(t, 5, ac.MinAckCursor(), "tag 5 unacked")

	ac.OnConsumerUnregister("c1")
	ackCursorEq(t, 5, ac.Head(), "head is highest published tag")
	ackCursorEq(t, ac.Head(), ac.MinAckCursor(), "no consumers remain -> min = head")
	if ac.ConsumerCount() != 0 {
		t.Fatalf("expected 0 consumers, got %d", ac.ConsumerCount())
	}
}

func TestAckCursor_NoConsumersMinIsHead(t *testing.T) {
	ac := NewAckCursor()
	ackCursorEq(t, 0, ac.MinAckCursor(), "fresh cursor min is 0")
	ackCursorEq(t, 0, ac.Head(), "fresh cursor head is 0")

	ac.OnPublish(10)
	ackCursorEq(t, 10, ac.Head(), "OnPublish advances head")
	ackCursorEq(t, 0, ac.MinAckCursor(), "no consumers -> OnPublish does not move min")
}

func TestAckCursor_IsSafeToOverwrite_DifferentSlots(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(1)
	ac.OnDeliver(1, "c1")

	if !ac.IsSafeToOverwrite(1, 2, 4) {
		t.Fatal("different slots (1&3=1, 2&3=2) should be safe")
	}
}

func TestAckCursor_IsSafeToOverwrite_SameSlotAcked(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(1)
	ac.OnDeliver(1, "c1")
	ackCursorEq(t, 1, ac.MinAckCursor(), "tag 1 unacked -> min 1")

	if !ac.IsSafeToOverwrite(0, 4, 4) {
		t.Fatal("same slot (0&3=0, 4&3=0), oldTag 0 < minAckCursor 1 -> safe")
	}
}

func TestAckCursor_IsSafeToOverwrite_SameSlotNotAcked(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(0)
	ac.OnDeliver(0, "c1")
	ackCursorEq(t, 0, ac.MinAckCursor(), "tag 0 unacked -> min 0")

	if ac.IsSafeToOverwrite(0, 4, 4) {
		t.Fatal("same slot, oldTag 0 still unacked (0 < minAckCursor 0 is false) -> not safe")
	}
}

func TestAckCursor_ConcurrentDeliverAck(t *testing.T) {
	const consumers = 100
	const tagsPer = 100
	total := consumers * tagsPer

	ac := NewAckCursor()
	for c := 0; c < consumers; c++ {
		ac.OnConsumerRegister(fmt.Sprintf("c%d", c))
	}

	var deliverWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		deliverWG.Add(1)
		go func(cid int) {
			defer deliverWG.Done()
			ctag := fmt.Sprintf("c%d", cid)
			for k := 0; k < tagsPer; k++ {
				tag := uint64(cid*tagsPer + k + 1)
				ac.OnPublish(tag)
				ac.OnDeliver(tag, ctag)
			}
		}(c)
	}
	deliverWG.Wait()

	ackCursorEq(t, uint64(total), ac.Head(), "head is highest published tag")
	ackCursorEq(t, 1, ac.MinAckCursor(), "lowest unacked is tag 1")

	var violated atomic.Bool
	done := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		prevMin := ac.MinAckCursor()
		for {
			select {
			case <-done:
				return
			default:
			}
			m := ac.MinAckCursor()
			h := ac.Head()
			if m > h {
				violated.Store(true)
			}
			if m < prevMin {
				violated.Store(true)
			} else if m > prevMin {
				prevMin = m
			}
			runtime.Gosched()
		}
	}()

	var ackWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		ackWG.Add(1)
		go func(cid int) {
			defer ackWG.Done()
			ctag := fmt.Sprintf("c%d", cid)
			order := make([]int, tagsPer)
			for i := range order {
				order[i] = i
			}
			r := rand.New(rand.NewSource(int64(cid + 1)))
			r.Shuffle(tagsPer, func(i, j int) { order[i], order[j] = order[j], order[i] })
			for _, k := range order {
				tag := uint64(cid*tagsPer + k + 1)
				ac.OnAck(tag, ctag)
			}
		}(c)
	}
	ackWG.Wait()
	close(done)
	<-monitorDone

	if violated.Load() {
		t.Fatal("minAckCursor exceeded head or went backwards during concurrent acks")
	}
	ackCursorEq(t, uint64(total), ac.MinAckCursor(), "all acked -> min = head")
}

func TestAckCursor_MinAckCursorMonotonic(t *testing.T) {
	const n = 100
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	for tag := uint64(1); tag <= n; tag++ {
		ac.OnPublish(tag)
		ac.OnDeliver(tag, "c1")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "after deliver 1-100")

	order := make([]uint64, n)
	for i := range order {
		order[i] = uint64(i + 1)
	}
	r := rand.New(rand.NewSource(7))
	r.Shuffle(int(n), func(i, j int) { order[i], order[j] = order[j], order[i] })

	prev := ac.MinAckCursor()
	for _, tag := range order {
		ac.OnAck(tag, "c1")
		cur := ac.MinAckCursor()
		if cur < prev {
			t.Fatalf("minAckCursor went backwards: %d -> %d after acking %d", prev, cur, tag)
		}
		prev = cur
	}
	ackCursorEq(t, n, ac.MinAckCursor(), "all acked -> min = head = 100")
}

func TestAckCursor_OnPublishZeroAllocs(t *testing.T) {
	ac := NewAckCursor()
	ac.OnPublish(1000)

	allocs := testing.AllocsPerRun(200, func() {
		ac.OnPublish(1000)
	})
	if allocs != 0 {
		t.Fatalf("OnPublish must not allocate, got %v", allocs)
	}
}

func TestAckCursor_MinAckCursorZeroAllocs(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(1)
	ac.OnDeliver(1, "c1")

	allocs := testing.AllocsPerRun(200, func() {
		_ = ac.MinAckCursor()
	})
	if allocs != 0 {
		t.Fatalf("MinAckCursor must not allocate, got %v", allocs)
	}
}

func TestAckCursor_OnAckNoAllocsAfterWarmup(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(1)
	ac.OnDeliver(1, "c1")
	ac.OnAck(1, "c1")

	allocs := testing.AllocsPerRun(200, func() {
		ac.OnAck(1, "c1")
	})
	if allocs != 0 {
		t.Fatalf("OnAck on empty consumer must not allocate, got %v", allocs)
	}
}

func TestAckCursor_DuplicateAck(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(1)
	ac.OnDeliver(1, "c1")

	if !ac.OnAck(1, "c1") {
		t.Fatal("first ack should return true")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "after ack 1, min = head = 1")

	if ac.OnAck(1, "c1") {
		t.Fatal("duplicate ack should return false")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "duplicate ack must not change min")
}

func TestAckCursor_AckUnknownConsumer(t *testing.T) {
	ac := NewAckCursor()
	if ac.OnAck(1, "ghost") {
		t.Fatal("ack for unknown consumer should return false")
	}
	ackCursorEq(t, 0, ac.MinAckCursor(), "unknown consumer ack must not change min")
}

func TestAckCursor_AckUnknownTag(t *testing.T) {
	ac := NewAckCursor()
	ac.OnConsumerRegister("c1")
	ac.OnPublish(1)
	ac.OnDeliver(1, "c1")

	if ac.OnAck(99, "c1") {
		t.Fatal("ack for unknown tag should return false")
	}
	ackCursorEq(t, 1, ac.MinAckCursor(), "unknown tag ack must not change min")
}
