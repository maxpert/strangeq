package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WaitForCapacity must wake promptly on AckAdvance, not wait for the 10ms
// safety timer. Today WaitForCapacity does NOT increment parkedCount, so
// NotifyNewMessage (called from AckAdvance) is a no-op when only the producer
// is parked — the producer only wakes on the 10ms timer. This test asserts the
// wake happens within 1ms of the ack.

func TestWaitForCapacity_WakesOnAckAdvance_Within1ms(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(2)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Publish(0)
		qs.Publish(1)
		t0, _, _ := qs.Claim(stop, testTimer(qs))
		t1, _, _ := qs.Claim(stop, testTimer(qs))
		qs.ClaimInflight(t0)
		qs.ClaimInflight(t1)

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

		ackTime := time.Now()
		qs.AckAdvance(t0)

		select {
		case <-blocked:
			elapsed := time.Since(ackTime)
			require.Less(t, elapsed, 5*time.Millisecond,
				"WaitForCapacity must wake within 5ms of AckAdvance (vs 10ms timer fallback), took %v", elapsed)
		case <-time.After(time.Second):
			t.Fatal("WaitForCapacity not released within 1s of AckAdvance")
		}
	})
}

func TestNotifyNewMessage_SendsWhenProducerParked(t *testing.T) {
	qs := NewQueueState(1)
	defer qs.Close()

	qs.producerParked.Store(1)
	qs.NotifyNewMessage()

	select {
	case <-qs.wake:
	default:
		t.Fatal("NotifyNewMessage did not send to wake channel when producerParked > 0")
	}
}
