package server

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wsbGetFunc is the basic.Get call shape the visibility poll needs.
type wsbGetFunc func(queue string, autoAck bool) (amqp.Delivery, bool, error)

// wsbGetT is the minimal *testing.T surface the getter seam needs. It exists so
// the persistent-error control can drive the failure path with a recorder
// instead of failing the real test: a fake *testing.T cannot be constructed
// (it has unexported methods), so the seam cannot take *testing.T literally and
// still be exercised on both outcomes. *testing.T satisfies this interface, so
// every production caller is unchanged.
type wsbGetT interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}

// getVisibleWith polls get until a delivery appears.
//
// A fire-and-forget publish on ANOTHER connection is NOT ordered with this
// connection's frames: AMQP orders only a channel's own frames (a publish is
// enqueued before that channel's next frame), so a Get issued from a freshly
// opened connection may legitimately race the enqueue. This waits for the
// message the same way this suite's later assertions already do, instead of
// hard-asserting on the first Get.
//
// R2: a successful poll clears any earlier transient Get error. Without this,
// one blip leaves gerr set and require.NoError fails on a poll that DID see the
// message — the error is stale state, not a signal. A persistent error never
// satisfies the condition, so it still times out and still fails.
func getVisibleWith(t wsbGetT, get wsbGetFunc, queue string) amqp.Delivery {
	t.Helper()
	var (
		got  amqp.Delivery
		gerr error
	)
	require.Eventually(t, func() bool {
		d, ok, err := get(queue, false)
		if err != nil {
			gerr = err
			return false
		}
		if !ok {
			return false
		}
		got, gerr = d, nil // a later success clears an earlier transient error
		return true
	}, 2*time.Second, 20*time.Millisecond, "message must become visible on %q", queue)
	require.NoError(t, gerr)
	return got
}

// getVisible is getVisibleWith bound to a real channel.
func getVisible(t *testing.T, ch *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	return getVisibleWith(t, ch.Get, queue)
}

// wsbFailRecorder implements wsbGetT without touching a real *testing.T, so a
// test can assert that the seam FAILS on a persistent error. FailNow mirrors
// the real method by exiting the calling goroutine; the control runs the seam
// on its own goroutine so that Goexit cannot abort the test itself.
type wsbFailRecorder struct {
	failed atomic.Bool
	mu     sync.Mutex
	msgs   []string
}

func (r *wsbFailRecorder) Helper() {}

func (r *wsbFailRecorder) Errorf(format string, args ...interface{}) {
	r.mu.Lock()
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

func (r *wsbFailRecorder) FailNow() {
	r.failed.Store(true)
	runtime.Goexit()
}

// TestWsbGetVisibleClearsTransientError: one transient Get error followed by a
// success must not leave gerr set. Red before the `gerr = nil` fix (the
// successful poll still reported the stale error); green after.
func TestWsbGetVisibleClearsTransientError(t *testing.T) {
	var calls atomic.Int64
	delivery := getVisibleWith(t, func(queue string, autoAck bool) (amqp.Delivery, bool, error) {
		if calls.Add(1) == 1 {
			return amqp.Delivery{}, false, errors.New("transient get failure")
		}
		return amqp.Delivery{Body: []byte("ok")}, true, nil
	}, "transient-q")
	require.Equal(t, "ok", string(delivery.Body), "the successful poll's delivery must be returned")
	require.GreaterOrEqual(t, calls.Load(), int64(2), "the poll must have retried after the transient error")
}

// TestWsbGetVisibleStillFailsOnPersistentError is the counterexample control:
// when Get never succeeds, the poll times out and the seam fails. It runs on
// its own goroutine because the recorder's FailNow exits the caller, exactly as
// a real *testing.T.FailNow would.
func TestWsbGetVisibleStillFailsOnPersistentError(t *testing.T) {
	rec := &wsbFailRecorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = getVisibleWith(rec, func(queue string, autoAck bool) (amqp.Delivery, bool, error) {
			return amqp.Delivery{}, false, errors.New("persistent get failure")
		}, "never")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("seam did not return after its poll timed out")
	}
	require.True(t, rec.failed.Load(),
		"a persistent Get error must still fail the poll, got messages %v", rec.msgs)
}

// TestWSB_GetRequeueScopedToClosingConnection: connection close must requeue
// ONLY its own unacked basic.get deliveries — not every connection's (P1:
// RequeueAllGetDeliveries requeued globally, so another client's later ack
// became a no-op and the message was delivered twice).
func TestWSB_GetRequeueScopedToClosingConnection(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	setup, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer setup.Close()
	sch, err := setup.Channel()
	require.NoError(t, err)
	_, err = sch.QueueDeclare("gq", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, sch.Publish("", "gq", false, false, amqp.Publishing{Body: []byte("A")}))
	require.NoError(t, sch.Publish("", "gq", false, false, amqp.Publishing{Body: []byte("B")}))

	conn1, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	c1, err := conn1.Channel()
	require.NoError(t, err)
	a := getVisible(t, c1, "gq")
	assert.Equal(t, "A", string(a.Body))

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	c2, err := conn2.Channel()
	require.NoError(t, err)
	b := getVisible(t, c2, "gq")
	assert.Equal(t, "B", string(b.Body))

	// Close conn1: ONLY its delivery (A) is requeued.
	require.NoError(t, conn1.Close())

	conn3, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn3.Close()
	c3, err := conn3.Channel()
	require.NoError(t, err)

	var gotA amqp.Delivery
	require.Eventually(t, func() bool {
		d, ok, err := c3.Get("gq", false)
		if err != nil || !ok {
			return false
		}
		gotA = d
		return true
	}, 2*time.Second, 20*time.Millisecond, "A must be requeued after conn1 closes")
	assert.Equal(t, "A", string(gotA.Body))
	assert.True(t, gotA.Redelivered)

	// The NEXT get is empty: B belongs to conn2 and must NOT have been
	// requeued (today's global requeue makes this return B — a duplicate).
	d2, ok, err := c3.Get("gq", true)
	require.NoError(t, err)
	assert.False(t, ok, "B must still be owned by conn2, got %v", d2)

	// conn2's ack of B still works (its delivery was never requeued).
	require.NoError(t, b.Ack(false))

	// After conn3 acks A, the queue is empty.
	require.NoError(t, gotA.Ack(false))
	require.Eventually(t, func() bool {
		q, err := c3.QueueDeclarePassive("gq", false, false, false, false, nil)
		return err == nil && q.Messages == 0
	}, 2*time.Second, 20*time.Millisecond)
}

// TestWSB_GetRequeueOwnDeliveriesOnClose: a connection's own unacked get
// deliveries come back after it closes.
func TestWSB_GetRequeueOwnDeliveriesOnClose(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	setup, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer setup.Close()
	sch, err := setup.Channel()
	require.NoError(t, err)
	_, err = sch.QueueDeclare("gq2", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, sch.Publish("", "gq2", false, false, amqp.Publishing{Body: []byte("A")}))

	conn1, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	c1, err := conn1.Channel()
	require.NoError(t, err)
	a := getVisible(t, c1, "gq2")
	assert.Equal(t, "A", string(a.Body))
	require.NoError(t, conn1.Close())

	require.Eventually(t, func() bool {
		d, ok, err := sch.Get("gq2", true)
		return err == nil && ok && string(d.Body) == "A"
	}, 2*time.Second, 20*time.Millisecond, "A must be available again after conn1 closes")
}
