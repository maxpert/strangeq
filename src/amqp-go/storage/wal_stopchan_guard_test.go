package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// M3 — WriteAsync, WriteSharedAsync, and Write must guard against sending on
// writeChan after the WAL's stopChan is closed (send-on-closed-channel panic)
// and against a nil sharedWAL pointer race during concurrent Close. The existing
// Acknowledge method already uses a select-on-stopChan guard; these three write
// paths must follow the same pattern.

// TestM3_WriteAsync_AfterClose_DoesNotPanicOrBlock proves that WriteAsync called
// after Close returns promptly with an error callback and does not panic.
func TestM3_WriteAsync_AfterClose_DoesNotPanicOrBlock(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, wm.Close())

	var callbacks atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			msg := &protocol.Message{Body: []byte("x"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: uint64(i + 1)}
			wm.WriteAsync("q", msg, uint64(i+1), func(err error) {
				callbacks.Add(1)
				assert.Error(t, err, "onDone must receive an error after close")
			})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WriteAsync blocked after Close (stopChan guard missing)")
	}
	require.Equal(t, int64(100), callbacks.Load(), "all onDone callbacks must fire")
}

// TestM3_WriteSharedAsync_AfterClose_ReturnsError proves WriteSharedAsync
// returns an error (not a panic, not a block) after Close.
func TestM3_WriteSharedAsync_AfterClose_ReturnsError(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, wm.Close())

	subs := []sharedSub{
		{queueName: "q1", offset: 1, message: &protocol.Message{Body: []byte("x"), DeliveryMode: 2, DeliveryTag: 1}},
		{queueName: "q2", offset: 2, message: &protocol.Message{Body: []byte("x"), DeliveryMode: 2, DeliveryTag: 2}},
	}
	err = wm.WriteSharedAsync(subs, []byte("shared-body"), 2)
	require.Error(t, err, "WriteSharedAsync must return error after close")
}

// TestM3_Write_AfterClose_ReturnsError proves the synchronous Write returns an
// error (not a panic, not a block) after Close.
func TestM3_Write_AfterClose_ReturnsError(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, wm.Close())

	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 1}
	done := make(chan error, 1)
	go func() {
		done <- wm.Write("q", msg, 1)
	}()
	select {
	case err := <-done:
		require.Error(t, err, "Write must return error after close")
	case <-time.After(5 * time.Second):
		t.Fatal("Write blocked after Close (stopChan guard missing)")
	}
}

// TestM3_ConcurrentClose_NoPanic races WriteAsync calls against Close to verify
// the nil-pointer race on sharedWAL is eliminated (local capture prevents the
// dereference-after-nil race).
func TestM3_ConcurrentClose_NoPanic(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)

	var callbacks atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				msg := &protocol.Message{Body: []byte("x"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 1}
				wm.WriteAsync("q", msg, 1, func(err error) {
					callbacks.Add(1)
				})
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, wm.Close())
	close(stop)
	wg.Wait()

	t.Logf("concurrent close: %d callbacks fired", callbacks.Load())
}
