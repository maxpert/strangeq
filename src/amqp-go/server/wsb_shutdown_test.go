package server

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/storage"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wsbCtx(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// TestWSB_Shutdown_ClientSees320: a connected client receives connection.close
// 320 CONNECTION_FORCED, the listener stops, and new dials fail.
func TestWSB_Shutdown_ClientSees320(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	_ = ch
	errCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	ctx, cancel := wsbCtx(5 * time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	select {
	case e := <-errCh:
		require.NotNil(t, e)
		assert.Equal(t, 320, e.Code)
		assert.True(t, e.Server, "close must come from the server")
	case <-time.After(2 * time.Second):
		t.Fatal("client never saw connection.close 320")
	}

	assert.False(t, srv.IsListening())
	_, err = amqp.Dial("amqp://guest:guest@" + addr + "/")
	assert.Error(t, err, "new dials must fail after shutdown")
}

// TestWSB_Shutdown_ClosesOwnedStorageAndRecovers: Shutdown closes the
// builder-owned storage, and a server rebuilt on the same path recovers the
// durable message.
func TestWSB_Shutdown_ClosesOwnedStorageAndRecovers(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = dir
	cfg.Server.LogLevel = "silent"

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	require.Eventually(t, srv.IsListening, 2*time.Second, 10*time.Millisecond)
	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("dur-q", true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Confirm(false))
	pub, err := ch.PublishWithDeferredConfirm("", "dur-q", false, false,
		amqp.Publishing{DeliveryMode: 2, Body: []byte("durable-1")})
	require.NoError(t, err)
	require.True(t, pub.Wait(), "persistent publish must be confirmed")
	require.NoError(t, conn.Close())

	ctx, cancel := wsbCtx(5 * time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
	assert.True(t, srv.storageClosed.Load(), "owned storage must be closed")

	// Linux only: no fds remain open under the storage path.
	if runtime.GOOS == "linux" {
		matches, _ := fdCountUnder(dir)
		assert.Equal(t, 0, matches, "no fds under the storage path after close")
	}

	// Rebuild on the same path: recovery returns the durable message.
	cfg2 := config.DefaultConfig()
	cfg2.Network.Address = "127.0.0.1:0"
	cfg2.Storage.Path = dir
	cfg2.Server.LogLevel = "silent"
	srv2, err := NewServerBuilder().WithConfig(cfg2).Build()
	require.NoError(t, err, "rebuilt server on a cleanly closed store must boot")
	errCh2 := make(chan error, 1)
	go func() { errCh2 <- srv2.Start() }()
	require.Eventually(t, srv2.IsListening, 2*time.Second, 10*time.Millisecond)
	defer func() {
		ctx2, cancel2 := wsbCtx(5 * time.Second)
		defer cancel2()
		_ = srv2.Shutdown(ctx2)
	}()

	srv2.Mutex.RLock()
	addr2 := srv2.Listener.Addr().String()
	srv2.Mutex.RUnlock()
	conn2, err := amqp.Dial("amqp://guest:guest@" + addr2 + "/")
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	msg, ok, err := ch2.Get("dur-q", true)
	require.NoError(t, err)
	require.True(t, ok, "durable message must recover from a cleanly closed store")
	assert.Equal(t, "durable-1", string(msg.Body))
}

// fdCountUnder counts open fds under path (Linux /proc only).
func fdCountUnder(path string) (int, error) {
	if runtime.GOOS != "linux" {
		return 0, nil
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	count := 0
	for _, e := range entries {
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, path) {
			count++
		}
	}
	return count, nil
}

// TestWSB_Shutdown_Idempotent: concurrent and repeated Shutdown calls share
// one execution and one result.
func TestWSB_Shutdown_Idempotent(t *testing.T) {
	srv, _ := startWSBServer(t, nil, nil)

	ctx, cancel := wsbCtx(5 * time.Second)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = srv.Shutdown(ctx)
		}(i)
	}
	wg.Wait()
	third := srv.Shutdown(ctx)

	assert.Equal(t, results[0], results[1], "concurrent Shutdowns must return the same value")
	assert.Equal(t, results[0], third, "repeated Shutdown must return the same value")
	assert.NoError(t, results[0])
}

// TestWSB_Shutdown_ForceClosesNonCooperativeClient: with a client that never
// reads (no close-ok), Shutdown hits the context deadline, force-closes, and
// STILL closes storage cleanly after the forced join.
func TestWSB_Shutdown_ForceClosesNonCooperativeClient(t *testing.T) {
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		s.closeOKTimeoutOverride = 10 * time.Second // longer than the shutdown ctx
	})

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	// Never read or reply again.

	ctx, cancel := wsbCtx(200 * time.Millisecond)
	defer cancel()
	start := time.Now()
	err := srv.Shutdown(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "want ctx deadline, got %v", err)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.True(t, srv.storageClosed.Load(), "storage must close after the forced join")

	// The client first sees connection.close 320 (buffered, unread until now),
	// then EOF from the force-close.
	r.expectConnectionClose(320)
	r.expectEOF()
}

// TestWSB_Shutdown_LeavesInjectedStorageOpen: storage provided via WithStorage
// is NOT closed by Shutdown (the caller owns it).
func TestWSB_Shutdown_LeavesInjectedStorageOpen(t *testing.T) {
	st, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() }) // the TEST owns and closes it

	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir() // unused: storage is injected
	cfg.Server.LogLevel = "silent"
	srv, err := NewServerBuilder().WithConfig(cfg).WithStorage(st).Build()
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	require.Eventually(t, srv.IsListening, 2*time.Second, 10*time.Millisecond)

	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()
	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("inj-q", true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	ctx, cancel := wsbCtx(5 * time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
	assert.False(t, srv.storageClosed.Load(), "injected storage must NOT be closed by Shutdown")

	_, err = st.GetQueue("inj-q")
	assert.NoError(t, err, "injected storage must still work")
}

// TestWSB_Stop_RemainsListenerOnly: Stop() is unchanged — existing
// connections keep working; only the listener closes.
func TestWSB_Stop_RemainsListenerOnly(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("stop-q", false, true, false, false, nil)
	require.NoError(t, err)

	require.NoError(t, srv.Stop())
	assert.False(t, srv.IsListening())

	// Existing connection still round-trips.
	require.NoError(t, ch.Publish("", "stop-q", false, false, amqp.Publishing{Body: []byte("m")}))
	msg, ok, err := ch.Get("stop-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "m", string(msg.Body))
}

// TestWSB_Health_Lifecycle: Ready()/Health() track the lifecycle.
func TestWSB_Health_Lifecycle(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"
	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err)

	assert.False(t, srv.Ready(), "not started")

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	require.Eventually(t, srv.IsListening, 2*time.Second, 10*time.Millisecond)

	assert.True(t, srv.Ready())
	assert.Equal(t, "healthy", srv.Health().Status)

	ctx, cancel := wsbCtx(5 * time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	assert.False(t, srv.Ready())
	assert.Equal(t, "stopped", srv.Health().Status)
}
