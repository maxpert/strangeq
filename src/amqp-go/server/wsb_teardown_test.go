package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// TestWSB_Teardown_JoinsAllGoroutinesBeforeCleanup: when a connection with a
// running consumer dies abruptly, EVERY per-connection goroutine must have
// exited before cleanupConnection runs (P1: the old teardown never joined the
// heartbeat sender or the delivery loop, racing UnregisterConsumer and its
// requeue).
func TestWSB_Teardown_JoinsAllGoroutinesBeforeCleanup(t *testing.T) {
	liveCh := make(chan int32, 32)
	_, addr := startWSBServer(t, nil, func(s *Server) {
		s.onConnTornDown = func(rt *connRuntime) {
			liveCh <- rt.liveGoroutines.Load()
		}
	})

	for i := 0; i < 20; i++ {
		r := dialRaw(t, addr)
		r.handshake("guest", "guest", 0, 0)
		r.openChannel(1)
		r.declareQueue(1, "td-q")
		r.consume(1, "td-q", "c1")
		// Abrupt close: no close handshake, the reader hits EOF with the
		// delivery loop and forwarders still live.
		require.NoError(t, r.c.Close())

		select {
		case v := <-liveCh:
			require.Equal(t, int32(0), v, "all per-connection goroutines must be joined before cleanup (iter %d)", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("onConnTornDown not called within 2s (iter %d)", i)
		}
	}
}

// TestWSB_Teardown_HeartbeatGoroutineExitsOnDone: with heartbeats enabled, the
// heartbeat sender must exit on conn.Done at teardown — it must not linger
// until its next tick (the old sender selected only on its own done channel,
// which only it could close).
func TestWSB_Teardown_HeartbeatGoroutineExitsOnDone(t *testing.T) {
	liveCh := make(chan int32, 1)
	_, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.HeartbeatIntervalMS = 2000
	}, func(s *Server) {
		s.onConnTornDown = func(rt *connRuntime) {
			liveCh <- rt.liveGoroutines.Load()
		}
	})

	conn, err := amqp.DialConfig("amqp://guest:guest@"+addr+"/", amqp.Config{Heartbeat: 2 * time.Second})
	require.NoError(t, err)

	start := time.Now()
	require.NoError(t, conn.Close())

	select {
	case v := <-liveCh:
		require.Equal(t, int32(0), v)
		require.Less(t, time.Since(start), 1*time.Second,
			"teardown must not wait for the heartbeat tick (1s at hb=2s)")
	case <-time.After(2 * time.Second):
		t.Fatal("teardown did not complete within 2s")
	}
}
