package server

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConnections_ReportsRealUsername(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Lifecycle = NewLifecycleManager(srv, srv.Config)

	conn, _ := newPipeConnWithFrames(t)
	conn.Username = "alice"
	conn.Vhost = "/prod"

	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	conns := srv.Lifecycle.GetConnections()
	found := false
	for _, c := range conns {
		if c.ID == conn.ID {
			assert.Equal(t, "alice", c.Username, "GetConnections must report real username")
			assert.Equal(t, "/prod", c.VirtualHost)
			found = true
		}
	}
	assert.True(t, found, "connection should be in GetConnections output")
}

func TestGetConnections_UsesConnectionTimestamps(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Lifecycle = NewLifecycleManager(srv, srv.Config)

	conn, _ := newPipeConnWithFrames(t)
	conn.Username = "bob"
	before := time.Now()
	conn.ConnectedAt = before
	conn.TouchActivity()

	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	conns := srv.Lifecycle.GetConnections()
	found := false
	for _, c := range conns {
		if c.ID == conn.ID {
			assert.Equal(t, before, c.ConnectedAt, "GetConnections must use conn.ConnectedAt")
			assert.True(t, c.LastActivity.Sub(before) >= 0, "GetConnections must use conn.GetLastActivity()")
			found = true
		}
	}
	assert.True(t, found)
}

func TestGetStats_ReportsCounters(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Lifecycle = NewLifecycleManager(srv, srv.Config)

	srv.messagesPublished.Add(42)
	srv.messagesDelivered.Add(10)
	srv.bytesReceived.Add(1024)
	srv.bytesSent.Add(2048)

	stats := srv.Lifecycle.GetStats()
	assert.Equal(t, int64(42), stats.MessagesPublished)
	assert.Equal(t, int64(10), stats.MessagesDelivered)
	assert.Equal(t, int64(1024), stats.BytesReceived)
	assert.Equal(t, int64(2048), stats.BytesSent)
}

func TestLifecycleManager_NoDoubleMetricsCollection(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Network.Address = "127.0.0.1:0"

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err)

	lm := NewLifecycleManager(srv, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = lm.Start(ctx)
	require.NoError(t, err)

	srv.Mutex.RLock()
	hasCancel := srv.metricsCancel != nil
	srv.Mutex.RUnlock()
	assert.True(t, hasCancel, "Server.Start should have set metricsCancel (one collector)")

	cancel()
	time.Sleep(200 * time.Millisecond)

	_ = lm.GetState()
}

func TestIsListenerClosedErr(t *testing.T) {
	assert.True(t, isListenerClosedErr(nil) == false)
	assert.False(t, isListenerClosedErr(nil))
}

func TestConnectionMetadataFields(t *testing.T) {
	conn := protocol.NewConnection(nil)
	assert.False(t, conn.ConnectedAt.IsZero())
	assert.True(t, conn.GetLastActivity().UnixNano() > 0)
}
