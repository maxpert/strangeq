package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestWSB_HeartbeatZero_ConnectionSurvives: a connection negotiated to
// heartbeat=0 must NOT be torn down right after the handshake (P1: the
// heartbeat sender used to close its done channel immediately when hb == 0,
// and the teardown select treated that as a trigger).
func TestWSB_HeartbeatZero_ConnectionSurvives(t *testing.T) {
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.HeartbeatIntervalMS = 0
	}, nil)

	conn, err := amqp.DialConfig("amqp://guest:guest@"+addr+"/", amqp.Config{Heartbeat: 0})
	require.NoError(t, err)
	defer conn.Close()

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("hb-q", false, true, false, false, nil)
	require.NoError(t, err)

	for i := 0; i < 11; i++ {
		body := fmt.Sprintf("hb-%d", i)
		require.NoError(t, ch.Publish("", "hb-q", false, false, amqp.Publishing{Body: []byte(body)}))
		msg, ok, err := ch.Get("hb-q", true)
		require.NoError(t, err)
		require.True(t, ok, "get %d", i)
		require.Equal(t, body, string(msg.Body))
		time.Sleep(200 * time.Millisecond)
	}

	assert.False(t, conn.IsClosed(), "hb=0 connection must survive")
	select {
	case e := <-closeCh:
		t.Fatalf("hb=0 connection closed unexpectedly: %v", e)
	default:
	}
	assert.Equal(t, 1, wsbConnCount(srv))
}

// TestWSB_HeartbeatSubSecond_RoundsUp: HeartbeatIntervalMS → whole seconds,
// rounding UP so a positive sub-second setting never becomes 0 (disabled).
func TestWSB_HeartbeatSubSecond_RoundsUp(t *testing.T) {
	srv := &Server{Log: zap.NewNop(), Config: config.DefaultConfig()}
	cases := []struct {
		ms   int64
		want uint16
	}{
		{500, 1},
		{1500, 2},
		{0, 0},
		{-1, 0},
		{1e12, 65535},
		{1000, 1},
		{60000, 60},
	}
	for _, c := range cases {
		srv.Config.Network.HeartbeatIntervalMS = c.ms
		assert.Equal(t, c.want, srv.serverHeartbeatSeconds(), "ms=%d", c.ms)
	}

	// The tune frame carries the rounded-up value.
	srv.Config.Network.HeartbeatIntervalMS = 500
	srv.MetricsCollector = &NoOpMetricsCollector{}
	conn, frameCh := newPipeConnWithFrames(t)
	require.NoError(t, srv.sendConnectionTune(conn))
	_, _, payload := nextMethodFrame(t, frameCh)
	tune := &protocol.ConnectionTuneMethod{}
	require.NoError(t, tune.Deserialize(payload))
	assert.Equal(t, uint16(1), tune.Heartbeat)
}

// TestWSB_HeartbeatClientZeroHonored: AMQP 0-9-1 tune-ok heartbeat is "the
// delay the client wants; zero means the client does not want a heartbeat" —
// the server must honor a client's 0 instead of forcing its own value.
func TestWSB_HeartbeatClientZeroHonored(t *testing.T) {
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.HeartbeatIntervalMS = 60000 // server proposes 60s
	}, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0) // client asks for NO heartbeats

	require.Eventually(t, func() bool {
		c := wsbServerConn(srv)
		return c != nil && c.HeartbeatSec.Load() == 0
	}, 2*time.Second, 10*time.Millisecond, "negotiated heartbeat must be the client's 0")

	// No heartbeat frame may arrive (heartbeats disabled).
	r.expectSilence(300 * time.Millisecond)
}

// TestWSB_HeartbeatNegotiation_ClientValueWins covers the §4.4 rule directly:
// negotiatedHeartbeat := clientHeartbeat.
func TestWSB_HeartbeatNegotiation_ClientValueWins(t *testing.T) {
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.HeartbeatIntervalMS = 60000
	}, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 7) // client wants 7s

	require.Eventually(t, func() bool {
		c := wsbServerConn(srv)
		return c != nil && c.HeartbeatSec.Load() == 7
	}, 2*time.Second, 10*time.Millisecond)
}
