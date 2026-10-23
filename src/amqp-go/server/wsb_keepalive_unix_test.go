//go:build unix

package server

import (
	"net"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// keepaliveOpt reads SO_KEEPALIVE from a *net.TCPConn.
func keepaliveOpt(t *testing.T, c *net.TCPConn) int {
	t.Helper()
	rc, err := c.SyscallConn()
	require.NoError(t, err)
	var val int
	require.NoError(t, rc.Control(func(fd uintptr) {
		v, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE)
		require.NoError(t, err)
		val = v
	}))
	return val
}

// TestWSB_TuneSocket_KeepAlive: tuneSocket honours Network.TCPKeepAlive —
// explicitly, including the opt-out (Go's listener enables keepalive by
// default, so "unset" would silently be on).
func TestWSB_TuneSocket_KeepAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	newPair := func(t *testing.T) *net.TCPConn {
		dialed := make(chan net.Conn, 1)
		go func() {
			c, derr := net.Dial("tcp", ln.Addr().String())
			if derr == nil {
				dialed <- c
			}
		}()
		srvSide, err := ln.Accept()
		require.NoError(t, err)
		client := <-dialed
		t.Cleanup(func() { _ = client.Close() })
		t.Cleanup(func() { _ = srvSide.Close() })
		return srvSide.(*net.TCPConn)
	}

	t.Run("enabled", func(t *testing.T) {
		s := &Server{Log: zap.NewNop(), Config: &config.AMQPConfig{}}
		s.Config.Network = interfaces.NetworkConfig{TCPKeepAlive: true, TCPKeepAliveIntervalMS: 30000}
		c := newPair(t)
		s.tuneSocket(c)
		assert.NotEqual(t, 0, keepaliveOpt(t, c), "SO_KEEPALIVE must be on (nonzero)")
	})

	t.Run("disabled", func(t *testing.T) {
		s := &Server{Log: zap.NewNop(), Config: &config.AMQPConfig{}}
		s.Config.Network = interfaces.NetworkConfig{TCPKeepAlive: false}
		c := newPair(t)
		s.tuneSocket(c)
		assert.Equal(t, 0, keepaliveOpt(t, c), "SO_KEEPALIVE must be off on explicit opt-out")
	})
}
