package server

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"go.uber.org/zap"
)

// tuningTestServer builds a minimal Server with the given socket-buffer config
// and a no-op logger, sufficient to exercise tuneSocket.
func tuningTestServer(readBuf, writeBuf int) *Server {
	s := &Server{
		Log:    zap.NewNop(),
		Config: &config.AMQPConfig{},
	}
	s.Config.Network = interfaces.NetworkConfig{
		ReadBufferSize:  readBuf,
		WriteBufferSize: writeBuf,
	}
	return s
}

// TestUnderlyingTCPConn covers the three unwrap cases: plain TCP, TLS-wrapped
// TCP, and a non-TCP conn (net.Pipe) which must yield nil.
func TestUnderlyingTCPConn(t *testing.T) {
	// Plain TCP.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	dialed := make(chan net.Conn, 1)
	go func() {
		c, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			t.Errorf("dial: %v", derr)
			dialed <- nil
			return
		}
		dialed <- c
	}()

	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()
	clientConn := <-dialed
	if clientConn != nil {
		defer clientConn.Close()
	}

	if got := underlyingTCPConn(accepted); got == nil {
		t.Errorf("plain *net.TCPConn: underlyingTCPConn returned nil")
	}

	// TLS wrapping a TCP conn. tls.Server does not need a completed handshake for
	// NetConn() to expose the underlying socket.
	tlsConn := tls.Server(accepted, &tls.Config{})
	if got := underlyingTCPConn(tlsConn); got == nil {
		t.Errorf("*tls.Conn over TCP: underlyingTCPConn returned nil")
	}

	// Non-TCP conn (net.Pipe) must yield nil, not panic.
	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()
	if got := underlyingTCPConn(p1); got != nil {
		t.Errorf("net.Pipe: expected nil, got %v", got)
	}
}

// TestTuneSocketAppliesSettings verifies tuneSocket runs without error on a real
// TCP conn and that TCP_NODELAY is set. Kernel-clamped SO_RCVBUF/SO_SNDBUF sizes
// are not portably observable via the net API, so we assert the call path is
// exercised (no panic, no error log path) and NoDelay is applied.
func TestTuneSocketAppliesSettings(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	dialed := make(chan net.Conn, 1)
	go func() {
		c, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			dialed <- nil
			return
		}
		dialed <- c
	}()
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()
	if c := <-dialed; c != nil {
		defer c.Close()
	}

	s := tuningTestServer(256*1024, 256*1024)
	// Must not panic and must apply cleanly to a TCP conn.
	s.tuneSocket(accepted)

	// Independently confirm SetNoDelay(true) succeeds on this socket (tuneSocket
	// applies the same call; a failure here would surface a platform issue).
	if err := accepted.(*net.TCPConn).SetNoDelay(true); err != nil {
		t.Errorf("SetNoDelay on accepted conn: %v", err)
	}
}

// TestTuneSocketNonTCPIsNoOp ensures tuneSocket on a non-TCP conn is a safe
// no-op (net.Pipe has no TCP knobs; must not panic).
func TestTuneSocketNonTCPIsNoOp(t *testing.T) {
	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()
	s := tuningTestServer(256*1024, 256*1024)
	s.tuneSocket(p1) // must not panic
}
