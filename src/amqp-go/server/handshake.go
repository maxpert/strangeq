package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

const (
	// handshakeTimeout is the single absolute deadline for the whole AMQP
	// handshake, measured from the end of the protocol header (§1.2).
	handshakeTimeout = 30 * time.Second
	// connectionCloseOKTimeout bounds how long the server waits for a client's
	// connection.close-ok (or close) after sending connection.close before
	// force-closing the socket (§0.2 D5). Closing sooner, while client bytes
	// are still unread in our receive buffer, makes the kernel send RST and
	// the client sees a bare reset instead of the close reason.
	connectionCloseOKTimeout = 3 * time.Second
	// protocolHeaderTimeout bounds the read of the 8-byte AMQP protocol header
	// plus the TLS handshake it drives on a *tls.Conn (TLS 1.2 is 2 RTT, so
	// even a 1s-RTT link finishes well under 5s). No legitimate client sends
	// its header late (§2.2).
	protocolHeaderTimeout = 5 * time.Second
	// minPendingHandshakes is the floor for the pending-handshake limit (§2.2).
	minPendingHandshakes = 64
)

// amqpProtocolHeader is the only protocol header this server accepts, and the
// reply required by AMQP 0-9-1 §4.2.2 when a peer offers anything else.
var amqpProtocolHeader = [8]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

func (s *Server) headerTimeout() time.Duration {
	if s.headerTimeoutOverride > 0 {
		return s.headerTimeoutOverride
	}
	return protocolHeaderTimeout
}

func (s *Server) handshakeTimeout() time.Duration {
	if s.handshakeTimeoutOverride > 0 {
		return s.handshakeTimeoutOverride
	}
	return handshakeTimeout
}

func (s *Server) closeOKTimeout() time.Duration {
	if s.closeOKTimeoutOverride > 0 {
		return s.closeOKTimeoutOverride
	}
	return connectionCloseOKTimeout
}

// errHandshakeIO signals a read error or deadline during the handshake: the
// connection is closed silently (no close frame — the peer may be gone).
var errHandshakeIO = errors.New("handshake I/O failure")

// handshakeCap bounds a single handshake frame (same value the pre-WS-B code
// used for every handshake read).
const handshakeCap = uint32(131072 + 8)

// readHandshakeMethod reads the next frame for the expected connection-class
// method. It returns the method payload (after the 4-byte class/method
// prefix), or an error. Every error return has ALREADY handled the wire (sent
// connection.close where the rules say so); the caller just returns (running
// awaitHandshakeCloseOK when a close was sent).
//
// Rules, in order (§1.2): read error/deadline → silent close; channel-0
// heartbeat → ignore, keep reading (same absolute deadline); non-method frame
// → 505; method on channel ≠ 0 → 505; payload < 4 → 501; client 10.50 →
// close-ok + errClientRequestedClose; 10.<wantMethod> → payload; anything else
// → 503 naming the expected step.
func (s *Server) readHandshakeMethod(conn *protocol.Connection, wantMethod uint16, step string) ([]byte, error) {
	for {
		frame, err := protocol.ReadFrameOptimizedWithLimit(conn.Reader, handshakeCap)
		if err != nil {
			s.Log.Debug("handshake read failed",
				zap.String("connection_id", conn.ID),
				zap.String("step", step),
				zap.Error(err))
			return nil, errHandshakeIO
		}
		conn.TouchActivity()

		if frame.Type == protocol.FrameHeartbeat && frame.Channel == 0 {
			protocol.PutFrame(frame)
			continue
		}
		if frame.Type != protocol.FrameMethod {
			protocol.PutFrame(frame)
			return nil, s.connectionError(conn, amqperrors.UnexpectedFrame,
				"UNEXPECTED_FRAME - expected connection."+step, 0, 0)
		}
		if frame.Channel != 0 {
			var cls, mth uint16
			if len(frame.Payload) >= 4 {
				cls = (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
				mth = (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
			}
			protocol.PutFrame(frame)
			return nil, s.connectionError(conn, amqperrors.UnexpectedFrame,
				fmt.Sprintf("UNEXPECTED_FRAME - method %d.%d on channel %d during handshake", cls, mth, frame.Channel),
				cls, mth)
		}
		if len(frame.Payload) < 4 {
			protocol.PutFrame(frame)
			return nil, s.connectionError(conn, amqperrors.FrameError,
				"FRAME_ERROR - method frame payload too short", 0, 0)
		}
		classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
		methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
		payload := frame.Payload[4:]

		if classID == 10 && methodID == protocol.ConnectionClose {
			protocol.PutFrame(frame)
			rt := s.runtimeFor(conn)
			rt.closeHandshakeStarted.Store(true)
			if err := s.sendConnectionCloseOK(conn); err != nil {
				s.Log.Debug("failed to send connection.close-ok during handshake", zap.Error(err))
			}
			return nil, errClientRequestedClose
		}
		if classID == 10 && methodID == wantMethod {
			// The payload escapes the pooled frame: copy it before PutFrame.
			out := make([]byte, len(payload))
			copy(out, payload)
			protocol.PutFrame(frame)
			return out, nil
		}
		protocol.PutFrame(frame)
		return nil, s.connectionError(conn, amqperrors.CommandInvalid,
			fmt.Sprintf("COMMAND_INVALID - expected connection.%s, got %d.%d", step, classID, methodID),
			classID, methodID)
	}
}

// awaitHandshakeCloseOK waits for the client's answer to a handshake-time
// connection.close: it reads and discards frames until it gets 10.51 (or 10.50,
// answered with close-ok), hits an error, or the close-ok deadline passes. The
// caller's deferred conn.Conn.Close() closes the socket afterwards.
func (s *Server) awaitHandshakeCloseOK(conn *protocol.Connection) {
	_ = conn.Conn.SetReadDeadline(time.Now().Add(s.closeOKTimeout()))
	for {
		frame, err := protocol.ReadFrameOptimizedWithLimit(conn.Reader, handshakeCap)
		if err != nil {
			return
		}
		cls, mth, isMethod := methodIDs(frame)
		switch {
		case isMethod && frame.Channel == 0 && cls == 10 && mth == protocol.ConnectionCloseOK:
			protocol.PutFrame(frame)
			return
		case isMethod && frame.Channel == 0 && cls == 10 && mth == protocol.ConnectionClose:
			protocol.PutFrame(frame)
			if err := s.sendConnectionCloseOK(conn); err != nil {
				return
			}
		default:
			protocol.PutFrame(frame)
		}
	}
}

// --- Pre-auth limits (§2) ---

// dispatchAccepted gates a freshly accepted socket: refused outright while
// shutting down or once the pending-handshake budget is exhausted; otherwise a
// handshake slot is taken and handleConnection runs on its own goroutine.
// connWG.Add runs under RLock: Shutdown sets ShuttingDown under Lock before
// connWG.Wait (§11), so no Add races a Wait.
func (s *Server) dispatchAccepted(nc net.Conn) {
	s.Mutex.RLock()
	if s.ShuttingDown {
		s.Mutex.RUnlock()
		_ = nc.Close()
		return
	}
	if !s.tryAcquireHandshakeSlot() {
		s.Mutex.RUnlock()
		_ = nc.Close()
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordConnectionError("handshake_backlog")
		}
		s.Log.Debug("pending-handshake limit reached; refusing socket",
			zap.String("remote", nc.RemoteAddr().String()))
		return
	}
	s.connWG.Add(1)
	s.Mutex.RUnlock()
	go s.handleConnection(nc)
}

func (s *Server) pendingHandshakeLimit() int64 {
	if s.pendingLimitOverride > 0 {
		return s.pendingLimitOverride
	}
	n := int64(s.Config.Network.MaxConnections / 4)
	if n < minPendingHandshakes {
		n = minPendingHandshakes
	}
	return n
}

func (s *Server) tryAcquireHandshakeSlot() bool {
	limit := s.pendingHandshakeLimit()
	for {
		n := s.pendingHandshakes.Load()
		if n >= limit {
			return false
		}
		if s.pendingHandshakes.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// readProtocolHeader reads and validates the 8-byte AMQP protocol header under
// one deadline that ALSO bounds the TLS handshake the read drives on a
// *tls.Conn (§2.4). On a version mismatch it replies with the supported header
// as AMQP 0-9-1 §4.2.2 requires, then closes.
func (s *Server) readProtocolHeader(nc net.Conn) bool {
	_ = nc.SetDeadline(time.Now().Add(s.headerTimeout())) // read AND write: bounds the TLS handshake both ways
	var hdr [8]byte
	if _, err := io.ReadFull(nc, hdr[:]); err != nil {
		s.Log.Debug("protocol header read failed", zap.Error(err))
		return false
	}
	if hdr != amqpProtocolHeader {
		_, _ = nc.Write(amqpProtocolHeader[:]) // AMQP 0-9-1 §4.2.2: reply with the supported header, then close
		s.Log.Warn("unsupported protocol header", zap.Binary("header", hdr[:]))
		return false
	}
	_ = nc.SetDeadline(time.Time{})
	return true
}
