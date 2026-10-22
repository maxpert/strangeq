package server

import "time"

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
