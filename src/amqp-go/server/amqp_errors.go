package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// ChannelError is a soft (channel-scoped) AMQP exception. Returning one from a
// handler closes ONLY that channel; the connection keeps running. It lives in
// package server (not errors/): it is dispatcher control flow, and its
// unexported sent flag records server protocol state (whether the close frame
// is already on the wire), which no other package should see.
type ChannelError struct {
	Code              uint16
	Text              string // full reply-text, e.g. "NOT_FOUND - no queue 'q' in vhost '/'"
	ClassID, MethodID uint16
	Cause             error // optional, for logs only (never sent on the wire)
	sent              bool  // channel.close already written by the raising site
}

func (e *ChannelError) Error() string {
	return fmt.Sprintf("channel exception %d (%d.%d): %s", e.Code, e.ClassID, e.MethodID, e.Text)
}
func (e *ChannelError) Unwrap() error { return e.Cause }

// ConnectionError is a hard AMQP exception: connection.close is sent, then the
// connection is torn down after close-ok (or connectionCloseOKTimeout).
type ConnectionError struct {
	Code              uint16
	Text              string
	ClassID, MethodID uint16
	Cause             error
	sent              bool
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection exception %d (%d.%d): %s", e.Code, e.ClassID, e.MethodID, e.Text)
}
func (e *ConnectionError) Unwrap() error { return e.Cause }

// isSoftCode reports AMQP 0-9-1 channel-level reply codes (D3, matching
// RabbitMQ's soft/hard classification).
func isSoftCode(code uint16) bool {
	switch code {
	case 311, 312, 313, 403, 404, 405, 406:
		return true
	}
	return false
}

// errNoCurrentQueue is the sentinel behind an empty queue name with no
// previously declared queue on the channel (§3.6 resolveQueueName).
var errNoCurrentQueue = errors.New("no previously declared queue")

// isConnIOError is the internal "the socket is dead" detector. It only works
// if write errors are wrapped with %w at their raising sites.
func isConnIOError(err error) bool {
	var ne net.Error
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.As(err, &ne)
}

// channelError sends channel.close (idempotently), marks the channel closing,
// and returns a *ChannelError with sent=true. Handlers `return s.channelError(...)`.
func (s *Server) channelError(conn *protocol.Connection, channelID, code uint16, text string, classID, methodID uint16) *ChannelError {
	if channelID == 0 || !isSoftCode(code) {
		// A channel-level code on channel 0 is a connection error, and a hard
		// code never goes out on channel.close (the documented tx-commit 541
		// bypasses this helper entirely; see §3.6). Convert to the hard path:
		// failConnection sends connection.close; the returned error records
		// that the wire is already handled.
		s.connectionError(conn, code, text, classID, methodID)
		return &ChannelError{Code: code, Text: text, ClassID: classID, MethodID: methodID, sent: true}
	}
	rt := s.runtimeFor(conn)
	if rt.markChannelClosing(channelID) {
		s.sendChannelClose(conn, channelID, code, text, classID, methodID)
	}
	s.Log.Warn("channel error",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID),
		zap.Uint16("code", code),
		zap.String("text", text))
	return &ChannelError{Code: code, Text: text, ClassID: classID, MethodID: methodID, sent: true}
}

// connectionError starts the connection close handshake (idempotently) and
// returns a *ConnectionError with sent=true.
func (s *Server) connectionError(conn *protocol.Connection, code uint16, text string, classID, methodID uint16) *ConnectionError {
	rt := s.runtimeFor(conn)
	s.failConnection(rt, code, text, classID, methodID)
	return &ConnectionError{Code: code, Text: text, ClassID: classID, MethodID: methodID, sent: true}
}

// failConnection sends connection.close exactly once per connection (D5): it
// stops sending everything else, and force-closes the socket after
// closeOKTimeout if the client never replies close-ok.
func (s *Server) failConnection(rt *connRuntime, code uint16, text string, classID, methodID uint16) {
	if !rt.connClosing.CompareAndSwap(false, true) {
		return
	}
	rt.closeHandshakeStarted.Store(true)
	conn := rt.conn
	if rt.handshakeDone.Load() {
		// Durable confirms are real; deliver them before the close (best-effort).
		_ = s.drainConfirmNacks(conn)
		_ = s.flushAllChannelConfirms(conn)
	}
	s.Log.Warn("connection error",
		zap.String("connection_id", conn.ID),
		zap.Uint16("code", code),
		zap.String("text", text),
		zap.Uint16("class_id", classID),
		zap.Uint16("method_id", methodID))
	if err := s.sendConnectionClose(conn, code, text, classID, methodID); err != nil {
		conn.Closed.Store(true)
		_ = conn.Conn.Close()
		return
	}
	t := time.AfterFunc(s.closeOKTimeout(), func() {
		conn.Closed.Store(true)
		_ = conn.Conn.Close()
	})
	rt.closeTimer.Store(t)
}

// malformed is the standard reply for an unparseable method payload:
// connection.close 502 SYNTAX_ERROR.
func (s *Server) malformed(conn *protocol.Connection, err error, classID, methodID uint16) *ConnectionError {
	return s.connectionError(conn, amqperrors.SyntaxError,
		fmt.Sprintf("SYNTAX_ERROR - malformed %d.%d: %v", classID, methodID, err), classID, methodID)
}

// frameCtx captures where a frame was being processed when an error was
// raised, for error classification (class/method echo in close frames).
type frameCtx struct {
	channel, classID, methodID uint16
}

// methodIDs extracts class and method IDs from a method frame's payload.
func methodIDs(f *protocol.Frame) (classID, methodID uint16, isMethod bool) {
	if f.Type != protocol.FrameMethod || len(f.Payload) < 4 {
		return 0, 0, false
	}
	classID = (uint16(f.Payload[0]) << 8) | uint16(f.Payload[1])
	methodID = (uint16(f.Payload[2]) << 8) | uint16(f.Payload[3])
	return classID, methodID, true
}

// classify maps an arbitrary handler error to a *ChannelError or a
// *ConnectionError (§3.5). Every result is already sent on the wire (the
// raising helpers are idempotent), except a handler-constructed typed error
// whose sent flag is false.
func (s *Server) classify(rt *connRuntime, fc frameCtx, err error) error {
	var ce *ChannelError
	if errors.As(err, &ce) {
		if ce.ClassID == 0 {
			ce.ClassID = fc.classID
		}
		if ce.MethodID == 0 {
			ce.MethodID = fc.methodID
		}
		if !ce.sent {
			s.channelError(rt.conn, fc.channel, ce.Code, ce.Text, ce.ClassID, ce.MethodID)
			ce.sent = true
		}
		return ce
	}
	var cne *ConnectionError
	if errors.As(err, &cne) {
		if cne.ClassID == 0 {
			cne.ClassID = fc.classID
		}
		if cne.MethodID == 0 {
			cne.MethodID = fc.methodID
		}
		if !cne.sent {
			s.failConnection(rt, cne.Code, cne.Text, cne.ClassID, cne.MethodID)
			cne.sent = true
		}
		return cne
	}
	if errors.Is(err, errNoCurrentQueue) {
		// RabbitMQ expand_queue_name_shortcut raises not_found; deliberately
		// not the AMQP-XML "502", which is a hard code.
		return s.channelError(rt.conn, fc.channel, amqperrors.NotFound,
			"NOT_FOUND - no previously declared queue", fc.classID, fc.methodID)
	}
	if errors.Is(err, interfaces.ErrQueueNotFound) || errors.Is(err, interfaces.ErrExchangeNotFound) {
		return s.channelError(rt.conn, fc.channel, amqperrors.NotFound,
			"NOT_FOUND - "+err.Error(), fc.classID, fc.methodID)
	}
	if errors.Is(err, broker.ErrInvalidQueueArgument) || errors.Is(err, broker.ErrExchangeTypeMismatch) {
		return s.channelError(rt.conn, fc.channel, amqperrors.PreconditionFailed,
			"PRECONDITION_FAILED - "+err.Error(), fc.classID, fc.methodID)
	}
	if errors.Is(err, interfaces.ErrTransactionNotActive) || errors.Is(err, interfaces.ErrTransactionNotFound) {
		return s.channelError(rt.conn, fc.channel, amqperrors.PreconditionFailed,
			"PRECONDITION_FAILED - channel is not transactional", fc.classID, fc.methodID)
	}
	var ae *amqperrors.AMQPError
	if errors.As(err, &ae) {
		code := uint16(ae.Code)
		if isSoftCode(code) {
			return s.channelError(rt.conn, fc.channel, code, ae.Message, fc.classID, fc.methodID)
		}
		return s.connectionError(rt.conn, code, ae.Message, fc.classID, fc.methodID)
	}
	// TRANSITIONAL (§16.2, delete once WS-D's brokerErrorReply seam lands):
	// today's broker returns untyped string errors for missing queues and
	// exchanges; mapping them to 404 turns what would be a 541 connection
	// close into the channel close RabbitMQ sends.
	msg := err.Error()
	if brokerErrorIsNotFound(msg) {
		return s.channelError(rt.conn, fc.channel, amqperrors.NotFound,
			"NOT_FOUND - "+msg, fc.classID, fc.methodID)
	}
	// Default: internal_error is a hard (connection) error, matching RabbitMQ.
	// Strictly better than the previous bare EOF.
	return s.connectionError(rt.conn, amqperrors.InternalError,
		"INTERNAL_ERROR - "+msg, fc.classID, fc.methodID)
}

// brokerErrorIsNotFound is the transitional broker-error text match (§16.2).
// It is a standalone helper so the merge with WS-D can point classify at
// brokerErrorReply by changing this one function (and deleting its callers'
// reliance on text).
func brokerErrorIsNotFound(msg string) bool {
	return strings.Contains(msg, "not found") || strings.Contains(msg, "does not exist")
}

// handleFrameError is the central error path for processFrames and
// ackProcessor. It returns fatal=true when the calling loop must exit.
// onProcessor=true only when called from processFrames (safe to tear a channel
// down — see §3.4 for why the ack processor must not).
func (s *Server) handleFrameError(rt *connRuntime, fc frameCtx, err error, onProcessor bool) (fatal bool) {
	if errors.Is(err, errClientRequestedClose) || errors.Is(err, errCloseOKReceived) {
		rt.conn.Closed.Store(true)
		return true
	}
	if isConnIOError(err) {
		// Never try to send a close frame on a dead socket.
		rt.conn.Closed.Store(true)
		_ = rt.conn.Conn.Close()
		return true
	}
	classified := s.classify(rt, fc, err)
	switch e := classified.(type) {
	case *ChannelError:
		if onProcessor {
			// Release the channel's resources immediately (D4): consumers are
			// unregistered and unacked deliveries requeued. The ID stays in
			// rt.closingChannels until the client's close/close-ok arrives.
			s.teardownChannel(rt.conn, fc.channel)
		}
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordChannelError(strconv.Itoa(int(e.Code)))
		}
		return false
	case *ConnectionError:
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordConnectionError(strconv.Itoa(int(e.Code)))
		}
		// Keep looping so the client close-ok can be received; the closing
		// gate (§3.7) discards everything else and the close timer bounds it.
		return false
	}
	return false
}
