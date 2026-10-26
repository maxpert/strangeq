package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

var errClientRequestedClose = errors.New("connection closed by client")

// errCloseOKReceived signals that the client answered our connection.close
// with connection.close-ok: the close handshake completed and the connection
// can be torn down.
var errCloseOKReceived = errors.New("connection.close-ok received")

// parseConnectionTuneOK parses the connection.tune-ok payload (after the
// 4-byte class/method ID) and extracts the negotiated parameters.
// The payload layout is: ChannelMax (uint16), FrameMax (uint32), Heartbeat (uint16).
func parseConnectionTuneOK(payload []byte) (channelMax uint16, frameMax uint32, heartbeat uint16, err error) {
	if len(payload) < 8 {
		return 0, 0, 0, fmt.Errorf("tune-ok payload too short: %d bytes", len(payload))
	}
	channelMax = binary.BigEndian.Uint16(payload[0:2])
	frameMax = binary.BigEndian.Uint32(payload[2:6])
	heartbeat = binary.BigEndian.Uint16(payload[6:8])
	return channelMax, frameMax, heartbeat, nil
}

// sendConnectionStart sends the connection.start method frame
func (s *Server) sendConnectionStart(conn *protocol.Connection) error {
	// Create server properties
	serverProperties := map[string]interface{}{
		"product":   AMQPProduct,
		"version":   AMQPVersion,
		"platform":  AMQPPlatform,
		"copyright": AMQPCopyright,
		"capabilities": map[string]interface{}{
			"publisher_confirms":           true,
			"exchange_exchange_bindings":   true,
			"basic.nack":                   true,
			"consumer_cancel_notify":       true,
			"connection.blocked":           true,
			"authentication_failure_close": true,
		},
	}

	// Determine available mechanisms
	mechanisms := "PLAIN" // Default
	if s.MechanismRegistry != nil {
		mechanisms = s.MechanismRegistry.String()
	} else if s.Config != nil && len(s.Config.Security.AuthMechanisms) > 0 {
		// Use configured mechanisms
		for i, mech := range s.Config.Security.AuthMechanisms {
			if i > 0 {
				mechanisms += " "
			}
			mechanisms += mech
		}
	}

	// Create the connection.start method
	startMethod := &protocol.ConnectionStartMethod{
		VersionMajor:     0,
		VersionMinor:     9,
		ServerProperties: serverProperties,
		Mechanisms:       []string{mechanisms},
		Locales:          []string{"en_US"},
	}

	methodData, err := startMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.start: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 10, methodData) // 10.10 = connection.start

	return s.writeFrame(conn, frame)
}

// sendConnectionTune sends the connection.tune method frame
func (s *Server) sendConnectionTune(conn *protocol.Connection) error {
	channelMax := uint16(s.Config.Server.MaxChannelsPerConnection)
	frameMax := uint32(s.Config.Server.MaxFrameSize)
	heartbeat := s.serverHeartbeatSeconds()

	tuneMethod := &protocol.ConnectionTuneMethod{
		ChannelMax: channelMax,
		FrameMax:   frameMax,
		Heartbeat:  heartbeat,
	}

	methodData, err := tuneMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.tune: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 30, methodData) // 10.30 = connection.tune

	return s.writeFrame(conn, frame)
}

// sendConnectionOpenOK sends the connection.open-ok method frame
func (s *Server) sendConnectionOpenOK(conn *protocol.Connection) error {
	openOKMethod := &protocol.ConnectionOpenOKMethod{
		Reserved1: "", // Currently unused
	}

	methodData, err := openOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.open-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 41, methodData) // 10.41 = connection.open-ok

	return s.writeFrame(conn, frame)
}

// processConnectionOpen handles the connection.open method. The caller (the
// handshake state machine) has already verified this is 10.40; payload is the
// method body after the 4-byte class/method prefix. Every failure sends
// connection.close via connectionError (the caller then waits for close-ok).
func (s *Server) processConnectionOpen(conn *protocol.Connection, payload []byte) error {
	// Deserialize the connection.open method
	openMethod := &protocol.ConnectionOpenMethod{}
	err := openMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize connection.open", zap.Error(err))
		return s.connectionError(conn, amqperrors.SyntaxError, "SYNTAX_ERROR - malformed connection.open", 10, 40)
	}

	// Defence in depth (§1.3): with authentication enabled, connection.open on
	// a connection whose start-ok produced no identity is refused. Unreachable
	// once handleConnectionStartOK enforces the same invariant.
	if s.Config != nil && s.Config.Security.AuthenticationEnabled && conn.Username == "" {
		s.Log.Warn("connection.open without an authenticated identity",
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.AccessRefused, "ACCESS_REFUSED - not authenticated", 10, 40)
	}

	// Normalize empty vhost to "/"
	vhost := openMethod.VirtualHost
	if vhost == "" {
		vhost = "/"
	}

	// AMQP 0.9.1 spec, connection.open "security" rule (SHOULD):
	//   "The server SHOULD verify that the client has permission to access
	//    the specified virtual host."
	// When authorization is enabled, fail-closed if there is no valid user.
	if s.Config != nil && s.Config.Security.AuthorizationEnabled {
		if conn.User == nil {
			s.Log.Warn("Authorization enabled but no authenticated user",
				zap.String("connection_id", conn.ID))
			return s.connectionError(conn, amqperrors.AccessRefused,
				"authorization enabled but no authenticated user", 10, 40)
		}
		user, ok := conn.User.(*interfaces.User)
		if !ok {
			s.Log.Warn("Unsupported user type for authorization",
				zap.String("connection_id", conn.ID))
			return s.connectionError(conn, amqperrors.AccessRefused,
				"unsupported user type for authorization", 10, 40)
		}

		// Hold RLock for both vhost permission and loopback checks to prevent
		// concurrent RefreshUser from mutating fields mid-check.
		user.RLock()
		hasVHostPermission := false
		for _, vhp := range user.VHostPermissions {
			if vhp.VHost == vhost {
				hasVHostPermission = true
				break
			}
		}
		loopbackOnly := user.LoopbackOnly
		user.RUnlock()

		if !hasVHostPermission {
			s.Log.Warn("Vhost access refused",
				zap.String("connection_id", conn.ID),
				zap.String("username", user.Username),
				zap.String("vhost", vhost))
			return s.connectionError(conn, amqperrors.InvalidPath,
				fmt.Sprintf("access to vhost %s refused for user %s", vhost, user.Username), 10, 40)
		}

		// RabbitMQ loopback restriction: users with LoopbackOnly=true may only
		// connect from localhost (loopback interface).
		if loopbackOnly && !isLoopbackConn(conn.Conn) {
			s.Log.Warn("Loopback restriction violated",
				zap.String("connection_id", conn.ID),
				zap.String("username", user.Username))
			return s.connectionError(conn, amqperrors.AccessRefused,
				fmt.Sprintf("user %s can only connect via localhost", user.Username), 10, 40)
		}
	} else if conn.User != nil {
		// Authorization disabled, but still enforce loopback restriction
		// for users that have it (e.g. guest via ANONYMOUS mechanism).
		if user, ok := conn.User.(*interfaces.User); ok {
			user.RLock()
			loopbackOnly := user.LoopbackOnly
			user.RUnlock()
			if loopbackOnly && !isLoopbackConn(conn.Conn) {
				s.Log.Warn("Loopback restriction violated",
					zap.String("connection_id", conn.ID),
					zap.String("username", user.Username))
				return s.connectionError(conn, amqperrors.AccessRefused,
					fmt.Sprintf("user %s can only connect via localhost", user.Username), 10, 40)
			}
		}
	}

	// Set the connection's vhost
	conn.Vhost = vhost

	s.Log.Info("Connection opened",
		zap.String("connection_id", conn.ID),
		zap.String("vhost", conn.Vhost))

	// Send connection.open-ok
	return s.sendConnectionOpenOK(conn)
}

// isLoopbackConn checks if the remote address of a connection is a loopback address.
// Returns false for non-TCP connections (conservative: deny by default).
func isLoopbackConn(conn net.Conn) bool {
	if conn == nil {
		return false
	}
	addr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return false
	}
	return addr.IP.IsLoopback()
}

// handleConnectionStartOK handles the connection.start-ok method and performs
// authentication. Every failure branch returns a non-nil *ConnectionError (the
// close frame is already on the wire), so no path can fall through to
// connection.tune unauthenticated (P0-1 bypass #2). On success, with
// authentication enabled, conn.Username is non-empty.
func (s *Server) handleConnectionStartOK(conn *protocol.Connection, payload []byte) error {
	// Parse connection.start-ok
	startOK, err := protocol.ParseConnectionStartOK(payload)
	if err != nil {
		s.Log.Error("Failed to parse connection.start-ok",
			zap.String("connection_id", conn.ID),
			zap.Error(err))
		return s.connectionError(conn, amqperrors.CommandInvalid,
			"COMMAND_INVALID - failed to parse connection.start-ok", 10, 11)
	}

	// Capture the client-advertised properties/capabilities once, before any
	// auth branch returns. SQ-12 gates connection.blocked emission on the
	// client's connection.blocked capability (conn.ClientSupportsBlocked()).
	conn.ClientProperties = startOK.ClientProperties

	// WS-E: host policy and failed-auth rate limiting run before SASL and
	// before the authentication-disabled early return, so host lists apply
	// even when authentication is off (they are network policy, not identity).
	if err := s.admitPeer(conn); err != nil {
		return err
	}

	s.Log.Debug("Connection.start-ok parsed",
		zap.String("connection_id", conn.ID),
		zap.String("mechanism", startOK.Mechanism),
		zap.String("locale", startOK.Locale))

	// Check if authentication is enabled
	if s.Config != nil && !s.Config.Security.AuthenticationEnabled {
		// Authentication disabled, allow connection as guest
		conn.Username = "guest"
		s.Log.Info("Authentication disabled, allowing connection as guest",
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Fail-closed: if auth is enabled but no authenticator or mechanism registry,
	// do NOT fall through to guest — refuse the connection.
	if s.Authenticator == nil || s.MechanismRegistry == nil {
		s.Log.Error("Authentication enabled but authenticator or mechanism registry is nil",
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.InternalError,
			"INTERNAL_ERROR - authentication enabled but no authenticator configured", 10, 11)
	}

	// Get the mechanism
	mechanism, err := s.MechanismRegistry.Get(startOK.Mechanism)
	if err != nil {
		s.Log.Warn("Unsupported authentication mechanism",
			zap.String("connection_id", conn.ID),
			zap.String("mechanism", startOK.Mechanism),
			zap.Error(err))
		// WS-E: rate-limit accounting on every refused attempt; WS-B: the
		// close itself goes out through the typed ConnectionError path so
		// exactly one connection.close is written (S4).
		s.recordAuthFailure(conn, "unsupported_mechanism")
		return s.connectionError(conn, amqperrors.AccessRefused,
			fmt.Sprintf("ACCESS_REFUSED - unsupported mechanism %s", startOK.Mechanism), 10, 11)
	}

	// Authenticate using the mechanism
	userInterface, err := mechanism.Authenticate(startOK.Response, s.Authenticator)
	if err != nil {
		s.Log.Warn("Authentication failed",
			zap.String("connection_id", conn.ID),
			zap.String("mechanism", startOK.Mechanism),
			zap.String("remote_addr", remoteAddrString(conn)),
			zap.Error(err))
		// WS-E: rate-limit accounting on every refused attempt. The client
		// only ever sees the fixed text; the cause stays in the log. WS-B:
		// exactly one connection.close via the typed ConnectionError (S4).
		s.recordAuthFailure(conn, "bad_credentials")
		cne := s.connectionError(conn, amqperrors.AccessRefused,
			"ACCESS_REFUSED - authentication failed", 10, 11)
		cne.Cause = fmt.Errorf("authentication failed: %w", err)
		return cne
	}

	// Extract username from authenticated user
	if user, ok := userInterface.(*interfaces.User); ok {
		if err := s.checkUserAccess(conn, user.Username); err != nil {
			return err
		}
		conn.Username = user.Username
		conn.User = user
		s.Log.Info("User authenticated successfully",
			zap.String("connection_id", conn.ID),
			zap.String("username", user.Username),
			zap.String("mechanism", startOK.Mechanism))
	} else {
		conn.Username = "authenticated"
		s.Log.Info("Authentication successful",
			zap.String("connection_id", conn.ID),
			zap.String("mechanism", startOK.Mechanism))
	}

	// With authentication enabled, a successful start-ok MUST have produced a
	// non-empty identity; otherwise the connection would proceed with no
	// principal (P0-1 bypass #1's remaining hole).
	if s.Config != nil && s.Config.Security.AuthenticationEnabled && conn.Username == "" {
		s.Log.Error("authentication produced no identity",
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.AccessRefused,
			"ACCESS_REFUSED - authentication produced no identity", 10, 11)
	}

	return nil
}

// processConnectionMethod processes connection-level methods received AFTER
// the handshake completed (the handshake state machine reads its own frames;
// see processConnectionFrames). Anything handshake-scoped here is a hard
// command-invalid (503), and start-ok is never re-accepted: a mid-connection
// identity change is not permitted (P0-1).
func (s *Server) processConnectionMethod(conn *protocol.Connection, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ConnectionStartOK: // Method ID 11 for connection class
		s.Log.Warn("connection.start-ok after handshake",
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.CommandInvalid,
			"COMMAND_INVALID - connection.start-ok after handshake", 10, 11)
	case protocol.ConnectionTuneOK: // Method ID 31 for connection class
		s.Log.Warn("connection.tune-ok after handshake",
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.CommandInvalid,
			"COMMAND_INVALID - connection.tune-ok after handshake", 10, 31)
	case protocol.ConnectionSecureOK: // Method ID 21 — multi-step SASL not supported
		s.Log.Warn("Received unexpected connection.secure-ok (multi-step SASL not supported)",
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.CommandInvalid,
			"COMMAND_INVALID - multi-step SASL not supported", 10, 21)
	case protocol.ConnectionOpen, protocol.ConnectionOpenOK: // 40 / 41
		s.Log.Warn("connection.open/open-ok after handshake",
			zap.Uint16("method_id", methodID),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.CommandInvalid,
			fmt.Sprintf("COMMAND_INVALID - unexpected connection method 10.%d after handshake", methodID), 10, methodID)
	case protocol.ConnectionClose: // Method ID 50 for connection class
		// Flush confirms BEFORE close-ok: after connection.close/close-ok
		// nothing else may be written, and writing basic.acks after close-ok
		// violates the protocol.
		rt := s.runtimeFor(conn)
		if rt.handshakeDone.Load() {
			_ = s.drainConfirmNacks(conn)
			_ = s.flushAllChannelConfirms(conn)
		}
		rt.closeHandshakeStarted.Store(true)
		s.Log.Info("Received connection.close", zap.String("connection_id", conn.ID))
		if err := s.sendConnectionCloseOK(conn); err != nil {
			s.Log.Error("Failed to send connection.close-ok", zap.Error(err))
		}
		conn.Closed.Store(true)
		return errClientRequestedClose
	case protocol.ConnectionCloseOK: // Method ID 51 — client answers our close
		rt := s.runtimeFor(conn)
		if rt.connClosing.Load() {
			return errCloseOKReceived
		}
		return s.connectionError(conn, amqperrors.CommandInvalid,
			"COMMAND_INVALID - unexpected connection.close-ok", 10, 51)
	default:
		s.Log.Warn("Unknown connection method ID",
			zap.Uint16("method_id", methodID),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown connection method 10.%d", methodID), 10, methodID)
	}
}

// sendConnectionClose sends the connection.close method frame
func (s *Server) sendConnectionClose(conn *protocol.Connection, replyCode uint16, replyText string, classID, methodID uint16) error {
	closeMethod := &protocol.ConnectionCloseMethod{
		ReplyCode: replyCode,
		ReplyText: replyText,
		ClassID:   classID,
		MethodID:  methodID,
	}

	methodData, err := closeMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.close: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 50, methodData) // 10.50 = connection.close

	if err := s.writeFrame(conn, frame); err != nil {
		return err
	}

	// Mark connection as closed
	conn.Closed.Store(true)

	return nil
}
