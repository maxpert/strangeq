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
			"consumer_cancel_notify":       false,
			"connection.blocked":           false,
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

	return protocol.WriteFrameToConnection(conn, frame)
}

// sendConnectionTune sends the connection.tune method frame
func (s *Server) sendConnectionTune(conn *protocol.Connection) error {
	channelMax := uint16(s.Config.Server.MaxChannelsPerConnection)
	frameMax := uint32(s.Config.Server.MaxFrameSize)
	heartbeat := uint16(s.Config.Network.HeartbeatIntervalMS / 1000)

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

	return protocol.WriteFrameToConnection(conn, frame)
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

	return protocol.WriteFrameToConnection(conn, frame)
}

// sendConnectionOpen sends the connection.open method frame
func (s *Server) sendConnectionOpen(conn *protocol.Connection) error {
	// Create the connection.open method
	openMethod := &protocol.ConnectionOpenMethod{
		VirtualHost: "/",
		Reserved1:   "",
		Reserved2:   0,
	}

	methodData, err := openMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.open: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 40, methodData) // 10.40 = connection.open

	return protocol.WriteFrameToConnection(conn, frame)
}

// processConnectionOpen handles the connection.open method
func (s *Server) processConnectionOpen(conn *protocol.Connection, frame *protocol.Frame) error {
	if len(frame.Payload) < 4 {
		return fmt.Errorf("method frame payload too short")
	}

	classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])

	// Verify this is the connection.open method (class 10, method 40)
	if classID != 10 || methodID != 40 {
		return fmt.Errorf("expected connection.open (10.40) but got %d.%d", classID, methodID)
	}

	// Deserialize the connection.open method
	openMethod := &protocol.ConnectionOpenMethod{}
	err := openMethod.Deserialize(frame.Payload[4:])
	if err != nil {
		s.Log.Error("Failed to deserialize connection.open", zap.Error(err))
		return err
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
			s.sendConnectionClose(conn, amqperrors.AccessRefused,
				"authorization enabled but no authenticated user", 10, 40)
			return fmt.Errorf("authorization enabled but no authenticated user")
		}
		user, ok := conn.User.(*interfaces.User)
		if !ok {
			s.Log.Warn("Unsupported user type for authorization",
				zap.String("connection_id", conn.ID))
			s.sendConnectionClose(conn, amqperrors.AccessRefused,
				"unsupported user type for authorization", 10, 40)
			return fmt.Errorf("unsupported user type for authorization")
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
			s.sendConnectionClose(conn, amqperrors.InvalidPath,
				fmt.Sprintf("access to vhost %s refused for user %s", vhost, user.Username), 10, 40)
			return fmt.Errorf("access to vhost %s refused for user %s", vhost, user.Username)
		}

		// RabbitMQ loopback restriction: users with LoopbackOnly=true may only
		// connect from localhost (loopback interface).
		if loopbackOnly && !isLoopbackConn(conn.Conn) {
			s.Log.Warn("Loopback restriction violated",
				zap.String("connection_id", conn.ID),
				zap.String("username", user.Username))
			s.sendConnectionClose(conn, amqperrors.AccessRefused,
				fmt.Sprintf("user %s can only connect via localhost", user.Username), 10, 40)
			return fmt.Errorf("user %s can only connect via localhost", user.Username)
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
				s.sendConnectionClose(conn, amqperrors.AccessRefused,
					fmt.Sprintf("user %s can only connect via localhost", user.Username), 10, 40)
				return fmt.Errorf("user %s can only connect via localhost", user.Username)
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

// handleConnectionStartOK handles the connection.start-ok method and performs authentication
func (s *Server) handleConnectionStartOK(conn *protocol.Connection, payload []byte) error {
	// Parse connection.start-ok
	startOK, err := protocol.ParseConnectionStartOK(payload)
	if err != nil {
		s.Log.Error("Failed to parse connection.start-ok",
			zap.String("connection_id", conn.ID),
			zap.Error(err))
		return s.sendConnectionClose(conn, 503, "Failed to parse connection.start-ok", 10, 11)
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
		return s.sendConnectionClose(conn, amqperrors.InternalError,
			"authentication enabled but no authenticator configured", 10, 11)
	}

	// Get the mechanism
	mechanism, err := s.MechanismRegistry.Get(startOK.Mechanism)
	if err != nil {
		s.Log.Warn("Unsupported authentication mechanism",
			zap.String("connection_id", conn.ID),
			zap.String("mechanism", startOK.Mechanism),
			zap.Error(err))
		return s.sendConnectionClose(conn, 403,
			fmt.Sprintf("Unsupported mechanism: %s", startOK.Mechanism), 10, 11)
	}

	// Authenticate using the mechanism
	userInterface, err := mechanism.Authenticate(startOK.Response, s.Authenticator)
	if err != nil {
		s.Log.Warn("Authentication failed",
			zap.String("connection_id", conn.ID),
			zap.String("mechanism", startOK.Mechanism),
			zap.Error(err))
		_ = s.sendConnectionClose(conn, 403,
			"Authentication failed", 10, 11)
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Extract username from authenticated user
	if user, ok := userInterface.(*interfaces.User); ok {
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

	return nil
}

// processConnectionMethod processes connection-level methods
func (s *Server) processConnectionMethod(conn *protocol.Connection, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ConnectionStartOK: // Method ID 11 for connection class
		// Process connection.start-ok and authenticate
		s.Log.Debug("Received connection.start-ok", zap.String("connection_id", conn.ID))
		return s.handleConnectionStartOK(conn, payload)
	case protocol.ConnectionTuneOK: // Method ID 31 for connection class
		s.Log.Debug("Received connection.tune-ok", zap.String("connection_id", conn.ID))
		return nil
	case protocol.ConnectionSecureOK: // Method ID 21 — multi-step SASL not supported
		s.Log.Warn("Received unexpected connection.secure-ok (multi-step SASL not supported)",
			zap.String("connection_id", conn.ID))
		return s.sendConnectionClose(conn, 503, "COMMAND_INVALID - multi-step SASL not supported", 10, 21)
	case protocol.ConnectionOpenOK: // Method ID 41 for connection class
		// Process connection.open-ok
		s.Log.Debug("Received connection.open-ok", zap.String("connection_id", conn.ID))
		return nil
	case protocol.ConnectionClose: // Method ID 50 for connection class
		// Process connection.close
		s.Log.Info("Received connection.close", zap.String("connection_id", conn.ID))
		if err := s.sendConnectionCloseOK(conn); err != nil {
			s.Log.Error("Failed to send connection.close-ok", zap.Error(err))
		}
		conn.Closed.Store(true)
		return errClientRequestedClose
	default:
		s.Log.Warn("Unknown connection method ID",
			zap.Uint16("method_id", methodID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown connection method ID: %d", methodID)
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

	if err := protocol.WriteFrameToConnection(conn, frame); err != nil {
		return err
	}

	// Mark connection as closed
	conn.Closed.Store(true)

	return nil
}
