package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

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

	return protocol.WriteFrameToConnection(conn, frame)
}

// sendConnectionTune sends the connection.tune method frame
func (s *Server) sendConnectionTune(conn *protocol.Connection) error {
	// Create the connection.tune method
	tuneMethod := &protocol.ConnectionTuneMethod{
		ChannelMax: 65535,  // Maximum number of channels allowed per connection
		FrameMax:   131072, // Maximum frame size in bytes
		Heartbeat:  60,     // Heartbeat interval in seconds (0 = disabled)
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

	// Validate the virtual host
	// For now, allow "/" and empty string (which defaults to "/")
	// In a real implementation, we'd have proper vhost management
	if openMethod.VirtualHost != "/" && openMethod.VirtualHost != "" {
		return fmt.Errorf("access to vhost %s not allowed", openMethod.VirtualHost)
	}

	// Set the connection's vhost
	conn.Vhost = openMethod.VirtualHost
	if conn.Vhost == "" {
		conn.Vhost = "/" // Default vhost
	}

	s.Log.Info("Connection opened",
		zap.String("connection_id", conn.ID),
		zap.String("vhost", conn.Vhost))

	// Send connection.open-ok
	return s.sendConnectionOpenOK(conn)
}

// handleConnectionStartOK handles the connection.start-ok method and performs authentication
func (s *Server) handleConnectionStartOK(conn *protocol.Connection, payload []byte) error {
	// Parse connection.start-ok
	startOK, err := protocol.ParseConnectionStartOK(payload)
	if err != nil {
		s.Log.Error("Failed to parse connection.start-ok",
			zap.String("connection_id", conn.ID),
			zap.Error(err))
		return s.sendConnectionClose(conn, 503, "COMMAND_INVALID", "Failed to parse connection.start-ok")
	}

	s.Log.Debug("Connection.start-ok parsed",
		zap.String("connection_id", conn.ID),
		zap.String("mechanism", startOK.Mechanism),
		zap.String("locale", startOK.Locale))

	// Check if authentication is enabled
	if s.Config != nil && !s.Config.Security.AuthenticationEnabled {
		// Authentication disabled, allow connection
		conn.Username = "guest"
		s.Log.Info("Authentication disabled, allowing connection as guest",
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Perform authentication if authenticator is available
	if s.Authenticator != nil && s.MechanismRegistry != nil {
		// Get the mechanism
		mechanism, err := s.MechanismRegistry.Get(startOK.Mechanism)
		if err != nil {
			s.Log.Warn("Unsupported authentication mechanism",
				zap.String("connection_id", conn.ID),
				zap.String("mechanism", startOK.Mechanism),
				zap.Error(err))
			return s.sendConnectionClose(conn, 403, "ACCESS_REFUSED", fmt.Sprintf("Unsupported mechanism: %s", startOK.Mechanism))
		}

		// Authenticate using the mechanism
		userInterface, err := mechanism.Authenticate(startOK.Response, s.Authenticator)
		if err != nil {
			s.Log.Warn("Authentication failed",
				zap.String("connection_id", conn.ID),
				zap.String("mechanism", startOK.Mechanism),
				zap.Error(err))
			return s.sendConnectionClose(conn, 403, "ACCESS_REFUSED", "Authentication failed")
		}

		// Extract username from authenticated user
		if user, ok := userInterface.(*interfaces.User); ok {
			conn.Username = user.Username
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
	} else {
		// No authenticator configured, allow connection as guest
		conn.Username = "guest"
		s.Log.Info("No authenticator configured, allowing connection as guest",
			zap.String("connection_id", conn.ID))
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
		// Process connection.tune-ok
		s.Log.Debug("Received connection.tune-ok", zap.String("connection_id", conn.ID))
		return nil
	case protocol.ConnectionOpenOK: // Method ID 41 for connection class
		// Process connection.open-ok
		s.Log.Debug("Received connection.open-ok", zap.String("connection_id", conn.ID))
		return nil
	case protocol.ConnectionClose: // Method ID 50 for connection class
		// Process connection.close
		s.Log.Info("Received connection.close", zap.String("connection_id", conn.ID))
		// Send connection.close-ok and close the connection
		return s.sendConnectionCloseOK(conn)
	default:
		s.Log.Warn("Unknown connection method ID",
			zap.Uint16("method_id", methodID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown connection method ID: %d", methodID)
	}
}

// sendConnectionClose sends the connection.close method frame
func (s *Server) sendConnectionClose(conn *protocol.Connection, replyCode uint16, replyText, classMethod string) error {
	closeMethod := &protocol.ConnectionCloseMethod{
		ReplyCode: replyCode,
		ReplyText: replyText,
		ClassID:   0, // Set based on classMethod if needed
		MethodID:  0, // Set based on classMethod if needed
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
