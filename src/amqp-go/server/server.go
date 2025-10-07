package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

const (
	AMQPVersion   = "0.9.1"
	AMQPServer    = "amqp-go-server"
	AMQPProduct   = "AMQP-Go"
	AMQPPlatform  = "Go"
	AMQPCopyright = "Maxpert AMQP-Go Server"
)

// Server represents the AMQP server
type Server struct {
	Addr               string
	Listener           net.Listener
	Connections        map[string]*protocol.Connection
	Mutex              sync.RWMutex
	Shutdown           bool
	Log                *zap.Logger
	Broker             UnifiedBroker
	Config             *config.AMQPConfig
	Lifecycle          *LifecycleManager
	TransactionManager interfaces.TransactionManager
	Authenticator      interfaces.Authenticator
	MechanismRegistry  MechanismRegistry
}

// NewServer creates a new AMQP server
func NewServer(addr string) *Server {
	logger, _ := zap.NewProduction()
	return &Server{
		Addr:        addr,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger,
		Broker:      NewOriginalBrokerAdapter(broker.NewBroker()),
	}
}

// Start starts the AMQP server
func (s *Server) Start() error {
	// Listen on the specified address
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}

	s.Listener = listener
	s.Log.Info("AMQP server listening", zap.String("addr", s.Addr))

	// Accept connections
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			if s.Shutdown {
				return nil
			}
			s.Log.Error("Error accepting connection", zap.Error(err))
			continue
		}

		// Handle the connection in a goroutine
		go s.handleConnection(conn)
	}
}

// handleConnection handles a new client connection
func (s *Server) handleConnection(conn net.Conn) {
	// First, check the protocol header
	protoHeader := make([]byte, 8)
	_, err := conn.Read(protoHeader)
	if err != nil {
		s.Log.Error("Error reading protocol header", zap.Error(err))
		conn.Close()
		return
	}

	// Verify protocol header: "AMQP" followed by version (0, 0, 9, 1)
	if string(protoHeader[:4]) != "AMQP" {
		s.Log.Error("Invalid protocol header", zap.Binary("header", protoHeader))
		conn.Close()
		return
	}

	// Create a new connection instance (updated to match fixed structures.go)
	connection := protocol.NewConnection(conn)

	// Add to server's connections
	s.Mutex.Lock()
	if s.Shutdown {
		s.Mutex.Unlock()
		conn.Close()
		return
	}
	s.Connections[connection.ID] = connection
	s.Mutex.Unlock()

	// Start consumer delivery loop for this connection
	go s.consumerDeliveryLoop(connection)

	// Process frames for this connection
	s.processConnectionFrames(connection)

	// Clean up on connection close
	s.Mutex.Lock()
	delete(s.Connections, connection.ID)
	s.Mutex.Unlock()
}

// consumerDeliveryLoop continuously reads from consumer channels and sends messages to clients
func (s *Server) consumerDeliveryLoop(conn *protocol.Connection) {
	s.Log.Info("Starting consumer delivery loop", zap.String("connection_id", conn.ID))

	// This loop reads from consumer channels and sends basic.deliver frames to clients

	// Create a map to track consumer channels we're reading from
	consumerChannels := make(map[string]chan *protocol.Delivery)

	for {
		// Check if connection is closed
		conn.Mutex.RLock()
		closed := conn.Closed
		conn.Mutex.RUnlock()

		if closed {
			s.Log.Debug("Connection closed, stopping consumer delivery loop", zap.String("connection_id", conn.ID))
			break
		}

		// Refresh the list of consumer channels periodically
		// In a real implementation, you'd use a more sophisticated approach
		conn.Mutex.Lock()
		channelCount := len(conn.Channels)
		consumerCount := 0
		for _, channel := range conn.Channels {
			channel.Mutex.RLock()
			for _, consumer := range channel.Consumers {
				consumerCount++
				// Add consumer's message channel to our tracking map if not already there
				if _, exists := consumerChannels[consumer.Tag]; !exists {
					consumerChannels[consumer.Tag] = consumer.Messages
					s.Log.Info("Added consumer to delivery loop",
						zap.String("consumer_tag", consumer.Tag),
						zap.String("queue", consumer.Queue))
				}
			}
			channel.Mutex.RUnlock()
		}
		conn.Mutex.Unlock()

		s.Log.Info("Consumer delivery loop iteration",
			zap.String("connection_id", conn.ID),
			zap.Int("channels", channelCount),
			zap.Int("consumers", consumerCount),
			zap.Int("tracked_channels", len(consumerChannels)))

		// Check each consumer channel for messages
		for consumerTag, msgChan := range consumerChannels {
			s.Log.Debug("Checking consumer channel",
				zap.String("consumer_tag", consumerTag),
				zap.Int("buffered_messages", len(msgChan)))

			select {
			case delivery := <-msgChan:
				// Got a message, send it to the client
				s.Log.Info("Sending message to consumer",
					zap.String("consumer_tag", consumerTag),
					zap.Uint64("delivery_tag", delivery.DeliveryTag),
					zap.String("exchange", delivery.Exchange),
					zap.String("routing_key", delivery.RoutingKey))

				// Find the channel and consumer for this delivery
				var targetChannel *protocol.Channel
				var targetConsumer *protocol.Consumer

				conn.Mutex.RLock()
				for _, channel := range conn.Channels {
					channel.Mutex.RLock()
					if consumer, exists := channel.Consumers[consumerTag]; exists {
						targetChannel = channel
						targetConsumer = consumer
						channel.Mutex.RUnlock()
						break
					}
					channel.Mutex.RUnlock()
				}
				conn.Mutex.RUnlock()

				if targetChannel != nil && targetConsumer != nil {
					s.Log.Info("About to send basic.deliver",
						zap.String("consumer_tag", consumerTag),
						zap.Uint16("channel_id", targetChannel.ID))

					// Send basic.deliver frame to the client
					err := s.sendBasicDeliver(
						conn,
						targetChannel.ID,
						consumerTag,
						delivery.DeliveryTag,
						delivery.Redelivered,
						delivery.Exchange,
						delivery.RoutingKey,
						delivery.Message,
					)
					if err != nil {
						s.Log.Error("Failed to send basic.deliver",
							zap.Error(err),
							zap.String("consumer_tag", consumerTag))
					} else {
						s.Log.Info("Sent basic.deliver successfully",
							zap.String("consumer_tag", consumerTag),
							zap.Uint64("delivery_tag", delivery.DeliveryTag))
					}
				} else {
					s.Log.Info("Could not find target channel/consumer",
						zap.String("consumer_tag", consumerTag))
				}
			default:
				// No message available on this channel, continue to next
				s.Log.Debug("No message available on consumer channel",
					zap.String("consumer_tag", consumerTag))
			}
		}

		// Sleep briefly to prevent busy waiting
		time.Sleep(10 * time.Millisecond)
	}

	s.Log.Debug("Consumer delivery loop stopped", zap.String("connection_id", conn.ID))
}

// processConnectionFrames reads and processes frames from a connection
func (s *Server) processConnectionFrames(conn *protocol.Connection) {
	defer conn.Conn.Close()

	// Send connection start frame
	if err := s.sendConnectionStart(conn); err != nil {
		s.Log.Error("Error sending connection start", zap.Error(err))
		return
	}

	// Wait for connection.start-ok from client
	frame, err := protocol.ReadFrame(conn.Conn)
	if err != nil {
		s.Log.Error("Error reading connection.start-ok", zap.Error(err))
		return
	}

	if frame.Type != protocol.FrameMethod {
		s.Log.Error("Expected method frame", zap.Int("type", int(frame.Type)))
		return
	}

	// Process the method frame to validate it's connection.start-ok
	// This is a simplified check - in reality we'd decode the method ID
	// and verify it matches connection.start-ok

	// For now, assume it's valid and send tune parameters
	if err := s.sendConnectionTune(conn); err != nil {
		s.Log.Error("Error sending connection tune", zap.Error(err))
		return
	}

	// Wait for connection.tune-ok from client
	frame, err = protocol.ReadFrame(conn.Conn)
	if err != nil {
		s.Log.Error("Error reading connection.tune-ok", zap.Error(err))
		return
	}

	if frame.Type != protocol.FrameMethod {
		s.Log.Error("Expected method frame", zap.Int("type", int(frame.Type)))
		return
	}

	// Wait for connection.open from client
	frame, err = protocol.ReadFrame(conn.Conn)
	if err != nil {
		s.Log.Error("Error reading connection.open", zap.Error(err))
		return
	}

	if frame.Type != protocol.FrameMethod {
		s.Log.Error("Expected method frame", zap.Int("type", int(frame.Type)))
		return
	}

	// Process connection.open method
	if err := s.processConnectionOpen(conn, frame); err != nil {
		s.Log.Error("Error processing connection.open", zap.Error(err))
		return
	}

	// At this point, the connection handshake is complete
	s.Log.Info("Connection handshake completed", zap.String("connection_id", conn.ID))

	// Now enter the main frame processing loop
	for {
		frame, err := protocol.ReadFrame(conn.Conn)
		if err != nil {
			if err.Error() == "EOF" {
				s.Log.Info("Connection closed by client", zap.String("connection_id", conn.ID))
			} else {
				s.Log.Error("Error reading frame from connection",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
			}
			return
		}

		// Process the frame based on its type
		switch frame.Type {
		case protocol.FrameMethod:
			if err := s.processMethodFrame(conn, frame); err != nil {
				s.Log.Error("Error processing method frame",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
				return
			}
		case protocol.FrameHeader:
			if err := s.processHeaderFrame(conn, frame); err != nil {
				s.Log.Error("Error processing header frame",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
				return
			}
		case protocol.FrameBody:
			if err := s.processBodyFrame(conn, frame); err != nil {
				s.Log.Error("Error processing body frame",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
				return
			}
		case protocol.FrameHeartbeat:
			// Handle heartbeat frames
			s.Log.Debug("Heartbeat frame received", zap.String("connection_id", conn.ID))
		default:
			s.Log.Warn("Unknown frame type",
				zap.Int("type", int(frame.Type)),
				zap.String("connection_id", conn.ID))
		}
	}
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

	return protocol.WriteFrame(conn.Conn, frame)
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

	return protocol.WriteFrame(conn.Conn, frame)
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

	return protocol.WriteFrame(conn.Conn, frame)
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

	return protocol.WriteFrame(conn.Conn, frame)
}

// processMethodFrame processes an incoming method frame
func (s *Server) processMethodFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	// Check if it's a connection-level method (Channel 0)
	if frame.Channel != 0 {
		// This is a channel-level method, process differently
		return s.processChannelMethod(conn, frame)
	}

	// For now, just check the class and method IDs in the payload
	if len(frame.Payload) < 4 {
		return fmt.Errorf("method frame payload too short")
	}

	classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])

	// Process based on class and method IDs
	switch {
	case classID == 10: // Connection class
		return s.processConnectionMethod(conn, methodID, frame.Payload[4:])
	case classID == 20: // Channel class
		return s.processChannelMethod(conn, frame)
	default:
		return fmt.Errorf("unknown class ID: %d", classID)
	}
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

// processChannelMethod processes channel-level methods
func (s *Server) processChannelMethod(conn *protocol.Connection, frame *protocol.Frame) error {
	if len(frame.Payload) < 4 {
		s.Log.Warn("Method frame payload too short",
			zap.Int("length", len(frame.Payload)),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("method frame payload too short")
	}

	classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])

	// Process based on class and method IDs
	switch classID {
	case 20: // Channel class
		return s.processChannelSpecificMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	case 40: // Exchange class
		return s.processExchangeMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	case 50: // Queue class
		return s.processQueueMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	case 60: // Basic class
		return s.processBasicMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	case 90: // Transaction class
		return s.processTransactionMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	default:
		s.Log.Warn("Unknown class ID",
			zap.Uint16("class_id", classID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown class ID: %d", classID)
	}
}

// processChannelSpecificMethod handles specific channel methods
func (s *Server) processChannelSpecificMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ChannelOpen: // Method ID 10 for channel class
		// Create new channel
		newChannel := protocol.NewChannel(channelID, conn)
		conn.Mutex.Lock()
		conn.Channels[channelID] = newChannel
		conn.Mutex.Unlock()

		s.Log.Debug("Channel opened",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		// Send channel.open-ok
		return s.sendChannelOpenOK(conn, channelID)

	case protocol.ChannelClose: // Method ID 40 for channel class
		s.Log.Debug("Channel close requested",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		// Clean up channel resources
		conn.Mutex.Lock()
		if channel, exists := conn.Channels[channelID]; exists {
			// Cancel all consumers on this channel
			channel.Mutex.Lock()
			for consumerTag := range channel.Consumers {
				s.Log.Debug("Canceling consumer due to channel close",
					zap.String("consumer_tag", consumerTag),
					zap.Uint16("channel_id", channelID))
			}
			channel.Consumers = make(map[string]*protocol.Consumer) // Clear all consumers
			channel.Closed = true
			channel.Mutex.Unlock()

			// Remove channel from connection
			delete(conn.Channels, channelID)
		}
		conn.Mutex.Unlock()

		// Send channel.close-ok
		return s.sendChannelCloseOK(conn, channelID)

	default:
		s.Log.Warn("Unknown channel method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown channel method ID: %d", methodID)
	}
}

// processExchangeMethod handles exchange-related methods
func (s *Server) processExchangeMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ExchangeDeclare: // Method ID 10 for exchange class
		return s.handleExchangeDeclare(conn, channelID, payload)
	case protocol.ExchangeDelete: // Method ID 20 for exchange class
		return s.handleExchangeDelete(conn, channelID, payload)
	case protocol.ExchangeUnbind: // Method ID 40 for exchange class
		return s.handleExchangeUnbind(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown exchange method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown exchange method ID: %d", methodID)
	}
}

// processQueueMethod handles queue-related methods
func (s *Server) processQueueMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.QueueDeclare: // Method ID 10 for queue class
		return s.handleQueueDeclare(conn, channelID, payload)
	case protocol.QueueBind: // Method ID 20 for queue class
		return s.handleQueueBind(conn, channelID, payload)
	case protocol.QueueUnbind: // Method ID 50 for queue class
		return s.handleQueueUnbind(conn, channelID, payload)
	case protocol.QueueDelete: // Method ID 40 for queue class
		return s.handleQueueDelete(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown queue method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown queue method ID: %d", methodID)
	}
}

// handleExchangeDeclare handles the exchange.declare method
func (s *Server) handleExchangeDeclare(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the exchange.declare method
	declareMethod := &protocol.ExchangeDeclareMethod{}
	// We need to implement deserialization from the payload
	// Since we only have serialization, let's implement a simple approach
	err := declareMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.declare",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Exchange declared",
		zap.String("exchange", declareMethod.Exchange),
		zap.String("type", declareMethod.Type),
		zap.Bool("durable", declareMethod.Durable),
		zap.Bool("auto_delete", declareMethod.AutoDelete),
		zap.Bool("internal", declareMethod.Internal))

	// Call the broker to declare the exchange
	err = s.Broker.DeclareExchange(
		declareMethod.Exchange,
		declareMethod.Type,
		declareMethod.Durable,
		declareMethod.AutoDelete,
		declareMethod.Internal,
		declareMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to declare exchange",
			zap.Error(err),
			zap.String("exchange", declareMethod.Exchange))
		return err
	}

	// Send exchange.declare-ok response
	return s.sendExchangeDeclareOK(conn, channelID)
}

// handleExchangeDelete handles the exchange.delete method
func (s *Server) handleExchangeDelete(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the exchange.delete method
	deleteMethod := &protocol.ExchangeDeleteMethod{}
	err := deleteMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.delete",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Exchange deleted",
		zap.String("exchange", deleteMethod.Exchange),
		zap.Bool("if_unused", deleteMethod.IfUnused))

	// Call the broker to delete the exchange
	err = s.Broker.DeleteExchange(deleteMethod.Exchange, deleteMethod.IfUnused)
	if err != nil {
		s.Log.Error("Failed to delete exchange",
			zap.Error(err),
			zap.String("exchange", deleteMethod.Exchange))
		return err
	}

	// Send exchange.delete-ok response
	return s.sendExchangeDeleteOK(conn, channelID)
}

// handleExchangeUnbind handles the exchange.unbind method
func (s *Server) handleExchangeUnbind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the exchange.unbind method
	unbindMethod := &protocol.ExchangeUnbindMethod{}
	err := unbindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.unbind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Exchange unbound",
		zap.String("destination", unbindMethod.Destination),
		zap.String("source", unbindMethod.Source),
		zap.String("routing_key", unbindMethod.RoutingKey))

	// Call the broker to unbind the exchange
	// In a real implementation, you'd remove the binding between source and destination exchanges
	// For now, we'll just log the operation

	// Send exchange.unbind-ok response
	return s.sendExchangeUnbindOK(conn, channelID)
}

// handleQueueDeclare handles the queue.declare method
func (s *Server) handleQueueDeclare(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.declare method
	declareMethod := &protocol.QueueDeclareMethod{}
	err := declareMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.declare",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue declared",
		zap.String("queue", declareMethod.Queue),
		zap.Bool("durable", declareMethod.Durable),
		zap.Bool("auto_delete", declareMethod.AutoDelete),
		zap.Bool("exclusive", declareMethod.Exclusive))

	// Call the broker to declare the queue
	queue, err := s.Broker.DeclareQueue(
		declareMethod.Queue,
		declareMethod.Durable,
		declareMethod.AutoDelete,
		declareMethod.Exclusive,
		declareMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to declare queue",
			zap.Error(err),
			zap.String("queue", declareMethod.Queue))
		return err
	}

	// Send queue.declare-ok response with queue info
	return s.sendQueueDeclareOK(conn, channelID, queue.Name, uint32(len(queue.Messages)), 0)
}

// handleQueueBind handles the queue.bind method
func (s *Server) handleQueueBind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.bind method
	bindMethod := &protocol.QueueBindMethod{}
	err := bindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.bind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue bound",
		zap.String("queue", bindMethod.Queue),
		zap.String("exchange", bindMethod.Exchange),
		zap.String("routing_key", bindMethod.RoutingKey))

	// Call the broker to bind the queue
	err = s.Broker.BindQueue(
		bindMethod.Queue,
		bindMethod.Exchange,
		bindMethod.RoutingKey,
		bindMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to bind queue",
			zap.Error(err),
			zap.String("queue", bindMethod.Queue),
			zap.String("exchange", bindMethod.Exchange))
		return err
	}

	// Send queue.bind-ok response
	return s.sendQueueBindOK(conn, channelID)
}

// handleQueueUnbind handles the queue.unbind method
func (s *Server) handleQueueUnbind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.unbind method
	// We need to create a structure for this, but for now we'll implement a basic approach
	// The queue.unbind method has similar structure to queue.bind but without arguments

	// Create a temporary approach - reuse QueueBindMethod for deserialization since structure is similar
	unbindMethod := &protocol.QueueBindMethod{}
	err := unbindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.unbind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue unbound",
		zap.String("queue", unbindMethod.Queue),
		zap.String("exchange", unbindMethod.Exchange),
		zap.String("routing_key", unbindMethod.RoutingKey))

	// Call the broker to unbind the queue
	err = s.Broker.UnbindQueue(
		unbindMethod.Queue,
		unbindMethod.Exchange,
		unbindMethod.RoutingKey,
	)

	if err != nil {
		s.Log.Error("Failed to unbind queue",
			zap.Error(err),
			zap.String("queue", unbindMethod.Queue),
			zap.String("exchange", unbindMethod.Exchange))
		return err
	}

	// Send queue.unbind-ok response
	return s.sendQueueUnbindOK(conn, channelID)
}

// sendQueueUnbindOK sends the queue.unbind-ok method frame
func (s *Server) sendQueueUnbindOK(conn *protocol.Connection, channelID uint16) error {
	// queue.unbind-ok has no content, similar to bind-ok
	bindOKMethod := &protocol.QueueBindOKMethod{} // Reuse the same structure since both have no content

	methodData, err := bindOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing queue.unbind-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 50, 51, methodData) // 50.51 = queue.unbind-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// handleQueueDelete handles the queue.delete method
func (s *Server) handleQueueDelete(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.delete method
	deleteMethod := &protocol.QueueDeleteMethod{}
	err := deleteMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.delete",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue deleted",
		zap.String("queue", deleteMethod.Queue),
		zap.Bool("if_unused", deleteMethod.IfUnused),
		zap.Bool("if_empty", deleteMethod.IfEmpty))

	// Call the broker to delete the queue
	err = s.Broker.DeleteQueue(deleteMethod.Queue, deleteMethod.IfUnused, deleteMethod.IfEmpty)
	if err != nil {
		s.Log.Error("Failed to delete queue",
			zap.Error(err),
			zap.String("queue", deleteMethod.Queue))
		return err
	}

	// Send queue.delete-ok response with number of deleted messages
	return s.sendQueueDeleteOK(conn, channelID, 0)
}

// sendConnectionClose sends the connection.close method frame
func (s *Server) sendConnectionClose(conn *protocol.Connection, replyCode uint16, replyText, classMethod string) error {
	closeMethod := &protocol.ConnectionCloseMethod{
		ReplyCode: replyCode,
		ReplyText: replyText,
		ClassID:   0,  // Set based on classMethod if needed
		MethodID:  0,  // Set based on classMethod if needed
	}

	methodData, err := closeMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.close: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 50, methodData) // 10.50 = connection.close

	if err := protocol.WriteFrame(conn.Conn, frame); err != nil {
		return err
	}

	// Mark connection as closed
	conn.Mutex.Lock()
	conn.Closed = true
	conn.Mutex.Unlock()

	return nil
}

// sendConnectionCloseOK sends the connection.close-ok method frame
func (s *Server) sendConnectionCloseOK(conn *protocol.Connection) error {
	closeOKMethod := &protocol.ConnectionCloseOKMethod{}

	methodData, err := closeOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing connection.close-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrame(10, 51, methodData) // 10.51 = connection.close-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendChannelOpenOK sends the channel.open-ok method frame
func (s *Server) sendChannelOpenOK(conn *protocol.Connection, channelID uint16) error {
	openOKMethod := &protocol.ChannelOpenOKMethod{
		Reserved1: "", // Currently unused
	}

	methodData, err := openOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing channel.open-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 20, 11, methodData) // 20.11 = channel.open-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendChannelCloseOK sends the channel.close-ok method frame
func (s *Server) sendChannelCloseOK(conn *protocol.Connection, channelID uint16) error {
	closeOKMethod := &protocol.ChannelCloseOKMethod{}

	methodData, err := closeOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing channel.close-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 20, 41, methodData) // 20.41 = channel.close-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendExchangeDeclareOK sends the exchange.declare-ok method frame
func (s *Server) sendExchangeDeclareOK(conn *protocol.Connection, channelID uint16) error {
	declareOKMethod := &protocol.ExchangeDeclareOKMethod{}

	methodData, err := declareOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing exchange.declare-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 40, 11, methodData) // 40.11 = exchange.declare-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendExchangeDeleteOK sends the exchange.delete-ok method frame
func (s *Server) sendExchangeDeleteOK(conn *protocol.Connection, channelID uint16) error {
	deleteOKMethod := &protocol.ExchangeDeleteOKMethod{}

	methodData, err := deleteOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing exchange.delete-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 40, 21, methodData) // 40.21 = exchange.delete-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendExchangeUnbindOK sends the exchange.unbind-ok method frame
func (s *Server) sendExchangeUnbindOK(conn *protocol.Connection, channelID uint16) error {
	unbindOKMethod := &protocol.ExchangeUnbindOKMethod{}

	methodData, err := unbindOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing exchange.unbind-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 40, 41, methodData) // 40.41 = exchange.unbind-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendQueueDeclareOK sends the queue.declare-ok method frame
func (s *Server) sendQueueDeclareOK(conn *protocol.Connection, channelID uint16, queueName string, messageCount, consumerCount uint32) error {
	declareOKMethod := &protocol.QueueDeclareOKMethod{
		Queue:         queueName,
		MessageCount:  messageCount,
		ConsumerCount: consumerCount,
	}

	methodData, err := declareOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing queue.declare-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 50, 11, methodData) // 50.11 = queue.declare-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendQueueBindOK sends the queue.bind-ok method frame
func (s *Server) sendQueueBindOK(conn *protocol.Connection, channelID uint16) error {
	bindOKMethod := &protocol.QueueBindOKMethod{}

	methodData, err := bindOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing queue.bind-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 50, 21, methodData) // 50.21 = queue.bind-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendQueueDeleteOK sends the queue.delete-ok method frame
func (s *Server) sendQueueDeleteOK(conn *protocol.Connection, channelID uint16, messageCount uint32) error {
	deleteOKMethod := &protocol.QueueDeleteOKMethod{
		MessageCount: messageCount,
	}

	methodData, err := deleteOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing queue.delete-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 50, 41, methodData) // 50.41 = queue.delete-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// processBasicMethod handles basic-class methods
func (s *Server) processBasicMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.BasicQos: // Method ID 10 for basic class
		return s.handleBasicQos(conn, channelID, payload)
	case protocol.BasicPublish: // Method ID 40 for basic class
		return s.handleBasicPublish(conn, channelID, payload)
	case protocol.BasicConsume: // Method ID 20 for basic class
		return s.handleBasicConsume(conn, channelID, payload)
	case protocol.BasicCancel: // Method ID 30 for basic class
		return s.handleBasicCancel(conn, channelID, payload)
	case protocol.BasicGet: // Method ID 70 for basic class
		return s.handleBasicGet(conn, channelID, payload)
	case protocol.BasicAck: // Method ID 80 for basic class
		return s.handleBasicAck(conn, channelID, payload)
	case protocol.BasicReject: // Method ID 90 for basic class
		return s.handleBasicReject(conn, channelID, payload)
	case protocol.BasicNack: // Method ID 120 for basic class
		return s.handleBasicNack(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown basic method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown basic method ID: %d", methodID)
	}
}

// processTransactionMethod processes transaction class methods
func (s *Server) processTransactionMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.TxSelect: // Method ID 10 for tx class
		return s.handleTxSelect(conn, channelID, payload)
	case protocol.TxCommit: // Method ID 20 for tx class
		return s.handleTxCommit(conn, channelID, payload)
	case protocol.TxRollback: // Method ID 30 for tx class
		return s.handleTxRollback(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown transaction method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown transaction method ID: %d", methodID)
	}
}

// handleTxSelect handles the tx.select method
func (s *Server) handleTxSelect(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the tx.select method
	_, err := protocol.DeserializeTxSelectMethod(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize tx.select",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Transaction select requested",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	// Put channel into transactional mode
	if s.TransactionManager != nil {
		err = s.TransactionManager.Select(channelID)
		if err != nil {
			s.Log.Error("Failed to select transaction mode",
				zap.Error(err),
				zap.Uint16("channel_id", channelID))
			return err
		}
	} else {
		s.Log.Warn("No transaction manager available")
		return fmt.Errorf("transactions not supported")
	}

	// Send tx.select-ok response
	response := protocol.NewTxSelectOKFrame(channelID)
	return protocol.WriteFrame(conn.Conn, response)
}

// handleTxCommit handles the tx.commit method
func (s *Server) handleTxCommit(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the tx.commit method
	_, err := protocol.DeserializeTxCommitMethod(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize tx.commit",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Transaction commit requested",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	// Commit pending operations
	if s.TransactionManager != nil {
		err = s.TransactionManager.Commit(channelID)
		if err != nil {
			s.Log.Error("Failed to commit transaction",
				zap.Error(err),
				zap.Uint16("channel_id", channelID))
			return err
		}
	} else {
		return fmt.Errorf("transactions not supported")
	}

	// Send tx.commit-ok response
	response := protocol.NewTxCommitOKFrame(channelID)
	return protocol.WriteFrame(conn.Conn, response)
}

// handleTxRollback handles the tx.rollback method
func (s *Server) handleTxRollback(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the tx.rollback method
	_, err := protocol.DeserializeTxRollbackMethod(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize tx.rollback",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Transaction rollback requested",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	// Rollback pending operations
	if s.TransactionManager != nil {
		err = s.TransactionManager.Rollback(channelID)
		if err != nil {
			s.Log.Error("Failed to rollback transaction",
				zap.Error(err),
				zap.Uint16("channel_id", channelID))
			return err
		}
	} else {
		return fmt.Errorf("transactions not supported")
	}

	// Send tx.rollback-ok response
	response := protocol.NewTxRollbackOKFrame(channelID)
	return protocol.WriteFrame(conn.Conn, response)
}

// handleBasicQos handles the basic.qos method
func (s *Server) handleBasicQos(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.qos method
	qosMethod := &protocol.BasicQosMethod{}
	err := qosMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.qos",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic QoS settings",
		zap.Uint32("prefetch_size", qosMethod.PrefetchSize),
		zap.Uint16("prefetch_count", qosMethod.PrefetchCount),
		zap.Bool("global", qosMethod.Global))

	// Get the channel
	conn.Mutex.RLock()
	channel, exists := conn.Channels[channelID]
	conn.Mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}

	// Update channel prefetch settings
	channel.Mutex.Lock()
	channel.PrefetchCount = qosMethod.PrefetchCount
	channel.PrefetchSize = qosMethod.PrefetchSize
	channel.GlobalPrefetch = qosMethod.Global
	channel.Mutex.Unlock()

	// If global is true, we would apply these settings to all channels
	// For now, we'll just apply to this channel

	// Send basic.qos-ok response
	return s.sendBasicQosOK(conn, channelID)
}

// handleBasicPublish handles the basic.publish method
func (s *Server) handleBasicPublish(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.publish method
	publishMethod := &protocol.BasicPublishMethod{}
	err := publishMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.publish",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic publish received",
		zap.String("exchange", publishMethod.Exchange),
		zap.String("routing_key", publishMethod.RoutingKey),
		zap.Bool("mandatory", publishMethod.Mandatory),
		zap.Bool("immediate", publishMethod.Immediate))

	// Create a pending message to track this publication
	// The full message will be completed when we receive the header and body frames
	pendingMsg := &protocol.PendingMessage{
		Method:   publishMethod,
		Body:     make([]byte, 0),
		Received: 0,
		Channel:  conn.Channels[channelID], // Get the channel reference
	}

	// Store the pending message for this channel
	conn.Mutex.Lock()
	conn.PendingMessages[channelID] = pendingMsg
	conn.Mutex.Unlock()

	s.Log.Debug("Started tracking pending message",
		zap.Uint16("channel_id", channelID),
		zap.String("connection_id", conn.ID))

	return nil
}

// sendBasicQosOK sends the basic.qos-ok method frame
func (s *Server) sendBasicQosOK(conn *protocol.Connection, channelID uint16) error {
	qosOKMethod := &protocol.BasicQosOKMethod{}

	methodData, err := qosOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing basic.qos-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 60, 11, methodData) // 60.11 = basic.qos-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// processHeaderFrame processes content header frames
func (s *Server) processHeaderFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	conn.Mutex.Lock()
	defer conn.Mutex.Unlock()

	// Check if there's a pending message for this channel that needs a header
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		// This could be a valid scenario - maybe not every header frame is part of a publish
		s.Log.Warn("Header frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Parse the content header
	contentHeader, err := protocol.ReadContentHeader(frame)
	if err != nil {
		s.Log.Error("Failed to parse content header",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel", frame.Channel))
		return err
	}

	// Attach the header to the pending message
	pendingMsg.Header = contentHeader

	s.Log.Debug("Content header received for pending message",
		zap.String("exchange", pendingMsg.Method.Exchange),
		zap.String("routing_key", pendingMsg.Method.RoutingKey),
		zap.Uint64("body_size", contentHeader.BodySize))

	// For empty messages (body_size = 0), no body frame will be sent
	// We should process the complete message immediately
	if contentHeader.BodySize == 0 {
		s.Log.Debug("Empty message detected - processing immediately",
			zap.String("exchange", pendingMsg.Method.Exchange),
			zap.String("routing_key", pendingMsg.Method.RoutingKey))

		// Process the complete message immediately
		if err := s.processCompleteMessage(conn, frame.Channel, pendingMsg); err != nil {
			s.Log.Error("Failed to process complete empty message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}

		// Remove the pending message from the map since it's now complete
		delete(conn.PendingMessages, frame.Channel)
	}

	return nil
}

// processBodyFrame processes content body frames
func (s *Server) processBodyFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	conn.Mutex.Lock()
	defer conn.Mutex.Unlock()

	// Check if there's a pending message for this channel that needs body content
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		s.Log.Warn("Body frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Check if we have a header for this message
	if pendingMsg.Header == nil {
		s.Log.Warn("Body frame received before header frame",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("body frame received before header frame")
	}

	// Append the body content to the pending message
	pendingMsg.Body = append(pendingMsg.Body, frame.Payload...)
	pendingMsg.Received += uint64(len(frame.Payload))

	s.Log.Debug("Body frame received for pending message",
		zap.Uint16("channel", frame.Channel),
		zap.Uint64("received", pendingMsg.Received),
		zap.Uint64("expected", pendingMsg.Header.BodySize))

	// Check if we've received the complete message
	if pendingMsg.Received >= pendingMsg.Header.BodySize {
		// We have received all the body data
		if pendingMsg.Received > pendingMsg.Header.BodySize {
			s.Log.Error("Received more body data than expected",
				zap.Uint64("received", pendingMsg.Received),
				zap.Uint64("expected", pendingMsg.Header.BodySize))
			return fmt.Errorf("received more body data than expected")
		}

		// Finalize the message by trimming any extra bytes
		if pendingMsg.Received > uint64(len(pendingMsg.Body)) {
			// This shouldn't happen, but just in case
			pendingMsg.Body = pendingMsg.Body[:pendingMsg.Header.BodySize]
		} else if pendingMsg.Received < uint64(len(pendingMsg.Body)) {
			pendingMsg.Body = pendingMsg.Body[:pendingMsg.Received]
		}

		// Process the complete message
		if err := s.processCompleteMessage(conn, frame.Channel, pendingMsg); err != nil {
			s.Log.Error("Failed to process complete message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}

		// Remove the pending message from the map
		delete(conn.PendingMessages, frame.Channel)
	}

	return nil
}

// processCompleteMessage processes a message that has been fully received (method + header + body)
func (s *Server) processCompleteMessage(conn *protocol.Connection, channelID uint16, pendingMsg *protocol.PendingMessage) error {
	s.Log.Info("Processing complete message",
		zap.String("exchange", pendingMsg.Method.Exchange),
		zap.String("routing_key", pendingMsg.Method.RoutingKey),
		zap.Uint64("body_size", uint64(len(pendingMsg.Body))))

	// Convert the pending message to a protocol.Message
	s.Log.Debug("Creating message from pending message",
		zap.String("content_type", pendingMsg.Header.ContentType),
		zap.Int("body_size", len(pendingMsg.Body)),
		zap.Uint16("property_flags", pendingMsg.Header.PropertyFlags))

	message := &protocol.Message{
		Body:            pendingMsg.Body,
		Headers:         pendingMsg.Header.Headers,
		Exchange:        pendingMsg.Method.Exchange,
		RoutingKey:      pendingMsg.Method.RoutingKey,
		ContentType:     pendingMsg.Header.ContentType,
		ContentEncoding: pendingMsg.Header.ContentEncoding,
		DeliveryMode:    pendingMsg.Header.DeliveryMode,
		Priority:        pendingMsg.Header.Priority,
		CorrelationID:   pendingMsg.Header.CorrelationID,
		ReplyTo:         pendingMsg.Header.ReplyTo,
		Expiration:      pendingMsg.Header.Expiration,
		MessageID:       pendingMsg.Header.MessageID,
		Timestamp:       pendingMsg.Header.Timestamp,
		Type:            pendingMsg.Header.Type,
		UserID:          pendingMsg.Header.UserID,
		AppID:           pendingMsg.Header.AppID,
		ClusterID:       pendingMsg.Header.ClusterID,
	}

	// Route the message using the broker
	err := s.Broker.PublishMessage(message.Exchange, message.RoutingKey, message)
	if err != nil {
		s.Log.Error("Failed to route message",
			zap.Error(err),
			zap.String("exchange", message.Exchange),
			zap.String("routing_key", message.RoutingKey))
		return err
	}

	s.Log.Debug("Message successfully routed",
		zap.String("exchange", message.Exchange),
		zap.String("routing_key", message.RoutingKey))

	return nil
}

// handleBasicConsume handles the basic.consume method
func (s *Server) handleBasicConsume(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.consume method
	consumeMethod := &protocol.BasicConsumeMethod{}
	err := consumeMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.consume",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Basic consume requested",
		zap.String("queue", consumeMethod.Queue),
		zap.String("consumer_tag", consumeMethod.ConsumerTag),
		zap.Bool("no_ack", consumeMethod.NoAck),
		zap.Bool("exclusive", consumeMethod.Exclusive))

	// Get the channel
	conn.Mutex.RLock()
	channel, exists := conn.Channels[channelID]
	conn.Mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}

	// Check if queue exists in the broker
	// This is a simplified check - in a real implementation you'd verify queue exists
	// For now, we'll proceed assuming the queue exists

	// Create a new consumer
	consumer := &protocol.Consumer{
		Tag:       consumeMethod.ConsumerTag,
		Channel:   channel,
		Queue:     consumeMethod.Queue,
		NoAck:     consumeMethod.NoAck,
		Exclusive: consumeMethod.Exclusive,
		Args:      consumeMethod.Arguments,
		Messages:  make(chan *protocol.Delivery, 100), // Buffer for pending messages
		Cancel:    make(chan struct{}, 1),             // Channel to signal cancellation
	}

	// Add the consumer to the channel
	channel.Mutex.Lock()
	channel.Consumers[consumer.Tag] = consumer
	channel.Mutex.Unlock()

	// Register the consumer with the broker
	err = s.Broker.RegisterConsumer(consumeMethod.Queue, consumer.Tag, consumer)
	if err != nil {
		s.Log.Error("Failed to register consumer with broker",
			zap.Error(err),
			zap.String("consumer_tag", consumer.Tag),
			zap.String("queue", consumeMethod.Queue))
		return err
	}

	// Send basic.consume-ok response
	err = s.sendBasicConsumeOK(conn, channelID, consumer.Tag)
	if err != nil {
		s.Log.Error("Failed to send basic.consume-ok",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Consumer registered",
		zap.String("consumer_tag", consumer.Tag),
		zap.String("queue", consumer.Queue))

	// In a real implementation, we would now start delivering messages
	// to this consumer from the specified queue

	return nil
}

// handleBasicCancel handles the basic.cancel method
func (s *Server) handleBasicCancel(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.cancel method
	cancelMethod := &protocol.BasicCancelMethod{}
	err := cancelMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.cancel",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Basic cancel requested",
		zap.String("consumer_tag", cancelMethod.ConsumerTag))

	// Get the channel
	conn.Mutex.RLock()
	channel, exists := conn.Channels[channelID]
	conn.Mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}

	// Remove the consumer
	channel.Mutex.Lock()
	consumer, exists := channel.Consumers[cancelMethod.ConsumerTag]
	if !exists {
		channel.Mutex.Unlock()
		// Consumer doesn't exist, but this might be OK depending on the spec
		s.Log.Warn("Attempted to cancel non-existent consumer",
			zap.String("consumer_tag", cancelMethod.ConsumerTag))
		return nil
	}

	// Close the consumer's message channel and signal cancellation
	close(consumer.Cancel)
	delete(channel.Consumers, cancelMethod.ConsumerTag)
	channel.Mutex.Unlock()

	// Unregister the consumer with the broker
	err = s.Broker.UnregisterConsumer(cancelMethod.ConsumerTag)
	if err != nil {
		s.Log.Error("Failed to unregister consumer with broker",
			zap.Error(err),
			zap.String("consumer_tag", cancelMethod.ConsumerTag))
		// Continue anyway since we've already removed it locally
	}

	// Send basic.cancel-ok response
	if !cancelMethod.NoWait {
		err = s.sendBasicCancelOK(conn, channelID, cancelMethod.ConsumerTag)
		if err != nil {
			s.Log.Error("Failed to send basic.cancel-ok",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel_id", channelID))
			return err
		}
	}

	s.Log.Info("Consumer cancelled",
		zap.String("consumer_tag", cancelMethod.ConsumerTag))

	return nil
}

// handleBasicGet handles the basic.get method
func (s *Server) handleBasicGet(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.get method
	getMethod := &protocol.BasicGetMethod{}
	err := getMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.get",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic get requested",
		zap.String("queue", getMethod.Queue),
		zap.Bool("no_ack", getMethod.NoAck))

	// For now, we'll respond with basic.get-empty since we don't have actual message retrieval implemented yet
	// In a real implementation, we would try to get the next message from the queue
	err = s.sendBasicGetEmpty(conn, channelID)
	if err != nil {
		s.Log.Error("Failed to send basic.get-empty",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	return nil
}

// sendBasicConsumeOK sends the basic.consume-ok method frame
func (s *Server) sendBasicConsumeOK(conn *protocol.Connection, channelID uint16, consumerTag string) error {
	consumeOKMethod := &protocol.BasicConsumeOKMethod{
		ConsumerTag: consumerTag,
	}

	methodData, err := consumeOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing basic.consume-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 60, 21, methodData) // 60.21 = basic.consume-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendBasicCancelOK sends the basic.cancel-ok method frame
func (s *Server) sendBasicCancelOK(conn *protocol.Connection, channelID uint16, consumerTag string) error {
	cancelOKMethod := &protocol.BasicCancelOKMethod{
		ConsumerTag: consumerTag,
	}

	methodData, err := cancelOKMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing basic.cancel-ok: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 60, 31, methodData) // 60.31 = basic.cancel-ok

	return protocol.WriteFrame(conn.Conn, frame)
}

// sendBasicDeliver sends a basic.deliver method frame to a consumer
func (s *Server) sendBasicDeliver(conn *protocol.Connection, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	s.Log.Info("ENTERING sendBasicDeliver function",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Uint64("delivery_tag", deliveryTag),
		zap.String("exchange", exchange),
		zap.String("routing_key", routingKey),
		zap.Int("message_body_size", len(message.Body)))

	deliverMethod := &protocol.BasicDeliverMethod{
		ConsumerTag: consumerTag,
		DeliveryTag: deliveryTag,
		Redelivered: redelivered,
		Exchange:    exchange,
		RoutingKey:  routingKey,
	}

	methodData, err := deliverMethod.Serialize()
	if err != nil {
		s.Log.Error("Error serializing basic.deliver", zap.Error(err))
		return fmt.Errorf("error serializing basic.deliver: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 60, 60, methodData) // 60.60 = basic.deliver

	s.Log.Debug("About to send basic.deliver method frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	// NOTE: We'll prepare all frames first, then send them atomically

	// Continue with content header and body frames
	s.Log.Info("Continuing with content header and body frames",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	// Send content header frame
	// Create a content header frame with the message properties
	s.Log.Info("Creating content header structure",
		zap.String("consumer_tag", consumerTag),
		zap.String("content_type", message.ContentType))

	contentHeader := &protocol.ContentHeader{
		ClassID:         60, // basic class
		Weight:          0,
		BodySize:        uint64(len(message.Body)),
		PropertyFlags:   0,
		Headers:         message.Headers,
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    message.DeliveryMode,
		Priority:        message.Priority,
		CorrelationID:   message.CorrelationID,
		ReplyTo:         message.ReplyTo,
		Expiration:      message.Expiration,
		MessageID:       message.MessageID,
		Timestamp:       message.Timestamp,
		Type:            message.Type,
		UserID:          message.UserID,
		AppID:           message.AppID,
		ClusterID:       message.ClusterID,
	}

	// Set property flags based on which properties are set
	propertyFlags := uint16(0)
	if len(message.Headers) > 0 {
		propertyFlags |= protocol.FlagHeaders // headers
	}
	if message.ContentType != "" {
		propertyFlags |= protocol.FlagContentType // content-type
	}
	if message.ContentEncoding != "" {
		propertyFlags |= protocol.FlagContentEncoding // content-encoding
	}
	if message.DeliveryMode != 0 {
		propertyFlags |= protocol.FlagDeliveryMode // delivery-mode
	}
	if message.Priority != 0 {
		propertyFlags |= protocol.FlagPriority // priority
	}
	if message.CorrelationID != "" {
		propertyFlags |= protocol.FlagCorrelationID // correlation-id
	}
	if message.ReplyTo != "" {
		propertyFlags |= protocol.FlagReplyTo // reply-to
	}
	if message.Expiration != "" {
		propertyFlags |= protocol.FlagExpiration // expiration
	}
	if message.MessageID != "" {
		propertyFlags |= protocol.FlagMessageID // message-id
	}
	if message.Timestamp != 0 {
		propertyFlags |= protocol.FlagTimestamp // timestamp
	}
	if message.Type != "" {
		propertyFlags |= protocol.FlagType // type
	}
	if message.UserID != "" {
		propertyFlags |= protocol.FlagUserID // user-id
	}
	if message.AppID != "" {
		propertyFlags |= protocol.FlagAppID // app-id
	}
	if message.ClusterID != "" {
		propertyFlags |= protocol.FlagClusterID // cluster-id
	}

	contentHeader.PropertyFlags = propertyFlags

	s.Log.Info("Property flags set",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("property_flags", propertyFlags))

	// Use ContentHeader.Serialize() to get properly formatted header payload
	s.Log.Info("About to serialize content header",
		zap.String("consumer_tag", consumerTag))

	headerPayload, err := contentHeader.Serialize()
	if err != nil {
		s.Log.Error("Error serializing content header", zap.Error(err))
		return fmt.Errorf("error serializing content header: %v", err)
	}

	s.Log.Info("Content header serialized successfully",
		zap.String("consumer_tag", consumerTag),
		zap.Int("payload_length", len(headerPayload)))

	s.Log.Debug("Content header serialization complete",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("class_id", contentHeader.ClassID),
		zap.Uint64("body_size", contentHeader.BodySize),
		zap.Uint16("property_flags", contentHeader.PropertyFlags),
		zap.String("content_type", contentHeader.ContentType),
		zap.Int("header_payload_length", len(headerPayload)))

	s.Log.Info("Creating content header frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Uint64("body_size", uint64(len(message.Body))),
		zap.Uint16("property_flags", propertyFlags),
		zap.Int("header_payload_size", len(headerPayload)))

	// Create header frame directly with serialized payload
	headerFrame := &protocol.Frame{
		Type:    protocol.FrameHeader,
		Channel: channelID,
		Size:    uint32(len(headerPayload)),
		Payload: headerPayload,
	}

	s.Log.Info("Header frame created",
		zap.String("consumer_tag", consumerTag))

	s.Log.Info("Sending content header frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Uint64("body_size", contentHeader.BodySize))

	// Prepare all frames for atomic transmission
	s.Log.Info("Preparing all frames for atomic transmission",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	// Serialize all three frames
	methodFrameData, err := frame.MarshalBinary()
	if err != nil {
		s.Log.Error("Error serializing method frame", zap.Error(err))
		return fmt.Errorf("error serializing method frame: %v", err)
	}

	headerFrameData, err := headerFrame.MarshalBinary()
	if err != nil {
		s.Log.Error("Error serializing header frame", zap.Error(err))
		return fmt.Errorf("error serializing header frame: %v", err)
	}

	// Send content body frame using the proper protocol function
	s.Log.Info("Creating body frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("body_size", len(message.Body)),
		zap.String("body_content", string(message.Body)))

	bodyFrame := protocol.EncodeBodyFrameForChannel(channelID, message.Body)

	s.Log.Debug("Body frame created",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("payload_size", len(bodyFrame.Payload)),
		zap.String("payload_content", string(bodyFrame.Payload)))

	s.Log.Debug("Sending body frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("body_size", len(message.Body)))

	bodyFrameData, err := bodyFrame.MarshalBinary()
	if err != nil {
		s.Log.Error("Error serializing body frame", zap.Error(err))
		return fmt.Errorf("error serializing body frame: %v", err)
	}

	// Combine all frames into single atomic write
	allFrames := make([]byte, 0, len(methodFrameData)+len(headerFrameData)+len(bodyFrameData))
	allFrames = append(allFrames, methodFrameData...)
	allFrames = append(allFrames, headerFrameData...)
	allFrames = append(allFrames, bodyFrameData...)

	s.Log.Info("Writing all frames atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("total_bytes", len(allFrames)),
		zap.Int("method_frame_size", len(methodFrameData)),
		zap.Int("header_frame_size", len(headerFrameData)),
		zap.Int("body_frame_size", len(bodyFrameData)))

	// Atomic write of all three frames
	_, err = conn.Conn.Write(allFrames)
	if err != nil {
		s.Log.Error("Error sending all frames atomically",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("error sending frames atomically: %v", err)
	}

	s.Log.Info("Sent all frames atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("body_size", len(message.Body)))

	s.Log.Info("basic.deliver complete - all frames sent atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	return nil
}

// sendBasicGetEmpty sends the basic.get-empty method frame
func (s *Server) sendBasicGetEmpty(conn *protocol.Connection, channelID uint16) error {
	getEmptyMethod := &protocol.BasicGetEmptyMethod{
		Reserved1: "amq.empty", // Standard reserved value
	}

	methodData, err := getEmptyMethod.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing basic.get-empty: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 60, 72, methodData) // 60.72 = basic.get-empty

	return protocol.WriteFrame(conn.Conn, frame)
}

// handleBasicAck handles the basic.ack method
func (s *Server) handleBasicAck(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.ack method
	ackMethod := &protocol.BasicAckMethod{}
	err := ackMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.ack",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic ack received",
		zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
		zap.Bool("multiple", ackMethod.Multiple))

	// We need to determine which consumer sent this acknowledgment
	// In a real implementation, we would have a better way to track this
	// For now, we'll look through all consumers on this channel

	conn.Mutex.RLock()
	channel, exists := conn.Channels[channelID]
	conn.Mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}

	// Find the consumer that sent this acknowledgment
	// In a real implementation, you'd have a better way to associate deliveries with consumers
	var consumerTag string
	channel.Mutex.RLock()
	for tag, consumer := range channel.Consumers {
		// Check if this consumer might have sent this delivery
		// In a real implementation, you'd track delivery tags per consumer
		if consumer.Channel.DeliveryTag >= ackMethod.DeliveryTag {
			consumerTag = tag
			break
		}
	}
	channel.Mutex.RUnlock()

	if consumerTag == "" {
		s.Log.Warn("Could not find consumer for acknowledgment",
			zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return nil // Not an error, just a warning
	}

	// Tell the broker to acknowledge the message
	err = s.Broker.AcknowledgeMessage(consumerTag, ackMethod.DeliveryTag, ackMethod.Multiple)
	if err != nil {
		s.Log.Error("Failed to acknowledge message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
			zap.Bool("multiple", ackMethod.Multiple))
		return err
	}

	s.Log.Info("Message acknowledged",
		zap.String("consumer_tag", consumerTag),
		zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
		zap.Bool("multiple", ackMethod.Multiple),
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	return nil
}

// handleBasicReject handles the basic.reject method
func (s *Server) handleBasicReject(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.reject method
	rejectMethod := &protocol.BasicRejectMethod{}
	err := rejectMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.reject",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic reject received",
		zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
		zap.Bool("requeue", rejectMethod.Requeue))

	// We need to determine which consumer sent this rejection
	// In a real implementation, we would have a better way to track this

	conn.Mutex.RLock()
	channel, exists := conn.Channels[channelID]
	conn.Mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}

	// Find the consumer that sent this rejection
	var consumerTag string
	channel.Mutex.RLock()
	for tag, consumer := range channel.Consumers {
		// Check if this consumer might have sent this delivery
		if consumer.Channel.DeliveryTag >= rejectMethod.DeliveryTag {
			consumerTag = tag
			break
		}
	}
	channel.Mutex.RUnlock()

	if consumerTag == "" {
		s.Log.Warn("Could not find consumer for rejection",
			zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return nil // Not an error, just a warning
	}

	// Tell the broker to reject the message
	err = s.Broker.RejectMessage(consumerTag, rejectMethod.DeliveryTag, rejectMethod.Requeue)
	if err != nil {
		s.Log.Error("Failed to reject message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
			zap.Bool("requeue", rejectMethod.Requeue))
		return err
	}

	s.Log.Info("Message rejected",
		zap.String("consumer_tag", consumerTag),
		zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
		zap.Bool("requeue", rejectMethod.Requeue),
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	return nil
}

// handleBasicNack handles the basic.nack method
func (s *Server) handleBasicNack(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.nack method
	nackMethod := &protocol.BasicNackMethod{}
	err := nackMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.nack",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic nack received",
		zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
		zap.Bool("multiple", nackMethod.Multiple),
		zap.Bool("requeue", nackMethod.Requeue))

	// We need to determine which consumer sent this nack
	// In a real implementation, we would have a better way to track this

	conn.Mutex.RLock()
	channel, exists := conn.Channels[channelID]
	conn.Mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}

	// Find the consumer that sent this nack
	var consumerTag string
	channel.Mutex.RLock()
	for tag, consumer := range channel.Consumers {
		// Check if this consumer might have sent this delivery
		if consumer.Channel.DeliveryTag >= nackMethod.DeliveryTag {
			consumerTag = tag
			break
		}
	}
	channel.Mutex.RUnlock()

	if consumerTag == "" {
		s.Log.Warn("Could not find consumer for nack",
			zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return nil // Not an error, just a warning
	}

	// Tell the broker to nack the message
	err = s.Broker.NacknowledgeMessage(consumerTag, nackMethod.DeliveryTag, nackMethod.Multiple, nackMethod.Requeue)
	if err != nil {
		s.Log.Error("Failed to nack message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
			zap.Bool("multiple", nackMethod.Multiple),
			zap.Bool("requeue", nackMethod.Requeue))
		return err
	}

	s.Log.Info("Message negatively acknowledged",
		zap.String("consumer_tag", consumerTag),
		zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
		zap.Bool("multiple", nackMethod.Multiple),
		zap.Bool("requeue", nackMethod.Requeue),
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop() error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.Shutdown = true
	if s.Listener != nil {
		return s.Listener.Close()
	}
	return nil
}

// StartWithQuitChannel starts the server and returns a quit channel that can be used to stop it
func (s *Server) StartWithQuitChannel(quit <-chan struct{}) error {
	// Listen on the specified address
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}

	s.Listener = listener
	s.Log.Info("AMQP server listening", zap.String("addr", s.Addr))

	// Accept connections
	for {
		// Use a non-blocking approach with select to check for quit signal
		connChan := make(chan net.Conn, 1)
		errChan := make(chan error, 1)

		go func() {
			conn, err := s.Listener.Accept()
			connChan <- conn
			errChan <- err
		}()

		select {
		case conn := <-connChan:
			err := <-errChan
			if err != nil {
				if s.Shutdown {
					return nil
				}
				s.Log.Error("Error accepting connection", zap.Error(err))
				continue
			}

			// Handle the connection in a goroutine
			go s.handleConnection(conn)
		case <-quit:
			s.Log.Info("Server shutdown requested")
			s.Listener.Close()
			return nil
		}
	}
}
