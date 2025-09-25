package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"github.com/maxpert/amqp-go/internal/broker"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	AMQPVersion = "0.9.1"
	AMQPServer  = "amqp-go-server"
	AMQPProduct = "AMQP-Go"
	AMQPPlatform = "Go"
	AMQPCopyright = "Maxpert AMQP-Go Server"
)

// Server represents the AMQP server
type Server struct {
	Addr        string
	Listener    net.Listener
	Connections map[string]*protocol.Connection
	Mutex       sync.RWMutex
	Shutdown    bool
	Log         *zap.Logger
	Broker      *broker.Broker
}

// NewServer creates a new AMQP server
func NewServer(addr string) *Server {
	logger, _ := zap.NewProduction()
	return &Server{
		Addr:        addr,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger,
		Broker:      broker.NewBroker(),
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
	
	// Process frames for this connection
	s.processConnectionFrames(connection)
	
	// Clean up on connection close
	s.Mutex.Lock()
	delete(s.Connections, connection.ID)
	s.Mutex.Unlock()
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
	
	// Send connection.open
	if err := s.sendConnectionOpen(conn); err != nil {
		s.Log.Error("Error sending connection open", zap.Error(err))
		return
	}
	
	// Wait for connection.open-ok from client
	frame, err = protocol.ReadFrame(conn.Conn)
	if err != nil {
		s.Log.Error("Error reading connection.open-ok", zap.Error(err))
		return
	}
	
	if frame.Type != protocol.FrameMethod {
		s.Log.Error("Expected method frame", zap.Int("type", int(frame.Type)))
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
			// Handle header frames
		case protocol.FrameBody:
			// Handle body frames
		case protocol.FrameHeartbeat:
			// Handle heartbeat frames
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
		"product": AMQPProduct,
		"version": AMQPVersion,
		"platform": AMQPPlatform,
		"copyright": AMQPCopyright,
		"capabilities": map[string]interface{}{
			"publisher_confirms": true,
			"exchange_exchange_bindings": true,
			"basic.nack": true,
			"consumer_cancel_notify": true,
			"connection.blocked": true,
			"authentication_failure_close": true,
		},
	}
	
	// Create the connection.start method
	startMethod := &protocol.ConnectionStartMethod{
		VersionMajor:    0,
		VersionMinor:    9,
		ServerProperties: serverProperties,
		Mechanisms:      []string{"PLAIN"},
		Locales:         []string{"en_US"},
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
		ChannelMax: 65535, // Maximum number of channels allowed per connection
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

// processConnectionMethod processes connection-level methods
func (s *Server) processConnectionMethod(conn *protocol.Connection, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ConnectionStartOK: // Method ID 11 for connection class
		// Process connection.start-ok
		s.Log.Debug("Received connection.start-ok", zap.String("connection_id", conn.ID))
		return nil
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

func main() {
	// Define command-line flags
	var (
		addr      = flag.String("addr", ":5672", "Address to listen on")
		logLevel  = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		logFile   = flag.String("log-file", "", "Log file path (optional, logs to stdout if not specified)")
		daemonize = flag.Bool("daemonize", false, "Run as daemon (background process)")
	)
	
	flag.Parse()
	
	// Create logger with specified level and output
	logger := createLogger(*logLevel, *logFile)
	defer logger.Sync() // flushes buffer, if any
	
	// If daemonize flag is set, we could implement actual daemonization
	// For now, we'll just log that daemon mode is requested
	if *daemonize {
		logger.Info("Daemon mode requested", zap.Bool("daemonize", *daemonize))
		// In a real implementation, you would:
		// - Fork the process
		// - Redirect stdin, stdout, stderr to /dev/null or log files
		// - Create a new session
		// - Change working directory
		// - Clear file mode creation mask
	}
	
	server := &Server{
		Addr:        *addr,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger,
	}
	
	logger.Info("Starting AMQP server...", 
		zap.String("address", *addr),
		zap.Bool("daemonize", *daemonize),
		zap.String("log_level", *logLevel),
		zap.String("log_file", *logFile))
	
	if err := server.Start(); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}

func createLogger(level string, filePath string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zap.DebugLevel
	case "info":
		zapLevel = zap.InfoLevel
	case "warn":
		zapLevel = zap.WarnLevel
	case "error":
		zapLevel = zap.ErrorLevel
	default:
		zapLevel = zap.InfoLevel
	}
	
	var w zapcore.WriteSyncer
	if filePath != "" {
		// Create a file logger
		f, err := os.Create(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log file: %v\n", err)
			os.Exit(1)
		}
		w = zapcore.AddSync(f)
	} else {
		// Use stdout
		w = zapcore.AddSync(os.Stdout)
	}
	
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	
	core := zapcore.NewCore(encoder, w, zapLevel)
	
	return zap.New(core, zap.AddCaller())
}