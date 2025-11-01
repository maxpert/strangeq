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
	MetricsCollector   MetricsCollector
	StartTime          time.Time
}

// NewServer creates a new AMQP server
func NewServer(addr string) *Server {
	logger, _ := zap.NewProduction()
	return &Server{
		Addr:             addr,
		Connections:      make(map[string]*protocol.Connection),
		Log:              logger,
		Broker:           NewOriginalBrokerAdapter(broker.NewBroker()),
		MetricsCollector: &NoOpMetricsCollector{},
		StartTime:        time.Now(),
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

	// Record connection created
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordConnectionCreated()
	}

	// Start consumer delivery loop for this connection
	go s.consumerDeliveryLoop(connection)

	// Process frames for this connection
	s.processConnectionFrames(connection)

	// Clean up on connection close
	s.Mutex.Lock()
	delete(s.Connections, connection.ID)
	s.Mutex.Unlock()

	// Record connection closed
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordConnectionClosed()
	}
}

// NOTE: memoryMonitor implementation moved to server/memory_monitor.go for Phase 2 (lock-free unbounded channels)

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

	// Process connection.start-ok and perform authentication
	if len(frame.Payload) >= 4 {
		classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
		methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])

		if classID == 10 && methodID == protocol.ConnectionStartOK {
			// Handle authentication
			if err := s.handleConnectionStartOK(conn, frame.Payload[4:]); err != nil {
				s.Log.Error("Authentication failed",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
				return
			}
		}
	}

	// Send tune parameters
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

	// RabbitMQ-style architecture: Three separate mailboxes with handlers, reader, and heartbeat sender
	// This ensures connection stays alive even during backpressure

	// Start frame reader goroutine (reads from TCP → routes to mailboxes)
	readerDone := make(chan struct{})
	go s.readFrames(conn, readerDone)

	// Start three handler goroutines (RabbitMQ-style: one per mailbox)
	heartbeatHandlerDone := make(chan struct{})
	go s.processHeartbeatMailbox(conn, heartbeatHandlerDone)

	channelHandlerDone := make(chan struct{})
	go s.processChannelMailbox(conn, channelHandlerDone)

	connectionHandlerDone := make(chan struct{})
	go s.processConnectionMailbox(conn, connectionHandlerDone)

	// Start heartbeat sender goroutine (sends heartbeat frames to client periodically)
	// This keeps connection alive independently of frame processing
	heartbeatSenderDone := make(chan struct{})
	go s.sendHeartbeats(conn, heartbeatSenderDone)

	// Wait for any goroutine to finish (connection close or error)
	select {
	case <-readerDone:
		s.Log.Info("Frame reader completed", zap.String("connection_id", conn.ID))
	case <-heartbeatHandlerDone:
		s.Log.Info("Heartbeat handler completed", zap.String("connection_id", conn.ID))
	case <-channelHandlerDone:
		s.Log.Info("Channel handler completed", zap.String("connection_id", conn.ID))
	case <-connectionHandlerDone:
		s.Log.Info("Connection handler completed", zap.String("connection_id", conn.ID))
	case <-heartbeatSenderDone:
		s.Log.Info("Heartbeat sender completed", zap.String("connection_id", conn.ID))
	}

	// Close mailboxes to signal handlers to stop
	conn.Mailboxes.Close()
}

// readFrames reads frames from TCP connection and enqueues them to FrameQueue
// This goroutine NEVER blocks on frame processing - only on enqueue when queue is full
// RabbitMQ-style: When queue fills, blocks reader → blocks TCP → natural backpressure
func (s *Server) readFrames(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	for {
		// Read frame from TCP connection
		frame, err := protocol.ReadFrame(conn.Conn)
		if err != nil {
			if err.Error() == "EOF" {
				s.Log.Info("Connection closed by client", zap.String("connection_id", conn.ID))
			} else {
				s.Log.Error("Error reading frame from connection",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
			}

			// Mark connection as closed
			conn.Mutex.Lock()
			conn.Closed = true
			conn.Mutex.Unlock()

			return
		}

		// Route frame to appropriate mailbox (RabbitMQ-style architecture)
		// Reader NEVER blocks - unbounded queues ensure frames always enqueue
		//
		// Frame routing:
		// - Heartbeat frames → Heartbeat mailbox (processed independently)
		// - Method/Header/Body frames on channel 0 → Connection mailbox (connection control)
		// - Method/Header/Body frames on channel > 0 → Channel mailbox (data frames)
		//
		// This ensures:
		// 1. Heartbeats are always processed (never starved)
		// 2. Connection control is separated from data processing
		// 3. Data frames don't block heartbeat or connection control
		if frame.Type == protocol.FrameHeartbeat {
			err = conn.Mailboxes.Heartbeat.Enqueue(frame)
		} else if frame.Channel == 0 {
			// Channel 0 is reserved for connection control
			err = conn.Mailboxes.Connection.Enqueue(frame)
		} else {
			// All other frames are channel data frames
			err = conn.Mailboxes.Channel.Enqueue(frame)
		}

		if err != nil {
			// Mailbox closed, connection is closing
			return
		}
	}
}

// processHeartbeatMailbox processes frames from the heartbeat mailbox (RabbitMQ-style)
// This handler is independent and ensures heartbeats are never starved
func (s *Server) processHeartbeatMailbox(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	for {
		// Dequeue next frame from heartbeat mailbox
		_, ok := conn.Mailboxes.Heartbeat.Dequeue()
		if !ok {
			// Mailbox closed, connection is closing
			s.Log.Info("Heartbeat mailbox closed", zap.String("connection_id", conn.ID))
			return
		}

		// Heartbeat frames don't need processing - just log them
		s.Log.Debug("Received heartbeat", zap.String("connection_id", conn.ID))
	}
}

// processConnectionMailbox processes frames from the connection mailbox (RabbitMQ-style)
// This handler processes channel 0 frames (connection control)
func (s *Server) processConnectionMailbox(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	for {
		// Dequeue next frame from connection mailbox
		frame, ok := conn.Mailboxes.Connection.Dequeue()
		if !ok {
			// Mailbox closed, connection is closing
			s.Log.Info("Connection mailbox closed", zap.String("connection_id", conn.ID))
			return
		}

		// Process connection frames (same logic as channel frames, just on channel 0)
		if err := s.processFrame(conn, frame); err != nil {
			s.Log.Error("Error processing connection frame",
				zap.String("connection_id", conn.ID),
				zap.Error(err))

			// Mark connection as closed
			conn.Mutex.Lock()
			conn.Closed = true
			conn.Mutex.Unlock()

			return
		}
	}
}

// processChannelMailbox processes frames from the channel mailbox (RabbitMQ-style)
// This handler processes data frames (channel > 0) and MAY block on body frames
func (s *Server) processChannelMailbox(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	for {
		// Dequeue next frame from channel mailbox
		frame, ok := conn.Mailboxes.Channel.Dequeue()
		if !ok {
			// Mailbox closed, connection is closing
			s.Log.Info("Channel mailbox closed", zap.String("connection_id", conn.ID))
			return
		}

		// Process channel frames
		if err := s.processFrame(conn, frame); err != nil {
			s.Log.Error("Error processing channel frame",
				zap.String("connection_id", conn.ID),
				zap.Error(err))

			// Mark connection as closed
			conn.Mutex.Lock()
			conn.Closed = true
			conn.Mutex.Unlock()

			return
		}
	}
}

// processFrame processes a single frame (common logic for both channel and connection frames)
func (s *Server) processFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	// Process the frame based on its type
	// Process synchronously to maintain frame ordering (AMQP requirement)
	switch frame.Type {
	case protocol.FrameMethod:
		return s.processMethodFrame(conn, frame)
	case protocol.FrameHeader:
		return s.processHeaderFrame(conn, frame)
	case protocol.FrameBody:
		// Process body frames synchronously (MAY block on broker backpressure)
		// This is OK because:
		// 1. Reader goroutine continues reading into mailboxes (including heartbeats)
		// 2. Unbounded mailboxes ensure reader never blocks
		// 3. We maintain AMQP frame ordering requirement
		return s.processBodyFrame(conn, frame)
	case protocol.FrameHeartbeat:
		// Handle heartbeat frames immediately (never blocks)
		// Heartbeats are processed inline to maintain connection liveness
		s.Log.Debug("Heartbeat frame received", zap.String("connection_id", conn.ID))
		return nil
	default:
		s.Log.Warn("Unknown frame type",
			zap.Int("type", int(frame.Type)),
			zap.String("connection_id", conn.ID))
		return nil
	}
}

// sendHeartbeats periodically sends heartbeat frames to the client
// This keeps the connection alive independently of frame processing
// RabbitMQ sends heartbeats every (heartbeat_timeout / 2) seconds
func (s *Server) sendHeartbeats(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	// Send heartbeats very frequently (every 5 seconds) to prevent timeout
	// even under heavy load with WriteMutex contention.
	// TODO: Use negotiated heartbeat value from connection.tune
	heartbeatInterval := 5 * time.Second
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if connection is closed
			conn.Mutex.RLock()
			if conn.Closed {
				conn.Mutex.RUnlock()
				s.Log.Debug("Connection closed, stopping heartbeat sender", zap.String("connection_id", conn.ID))
				return
			}
			conn.Mutex.RUnlock()

			// Send heartbeat frame
			heartbeatFrame := &protocol.Frame{
				Type:    protocol.FrameHeartbeat,
				Channel: 0,
				Payload: []byte{},
			}

			// WriteFrame handles locking internally
			if err := protocol.WriteFrameToConnection(conn, heartbeatFrame); err != nil {
				s.Log.Error("Failed to send heartbeat",
					zap.String("connection_id", conn.ID),
					zap.Error(err))

				// Mark connection as closed
				conn.Mutex.Lock()
				conn.Closed = true
				conn.Mutex.Unlock()

				return
			}

			s.Log.Debug("Sent heartbeat to client", zap.String("connection_id", conn.ID))

		case <-done:
			// Connection is closing
			return
		}
	}
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

// processBasicMethod handles basic-class methods
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
