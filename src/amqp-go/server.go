package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
}

// NewServer creates a new AMQP server
func NewServer(addr string) *Server {
	logger, _ := zap.NewProduction()
	return &Server{
		Addr:        addr,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger,
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
	switch {
	case classID == 20: // Channel class
		return s.processChannelSpecificMethod(conn, frame.Channel, methodID, frame.Payload[4:])
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