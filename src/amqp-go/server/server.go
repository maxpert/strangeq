package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

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
	metricsCancel      context.CancelFunc

	messagesPublished atomic.Int64
	messagesDelivered atomic.Int64
	bytesReceived     atomic.Int64
	bytesSent         atomic.Int64
}

// NewServer creates a new AMQP server with default storage
func NewServer(addr string) *Server {
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	// Storage is always persistent - use default path from config

	serverBuilder := NewServerBuilder().WithConfig(cfg)
	srv, err := serverBuilder.Build()
	if err != nil {
		// Fallback to minimal configuration
		logger, _ := zap.NewProduction()
		return &Server{
			Addr:             addr,
			Connections:      make(map[string]*protocol.Connection),
			Log:              logger,
			MetricsCollector: &NoOpMetricsCollector{},
			StartTime:        time.Now(),
		}
	}
	return srv
}

// Start starts the AMQP server
func (s *Server) Start() error {
	listener, err := s.createListener()
	if err != nil {
		return err
	}

	s.Mutex.Lock()
	s.Listener = listener
	s.Mutex.Unlock()

	if s.Config.Security.TLSEnabled {
		s.Log.Info("AMQP server listening (TLS)", zap.String("addr", s.Addr))
	} else {
		s.Log.Info("AMQP server listening", zap.String("addr", s.Addr))
	}

	// Start system metrics collection in background
	if s.MetricsCollector != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.Mutex.Lock()
		s.metricsCancel = cancel
		s.Mutex.Unlock()
		go s.startSystemMetricsCollection(ctx)
		s.Log.Info("Started system metrics collection")
	}

	return s.acceptLoop()
}

// createListener creates the appropriate listener based on TLS configuration.
func (s *Server) createListener() (net.Listener, error) {
	if s.Config.Security.TLSEnabled {
		tlsCfg, err := loadTLSConfig(s.Config.Security)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
		return createTLSListener(s.Addr, tlsCfg)
	}
	return net.Listen("tcp", s.Addr)
}

// IsListening returns true if the server listener has been created and is ready
// to accept connections.
func (s *Server) IsListening() bool {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	return s.Listener != nil && !s.Shutdown
}

// acceptLoop accepts connections in a loop until shutdown.
func (s *Server) acceptLoop() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			s.Mutex.RLock()
			shutdown := s.Shutdown
			s.Mutex.RUnlock()
			if shutdown {
				return nil
			}
			s.Log.Error("Error accepting connection", zap.Error(err))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		go s.handleConnection(conn)
	}
}

// underlyingTCPConn returns the *net.TCPConn beneath conn, unwrapping a
// *tls.Conn if necessary, or nil if conn is not TCP-backed (e.g. a net.Pipe in
// tests). tls.Conn.NetConn exposes the socket the TLS session runs over.
func underlyingTCPConn(conn net.Conn) *net.TCPConn {
	switch c := conn.(type) {
	case *net.TCPConn:
		return c
	case *tls.Conn:
		if inner, ok := c.NetConn().(*net.TCPConn); ok {
			return inner
		}
	}
	return nil
}

// tuneSocket applies TCP-level tuning to a freshly accepted connection (SQ-2):
//   - TCP_NODELAY (disable Nagle) so small confirm/RPC frames flush immediately
//     instead of waiting to coalesce. Nagle interacting with the peer's delayed
//     ACK can stall a publish-confirm round trip up to ~40ms.
//   - SO_RCVBUF / SO_SNDBUF sized from Network.Read/WriteBufferSize so the
//     kernel can hold larger in-flight windows under throughput bursts.
//
// It is a no-op for non-TCP connections. Failures are logged, not fatal — the
// connection is still usable, just untuned.
func (s *Server) tuneSocket(conn net.Conn) {
	tcp := underlyingTCPConn(conn)
	if tcp == nil {
		return
	}
	if err := tcp.SetNoDelay(true); err != nil {
		s.Log.Warn("SetNoDelay failed", zap.Error(err))
	}
	if n := s.Config.Network.ReadBufferSize; n > 0 {
		if err := tcp.SetReadBuffer(n); err != nil {
			s.Log.Warn("SetReadBuffer failed", zap.Int("bytes", n), zap.Error(err))
		}
	}
	if n := s.Config.Network.WriteBufferSize; n > 0 {
		if err := tcp.SetWriteBuffer(n); err != nil {
			s.Log.Warn("SetWriteBuffer failed", zap.Int("bytes", n), zap.Error(err))
		}
	}
}

// handleConnection handles a new client connection
func (s *Server) handleConnection(conn net.Conn) {
	// Tune the socket before the handshake so NoDelay/buffer sizing apply to the
	// whole connection lifetime, including connection.start-ok latency.
	s.tuneSocket(conn)

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

	// Create a new connection instance. All frame reads for this connection are
	// coalesced through a bufio.Reader (SQ-1); the 8-byte protocol header above
	// was read raw before the buffer existed, so no buffered bytes are lost.
	connection := protocol.NewConnectionWithReadBuffer(conn, s.Config.Network.ReadCoalesceBufferSize)

	// Add to server's connections, enforcing MaxConnections
	s.Mutex.Lock()
	if s.Shutdown {
		s.Mutex.Unlock()
		conn.Close()
		return
	}
	if s.Config.Network.MaxConnections > 0 && len(s.Connections) >= s.Config.Network.MaxConnections {
		s.Mutex.Unlock()
		s.Log.Warn("Max connections exceeded, refusing connection",
			zap.String("connection_id", connection.ID),
			zap.Int("max", s.Config.Network.MaxConnections))
		conn.Close()
		return
	}
	s.Connections[connection.ID] = connection
	s.Mutex.Unlock()

	// Record connection created
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordConnectionCreated()
	}

	// Process frames for this connection
	// Note: consumerDeliveryLoop is started later in processConnectionFrames
	// after the handshake completes (line 312)
	s.processConnectionFrames(connection)

	s.cleanupConnection(connection)
}

// cleanupConnection tears down all per-channel state for a connection:
// cancels consumers, closes channels, cleans up transaction state, and
// removes the connection from the server's connection map.
func (s *Server) cleanupConnection(connection *protocol.Connection) {
	// Clean up on connection close - iterate all channels and cancel consumers
	connection.Channels.Range(func(key, value interface{}) bool {
		channel := value.(*protocol.Channel)
		channel.Mutex.Lock()
		// Collect consumer tags to unregister
		consumerTags := make([]string, 0, len(channel.Consumers))
		for consumerTag := range channel.Consumers {
			consumerTags = append(consumerTags, consumerTag)
		}
		channel.Consumers = make(map[string]*protocol.Consumer)
		channel.Closed = true
		channel.Mutex.Unlock()

		connection.ChannelCount.Add(-1)

		// Clean up transaction state for this channel to prevent
		// cross-connection state leak where stale tx state keyed by
		// channelID persists after the connection is gone.
		if s.TransactionManager != nil {
			s.TransactionManager.Close(channel.ID)
		}

		// Unregister from broker (stops poll goroutines)
		if s.Broker != nil {
			for _, consumerTag := range consumerTags {
				err := s.Broker.UnregisterConsumer(consumerTag)
				if err != nil {
					s.Log.Warn("Failed to unregister consumer on connection close",
						zap.String("consumer_tag", consumerTag),
						zap.String("connection_id", connection.ID),
						zap.Error(err))
				} else {
					s.Log.Debug("Unregistered consumer on connection close",
						zap.String("consumer_tag", consumerTag),
						zap.String("connection_id", connection.ID))
				}
			}
		}

		return true // continue iteration
	})

	// Requeue any orphaned basic.get deliveries (no_ack=false, unacked)
	if s.Broker != nil {
		s.Broker.RequeueAllGetDeliveries()
	}

	if s.Broker != nil {
		if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
			ownedQueues := sb.broker.GetQueuesOwnedByConnection(connection.ID)
			for _, qName := range ownedQueues {
				if _, err := s.Broker.DeleteQueue(qName, false, false); err != nil {
					s.Log.Warn("Failed to delete exclusive queue on connection close",
						zap.String("queue", qName),
						zap.String("connection_id", connection.ID),
						zap.Error(err))
				} else {
					s.Log.Debug("Deleted exclusive queue on connection close",
						zap.String("queue", qName),
						zap.String("connection_id", connection.ID))
				}
			}
		}
	}

	s.Mutex.Lock()
	delete(s.Connections, connection.ID)
	s.Mutex.Unlock()

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordConnectionClosed()
	}

	s.updateConsumersTotal()
}

// negotiateUint16 returns the minimum of server and client values, where 0
// means unlimited (the non-zero value wins; if both are 0, returns 0).
func negotiateUint16(server, client uint16) uint16 {
	if server == 0 {
		return client
	}
	if client == 0 {
		return server
	}
	if server < client {
		return server
	}
	return client
}

// negotiateUint32 returns the minimum of server and client values, where 0
// means unlimited (the non-zero value wins; if both are 0, returns 0).
func negotiateUint32(server, client uint32) uint32 {
	if server == 0 {
		return client
	}
	if client == 0 {
		return server
	}
	if server < client {
		return server
	}
	return client
}

// processConnectionFrames reads and processes frames from a connection
func (s *Server) processConnectionFrames(conn *protocol.Connection) {
	defer conn.Conn.Close()

	handshakeCap := uint32(131072 + 8)

	// Send connection start frame
	if err := s.sendConnectionStart(conn); err != nil {
		s.Log.Error("Error sending connection start", zap.Error(err))
		return
	}

	conn.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	frame, err := protocol.ReadFrameOptimizedWithLimit(conn.Reader, handshakeCap)
	if err != nil {
		s.Log.Error("Error reading connection.start-ok", zap.Error(err))
		return
	}
	conn.TouchActivity()

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
	conn.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	frame, err = protocol.ReadFrameOptimizedWithLimit(conn.Reader, handshakeCap)
	if err != nil {
		s.Log.Error("Error reading connection.tune-ok", zap.Error(err))
		return
	}
	conn.TouchActivity()

	if frame.Type != protocol.FrameMethod {
		s.Log.Error("Expected method frame", zap.Int("type", int(frame.Type)))
		return
	}

	// Parse tune-ok payload and store negotiated values on conn
	if len(frame.Payload) >= 4 {
		clientChMax, clientFrameMax, clientHeartbeat, parseErr := parseConnectionTuneOK(frame.Payload[4:])
		if parseErr != nil {
			s.Log.Error("Failed to parse connection.tune-ok", zap.Error(parseErr))
			return
		}

		serverChMax := uint16(s.Config.Server.MaxChannelsPerConnection)
		serverFrameMax := uint32(s.Config.Server.MaxFrameSize)
		serverHeartbeat := uint16(s.Config.Network.HeartbeatIntervalMS / 1000)

		negotiatedChMax := negotiateUint16(serverChMax, clientChMax)
		negotiatedFrameMax := negotiateUint32(serverFrameMax, clientFrameMax)
		negotiatedHeartbeat := negotiateUint16(serverHeartbeat, clientHeartbeat)

		conn.MaxChannels = negotiatedChMax
		conn.MaxFrameSize = negotiatedFrameMax
		conn.HeartbeatSec.Store(uint32(negotiatedHeartbeat))

		s.Log.Debug("Negotiated connection parameters",
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_max", negotiatedChMax),
			zap.Uint32("frame_max", negotiatedFrameMax),
			zap.Uint16("heartbeat", negotiatedHeartbeat))
	}

	// Wait for connection.open from client
	conn.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	frame, err = protocol.ReadFrameOptimizedWithLimit(conn.Reader, handshakeCap)
	if err != nil {
		s.Log.Error("Error reading connection.open", zap.Error(err))
		return
	}
	conn.TouchActivity()

	if frame.Type != protocol.FrameMethod {
		s.Log.Error("Expected method frame", zap.Int("type", int(frame.Type)))
		return
	}

	// Process connection.open method
	if err := s.processConnectionOpen(conn, frame); err != nil {
		s.Log.Error("Error processing connection.open", zap.Error(err))
		return
	}

	// Clear handshake deadline; readFrames will manage read deadlines for heartbeats
	conn.Conn.SetReadDeadline(time.Time{})

	// At this point, the connection handshake is complete
	s.Log.Info("Connection handshake completed", zap.String("connection_id", conn.ID))

	// Reader/processor separation: Reader enqueues frames, processor handles them
	// This allows the reader to keep draining TCP socket even when processor blocks on fsync
	readerDone := make(chan struct{})
	go s.readFrames(conn, readerDone)

	// Start frame processor goroutine (can block on WAL fsync without affecting reader)
	processorDone := make(chan struct{})
	go s.processFrames(conn, processorDone)

	// Start ACK processor goroutine (handles basic.ack/nack/reject off the frame processor)
	ackProcessorDone := make(chan struct{})
	go s.ackProcessor(conn, ackProcessorDone)

	// Start heartbeat sender goroutine (sends heartbeat frames to client periodically)
	heartbeatSenderDone := make(chan struct{})
	go s.sendHeartbeats(conn, heartbeatSenderDone)

	// Start consumer delivery loop
	consumerDeliveryDone := make(chan struct{})
	conn.ConsumersDirty.Store(true)
	go s.consumerDeliveryLoop(conn, consumerDeliveryDone)

	// Wait for any goroutine to finish (connection close or error)
	select {
	case <-readerDone:
		s.Log.Info("Frame reader completed", zap.String("connection_id", conn.ID))
	case <-processorDone:
		s.Log.Info("Frame processor completed", zap.String("connection_id", conn.ID))
	case <-heartbeatSenderDone:
		s.Log.Info("Heartbeat sender completed", zap.String("connection_id", conn.ID))
	case <-consumerDeliveryDone:
		s.Log.Info("Consumer delivery completed", zap.String("connection_id", conn.ID))
	case <-ackProcessorDone:
		s.Log.Info("ACK processor completed", zap.String("connection_id", conn.ID))
	}

	// Close the connection to unblock reader and processor
	conn.Conn.Close()
	// Signal all goroutines that the connection is shutting down.
	// This unblocks processFrame if it's waiting to send to AckQueue.
	close(conn.Done)

	// Wait for BOTH senders to ackQueue to exit before closing it. The frame
	// processor routes acks it dequeues, and (since the SQ-0 fix) the reader
	// diverts acks directly. processFrames may exit on error before the reader
	// closes FrameQueue, so we must explicitly wait for the reader too;
	// otherwise a late reader ack-send could hit a closed channel and panic.
	<-processorDone
	<-readerDone
	// Close ackQueue so ackProcessor drains remaining frames and exits
	close(conn.AckQueue)
	<-ackProcessorDone
}

// readFrames reads frames from TCP connection and enqueues them for processing
// This goroutine NEVER blocks on frame processing - it only reads and enqueues
func (s *Server) readFrames(conn *protocol.Connection, done chan struct{}) {
	defer close(done)
	defer close(conn.FrameQueue) // Signal processor to stop

	// pending holds non-ack frames that could not be handed to processFrames
	// without blocking, because its FrameQueue is full while a basic.publish is
	// parked in QueueState.WaitForCapacity. Buffering them here (in order) keeps
	// the reader draining the socket, so it can keep diverting acks — the very
	// frames that release the prefetch gate and drain the queue, lifting the
	// backpressure that stalls processFrames. Without this, the reader would
	// block on the full FrameQueue, acks queued behind the parked publish on
	// the same connection would never be read, and a client that both publishes
	// and consumes over one connection would deadlock (SQ-0 secondary effect).
	// It drains back to nil as soon as processFrames makes progress.
	//
	// SQ-0 step 6: this buffer is BOUNDED by two per-connection mechanisms
	// (design §6.1). When its byte footprint crosses ReaderOverflowFlowBytes we
	// assert channel.flow(active=false) to the client (best-effort ask to pause
	// publishing) and resume with channel.flow(active=true) when it fully
	// drains; if it exceeds the hard ReaderOverflowHardCapBytes cap (a client
	// ignoring flow, or a pure-publish flood) we close the connection rather
	// than grow memory without bound.
	var pending []*protocol.Frame
	var pendingBytes int64
	flowPaused := false
	flowLimit := s.Config.Network.ReaderOverflowFlowBytes
	hardCap := s.Config.Network.ReaderOverflowHardCapBytes

	// The channel.flow writes are issued from a dedicated sender goroutine, NOT
	// from the reader. The reader must never block — not even on the connection
	// WriteMutex. If it did, a cross-process cycle wedges the connection: a
	// delivery writer holds WriteMutex blocked on a full client TCP window; the
	// client's dispatch goroutine (which would drain that window) is itself
	// blocked sending its flow-ok behind the client publisher's send mutex; the
	// publisher is blocked because OUR reader stopped reading while waiting on
	// WriteMutex. Handing the write to a separate goroutine keeps the reader
	// reading, which unwinds that cycle from the client side. Latest-wins
	// semantics: the reader stores the desired state and pokes the wake channel;
	// the sender collapses bursts of transitions to the newest state.
	flowDesiredActive := &atomic.Bool{}
	flowDesiredActive.Store(true)
	flowWake := make(chan struct{}, 1)
	flowQuit := make(chan struct{})
	defer close(flowQuit)
	go func() {
		lastSent := true
		for {
			select {
			case <-flowWake:
			case <-flowQuit:
				return
			case <-conn.Done:
				return
			}
			want := flowDesiredActive.Load()
			if want == lastSent {
				continue
			}
			if err := s.setConnectionFlow(conn, want); err != nil {
				// Could not deliver the pause signal (client not reading its
				// socket). The reader's hard cap still bounds memory; closing
				// here just accelerates the inevitable teardown.
				s.Log.Warn("reader overflow: channel.flow send failed; closing connection",
					zap.String("connection_id", conn.ID), zap.Bool("active", want), zap.Error(err))
				conn.Closed.Store(true)
				conn.Conn.Close()
				return
			}
			lastSent = want
		}
	}()
	requestFlow := func(active bool) {
		flowDesiredActive.Store(active)
		select {
		case flowWake <- struct{}{}:
		default:
		}
	}

	for {
		// Opportunistically hand buffered frames to processFrames whenever it
		// has drained space. Non-blocking: never wait on a full FrameQueue.
		for len(pending) > 0 {
			// Capture Size before the send: once processFrames receives the
			// frame it may pool it, and the recycled frame can be overwritten
			// while we still hold the stale reference.
			head := pending[0]
			headSize := int64(head.Size)
			select {
			case conn.FrameQueue <- head:
				pendingBytes -= headSize
				pending[0] = nil
				pending = pending[1:]
				continue
			case <-conn.Done:
				return
			default:
			}
			break
		}
		if len(pending) == 0 {
			pending = nil // release backing array once fully drained
			pendingBytes = 0
			// Backlog cleared: lift the earlier flow-off signal so the client
			// may resume publishing.
			if flowPaused {
				flowPaused = false
				requestFlow(true)
			}
		}

		hb := conn.HeartbeatSec.Load()
		if hb > 0 {
			conn.Conn.SetReadDeadline(time.Now().Add(time.Duration(2*hb) * time.Second))
		}

		maxSize := conn.MaxFrameSize
		if maxSize == 0 {
			maxSize = protocol.MaxInboundFrameSize
		}
		frame, err := protocol.ReadFrameOptimizedWithLimit(conn.Reader, maxSize)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if hb > 0 {
					s.Log.Info("heartbeat missed, closing dead connection",
						zap.String("connection_id", conn.ID))
				}
			} else if err.Error() == "EOF" {
				s.Log.Info("Connection closed by client", zap.String("connection_id", conn.ID))
			} else {
				s.Log.Error("Error reading frame from connection",
					zap.String("connection_id", conn.ID),
					zap.Error(err))
				if s.MetricsCollector != nil {
					s.MetricsCollector.RecordConnectionError("read")
				}
			}

			conn.Closed.Store(true)
			return
		}

		conn.TouchActivity()

		s.bytesReceived.Add(int64(frame.Size))

		// Divert basic.ack/nack/reject straight to the ACK processor,
		// bypassing FrameQueue and the (potentially publish-blocked)
		// processFrames goroutine. A basic.publish that hits queue
		// backpressure blocks processFrames in QueueState.WaitForCapacity;
		// if acks had to wait behind it in FrameQueue, the very acks that
		// release the prefetch gate — and thereby drain the queue and lift
		// the backpressure — could never be processed. A client that
		// publishes and consumes over one connection would then deadlock.
		// ackProcessor never blocks on broker backpressure, so routing acks
		// here keeps the backpressure-relief path permanently live. (Acks
		// were already handled on a separate goroutine, so this introduces
		// no new reordering — it only moves the diversion point earlier.)
		if isAckFrame(frame) {
			select {
			case conn.AckQueue <- frame:
			case <-conn.Done:
				protocol.PutFrame(frame)
				return
			}
			continue
		}

		// Enqueue for processFrames. Try a non-blocking direct hand-off only when
		// nothing is buffered (buffering must preserve per-channel frame order —
		// a publish's method, header and body frames must stay contiguous). When
		// FrameQueue is full, buffer instead; the reader must never block here or
		// acks stall behind it.
		if len(pending) == 0 {
			select {
			case conn.FrameQueue <- frame:
				continue // queued directly
			case <-conn.Done:
				protocol.PutFrame(frame)
				return
			default:
				// FrameQueue full — fall through to bounded buffering.
			}
		}
		pending = append(pending, frame)
		pendingBytes += int64(frame.Size)

		// Hard cap: a client ignoring flow, or flooding publishes while never
		// consuming, must not grow reader memory without bound. Close it.
		if hardCap > 0 && pendingBytes > hardCap {
			s.Log.Warn("reader overflow exceeded hard cap; closing connection",
				zap.String("connection_id", conn.ID),
				zap.Int64("pending_bytes", pendingBytes),
				zap.Int64("hard_cap_bytes", hardCap))
			if s.MetricsCollector != nil {
				s.MetricsCollector.RecordConnectionError("reader_overflow")
			}
			conn.Closed.Store(true)
			conn.Conn.Close()
			return
		}

		// Soft threshold: ask the client to pause publishing via channel.flow.
		// Best-effort, delivered by the flow-sender goroutine so the reader
		// itself never blocks on the write (or on the WriteMutex). A client that
		// ignores the signal keeps growing the backlog into the hard cap above.
		if !flowPaused && flowLimit > 0 && pendingBytes >= flowLimit {
			flowPaused = true
			requestFlow(false)
			s.Log.Info("reader overflow: requesting channel.flow(false)",
				zap.String("connection_id", conn.ID),
				zap.Int64("pending_bytes", pendingBytes))
		}
	}
}

// processFrames processes frames from the queue
// This goroutine CAN block on WAL fsync without affecting the reader
func (s *Server) processFrames(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	for {
		select {
		case frame, ok := <-conn.FrameQueue:
			if !ok {
				s.Log.Info("Frame queue closed", zap.String("connection_id", conn.ID))
				return
			}

			if frame.Type == protocol.FrameHeartbeat {
				protocol.PutFrame(frame)
				continue
			}

			// Capture before processFrame: it may transfer frame ownership
			// (AckQueue hand-off) or we pool it right after — either way the
			// next owner can overwrite the frame while we still hold the ref.
			frameChannel := frame.Channel
			pooled, err := s.processFrame(conn, frame)
			if pooled {
				protocol.PutFrame(frame)
			}
			if err != nil {
				if errors.Is(err, errClientRequestedClose) {
					s.Log.Info("Connection closed by client",
						zap.String("connection_id", conn.ID))
					conn.Closed.Store(true)
					return
				}
				s.Log.Error("Error processing frame",
					zap.String("connection_id", conn.ID),
					zap.Uint16("channel", frameChannel),
					zap.Error(err))
				if s.MetricsCollector != nil {
					if frameChannel != 0 {
						s.MetricsCollector.RecordChannelError("method_processing")
					}
					s.MetricsCollector.RecordConnectionError("frame_processing")
				}
				conn.Closed.Store(true)
				return
			}

		case <-done:
			return
		}
	}
}

// processFrame processes a single frame directly (no mailbox queuing).
// Returns (pooled, error): pooled=true means the frame was consumed and the
// caller may return it to the frame pool; pooled=false means the frame was
// handed off to another goroutine (e.g., AckQueue) and must NOT be pooled.
func (s *Server) processFrame(conn *protocol.Connection, frame *protocol.Frame) (bool, error) {
	switch frame.Type {
	case protocol.FrameMethod:
		if isAckFrame(frame) {
			select {
			case conn.AckQueue <- frame:
				return false, nil
			case <-conn.Done:
				return true, nil
			}
		}
		return true, s.processMethodFrame(conn, frame)
	case protocol.FrameHeader:
		return true, s.processHeaderFrame(conn, frame)
	case protocol.FrameBody:
		return true, s.processBodyFrame(conn, frame)
	case protocol.FrameHeartbeat:
		return true, nil
	default:
		s.Log.Warn("Unknown frame type",
			zap.Int("type", int(frame.Type)),
			zap.String("connection_id", conn.ID))
		return true, nil
	}
}

func isAckFrame(frame *protocol.Frame) bool {
	if frame.Type != protocol.FrameMethod || len(frame.Payload) < 4 {
		return false
	}
	classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
	if classID != 60 {
		return false
	}
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
	switch methodID {
	case protocol.BasicAck, protocol.BasicReject, protocol.BasicNack:
		return true
	default:
		return false
	}
}

func (s *Server) processAckFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	if len(frame.Payload) < 4 {
		return fmt.Errorf("ack frame payload too short")
	}
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
	payload := frame.Payload[4:]
	switch methodID {
	case protocol.BasicAck:
		return s.handleBasicAck(conn, frame.Channel, payload)
	case protocol.BasicReject:
		return s.handleBasicReject(conn, frame.Channel, payload)
	case protocol.BasicNack:
		return s.handleBasicNack(conn, frame.Channel, payload)
	default:
		return fmt.Errorf("unexpected method %d in ack processor", methodID)
	}
}

func (s *Server) ackProcessor(conn *protocol.Connection, done chan struct{}) {
	defer close(done)
	for frame := range conn.AckQueue {
		// Capture before PutFrame: the pool may recycle the frame to another
		// goroutine's GetFrame, which overwrites it while we still hold the ref.
		frameChannel := frame.Channel
		err := s.processAckFrame(conn, frame)
		protocol.PutFrame(frame)
		if err != nil {
			s.Log.Error("Error processing ACK frame",
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frameChannel),
				zap.Error(err))
			if s.MetricsCollector != nil {
				s.MetricsCollector.RecordConnectionError("ack_processing")
			}
			conn.Closed.Store(true)
			return
		}
	}
}

// sendHeartbeats periodically sends heartbeat frames to the client
// This keeps the connection alive independently of frame processing
// RabbitMQ sends heartbeats every (heartbeat_timeout / 2) seconds
func (s *Server) sendHeartbeats(conn *protocol.Connection, done chan struct{}) {
	defer close(done)

	hb := conn.HeartbeatSec.Load()
	if hb == 0 {
		return
	}

	heartbeatInterval := time.Duration(hb) * time.Second / 2
	if heartbeatInterval < 1*time.Second {
		heartbeatInterval = 1 * time.Second
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if connection is closed
			if conn.Closed.Load() {
				s.Log.Debug("Connection closed, stopping heartbeat sender", zap.String("connection_id", conn.ID))
				return
			}

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
				conn.Closed.Store(true)

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
	case 85: // Confirm class
		return s.processConfirmMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	case 90: // Transaction class
		return s.processTransactionMethod(conn, frame.Channel, methodID, frame.Payload[4:])
	default:
		s.Log.Warn("Unknown class ID",
			zap.Uint16("class_id", classID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown class ID: %d", classID)
	}
}

// Stop stops the server gracefully
func (s *Server) Stop() error {
	s.Mutex.Lock()
	s.Shutdown = true
	ln := s.Listener
	cancel := s.metricsCancel
	s.Mutex.Unlock()

	if cancel != nil {
		cancel()
	}

	if ln != nil {
		return ln.Close()
	}
	return nil
}

// StartWithQuitChannel starts the server and returns a quit channel that can be used to stop it
func (s *Server) StartWithQuitChannel(quit <-chan struct{}) error {
	listener, err := s.createListener()
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	s.Mutex.Lock()
	s.Listener = listener
	s.Mutex.Unlock()

	if s.Config.Security.TLSEnabled {
		s.Log.Info("AMQP server listening (TLS)", zap.String("addr", s.Addr))
	} else {
		s.Log.Info("AMQP server listening", zap.String("addr", s.Addr))
	}

	// Start system metrics collection in background
	if s.MetricsCollector != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.Mutex.Lock()
		s.metricsCancel = cancel
		s.Mutex.Unlock()
		go s.startSystemMetricsCollection(ctx)
		s.Log.Info("Started system metrics collection")
	}

	// Accept connections
	for {
		// Use a non-blocking approach with select to check for quit signal
		connChan := make(chan net.Conn, 1)
		errChan := make(chan error, 1)

		go func() {
			conn, err := listener.Accept()
			connChan <- conn
			errChan <- err
		}()

		select {
		case conn := <-connChan:
			err := <-errChan
			if err != nil {
				s.Mutex.RLock()
				shutdown := s.Shutdown
				s.Mutex.RUnlock()
				if shutdown {
					return nil
				}
				s.Log.Error("Error accepting connection", zap.Error(err))
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Handle the connection in a goroutine
			go s.handleConnection(conn)
		case <-quit:
			s.Log.Info("Server shutdown requested")
			s.Mutex.Lock()
			s.Shutdown = true
			cancel := s.metricsCancel
			s.Mutex.Unlock()
			if cancel != nil {
				cancel()
			}
			listener.Close()
			return nil
		}
	}
}
