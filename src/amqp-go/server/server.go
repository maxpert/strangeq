package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqperrors "github.com/maxpert/amqp-go/errors"
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
	ShuttingDown       bool
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

	// _alarmPad separates alarmState from the hot per-message write counters
	// above. alarmState is loaded on every publish (the SQ-12 gate) but the
	// counters above are stored on every publish/deliver; without a cache-line
	// gap the read-mostly alarmState would false-share with those hot writes and
	// bounce the line between cores. One 64-byte cache line of padding.
	_alarmPad [64]byte

	// alarmState is the SQ-12 resource-alarm bitmask: bit0 = memory (AlarmMemory),
	// bit1 = disk (AlarmDisk). Zero means no active alarm. The publish gate is a
	// single atomic load + not-taken branch when unset. W6/SQ-12 drives the bits
	// from the AlarmMonitor; W1 only provides the field and accessor. alarmState
	// itself doubles as the "publishBlocked" flag the reader-pause consults.
	alarmState atomic.Uint32

	// publishedWhileBlocked counts publishes that completed while a resource
	// alarm was active (SQ-12). Only written on the not-taken publish-gate
	// branch (rare), so it does not need hot-path cache-line isolation.
	publishedWhileBlocked atomic.Int64

	// alarm holds the resolved resource-alarm thresholds + samplers (SQ-12), or
	// nil when resource alarms are disabled. When nil the monitor goroutine is
	// never spawned and the broker does zero RSS/Statfs sampling (the unset
	// zero-cost contract). Set once during Build; read-only thereafter.
	alarm *alarmThresholds

	// alarmParkProbe overrides the reader-park liveness re-arm interval (SQ-12).
	// Zero falls back to defaultAlarmParkProbe. Tests set it small; production
	// leaves it at the default.
	alarmParkProbe time.Duration

	// readerBackpressureProbe overrides the reader-backpressure hand-off probe
	// interval (SQ-13). Zero falls back to Config.Network.ReaderBackpressureProbeMS
	// (and then to defaultReaderBackpressureProbe). Tests set it small; production
	// leaves it unset and drives it from config.
	readerBackpressureProbe time.Duration

	// depthLivenessProbe overrides the queue-depth reader-park liveness interval
	// (iteration 2, option A). Zero falls back to depthBackpressureLivenessInterval.
	// Tests set it small; production leaves it at the default.
	depthLivenessProbe time.Duration

	// alarmCancel stops the alarm monitor goroutine on shutdown; nil when the
	// monitor was never started.
	alarmCancel context.CancelFunc

	// unsafeRecovery holds the boot-gating recovery faults that
	// --unsafe-recovery overrode, or nil on a clean boot. It is a PURE APPEND
	// at the end of the struct so every field above — in particular the
	// _alarmPad / alarmState cache-line isolation — keeps its byte offset.
	// Written once during Build, read-only thereafter.
	unsafeRecovery []*interfaces.RecoveryFault

	// unsafeRecoveryCancel stops the degraded-boot nag goroutine; nil unless
	// the broker actually booted degraded, so a clean boot spawns nothing.
	unsafeRecoveryCancel context.CancelFunc

	// generateConsumerTag overrides the source of server-generated consumer
	// tags for THIS server. Nil in production, which is the whole contract:
	// consumerTagGenerator() falls back to protocol.GenerateConsumerTag, so no
	// construction site has to remember to set it. It exists for one reason —
	// with a 128-bit random draw the re-draw branch in claimConsumerTag is
	// unreachable, so "a generated tag cannot collide with a client-supplied
	// tag on the same channel" would be a documented premise rather than an
	// asserted one (canon rule 11). A test substitutes a colliding generator to
	// drive that branch.
	//
	// It is a FIELD, not a package-level var, deliberately. As a process-global
	// it was race-free only for as long as no test in this package called
	// t.Parallel() — a load-bearing premise written down nowhere, inside the
	// very seam that exists to stop premises being written down instead of
	// checked. Per-server scoping removes the premise rather than documenting
	// it. Appended at the end of the struct so no pre-existing field moves.
	generateConsumerTag func() string

	// --- WS-B (append-only; see specs/WS-B-server-safety.md) ---
	pendingHandshakes            atomic.Int64  // sockets accepted but not yet past connection.open-ok
	headerTimeoutOverride        time.Duration // tests; 0 → protocolHeaderTimeout
	handshakeTimeoutOverride     time.Duration // tests; 0 → handshakeTimeout
	pendingLimitOverride         int64         // tests; 0 → max(64, MaxConnections/4)
	writeTimeoutOverride         time.Duration // tests; 0 → defaultWriteTimeout
	closeOKTimeoutOverride       time.Duration // tests; 0 → connectionCloseOKTimeout
	frameQueueByteBudgetOverride int64         // tests; 0 → defaultFrameQueueByteBudget

	// --- WS-I Phase B: header-time fair publish lease (append-only) ---
	limiterStepHook func(step string) // tests only; nil in production (per-step observation)
	leaseCreated    *atomic.Int64     // tests only; nil in production (grant counter)
	leaseReleased   *atomic.Int64     // tests only; nil in production (release counter)
	limiterOnce     sync.Once         // lazily builds limiter (Build or first publish)
	limiter         *PublishLimiter   // the global publish-inflight lease (G)

	runtimes       sync.Map           // *protocol.Connection → *connRuntime
	panics         atomic.Int64       // exposed via PanicCount()
	storage        interfaces.Storage // retained from Build (§11.4)
	ownsStorage    bool               // builder created it (not WithStorage)
	ownsBroker     bool               // builder created the StorageBroker (not WithUnifiedBroker)
	connWG         sync.WaitGroup     // one per handleConnection (dispatchAccepted Adds)
	shutdownOnce   sync.Once
	shutdownErr    error
	storageClosed  atomic.Bool           // set after storage.Close returns (tests/health)
	onConnTornDown func(rt *connRuntime) // tests only; nil in production (cold path, §13)
}

// markUnsafeRecovery records that this process booted with --unsafe-recovery
// after discarding artifacts it could not recover. Called once from Build.
func (s *Server) markUnsafeRecovery(faults []*interfaces.RecoveryFault) {
	s.unsafeRecovery = faults
}

// UnsafeRecoveryFaults reports the artifacts this process discarded at boot.
// Empty on a clean boot. This is the in-process form of the pinned metric.
func (s *Server) UnsafeRecoveryFaults() []*interfaces.RecoveryFault { return s.unsafeRecovery }

// unsafeRecoveryNagInterval is how often a degraded broker re-announces that it
// is degraded.
//
// One startup line is precisely what let two contaminated data directories sit
// unnoticed in this repository for three weeks: nobody reads a log line from
// three weeks ago, and nothing on a dashboard changed. A degraded boot has to
// stay loud for as long as the process runs.
const unsafeRecoveryNagInterval = 5 * time.Minute

// nagUnsafeRecovery re-announces the degraded boot for the lifetime of the
// process. Spawned only when the broker actually booted degraded.
func (s *Server) nagUnsafeRecovery(ctx context.Context) {
	ticker := time.NewTicker(unsafeRecoveryNagInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, f := range s.unsafeRecovery {
				s.Log.Error("THIS BROKER IS RUNNING DEGRADED: it started with "+interfaces.UnsafeRecoveryFlag+
					" after discarding data it could not recover",
					zap.String("outcome", f.Outcome.String()),
					zap.String("stage", f.Stage),
					zap.String("path", f.Artifact),
					zap.String("cost", f.Cost),
					zap.Duration("degraded_for", time.Since(s.StartTime)))
			}
		}
	}
}

// startUnsafeRecoveryNag spawns the degraded-boot nag if and only if this
// process booted degraded. Returns whether it started one.
func (s *Server) startUnsafeRecoveryNag() bool {
	if len(s.unsafeRecovery) == 0 {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Mutex.Lock()
	s.unsafeRecoveryCancel = cancel
	s.Mutex.Unlock()
	go s.nagUnsafeRecovery(ctx)
	return true
}

// consumerTagGenerator returns the generator this server draws server-side
// consumer tags from: the substituted one if a test installed it, otherwise the
// production protocol.GenerateConsumerTag. Read once per basic.consume with an
// empty consumer-tag, so it is off both the per-message and per-ack paths.
func (s *Server) consumerTagGenerator() func() string {
	if s.generateConsumerTag != nil {
		return s.generateConsumerTag
	}
	return protocol.GenerateConsumerTag
}

// Resource-alarm bits stored in Server.alarmState (SQ-12). Part of the Wave 2
// frozen contract: bit0 = memory pressure, bit1 = disk pressure.
const (
	AlarmMemory uint32 = 1 << 0
	AlarmDisk   uint32 = 1 << 1
)

// AlarmState returns the current resource-alarm bitmask (see AlarmMemory /
// AlarmDisk). A single atomic load; safe for concurrent use. The publish hot
// path uses this to gate on active alarms (SQ-12).
func (s *Server) AlarmState() uint32 {
	return s.alarmState.Load()
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

	// The server is actually serving now — the lifecycle only used to reach
	// "running" after Start returned (i.e. on the way down).
	if s.Lifecycle != nil {
		s.Lifecycle.markRunning()
	}

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

	// SQ-12: start the resource-alarm monitor (no-op / not spawned when alarms
	// are disabled — the unset zero-cost contract).
	if s.startAlarmMonitor() {
		s.Log.Info("Started resource-alarm monitor",
			zap.Duration("interval", s.alarm.interval))
	}

	// A broker that booted with --unsafe-recovery stays loud for its whole
	// lifetime. Not spawned on a clean boot.
	if s.startUnsafeRecoveryNag() {
		s.Log.Error("THIS BROKER IS RUNNING DEGRADED: it started with "+interfaces.UnsafeRecoveryFlag+
			" after discarding data it could not recover",
			zap.Int("discarded_artifacts", len(s.unsafeRecovery)))
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
	return s.Listener != nil && !s.ShuttingDown
}

// acceptLoop accepts connections in a loop until shutdown.
func (s *Server) acceptLoop() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			s.Mutex.RLock()
			shutdown := s.ShuttingDown
			s.Mutex.RUnlock()
			if shutdown {
				return nil
			}
			s.Log.Error("Error accepting connection", zap.Error(err))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.dispatchAccepted(conn)
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
	if s.Config.Network.TCPKeepAlive {
		if err := tcp.SetKeepAlive(true); err != nil {
			s.Log.Warn("SetKeepAlive failed", zap.Error(err))
		}
		if ms := s.Config.Network.TCPKeepAliveIntervalMS; ms > 0 {
			if err := tcp.SetKeepAlivePeriod(time.Duration(ms) * time.Millisecond); err != nil {
				s.Log.Warn("SetKeepAlivePeriod failed", zap.Error(err))
			}
		}
	} else {
		// Honour an explicit opt-out; Go's listener enables 15s keepalive by
		// default, so "unset" would silently be on.
		_ = tcp.SetKeepAlive(false)
	}
}

// handleConnection handles a new client connection (§2.4). It owns the
// pre-auth handshake slot from dispatchAccepted until connection.open-ok is
// sent (or it exits), so every unauthenticated socket is bounded.
func (s *Server) handleConnection(conn net.Conn) {
	defer s.connWG.Done()
	slot := &handshakeSlot{s: s}
	defer slot.release()
	defer s.recoverConnection(conn, "handleConnection") // §8; runs before slot.release, after finishConnection

	// Tune the socket before the handshake so NoDelay/buffer sizing apply to the
	// whole connection lifetime, including connection.start-ok latency.
	s.tuneSocket(conn)

	// First, check the protocol header (deadline-bounded; bounds the TLS
	// handshake too).
	if !s.readProtocolHeader(conn) {
		_ = conn.Close()
		return
	}

	// Create a new connection instance. All frame reads for this connection are
	// coalesced through a bufio.Reader (SQ-1); the 8-byte protocol header above
	// was read raw before the buffer existed, so no buffered bytes are lost.
	connection := protocol.NewConnectionWithReadBuffer(conn, s.Config.Network.ReadCoalesceBufferSize)
	rt := s.newConnRuntime(connection, slot)

	// Add to server's connections, enforcing MaxConnections
	s.Mutex.Lock()
	if s.ShuttingDown || (s.Config.Network.MaxConnections > 0 && len(s.Connections) >= s.Config.Network.MaxConnections) {
		s.Mutex.Unlock()
		s.runtimes.Delete(connection)
		if !s.ShuttingDown {
			s.Log.Warn("Max connections exceeded, refusing connection",
				zap.String("connection_id", connection.ID),
				zap.Int("max", s.Config.Network.MaxConnections))
		}
		_ = conn.Close()
		return
	}
	s.Connections[connection.ID] = connection
	s.Mutex.Unlock()

	// Record connection created
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordConnectionCreated()
	}

	// Process frames for this connection
	// Note: consumerDeliveryLoop is started later in processConnectionFrames,
	// after the handshake completes.
	defer s.finishConnection(rt) // cleanupConnection (recover-guarded) + runtimes.Delete + close(rt.torndown)
	s.processConnectionFrames(connection)
}

// cleanupConnection tears down all per-channel state for a connection:
// cancels consumers, closes channels, cleans up transaction state, and
// removes the connection from the server's connection map.
func (s *Server) cleanupConnection(connection *protocol.Connection) {
	// WS-I Phase B: release connection-owned publish leases FIRST, ONCE
	// (leasesRevoked was set at teardown step 3 — this drain is once-only),
	// BEFORE any channel lock or broker call below, so a panic/deadlock later
	// in this function can never leak budget. Broker-owned transferred leases
	// are untouched (they release on WAL completion, even after the
	// connection is gone). The reader and processor have already exited, so
	// this cannot race them.
	if rt, ok := s.runtimes.Load(connection); ok {
		s.revokeAndDrainConnectionLeases(rt.(*connRuntime))
	} else {
		// Bare-connection unit tests (no registered runtime): drain directly.
		for _, le := range connection.LeaseFIFO.Drain() {
			if le != nil {
				le.Release()
			}
		}
		releaseHeaderLease(connection)
		for _, pm := range connection.PendingMessages {
			takeLease(pm)()
		}
	}

	// Clean up on connection close - iterate all channels and cancel consumers
	connection.Channels.Range(func(key, value interface{}) bool {
		channel := value.(*protocol.Channel)
		channel.Mutex.Lock()
		// Collect broker-internal consumer identities to unregister. NOT wire
		// tags: those are only channel-scoped, so unregistering by tag would
		// tear down whichever same-tagged consumer registered last — this
		// connection's teardown killing another connection's consumer.
		consumerIDs := make([]string, 0, len(channel.Consumers))
		for _, consumer := range channel.Consumers {
			consumerIDs = append(consumerIDs, consumer.ID)
		}
		channel.Consumers = make(map[string]*protocol.Consumer)
		channel.Closed = true
		channel.Mutex.Unlock()

		connection.ChannelCount.Add(-1)

		// Clean up transaction state for this channel to prevent
		// cross-connection state leak where stale tx state keyed by
		// channelID persists after the connection is gone. Conn-scoped
		// (D-NEW-1): only THIS connection's (conn, channel) entry is dropped;
		// another connection's same-numbered tx channel is untouched.
		if txm := s.txm(connection); txm != nil {
			txm.Close(channel.ID)
		}

		// Unregister from broker (stops poll goroutines)
		if s.Broker != nil {
			for _, consumerID := range consumerIDs {
				err := s.Broker.UnregisterConsumer(consumerID)
				if err != nil {
					s.Log.Warn("Failed to unregister consumer on connection close",
						zap.String("consumer_id", consumerID),
						zap.String("connection_id", connection.ID),
						zap.Error(err))
				} else {
					s.Log.Debug("Unregistered consumer on connection close",
						zap.String("consumer_id", consumerID),
						zap.String("connection_id", connection.ID))
				}
			}
		}

		return true // continue iteration
	})

	// Requeue this connection's orphaned basic.get deliveries (no_ack=false,
	// unacked) — scoped to the closing connection (WS-B §10).
	if s.Broker != nil {
		s.Broker.RequeueGetDeliveries(connection.ID)
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

// serverHeartbeatSeconds converts Network.HeartbeatIntervalMS to the whole-second
// value AMQP carries, rounding UP so a positive sub-second setting never becomes 0
// (disabled). <=0 means disabled; clamps to 65535.
func (s *Server) serverHeartbeatSeconds() uint16 {
	ms := s.Config.Network.HeartbeatIntervalMS
	if ms <= 0 {
		return 0
	}
	sec := (ms + 999) / 1000
	if sec > math.MaxUint16 {
		sec = math.MaxUint16
	}
	return uint16(sec)
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

// processConnectionFrames reads and processes frames from a connection.
// The handshake is a strict state machine (§1.2): S1 start-ok → authenticate
// → tune, S2 tune-ok → negotiate, S3 open → open-ok. One ABSOLUTE deadline
// covers the whole handshake; every protocol violation sends connection.close
// and waits briefly for close-ok before the socket closes (D5).
func (s *Server) processConnectionFrames(conn *protocol.Connection) {
	defer conn.Conn.Close()

	// Send connection start frame
	if err := s.sendConnectionStart(conn); err != nil {
		s.Log.Error("Error sending connection start", zap.Error(err))
		return
	}

	rt := s.runtimeFor(conn)
	// ONE absolute handshake deadline, measured from the end of the protocol
	// header (§1.2) — not a fresh 30s per step.
	_ = conn.Conn.SetReadDeadline(time.Now().Add(s.handshakeTimeout()))

	// S1: await start-ok, then authenticate. Authentication must yield a
	// non-empty username when auth is enabled (bypass #1/#2: no path reaches
	// sendConnectionTune otherwise).
	payload, err := s.readHandshakeMethod(conn, protocol.ConnectionStartOK, "start-ok")
	if err != nil {
		var cne *ConnectionError
		if errors.As(err, &cne) {
			s.awaitHandshakeCloseOK(conn)
		}
		return
	}
	if err := s.handleConnectionStartOK(conn, payload); err != nil {
		s.Log.Error("Authentication failed",
			zap.String("connection_id", conn.ID),
			zap.Error(err))
		s.awaitHandshakeCloseOK(conn)
		return
	}

	// Send tune parameters
	if err := s.sendConnectionTune(conn); err != nil {
		s.Log.Error("Error sending connection tune", zap.Error(err))
		return
	}

	// S2: await tune-ok, then negotiate.
	payload, err = s.readHandshakeMethod(conn, protocol.ConnectionTuneOK, "tune-ok")
	if err != nil {
		var cne *ConnectionError
		if errors.As(err, &cne) {
			s.awaitHandshakeCloseOK(conn)
		}
		return
	}
	clientChMax, clientFrameMax, clientHeartbeat, parseErr := parseConnectionTuneOK(payload)
	if parseErr != nil {
		s.Log.Error("Failed to parse connection.tune-ok", zap.Error(parseErr))
		_ = s.connectionError(conn, amqperrors.SyntaxError, "SYNTAX_ERROR - malformed connection.tune-ok", 10, 31)
		s.awaitHandshakeCloseOK(conn)
		return
	}

	serverChMax := uint16(s.Config.Server.MaxChannelsPerConnection)
	serverFrameMax := uint32(s.Config.Server.MaxFrameSize)

	negotiatedChMax := negotiateUint16(serverChMax, clientChMax)
	negotiatedFrameMax := negotiateUint32(serverFrameMax, clientFrameMax)
	// AMQP 0-9-1: connection.tune-ok.heartbeat is "the delay … the client
	// wants. Zero means the client does not want a heartbeat." RabbitMQ
	// uses the tune-ok value as-is (§4.4).
	negotiatedHeartbeat := clientHeartbeat

	conn.MaxChannels = negotiatedChMax
	conn.MaxFrameSize = negotiatedFrameMax
	conn.HeartbeatSec.Store(uint32(negotiatedHeartbeat))

	s.Log.Debug("Negotiated connection parameters",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_max", negotiatedChMax),
		zap.Uint32("frame_max", negotiatedFrameMax),
		zap.Uint16("heartbeat", negotiatedHeartbeat))

	// S3: await open, then vhost/authz/loopback checks (+ username re-check).
	payload, err = s.readHandshakeMethod(conn, protocol.ConnectionOpen, "open")
	if err != nil {
		var cne *ConnectionError
		if errors.As(err, &cne) {
			s.awaitHandshakeCloseOK(conn)
		}
		return
	}
	if err := s.processConnectionOpen(conn, payload); err != nil {
		s.Log.Error("Error processing connection.open", zap.Error(err))
		s.awaitHandshakeCloseOK(conn)
		return
	}

	// The handshake is complete: mark it, release the pre-auth slot (§2.3),
	// and clear the handshake deadline; readFrames manages read deadlines for
	// heartbeats from here.
	rt.handshakeDone.Store(true)
	rt.releaseHandshakeSlot()
	_ = conn.Conn.SetReadDeadline(time.Time{})

	// At this point, the connection handshake is complete
	s.Log.Info("Connection handshake completed", zap.String("connection_id", conn.ID))

	// Reader/processor separation: Reader enqueues frames, processor handles them
	// This allows the reader to keep draining TCP socket even when processor blocks on fsync.
	// Every goroutine is registered in rt.wg and panic-recovered via goConn (§8/§13).
	readerDone := make(chan struct{})
	s.goConn(rt, "reader", func() { s.readFrames(rt, readerDone) })

	// Start frame processor goroutine (can block on WAL fsync without affecting reader)
	processorDone := make(chan struct{})
	s.goConn(rt, "processor", func() { s.processFrames(rt, processorDone) })

	// Start ACK processor goroutine (handles basic.ack/nack/reject off the frame processor)
	ackProcessorDone := make(chan struct{})
	s.goConn(rt, "ack-processor", func() { s.ackProcessor(rt, ackProcessorDone) })

	// Start the heartbeat sender ONLY when a heartbeat was negotiated: with
	// hb == 0 there is no sender at all, so it can never become a spurious
	// teardown trigger (P1 heartbeat=0 disconnect).
	if conn.HeartbeatSec.Load() > 0 {
		s.goConn(rt, "heartbeat", func() { s.sendHeartbeats(conn) })
	}

	// Start consumer delivery loop
	conn.ConsumersDirty.Store(true)
	s.goConn(rt, "delivery", func() { s.consumerDeliveryLoop(rt) })

	// Start publisher-confirm flusher (SQ-5: batches basic.ack writes for
	// pipelining confirm-mode publishers)
	confirmFlusherDone := make(chan struct{})
	s.goConn(rt, "confirm-flusher", func() { s.confirmFlusher(conn, confirmFlusherDone) })

	// SQ-12: a freshly handshaked connection has not published yet, so it is
	// consumer-only and must NOT be blocked (RabbitMQ blocks publishers, not
	// consumers) — no emission here and its reader is never paused. It becomes
	// blocked, with the handshake/clear race reconciled, at its first publish
	// under an active alarm (blockPublisherConnection in processCompleteMessage).

	// 1. Trigger: ONLY input-side goroutines. Heartbeat/delivery/flusher exits
	//    are never triggers; when they fail they close the socket (writeLocked,
	//    §7), which makes the reader fail, which is a trigger.
	select {
	case <-readerDone:
		s.Log.Info("Frame reader completed", zap.String("connection_id", conn.ID))
	case <-processorDone:
		s.Log.Info("Frame processor completed", zap.String("connection_id", conn.ID))
	case <-ackProcessorDone:
		s.Log.Info("ACK processor completed", zap.String("connection_id", conn.ID))
	}

	// 2. Confirm belt (teardown ordering), ONLY if no close handshake started:
	//    after connection.close/close-ok nothing else may be written (§1.4, §3.4).
	//    The socket is closed just below to unblock the reader/processor, and only
	//    afterwards is conn.Done signaled to stop the confirm flusher. A durability
	//    completion that advances a channel's confirm watermark in that window would
	//    otherwise be lost (the flusher's write hits the already-closed socket).
	//    Flush any advanced-but-unemitted confirms + pending nacks NOW, while the
	//    socket is still open. This runs on the teardown goroutine, concurrently
	//    with the still-live flusher, but is safe and idempotent — per-channel
	//    FlushConfirms is serialized under confirmFlushMu. Errors are ignored: the
	//    connection is closing regardless.
	if !rt.closeHandshakeStarted.Load() {
		_ = s.drainConfirmNacks(conn)
		_ = s.flushAllChannelConfirms(conn)
	}

	// 3. Unblock everyone: stop further writes, then close the socket. Closing
	//    the socket fails any writer blocked in Write while holding WriteMutex,
	//    releasing it (and the write deadline bounds it regardless). Go's
	//    tls.Conn.Close skips close_notify when a Write is in flight
	//    (crypto/tls activeCall interlock), so it cannot block behind a stuck
	//    writer.
	conn.Closed.Store(true)
	if t := rt.closeTimer.Load(); t != nil {
		t.Stop()
	}
	_ = conn.Conn.Close()
	rt.leaseCancel() // WS-I: unblock a reader parked in a lease Reserve
	// WS-I: from here on the processor discards (never admits) header/body
	// content. Drain the LeaseFIFO NOW: a processor parked mid-publish (e.g.
	// in a tx commit) must not hold QUEUED header leases for the whole park.
	// The processor pops nil for those frames and discards them safely;
	// processor-owned HeaderLease/PendingMessages leases are NOT touched here
	// — the once-only drain in cleanupConnection (after the joins) gets them
	// plus any late pushes.
	rt.leasesRevoked.Store(true)
	drainLeaseFIFO(conn)

	// 4. Signal shutdown to every Done-select (reader hand-offs, flow sender,
	//    heartbeat, confirm flusher, alarm/depth parks). This also unblocks
	//    processFrame if it's waiting to send to AckQueue.
	close(conn.Done)

	// 5. Wait for BOTH senders to ackQueue to exit before closing it. The frame
	//    processor routes acks it dequeues, and (since the SQ-0 fix) the reader
	//    diverts acks directly. processFrames may exit on error before the reader
	//    closes FrameQueue, so we must explicitly wait for the reader too;
	//    otherwise a late reader ack-send could hit a closed channel and panic.
	<-processorDone
	<-readerDone
	// Close ackQueue so ackProcessor drains remaining frames and exits
	close(conn.AckQueue)
	<-ackProcessorDone
	// The confirm flusher exits on conn.Done (closed above); if it was mid-write
	// the closed socket fails the write and it exits on its own error path.
	<-confirmFlusherDone

	// 6. Join EVERYTHING else (heartbeat, delivery, forwarders, flow sender).
	//    There is no cycle: nothing joined here waits on cleanupConnection,
	//    which runs only after this (§13.3).
	rt.wg.Wait()
}

// defaultReaderBackpressureProbe bounds how long the reader blocks handing a
// buffered frame to a full FrameQueue before it breaks out to read exactly one
// more frame off the socket (SQ-13), when neither the Server test-override nor
// Config.Network.ReaderBackpressureProbeMS is set.
const defaultReaderBackpressureProbe = 5 * time.Millisecond

// readerBackpressureProbeInterval is the reader-backpressure hand-off probe
// interval (SQ-13). The Server test-override wins; otherwise it is driven from
// Config.Network.ReaderBackpressureProbeMS, falling back to
// defaultReaderBackpressureProbe when that is unset. Mirrors alarmParkProbe.
func (s *Server) readerBackpressureProbeInterval() time.Duration {
	if s.readerBackpressureProbe > 0 {
		return s.readerBackpressureProbe
	}
	if ms := s.Config.Network.ReaderBackpressureProbeMS; ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return defaultReaderBackpressureProbe
}

// depthBackpressureLivenessInterval bounds how long the queue-depth reader park
// stays purely in the depth-poll (no socket read) before it breaks out to
// burst-drain currently-available RX and probe socket liveness. It only elapses
// when the queue stays pinned at/over HWM with no drain (all consumers gone — the
// teardown case, or severe overload), so the reader can still detect a client
// close (EOF) AND flush the producer's buffered-but-unread final window (which
// would otherwise strand its confirms). During healthy operation the queue drains
// within a poll interval and the reader resumes long before this fires.
const depthBackpressureLivenessInterval = 1 * time.Second

// depthLivenessInterval is the effective queue-depth reader-park liveness
// interval: the test override wins, else the default const.
func (s *Server) depthLivenessInterval() time.Duration {
	if s.depthLivenessProbe > 0 {
		return s.depthLivenessProbe
	}
	return depthBackpressureLivenessInterval
}

// parkReaderWhileDepthBackpressured parks the connection's reader while its
// queue-depth gate reports the target queue at/over HWM (iteration 2, option A).
// Not reading the socket IS the backpressure: the client's TCP window fills and
// its Publish blocks, pacing a producer-only connection to the consumer's drain
// rate so the durable queue stays small and ring-resident (which keeps consume
// throughput high) WITHOUT parking the frame processor mid-publish (which would
// strand an already-minted confirm tag at teardown).
//
// It POLLS the depth on a bounded re-check interval rather than blocking in a
// socket read, so it resumes the instant a consumer drains the queue below HWM
// even while the (blocked) client is silent — a read-blocking park would miss
// that drain and stall. It returns probing=true only when it broke out on the
// slow liveness timer (still pinned at HWM) so the caller burst-drains the
// producer's buffered-but-unread final window (teardown strand fix) and probes
// socket liveness; false when the queue drained (normal resume, gate cleared),
// the connection acquired a consumer (mixed conn — must keep reading its own
// acks), or the connection is tearing down.
func (s *Server) parkReaderWhileDepthBackpressured(conn *protocol.Connection) (probing bool) {
	gate := conn.DepthGateValue()
	if gate == nil || conn.HasActiveConsumer() {
		conn.SetDepthGate(nil)
		return false
	}
	recheck := time.NewTimer(s.readerBackpressureProbeInterval())
	defer recheck.Stop()
	liveness := time.NewTimer(s.depthLivenessInterval())
	defer liveness.Stop()
	for {
		if !gate.AtHighWaterMark() {
			conn.SetDepthGate(nil) // drained — resume full-speed reads
			return false
		}
		select {
		case <-conn.Done:
			return false
		case <-recheck.C:
			recheck.Reset(s.readerBackpressureProbeInterval())
		case <-liveness.C:
			return true // break out to burst-drain available RX + probe socket liveness
		}
	}
}

// readFrames reads frames from TCP connection and enqueues them for processing
// This goroutine NEVER blocks on frame processing - it only reads and enqueues
func (s *Server) readFrames(rt *connRuntime, done chan struct{}) {
	conn := rt.conn
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
	// rs (on the connRuntime) holds the pending hand-off buffer and the owed
	// leased-body progress (WS-I Phase B §B.3a): while a leased body is owed,
	// the reader never blocks on the limiter (TryReserve or close), the
	// alarm/depth parks are deferred, and the read deadline is
	// min(hbDeadline, oldest(lastProgress)+writeTimeout).
	rs := &rt.rs
	// deadlineActive tracks whether a read deadline is currently set on the
	// socket. Post-handshake the deadline is cleared, so it starts false. It only
	// matters on hb==0 connections, where it lets us clear a stale SQ-12
	// liveness-probe deadline exactly once instead of on every frame.
	deadlineActive := false
	// depthDraining is the queue-depth backpressure teardown burst-drain state
	// (iteration 2, option A). When the depth-park's liveness timer fires (the
	// queue is pinned >= HWM with no drain — teardown or overload), the reader
	// enters this mode: it reads all currently-available RX at full speed (short
	// deadline) instead of re-parking, so the producer's buffered-but-unread final
	// window is admitted + confirmed within the confirm grace even though the queue
	// is at HWM. It exits (re-parks) the moment a read finds RX empty. The producer
	// has stopped by teardown, so the window is bounded (~one TCP RX buffer/conn).
	depthDraining := false
	flowPaused := false
	flowLimit := s.Config.Network.ReaderOverflowFlowBytes
	hardCap := s.Config.Network.ReaderOverflowHardCapBytes

	// backpressureProbe bounds the reader-backpressure hand-off block (SQ-13):
	// while a backlog persists the reader blocks on the FrameQueue hand-off (not
	// reading the socket IS the backpressure) but breaks out every probe interval
	// to read exactly one frame, keeping acks + control frames flowing. Created
	// stopped and armed via Reset only while backlogged. Go 1.23+ timer semantics
	// guarantee Stop/Reset never leave a stale tick, so no manual channel drain is
	// needed.
	backpressureProbe := time.NewTimer(time.Hour)
	backpressureProbe.Stop()
	defer backpressureProbe.Stop()

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
	s.goConn(rt, "flow-sender", func() {
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
	})
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
		for len(rs.pending) > 0 {
			// Capture Size before the send: once processFrames receives the
			// frame it may pool it, and the recycled frame can be overwritten
			// while we still hold the stale reference.
			head := rs.pending[0]
			headSize := int64(head.Size)
			headCost := frameCost(head)
			// §9: "budget exhausted" behaves exactly like "channel full".
			if !s.frameBudgetAllows(conn, headCost) {
				break
			}
			conn.FrameQueueBytes.Add(headCost)
			select {
			case conn.FrameQueue <- head:
				rs.pendingBytes -= headSize
				rs.pending[0] = nil
				rs.pending = rs.pending[1:]
				continue
			case <-conn.Done:
				conn.FrameQueueBytes.Add(-headCost) // rollback: not handed off
				return
			default:
				conn.FrameQueueBytes.Add(-headCost) // rollback: not handed off
			}
			break
		}
		if len(rs.pending) == 0 {
			rs.pending = nil // release backing array once fully drained
			rs.pendingBytes = 0
			// Backlog cleared: lift the earlier flow-off signal so the client
			// may resume publishing.
			if flowPaused {
				flowPaused = false
				requestFlow(true)
			}
		}

		// SQ-13 reader TCP backpressure: a surviving backlog means the FrameQueue
		// is full and processFrames is parked (typically in
		// QueueState.WaitForCapacity). Rather than keep reading publishes off the
		// socket into unbounded memory (the old buffer-until-hard-cap-close
		// behaviour), BLOCK on the FrameQueue hand-off. Not reading the socket IS
		// the backpressure: the kernel RX window fills, the producer's write()
		// blocks, and the producer is paced to processFrames' drain rate — which,
		// on a shared publish+consume connection, is the consumer's drain rate.
		//
		// The block is bounded by a probe timer. On timeout the reader breaks out
		// to read EXACTLY ONE frame (with a read deadline armed below) so an ack or
		// control frame trapped behind the flood is still read within one probe
		// interval: acks divert to AckQueue (never blocked on broker backpressure)
		// releasing the prefetch gate, and control frames (esp. basic.consume) are
		// buffered and eventually processed. This is what keeps the
		// backpressure-relief path live and avoids the ack-starvation and
		// consume-staleness deadlocks. The reader still never blocks on a socket
		// write or the WriteMutex — only on this one channel send.
		backpressureProbed := false
		if len(rs.pending) > 0 {
			// Capture Size before the send: processFrames may pool the frame the
			// instant it receives it (mirrors the opportunistic drain above).
			head := rs.pending[0]
			headSize := int64(head.Size)
			headCost := frameCost(head)
			// §9: an exhausted byte budget makes the hand-off channel never-ready
			// (nil chan), so the reader parks on FrameBudgetWake exactly as it
			// would park on a full FrameQueue (TCP backpressure).
			q := conn.FrameQueue
			if !s.frameBudgetAllows(conn, headCost) {
				q = nil
			}
			backpressureProbe.Reset(s.readerBackpressureProbeInterval())
			select {
			case q <- head:
				backpressureProbe.Stop()
				// Add after the send: a transient negative is harmless
				// (frameBudgetAllows always admits q <= 0).
				conn.FrameQueueBytes.Add(headCost)
				rs.pendingBytes -= headSize
				rs.pending[0] = nil
				rs.pending = rs.pending[1:]
				continue
			case <-conn.FrameBudgetWake:
				backpressureProbe.Stop()
				continue // re-evaluate budget
			case <-conn.Done:
				backpressureProbe.Stop()
				return
			case <-backpressureProbe.C:
				// Probe fired: fall through to read EXACTLY ONE frame (bounded by
				// the read deadline armed below), then loop back and re-park. This
				// keeps acks + control frames flowing while backlogged.
				backpressureProbed = true
			}
		}

		// SQ-12 reader-pause backpressure: one atomic load per iteration (the hot
		// per-frame gate; zero-cost when unset). While a resource alarm is active
		// stop pulling application frames so TCP backpressure throttles the
		// publisher. Frames already read (in FrameQueue / pending) still drain and
		// their publishes complete — only subsequent unread publishes are deferred.
		//
		// Only PUBLISHING connections are paused (RabbitMQ parity): a consumer-only
		// connection (HasPublished false) is never parked, so its acks/consumes/
		// control frames keep flowing and a memory alarm can clear as queues drain.
		// The HasPublished load is short-circuited behind alarmState, so it adds no
		// cost on the unset hot path.
		probing := false
		if rs.owedLen() == 0 && s.alarmState.Load() != 0 && conn.HasPublished.Load() {
			probing = s.parkReaderWhileAlarmed(conn)
			// The connection may have torn down while we were parked.
			select {
			case <-conn.Done:
				return
			default:
			}
		}

		// Iteration 2 (option A): queue-depth reader backpressure. When a
		// producer-only connection has published to a queue at/over its HWM, stop
		// pulling NEW publishes off the socket (TCP backpressure) until a consumer
		// drains it — pacing the producer to the consumer's drain rate so the durable
		// queue stays small and ring-resident (fast consume), and never stranding a
		// confirm tag by parking the processor mid-publish. One atomic load per
		// iteration when unset (the common case). Skipped while burst-draining (the
		// liveness teardown drain runs full-speed until RX empties, then re-parks).
		if !depthDraining && rs.owedLen() == 0 && conn.DepthBackpressure.Load() {
			if s.parkReaderWhileDepthBackpressured(conn) {
				depthDraining = true // liveness fired: burst-drain the buffered window
			}
			select {
			case <-conn.Done:
				return
			default:
			}
		}

		// Compute the read deadline AFTER any park so a long park never counts
		// toward the heartbeat-missed window (heartbeat false-close bug). hb is
		// fixed for the connection's lifetime, so the deadlineActive bookkeeping
		// only ever clears a stale liveness-probe deadline on an hb==0 connection.
		//
		// WS-I Phase B: while a leased body is owed, the deadline is the MINIMUM
		// of the otherwise-computed deadline and oldest(lastProgress) +
		// writeTimeout — heartbeats and frames on other channels do not extend
		// it, so a client that stalls a leased body is closed even while keeping
		// the connection otherwise busy.
		hb := conn.HeartbeatSec.Load()
		var deadline time.Time
		switch {
		case depthDraining:
			// Option A teardown burst-drain: read available RX with a SHORT deadline
			// (highest precedence — overrides the long hb>0 deadline) so a read that
			// would block because RX is empty times out quickly and we re-park,
			// instead of blocking here for up to 2*hb. Each successful read admits a
			// buffered publish (→ confirmed); the timeout below exits the drain.
			deadline = time.Now().Add(s.readerBackpressureProbeInterval())
		case hb > 0:
			deadline = time.Now().Add(time.Duration(2*hb) * time.Second)
		case probing:
			// Heartbeats disabled but we broke out of the alarm park to probe:
			// bound the read so a quiet-but-alive peer does not wedge us; a dead
			// peer surfaces as a read error (heartbeat-disabled liveness bug).
			deadline = time.Now().Add(s.alarmParkProbeInterval())
		case backpressureProbed:
			// Heartbeats disabled but we broke out of the backpressure hand-off to
			// read one frame (SQ-13): bound the read so a quiet-but-alive backlogged
			// peer times out and re-parks (re-attempting the hand-off, which now
			// succeeds if the consumer has drained) instead of blocking here while
			// pending frames wait — and so a wedged peer with conn.Done still open
			// is not read indefinitely. A dead/half-closed peer surfaces via EOF or
			// a TCP-keepalive error (both non-timeout) and closes below.
			deadline = time.Now().Add(s.readerBackpressureProbeInterval())
		}
		if rs.owedLen() > 0 {
			if od := rs.oldestProgress().Add(s.writeTimeout()); deadline.IsZero() || od.Before(deadline) {
				deadline = od
			}
		}
		switch {
		case !deadline.IsZero():
			conn.Conn.SetReadDeadline(deadline)
			deadlineActive = true
		case deadlineActive:
			// hb==0 and not probing: clear the stale probe deadline exactly once
			// so a normal blocking read resumes.
			conn.Conn.SetReadDeadline(time.Time{})
			deadlineActive = false
		}

		maxSize := conn.MaxFrameSize
		if maxSize == 0 {
			maxSize = protocol.MaxInboundFrameSize
		}
		frame, err := protocol.ReadFrameOptimizedWithLimit(conn.Reader, maxSize)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if depthDraining {
					// Option A burst-drain: the short-deadline read found RX empty —
					// the producer's buffered window is fully drained (all confirmed).
					// Exit drain mode; the next iteration re-parks on depth if still
					// >= HWM. This is NOT a heartbeat miss (it takes precedence over the
					// hb>0 close below): a depth-drain timeout is expected, not a death.
					depthDraining = false
					continue
				}
				if rs.owedLen() > 0 && !time.Now().Before(rs.oldestProgress().Add(s.writeTimeout())) {
					// WS-I Phase B: a leased body made no progress within
					// writeTimeout of its last body byte. This is a body stall,
					// NOT a heartbeat death.
					s.bodyStallClose(conn, rs)
					return
				}
				if hb > 0 {
					s.Log.Info("heartbeat missed, closing dead connection",
						zap.String("connection_id", conn.ID))
				} else if probing && s.alarmState.Load() != 0 {
					// Liveness probe timed out on a quiet, still-blocked peer with
					// heartbeats disabled — not a death. Re-park instead of closing.
					continue
				} else if backpressureProbed {
					// SQ-13: the bounded backpressure-probe read timed out on a
					// quiet-but-alive backlogged peer (hb==0) — not a death. Re-park
					// on the FrameQueue hand-off; the loop re-drains pending first, so
					// a slot freed by the consumer meanwhile is used immediately. A
					// genuinely dead peer surfaces as EOF / a keepalive error (handled
					// below), not a timeout.
					continue
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

		// WS-I Phase B: enforce the owed-body deadline on EVERY read frame,
		// not just via the socket read deadline — the socket deadline only
		// bounds BLOCKING reads, so a client streaming non-progress frames
		// (e.g. zero-length body frames) would otherwise NEVER trip it.
		if rs.owedLen() > 0 && !time.Now().Before(rs.oldestProgress().Add(s.writeTimeout())) {
			s.bodyStallClose(conn, rs)
			return
		}

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
			// §9: an "ack" is classified by class/method only, so its size is
			// bounded per frame — a malicious oversized ack payload hard-closes
			// the connection (the reader must not write, so no close frame).
			if !s.admitOversizedAckFrame(conn, frame) {
				return
			}
			select {
			case conn.AckQueue <- frame:
			case <-conn.Done:
				protocol.PutFrame(frame)
				return
			}
			continue
		}

		// WS-I Phase B: header-time lease. A content-header frame reserves its
		// declared body bytes (+framing) BEFORE any body frame is read and
		// before the confirm tag exists; a body frame records owed progress.
		if frame.Type == protocol.FrameHeader {
			if !s.leaseForHeaderFrame(rt, frame, rs) {
				return
			}
		} else if frame.Type == protocol.FrameBody {
			rs.trackBody(frame)
		} else if frame.Type == protocol.FrameMethod && frame.Channel != 0 && len(frame.Payload) >= 4 &&
			frame.Payload[0] == 0 && frame.Payload[1] == 20 && (frame.Payload[3] == 40 || frame.Payload[3] == 41) {
			// A channel close / close-ok ends any owed leased body on it: its
			// remaining body frames are discarded by the closing gate.
			rs.clearOwed(frame.Channel)
		}

		// Enqueue for processFrames. Try a non-blocking direct hand-off only when
		// nothing is buffered (buffering must preserve per-channel frame order —
		// a publish's method, header and body frames must stay contiguous). When
		// FrameQueue is full, buffer instead; the reader must never block here or
		// acks stall behind it.
		cost := frameCost(frame)
		if len(rs.pending) == 0 && s.frameBudgetAllows(conn, cost) {
			conn.FrameQueueBytes.Add(cost)
			select {
			case conn.FrameQueue <- frame:
				continue // queued directly
			case <-conn.Done:
				conn.FrameQueueBytes.Add(-cost)
				protocol.PutFrame(frame)
				return
			default:
				// FrameQueue full — roll back and fall through to bounded buffering.
				conn.FrameQueueBytes.Add(-cost)
			}
		}
		rs.pending = append(rs.pending, frame)
		rs.pendingBytes += int64(frame.Size)

		// Hard cap: a client ignoring flow, or flooding publishes while never
		// consuming, must not grow reader memory without bound. Close it.
		if hardCap > 0 && rs.pendingBytes > hardCap {
			s.Log.Warn("reader overflow exceeded hard cap; closing connection",
				zap.String("connection_id", conn.ID),
				zap.Int64("pending_bytes", rs.pendingBytes),
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
		if !flowPaused && flowLimit > 0 && rs.pendingBytes >= flowLimit {
			flowPaused = true
			requestFlow(false)
			s.Log.Info("reader overflow: requesting channel.flow(false)",
				zap.String("connection_id", conn.ID),
				zap.Int64("pending_bytes", rs.pendingBytes))
		}
	}
}

// processFrames processes frames from the queue
// This goroutine CAN block on WAL fsync without affecting the reader
func (s *Server) processFrames(rt *connRuntime, done chan struct{}) {
	conn := rt.conn
	defer close(done)

	for {
		select {
		case frame, ok := <-conn.FrameQueue:
			if !ok {
				s.Log.Info("Frame queue closed", zap.String("connection_id", conn.ID))
				return
			}
			rt.procSeq.Add(1) // lease-wait stall guard: one tick per dequeued frame

			// §9: capture the budget cost immediately (before processFrame can
			// pool the frame) and release it on EVERY path that finishes with
			// the frame — heartbeat, gate discards, and after processFrame
			// (whether the frame is pooled or handed to the AckQueue).
			cost := frameCost(frame)

			if frame.Type == protocol.FrameHeartbeat {
				s.releaseFrameBudget(conn, cost)
				protocol.PutFrame(frame)
				continue
			}

			// WS-I: pair the lease the reader reserved for this header FIRST
			// (FIFO order equals frame order; the reader's pending buffer and
			// FrameQueue both preserve read order), so EVERY discard arm below
			// can release it — including the revoked-teardown arm after an
			// early FIFO drain (Pop is nil then, which is safe).
			if frame.Type == protocol.FrameHeader {
				conn.HeaderLease = conn.LeaseFIFO.Pop()
			}

			// WS-I: once leases are revoked (teardown), header/body content is
			// NEVER admitted unleased: discard it, releasing its budget and
			// its paired lease. Method frames (close/close-ok) still process.
			if rt.leasesRevoked.Load() && (frame.Type == protocol.FrameHeader || frame.Type == protocol.FrameBody) {
				releaseHeaderLease(conn)
				s.releaseFrameBudget(conn, cost)
				protocol.PutFrame(frame)
				continue
			}

			// Capture the error-classification context before processFrame: it
			// may transfer frame ownership (AckQueue hand-off) or we pool it
			// right after — either way the next owner can overwrite the frame
			// while we still hold the ref.
			fc := frameCtx{channel: frame.Channel}
			if cls, mth, isMethod := methodIDs(frame); isMethod {
				fc.classID, fc.methodID = cls, mth
			} else if frame.Type == protocol.FrameHeader || frame.Type == protocol.FrameBody {
				fc.classID, fc.methodID = 60, 40 // basic.publish content frames
			}

			// §6.3 per-frame channel-ID gate: a channel ID above the negotiated
			// channel_max is a hard 530. conn.MaxChannels is written once during
			// the handshake, before this goroutine starts, so this is a plain read.
			if frame.Channel != 0 && conn.MaxChannels > 0 && frame.Channel > conn.MaxChannels {
				err := s.connectionError(conn, amqperrors.NotAllowed,
					fmt.Sprintf("NOT_ALLOWED - channel %d exceeds negotiated channel_max %d", frame.Channel, conn.MaxChannels),
					fc.classID, fc.methodID)
				s.releaseFrameBudget(conn, cost)
				protocol.PutFrame(frame)
				releaseHeaderLease(conn) // WS-I: discard arm
				if s.handleFrameError(rt, fc, err, true) {
					return
				}
				continue
			}

			// §3.7 closing gate: after sending close, discard everything except
			// close / close-ok (two atomic loads when nothing is closing).
			if rt.connClosing.Load() || rt.closingCount.Load() != 0 {
				if s.discardWhileClosing(rt, frame) {
					s.releaseFrameBudget(conn, cost)
					protocol.PutFrame(frame)
					releaseHeaderLease(conn) // WS-I: discard arm
					continue
				}
			}

			pooled, err := s.processFrame(conn, frame)
			s.releaseFrameBudget(conn, cost)
			if pooled {
				protocol.PutFrame(frame)
			}
			// WS-I: the header path transfers a live lease into the pending
			// message (this is then a no-op); an error arm leaves it here.
			releaseHeaderLease(conn)
			if err != nil {
				if s.handleFrameError(rt, fc, err, true) {
					return
				}
			}

		case <-conn.Done:
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
		return true, s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown frame type %d", frame.Type), 0, 0)
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
		return s.connectionError(conn, amqperrors.FrameError, "FRAME_ERROR - ack frame payload too short", 60, 0)
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
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unexpected method 60.%d in ack processor", methodID), 60, methodID)
	}
}

func (s *Server) ackProcessor(rt *connRuntime, done chan struct{}) {
	conn := rt.conn
	defer close(done)
	for frame := range conn.AckQueue {
		// §3.7: ack frames are never close frames, so for a closing channel or
		// connection they are always discarded.
		if rt.connClosing.Load() || (frame.Channel != 0 && rt.isChannelClosing(frame.Channel)) {
			protocol.PutFrame(frame)
			continue
		}
		// Capture before PutFrame: the pool may recycle the frame to another
		// goroutine's GetFrame, which overwrites it while we still hold the ref.
		fc := frameCtx{channel: frame.Channel, classID: 60}
		if len(frame.Payload) >= 4 {
			fc.methodID = (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
		}
		err := s.processAckFrame(conn, frame)
		protocol.PutFrame(frame)
		if err != nil {
			if s.handleFrameError(rt, fc, err, false) {
				return
			}
		}
	}
}

// sendHeartbeats periodically sends heartbeat frames to the client
// This keeps the connection alive independently of frame processing
// RabbitMQ sends heartbeats every (heartbeat_timeout / 2) seconds.
// Only started when the negotiated heartbeat is > 0 (§4.2).
func (s *Server) sendHeartbeats(conn *protocol.Connection) {
	hb := conn.HeartbeatSec.Load()

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

			// writeFrame arms the write deadline; on error it has already
			// marked the connection closed and closed the socket.
			if err := s.writeFrame(conn, heartbeatFrame); err != nil {
				s.Log.Error("Failed to send heartbeat",
					zap.String("connection_id", conn.ID),
					zap.Error(err))

				// Mark connection as closed
				conn.Closed.Store(true)

				return
			}

			s.Log.Debug("Sent heartbeat to client", zap.String("connection_id", conn.ID))

		case <-conn.Done:
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

	if len(frame.Payload) < 4 {
		return s.connectionError(conn, amqperrors.FrameError, "FRAME_ERROR - method frame payload too short", 0, 0)
	}

	classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])

	// Only the connection class is valid on channel 0 (AMQP 0-9-1 §4.1).
	switch {
	case classID == 10: // Connection class
		return s.processConnectionMethod(conn, methodID, frame.Payload[4:])
	case classID == 20 && methodID == protocol.ChannelOpen:
		return s.connectionError(conn, amqperrors.CommandInvalid,
			"COMMAND_INVALID - channel.open on channel 0", 20, 10)
	case classID == 20 || classID == 40 || classID == 50 || classID == 60 || classID == 85 || classID == 90:
		return s.connectionError(conn, amqperrors.CommandInvalid,
			fmt.Sprintf("COMMAND_INVALID - class %d on channel 0", classID), classID, methodID)
	default:
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown method %d.%d", classID, methodID), classID, methodID)
	}
}

// processChannelMethod processes channel-level methods
func (s *Server) processChannelMethod(conn *protocol.Connection, frame *protocol.Frame) error {
	if len(frame.Payload) < 4 {
		s.Log.Warn("Method frame payload too short",
			zap.Int("length", len(frame.Payload)),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.FrameError, "FRAME_ERROR - method frame payload too short", 0, 0)
	}

	classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
	methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])

	// §6.3 opened-channel gate: every method except channel.open requires a
	// previously opened channel (AMQP 0-9-1 "expected 'channel.open'"). One
	// sync.Map.Load per method frame — the read-mostly lock-free fast path.
	if !(classID == 20 && methodID == protocol.ChannelOpen) {
		if _, ok := conn.Channels.Load(frame.Channel); !ok {
			// A channel in the closing set WAS opened and torn down at error
			// time (D4); its close / close-ok must still complete the
			// handshake (§3.7 lets only those through).
			isCloseHandshake := classID == 20 && (methodID == protocol.ChannelClose || methodID == protocol.ChannelCloseOK)
			if !(isCloseHandshake && s.runtimeFor(conn).isChannelClosing(frame.Channel)) {
				return s.connectionError(conn, amqperrors.ChannelErrorCode,
					fmt.Sprintf("CHANNEL_ERROR - expected 'channel.open' on channel %d", frame.Channel), classID, methodID)
			}
		}
	}

	// Process based on class and method IDs
	switch classID {
	case 10: // Connection class is invalid on a channel
		return s.connectionError(conn, amqperrors.CommandInvalid,
			fmt.Sprintf("COMMAND_INVALID - connection method on channel %d", frame.Channel), classID, methodID)
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
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown method %d.%d", classID, methodID), classID, methodID)
	}
}

// Stop stops the server gracefully
func (s *Server) Stop() error {
	s.Mutex.Lock()
	s.ShuttingDown = true
	ln := s.Listener
	cancel := s.metricsCancel
	alarmCancel := s.alarmCancel
	nagCancel := s.unsafeRecoveryCancel
	s.Mutex.Unlock()

	if cancel != nil {
		cancel()
	}
	if alarmCancel != nil {
		alarmCancel()
	}
	if nagCancel != nil {
		nagCancel()
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

	if s.Lifecycle != nil {
		s.Lifecycle.markRunning()
	}

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

	// SQ-12: start the resource-alarm monitor (no-op / not spawned when alarms
	// are disabled — the unset zero-cost contract).
	if s.startAlarmMonitor() {
		s.Log.Info("Started resource-alarm monitor",
			zap.Duration("interval", s.alarm.interval))
	}

	// A broker that booted with --unsafe-recovery stays loud for its whole
	// lifetime. Not spawned on a clean boot.
	if s.startUnsafeRecoveryNag() {
		s.Log.Error("THIS BROKER IS RUNNING DEGRADED: it started with "+interfaces.UnsafeRecoveryFlag+
			" after discarding data it could not recover",
			zap.Int("discarded_artifacts", len(s.unsafeRecovery)))
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
				shutdown := s.ShuttingDown
				s.Mutex.RUnlock()
				if shutdown {
					return nil
				}
				s.Log.Error("Error accepting connection", zap.Error(err))
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Handle the connection in a goroutine
			s.dispatchAccepted(conn)
		case <-quit:
			s.Log.Info("Server shutdown requested")
			s.Mutex.Lock()
			s.ShuttingDown = true
			cancel := s.metricsCancel
			alarmCancel := s.alarmCancel
			s.Mutex.Unlock()
			if cancel != nil {
				cancel()
			}
			if alarmCancel != nil {
				alarmCancel()
			}
			listener.Close()
			return nil
		}
	}
}
