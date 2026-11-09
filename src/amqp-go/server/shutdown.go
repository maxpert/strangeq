package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// shutdownForceJoinTimeout bounds the extra wait after ctx.Done() before
// Shutdown gives up on live handleConnection goroutines (and refuses to close
// storage under them — a use-after-close).
const shutdownForceJoinTimeout = 5 * time.Second

// managementShutdownTimeout bounds the management surface's graceful stop
// (OPS-043/NFR-024). The surface owns a separate listener, so a wedged admin
// client can never delay broker teardown.
const managementShutdownTimeout = 5 * time.Second

// shutdownHandshakePoll, and shutdownHandshakeGrace with it, shape Shutdown's
// still-handshaking branch.
//
// A connection can be past connection.open-ok on the wire while handshakeDone is
// still unset: the broker writes open-ok inside processConnectionOpen
// (connection_handlers.go) and publishes the flag only after that returns
// (server.go), while the client's Dial returns the instant it READS open-ok. A
// Shutdown landing in that window used to close the socket with no close frame
// at all, so the peer saw the socket die (amqp091-go synthesizes
// Error{Code: FrameError, Server:false}) instead of the promised
// connection.close 320 CONNECTION_FORCED — the exact failure CI reported on
// TestShutdownWSB (expected 320, actual 501, Server=false).
//
// The still-handshaking branch therefore waits this long for handshakeDone to
// settle (the handshake needs one round trip once the frames are on the wire)
// and takes the graceful path when it does. If the grace expires the connection
// never reached connection.open, where a close frame is not protocol-legal, so
// it is closed raw exactly as before. Half a second is far wider than the
// in-flight handshake it covers and far narrower than any shutdown budget.
const (
	shutdownHandshakePoll  = 2 * time.Millisecond
	shutdownHandshakeGrace = 500 * time.Millisecond
)

// ErrShutdownIncomplete is returned (wrapping the context error) when
// connection goroutines outlived the shutdown context AND the force-join
// grace; storage is left open in that case.
var ErrShutdownIncomplete = errors.New("shutdown: connections did not terminate; storage left open")

// Shutdown stops the broker gracefully. It is idempotent and safe to call
// concurrently (later callers block until the first finishes and get its
// result).
//
//  1. Stop accepting: Stop() (closes the listener, cancels metrics/alarm/nag).
//  2. For each connection: if its handshake completed, flush confirms and send
//     connection.close 320 "CONNECTION_FORCED - broker shutdown" (via
//     failConnection, one goroutine per connection so a non-reading peer
//     cannot serialize the rest); if still handshaking, close the socket.
//  3. Wait for every handleConnection to finish (connWG) or ctx.Done().
//  4. On ctx.Done: close every remaining socket, then wait up to
//     shutdownForceJoinTimeout more. If they STILL have not finished, log,
//     skip step 5 (closing storage under live goroutines is a
//     use-after-close), and return fmt.Errorf("%w: %w", ErrShutdownIncomplete,
//     ctx.Err()).
//  5. Close the broker (StorageBroker.Close) if ownsBroker, then
//     storage.Close() if ownsStorage; set storageClosed.
//  6. Return nil if step 3 finished before ctx.Done; otherwise ctx.Err()
//     (forced but clean).
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.shutdownErr = s.shutdown(ctx)
	})
	return s.shutdownErr
}

// forceConnectionClose handles the one shutdown case that cannot be decided in
// the caller: a connection with a runtime whose handshake has not settled yet.
//
// It exists because of the connection.open-ok / handshakeDone window described at
// the constants above. The caller has already dealt with the two decidable cases
// (no runtime at all, handshake already done), so this only has to give the
// in-flight handshake its grace and then pick the close that is actually legal:
// the graceful CONNECTION_FORCED the moment connection.open lands, or the raw
// socket close if the peer never gets there.
//
// Every close site's cause telemetry is once-guarded (recordConnectionClose /
// recordCloseCause), so exactly one cause is ever recorded for this connection.
func (s *Server) forceConnectionClose(c *protocol.Connection, r *connRuntime) {
	deadline := time.Now().Add(shutdownHandshakeGrace)
	for time.Now().Before(deadline) {
		if r.handshakeDone.Load() {
			// The peer's own Dial is already past open-ok, so a close frame is
			// protocol-legal and it is the promised one.
			s.failConnection(r, amqperrors.ConnectionForced,
				"CONNECTION_FORCED - broker shutdown", 0, 0)
			return
		}
		time.Sleep(shutdownHandshakePoll)
	}
	// Never reached connection.open: a close frame is not protocol-legal there,
	// so this stays the raw close (with its own cause record).
	s.recordConnectionClose(c, ConnectionCloseForcedShutdown)
	_ = c.Conn.Close()
}

func (s *Server) shutdown(ctx context.Context) (err error) {
	// Shutdown telemetry (spec 06 §3.5): one result counter and the wall-clock
	// duration, recorded on every exit path.
	shutdownStart := time.Now()
	shutdownResult := ShutdownResultClean
	defer func() {
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordShutdown(shutdownResult, time.Since(shutdownStart).Seconds())
		}
	}()

	if s.Lifecycle != nil {
		s.Lifecycle.setState(StateStopping)
		defer func() {
			s.Lifecycle.stateMutex.Lock()
			s.Lifecycle.state = StateStopped
			s.Lifecycle.stopTime = time.Now()
			s.Lifecycle.stateMutex.Unlock()
		}()
	}

	// 0. Stop the management (control-plane) surface FIRST, before any broker
	//    teardown. It is a separate listener with its own goroutines; stopping
	//    it first guarantees no management request can touch the broker while
	//    it is being torn down, and its own 5 s budget keeps a wedged admin
	//    client from delaying broker shutdown (OPS-043).
	if s.management != nil {
		mctx, cancel := context.WithTimeout(context.Background(), managementShutdownTimeout)
		if err := s.management.Shutdown(mctx); err != nil {
			s.Log.Warn("shutdown: management surface did not stop cleanly", zap.Error(err))
		}
		cancel()
	}

	// 1. Stop accepting (idempotent; Stop already sets ShuttingDown under Lock,
	//    and dispatchAccepted does connWG.Add under RLock after checking the
	//    flag — so no Add can race the Wait below).
	if err := s.Stop(); err != nil && !isListenerClosedErr(err) {
		s.Log.Warn("shutdown: listener close error (continuing)", zap.Error(err))
	}

	// WS-I: close the publish-lease limiter so any reader still parked in a
	// blocking Reserve fails with ErrLimiterClosed (hb==0 connections have no
	// heartbeat writer to detect a wedged peer; this is their bounded wait).
	// Outstanding leases — including broker-held onReleased callbacks — still
	// release normally on WAL completion.
	s.publishLimiter().Close()

	// 2. Ask every connection to close, one goroutine per connection.
	s.Mutex.RLock()
	conns := make([]*protocol.Connection, 0, len(s.Connections))
	for _, c := range s.Connections {
		conns = append(conns, c)
	}
	s.Mutex.RUnlock()
	for _, c := range conns {
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordShutdownConnectionForced()
		}
		rt, ok := s.runtimes.Load(c)
		switch {
		case !ok:
			// No runtime: nothing on this socket can speak AMQP (unit tests
			// build bare connections). Recorded and closed synchronously, so the
			// cause is visible the moment Shutdown returns.
			s.recordConnectionClose(c, ConnectionCloseForcedShutdown)
			_ = c.Conn.Close()
		case rt.(*connRuntime).handshakeDone.Load():
			// failConnection records the forced_shutdown cause once (the code
			// is CONNECTION_FORCED), so this branch must NOT record it again.
			go s.failConnection(rt.(*connRuntime), amqperrors.ConnectionForced,
				"CONNECTION_FORCED - broker shutdown", 0, 0)
		default:
			// Mid-handshake: the peer may already be past connection.open-ok on
			// the wire while handshakeDone is still unset, so this one gets the
			// bounded grace before the raw close. See forceConnectionClose.
			go s.forceConnectionClose(c, rt.(*connRuntime))
		}
	}

	// 3. Wait for every handleConnection to finish, or ctx.Done().
	joined := make(chan struct{})
	go func() { s.connWG.Wait(); close(joined) }()
	forced := false
	select {
	case <-joined:
	case <-ctx.Done():
		forced = true
		shutdownResult = ShutdownResultForced
	}

	// 4. Forced: close every remaining socket, then wait up to
	//    shutdownForceJoinTimeout more.
	if forced {
		s.Mutex.RLock()
		for _, c := range s.Connections {
			_ = c.Conn.Close()
		}
		s.Mutex.RUnlock()
		select {
		case <-joined:
		case <-time.After(shutdownForceJoinTimeout):
			shutdownResult = ShutdownResultIncomplete
			s.Log.Error("shutdown: connections did not terminate; storage left open")
			return fmt.Errorf("%w: %w", ErrShutdownIncomplete, ctx.Err())
		}
	}

	// 5. Close owned broker + storage. RequeueGetDeliveries and exclusive-queue
	//    deletion already ran per connection inside cleanupConnection, because
	//    step 3 joined all handleConnections first.
	if s.ownsBroker {
		if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
			sb.broker.Close()
		}
	}
	if s.ownsStorage && s.storage != nil {
		// interfaces.Storage has no Close method; the concrete stores do.
		if closer, ok := s.storage.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				if s.MetricsCollector != nil {
					s.MetricsCollector.RecordStorageCloseError()
				}
				s.Log.Error("shutdown: storage close failed", zap.Error(err))
			} else {
				s.storageClosed.Store(true)
			}
		}
	}

	// 6. nil when the join finished before ctx.Done; otherwise the (forced but
	//    clean) context error.
	if forced {
		return ctx.Err()
	}
	return nil
}

// Ready reports that recovery completed (Build is synchronous, so a built
// server has recovered), the listener is bound, and shutdown has not begun.
func (s *Server) Ready() bool {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	return s.Listener != nil && !s.ShuttingDown
}

// Health returns LifecycleManager.Health() with Status forced to "healthy"
// iff Ready(), and a warning when the broker booted with --unsafe-recovery.
func (s *Server) Health() interfaces.HealthStatus {
	var st interfaces.HealthStatus
	if s.Lifecycle != nil {
		st = s.Lifecycle.Health()
	} else {
		st = interfaces.HealthStatus{Status: "unknown", Timestamp: time.Now()}
	}
	if s.Ready() {
		st.Status = "healthy"
	}
	if faults := s.UnsafeRecoveryFaults(); len(faults) > 0 {
		st.Warnings = append(st.Warnings,
			fmt.Sprintf("broker booted degraded with %s after discarding %d artifact(s)",
				interfaces.UnsafeRecoveryFlag, len(faults)))
	}
	return st
}
