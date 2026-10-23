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

func (s *Server) shutdown(ctx context.Context) error {
	if s.Lifecycle != nil {
		s.Lifecycle.setState(StateStopping)
		defer func() {
			s.Lifecycle.stateMutex.Lock()
			s.Lifecycle.state = StateStopped
			s.Lifecycle.stopTime = time.Now()
			s.Lifecycle.stateMutex.Unlock()
		}()
	}

	// 1. Stop accepting (idempotent; Stop already sets ShuttingDown under Lock,
	//    and dispatchAccepted does connWG.Add under RLock after checking the
	//    flag — so no Add can race the Wait below).
	if err := s.Stop(); err != nil && !isListenerClosedErr(err) {
		s.Log.Warn("shutdown: listener close error (continuing)", zap.Error(err))
	}

	// 2. Ask every connection to close, one goroutine per connection.
	s.Mutex.RLock()
	conns := make([]*protocol.Connection, 0, len(s.Connections))
	for _, c := range s.Connections {
		conns = append(conns, c)
	}
	s.Mutex.RUnlock()
	for _, c := range conns {
		if rt, ok := s.runtimes.Load(c); ok && rt.(*connRuntime).handshakeDone.Load() {
			go s.failConnection(rt.(*connRuntime), amqperrors.ConnectionForced,
				"CONNECTION_FORCED - broker shutdown", 0, 0)
		} else {
			// Still handshaking (or no runtime): just close the socket.
			_ = c.Conn.Close()
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
