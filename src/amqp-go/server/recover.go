package server

import (
	"net"
	"runtime/debug"

	"go.uber.org/zap"
)

// PanicCount reports how many panics have been recovered in per-connection
// goroutines since boot (§8). WS-E may export it as amqp_panics_total.
func (s *Server) PanicCount() int64 { return s.panics.Load() }

func (s *Server) notePanic(where, connID string, r any) {
	s.panics.Add(1)
	s.Log.Error("recovered panic in connection goroutine",
		zap.String("goroutine", where), zap.String("connection_id", connID),
		zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
}

// goConn starts fn as a per-connection goroutine: registered in rt.wg, counted
// in rt.liveGoroutines, panic-recovered. On panic the connection is marked
// closed and its socket closed; the reader then fails, the standard teardown
// (§13) runs, and cleanupConnection executes exactly once via finishConnection.
//
// Known limitation: if a goroutine panics while holding channel.Mutex with a
// manual Unlock (e.g. basic_handlers.go), cleanupConnection's
// channel.Mutex.Lock() deadlocks and that one connection's teardown hangs. The
// process stays alive and other connections are unaffected. WriteMutex is safe
// because writeLocked uses defer. Converting manual unlocks to defer is out of
// scope (§8).
func (s *Server) goConn(rt *connRuntime, name string, fn func()) {
	rt.wg.Add(1)
	rt.liveGoroutines.Add(1)
	go func() {
		defer rt.wg.Done()
		defer rt.liveGoroutines.Add(-1)
		defer func() {
			if r := recover(); r != nil {
				s.notePanic(name, rt.conn.ID, r)
				rt.conn.Closed.Store(true)
				_ = rt.conn.Conn.Close()
			}
		}()
		fn()
	}()
}

// recoverConnection is deferred in handleConnection (outer guard: handshake,
// registration, and anything not already covered by goConn).
func (s *Server) recoverConnection(nc net.Conn, where string) {
	if r := recover(); r != nil {
		s.notePanic(where, "", r)
		_ = nc.Close()
	}
}

// finishConnection is handleConnection's single teardown path: the test
// observability hook (all per-connection goroutines already joined), then
// cleanupConnection behind its own recover, then runtime bookkeeping.
func (s *Server) finishConnection(rt *connRuntime) {
	if s.onConnTornDown != nil {
		s.onConnTornDown(rt)
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				s.notePanic("cleanup", rt.conn.ID, r)
			}
		}()
		s.cleanupConnection(rt.conn)
	}()
	s.runtimes.Delete(rt.conn)
	if t := rt.closeTimer.Load(); t != nil {
		t.Stop()
	}
	close(rt.torndown)
}
