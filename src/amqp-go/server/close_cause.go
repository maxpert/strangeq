package server

import (
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
)

// recordCloseCause records a connection's close cause EXACTLY ONCE per
// connection. When a runtime is known it is the once-guard (the connection's
// runtime outlives every close site and is unique); a nil runtime falls back to
// an unguarded record for sockets that never registered one.
//
// Callers that must also mark the connection closed use recordConnectionClose;
// failConnection (which deliberately keeps reading for the client's close-ok)
// uses this directly.
func (s *Server) recordCloseCause(rt *connRuntime, cause string) {
	if rt != nil && rt.closeCauseRecorded.Swap(true) {
		return // already recorded for this connection
	}
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordConnectionClosedByCause(cause)
	}
}

// recordConnectionClose marks a connection closed and records its bounded close
// cause exactly once (spec 06 §3.5). It replaces a bare conn.Closed.Store(true)
// at a hard-close site so the close is counted, not merely performed. Multiple
// close sites on one connection cannot double-count.
func (s *Server) recordConnectionClose(conn *protocol.Connection, cause string) {
	if rt, ok := s.runtimes.Load(conn); ok {
		conn.Closed.Store(true)
		s.recordCloseCause(rt.(*connRuntime), cause)
		return
	}
	if conn.Closed.Swap(true) {
		return // already closed (and therefore already counted)
	}
	s.recordCloseCause(nil, cause)
}

// connectionCloseCauseForCode maps an AMQP close code to the bounded cause
// enum. Only CONNECTION_FORCED is a policy close; every other code is a
// protocol-level peer violation, reported as protocol_error.
func connectionCloseCauseForCode(code uint16) string {
	if code == amqperrors.ConnectionForced {
		return ConnectionCloseForcedShutdown
	}
	return ConnectionCloseProtocolError
}
