package server

import (
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// SQ-12 reader-pause backpressure and its three correctness bugs. Each test
// drives readFrames directly over a net.Pipe with the connection registered in
// s.Connections so applyAlarmBits' wake path reaches it. The connections do NOT
// advertise the connection.blocked capability, so no control frames are written
// to the client side — keeping these tests focused purely on the reader's
// park/resume/liveness behavior. Run under -race.
// ============================================================================

// registerPauseConn builds a pipe-backed connection registered on srv, returning
// the client side to drive it from.
func registerPauseConn(t *testing.T, srv *Server) (*protocol.Connection, net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	conn := protocol.NewConnection(serverConn)
	srv.Connections[conn.ID] = conn
	return conn, clientConn
}

// TestReaderPause_PausesWhileAlarmedResumesOnClear is the baseline: while a
// resource alarm is active the reader must not consume socket frames; on clear
// it must resume and consume them.
func TestReaderPause_PausesWhileAlarmedResumesOnClear(t *testing.T) {
	srv := makeTestServer()
	srv.Connections = map[string]*protocol.Connection{}
	srv.alarmParkProbe = 30 * time.Second // no liveness probe within the test window
	conn, clientConn := registerPauseConn(t, srv)
	defer clientConn.Close()
	conn.HeartbeatSec.Store(0)

	// Arm before the reader starts so its first loop iteration parks.
	srv.applyAlarmBits(AlarmMemory)

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	writeDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Write(nonAckPublishFrameBytes(t, 32))
		writeDone <- err
	}()

	select {
	case f := <-conn.FrameQueue:
		t.Fatalf("reader consumed a frame while paused: %+v", f)
	case <-writeDone:
		t.Fatal("client write completed while paused — the frame was consumed")
	case <-time.After(300 * time.Millisecond):
		// still parked — correct
	}

	srv.applyAlarmBits(0) // clear -> wake the reader

	select {
	case f := <-conn.FrameQueue:
		if f == nil {
			t.Fatal("FrameQueue closed instead of delivering the frame")
		}
	case <-readerDone:
		t.Fatal("reader exited instead of resuming")
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not resume and consume the frame after clear")
	}

	clientConn.Close()
	<-readerDone
}

// TestReaderPause_LongParkDoesNotTripHeartbeatClose proves BUG A (heartbeat
// false-close): a park longer than 2*heartbeat must not close the connection,
// and on resume the reader must set a FRESH read deadline so it does not
// instantly time out against the pre-park deadline.
func TestReaderPause_LongParkDoesNotTripHeartbeatClose(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive; needs > 2*heartbeat of real time")
	}
	srv := makeTestServer()
	srv.Connections = map[string]*protocol.Connection{}
	srv.alarmParkProbe = 30 * time.Second
	conn, clientConn := registerPauseConn(t, srv)
	defer clientConn.Close()
	conn.HeartbeatSec.Store(1) // 2*hb = 2s heartbeat-missed window

	srv.applyAlarmBits(AlarmMemory)
	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	// Park well beyond 2*hb. The reader must stay alive (parked in select, no
	// active read deadline).
	select {
	case <-readerDone:
		t.Fatal("reader closed the connection during a long park (heartbeat false-close)")
	case <-time.After(2500 * time.Millisecond):
	}

	// Clear and publish. The reader must read the frame, not immediately
	// heartbeat-close against a stale deadline.
	srv.applyAlarmBits(0)
	go func() { _, _ = clientConn.Write(nonAckPublishFrameBytes(t, 16)) }()

	select {
	case f := <-conn.FrameQueue:
		if f == nil {
			t.Fatal("FrameQueue closed — reader died instead of reading the post-park frame")
		}
	case <-readerDone:
		t.Fatal("reader closed instead of reading the post-park frame (stale-deadline false-close)")
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not read the post-park frame")
	}

	clientConn.Close()
	<-readerDone
}

// TestReaderPause_LostWakeupFree proves BUG B (lost wakeup): across many
// clear/wake cycles the reader must always resume once alarmState is 0. A frame
// is fed each cycle; a lost wakeup would leave the reader parked and hang the
// FrameQueue receive. Run under -race to exercise the store-before-poke ordering
// against the reader's post-wake re-check.
func TestReaderPause_LostWakeupFree(t *testing.T) {
	srv := makeTestServer()
	srv.Connections = map[string]*protocol.Connection{}
	srv.alarmParkProbe = 10 * time.Second // rely on the wake path, not the probe
	conn, clientConn := registerPauseConn(t, srv)
	defer clientConn.Close()
	conn.HeartbeatSec.Store(0)

	srv.applyAlarmBits(AlarmMemory) // arm; reader parks on start
	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	const cycles = 500
	for i := 0; i < cycles; i++ {
		srv.applyAlarmBits(0) // clear + poke wake — reader must resume
		go func() { _, _ = clientConn.Write(nonAckPublishFrameBytes(t, 8)) }()
		select {
		case f := <-conn.FrameQueue:
			if f == nil {
				t.Fatalf("cycle %d: FrameQueue closed; reader exited", i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("cycle %d: reader wedged parked after clear (lost wakeup)", i)
		}
		srv.applyAlarmBits(AlarmMemory) // re-arm; reader re-parks on its next iteration
	}

	srv.applyAlarmBits(0)
	clientConn.Close()
	<-readerDone
}

// TestReaderPause_HeartbeatsDisabledLiveness proves BUG C (heartbeat-disabled
// liveness): with hb==0 and an alarm that never clears, a parked reader must
// still unwind when the peer dies — via the bounded park re-arm that breaks out
// to probe the socket.
func TestReaderPause_HeartbeatsDisabledLiveness(t *testing.T) {
	srv := makeTestServer()
	srv.Connections = map[string]*protocol.Connection{}
	srv.alarmParkProbe = 100 * time.Millisecond // fast liveness re-arm
	conn, clientConn := registerPauseConn(t, srv)
	conn.HeartbeatSec.Store(0) // heartbeats disabled

	srv.applyAlarmBits(AlarmMemory) // arm and never clear
	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	time.Sleep(150 * time.Millisecond) // let it park at least once
	clientConn.Close()                 // peer dies

	select {
	case <-readerDone:
		// correct: reader unwound on peer death despite hb==0 and an active alarm
	case <-time.After(3 * time.Second):
		t.Fatal("reader wedged parked on a dead connection (hb==0, alarm never cleared)")
	}
}
