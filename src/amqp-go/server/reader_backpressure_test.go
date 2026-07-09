package server

import (
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// SQ-13 reader TCP backpressure.
//
// Root cause: the 1P/1C transient. A producer floods faster than processFrames
// drains conn.FrameQueue. The old reader refused to block: it buffered every
// unread frame into an in-memory `pending` slice up to the 64 MiB hard cap and
// then force-closed the connection, losing the backlog and collapsing
// throughput to dead air.
//
// The fix converts the reader from "buffer-until-death" to
// "block-for-TCP-backpressure": while a backlog persists (FrameQueue full) the
// reader BLOCKS on the FrameQueue hand-off instead of reading more frames off
// the socket. Not reading the socket IS the backpressure — the kernel RX window
// fills, the producer's write() blocks, and the producer is paced to
// processFrames' drain rate. The block is bounded by a probe timer so an ack or
// control frame trapped behind the flood on a shared connection is still read
// within one probe interval (keeping the backpressure-relief path live and
// avoiding the ack-starvation / consume-staleness deadlocks the design flagged).
//
// These tests drive readFrames directly over a synchronous net.Pipe with a
// tiny FrameQueue, mirroring reader_overflow_test.go / reader_pause_test.go.
// net.Pipe blocks the client Write the instant the reader stops reading, which
// is exactly the backpressure signal — no port 5672 needed. Run under -race.
// ============================================================================

// basicConsumeFrameBytes builds the wire bytes of a basic.consume method frame
// (class 60, method 20) on channel 1. It is NOT an ack frame, so readFrames
// routes it through pending/FrameQueue rather than diverting it to AckQueue —
// modelling a control frame trapped behind a publish flood on a shared
// connection.
func basicConsumeFrameBytes(t testing.TB) []byte {
	t.Helper()
	frame := protocol.EncodeMethodFrameForChannel(1, 60, protocol.BasicConsume, make([]byte, 16))
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	return data
}

// TestReaderBackpressures_NoCloseWhenBacklogged is the core proving test. A
// publisher floods far more than the hard-cap worth of bytes while a slow
// drainer models a throttled processFrames. The old reader buffered the flood
// into `pending`, blew the hard cap and force-closed (losing frames); the fixed
// reader blocks on the FrameQueue hand-off so the flood is paced to the
// drainer, `pending` stays tiny, the hard cap is never reached, and every frame
// is delivered in order.
func TestReaderBackpressures_NoCloseWhenBacklogged(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	srv := makeTestServer()
	srv.Config.Network.ReaderOverflowFlowBytes = 0      // isolate the hard cap: no flow signal
	srv.Config.Network.ReaderOverflowHardCapBytes = 500 // tiny cap: ~5 frames of backlog
	// Large probe so the blocking hand-off (not the probe read) paces the flood;
	// the drainer frees a slot long before the probe could fire.
	srv.readerBackpressureProbe = 200 * time.Millisecond

	conn := newOverflowTestConn(serverConn)

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	const nFrames = 50
	const payload = 100 // ~104 wire bytes each; 50 * 104 = 5200 bytes >> 500 hard cap

	// Slow drainer: dequeue one frame per ~1ms, modelling a throttled
	// processFrames. Because it drains faster than the 200ms probe, the reader's
	// blocking hand-off always wins and `pending` never accumulates.
	allDrained := make(chan struct{})
	go func() {
		count := 0
		for f := range conn.FrameQueue {
			if f != nil {
				count++
			}
			if count == nFrames {
				close(allDrained)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		data := nonAckPublishFrameBytes(t, payload)
		for i := 0; i < nFrames; i++ {
			if _, err := clientConn.Write(data); err != nil {
				return // pipe closed unexpectedly (e.g. a hard-cap teardown)
			}
		}
	}()

	select {
	case <-allDrained:
		// correct: the reader paced the flood to the drainer and delivered every frame
	case <-readerDone:
		t.Fatal("reader closed the connection while backlogged (hard-cap close) — backpressure not applied")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out before all frames drained")
	}
	if conn.Closed.Load() {
		t.Fatal("connection must not be closed under backpressure (hard cap must never be reached)")
	}
}

// TestReaderProbeDrainsAckWhileBacklogged proves the ack-starvation invariant.
// With the FrameQueue full and never drained, the reader parks on the hand-off.
// A basic.ack sent behind the flood must still be read within a few probe
// intervals and diverted to AckQueue — the very path that releases the prefetch
// gate on a shared publish+consume connection. A naive "never read while
// blocked" implementation traps the ack forever and this fails.
func TestReaderProbeDrainsAckWhileBacklogged(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	srv := makeTestServer()
	srv.Config.Network.ReaderOverflowFlowBytes = 0
	srv.Config.Network.ReaderOverflowHardCapBytes = 1 << 30 // effectively disabled
	srv.readerBackpressureProbe = 20 * time.Millisecond     // tiny probe

	conn := newOverflowTestConn(serverConn) // FrameQueue cap 1, never drained

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	// Establish a backlog: pub1 fills FrameQueue(1), pub2 lands in pending and the
	// reader parks on the hand-off. Then send the ack behind them.
	go func() {
		pub := nonAckPublishFrameBytes(t, 32)
		if _, err := clientConn.Write(pub); err != nil {
			return
		}
		if _, err := clientConn.Write(pub); err != nil {
			return
		}
		_, _ = clientConn.Write(ackFrameBytes(t))
	}()

	select {
	case f := <-conn.AckQueue:
		if f == nil {
			t.Fatal("AckQueue closed instead of delivering the ack")
		}
		// correct: the probe-fire read reached the ack behind the flood
	case <-readerDone:
		t.Fatal("reader exited instead of probing for the ack")
	case <-time.After(3 * time.Second):
		t.Fatal("ack trapped behind the publish flood — probe never drained it (ack-starvation deadlock)")
	}

	clientConn.Close()
	<-readerDone
}

// TestReaderProbeDrainsControlFrame is the debate's deadlock catch: a non-ack
// control frame (basic.consume) trapped behind a publish flood on a backlogged
// connection must still be read within a few probe intervals — proving no
// consume-staleness hang. Unlike an ack it is not diverted to AckQueue, so we
// observe it via the client Write completing: on net.Pipe the Write returns only
// once the reader has consumed those bytes off the socket. A naive "never read
// while blocked" implementation leaves the reader parked, the Write blocks
// forever, and this fails/hangs.
func TestReaderProbeDrainsControlFrame(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	srv := makeTestServer()
	srv.Config.Network.ReaderOverflowFlowBytes = 0
	srv.Config.Network.ReaderOverflowHardCapBytes = 1 << 30 // effectively disabled
	srv.readerBackpressureProbe = 20 * time.Millisecond     // tiny probe

	conn := newOverflowTestConn(serverConn) // FrameQueue cap 1, never drained

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	// pub1 -> FrameQueue(1); pub2 -> pending (reader parks). Both writes complete
	// while the reader is still draining normally.
	pub := nonAckPublishFrameBytes(t, 32)
	if _, err := clientConn.Write(pub); err != nil {
		t.Fatalf("write pub1: %v", err)
	}
	if _, err := clientConn.Write(pub); err != nil {
		t.Fatalf("write pub2: %v", err)
	}

	// The reader is now backlogged and parked on the hand-off. This trailing
	// control-frame write completes only if the reader's probe reads it off the
	// socket while backlogged.
	consumeWritten := make(chan error, 1)
	go func() {
		_, err := clientConn.Write(basicConsumeFrameBytes(t))
		consumeWritten <- err
	}()

	select {
	case err := <-consumeWritten:
		if err != nil {
			t.Fatalf("control-frame write failed: %v", err)
		}
		// correct: the probe-fire read consumed the control frame while backlogged
	case <-readerDone:
		t.Fatal("reader exited instead of probing for the control frame")
	case <-time.After(3 * time.Second):
		t.Fatal("control frame trapped behind the publish flood — probe never read it (consume-staleness hang)")
	}

	clientConn.Close()
	<-readerDone
}

// TestReaderBackpressureProbeKnob is the unit test for the probe interval knob:
// the config default, the config override, the Server test-override precedence,
// and the zero-falls-back-to-default behaviour, mirroring alarmParkProbe.
func TestReaderBackpressureProbeKnob(t *testing.T) {
	srv := makeTestServer()

	if got := srv.readerBackpressureProbeInterval(); got != defaultReaderBackpressureProbe {
		t.Fatalf("default probe interval = %v, want %v", got, defaultReaderBackpressureProbe)
	}

	srv.Config.Network.ReaderBackpressureProbeMS = 25
	if got, want := srv.readerBackpressureProbeInterval(), 25*time.Millisecond; got != want {
		t.Fatalf("config probe interval = %v, want %v", got, want)
	}

	srv.readerBackpressureProbe = 3 * time.Millisecond // test override wins over config
	if got, want := srv.readerBackpressureProbeInterval(), 3*time.Millisecond; got != want {
		t.Fatalf("override probe interval = %v, want %v", got, want)
	}

	srv.readerBackpressureProbe = 0
	srv.Config.Network.ReaderBackpressureProbeMS = 0 // zero config -> default
	if got := srv.readerBackpressureProbeInterval(); got != defaultReaderBackpressureProbe {
		t.Fatalf("zero-config probe interval = %v, want %v", got, defaultReaderBackpressureProbe)
	}
}
