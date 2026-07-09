package server

import (
	"bufio"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// SQ-0 step 6 reader-overflow backstops, re-scoped for SQ-13 reader
// backpressure.
//
// Under SQ-13 the reader BLOCKS on the FrameQueue hand-off while backlogged
// (blocking IS the backpressure), so on a healthy connection the backlog is
// paced to the consumer's drain rate and never accumulates. The two SQ-0
// backstops therefore now fire ONLY for a GENUINELY WEDGED flooder: a client
// that floods publishes AND never lets its own FrameQueue drain. In that state
// the reader's probe breaks the hand-off block every probe interval to read one
// more frame, so the backlog still grows one frame per probe interval into:
//   - the soft threshold that asserts channel.flow(active=false) to the client
//     (best-effort ask to pause publishing), and
//   - the hard cap that closes the connection (memory must not grow without
//     bound even for a peer that ignores flow and never drains).
//
// These tests drive readFrames directly over a synchronous net.Pipe with NO
// drainer on FrameQueue — modelling exactly that wedged flooder — and set a
// small backpressure probe so the probe reads advance to the cap/threshold
// promptly and deterministically.
// ============================================================================

// nonAckPublishFrameBytes builds the wire bytes of a basic.publish method frame
// (class 60, method 40) on channel 1 with a payload of the requested size. It is
// deliberately NOT an ack frame, so readFrames routes it to FrameQueue/pending
// rather than diverting it to the AckQueue.
func nonAckPublishFrameBytes(t testing.TB, payloadSize int) []byte {
	t.Helper()
	frame := protocol.EncodeMethodFrameForChannel(1, 60, 40, make([]byte, payloadSize))
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	return data
}

// newOverflowTestConn builds a Connection wired to serverConn with a
// deliberately tiny FrameQueue (cap 1) so the reader spills into the overflow
// buffer after a single frame, and no processFrames draining it.
func newOverflowTestConn(serverConn net.Conn) *protocol.Connection {
	return &protocol.Connection{
		ID:   "overflow-test",
		Conn: serverConn,
		// readFrames reads through Reader (SQ-1), never the raw Conn; a
		// struct-literal Connection must wire it up explicitly.
		Reader:     bufio.NewReader(serverConn),
		FrameQueue: make(chan *protocol.Frame, 1),
		AckQueue:   make(chan *protocol.Frame, 16),
		Done:       make(chan struct{}),
	}
}

// TestReaderOverflowHardCapClosesConnection verifies the hard cap for a wedged
// flooder: with FrameQueue never drained the reader blocks on the hand-off, but
// its probe reads one more frame per interval, so the backlog still crosses
// ReaderOverflowHardCapBytes and the reader closes the connection instead of
// buffering without bound.
func TestReaderOverflowHardCapClosesConnection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	srv := makeTestServer()
	srv.Config.Network.ReaderOverflowFlowBytes = 0 // isolate the hard cap
	srv.Config.Network.ReaderOverflowHardCapBytes = 200
	srv.readerBackpressureProbe = time.Millisecond // advance the wedged backlog promptly

	conn := newOverflowTestConn(serverConn)

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	// Flood ~104-byte publish frames. FrameQueue(1) swallows the first, the next
	// parks the reader on the hand-off, and each probe read piles one more frame
	// into pending — crossing the 200-byte hard cap within a couple of probes.
	go func() {
		data := nonAckPublishFrameBytes(t, 100)
		for i := 0; i < 100; i++ {
			if _, err := clientConn.Write(data); err != nil {
				return // pipe closed by the hard-cap teardown
			}
		}
	}()

	select {
	case <-readerDone:
		// reader returned — connection closed by the hard cap
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not close the connection on hard-cap overflow")
	}
	if !conn.Closed.Load() {
		t.Fatal("expected conn.Closed to be set after hard-cap overflow")
	}
}

// TestReaderOverflowAssertsChannelFlow verifies the soft threshold for a wedged
// flooder: with FrameQueue never drained the probe reads grow the backlog past
// ReaderOverflowFlowBytes, making the reader send channel.flow(active=false) to
// the client, without closing the connection.
func TestReaderOverflowAssertsChannelFlow(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	srv := makeTestServer()
	srv.Config.Network.ReaderOverflowFlowBytes = 200
	srv.Config.Network.ReaderOverflowHardCapBytes = 1 << 30 // effectively disabled
	srv.readerBackpressureProbe = time.Millisecond          // advance the wedged backlog promptly

	conn := newOverflowTestConn(serverConn)
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)
	defer func() { serverConn.Close(); <-readerDone }()

	// Client reader: capture a server-initiated channel.flow(false).
	gotFlowFalse := make(chan struct{}, 1)
	go func() {
		for {
			f, err := protocol.ReadFrame(clientConn)
			if err != nil {
				return
			}
			if f.Type == protocol.FrameMethod && len(f.Payload) >= 5 {
				classID := binary.BigEndian.Uint16(f.Payload[0:2])
				methodID := binary.BigEndian.Uint16(f.Payload[2:4])
				if classID == 20 && methodID == 20 {
					m := &protocol.ChannelFlowMethod{}
					if err := m.Deserialize(f.Payload[4:]); err == nil && !m.Active {
						select {
						case gotFlowFalse <- struct{}{}:
						default:
						}
						return
					}
				}
			}
		}
	}()

	// Client writer: publish frames until the reader crosses the flow threshold.
	go func() {
		data := nonAckPublishFrameBytes(t, 100)
		for i := 0; i < 100; i++ {
			if _, err := clientConn.Write(data); err != nil {
				return
			}
		}
	}()

	select {
	case <-gotFlowFalse:
		// reader asserted channel.flow(false) on overflow
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not assert channel.flow(false) on overflow")
	}
	if conn.Closed.Load() {
		t.Fatal("connection must not be closed at the soft flow threshold (hard cap not reached)")
	}
}
