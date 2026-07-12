package server

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"runtime"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the zero-copy vectored (writev) delivery path added for
// large message bodies. The gate (writevBodyThreshold) routes any batch
// carrying a body >= 32 KiB through net.Buffers so the body is referenced in
// place instead of being memmoved into a contiguous frame buffer. Small-body
// batches keep the byte-identical contiguous path.
//
// The correctness contract we pin: for every gated (large-body) delivery the
// bytes emitted on the wire are IDENTICAL to what the pre-existing contiguous
// serializer (serializeDeliveryInto) would have produced. The behavioural
// contract we pin: the gate actually routes large bodies through the scattered
// path (W6) and that path allocates no per-delivery body-sized buffer (W7).

// seededBody returns an n-byte body whose contents are position-dependent, so
// any offset/aliasing/torn-splice bug in the vectored builder shows up as a
// byte mismatch rather than passing by luck.
func seededBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// referenceDeliveryStream serializes the batch through the UNCHANGED contiguous
// serializer, producing the canonical byte stream the vectored path must match
// exactly. It mirrors the channel==nil branch of sendBatchedDeliveries (wire
// tag == delivery.DeliveryTag), which is the case makeTestServer produces.
func referenceDeliveryStream(t *testing.T, srv *Server, channelID uint16, consumerTag string, deliveries []*protocol.Delivery) []byte {
	t.Helper()
	var ref []byte
	for _, d := range deliveries {
		require.NoError(t, srv.serializeDeliveryInto(&ref, channelID, consumerTag,
			d.DeliveryTag, d.Redelivered, d.Exchange, d.RoutingKey, d.Message))
	}
	return ref
}

// driveBatched runs sendBatchedDeliveries over a net.Pipe (a non-*net.TCPConn,
// so net.Buffers.WriteTo takes its sequential fallback) and returns the exact
// bytes the client side received.
func driveBatched(t *testing.T, srv *Server, channelID uint16, consumerTag string, deliveries []*protocol.Delivery) []byte {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	require.NoError(t, srv.sendBatchedDeliveries(conn, channelID, consumerTag, deliveries))

	clientConn.Close()
	<-drainDone
	return received.Bytes()
}

// W1 — byte identity, single body frame, durable and transient, at and above
// the gate threshold. Proves the vectored splice reproduces the contiguous
// bytes exactly and the body round-trips bit-for-bit.
func TestWritev_ByteIdentical_SingleFrame(t *testing.T) {
	sizes := []int{writevBodyThreshold, 64 * 1024, 100 * 1024}
	for _, size := range sizes {
		for _, mode := range []uint8{2, 1} { // durable, transient
			body := seededBody(size)
			d := makeDelivery(1, body)
			d.Message.DeliveryMode = mode

			srv := makeTestServer()
			deliveries := []*protocol.Delivery{d}

			ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)
			got := driveBatched(t, srv, 1, "test-consumer", deliveries)

			require.Equal(t, ref, got,
				"vectored stream must be byte-identical to contiguous (size=%d mode=%d)", size, mode)

			// Independently confirm the delivered body round-trips.
			frames := parseFrames(t, got)
			require.Equal(t, 3, len(frames), "size=%d: method+header+body", size)
			assert.Equal(t, byte(protocol.FrameBody), frames[2].Type)
			assert.Equal(t, body, frames[2].Payload, "size=%d mode=%d: body bytes must match", size, mode)
		}
	}
}

// W2 — byte identity, multi body frame. A small negotiated MaxFrameSize forces
// the 64 KiB body to split into many body frames, exercising the 2+3k iovec
// interleave (prefix / body / 0xCE per chunk).
func TestWritev_ByteIdentical_MultiFrame(t *testing.T) {
	srv := makeTestServer()
	srv.Config.Server.MaxFrameSize = 4096 // 64KB body => many body frames

	body := seededBody(64 * 1024)
	d := makeDelivery(1, body)
	deliveries := []*protocol.Delivery{d}

	ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)
	got := driveBatched(t, srv, 1, "test-consumer", deliveries)
	require.Equal(t, ref, got, "multi-frame vectored stream must equal contiguous")

	frames := parseFrames(t, got)
	// method + header + ceil(65536 / (4096-8)) body frames
	maxBodyPerFrame := 4096 - 8
	wantBodyFrames := (len(body) + maxBodyPerFrame - 1) / maxBodyPerFrame
	require.Equal(t, 2+wantBodyFrames, len(frames))

	var reassembled []byte
	for _, f := range frames {
		if f.Type == protocol.FrameBody {
			require.LessOrEqual(t, int(f.Size), maxBodyPerFrame, "each body frame within frame-max")
			reassembled = append(reassembled, f.Payload...)
		}
	}
	assert.Equal(t, body, reassembled, "reassembled multi-frame body must be bit-identical")
}

// W3 — a batch spanning multiple large messages (covers the >=3x64KB case that
// used to hit the unpooled make()). Assert order + per-message byte identity.
func TestWritev_ByteIdentical_BatchLargeBodies(t *testing.T) {
	for _, n := range []int{2, 3, 5} {
		srv := makeTestServer()
		deliveries := make([]*protocol.Delivery, n)
		bodies := make([][]byte, n)
		for i := 0; i < n; i++ {
			bodies[i] = seededBody(64*1024 + i) // distinct lengths/content per message
			deliveries[i] = makeDelivery(uint64(i+1), bodies[i])
		}

		ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)
		got := driveBatched(t, srv, 1, "test-consumer", deliveries)
		require.Equal(t, ref, got, "n=%d: batched vectored stream must equal contiguous", n)

		frames := parseFrames(t, got)
		require.Equal(t, n*3, len(frames), "n=%d: 3 frames per delivery", n)
		for i := 0; i < n; i++ {
			assert.Equal(t, byte(protocol.FrameMethod), frames[i*3].Type)
			assert.Equal(t, byte(protocol.FrameHeader), frames[i*3+1].Type)
			assert.Equal(t, byte(protocol.FrameBody), frames[i*3+2].Type)
			assert.Equal(t, bodies[i], frames[i*3+2].Payload, "n=%d msg=%d body order/identity", n, i)
		}
	}
}

// W4a — sequential fallback correctness over net.Pipe with a segmenting reader.
// driveBatched already uses net.Pipe; here we read one byte at a time to force
// the WriteTo fallback through many partial reads and assert nothing tears.
func TestWritev_Fallback_NetPipeByteReader(t *testing.T) {
	srv := makeTestServer()
	body := seededBody(64 * 1024)
	deliveries := []*protocol.Delivery{makeDelivery(1, body)}
	ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	got := make([]byte, len(ref))
	readDone := make(chan error, 1)
	go func() {
		// io.ReadFull drains exactly len(ref) bytes; the pipe delivers them in
		// whatever chunks the writer's per-iovec Writes produce.
		_, err := io.ReadFull(clientConn, got)
		readDone <- err
	}()

	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries))
	require.NoError(t, <-readDone)
	require.Equal(t, ref, got, "fallback over net.Pipe must be byte-identical")
}

// W4b — correctness over *tls.Conn. A tls.Conn is not a *net.TCPConn, so
// net.Buffers.WriteTo falls back to a per-iovec Write (each becomes its own TLS
// record). We must pass conn.Conn (the *tls.Conn) so TLS is never bypassed; the
// decrypted stream must be byte-identical to the contiguous serialization.
func TestWritev_Fallback_TLSConn(t *testing.T) {
	certs := generateTestTLSCerts(t)
	cert, err := tls.LoadX509KeyPair(certs.CertFile, certs.KeyFile)
	require.NoError(t, err)

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	clientTLS := tls.Client(c1, &tls.Config{InsecureSkipVerify: true})
	serverTLS := tls.Server(c2, &tls.Config{Certificates: []tls.Certificate{cert}})

	hs := make(chan error, 2)
	go func() { hs <- clientTLS.Handshake() }()
	go func() { hs <- serverTLS.Handshake() }()
	for i := 0; i < 2; i++ {
		require.NoError(t, <-hs)
	}

	srv := makeTestServer()
	body := seededBody(64 * 1024)
	deliveries := []*protocol.Delivery{makeDelivery(1, body)}
	ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)

	conn := protocol.NewConnection(serverTLS)
	got := make([]byte, len(ref))
	readDone := make(chan error, 1)
	go func() {
		_, rerr := io.ReadFull(clientTLS, got)
		readDone <- rerr
	}()

	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries))
	require.NoError(t, <-readDone)
	require.Equal(t, ref, got, "decrypted TLS stream must be byte-identical")
}

// W6 — gate boundary. Over a countingConn (non-TCP, so WriteTo falls back to a
// per-iovec Write), a sub-threshold body takes the contiguous path (one Write),
// while an at-threshold body takes the vectored path emitting exactly three
// iovecs for a single body frame (scaffolding, zero-copy body, shared 0xCE) =>
// three Writes. This is the failing-first assertion for the feature.
func TestWritev_GateBoundary(t *testing.T) {
	cases := []struct {
		name       string
		bodyLen    int
		wantWrites int
	}{
		{"sub-threshold contiguous", writevBodyThreshold - 1, 1},
		{"at-threshold vectored", writevBodyThreshold, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			counter := &countingConn{Conn: serverConn}
			conn := protocol.NewConnection(counter)
			srv := makeTestServer()

			var received bytes.Buffer
			drainDone := make(chan struct{})
			drainConn(clientConn, &received, drainDone)

			deliveries := []*protocol.Delivery{makeDelivery(1, seededBody(tc.bodyLen))}
			require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries))

			clientConn.Close()
			<-drainDone

			assert.Equal(t, tc.wantWrites, counter.getWriteCount(),
				"%s: body=%d expected %d Write calls", tc.name, tc.bodyLen, tc.wantWrites)
		})
	}
}

// discardConn discards all writes without blocking so allocation can be
// measured without a reader goroutine. Only Write is exercised (WriteTo's
// sequential fallback), so the embedded nil net.Conn is never touched.
type discardConn struct{ net.Conn }

func (discardConn) Write(p []byte) (int, error) { return len(p), nil }

// bytesPerOp returns the average bytes allocated per call to f, using the
// cumulative TotalAlloc counter (unaffected by GC), after one warmup call to
// prime pools and lazy state.
func bytesPerOp(runs int, f func()) uint64 {
	f() // warmup
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// W7 — the vectored path allocates no per-delivery body-sized buffer. A batch
// of 3x64KB would allocate ~192 KiB on the contiguous path (the unpooled
// make()) plus copy every body; the vectored path only allocates small
// scaffolding scratch (pooled) + the net.Buffers header. We assert bytes/op is
// far below a single body, which is impossible if the body is copied or the big
// buffer is allocated.
func TestWritev_NoBigAlloc_64KB(t *testing.T) {
	srv := makeTestServer()
	conn := protocol.NewConnection(discardConn{})

	const n = 3
	deliveries := make([]*protocol.Delivery, n)
	for i := 0; i < n; i++ {
		deliveries[i] = makeDelivery(uint64(i+1), seededBody(64*1024))
	}

	perOp := bytesPerOp(200, func() {
		_ = srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries)
	})
	t.Logf("vectored 3x64KB delivery: %d bytes/op", perOp)

	// 32 KiB is half of one body: comfortably below the ~192 KiB the make()
	// would add and below the 64 KiB a single reintroduced body copy would add,
	// yet far above the true steady-state scaffolding allocation.
	assert.Less(t, perOp, uint64(32*1024),
		"vectored path must not allocate a body-sized buffer (got %d bytes/op)", perOp)
}

// Scratch-regrow discipline: a large batch whose small-frame scaffolding
// exceeds the initial pooled scratch capacity forces the scratch slab to
// reallocate mid-build. Byte identity proves sub-slices are materialized only
// after the slab has stopped growing (two-phase build), so a regrow never
// invalidates a body-adjacent scaffolding slice.
func TestWritev_ScratchRegrow_ByteIdentical(t *testing.T) {
	srv := makeTestServer()
	const n = 80 // ~80 * ~60B scaffolding >> 1KiB initial scratch => several regrows
	deliveries := make([]*protocol.Delivery, n)
	for i := 0; i < n; i++ {
		// one large body per message keeps the batch on the vectored path
		deliveries[i] = makeDelivery(uint64(i+1), seededBody(32*1024+i))
	}

	ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)
	got := driveBatched(t, srv, 1, "test-consumer", deliveries)
	require.Equal(t, ref, got, "byte identity must survive scratch regrow")
}

// Empty-body message inside a large-body batch: the empty message must produce
// method+header only (no body frame), and the large message its body frame,
// still byte-identical to the contiguous encoder.
func TestWritev_MixedEmptyAndLargeBody(t *testing.T) {
	srv := makeTestServer()
	empty := makeDelivery(1, nil)
	large := makeDelivery(2, seededBody(64*1024))
	deliveries := []*protocol.Delivery{empty, large}

	ref := referenceDeliveryStream(t, srv, 1, "test-consumer", deliveries)
	got := driveBatched(t, srv, 1, "test-consumer", deliveries)
	require.Equal(t, ref, got, "mixed empty/large batch must be byte-identical")

	frames := parseFrames(t, got)
	// empty: method+header (2), large: method+header+body (3)
	require.Equal(t, 5, len(frames))
	assert.Equal(t, byte(protocol.FrameMethod), frames[0].Type)
	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)
	assert.Equal(t, byte(protocol.FrameMethod), frames[2].Type)
	assert.Equal(t, byte(protocol.FrameHeader), frames[3].Type)
	assert.Equal(t, byte(protocol.FrameBody), frames[4].Type)
}

// sendBasicDeliver (single-delivery path) must take the same gated vectored
// path for large bodies and stay byte-identical to the contiguous serializer.
func TestWritev_SingleDeliver_ByteIdentical(t *testing.T) {
	srv := makeTestServer()
	body := seededBody(64 * 1024)
	msg := &protocol.Message{
		Body:         body,
		Exchange:     "test-exchange",
		RoutingKey:   "test.key",
		ContentType:  "text/plain",
		DeliveryMode: 2,
		DeliveryTag:  7,
	}

	var ref []byte
	require.NoError(t, srv.serializeDeliveryInto(&ref, 1, "ct", 7, false, "test-exchange", "test.key", msg))

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	got := make([]byte, len(ref))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(clientConn, got)
		readDone <- err
	}()

	require.NoError(t, srv.sendBasicDeliver(conn, 1, "ct", 7, false, "test-exchange", "test.key", msg))
	require.NoError(t, <-readDone)
	require.Equal(t, ref, got, "sendBasicDeliver vectored path must be byte-identical")
}
