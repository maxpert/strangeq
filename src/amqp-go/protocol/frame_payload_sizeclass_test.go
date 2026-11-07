package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"
)

// THE FRAME PAYLOAD MUST BE ALLOCATED AT EXACTLY ITS WIRE SIZE.
//
// The single-frame body is DONATED to the stored message (processBodyFrame:
// pendingMsg.Body = frame.Payload), so the payload's backing array is what the
// hot cache, the WAL hand-off and the publish lease keep alive for the whole
// life of a message. Allocating it at size+1 (to read the frame-end octet in
// the same call) costs nothing for most sizes, but at a Go size-class or page
// boundary the one extra byte buys a whole extra class:
//
//	1 KiB body:  make(1025)  -> 1,152 B  (+12.5 %)
//	64 KiB body: make(65537) -> 73,728 B (+12.5 %, one extra 8 KiB page)
//
// and those are exactly the body sizes of the F1-F5 rows. These tests pin the
// payload's capacity to its size and the allocation to the size class of the
// body itself.

func frameStream(t *testing.T, n, payloadSize int) []byte {
	t.Helper()
	payload := make([]byte, payloadSize)
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.Write(AppendFrame(nil, FrameBody, 1, payload))
	}
	return buf.Bytes()
}

func TestReadFrameOptimized_PayloadCapacityIsExactSize(t *testing.T) {
	for _, size := range []int{0, 1, 1024, 4096, 65536} {
		r := bufio.NewReaderSize(bytes.NewReader(frameStream(t, 1, size)), 64*1024)
		f, err := ReadFrameOptimizedWithLimit(r, 0)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if len(f.Payload) != size {
			t.Fatalf("size %d: len(payload) = %d", size, len(f.Payload))
		}
		if size > 0 && cap(f.Payload) != size {
			t.Fatalf("size %d: cap(payload) = %d, want %d: the frame-end octet must not "+
				"share the donated body's backing array", size, cap(f.Payload), size)
		}
		PutFrame(f)
	}
}

// bytesPerRead returns the heap bytes allocated per ReadFrameOptimizedWithLimit
// call for frames of the given payload size, read through a bufio.Reader the
// way the server's readFrames does.
func bytesPerRead(t *testing.T, size int) float64 {
	t.Helper()
	const n = 200
	stream := frameStream(t, n, size)
	r := bufio.NewReaderSize(bytes.NewReader(stream), 64*1024)
	keep := make([]*Frame, 0, n)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		f, err := ReadFrameOptimizedWithLimit(r, 0)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		keep = append(keep, f)
	}
	runtime.ReadMemStats(&after)
	for _, f := range keep {
		PutFrame(f)
	}
	return float64(after.TotalAlloc-before.TotalAlloc) / n
}

func TestReadFrameOptimized_BodyAllocationStaysInItsSizeClass(t *testing.T) {
	// 1 KiB: the 1,024 B class, not the 1,152 B class. The slack covers the
	// pooled *Frame (amortised) and nothing else.
	if got := bytesPerRead(t, 1024); got > 1024+96 {
		t.Fatalf("1 KiB frame: %.0f B allocated per read, want <= %d (the 1,152 B size "+
			"class means the payload was allocated at size+1)", got, 1024+96)
	}
	// 64 KiB: 8 pages, not 9.
	if got := bytesPerRead(t, 65536); got > 65536+1024 {
		t.Fatalf("64 KiB frame: %.0f B allocated per read, want <= %d (73,728 B means the "+
			"payload was allocated at size+1 and spilled onto a ninth page)", got, 65536+1024)
	}
}

func TestReadFrameOptimized_BadFrameEndStillRejected(t *testing.T) {
	raw := frameStream(t, 1, 1024)
	raw[len(raw)-1] = 0x00
	_, err := ReadFrameOptimizedWithLimit(bufio.NewReader(bytes.NewReader(raw)), 0)
	if err == nil {
		t.Fatal("a frame with a bad frame-end octet was accepted")
	}
}

func TestReadFrameOptimized_TruncatedFrameEndIsUnexpectedEOF(t *testing.T) {
	raw := frameStream(t, 1, 1024)
	raw = raw[:len(raw)-1] // payload complete, frame-end octet missing
	_, err := ReadFrameOptimizedWithLimit(bufio.NewReader(bytes.NewReader(raw)), 0)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated frame: err = %v, want io.ErrUnexpectedEOF", err)
	}
	// An unbuffered reader takes the same path.
	_, err = ReadFrameOptimizedWithLimit(bytes.NewReader(raw), 0)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated frame (unbuffered): err = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReadFrame_PayloadCapacityIsExactSize(t *testing.T) {
	f, err := ReadFrame(bytes.NewReader(frameStream(t, 1, 1024)))
	if err != nil {
		t.Fatal(err)
	}
	if cap(f.Payload) != 1024 {
		t.Fatalf("cap(payload) = %d, want 1024", cap(f.Payload))
	}
}

// endReadCounter counts every underlying Read call. On a real connection each
// is a read(2) syscall, so the count is the syscall volume of the read path.
type endReadCounter struct {
	r     io.Reader
	reads int
}

func (c *endReadCounter) Read(p []byte) (int, error) {
	c.reads++
	return c.r.Read(p)
}

// TestReadFrameOptimized_EndOctetIsBufferServed proves that reading the
// frame-end octet separately (a1) does NOT add a read syscall on the production
// path.
//
// The production connection reader is a *bufio.Reader (protocol/structures.go
// NewConnectionWithReadBuffer; 64 KiB by default). readFrameEnd takes the
// io.ByteReader fast path, and because the preceding 7-byte header read always
// leaves the buffer non-empty, ReadByte is served from the buffer. Reading N
// frames must therefore cost no more underlying reads than the old single
// size+1 read. This is the worst case for the argument: a 64 KiB payload with a
// 64 KiB buffer (the F3 row) -- the frame is larger than the buffer, so a
// refill is owed per frame either way, and the end octet must still come from
// the buffer rather than forcing a second one.
func TestReadFrameOptimized_EndOctetIsBufferServed(t *testing.T) {
	const n = 64
	for _, size := range []int{1024, 65536} {
		cr := &endReadCounter{r: bytes.NewReader(frameStream(t, n, size))}
		br := bufio.NewReaderSize(cr, DefaultReadBufferSize)
		for i := 0; i < n; i++ {
			f, err := ReadFrameOptimizedWithLimit(br, 0)
			if err != nil {
				t.Fatalf("payload %d: frame %d: %v", size, i, err)
			}
			PutFrame(f)
		}
		// One buffer fill per frame (the initial fill plus at most one refill
		// per frame); a separate kernel read for the end octet would make this
		// ~2 per frame.
		if cr.reads > n+1 {
			t.Fatalf("payload %d: %d underlying reads for %d frames (%.3f/frame); the "+
				"frame-end octet must be served from the bufio.Reader, not read from the "+
				"kernel", size, cr.reads, n, float64(cr.reads)/n)
		}
	}
}
