package protocol

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

// countingReader wraps an in-memory stream and counts every Read call made
// against it. On a real connection each such call is a read syscall on the
// net.Conn, so this count is a direct proxy for the syscall volume of the read
// path (SQ-1). The purpose of BenchmarkReadFrames is to show that routing frame
// reads through a bufio.Reader collapses the ~2-syscalls-per-frame pattern of
// the raw path (7-byte header read + payload read, ReadFrameOptimizedWithLimit)
// into roughly one underlying read per buffer-fill.
type countingReader struct {
	r     *bytes.Reader
	reads int
}

func (c *countingReader) Read(p []byte) (int, error) {
	c.reads++
	return c.r.Read(p)
}

// buildFrameStream serializes n frames of the given payload size into one
// contiguous byte slice, mimicking a burst of frames arriving on the wire.
func buildFrameStream(n, payloadSize int) []byte {
	payload := make([]byte, payloadSize)
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.Write(AppendFrame(nil, FrameMethod, 1, payload))
	}
	return buf.Bytes()
}

// readAll drains the stream through the real read path and returns the number
// of underlying Read calls made. reader is what ReadFrameOptimizedWithLimit
// reads from (either the raw countingReader or a bufio.Reader wrapping it).
func readAll(b *testing.B, reader io.Reader, cr *countingReader, n int) {
	for i := 0; i < n; i++ {
		frame, err := ReadFrameOptimizedWithLimit(reader, MaxInboundFrameSize)
		if err != nil {
			b.Fatalf("read frame %d: %v", i, err)
		}
		PutFrame(frame)
	}
}

// BenchmarkReadFrames compares the frame read path with and without the SQ-1
// bufio read-coalescing buffer. "Unbuffered" reads straight off the stream (the
// pre-SQ-1 behavior: ReadFrameOptimizedWithLimit issues two io.ReadFull calls
// per frame, each a Read on the underlying conn). "Buffered64K" wraps the same
// stream in a 64 KiB bufio.Reader (the SQ-1 behavior). Both report avg_reads/op
// = underlying Read calls per frame; the buffered path should be far below the
// unbuffered path's ~2.0.
func BenchmarkReadFrames(b *testing.B) {
	const (
		frames      = 256
		payloadSize = 64 // small method-sized frames: the worst case for syscall/frame
	)
	stream := buildFrameStream(frames, payloadSize)

	b.Run("Unbuffered", func(b *testing.B) {
		b.ReportAllocs()
		var totalReads int
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cr := &countingReader{r: bytes.NewReader(stream)}
			readAll(b, cr, cr, frames)
			totalReads += cr.reads
		}
		b.StopTimer()
		b.ReportMetric(float64(totalReads)/float64(b.N*frames), "reads/frame")
	})

	b.Run("Buffered64K", func(b *testing.B) {
		b.ReportAllocs()
		var totalReads int
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cr := &countingReader{r: bytes.NewReader(stream)}
			br := bufio.NewReaderSize(cr, DefaultReadBufferSize)
			readAll(b, br, cr, frames)
			totalReads += cr.reads
		}
		b.StopTimer()
		b.ReportMetric(float64(totalReads)/float64(b.N*frames), "reads/frame")
	})
}
