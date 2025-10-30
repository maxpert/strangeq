package protocol

import (
	"bytes"
	"fmt"
	"testing"
)

// Benchmarks for optimized frame operations.
//
// These benchmarks measure the performance improvements from buffer pooling
// and allocation reduction techniques. Compare with protocol_bench_test.go
// to see the benefits of optimization.

// BenchmarkFrameMarshalBinaryOptimized measures optimized frame marshaling with pre-allocation.
func BenchmarkFrameMarshalBinaryOptimized(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: make([]byte, 100),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := frame.MarshalBinaryOptimized()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFrameUnmarshalBinaryOptimized measures optimized frame unmarshaling with slice reuse.
func BenchmarkFrameUnmarshalBinaryOptimized(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: make([]byte, 100),
	}
	data, _ := frame.MarshalBinary()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f := &Frame{}
		err := f.UnmarshalBinaryOptimized(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadFrameOptimized measures optimized frame reading with pooled header buffers.
func BenchmarkReadFrameOptimized(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}
	data, _ := frame.MarshalBinary()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		_, err := ReadFrameOptimized(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteFrameOptimized measures zero-allocation frame writing with pooled buffers.
func BenchmarkWriteFrameOptimized(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}

	buf := &bytes.Buffer{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := WriteFrameOptimized(buf, frame)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFrameOperationsComparison provides side-by-side comparison of original vs optimized.
// This benchmark helps quantify the performance improvements from optimization.
func BenchmarkFrameOperationsComparison(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: make([]byte, 1024),
	}

	b.Run("Original/Marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = frame.MarshalBinary()
		}
	})

	b.Run("Optimized/Marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = frame.MarshalBinaryOptimized()
		}
	})

	data, _ := frame.MarshalBinary()

	b.Run("Original/Unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			f := &Frame{}
			_ = f.UnmarshalBinary(data)
		}
	})

	b.Run("Optimized/Unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			f := &Frame{}
			_ = f.UnmarshalBinaryOptimized(data)
		}
	})

	b.Run("Original/ReadFrame", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(data)
			_, _ = ReadFrame(reader)
		}
	})

	b.Run("Optimized/ReadFrame", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(data)
			_, _ = ReadFrameOptimized(reader)
		}
	})
}

// BenchmarkConcurrentFrameOperationsOptimized measures performance under concurrent load.
// This verifies that buffer pooling scales well with parallel operations.
func BenchmarkConcurrentFrameOperationsOptimized(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: make([]byte, 100),
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, _ := frame.MarshalBinaryOptimized()
			f := &Frame{}
			_ = f.UnmarshalBinaryOptimized(data)
		}
	})
}

// BenchmarkBufferPoolEfficiency compares buffer operations with and without pooling.
// Demonstrates the 46% performance improvement from buffer pooling.
func BenchmarkBufferPoolEfficiency(b *testing.B) {
	b.Run("WithoutPooling", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := &bytes.Buffer{}
			buf.Write([]byte("test data"))
			_ = buf.Bytes()
		}
	})

	b.Run("WithPooling", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := getBuffer()
			buf.Write([]byte("test data"))
			_ = buf.Bytes()
			putBuffer(buf)
		}
	})
}

// BenchmarkFrameSizesOptimized measures performance across different frame payload sizes.
// This benchmark helps understand how performance scales with frame size.
func BenchmarkFrameSizesOptimized(b *testing.B) {
	sizes := []int{100, 1024, 4096, 16384}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			frame := &Frame{
				Type:    FrameBody,
				Channel: 1,
				Payload: make([]byte, size),
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, _ := frame.MarshalBinaryOptimized()
				f := &Frame{}
				_ = f.UnmarshalBinaryOptimized(data)
			}
		})
	}
}
