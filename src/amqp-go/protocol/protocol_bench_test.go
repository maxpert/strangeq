package protocol

import (
	"bytes"
	"testing"
)

// Benchmark frame marshaling
func BenchmarkFrameMarshalBinary(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: make([]byte, 100),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := frame.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark frame unmarshaling
func BenchmarkFrameUnmarshalBinary(b *testing.B) {
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
		err := f.UnmarshalBinary(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark different frame sizes
func BenchmarkFrameSizes(b *testing.B) {
	sizes := []int{100, 1024, 4096, 16384}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			frame := &Frame{
				Type:    FrameBody,
				Channel: 1,
				Payload: make([]byte, size),
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, _ := frame.MarshalBinary()
				f := &Frame{}
				_ = f.UnmarshalBinary(data)
			}
		})
	}
}

// Benchmark ReadFrame from buffer
func BenchmarkReadFrame(b *testing.B) {
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
		_, err := ReadFrame(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark method serialization
func BenchmarkMethodSerialization(b *testing.B) {
	b.Run("ConnectionStart", func(b *testing.B) {
		method := &ConnectionStartMethod{
			VersionMajor: 0,
			VersionMinor: 9,
			ServerProperties: map[string]interface{}{
				"product": "test",
				"version": "1.0",
			},
			Mechanisms: []string{"PLAIN"},
			Locales:    []string{"en_US"},
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := method.Serialize()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ConnectionStartOK", func(b *testing.B) {
		method := &ConnectionStartOKMethod{
			ClientProperties: map[string]interface{}{
				"product": "client",
			},
			Mechanism: "PLAIN",
			Response:  []byte("\x00guest\x00guest"),
			Locale:    "en_US",
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := method.Serialize()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BasicPublish", func(b *testing.B) {
		method := &BasicPublishMethod{
			Exchange:   "test.exchange",
			RoutingKey: "test.key",
			Mandatory:  false,
			Immediate:  false,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := method.Serialize()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BasicDeliver", func(b *testing.B) {
		method := &BasicDeliverMethod{
			ConsumerTag: "consumer-123",
			DeliveryTag: 1,
			Redelivered: false,
			Exchange:    "test.exchange",
			RoutingKey:  "test.key",
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := method.Serialize()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark field table encoding
func BenchmarkFieldTableEncoding(b *testing.B) {
	b.Run("SmallTable", func(b *testing.B) {
		table := map[string]interface{}{
			"key1": "value1",
			"key2": int32(42),
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := EncodeFieldTable(table)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MediumTable", func(b *testing.B) {
		table := map[string]interface{}{
			"key1":  "value1",
			"key2":  int32(42),
			"key3":  true,
			"key4":  "another string",
			"key5":  int32(999),
			"key6":  false,
			"key7":  "more data",
			"key8":  int32(12345),
			"key9":  "test value",
			"key10": int32(67890),
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := EncodeFieldTable(table)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("NestedTable", func(b *testing.B) {
		table := map[string]interface{}{
			"key1": "value1",
			"nested": map[string]interface{}{
				"inner1": "inner value",
				"inner2": int32(100),
			},
			"key2": int32(42),
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := EncodeFieldTable(table)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark field table decoding
func BenchmarkFieldTableDecoding(b *testing.B) {
	table := map[string]interface{}{
		"key1": "value1",
		"key2": int32(42),
		"key3": true,
		"key4": "another string",
		"key5": int32(999),
	}
	encoded, _ := EncodeFieldTable(table)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := decodeFieldTable(encoded, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark string encoding
func BenchmarkStringEncoding(b *testing.B) {
	b.Run("ShortString", func(b *testing.B) {
		str := "hello world"

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = EncodeShortString(str)
		}
	})

	b.Run("LongString", func(b *testing.B) {
		str := string(make([]byte, 1000))

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = encodeLongString(str)
		}
	})
}

// Benchmark string decoding
func BenchmarkStringDecoding(b *testing.B) {
	b.Run("ShortString", func(b *testing.B) {
		encoded := EncodeShortString("hello world")

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _ = decodeShortString(encoded, 0)
		}
	})

	b.Run("LongString", func(b *testing.B) {
		encoded := encodeLongString(string(make([]byte, 1000)))

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _ = decodeLongString(encoded, 0)
		}
	})
}

// Benchmark content header serialization
func BenchmarkContentHeaderSerialization(b *testing.B) {
	b.Run("MinimalHeader", func(b *testing.B) {
		header := &ContentHeader{
			ClassID:  60,
			BodySize: 100,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := header.Serialize()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FullHeader", func(b *testing.B) {
		header := &ContentHeader{
			ClassID:  60,
			BodySize: 1024,
			PropertyFlags: FlagContentType | FlagContentEncoding | FlagHeaders |
				FlagDeliveryMode | FlagPriority | FlagCorrelationID |
				FlagReplyTo | FlagExpiration | FlagMessageID | FlagTimestamp,
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			Headers: map[string]interface{}{
				"x-custom": "value",
			},
			DeliveryMode:  2,
			Priority:      5,
			CorrelationID: "correlation-123",
			ReplyTo:       "reply-queue",
			Expiration:    "60000",
			MessageID:     "msg-456",
			Timestamp:     1609459200,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := header.Serialize()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark content header deserialization
func BenchmarkContentHeaderDeserialization(b *testing.B) {
	header := &ContentHeader{
		ClassID:       60,
		BodySize:      1024,
		PropertyFlags: FlagContentType | FlagDeliveryMode,
		ContentType:   "application/json",
		DeliveryMode:  2,
	}
	headerData, _ := header.Serialize()
	frame := &Frame{
		Type:    FrameHeader,
		Channel: 1,
		Payload: headerData,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := ReadContentHeader(frame)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark encode method frame (full pipeline)
func BenchmarkEncodeMethodFrame(b *testing.B) {
	methodData := []byte{0x01, 0x02, 0x03, 0x04}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeMethodFrame(10, 10, methodData)
	}
}

// Benchmark concurrent frame operations
func BenchmarkConcurrentFrameOperations(b *testing.B) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: make([]byte, 100),
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, _ := frame.MarshalBinary()
			f := &Frame{}
			_ = f.UnmarshalBinary(data)
		}
	})
}
