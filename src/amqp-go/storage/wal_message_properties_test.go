package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allPropertiesMessage builds a protocol.Message with every one of the 14
// optional durable fields set to a distinct, non-default value, plus the
// structural Exchange/RoutingKey/DeliveryMode and a body. It is the canonical
// fixture for the ITER4 "persist the entire protocol.Message" contract: a
// durable round-trip must reconstruct every field bit-identically, including the
// 11 properties the pre-v4 record silently dropped (ContentType,
// ContentEncoding, Priority, CorrelationID, ReplyTo, MessageID, Timestamp, Type,
// UserID, AppID, ClusterID).
func allPropertiesMessage(deathTime time.Time) *protocol.Message {
	return &protocol.Message{
		Exchange:         "amq.topic",
		RoutingKey:       "orders.eu.created",
		DeliveryMode:     2,
		Body:             []byte("the-message-body-payload"),
		ContentType:      "application/json",
		ContentEncoding:  "gzip",
		Priority:         7,
		CorrelationID:    "corr-12345",
		ReplyTo:          "amq.rabbitmq.reply-to.abc",
		Expiration:       "60000",
		MessageID:        "msg-id-98765",
		Timestamp:        1720000000,
		Type:             "OrderCreated",
		UserID:           "publisher-user",
		AppID:            "checkout-service",
		ClusterID:        "cluster-a",
		Headers:          sampleXDeathHeaders(deathTime),
		EnqueueUnixMilli: 1720000000123,
	}
}

// assertAllProperties verifies got carries every field of allPropertiesMessage
// exactly (structural + all 14 optional properties + x-death header types).
func assertAllProperties(t *testing.T, got *protocol.Message, deathTime time.Time) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, "amq.topic", got.Exchange, "Exchange")
	assert.Equal(t, "orders.eu.created", got.RoutingKey, "RoutingKey")
	assert.Equal(t, uint8(2), got.DeliveryMode, "DeliveryMode")
	assert.Equal(t, "the-message-body-payload", string(got.Body), "Body")
	assert.Equal(t, "application/json", got.ContentType, "ContentType")
	assert.Equal(t, "gzip", got.ContentEncoding, "ContentEncoding")
	assert.Equal(t, uint8(7), got.Priority, "Priority")
	assert.Equal(t, "corr-12345", got.CorrelationID, "CorrelationID")
	assert.Equal(t, "amq.rabbitmq.reply-to.abc", got.ReplyTo, "ReplyTo")
	assert.Equal(t, "60000", got.Expiration, "Expiration")
	assert.Equal(t, "msg-id-98765", got.MessageID, "MessageID")
	assert.Equal(t, uint64(1720000000), got.Timestamp, "Timestamp")
	assert.Equal(t, "OrderCreated", got.Type, "Type")
	assert.Equal(t, "publisher-user", got.UserID, "UserID")
	assert.Equal(t, "checkout-service", got.AppID, "AppID")
	assert.Equal(t, "cluster-a", got.ClusterID, "ClusterID")
	assert.Equal(t, int64(1720000000123), got.EnqueueUnixMilli, "EnqueueUnixMilli")
	assertXDeathIntact(t, got.Headers, deathTime)
}

// TestWAL_AllProperties_RoundTrip is the headline ITER4 contract test: a message
// carrying all 14 optional fields is written durably, recovered after a restart,
// and every field must match — including the 11 pre-v4 dropped properties.
func TestWAL_AllProperties_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	deathTime := time.Unix(1720000000, 0).UTC()
	msg := allPropertiesMessage(deathTime)
	require.NoError(t, wm.Write("q", msg, 42))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm2.Close()

	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, 1, "the durable message must recover")
	assert.Equal(t, "q", recovered[0].QueueName)
	assertAllProperties(t, recovered[0].Message, deathTime)
	assert.True(t, recovered[0].Message.Redelivered, "recovered message must be marked redelivered")
}

// TestWAL_Properties_EdgeCases covers the empty-vs-absent semantics, zero
// numbers, max-length and overflow shortstr, and UTF-8 values, each round-tripped
// through WAL write + recover.
func TestWAL_Properties_EdgeCases(t *testing.T) {
	t.Run("all-absent-minimal", func(t *testing.T) {
		msg := &protocol.Message{RoutingKey: "rk", DeliveryMode: 2, Body: []byte("only-body")}
		got := walRoundTrip(t, msg, 1)
		assert.Equal(t, "only-body", string(got.Body))
		assert.Equal(t, "", got.ContentType)
		assert.Equal(t, "", got.ContentEncoding)
		assert.Equal(t, uint8(0), got.Priority)
		assert.Equal(t, "", got.CorrelationID)
		assert.Equal(t, "", got.ReplyTo)
		assert.Equal(t, "", got.Expiration)
		assert.Equal(t, "", got.MessageID)
		assert.Equal(t, uint64(0), got.Timestamp)
		assert.Equal(t, "", got.Type)
		assert.Equal(t, "", got.UserID)
		assert.Equal(t, "", got.AppID)
		assert.Equal(t, "", got.ClusterID)
		assert.Equal(t, int64(0), got.EnqueueUnixMilli)
		assert.Nil(t, got.Headers)
	})

	t.Run("present-empty-strings-recover-empty", func(t *testing.T) {
		// Present-but-empty string properties are encoded as absent (bit clear)
		// and recover as "" — absent and present-empty are equal (unset AMQP prop).
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 1, Body: []byte("b"),
			ContentType: "", CorrelationID: "", Type: "",
		}
		got := walRoundTrip(t, msg, 2)
		assert.Equal(t, "", got.ContentType)
		assert.Equal(t, "", got.CorrelationID)
		assert.Equal(t, "", got.Type)
	})

	t.Run("zero-numbers-absent", func(t *testing.T) {
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 1, Body: []byte("b"),
			Priority: 0, Timestamp: 0, EnqueueUnixMilli: 0,
		}
		got := walRoundTrip(t, msg, 3)
		assert.Equal(t, uint8(0), got.Priority)
		assert.Equal(t, uint64(0), got.Timestamp)
		assert.Equal(t, int64(0), got.EnqueueUnixMilli)
	})

	t.Run("nonzero-numbers-exact", func(t *testing.T) {
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 1, Body: []byte("b"),
			Priority: 255, Timestamp: 1<<63 - 1, EnqueueUnixMilli: -1,
		}
		got := walRoundTrip(t, msg, 4)
		assert.Equal(t, uint8(255), got.Priority)
		assert.Equal(t, uint64(1<<63-1), got.Timestamp)
		assert.Equal(t, int64(-1), got.EnqueueUnixMilli)
	})

	t.Run("max-length-255-shortstr", func(t *testing.T) {
		s := strings.Repeat("Z", 255)
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 2, Body: []byte("b"),
			ContentType: s, ContentEncoding: s, CorrelationID: s, ReplyTo: s,
			Expiration: s, MessageID: s, Type: s, UserID: s, AppID: s, ClusterID: s,
		}
		got := walRoundTrip(t, msg, 5)
		assert.Equal(t, s, got.ContentType)
		assert.Equal(t, s, got.ContentEncoding)
		assert.Equal(t, s, got.CorrelationID)
		assert.Equal(t, s, got.ReplyTo)
		assert.Equal(t, s, got.Expiration)
		assert.Equal(t, s, got.MessageID)
		assert.Equal(t, s, got.Type)
		assert.Equal(t, s, got.UserID)
		assert.Equal(t, s, got.AppID)
		assert.Equal(t, s, got.ClusterID)
	})

	t.Run("256-byte-shortstr-nacked", func(t *testing.T) {
		// A property longer than 255 bytes cannot fit a u8 length prefix.
		// The encoder must fail the write (nack), never truncate/corrupt.
		tmpDir := t.TempDir()
		wm, err := NewWALManager(tmpDir)
		require.NoError(t, err)
		defer wm.Close()
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 2, Body: []byte("b"),
			ContentType: strings.Repeat("X", 256),
		}
		err = wm.Write("q", msg, 6)
		require.Error(t, err, "a 256-byte shortstr property must nack the write, not truncate")
	})

	t.Run("utf8-multibyte", func(t *testing.T) {
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 2, Body: []byte("b"),
			CorrelationID: "café-☕-Ω", Type: "日本語", MessageID: "naïve",
		}
		got := walRoundTrip(t, msg, 7)
		assert.Equal(t, "café-☕-Ω", got.CorrelationID)
		assert.Equal(t, "日本語", got.Type)
		assert.Equal(t, "naïve", got.MessageID)
	})

	t.Run("large-headers-with-xdeath", func(t *testing.T) {
		deathTime := time.Unix(1720000042, 0).UTC()
		h := sampleXDeathHeaders(deathTime)
		h["x-extra-1"] = "value-1"
		h["x-extra-2"] = int64(2)
		h["x-extra-3"] = true
		msg := &protocol.Message{
			RoutingKey: "rk", DeliveryMode: 2, Body: []byte("b"), Headers: h,
		}
		got := walRoundTrip(t, msg, 8)
		assertXDeathIntact(t, got.Headers, deathTime)
		assert.Equal(t, "value-1", got.Headers["x-extra-1"])
		assert.Equal(t, int64(2), got.Headers["x-extra-2"])
		assert.Equal(t, true, got.Headers["x-extra-3"])
	})
}

// walRoundTrip writes msg durably, restarts the WAL manager, recovers, and
// returns the single recovered message.
func walRoundTrip(t *testing.T, msg *protocol.Message, offset uint64) *protocol.Message {
	t.Helper()
	tmpDir := t.TempDir()
	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	require.NoError(t, wm.Write("q", msg, offset))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() { wm2.Close() })

	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	return recovered[0].Message
}

// TestWAL_LiveRead_AllProperties proves the in-process positional read path
// (GetMessage/readMessageAtPosition, used for redelivery of a persisted message)
// reconstructs every property with Redelivered=false.
func TestWAL_LiveRead_AllProperties(t *testing.T) {
	tmpDir := t.TempDir()
	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	deathTime := time.Unix(1720000003, 0).UTC()
	msg := allPropertiesMessage(deathTime)
	require.NoError(t, wm.Write("liveq", msg, 77))
	time.Sleep(50 * time.Millisecond)

	got, err := wm.Read("liveq", 77)
	require.NoError(t, err)
	assertAllProperties(t, got, deathTime)
	assert.False(t, got.Redelivered, "positional live read must not mark redelivered")
}

// TestWAL_TxAtomic_AllProperties proves all properties survive the transaction
// slow path (WriteTxAtomic) and recovery.
func TestWAL_TxAtomic_AllProperties(t *testing.T) {
	tmpDir := t.TempDir()
	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	deathTime := time.Unix(1720000004, 0).UTC()
	msg := allPropertiesMessage(deathTime)
	records := []*RecoveryMessage{{QueueName: "txq", Message: msg, Offset: 500}}
	require.NoError(t, wm.WriteTxAtomic(records))
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm2.Close()

	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, 1, "committed tx message must recover")
	assert.Equal(t, "txq", recovered[0].QueueName)
	assertAllProperties(t, recovered[0].Message, deathTime)
}

// TestSegment_AllProperties_RoundTrip proves checkpointed segment-resident
// messages carry all properties through encode/decode, across a segment-manager
// restart, and through a compaction rewrite.
func TestSegment_AllProperties_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)

	deathTime := time.Unix(1720000005, 0).UTC()
	msg := allPropertiesMessage(deathTime)
	require.NoError(t, sm.Write("segq", msg, 7))

	// In-process read.
	got, err := sm.Read("segq", 7)
	require.NoError(t, err)
	assertAllProperties(t, got, deathTime)

	require.NoError(t, sm.Close())

	// Restart: reopen and recover from disk.
	sm2, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	defer sm2.Close()

	recovered, err := sm2.RecoverFromSegments()
	require.NoError(t, err)
	segMsgs, ok := recovered["segq"]
	require.True(t, ok, "segq must have recovered messages")
	require.Len(t, segMsgs, 1)
	assertAllProperties(t, segMsgs[0].Message, deathTime)
}

// TestSegment_AllProperties_SurviveCompaction proves that a segment compaction
// (which round-trips each surviving message through serialize/deserialize)
// preserves all properties on the unACKed survivors.
func TestSegment_AllProperties_SurviveCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := DefaultSegmentConfig()
	cfg.CompactionThreshold = 0.3
	sm, err := NewSegmentManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	deathTime := time.Unix(1720000006, 0).UTC()
	// Write 5 messages; #1 carries all properties (the survivor we check).
	survivor := allPropertiesMessage(deathTime)
	require.NoError(t, sm.Write("cq", survivor, 1))
	for i := uint64(2); i <= 5; i++ {
		require.NoError(t, sm.Write("cq", &protocol.Message{Body: []byte("filler"), DeliveryMode: 1}, i))
	}

	val, ok := sm.queueSegments.Load("cq")
	require.True(t, ok)
	qs := val.(*QueueSegments)
	qs.mutex.Lock()
	require.NoError(t, qs.sealSegment())
	qs.mutex.Unlock()
	require.NoError(t, qs.openNextSegment())

	// ACK #2..#5 (80% > 30% threshold) so only the all-properties #1 survives.
	for i := uint64(2); i <= 5; i++ {
		sm.Acknowledge("cq", i)
	}
	require.Eventually(t, func() bool {
		qs.bitmapMutex.RLock()
		c := int(qs.ackBitmap.GetCardinality())
		qs.bitmapMutex.RUnlock()
		return c >= 4
	}, 2*time.Second, 5*time.Millisecond)

	qs.tryCompaction()

	got, err := sm.Read("cq", 1)
	require.NoError(t, err, "unACKed all-properties message must survive compaction")
	assertAllProperties(t, got, deathTime)
}

// TestWAL_PropertiesDurableAfterFsync proves the confirm⇒durable chain holds for
// the wider v4 record: Write returns only after the group-commit fsync, and every
// property is recoverable after a simulated restart.
func TestWAL_PropertiesDurableAfterFsync(t *testing.T) {
	tmpDir := t.TempDir()
	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	deathTime := time.Unix(1720000007, 0).UTC()
	msg := allPropertiesMessage(deathTime)
	// Write blocks until the batch is fsynced (durability barrier). Its return
	// nil is the confirm; the record must then be recoverable bit-identically.
	require.NoError(t, wm.Write("dq", msg, 900))
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm2.Close()
	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assertAllProperties(t, recovered[0].Message, deathTime)
}

// TestWAL_V4_ByteGolden freezes the v4 wire format: a hand-built message must
// serialize to an exact expected byte layout (flags word, structural field order,
// u8 length prefixes, bodyKind byte, inline body), and the full record must carry
// the correct record-type tag, length, and CRC. It is the guard against silent
// layout drift.
func TestWAL_V4_ByteGolden(t *testing.T) {
	msg := &protocol.Message{
		Exchange:     "ex",
		RoutingKey:   "rk",
		DeliveryMode: 2,
		Body:         []byte("hi"),
		ContentType:  "ct", // sets flag 0x8000
		Priority:     9,    // sets flag 0x0800
	}
	const offset = uint64(1)
	const queue = "q"

	// Expected payload, byte-for-byte (big-endian). flags = ContentType|Priority
	// = 0x8000 | 0x0800 = 0x8800.
	expectedPayload := []byte{
		0x88, 0x00, // flags
		0x01, 'q', // queue u8-len + "q"
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset u64 = 1
		0x02, 'e', 'x', // exchange u8-len + "ex"
		0x02, 'r', 'k', // routingKey u8-len + "rk"
		0x02,                   // deliveryMode
		0x00,                   // bodyKind inline
		0x00, 0x00, 0x00, 0x02, // bodyLen u32 = 2
		'h', 'i', // body
		0x02, 'c', 't', // ContentType u8-len + "ct" (flag 0x8000)
		0x09, // Priority (flag 0x0800)
	}

	gotPayload, err := appendMessagePayload(nil, queue, msg, offset)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(expectedPayload, gotPayload),
		"v4 payload layout drift\nwant=%x\ngot =%x", expectedPayload, gotPayload)

	// Full record framing: [CRC u32][len u32][recordType u8][payload].
	data := append([]byte{WALRecordTypeMessage}, expectedPayload...)
	expectedLen := uint32(len(data))
	expectedRecord := make([]byte, 8)
	binary.BigEndian.PutUint32(expectedRecord[4:8], expectedLen)
	expectedRecord = append(expectedRecord, data...)
	binary.BigEndian.PutUint32(expectedRecord[0:4], crc32.ChecksumIEEE(expectedRecord[4:]))

	gotRecord, err := appendMessageRecord(nil, queue, msg, offset)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(expectedRecord, gotRecord),
		"v4 record framing drift\nwant=%x\ngot =%x", expectedRecord, gotRecord)

	// An all-absent message must produce a zero flags word.
	empty := &protocol.Message{RoutingKey: "rk", DeliveryMode: 1, Body: []byte("b")}
	emptyPayload, err := appendMessagePayload(nil, queue, empty, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(emptyPayload), 2)
	assert.Equal(t, uint16(0x0000), binary.BigEndian.Uint16(emptyPayload[0:2]),
		"an all-absent message must have flags == 0x0000")
	assert.Equal(t, uint16(0x0000), messagePresenceFlags(empty))
}

// TestWAL_BodyUnion_Seam proves the ITER4 body inline|reference union seam.
func TestWAL_BodyUnion_Seam(t *testing.T) {
	t.Run("normal-message-emits-inline-kind-byte", func(t *testing.T) {
		msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", DeliveryMode: 2, Body: []byte("body")}
		const queue = "q"
		payload, err := appendMessagePayload(nil, queue, msg, 7)
		require.NoError(t, err)

		// bodyKind lives right after the structural prefix:
		// flags(2) + queue(1+len) + offset(8) + exch(1+len) + rk(1+len) + deliveryMode(1).
		bodyKindPos := 2 + (1 + len(queue)) + 8 + (1 + len(msg.Exchange)) + (1 + len(msg.RoutingKey)) + 1
		require.Less(t, bodyKindPos, len(payload))
		assert.Equal(t, bodyKindInline, payload[bodyKindPos], "a normal message must emit bodyKind 0x00 (inline)")
	})

	t.Run("reference-arm-parsed-structurally-without-panic", func(t *testing.T) {
		// Hand-craft a bodyKind==0x01 (reference) record as an ITER5 file would:
		// structural prefix, then [bodyKind 0x01][refLen u16][locator], then some
		// trailing bytes. ITER4 has no resolver, so parse must length-skip the
		// locator structurally and return ok=false WITHOUT panicking.
		locator := []byte("shared-body-locator-opaque")
		var p []byte
		p = binary.BigEndian.AppendUint16(p, 0x0000) // flags: none
		p = append(p, 0x00)                          // queue u8-len 0
		p = binary.BigEndian.AppendUint64(p, 99)     // offset
		p = append(p, 0x00)                          // exchange u8-len 0
		p = append(p, 0x00)                          // routingKey u8-len 0
		p = append(p, 0x02)                          // deliveryMode
		p = append(p, bodyKindReference)             // bodyKind 0x01
		p = binary.BigEndian.AppendUint16(p, uint16(len(locator)))
		p = append(p, locator...)
		p = append(p, []byte("trailing")...) // extra bytes past the locator

		require.NotPanics(t, func() {
			_, _, _, ok := parseMessagePayload(p)
			assert.False(t, ok, "ITER4 must skip a body-reference record (no resolver)")
		})

		// A reference record whose refLen claims more bytes than present must also
		// be rejected cleanly (bounds-checked length-skip), never OOB-panic.
		var bad []byte
		bad = binary.BigEndian.AppendUint16(bad, 0x0000)
		bad = append(bad, 0x00)
		bad = binary.BigEndian.AppendUint64(bad, 1)
		bad = append(bad, 0x00, 0x00, 0x02)
		bad = append(bad, bodyKindReference)
		bad = binary.BigEndian.AppendUint16(bad, 0xFFFF) // claims 65535 locator bytes
		bad = append(bad, 0x01, 0x02, 0x03)              // but only 3 present
		require.NotPanics(t, func() {
			_, _, _, ok := parseMessagePayload(bad)
			assert.False(t, ok, "a truncated reference locator must be rejected, not panic")
		})
	})
}

// TestWAL_HotPathZeroAlloc is STAGE A test 8b (allocation guard): the
// no-properties / no-headers hot path must serialize with ZERO heap allocations
// once the batch buffer has grown to steady state, exactly as before the format
// change. A regression to per-record allocation fails here.
func TestWAL_HotPathZeroAlloc(t *testing.T) {
	body := make([]byte, 4096)
	for i := range body {
		body[i] = byte(i & 0xff)
	}
	// No properties, no headers — the common durable message.
	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", DeliveryMode: 2, Body: body}

	const perBatch = 8
	var buf []byte
	serialize := func() {
		buf = buf[:0]
		for j := 0; j < perBatch; j++ {
			var err error
			buf, err = appendMessageRecord(buf, "q", msg, uint64(j))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 4; i++ { // warm up to steady-state capacity
		serialize()
	}
	allocs := testing.AllocsPerRun(100, serialize)
	assert.Zero(t, allocs,
		"no-properties/no-headers hot path must serialize %d records with zero heap allocations (got %.1f)", perBatch, allocs)
}

// BenchmarkAppendMessageRecord_NoProps benchmarks the no-properties/no-headers
// durable hot path (the Stage A target message shape).
func BenchmarkAppendMessageRecord_NoProps(b *testing.B) {
	body := make([]byte, 4096)
	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", DeliveryMode: 2, Body: body}
	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		var err error
		buf, err = appendMessageRecord(buf, "q", msg, uint64(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}
