package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendMessageRecord_SharedBufferByteIdentical is the STAGE A A1 byte-
// identity golden for the v4 record. It proves the in-place record serializer
// (appendMessageRecord, which writes each record onto the tail of a SHARED batch
// buffer with record-relative CRC/length backpatch) emits the EXACT same on-disk
// bytes whether it starts from a fresh buffer or is appended onto a non-empty
// one — for every body size and field combination the production path produces,
// including a full basic.properties set.
//
// Byte identity is the load-bearing durability invariant (A1). The test
// deliberately appends records into a NON-EMPTY shared buffer so a bug that used
// absolute offsets (buf[0:4]/buf[4:]) instead of record-relative offsets
// (buf[recStart:...]) would corrupt the CRC/length of every record but the first
// and fail here.
func TestAppendMessageRecord_SharedBufferByteIdentical(t *testing.T) {
	wm, err := NewWALManager(t.TempDir())
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	bodySizes := []int{0, 1, 4096, 65536}

	type fieldSet struct {
		name string
		msg  *protocol.Message
	}
	fieldSets := []fieldSet{
		{name: "bare", msg: &protocol.Message{DeliveryMode: 2}},
		{name: "named", msg: &protocol.Message{Exchange: "ex", RoutingKey: "rk.a", DeliveryMode: 2}},
		{
			// Single-key header keeps EncodeFieldTable's output deterministic so
			// two independent serializations are byte-comparable, while still
			// exercising the Headers field-table + Expiration + EnqueueUnixMilli
			// optional fields and the CRC over them.
			name: "with-extensions",
			msg: &protocol.Message{
				Exchange: "amq.topic", RoutingKey: "rk.b", DeliveryMode: 2,
				Headers: map[string]interface{}{"x-attempt": int64(7)}, Expiration: "60000", EnqueueUnixMilli: 1720000000123,
			},
		},
		{
			// A property-rich message with distinct single-valued scalar/string
			// fields (deterministic encoding) across the whole basic.properties set.
			name: "all-props",
			msg: &protocol.Message{
				Exchange: "amq.topic", RoutingKey: "rk.c", DeliveryMode: 2,
				ContentType: "application/json", ContentEncoding: "gzip", Priority: 5,
				CorrelationID: "corr", ReplyTo: "reply", Expiration: "1000", MessageID: "mid",
				Timestamp: 42, Type: "T", UserID: "u", AppID: "a", ClusterID: "c",
				EnqueueUnixMilli: 7,
			},
		},
	}

	for _, fs := range fieldSets {
		for _, bs := range bodySizes {
			name := fmt.Sprintf("%s/body=%d", fs.name, bs)
			t.Run(name, func(t *testing.T) {
				body := make([]byte, bs)
				for i := range body {
					body[i] = byte((i*7 + bs) & 0xff)
				}
				msg := *fs.msg
				msg.Body = body
				const offset = uint64(0xABCDEF12)
				const queue = "orders"

				// Fresh single-allocation serialization is the reference image.
				ref, err := qw.serializeMessageVersioned(queue, &msg, offset)
				require.NoError(t, err)

				// (a) fresh buffer (recStart == 0): must equal the reference exactly.
				fresh, err := appendMessageRecord(nil, queue, &msg, offset)
				require.NoError(t, err)
				assert.True(t, bytes.Equal(ref, fresh),
					"fresh-buffer record must match reference\nref  =%x\nfresh=%x", ref, fresh)

				// (b) appended onto a non-empty shared buffer (recStart != 0): the
				// record slice carved out at recStart must equal the reference.
				prefix := []byte("PRECEDING-RECORD-BYTES-XYZ")
				shared := append([]byte(nil), prefix...)
				recStart := len(shared)
				shared, err = appendMessageRecord(shared, queue, &msg, offset)
				require.NoError(t, err)
				record := shared[recStart:]
				assert.True(t, bytes.Equal(ref, record),
					"shared-buffer record must match reference\nref   =%x\nshared=%x", ref, record)
				assert.True(t, bytes.Equal(prefix, shared[:recStart]), "preceding buffer bytes must not be mutated")
			})
		}
	}
}

// TestAppendMessageRecord_MultiRecordSharedBuffer proves several records packed
// back-to-back into ONE shared buffer each recover independently: every record's
// carved slice re-parses to its own CRC-valid, correctly-lengthed image, matching
// the legacy per-record serialization. This mirrors exactly what flushBatch does.
func TestAppendMessageRecord_MultiRecordSharedBuffer(t *testing.T) {
	wm, err := NewWALManager(t.TempDir())
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	msgs := []*protocol.Message{
		{RoutingKey: "a", Body: []byte("first"), DeliveryMode: 2},
		{RoutingKey: "bb", Exchange: "ex", Body: bytes.Repeat([]byte{0x5a}, 65536), DeliveryMode: 2},
		{RoutingKey: "ccc", Body: []byte{}, DeliveryMode: 2},
	}

	var buf []byte
	starts := make([]int, len(msgs))
	for i, m := range msgs {
		starts[i] = len(buf)
		var err error
		buf, err = appendMessageRecord(buf, "q", m, uint64(i+1))
		require.NoError(t, err)
	}

	for i, m := range msgs {
		ref, err := qw.serializeMessageVersioned("q", m, uint64(i+1))
		require.NoError(t, err)
		end := len(buf)
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		record := buf[starts[i]:end]
		assert.True(t, bytes.Equal(ref, record), "record %d must match its fresh serialization", i)
	}
}

// sampleXDeathHeaders builds a Headers map containing an x-death field array
// with the exact field types RabbitMQ (and W1's annotateXDeath) emits: an
// int64 count, a longstr reason/queue/exchange, a timestamp time, and a
// field-array of longstr routing-keys. It is the canonical durability fixture:
// these types must survive encode -> persist -> recover with types intact.
func sampleXDeathHeaders(deathTime time.Time) map[string]interface{} {
	return map[string]interface{}{
		"x-death": []interface{}{
			map[string]interface{}{
				"count":        int64(3),
				"reason":       "expired",
				"queue":        "src-q",
				"time":         deathTime,
				"exchange":     "dlx",
				"routing-keys": []interface{}{"rk-a", "rk-b"},
			},
		},
		"x-first-death-reason": "expired",
		"x-attempt":            int64(7),
	}
}

// assertXDeathIntact verifies a recovered Headers map preserved the x-death
// field types exactly (int64 count, time.Time time, []interface{} routing keys
// of strings).
func assertXDeathIntact(t *testing.T, headers map[string]interface{}, deathTime time.Time) {
	t.Helper()
	require.NotNil(t, headers, "headers must be reconstructed")
	assert.Equal(t, int64(7), headers["x-attempt"], "scalar int64 header must survive as int64")
	assert.Equal(t, "expired", headers["x-first-death-reason"])

	arr, ok := headers["x-death"].([]interface{})
	require.True(t, ok, "x-death must decode as a field array ([]interface{})")
	require.Len(t, arr, 1)

	entry, ok := arr[0].(map[string]interface{})
	require.True(t, ok, "x-death entry must decode as a field table")
	assert.Equal(t, int64(3), entry["count"], "x-death count must survive as int64")
	assert.Equal(t, "expired", entry["reason"])
	assert.Equal(t, "src-q", entry["queue"])
	assert.Equal(t, "dlx", entry["exchange"])

	tm, ok := entry["time"].(time.Time)
	require.True(t, ok, "x-death time must decode as time.Time")
	assert.Equal(t, deathTime.Unix(), tm.Unix(), "x-death timestamp must survive to second resolution")

	rks, ok := entry["routing-keys"].([]interface{})
	require.True(t, ok, "routing-keys must decode as a field array")
	require.Len(t, rks, 2)
	assert.Equal(t, "rk-a", rks[0])
	assert.Equal(t, "rk-b", rks[1])
}

// TestWAL_MessageExtensions_RoundTrip drives the full durable path: publish a
// message carrying Headers (x-death), Expiration and EnqueueUnixMilli through
// the WAL, close, reopen, and recover it — proving all three extension fields
// round-trip through the bumped record format with x-death types intact.
func TestWAL_MessageExtensions_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	deathTime := time.Unix(1720000000, 0).UTC()
	msg := &protocol.Message{
		Exchange:         "ex",
		RoutingKey:       "rk",
		Body:             []byte("payload-body"),
		DeliveryMode:     2,
		Expiration:       "60000",
		EnqueueUnixMilli: 1720000000123,
		Headers:          sampleXDeathHeaders(deathTime),
	}
	require.NoError(t, wm.Write("q", msg, 42))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm2.Close()

	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, 1, "the durable message must recover")

	got := recovered[0].Message
	assert.Equal(t, "payload-body", string(got.Body))
	assert.Equal(t, "60000", got.Expiration, "Expiration must be durable")
	assert.Equal(t, int64(1720000000123), got.EnqueueUnixMilli, "EnqueueUnixMilli must be durable")
	assertXDeathIntact(t, got.Headers, deathTime)
}

// TestWAL_LiveReadPreservesExtensions proves the in-process offset-index read
// path (readMessageAtPosition, used for redelivery of a persisted message)
// also reconstructs the extension fields, not just the crash-recovery scan.
func TestWAL_LiveReadPreservesExtensions(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	deathTime := time.Unix(1720000001, 0).UTC()
	msg := &protocol.Message{
		Exchange: "ex", RoutingKey: "rk", Body: []byte("live-body"), DeliveryMode: 2,
		Expiration: "15000", EnqueueUnixMilli: 555,
		Headers: sampleXDeathHeaders(deathTime),
	}
	require.NoError(t, wm.Write("liveq", msg, 77))
	time.Sleep(50 * time.Millisecond)

	got, err := wm.Read("liveq", 77)
	require.NoError(t, err)
	assert.Equal(t, "live-body", string(got.Body))
	assert.Equal(t, "15000", got.Expiration)
	assert.Equal(t, int64(555), got.EnqueueUnixMilli)
	assertXDeathIntact(t, got.Headers, deathTime)
}

// TestSegment_MessageExtensions_RoundTrip proves checkpointed segment-resident
// messages carry the same extension fields through encode/decode and across a
// segment-manager restart (loadExistingSegments -> collectAllMessages).
func TestSegment_MessageExtensions_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)

	deathTime := time.Unix(1720000002, 0).UTC()
	msg := &protocol.Message{
		Exchange: "ex", RoutingKey: "rk", Body: []byte("seg-body"), DeliveryMode: 2,
		Expiration: "45000", EnqueueUnixMilli: 424242,
		Headers: sampleXDeathHeaders(deathTime),
	}
	require.NoError(t, sm.Write("segq", msg, 7))

	// In-process read.
	got, err := sm.Read("segq", 7)
	require.NoError(t, err)
	assert.Equal(t, "seg-body", string(got.Body))
	assert.Equal(t, "45000", got.Expiration)
	assert.Equal(t, int64(424242), got.EnqueueUnixMilli)
	assertXDeathIntact(t, got.Headers, deathTime)

	require.NoError(t, sm.Close())

	// Reopen the segment manager against the same dir and recover from disk.
	sm2, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	defer sm2.Close()

	recovered, err := sm2.RecoverFromSegments()
	require.NoError(t, err)
	segMsgs, ok := recovered["segq"]
	require.True(t, ok, "segq must have recovered messages")
	require.Len(t, segMsgs, 1)

	rec := segMsgs[0].Message
	assert.Equal(t, "seg-body", string(rec.Body))
	assert.Equal(t, "45000", rec.Expiration, "Expiration must be durable across segment restart")
	assert.Equal(t, int64(424242), rec.EnqueueUnixMilli, "EnqueueUnixMilli must be durable across segment restart")
	assertXDeathIntact(t, rec.Headers, deathTime)
}

// TestSegment_TwoRecords_BackToBack proves readSegmentMessageAt reads two v4
// segment records laid out back-to-back in one file — the layout compaction and
// batch checkpoint produce — reconstructing each record's fields and preserving
// the position boundary between them.
func TestSegment_TwoRecords_BackToBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "two.seg")

	buf1, err := serializeSegmentMessage(
		&protocol.Message{Exchange: "ex", RoutingKey: "first", Body: []byte("first-seg"), DeliveryMode: 2}, 5)
	require.NoError(t, err)
	buf2, err := serializeSegmentMessage(
		&protocol.Message{
			Exchange: "ex", RoutingKey: "second", Body: []byte("second-seg"), DeliveryMode: 2,
			Expiration: "10000", EnqueueUnixMilli: 5, Headers: map[string]interface{}{"a": "b"},
		}, 6)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, append(buf1, buf2...), 0644))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	m1, err := readSegmentMessageAt(f, 0)
	require.NoError(t, err)
	assert.Equal(t, "first-seg", string(m1.Body))
	assert.Equal(t, "first", m1.RoutingKey)
	assert.Nil(t, m1.Headers, "a headers-less record must reconstruct nil Headers")
	assert.Equal(t, "", m1.Expiration)
	assert.Equal(t, int64(0), m1.EnqueueUnixMilli)

	m2, err := readSegmentMessageAt(f, int64(len(buf1)))
	require.NoError(t, err)
	assert.Equal(t, "second-seg", string(m2.Body))
	assert.Equal(t, "second", m2.RoutingKey)
	assert.Equal(t, "10000", m2.Expiration)
	assert.Equal(t, int64(5), m2.EnqueueUnixMilli)
	require.NotNil(t, m2.Headers)
	assert.Equal(t, "b", m2.Headers["a"])
}
