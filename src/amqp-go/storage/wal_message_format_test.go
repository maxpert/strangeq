package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestWAL_BackwardCompat_OldAndNewRecords proves an existing durable store
// recovers without rewrite: a single physical WAL file whose header predates
// W2 (v2) and that contains BOTH an old v2 record (no extensions) AND a v3
// record appended after the upgrade must recover both — the old one defaulting
// to nil Headers / "" Expiration / 0 EnqueueUnixMilli and the new one carrying
// its extensions. This is the append-on-restart mixed-version file case.
func TestWAL_BackwardCompat_OldAndNewRecords(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	oldMsg := &protocol.Message{
		Exchange: "ex", RoutingKey: "old-rk", Body: []byte("old-body"), DeliveryMode: 2,
	}
	newMsg := &protocol.Message{
		Exchange: "ex", RoutingKey: "new-rk", Body: []byte("new-body"), DeliveryMode: 2,
		Expiration: "30000", EnqueueUnixMilli: 999,
		Headers: map[string]interface{}{"k": "v"},
	}

	// A v2-header file (a store written before W2) that also has a v3 record
	// appended after restart. The v2 record has no extension bytes; the v3
	// record does. Recovery must parse both correctly.
	buf := append([]byte(WALMagic), WALVersion2)
	buf = append(buf, qw.serializeMessageVersioned(WALVersion2, "q", oldMsg, 1)...)
	buf = append(buf, qw.serializeMessageVersioned(WALVersion3, "q", newMsg, 2)...)

	path := filepath.Join(sharedWALDir(tmpDir), fmt.Sprintf("%020d%s", uint64(555555), WALFileExtension))
	require.NoError(t, os.WriteFile(path, buf, 0644))

	msgs, err := qw.scanWALFile(path)
	require.NoError(t, err)
	require.Len(t, msgs, 2, "both the old and new records must recover")

	byRK := map[string]*protocol.Message{}
	for _, m := range msgs {
		byRK[m.Message.RoutingKey] = m.Message
	}

	old := byRK["old-rk"]
	require.NotNil(t, old, "old record must recover")
	assert.Equal(t, "old-body", string(old.Body))
	assert.Nil(t, old.Headers, "old record must default to nil Headers")
	assert.Equal(t, "", old.Expiration, "old record must default to empty Expiration")
	assert.Equal(t, int64(0), old.EnqueueUnixMilli, "old record must default to 0 EnqueueUnixMilli")

	nw := byRK["new-rk"]
	require.NotNil(t, nw, "new record must recover")
	assert.Equal(t, "new-body", string(nw.Body))
	assert.Equal(t, "30000", nw.Expiration)
	assert.Equal(t, int64(999), nw.EnqueueUnixMilli)
	require.NotNil(t, nw.Headers)
	assert.Equal(t, "v", nw.Headers["k"])
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

// serializeSegmentMessageLegacy reproduces the pre-W2 segment record format
// (no extension block after the body) so the backward-compat test can craft a
// genuine old record next to a new one in the same file.
func serializeSegmentMessageLegacy(message *protocol.Message, offset uint64) []byte {
	var buf []byte
	buf = append(buf, 0, 0, 0, 0) // CRC placeholder
	buf = append(buf, 0, 0, 0, 0) // length placeholder

	buf = binary.BigEndian.AppendUint32(buf, 0) // empty queue name (segments key by queue dir)

	buf = binary.BigEndian.AppendUint64(buf, offset)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Exchange)))
	buf = append(buf, message.Exchange...)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.RoutingKey)))
	buf = append(buf, message.RoutingKey...)

	buf = append(buf, message.DeliveryMode)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Body)))
	buf = append(buf, message.Body...)

	// Framing: length excludes CRC+length; CRC covers everything after itself.
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(buf)-8))
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// TestSegment_BackwardCompat_OldAndNewRecords proves readSegmentMessageAt reads
// both a pre-W2 record (no extensions -> defaults) and a W2 record (with
// extensions) laid out back-to-back in one segment file, distinguishing them
// structurally by the presence of trailing bytes after the body.
func TestSegment_BackwardCompat_OldAndNewRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.seg")

	oldBuf := serializeSegmentMessageLegacy(
		&protocol.Message{Exchange: "ex", RoutingKey: "old", Body: []byte("old-seg"), DeliveryMode: 2}, 5)
	newBuf := serializeSegmentMessage(
		&protocol.Message{
			Exchange: "ex", RoutingKey: "new", Body: []byte("new-seg"), DeliveryMode: 2,
			Expiration: "10000", EnqueueUnixMilli: 5, Headers: map[string]interface{}{"a": "b"},
		}, 6)

	require.NoError(t, os.WriteFile(path, append(oldBuf, newBuf...), 0644))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	m1, err := readSegmentMessageAt(f, 0)
	require.NoError(t, err)
	assert.Equal(t, "old-seg", string(m1.Body))
	assert.Nil(t, m1.Headers, "pre-W2 segment record must default to nil Headers")
	assert.Equal(t, "", m1.Expiration)
	assert.Equal(t, int64(0), m1.EnqueueUnixMilli)

	m2, err := readSegmentMessageAt(f, int64(len(oldBuf)))
	require.NoError(t, err)
	assert.Equal(t, "new-seg", string(m2.Body))
	assert.Equal(t, "10000", m2.Expiration)
	assert.Equal(t, int64(5), m2.EnqueueUnixMilli)
	require.NotNil(t, m2.Headers)
	assert.Equal(t, "b", m2.Headers["a"])
}
