package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sharedWALDir returns the shared WAL directory for a WAL manager rooted at
// dataDir (matches NewWALManager's layout: <dataDir>/wal/shared).
func sharedWALDir(dataDir string) string {
	return filepath.Join(dataDir, "wal", "shared")
}

// TestWAL_NewFileHasVersionHeader verifies every new WAL file begins with the
// "SQWAL" magic followed by the current format version byte.
func TestWAL_NewFileHasVersionHeader(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	msg := &protocol.Message{
		Exchange:     "ex",
		RoutingKey:   "rk",
		Body:         []byte("hdr-check"),
		DeliveryMode: 2,
	}
	require.NoError(t, wm.Write("q", msg, 1))
	time.Sleep(30 * time.Millisecond)
	require.NoError(t, wm.Close())

	entries, err := os.ReadDir(sharedWALDir(tmpDir))
	require.NoError(t, err)

	var walPath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == WALFileExtension {
			walPath = filepath.Join(sharedWALDir(tmpDir), e.Name())
			break
		}
	}
	require.NotEmpty(t, walPath, "expected a .wal file")

	f, err := os.Open(walPath)
	require.NoError(t, err)
	defer f.Close()

	hdr := make([]byte, WALHeaderSize)
	_, err = f.ReadAt(hdr, 0)
	require.NoError(t, err)

	assert.Equal(t, WALMagic, string(hdr[:len(WALMagic)]), "file must start with SQWAL magic")
	assert.Equal(t, WALFormatVersion, hdr[len(WALMagic)], "version byte must be current format version")
}

// TestWAL_VersionedRoundTrip writes messages through a versioned (v1) WAL,
// closes, reopens, and recovers them — proving the header path round-trips.
func TestWAL_VersionedRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	const n = 8
	for i := 1; i <= n; i++ {
		msg := &protocol.Message{
			Exchange:     "ex",
			RoutingKey:   "rk",
			Body:         []byte(fmt.Sprintf("versioned-%d", i)),
			DeliveryMode: 2,
		}
		require.NoError(t, wm.Write("vq", msg, uint64(i)))
	}
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm2.Close()

	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, n, "all versioned records must recover")

	bodies := make(map[string]bool)
	for _, rm := range recovered {
		assert.Equal(t, "vq", rm.QueueName)
		bodies[string(rm.Message.Body)] = true
	}
	for i := 1; i <= n; i++ {
		assert.True(t, bodies[fmt.Sprintf("versioned-%d", i)], "missing body versioned-%d", i)
	}
}

// TestWAL_LegacyHeaderlessRecovery crafts a v0 (headerless) WAL file with valid
// records and proves it recovers correctly — existing data dirs keep working.
func TestWAL_LegacyHeaderlessRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	// Build legacy records (no file header) using the same record serialization
	// the WAL itself uses.
	var buf []byte
	const n = 4
	for i := 1; i <= n; i++ {
		msg := &protocol.Message{
			Exchange:     "legacy-ex",
			RoutingKey:   "legacy-rk",
			Body:         []byte(fmt.Sprintf("legacy-%d", i)),
			DeliveryMode: 2,
		}
		// Build genuine legacy (v1/v0) records with NO per-record type tag. The
		// canonical serializer now emits v2 (SQ-8), so a legacy fixture must ask
		// for the older format explicitly.
		buf = append(buf, qw.serializeMessageVersioned(WALVersion1, "legacy_q", msg, uint64(100+i))...)
	}

	// Write it directly with NO SQWAL header, mimicking a pre-SQ-4 file. Use a
	// large file number so directory-order recovery hits the auto-created v1
	// file (empty) and then this legacy file.
	legacyPath := filepath.Join(sharedWALDir(tmpDir), fmt.Sprintf("%020d%s", uint64(999999), WALFileExtension))
	require.NoError(t, os.WriteFile(legacyPath, buf, 0644))

	// Direct scan must parse it as legacy from offset 0.
	msgs, err := qw.scanWALFile(legacyPath)
	require.NoError(t, err)
	require.Len(t, msgs, n, "legacy file must parse all records from offset 0")
	for _, m := range msgs {
		assert.Equal(t, "legacy_q", m.QueueName)
		assert.Contains(t, string(m.Message.Body), "legacy-")
	}

	// Full recovery path must also include the legacy records.
	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)
	legacyCount := 0
	for _, rm := range recovered {
		if rm.QueueName == "legacy_q" {
			legacyCount++
		}
	}
	assert.Equal(t, n, legacyCount, "RecoverFromWAL must recover legacy headerless records")
}

// TestWAL_UnknownVersionErrors verifies a file whose header carries an unknown
// version produces a clear error (not a silent skip) on both the direct scan
// and the full recovery path.
func TestWAL_UnknownVersionErrors(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	// Craft a file: SQWAL magic + a bogus future version + some record-shaped
	// bytes we must never reach.
	bad := make([]byte, 0, 32)
	bad = append(bad, WALMagic...)
	bad = append(bad, byte(99)) // unknown version
	bad = append(bad, []byte("garbage-payload-bytes")...)

	badPath := filepath.Join(sharedWALDir(tmpDir), fmt.Sprintf("%020d%s", uint64(888888), WALFileExtension))
	require.NoError(t, os.WriteFile(badPath, bad, 0644))

	_, err = qw.scanWALFile(badPath)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedWALVersion), "scan must surface ErrUnsupportedWALVersion, got %v", err)

	_, rerr := wm.RecoverFromWAL()
	require.Error(t, rerr)
	assert.True(t, errors.Is(rerr, ErrUnsupportedWALVersion), "recovery must surface ErrUnsupportedWALVersion, got %v", rerr)
}

// TestWAL_ReservedRecordTypeConstants documents that the reserved record-type
// tag values are defined (for a future v2 tx-boundary marker) but distinct.
// This is a compile/value guard, not behavior — v1 writes no per-record tag.
func TestWAL_ReservedRecordTypeConstants(t *testing.T) {
	assert.Equal(t, uint8(0), WALRecordTypeMessage)
	assert.Equal(t, uint8(1), WALRecordTypeTxBoundary)
	assert.NotEqual(t, WALRecordTypeMessage, WALRecordTypeTxBoundary)
}
