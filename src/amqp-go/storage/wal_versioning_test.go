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

// TestWAL_HeaderlessFileRejected proves the ITER4 clean break: a pre-v4
// headerless (v0) WAL file — one with no SQWAL magic — is rejected loudly with
// ErrUnsupportedWALVersion rather than being silently mis-parsed from offset 0.
func TestWAL_HeaderlessFileRejected(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	// Bytes with NO SQWAL header, mimicking a pre-v4 file (record-shaped garbage
	// that must never be parsed). Long enough to exceed WALHeaderSize.
	headerless := []byte("this-is-not-a-versioned-wal-file-header-bytes")
	headerlessPath := filepath.Join(sharedWALDir(tmpDir), fmt.Sprintf("%020d%s", uint64(999999), WALFileExtension))
	require.NoError(t, os.WriteFile(headerlessPath, headerless, 0644))

	_, serr := qw.scanWALFile(headerlessPath)
	require.Error(t, serr)
	assert.True(t, errors.Is(serr, ErrUnsupportedWALVersion),
		"a headerless file must be rejected with ErrUnsupportedWALVersion, got %v", serr)

	_, rerr := wm.RecoverFromWAL()
	require.Error(t, rerr)
	assert.True(t, errors.Is(rerr, ErrUnsupportedWALVersion),
		"recovery must surface ErrUnsupportedWALVersion for a headerless file, got %v", rerr)
}

// TestWAL_UnsupportedVersionsRejected verifies the clean break rejects every
// pre-v4 format version (v1/v2/v3) AND any unknown future version, on both the
// direct scan and the full recovery path — never a silent skip.
func TestWAL_UnsupportedVersionsRejected(t *testing.T) {
	badVersions := []uint8{WALVersion1, WALVersion2, WALVersion3, 99}
	for _, v := range badVersions {
		t.Run(fmt.Sprintf("v%d", v), func(t *testing.T) {
			tmpDir := t.TempDir()
			wm, err := NewWALManager(tmpDir)
			require.NoError(t, err)
			defer wm.Close()
			qw := wm.sharedWAL

			// SQWAL magic + a non-v4 version + record-shaped bytes we must never reach.
			bad := append([]byte(WALMagic), v)
			bad = append(bad, []byte("garbage-payload-bytes")...)

			badPath := filepath.Join(sharedWALDir(tmpDir), fmt.Sprintf("%020d%s", uint64(888888), WALFileExtension))
			require.NoError(t, os.WriteFile(badPath, bad, 0644))

			_, serr := qw.scanWALFile(badPath)
			require.Error(t, serr)
			assert.True(t, errors.Is(serr, ErrUnsupportedWALVersion),
				"scan must surface ErrUnsupportedWALVersion for v%d, got %v", v, serr)

			_, rerr := wm.RecoverFromWAL()
			require.Error(t, rerr)
			assert.True(t, errors.Is(rerr, ErrUnsupportedWALVersion),
				"recovery must surface ErrUnsupportedWALVersion for v%d, got %v", v, rerr)
		})
	}
}

// TestWAL_ReservedRecordTypeConstants documents that the reserved record-type
// tag values are defined (for a future v2 tx-boundary marker) but distinct.
// This is a compile/value guard, not behavior — v1 writes no per-record tag.
func TestWAL_ReservedRecordTypeConstants(t *testing.T) {
	assert.Equal(t, uint8(0), WALRecordTypeMessage)
	assert.Equal(t, uint8(1), WALRecordTypeTxBoundary)
	assert.NotEqual(t, WALRecordTypeMessage, WALRecordTypeTxBoundary)
}
