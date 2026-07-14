package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// C0 — CRC verification must be ON by default (safe), and making it configurable
// must not break the record format (CRC field is always present, just zero when
// disabled). The read path must handle mixed files: zero-CRC records skip
// verification, non-zero records verify as normal.

// TestWALCRCFlag_ZeroValueDefaultsOn pins the safe-by-default contract: CRC
// verification is ON at every zero-value path, just like fsync.
func TestWALCRCFlag_ZeroValueDefaultsOn(t *testing.T) {
	assert.False(t, WALConfig{}.CRCDisabled, "zero-value WALConfig must have CRC on (CRCDisabled=false)")
	assert.False(t, DefaultWALConfig().CRCDisabled, "DefaultWALConfig must have CRC on (CRCDisabled=false)")
}

// TestWALCRCFlag_Honored proves the flag is honored both ways: with CRC enabled
// (default) the on-disk CRC field is non-zero and verified; with CRC disabled
// the CRC field is zero and records are still readable.
func TestWALCRCFlag_Honored(t *testing.T) {
	t.Run("enabled_writes_nonzero_crc", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		wm, err := NewWALManagerWithConfig(dir, cfg)
		require.NoError(t, err)
		defer wm.Close()

		msg := &protocol.Message{Body: []byte("body"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 1}
		require.NoError(t, wm.Write("q", msg, 1))

		rawCRC := readFirstRecordCRC(t, sharedWALDir(dir))
		assert.NotEqual(t, uint32(0), rawCRC, "CRC field must be non-zero when CRC is enabled")

		got, err := wm.Read("q", 1)
		require.NoError(t, err)
		assert.Equal(t, []byte("body"), got.Body)
	})

	t.Run("disabled_writes_zero_crc", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		cfg.CRCDisabled = true
		wm, err := NewWALManagerWithConfig(dir, cfg)
		require.NoError(t, err)
		defer wm.Close()

		msg := &protocol.Message{Body: []byte("body"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 1}
		require.NoError(t, wm.Write("q", msg, 1))

		rawCRC := readFirstRecordCRC(t, sharedWALDir(dir))
		assert.Equal(t, uint32(0), rawCRC, "CRC field must be zero when CRC is disabled")

		got, err := wm.Read("q", 1)
		require.NoError(t, err)
		assert.Equal(t, []byte("body"), got.Body)
	})
}

// TestWALCRC_MixedFiles_SafeRead writes batch 1 with CRC ON into one WAL file
// and batch 2 with CRC OFF into a second WAL file, then recovers both. This
// proves the read path handles mixed files gracefully: zero-CRC records skip
// verification while non-zero-CRC records verify as normal.
func TestWALCRC_MixedFiles_SafeRead(t *testing.T) {
	dir := t.TempDir()
	sharedDir := sharedWALDir(dir)
	require.NoError(t, os.MkdirAll(sharedDir, 0755))

	msg1 := &protocol.Message{Body: []byte("crc-on"), RoutingKey: "mixed-q", DeliveryMode: 2}
	rec1, err := appendMessageRecord(nil, "mixed-q", msg1, 10, false)
	require.NoError(t, err)
	buf1 := append([]byte(WALMagic), WALFormatVersion)
	buf1 = append(buf1, rec1...)
	require.NoError(t, os.WriteFile(
		filepath.Join(sharedDir, fmt.Sprintf("%020d%s", uint64(1), WALFileExtension)), buf1, 0644))

	msg2 := &protocol.Message{Body: []byte("crc-off"), RoutingKey: "mixed-q", DeliveryMode: 2}
	rec2, err := appendMessageRecord(nil, "mixed-q", msg2, 20, true)
	require.NoError(t, err)
	buf2 := append([]byte(WALMagic), WALFormatVersion)
	buf2 = append(buf2, rec2...)
	require.NoError(t, os.WriteFile(
		filepath.Join(sharedDir, fmt.Sprintf("%020d%s", uint64(2), WALFileExtension)), buf2, 0644))

	wm, err := NewWALManager(dir)
	require.NoError(t, err)
	defer wm.Close()

	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)

	bodies := map[uint64][]byte{}
	for _, rm := range recovered {
		if rm.QueueName == "mixed-q" {
			bodies[rm.Offset] = rm.Message.Body
		}
	}
	assert.Equal(t, []byte("crc-on"), bodies[10], "CRC-on record must recover")
	assert.Equal(t, []byte("crc-off"), bodies[20], "CRC-off record must recover")
}

// TestCRCReadPath_IncrementalHash verifies the incremental hash (crc32.NewIEEE
// + Write + Sum32) produces the same result as crc32.ChecksumIEEE for various
// data sizes, including the two-write pattern used on the read path
// (header[4:8] then data).
func TestCRCReadPath_IncrementalHash(t *testing.T) {
	sizes := []int{0, 1, 4, 7, 64, 256, 4096, 65536}
	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i * 31)
		}
		header := make([]byte, 8)
		binary.BigEndian.PutUint32(header[4:8], uint32(size))

		combined := make([]byte, 4+size)
		copy(combined[0:4], header[4:8])
		copy(combined[4:], data)
		oneShot := crc32.ChecksumIEEE(combined)

		h := crc32.NewIEEE()
		h.Write(header[4:8])
		h.Write(data)
		incremental := h.Sum32()

		assert.Equal(t, oneShot, incremental, "incremental hash must match one-shot for size %d", size)
	}
}

// readFirstRecordCRC reads the first record's CRC field from the first .wal
// file in dir.
func readFirstRecordCRC(t *testing.T, dir string) uint32 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var path string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == WALFileExtension {
			path = filepath.Join(dir, e.Name())
			break
		}
	}
	require.NotEmpty(t, path, "no WAL file found")
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	hdr := make([]byte, WALHeaderSize+8)
	_, err = f.ReadAt(hdr, 0)
	require.NoError(t, err)
	return binary.BigEndian.Uint32(hdr[WALHeaderSize : WALHeaderSize+4])
}
