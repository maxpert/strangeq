package storage

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewDisruptorStorage_FailsWhenWALDirUnwritable verifies that the constructor
// returns an error (not a silently degraded *DisruptorStorage) when WAL construction
// fails due to a disk/permission error. Before the fix, the error was discarded and
// the constructor returned a fully-constructed storage with wal=nil.
func TestNewDisruptorStorage_FailsWhenWALDirUnwritable(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a regular file where the WAL directory would be (<tmpDir>/wal).
	// os.MkdirAll will fail because the path is a file, not a directory.
	walDir := filepath.Join(tmpDir, "wal")
	require.NoError(t, os.WriteFile(walDir, []byte("blocker"), 0644))

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, 0, interfaces.EngineConfig{})
	require.Error(t, err, "constructor must fail when WAL dir cannot be created")
	assert.Nil(t, ds, "no storage should be returned on construction failure")
	assert.Contains(t, err.Error(), "WAL")
}

// TestNewDisruptorStorage_FailsWhenMetadataDirUnwritable verifies that metadata
// store construction failure is also fatal.
func TestNewDisruptorStorage_FailsWhenMetadataDirUnwritable(t *testing.T) {
	tmpDir := t.TempDir()

	// Block the metadata directory
	metaDir := filepath.Join(tmpDir, "metadata")
	require.NoError(t, os.WriteFile(metaDir, []byte("blocker"), 0644))

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, 0, interfaces.EngineConfig{})
	require.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "metadata")
}

// TestNewDisruptorStorage_SucceedsOnValidDir verifies the happy path still works.
func TestNewDisruptorStorage_SucceedsOnValidDir(t *testing.T) {
	tmpDir := t.TempDir()

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, 0, interfaces.EngineConfig{})
	require.NoError(t, err)
	require.NotNil(t, ds)
	assert.NotNil(t, ds.wal, "WAL should be initialized on valid dir")
	assert.NotNil(t, ds.segments, "segments should be initialized on valid dir")
	assert.NotNil(t, ds.metadataStore, "metadata store should be initialized on valid dir")
	_ = ds.Close()
}

// TestStoreMessage_DurableWithoutWAL_ReturnsError verifies the defense-in-depth
// check: even if a DisruptorStorage somehow has wal==nil, a durable (DeliveryMode=2)
// publish is rejected with an explicit error rather than silently stored in-memory only.
func TestStoreMessage_DurableWithoutWAL_ReturnsError(t *testing.T) {
	// Construct a DisruptorStorage with wal==nil directly (simulates a degraded state
	// that should now be impossible via the public constructor, but tests the defense).
	ds := &DisruptorStorage{
		ringBufferSize: DefaultRingBufferSize,
		spillThreshold: uint64(DefaultRingBufferSize) * uint64(DefaultSpillThresholdPercent) / 100,
		transactions:   make(map[string]*interfaces.Transaction),
		dataDir:        t.TempDir(),
	}

	msg := &protocol.Message{
		DeliveryTag:  1,
		DeliveryMode: 2, // durable
		Body:         []byte("durable payload"),
	}

	err := ds.StoreMessage("test-queue", msg)
	require.Error(t, err, "durable publish with nil WAL must be rejected")
	assert.Contains(t, err.Error(), "WAL unavailable")
}

// TestStoreMessage_TransientWithoutWAL_StillWorks verifies that transient (non-durable)
// messages can still be stored when WAL is nil — only durable messages are rejected.
func TestStoreMessage_TransientWithoutWAL_StillWorks(t *testing.T) {
	ds := &DisruptorStorage{
		ringBufferSize: DefaultRingBufferSize,
		spillThreshold: uint64(DefaultRingBufferSize) * uint64(DefaultSpillThresholdPercent) / 100,
		transactions:   make(map[string]*interfaces.Transaction),
		dataDir:        t.TempDir(),
	}

	msg := &protocol.Message{
		DeliveryTag:  1,
		DeliveryMode: 1, // transient
		Body:         []byte("transient payload"),
	}

	err := ds.StoreMessage("test-queue", msg)
	require.NoError(t, err, "transient publish with nil WAL should succeed (in-memory only)")

	retrieved, err := ds.GetMessage("test-queue", 1)
	require.NoError(t, err)
	assert.Equal(t, "transient payload", string(retrieved.Body))
}

// TestNewDisruptorStorage_OffsetStoreFailureTolerated verifies that offset store
// construction failure is non-fatal (offset checkpointing is a best-effort optimization).
func TestNewDisruptorStorage_OffsetStoreFailureTolerated(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file permission tests are unreliable on Windows")
	}

	tmpDir := t.TempDir()

	// Block the offset checkpoint directory
	offsetDir := filepath.Join(tmpDir, "offsets")
	require.NoError(t, os.WriteFile(offsetDir, []byte("blocker"), 0644))

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, 0, interfaces.EngineConfig{})
	// Offset store failure should NOT be fatal — the storage should still be usable.
	require.NoError(t, err, "offset store failure should be non-fatal")
	require.NotNil(t, ds)
	assert.Nil(t, ds.offsetStore, "offset store should be nil on failure")
	assert.NotNil(t, ds.wal, "WAL should still be initialized")
	_ = ds.Close()
}
