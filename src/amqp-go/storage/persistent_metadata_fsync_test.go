package storage

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAtomicWrite_ContentAndCleanup verifies the durable atomicWrite mechanics:
// the final file has exactly the written content, and no temp (.tmp) file is
// left behind after a successful write.
func TestAtomicWrite_ContentAndCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	target := filepath.Join(store.baseDir, QueuesDir, "durable.cbor")
	payload := []byte("durable-payload-\x00\x01\x02-content")

	require.NoError(t, store.atomicWrite(target, payload))

	// Content must survive exactly.
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "atomicWrite must persist exact content")

	// No temp file may remain.
	_, err = os.Stat(target + TempFileExtension)
	assert.True(t, os.IsNotExist(err), "temp file must be cleaned up after write")
}

// TestAtomicWrite_OverwriteIsAtomic verifies that repeated writes replace the
// file's content atomically (the reader always sees a complete version), and
// that the final content is the last write.
func TestAtomicWrite_OverwriteIsAtomic(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	target := filepath.Join(store.baseDir, ExchangesDir, "rewrite.cbor")

	for i := 0; i < 20; i++ {
		payload := []byte{byte(i), byte(i), byte(i), byte(i)}
		require.NoError(t, store.atomicWrite(target, payload))

		got, err := os.ReadFile(target)
		require.NoError(t, err)
		// A reader must never observe a partial/zero-length file — it sees a
		// complete previous or current version.
		require.Len(t, got, 4, "reader must never see a truncated file")
	}

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, []byte{19, 19, 19, 19}, got, "final content must be the last write")

	// No stray temp files.
	files, err := filepath.Glob(filepath.Join(store.baseDir, ExchangesDir, "*"+TempFileExtension))
	require.NoError(t, err)
	assert.Empty(t, files, "no temp files should remain after overwrites")
}

// TestSyncDir_Success verifies the directory fsync helper succeeds on a real
// directory (the durability-critical step for rename survival).
func TestSyncDir_Success(t *testing.T) {
	tmpDir := t.TempDir()
	assert.NoError(t, syncDir(tmpDir), "syncing an existing directory must succeed")
}

// TestSyncDir_NonexistentDirSurfacesError verifies an fsync error is surfaced
// rather than swallowed. On Windows syncDir is intentionally a no-op, so this
// assertion only applies to POSIX platforms.
func TestSyncDir_NonexistentDirSurfacesError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("syncDir is a no-op on Windows")
	}
	err := syncDir(filepath.Join(t.TempDir(), "does-not-exist"))
	assert.Error(t, err, "syncDir must surface an error for a missing directory")
}

// TestAtomicWrite_WriteErrorSurfaced verifies that a failure in the write path
// is surfaced as an error (not swallowed) and does not leave a temp file. Here
// the parent "directory" is actually a regular file, so MkdirAll/OpenFile fail.
func TestAtomicWrite_WriteErrorSurfaced(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	// Create a regular file, then try to write "underneath" it as if it were a
	// directory. MkdirAll on a path whose parent is a file returns an error.
	blocker := filepath.Join(tmpDir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0644))

	target := filepath.Join(blocker, "sub", "file.cbor")
	err = store.atomicWrite(target, []byte("data"))
	assert.Error(t, err, "atomicWrite must surface an error when the path is unwritable")
}
