package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newMetaStore builds a PersistentMetadataStore over a temp dir (cache enabled,
// the production default).
func newMetaStore(t *testing.T) *PersistentMetadataStore {
	t.Helper()
	pm, err := NewPersistentMetadataStore(t.TempDir())
	require.NoError(t, err)
	return pm
}

// TestSnapshotBindings_QueueFromCacheNoDisk proves the queue-binding path is
// served entirely from the authoritative in-memory cache: after the on-disk
// bindings directory is renamed away, the snapshot still returns every binding
// (spec mgmt-04 §5.1 test 1 / C-1).
func TestSnapshotBindings_QueueFromCacheNoDisk(t *testing.T) {
	pm := newMetaStore(t)

	require.NoError(t, pm.StoreBinding("q1", "x", "a", nil))
	require.NoError(t, pm.StoreBinding("q2", "x", "b", map[string]interface{}{"k": "v"}))
	require.NoError(t, pm.StoreBinding("q3", "y", "c", nil))

	// Remove the bindings directory from under the store. A cache-backed read
	// must be unaffected.
	dir := filepath.Join(pm.baseDir, BindingsDir)
	require.NoError(t, os.Rename(dir, dir+".moved"))

	queue, exchange, err := pm.SnapshotBindings()
	require.NoError(t, err)
	require.Len(t, queue, 3, "the cached queue bindings must survive the disk directory vanishing")
	require.Empty(t, exchange)

	got := map[string]bool{}
	for _, b := range queue {
		got[b.QueueName] = true
	}
	require.True(t, got["q1"] && got["q2"] && got["q3"], "all three queue bindings returned: %+v", got)
}

// TestSnapshotBindings_ExchangeFromDisk proves exchange-to-exchange bindings are
// read from the metadata directory (spec mgmt-04 §5.1 test 2).
func TestSnapshotBindings_ExchangeFromDisk(t *testing.T) {
	pm := newMetaStore(t)

	require.NoError(t, pm.StoreExchangeBinding("src", "dst", "rk1", nil))
	require.NoError(t, pm.StoreExchangeBinding("src", "dst2", "rk2", map[string]interface{}{"x": 1}))

	queue, exchange, err := pm.SnapshotBindings()
	require.NoError(t, err)
	require.Empty(t, queue)
	require.Len(t, exchange, 2)

	byRK := map[string]*exchangeBindingView{}
	for _, eb := range exchange {
		byRK[eb.RoutingKey] = &exchangeBindingView{src: eb.Source, dst: eb.Destination}
	}
	require.Equal(t, "src", byRK["rk1"].src)
	require.Equal(t, "dst", byRK["rk1"].dst)
	require.Equal(t, "src", byRK["rk2"].src)
	require.Equal(t, "dst2", byRK["rk2"].dst)
}

type exchangeBindingView struct{ src, dst string }

// TestSnapshotBindings_EmptyIsEmptySlice proves an empty store yields two empty
// (non-nil) slices and no error (spec mgmt-04 §5.1 test 3).
func TestSnapshotBindings_EmptyIsEmptySlice(t *testing.T) {
	pm := newMetaStore(t)

	queue, exchange, err := pm.SnapshotBindings()
	require.NoError(t, err)
	require.NotNil(t, queue)
	require.NotNil(t, exchange)
	require.Empty(t, queue)
	require.Empty(t, exchange)
}

// TestSnapshotBindings_ReflectsDeletes proves the snapshot tracks the cache
// through deletes (no stale row survives).
func TestSnapshotBindings_ReflectsDeletes(t *testing.T) {
	pm := newMetaStore(t)
	require.NoError(t, pm.StoreBinding("q", "x", "a", nil))
	require.NoError(t, pm.StoreBinding("q", "x", "b", nil))

	require.NoError(t, pm.DeleteBinding("q", "x", "a"))

	queue, _, err := pm.SnapshotBindings()
	require.NoError(t, err)
	require.Len(t, queue, 1)
	require.Equal(t, "b", queue[0].RoutingKey)
}

// TestStorageEngine_SnapshotBindings_Forwards proves the StorageEngine
// forwarder reaches the metadata store.
func TestStorageEngine_SnapshotBindings_Forwards(t *testing.T) {
	ds, err := NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	require.NoError(t, ds.StoreBinding("q", "x", "rk", nil))
	queue, exchange, err := ds.SnapshotBindings()
	require.NoError(t, err)
	require.Len(t, queue, 1)
	require.Empty(t, exchange)
}
