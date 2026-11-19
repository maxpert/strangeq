//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package cluster

import (
	"crypto/sha256"
	"encoding/base64"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/stretchr/testify/require"
)

func journalTestPeerAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func journalTestNodeConfig(t *testing.T) Config {
	t.Helper()
	peer := Peer{ID: 1, Address: journalTestPeerAddress(t)}
	return Config{
		ClusterID: "journal-node-test", Peer: peer, Seeds: []Peer{peer}, Listen: peer.Address,
		Directory: t.TempDir(), Shards: 2, AllowInsecure: true,
		Token:      base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		NewMachine: func(uint64) clusterraft.Machine { return &testMachine{} },
	}
}

func journalTestStorageDigest(t *testing.T, root string) map[string][sha256.Size]byte {
	t.Helper()
	files := make(map[string][sha256.Size]byte)
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = sha256.Sum256(data)
		return nil
	}))
	return files
}

// The ordinary configured cluster entry point must provision the one node
// journal without selecting an experimental storage backend.
func TestDefaultClusterProvisionsSegmentedJournal(t *testing.T) {
	cfg := journalTestNodeConfig(t)
	require.NoError(t, Initialize(cfg))
	require.FileExists(t, filepath.Join(cfg.Directory, journalDirectory, "extent-0000000000000001.raft-journal"))
	require.NoError(t, filepath.WalkDir(cfg.Directory, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			require.NotEqual(t, "raft.db", entry.Name(), "default cluster created a second Raft authority at %s", path)
		}
		return err
	}))
	node, err := Open(cfg)
	require.NoError(t, err)
	require.NoError(t, node.Close())
}

func TestJournalNodeRejectsLegacyStorageWithoutWriting(t *testing.T) {
	for _, legacy := range []string{"identity.json", "1/raft.db"} {
		t.Run(legacy, func(t *testing.T) {
			cfg := journalTestNodeConfig(t)
			require.NoError(t, Initialize(cfg))
			path := filepath.Join(cfg.Directory, legacy)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, []byte("legacy authority"), 0600))
			before := journalTestStorageDigest(t, cfg.Directory)
			node, err := Open(cfg)
			if node != nil {
				_ = node.Close()
			}
			require.ErrorContains(t, err, "storage mode")
			require.ErrorContains(t, Initialize(cfg), "storage mode")
			require.Equal(t, before, journalTestStorageDigest(t, cfg.Directory))
		})
	}
}

func TestJournalNodeRejectsCompetingOwner(t *testing.T) {
	cfg := journalTestNodeConfig(t)
	require.NoError(t, Initialize(cfg))
	first, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	// A distinct bind address prevents a socket conflict from satisfying the
	// storage ownership assertion.
	competing := cfg
	competing.Listen = journalTestPeerAddress(t)
	second, err := Open(competing)
	if second != nil {
		_ = second.Close()
	}
	require.ErrorContains(t, err, "lock node raft journal", "expected the node journal ownership lock")
	require.NoError(t, first.Close())
	second, err = Open(competing)
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestJournalNodeDiscoversGroupWithoutOpenedMarker(t *testing.T) {
	cfg := journalTestNodeConfig(t)
	cfg.Seeds = []Peer{{ID: 7, Address: journalTestPeerAddress(t)}}
	const shard = 2
	first, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	// This joining node has no seed shard list in openExisting. Its local
	// group must be recovered from the authoritative journal inventory.
	require.NoError(t, first.openGroup(shard, nil))
	require.FileExists(t, first.opened(shard))
	require.NoError(t, first.Close())
	require.NoError(t, os.Remove(first.opened(shard)))
	second, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	require.NotNil(t, second.group(shard), "a durable group disappeared with its interrupted opened marker")
	require.FileExists(t, second.opened(shard))
	require.NoFileExists(t, filepath.Join(second.store(shard), "raft.db"), "group discovery used a per-group Bolt store")
	require.NoError(t, second.Close())

	require.NoError(t, durableFile(second.retired(shard), []byte{1}))
	third, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = third.Close() })
	require.Nil(t, third.group(shard), "a persistent retirement marker must fence recovered journal records")
}

func TestJournalNodeRefusesMissingEstablishedActiveFile(t *testing.T) {
	cfg := journalTestNodeConfig(t)
	require.NoError(t, Initialize(cfg))
	active := filepath.Join(cfg.Directory, "raft-journal", "extent-0000000000000001.raft-journal")
	backup := filepath.Join(t.TempDir(), "test-journal")
	require.NoError(t, os.Rename(active, backup))

	node, err := Open(cfg)
	if node != nil {
		_ = node.Close()
	}
	require.ErrorContains(t, err, "missing journal extent")
	require.ErrorContains(t, Initialize(cfg), "missing journal extent")
	require.NoFileExists(t, active, "startup must not recreate the missing authority")
	if err := os.Rename(backup, active); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestOriginalVoterRejectsEmptyReplacementJournal(t *testing.T) {
	cfg := journalTestNodeConfig(t)
	require.NoError(t, Initialize(cfg))
	authority := filepath.Join(cfg.Directory, journalDirectory)
	require.NoError(t, os.Rename(authority, filepath.Join(t.TempDir(), "original")))
	replacement, err := clusterraft.OpenNodeJournal(authority, true)
	require.NoError(t, err)
	require.NoError(t, replacement.Close())
	for _, initialize := range []bool{false, true} {
		node, err := open(cfg, initialize)
		if node != nil {
			_ = node.Close()
		}
		require.ErrorContains(t, err, "lost its persistent store")
		inventory, err := clusterraft.OpenNodeJournal(authority, false)
		require.NoError(t, err)
		require.Empty(t, inventory.GroupIDs(), "an empty replacement must never bootstrap an original voter")
		require.NoError(t, inventory.Close())
	}
}
