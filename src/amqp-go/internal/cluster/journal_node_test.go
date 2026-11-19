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

func journalTestNodeConfig(t *testing.T, journal bool) Config {
	t.Helper()
	peer := Peer{ID: 1, Address: journalTestPeerAddress(t)}
	return Config{
		ClusterID: "journal-node-test", Peer: peer, Seeds: []Peer{peer}, Listen: peer.Address,
		Directory: t.TempDir(), Shards: 2, AllowInsecure: true, ExperimentalRaftJournal: journal,
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

func TestJournalNodeRejectsStorageModeChangesWithoutWriting(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		initialJournal bool
	}{
		{"bolt-to-journal", false},
		{"journal-to-bolt", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			cfg := journalTestNodeConfig(t, scenario.initialJournal)
			require.NoError(t, Initialize(cfg))
			before := journalTestStorageDigest(t, cfg.Directory)
			wrong := cfg
			wrong.ExperimentalRaftJournal = !cfg.ExperimentalRaftJournal
			node, err := Open(wrong)
			if node != nil {
				_ = node.Close()
			}
			require.ErrorContains(t, err, "storage mode", "configured backend silently reinterpreted an existing store")
			require.ErrorContains(t, Initialize(wrong), "storage mode", "initialization must not change an existing authority")
			require.Equal(t, before, journalTestStorageDigest(t, cfg.Directory))

			reopened, err := Open(cfg)
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
		})
	}
}

func TestJournalNodeRejectsCompetingOwner(t *testing.T) {
	cfg := journalTestNodeConfig(t, true)
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
	cfg := journalTestNodeConfig(t, true)
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
	cfg := journalTestNodeConfig(t, true)
	require.NoError(t, Initialize(cfg))
	active := filepath.Join(cfg.Directory, "raft-journal", "active.raft-journal")
	backup := filepath.Join(t.TempDir(), "test-journal")
	require.NoError(t, os.Rename(active, backup))

	node, err := Open(cfg)
	if node != nil {
		_ = node.Close()
	}
	require.ErrorContains(t, err, "lost its established persistent raft journal")
	require.ErrorContains(t, Initialize(cfg), "lost its established persistent raft journal")
	require.NoFileExists(t, active, "startup must not recreate the missing authority")
	if err := os.Rename(backup, active); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}
