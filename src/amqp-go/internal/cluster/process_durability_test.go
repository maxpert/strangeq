package cluster

import (
	"encoding/base64"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessDurabilityRejectsNonLocalInternalEntryPoints(t *testing.T) {
	cfg := localTestConfig(t)
	cfg.ProcessDurability = true
	_, err := Open(cfg)
	require.ErrorContains(t, err, "process durability requires a local node")
	require.ErrorContains(t, Initialize(cfg), "process durability requires a local node")
	entries, err := os.ReadDir(cfg.Directory)
	require.NoError(t, err)
	require.Empty(t, entries, "invalid policy must fail before persistent initialization")
}

type activationDurabilityOwner struct {
	nodeJournalOwner
	requireFull func() error
}

func (o activationDurabilityOwner) RequireFullDurability() error { return o.requireFull() }

func TestProcessActivationPersistsIntentBeforeUpgradeAndExposure(t *testing.T) {
	cfg := localTestConfig(t)
	cfg.ProcessDurability = true
	node, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	require.True(t, node.cfg.ProcessDurability)
	submitEventually(t, node, "q", "before", `["before"]`)
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := socket.Addr().String()
	require.NoError(t, socket.Close())
	activation := Config{Peer: Peer{Address: endpoint}, Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true}
	upgradeFailure := errors.New("injected full durability failure")
	owner := node.journal
	calls := 0
	node.journal = activationDurabilityOwner{nodeJournalOwner: owner, requireFull: func() error {
		calls++
		require.FileExists(t, filepath.Join(cfg.Directory, peerActivationFile))
		require.Nil(t, node.transport.Load())
		require.Empty(t, address(node.catalog.view(), node.cfg.Peer.ID))
		probe, err := net.Listen("tcp", endpoint)
		require.NoError(t, err, "peer listener must not start before durability upgrade")
		require.NoError(t, probe.Close())
		return upgradeFailure
	}}
	require.ErrorIs(t, node.ActivatePeers(activation), upgradeFailure)
	require.Equal(t, 1, calls)
	require.Nil(t, node.transport.Load())
	require.FileExists(t, filepath.Join(cfg.Directory, peerActivationFile))
	node.journal = owner
	require.NoError(t, node.Close())
	// The intent alone requires credentials on reopen, even though no listener
	// or endpoint commit succeeded. Startup cannot silently resume process mode.
	_, err = OpenLocal(cfg)
	require.ErrorContains(t, err, "persistent peer activation requires peer credentials")
	cfg.Peer, cfg.Token, cfg.AllowInsecure = activation.Peer, activation.Token, true
	node, err = OpenLocal(cfg)
	require.NoError(t, err)
	require.False(t, node.cfg.ProcessDurability)
	require.True(t, node.transport.Load().ready.Load())
	submitEventually(t, node, "q", "after", `["before","after"]`)
}

func TestProcessLocalStalePreflightRejectsActivatedStorage(t *testing.T) {
	for _, tc := range []struct {
		name, journal string
		dual          bool
	}{
		{name: "single", journal: journalDirectory},
		{name: "dual", journal: "raft-journal-0", dual: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := localTestConfig(t)
			cfg.Shards = 2
			cfg.ExperimentalRaftJournalDualStream = tc.dual
			cfg.ExperimentalRaftRangeSync = tc.dual
			cfg.ProcessDurability = true
			owner, err := OpenLocal(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, owner.Close()) })

			// Capture the local startup inputs at the point where OpenLocal
			// has observed no activation intent but does not own the journal.
			stale := owner.cfg
			require.True(t, stale.ProcessDurability)
			intent := filepath.Join(cfg.Directory, peerActivationFile)
			_, err = os.Lstat(intent)
			require.ErrorIs(t, err, os.ErrNotExist)

			socket, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			cfg.Peer.Address = socket.Addr().String()
			require.NoError(t, socket.Close())
			cfg.Token = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
			cfg.AllowInsecure = true
			require.NoError(t, owner.ActivatePeers(cfg))
			require.True(t, owner.transport.Load().ready.Load())
			require.FileExists(t, intent)
			require.NoError(t, owner.Close())

			// Recovery would truncate this incomplete frame from the active
			// extent. The stale opener must refuse before making that repair.
			active := filepath.Join(cfg.Directory, tc.journal, "extent-0000000000000001.raft-journal")
			body, err := os.ReadFile(active)
			require.NoError(t, err)
			torn := append(body, 0)
			require.NoError(t, os.WriteFile(active, torn, 0600))

			staleNode, err := open(stale, false)
			if staleNode != nil {
				require.NoError(t, staleNode.Close())
			}
			require.ErrorContains(t, err, "persistent peer activation requires peer credentials")
			got, err := os.ReadFile(active)
			require.NoError(t, err)
			require.Equal(t, torn, got, "refusal must precede recovery of the active extent")

			reopened, err := OpenLocal(cfg)
			require.NoError(t, err, "matching peer settings must still recover the journal")
			defer func() { require.NoError(t, reopened.Close()) }()
			require.False(t, reopened.cfg.ProcessDurability)
			require.True(t, reopened.transport.Load().ready.Load())
		})
	}
}

func TestLocalEndpointlessOpenRejectsDanglingActivationIntent(t *testing.T) {
	cfg := localTestConfig(t)
	node, err := OpenLocal(cfg)
	require.NoError(t, err)
	require.NoError(t, node.Close())
	require.NoError(t, os.Symlink("missing-intent", filepath.Join(cfg.Directory, peerActivationFile)))

	node, err = OpenLocal(cfg)
	if node != nil {
		require.NoError(t, node.Close())
	}
	require.ErrorContains(t, err, "persistent peer activation requires peer credentials")
}

func TestProcessActivationExistingIntentRequiresDirectoryFence(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, peerActivationFile)
	want := peerActivation{Peer: Peer{ID: 1, Address: "127.0.0.1:7661"}, Listen: "127.0.0.1:7661"}
	// Reproduce the state left by a successful link followed by a failed
	// directory sync: the matching intent is visible but not yet trusted.
	require.NoError(t, os.WriteFile(path, encode(want), 0600))
	fenceFailure := errors.New("injected activation directory sync failure")
	calls := 0
	err := preparePeerActivationIntent(path, want, func(parent string) error {
		calls++
		require.Equal(t, directory, parent)
		return fenceFailure
	})
	require.ErrorIs(t, err, fenceFailure)
	require.Equal(t, 1, calls, "matching retry must not trust an unfenced directory entry")
	require.FileExists(t, path)
	// Retrying with a successful fence preserves the same immutable intent.
	require.NoError(t, preparePeerActivationIntent(path, want, func(parent string) error {
		calls++
		return syncStorageDirectory(parent)
	}))
	require.Equal(t, 2, calls)
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, encode(want), actual)
}
