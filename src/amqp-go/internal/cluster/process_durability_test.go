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
