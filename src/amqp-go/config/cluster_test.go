package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterConfigRequiresExplicitSecurePeerSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.yml")
	require.NoError(t, os.WriteFile(path, []byte(`
cluster:
  id: demo
  node_id: 4
  peer_address: 127.0.0.1:24404
  seeds: ["1@127.0.0.1:24401"]
  discovery: ["127.0.0.1:24402", "127.0.0.1:24403"]
  token_file: peer.token
  allow_loopback_plaintext: true
`), 0600))
	cfg, report, err := LoadConfig(path)
	require.NoError(t, err)
	require.Empty(t, report.UnknownKeys)
	require.NoError(t, cfg.Validate())
	cfg.ResolvePaths(dir)
	require.Equal(t, filepath.Join(dir, "peer.token"), cfg.Cluster.TokenFile)
	require.Equal(t, uint64(4), cfg.Cluster.NodeID)
	require.Equal(t, []string{"1@127.0.0.1:24401"}, cfg.Cluster.Seeds)
	require.Equal(t, []string{"127.0.0.1:24402", "127.0.0.1:24403"}, cfg.Cluster.Discovery)

	cfg.Cluster.Discovery = []string{"192.0.2.1:24402"}
	require.ErrorContains(t, cfg.Validate(), "discovery")
	cfg.Cluster.Discovery = []string{"127.0.0.1:0"}
	require.ErrorContains(t, cfg.Validate(), "discovery")
	cfg.Cluster.Discovery = nil
	seeds := cfg.Cluster.Seeds
	cfg.Cluster.Seeds = nil
	require.ErrorContains(t, cfg.Validate(), "one to three")
	cfg.Cluster.Seeds = append(seeds, "2@127.0.0.1:24402", "3@127.0.0.1:24403")
	require.NoError(t, cfg.Validate())
	cfg.Cluster.Seeds = append(cfg.Cluster.Seeds, "4@127.0.0.1:24404")
	require.ErrorContains(t, cfg.Validate(), "one to three")
	cfg.Cluster.Seeds = seeds

	cfg.Cluster.AllowLoopbackPlaintext = false
	require.ErrorContains(t, cfg.Validate(), "peer TLS")
	cfg.Cluster.PeerCertFile = "peer.crt"
	cfg.Cluster.PeerKeyFile = "peer.key"
	require.NoError(t, cfg.Validate())
	cfg.Management.Enabled = true
	require.ErrorContains(t, cfg.Validate(), "management")
	require.NoError(t, DefaultConfig().Validate())
}
