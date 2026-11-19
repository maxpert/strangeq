package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessDurabilityRequiresLocalCluster(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "local"}[local], func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Cluster.Local = local
			cfg.Storage.Durability = "process"
			if local {
				require.NoError(t, cfg.Validate())
				require.Equal(t, "process", cfg.Durability())
				require.Equal(t, "process", cfg.GetEngine().Durability)
				require.False(t, cfg.GetEngine().WALSyncDisabled, "process must never become the legacy none policy")
			} else {
				require.ErrorContains(t, cfg.Validate(), "storage.durability process requires cluster.local")
			}
		})
	}
	cfg := DefaultConfig()
	cfg.Cluster = ClusterConfig{ID: "addressed", NodeID: 1, PeerAddress: "127.0.0.1:24401", Seeds: []string{"1@127.0.0.1:24401"}, TokenFile: "peer.token", AllowLoopbackPlaintext: true}
	cfg.Storage.Durability = "process"
	require.ErrorContains(t, cfg.Validate(), "requires cluster.local")
}

func TestProcessDurabilityLoadsAndRoundTripsExplicitLocalProfile(t *testing.T) {
	for _, ext := range []string{"yaml", "json"} {
		t.Run(ext, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "broker."+ext)
			content := "storage:\n  durability: process\ncluster:\n  local: true\n  shards: 1\n"
			if ext == "json" {
				content = `{"storage":{"durability":"process"},"cluster":{"local":true,"shards":1}}`
			}
			require.NoError(t, os.WriteFile(path, []byte(content), 0600))
			cfg, report, err := LoadConfig(path)
			require.NoError(t, err)
			require.Empty(t, report.UnknownKeys)
			require.NoError(t, cfg.Validate())
			require.Equal(t, "process", cfg.Durability())
			require.NoError(t, cfg.Save(path))
			reopened, _, err := LoadConfig(path)
			require.NoError(t, err)
			require.NoError(t, reopened.Validate())
			require.Equal(t, "process", reopened.Durability())
		})
	}
}

func TestProcessDurabilityBuilderKeepsLocalRequirement(t *testing.T) {
	_, err := NewConfigBuilder().WithDurability("process").Build()
	require.ErrorContains(t, err, "requires cluster.local")
	cfg := DefaultConfig()
	cfg.Cluster.Local = true
	built, err := FromConfig(cfg).WithDurability("process").Build()
	require.NoError(t, err)
	require.Equal(t, "process", built.Durability())
}
