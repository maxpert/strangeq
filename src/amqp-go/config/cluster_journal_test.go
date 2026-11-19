package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExperimentalRangeSyncRequiresClusterMode(t *testing.T) {
	cfg := DefaultConfig()
	require.False(t, cfg.Cluster.ExperimentalRaftRangeSync)
	cfg.Cluster.ExperimentalRaftRangeSync = true
	require.ErrorContains(t, cfg.Validate(), "cluster.id")
}

func TestClusterRangeSyncDecodesFromYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.yml")
	require.NoError(t, os.WriteFile(path, []byte(`
cluster:
  id: journal-fixture
  node_id: 1
  peer_address: "127.0.0.1:24401"
  seeds: ["1@127.0.0.1:24401"]
  token_file: peer.token
  allow_loopback_plaintext: true
  experimental_raft_range_sync: true
`), 0600))
	cfg, report, err := LoadConfig(path)
	require.NoError(t, err)
	require.Empty(t, report.UnknownKeys)
	require.True(t, cfg.Cluster.ExperimentalRaftRangeSync)
	require.NoError(t, cfg.Validate())

}

func TestExperimentalReplicaBodiesRequireAssignments(t *testing.T) {
	defaults := DefaultConfig()
	require.False(t, defaults.Cluster.ExperimentalReplicaBodies)
	defaults.Cluster.ExperimentalReplicaBodies = true
	require.ErrorContains(t, defaults.Validate(), "cluster.id")

	path := filepath.Join(t.TempDir(), "broker.yml")
	require.NoError(t, os.WriteFile(path, []byte(`
cluster:
  id: replica-bodies-fixture
  node_id: 1
  peer_address: "127.0.0.1:24401"
  seeds: ["1@127.0.0.1:24401"]
  token_file: peer.token
  allow_loopback_plaintext: true
  experimental_assignments: true
  experimental_replica_bodies: true
`), 0600))
	cfg, report, err := LoadConfig(path)
	require.NoError(t, err)
	require.Empty(t, report.UnknownKeys)
	require.True(t, cfg.Cluster.ExperimentalReplicaBodies)
	require.NoError(t, cfg.Validate())

	cfg.Cluster.ExperimentalAssignments = false
	require.ErrorContains(t, cfg.Validate(), "experimental_replica_bodies requires experimental_assignments")
}

func TestExperimentalJournalSmallBodyCompressionOptIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.yml")
	content := `
cluster:
  id: small-body-test
  node_id: 1
  peer_address: "127.0.0.1:24401"
  seeds: ["1@127.0.0.1:24401"]
  token_file: peer.token
  allow_loopback_plaintext: true
  experimental_assignments: true
  experimental_journal_small_body_compression: true
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	cfg, report, err := LoadConfig(path)
	require.NoError(t, err)
	require.Empty(t, report.UnknownKeys)
	require.ErrorContains(t, cfg.Validate(), "experimental_journal_small_body_compression requires experimental_journal_body_compression")
	require.NoError(t, os.WriteFile(path, []byte(content+"  experimental_journal_body_compression: true\n"), 0600))
	cfg, _, err = LoadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
}
