package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterDualStreamOptInRequiresJournalAndRangeSync(t *testing.T) {
	defaults := DefaultConfig()
	require.False(t, defaults.Cluster.ExperimentalRaftJournalDualStream)
	defaults.Cluster.ExperimentalRaftJournalDualStream = true
	require.ErrorContains(t, defaults.Validate(), "cluster.id")

	path := filepath.Join(t.TempDir(), "broker.yml")
	require.NoError(t, os.WriteFile(path, []byte(`
cluster:
  id: two-stream-fixture
  node_id: 1
  peer_address: "127.0.0.1:24401"
  seeds: ["1@127.0.0.1:24401"]
  token_file: peer.token
  allow_loopback_plaintext: true
  experimental_raft_journal: true
  experimental_raft_range_sync: true
  experimental_raft_journal_dual_stream: true
`), 0600))
	cfg, report, err := LoadConfig(path)
	require.NoError(t, err)
	require.Empty(t, report.UnknownKeys)
	require.True(t, cfg.Cluster.ExperimentalRaftJournalDualStream)
	require.NoError(t, cfg.Validate())
	cfg.Cluster.ExperimentalRaftRangeSync = false
	require.ErrorContains(t, cfg.Validate(), "experimental_raft_journal_dual_stream requires")
	cfg.Cluster.ExperimentalRaftJournal = false
	require.ErrorContains(t, cfg.Validate(), "experimental_raft_journal_dual_stream requires")
}
