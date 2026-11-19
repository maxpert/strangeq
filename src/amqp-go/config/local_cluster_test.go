package config

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLocalClusterProfileUsesUnifiedEngineWithoutPeerSettings(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, json.Unmarshal([]byte(`{"cluster":{"local":true,"shards":1}}`), cfg))
	require.True(t, cfg.Cluster.Enabled(), "cluster.local must select the unified singleton engine")
	require.NoError(t, cfg.Validate())
}
