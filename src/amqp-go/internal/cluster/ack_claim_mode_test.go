//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package cluster

import (
	"encoding/json"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
)

func TestAckAndClaimModeFencesExistingJournalAndIncompatiblePeers(t *testing.T) {
	t.Run("legacy journal", func(t *testing.T) {
		legacy := journalTestNodeConfig(t, true)
		require.NoError(t, Initialize(legacy))
		before := journalTestStorageDigest(t, legacy.Directory)
		enabled := legacy
		enableAckAndClaimForTest(t, &enabled)
		opened, err := Open(enabled)
		if opened != nil {
			require.NoError(t, opened.Close())
		}
		require.ErrorContains(t, err, "storage mode", "the new opcode needs a fresh journal authority")
		require.Equal(t, before, journalTestStorageDigest(t, legacy.Directory))
	})

	t.Run("fresh journal", func(t *testing.T) {
		legacy := journalTestNodeConfig(t, true)
		enabled := legacy
		enableAckAndClaimForTest(t, &enabled)
		require.NoError(t, Initialize(enabled))
		opened, err := Open(legacy)
		if opened != nil {
			require.NoError(t, opened.Close())
		}
		require.ErrorContains(t, err, "storage mode", "a peer without opcode support cannot reopen the new journal")

		node, err := Open(enabled)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, node.Close()) })
		capability, ok := any(node).(interface{ SupportsAckAndClaim() bool })
		require.True(t, ok && capability.SupportsAckAndClaim())
		body, err := node.client.Membership(t.Context(), enabled.Listen, nil)
		require.NoError(t, err)
		require.NotEmpty(t, body)
		incompatible, err := clusterwire.NewClient(clusterwire.Config{
			Listen: legacy.Listen, ClusterID: legacy.ClusterID, Token: legacy.Token, AllowInsecure: true,
		})
		require.NoError(t, err)
		t.Cleanup(incompatible.Close)
		_, err = incompatible.Membership(t.Context(), legacy.Listen, nil)
		require.ErrorContains(t, err, "HTTP status 401", "an incompatible peer cannot join the new wire mode")
	})
}

func enableAckAndClaimForTest(t *testing.T, cfg *Config) {
	t.Helper()
	// Decode the option by name so the fixture also exercises the old Node
	// entry point before the option is added to Config.
	require.NoError(t, json.Unmarshal([]byte(`{"ExperimentalAckAndClaim":true}`), cfg))
}
