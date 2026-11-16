package cluster

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlacementRetainsShardAndLimitsMovement(t *testing.T) {
	peers := []Peer{{1, "127.0.0.1:20001"}, {2, "127.0.0.1:20002"}, {3, "127.0.0.1:20003"}}
	c, err := newCatalog("test", 32, peers)
	require.NoError(t, err)
	before := c.view()
	peer := Peer{4, "127.0.0.1:20004"}
	cmd, _ := json.Marshal(catalogCommand{Join: &peer, Nonce: "0102030405060708090a0b0c0d0e0f10"})
	var result catalogReply
	require.NoError(t, json.Unmarshal(c.Apply(cmd), &result))
	require.Empty(t, result.Error)
	after := c.view()
	require.Equal(t, before.Shards, after.Shards)
	require.Equal(t, before.Placements, after.Placements)
	require.Len(t, after.Nodes, 4)
	moved := 0
	for id := uint64(1); id <= after.Shards; id++ {
		before := rank(before, id)
		after := rank(after, id)
		require.Len(t, after, 3)
		if contains(after, 4) {
			moved++
			for _, replica := range after {
				if replica != 4 {
					require.Contains(t, before, replica)
				}
			}
		} else {
			require.Equal(t, before, after)
		}
	}
	require.Positive(t, moved)
	require.Less(t, moved, int(after.Shards))

	require.NoError(t, json.Unmarshal(c.Apply(cmd), &result))
	require.Equal(t, after.Epoch, result.View.Epoch)
	mismatch, _ := json.Marshal(catalogCommand{Join: &peer, Nonce: "10ff030405060708090a0b0c0d0e0f10"})
	require.NoError(t, json.Unmarshal(c.Apply(mismatch), &result))
	require.NotEmpty(t, result.Error)

	report := placement{Group: 1, Index: 10, Voters: []uint64{1, 2, 4}}
	raw, _ := json.Marshal(catalogCommand{Report: &report})
	require.NoError(t, json.Unmarshal(c.Apply(raw), &result))
	require.Empty(t, result.Error)
	require.Equal(t, report, c.view().Placements[0])
	report.Index--
	raw, _ = json.Marshal(catalogCommand{Report: &report})
	c.Apply(raw)
	require.Equal(t, uint64(10), c.view().Placements[0].Index)

	snapshot, err := c.Snapshot()
	require.NoError(t, err)
	recovered, err := newCatalog("test", 32, peers)
	require.NoError(t, err)
	require.NoError(t, recovered.Restore(snapshot))
	require.Equal(t, c.view(), recovered.view())
	for _, invalid := range []string{"{}", fmt.Sprintf("%q", "invalid")} {
		require.Error(t, recovered.Restore([]byte(invalid)))
	}
	require.Equal(t, c.view(), recovered.view())
}
