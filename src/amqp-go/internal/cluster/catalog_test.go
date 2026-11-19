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

func TestCatalogLocalSeedActivationAndJoin(t *testing.T) {
	const clusterID = "local-genesis"
	const nonce = "0102030405060708090a0b0c0d0e0f10"
	seed := Peer{ID: 7}
	endpoint := Peer{ID: seed.ID, Address: "broker.example:7001"}
	joining := Peer{ID: 2, Address: "replica.example:7002"}
	c, err := newCatalog(clusterID, 2, []Peer{seed})
	require.NoError(t, err, "one persistent voter must be usable before it has a peer endpoint")
	require.Equal(t, []uint64{seed.ID}, desiredMembers(c.view(), 1))

	command := func(c *catalog, cmd any) catalogReply {
		t.Helper()
		var result catalogReply
		require.NoError(t, json.Unmarshal(c.Apply(encode(cmd)), &result))
		return result
	}
	report := catalogCommand{Report: &placement{Group: 1, Index: 7, Voters: []uint64{seed.ID}}}
	require.Empty(t, command(c, report).Error, "a voter without an endpoint can report a committed placement")
	require.Equal(t, uint64(7), c.view().Placements[0].Index)
	join := catalogCommand{Join: &joining, Nonce: nonce}
	require.NotEmpty(t, command(c, join).Error)
	local, err := c.Snapshot()
	require.NoError(t, err)
	activate := map[string]any{"ActivateSeed": endpoint}
	for _, invalid := range []any{
		map[string]any{"ActivateSeed": seed},
		map[string]any{"ActivateSeed": joining},
		map[string]any{"ActivateSeed": endpoint, "Report": report.Report},
		map[string]any{"ActivateSeed": endpoint, "Join": joining, "Nonce": nonce},
	} {
		require.NotEmpty(t, command(c, invalid).Error)
	}
	unchanged, err := c.Snapshot()
	require.NoError(t, err)
	require.Equal(t, local, unchanged)

	// A new replica can start with a seed discovery address and receive an
	// earlier snapshot from the lineage's local-only period.
	follower, err := newCatalog(clusterID, 2, []Peer{{seed.ID, "discovery.example:7001"}})
	require.NoError(t, err)
	require.NoError(t, follower.Restore(local))
	for _, replica := range []*catalog{c, follower} {
		before := replica.view()
		applied := command(replica, activate)
		require.Empty(t, applied.Error)
		require.Equal(t, before.Epoch+1, applied.View.Epoch)
		require.Equal(t, endpoint.Address, address(applied.View, seed.ID))
		require.Empty(t, before.Nodes[0].Address, "activation changed an earlier published view")
		require.Equal(t, applied.View, command(replica, activate).View)
		require.NotEmpty(t, command(replica, map[string]any{"ActivateSeed": Peer{seed.ID, "wrong.example:7001"}}).Error)
		require.NotEmpty(t, command(replica, catalogCommand{Join: &endpoint, Nonce: nonce}).Error)
		require.Empty(t, command(replica, join).Error)
		require.Empty(t, command(replica, activate).Error, "activation retry after a successful join")
	}
	require.Equal(t, c.view(), follower.view())
	joined, err := c.Snapshot()
	require.NoError(t, err)
	for _, initial := range []Peer{seed, endpoint} {
		recovered, err := newCatalog(clusterID, 2, []Peer{initial})
		require.NoError(t, err)
		require.NoError(t, recovered.Restore(joined))
		require.Equal(t, c.view(), recovered.view())
	}

	for _, damage := range []func(*view){
		func(v *view) { v.Nodes[1].Address = "" }, // seed lost its endpoint with a joined replica
		func(v *view) { v.Nodes[0].Address = "" }, // learner lacks an endpoint
		func(v *view) { v.Nodes[1].ID++ },         // original voter missing
		func(v *view) { v.Nonces[seed.ID] = nonce },
		func(v *view) { v.Nonces[99] = nonce },
		func(v *view) { v.Nonces[joining.ID] = "" },
	} {
		var snap catalogSnapshot
		require.NoError(t, json.Unmarshal(joined, &snap))
		damage(&snap.View)
		require.Error(t, follower.Restore(encode(snap)))
		require.Equal(t, c.view(), follower.view())
	}
}

func TestCatalogConfiguredSeedEndpoints(t *testing.T) {
	seeds := []Peer{{1, "one.example:7001"}, {2, "two.example:7002"}}
	_, err := newCatalog("configured", 1, []Peer{{seeds[0].ID, ""}, seeds[1]})
	require.Error(t, err)
	_, err = newCatalog("configured", 1, []Peer{{ID: 0}})
	require.Error(t, err)
	c, err := newCatalog("configured", 1, seeds)
	require.NoError(t, err)
	var reply catalogReply
	require.NoError(t, json.Unmarshal(c.Apply(encode(map[string]any{"ActivateSeed": seeds[0]})), &reply))
	require.NotEmpty(t, reply.Error)
	snapshot, err := c.Snapshot()
	require.NoError(t, err)
	var snap catalogSnapshot
	require.NoError(t, json.Unmarshal(snapshot, &snap))
	snap.View.Nodes[0].Address = "rebound.example:7001"
	require.Error(t, c.Restore(encode(snap)))
	require.Equal(t, seeds, c.view().Nodes)
}
