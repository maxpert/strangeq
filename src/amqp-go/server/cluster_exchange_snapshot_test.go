package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterExchangeAdmissionKeepsSnapshotWithinLimit(t *testing.T) {
	machine := NewClusterMachine()
	controlCharacters := strings.Repeat("\x01", 255)
	for id := range maxClusterExchanges {
		name := fmt.Sprintf("%04d%s", id, controlCharacters[:251])
		declared := machine.applyExchange(clusterExchangeCommand{
			Kind: exchangeDeclare, Name: name,
			Exchange: clusterExchange{Name: name, Type: "direct", Durable: true},
		})
		if declared.Code == "limit" {
			break
		}
		require.NoError(t, declared.err(name))
		bound := machine.applyExchange(clusterExchangeCommand{
			Kind: exchangeBind, Name: name,
			Binding: clusterBinding{
				Target: clusterTarget{Queue: controlCharacters, Incarnation: 1},
				Key:    controlCharacters,
			},
		})
		if bound.Code == "limit" {
			break
		}
		require.NoError(t, bound.err(name))
	}
	require.NotEmpty(t, machine.exchanges)
	require.Less(t, len(machine.exchanges), maxClusterExchanges, "fixture must reach the snapshot byte budget")

	snapshot, err := machine.Snapshot()
	require.NoError(t, err, "admitted topology must be checkpointable")
	restored := NewClusterMachine()
	require.NoError(t, restored.Restore(snapshot))
	require.Equal(t, machine.exchanges, restored.exchanges)
}
