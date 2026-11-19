package main

import (
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/require"
)

func TestClusterOptionsPreserveExplicitProcessDurabilityAndJournalProfile(t *testing.T) {
	for _, mode := range []string{"", interfaces.DurabilityBarrier, interfaces.DurabilityFlush, interfaces.DurabilityNone, interfaces.DurabilityProcess} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Storage.Durability = mode
			cfg.Cluster = config.ClusterConfig{Local: true, Shards: 1, ExperimentalAssignments: true, ExperimentalRaftRangeSync: true, ExperimentalRaftJournalDualStream: true}
			options, err := clusterOptions(cfg)
			require.NoError(t, err)
			require.Equal(t, mode == interfaces.DurabilityProcess, options.ProcessDurability)
			require.True(t, options.ExperimentalAssignments)
			require.True(t, options.ExperimentalRaftRangeSync)
			require.True(t, options.ExperimentalRaftJournalDualStream)
		})
	}
}
