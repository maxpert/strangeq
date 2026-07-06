package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRecovery_DurableQueuePolicyReResolved drives the real server recovery
// path (RecoveryManager.PerformRecovery) across a simulated restart and
// verifies the durable queue's x-argument policy is re-resolved from the
// persisted arguments by the same resolution function used at declare time.
func TestRecovery_DurableQueuePolicyReResolved(t *testing.T) {
	dataDir := t.TempDir()
	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}

	// "First boot": declare a durable queue with policy arguments.
	store1, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	broker1 := broker.NewStorageBroker(store1, engineConfig)

	_, err = broker1.DeclareQueue("recovered-policy-q", true, false, false, map[string]interface{}{
		"x-message-ttl":             int32(60000),
		"x-dead-letter-exchange":    "dlx",
		"x-dead-letter-routing-key": "dead",
		"x-overflow":                "reject-publish",
	})
	require.NoError(t, err)
	require.NotNil(t, broker1.GetQueuePolicy("recovered-policy-q"))

	// Durable metadata must have captured the queue (recovery reads it).
	metadata, err := store1.GetDurableEntityMetadata()
	require.NoError(t, err)
	require.NotEmpty(t, metadata.Queues)
	require.NoError(t, store1.Close())

	// "Restart": fresh storage over the same data dir, fresh broker, real
	// RecoveryManager as wired by ServerBuilder.Build.
	store2, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	defer store2.Close()
	broker2 := broker.NewStorageBroker(store2, engineConfig)
	adapter := NewStorageBrokerAdapter(broker2)

	recoveryManager := NewRecoveryManager(store2, adapter, zap.NewNop())
	stats, err := recoveryManager.PerformRecovery()
	require.NoError(t, err)
	require.Empty(t, stats.ValidationErrors)
	require.GreaterOrEqual(t, stats.DurableQueuesRecovered, 1)

	policy := broker2.GetQueuePolicy("recovered-policy-q")
	require.NotNil(t, policy, "recovery must re-resolve the queue policy")
	require.True(t, policy.HasMessageTTL)
	require.Equal(t, 60*time.Second, policy.MessageTTL)
	require.True(t, policy.HasDeadLetterExchange)
	require.Equal(t, "dlx", policy.DeadLetterExchange)
	require.True(t, policy.HasDeadLetterRoutingKey)
	require.Equal(t, "dead", policy.DeadLetterRoutingKey)
	require.Equal(t, broker.OverflowRejectPublish, policy.Overflow)
	require.False(t, policy.HasMaxLength)
	require.False(t, policy.HasQueueExpires)
}
