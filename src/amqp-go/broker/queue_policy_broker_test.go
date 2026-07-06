package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// TestDeclareQueue_PolicyResolvedAndAttached verifies that DeclareQueue
// resolves x-arguments once and attaches the typed policy to the queue's
// runtime state, reachable via both GetQueuePolicy and QueueState.Policy().
func TestDeclareQueue_PolicyResolvedAndAttached(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := broker.DeclareQueue("policy-q", false, false, false, map[string]interface{}{
		"x-message-ttl":          int32(30000),
		"x-dead-letter-exchange": "dlx",
		"x-max-length":           int64(500),
		"x-overflow":             "reject-publish",
	})
	require.NoError(t, err)

	policy := broker.GetQueuePolicy("policy-q")
	require.NotNil(t, policy, "policy must be attached at declare time")
	require.True(t, policy.HasMessageTTL)
	require.Equal(t, 30*time.Second, policy.MessageTTL)
	require.True(t, policy.HasDeadLetterExchange)
	require.Equal(t, "dlx", policy.DeadLetterExchange)
	require.True(t, policy.HasMaxLength)
	require.Equal(t, int64(500), policy.MaxLength)
	require.Equal(t, OverflowRejectPublish, policy.Overflow)

	// The hot-path access point is QueueState.Policy() — same pointer, one
	// atomic load.
	qs := broker.getOrCreateQueueState("policy-q")
	require.Same(t, policy, qs.Policy())
}

// TestDeclareQueue_NoArgumentsNilPolicy verifies queues without known
// x-arguments carry a nil policy (the hot path's cheap branch).
func TestDeclareQueue_NoArgumentsNilPolicy(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := broker.DeclareQueue("plain-q", false, false, false, nil)
	require.NoError(t, err)
	require.Nil(t, broker.GetQueuePolicy("plain-q"))

	_, err = broker.DeclareQueue("unknown-args-q", false, false, false, map[string]interface{}{
		"x-queue-type": "classic",
	})
	require.NoError(t, err)
	require.Nil(t, broker.GetQueuePolicy("unknown-args-q"))
}

// TestDeclareQueue_InvalidArgumentFailsDeclare verifies malformed known
// x-arguments fail the declare with ErrInvalidQueueArgument and leave no
// queue behind.
func TestDeclareQueue_InvalidArgumentFailsDeclare(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := broker.DeclareQueue("bad-q", false, false, false, map[string]interface{}{
		"x-message-ttl": "not-a-number",
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidQueueArgument),
		"error must wrap ErrInvalidQueueArgument, got: %v", err)

	// The queue must not have been stored.
	_, err = broker.storage.GetQueue("bad-q")
	require.ErrorIs(t, err, interfaces.ErrQueueNotFound)
	require.Nil(t, broker.GetQueuePolicy("bad-q"))
}

// TestDeclareQueue_RedeclareKeepsOriginalPolicy documents current redeclare
// semantics: arguments on a redeclare of an existing queue are not compared
// (known gap vs. AMQP 0.9.1 equivalence rules — differing args should be a
// 406) and must not overwrite the policy resolved from the original
// arguments.
func TestDeclareQueue_RedeclareKeepsOriginalPolicy(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := broker.DeclareQueue("redeclare-q", false, false, false, map[string]interface{}{
		"x-message-ttl": int32(10000),
	})
	require.NoError(t, err)

	// Redeclare with different (and even invalid) arguments: today these are
	// ignored entirely, so the declare succeeds and the original policy stays.
	_, err = broker.DeclareQueue("redeclare-q", false, false, false, map[string]interface{}{
		"x-message-ttl": int32(99999),
	})
	require.NoError(t, err)

	policy := broker.GetQueuePolicy("redeclare-q")
	require.NotNil(t, policy)
	require.Equal(t, 10*time.Second, policy.MessageTTL,
		"redeclare arguments must not overwrite the original policy")
}

// TestDeclareQueue_PolicySurvivesBrokerRestart simulates a broker restart on
// the same storage directory: the durable queue is found in storage by the
// new broker instance and its policy is re-resolved from the persisted
// arguments by the same resolution function.
func TestDeclareQueue_PolicySurvivesBrokerRestart(t *testing.T) {
	dataDir := t.TempDir()
	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}

	store1, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	broker1 := NewStorageBroker(store1, engineConfig)

	_, err = broker1.DeclareQueue("durable-policy-q", true, false, false, map[string]interface{}{
		"x-message-ttl": int32(45000),
		"x-max-length":  int64(2000),
	})
	require.NoError(t, err)
	require.NotNil(t, broker1.GetQueuePolicy("durable-policy-q"))
	require.NoError(t, store1.Close())

	// "Restart": fresh storage over the same data dir, fresh broker.
	store2, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	defer store2.Close()
	broker2 := NewStorageBroker(store2, engineConfig)

	// Re-declare (as recovery does): the queue exists in persisted metadata,
	// so this takes the existing-queue branch and re-resolves the policy from
	// the stored arguments (which may have been JSON round-tripped to float64).
	_, err = broker2.DeclareQueue("durable-policy-q", true, false, false, nil)
	require.NoError(t, err)

	policy := broker2.GetQueuePolicy("durable-policy-q")
	require.NotNil(t, policy, "policy must be re-resolved after restart")
	require.True(t, policy.HasMessageTTL)
	require.Equal(t, 45*time.Second, policy.MessageTTL)
	require.True(t, policy.HasMaxLength)
	require.Equal(t, int64(2000), policy.MaxLength)
	require.False(t, policy.HasDeadLetterExchange)
}
