package broker

// WS-D LOW: PurgeQueue on a NEVER-declared queue must return the
// queue-not-found sentinel (interfaces.ErrQueueNotFound → 404 through
// brokerErrorReply) instead of materializing a phantom QueueState and
// replying purge-ok 0 (RabbitMQ's with_or_die parity: purge of a missing
// queue is a channel exception, exactly like a passive declare).

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPurge_NeverDeclaredQueue_404NoPhantomState(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.PurgeQueue("ghost")
	require.Error(t, err, "purge of a never-declared queue is an error")
	assert.True(t, errors.Is(err, interfaces.ErrQueueNotFound),
		"the error must carry the queue-not-found sentinel (404 via brokerErrorReply), got %v", err)

	_, exists := b.queueStates.Load("ghost")
	assert.False(t, exists, "a failed purge must not materialize a phantom QueueState")

	// And the 404 mapping the server relies on still classifies it.
	_, _, ok := brokerPurgeClassifies(err)
	assert.True(t, ok, "brokerErrorReply must know this error")
}

// brokerPurgeClassifies isolates the errors.Is chain the server's
// brokerErrorReply performs, without importing package server (cycle).
func brokerPurgeClassifies(err error) (uint16, string, bool) {
	if errors.Is(err, interfaces.ErrQueueNotFound) {
		return 404, "NOT_FOUND - " + err.Error(), true
	}
	return 0, "", false
}

func TestPurge_DeclaredThenDeletedQueue_Also404(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "ephemeral")
	_, err := b.DeleteQueue("ephemeral", false, false)
	require.NoError(t, err)

	_, err = b.PurgeQueue("ephemeral")
	require.Error(t, err, "purge of a deleted queue is still a 404, not purge-ok 0")
	assert.True(t, errors.Is(err, interfaces.ErrQueueNotFound))
}

func TestPurge_ExistingQueue_StillWorks(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "real")
	publishBody(b, "real", "x")
	purged, err := b.PurgeQueue("real")
	require.NoError(t, err)
	assert.Equal(t, 1, purged)
}
