package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestBroker creates a test broker with in-memory storage
func createTestBroker(t *testing.T) (*StorageBroker, func()) {
	tmpDir := t.TempDir()
	store := storage.NewDisruptorStorageWithDataDir(tmpDir)

	engineConfig := interfaces.EngineConfig{
		AvailableChannelBuffer:  10000,
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}

	broker := NewStorageBroker(store, engineConfig)

	cleanup := func() {
		// Storage cleanup handled by TempDir
	}

	return broker, cleanup
}

// TestDirectExchangeRouting tests direct exchange routing logic
func TestDirectExchangeRouting(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Create a direct exchange
	exchange := &protocol.Exchange{
		Name:       "test.direct",
		Kind:       "direct",
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	err := broker.storage.StoreExchange(exchange)
	require.NoError(t, err)

	// Create queues
	queue1 := &protocol.Queue{Name: "queue1", Durable: false}
	queue2 := &protocol.Queue{Name: "queue2", Durable: false}
	queue3 := &protocol.Queue{Name: "queue3", Durable: false}

	err = broker.storage.StoreQueue(queue1)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue2)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue3)
	require.NoError(t, err)

	// Bind queues
	// queue1 and queue2 bound with same routing key
	err = broker.storage.StoreBinding("queue1", "test.direct", "key1", nil)
	require.NoError(t, err)
	err = broker.storage.StoreBinding("queue2", "test.direct", "key1", nil)
	require.NoError(t, err)
	// queue3 bound with different routing key
	err = broker.storage.StoreBinding("queue3", "test.direct", "key2", nil)
	require.NoError(t, err)

	t.Run("Exact routing key match delivers to bound queues", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("test message"),
			RoutingKey: "key1",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "key1", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "queue1", "queue1 should receive message with key1")
		assert.Contains(t, targetQueues, "queue2", "queue2 should receive message with key1")
		assert.NotContains(t, targetQueues, "queue3", "queue3 should NOT receive message with key1")
		assert.Len(t, targetQueues, 2, "Should route to exactly 2 queues")
	})

	t.Run("Non-matching routing key does not deliver", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("test message"),
			RoutingKey: "key2",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "key2", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "queue3", "queue3 should receive message with key2")
		assert.NotContains(t, targetQueues, "queue1", "queue1 should NOT receive message with key2")
		assert.NotContains(t, targetQueues, "queue2", "queue2 should NOT receive message with key2")
		assert.Len(t, targetQueues, 1, "Should route to exactly 1 queue")
	})

	t.Run("Unmatched routing key delivers to no queues", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("test message"),
			RoutingKey: "nonexistent",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "nonexistent", message)
		require.NoError(t, err)
		assert.Len(t, targetQueues, 0, "Should route to zero queues for unmatched key")
	})
}

// TestFanoutExchangeRouting tests fanout exchange routing logic
func TestFanoutExchangeRouting(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Create a fanout exchange
	exchange := &protocol.Exchange{
		Name:       "test.fanout",
		Kind:       "fanout",
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	err := broker.storage.StoreExchange(exchange)
	require.NoError(t, err)

	// Create multiple queues
	queue1 := &protocol.Queue{Name: "fanout.queue1", Durable: false}
	queue2 := &protocol.Queue{Name: "fanout.queue2", Durable: false}
	queue3 := &protocol.Queue{Name: "fanout.queue3", Durable: false}

	err = broker.storage.StoreQueue(queue1)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue2)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue3)
	require.NoError(t, err)

	// Bind all queues to fanout exchange (routing keys should be ignored)
	err = broker.storage.StoreBinding("fanout.queue1", "test.fanout", "different.key1", nil)
	require.NoError(t, err)
	err = broker.storage.StoreBinding("fanout.queue2", "test.fanout", "different.key2", nil)
	require.NoError(t, err)
	err = broker.storage.StoreBinding("fanout.queue3", "test.fanout", "different.key3", nil)
	require.NoError(t, err)

	t.Run("Fanout delivers to all bound queues regardless of routing key", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("fanout message"),
			RoutingKey: "any.routing.key", // This should be ignored
		}

		targetQueues, err := broker.findTargetQueues(exchange, "any.routing.key", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "fanout.queue1", "fanout.queue1 should receive message")
		assert.Contains(t, targetQueues, "fanout.queue2", "fanout.queue2 should receive message")
		assert.Contains(t, targetQueues, "fanout.queue3", "fanout.queue3 should receive message")
		assert.Len(t, targetQueues, 3, "Should route to all 3 queues")
	})

	t.Run("Fanout routing key is ignored", func(t *testing.T) {
		// Test with completely different routing key
		message := &protocol.Message{
			Body:       []byte("fanout message"),
			RoutingKey: "completely.different.key",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "completely.different.key", message)
		require.NoError(t, err)
		assert.Len(t, targetQueues, 3, "Should route to all queues regardless of routing key")
	})
}

// TestTopicExchangeRouting tests topic exchange routing logic
func TestTopicExchangeRouting(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Create a topic exchange
	exchange := &protocol.Exchange{
		Name:       "test.topic",
		Kind:       "topic",
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	err := broker.storage.StoreExchange(exchange)
	require.NoError(t, err)

	// Create queues
	queue1 := &protocol.Queue{Name: "topic.queue1", Durable: false}
	queue2 := &protocol.Queue{Name: "topic.queue2", Durable: false}
	queue3 := &protocol.Queue{Name: "topic.queue3", Durable: false}

	err = broker.storage.StoreQueue(queue1)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue2)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue3)
	require.NoError(t, err)

	// Bind queues with different patterns
	err = broker.storage.StoreBinding("topic.queue1", "test.topic", "exact.match", nil)
	require.NoError(t, err)
	err = broker.storage.StoreBinding("topic.queue2", "test.topic", "#", nil) // Matches everything
	require.NoError(t, err)
	err = broker.storage.StoreBinding("topic.queue3", "test.topic", "other.pattern", nil)
	require.NoError(t, err)

	t.Run("Exact pattern match", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("topic message"),
			RoutingKey: "exact.match",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "exact.match", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "topic.queue1", "queue1 should match exact pattern")
		assert.Contains(t, targetQueues, "topic.queue2", "queue2 should match # wildcard")
		assert.NotContains(t, targetQueues, "topic.queue3", "queue3 should NOT match")
		assert.Len(t, targetQueues, 2, "Should route to 2 queues")
	})

	t.Run("Hash wildcard matches everything", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("topic message"),
			RoutingKey: "any.routing.key.value",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "any.routing.key.value", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "topic.queue2", "queue2 with # pattern should match any key")
		assert.Len(t, targetQueues, 1, "Should route to only queue2 (hash wildcard)")
	})

	t.Run("Non-matching pattern does not deliver", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("topic message"),
			RoutingKey: "completely.different.key",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "completely.different.key", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "topic.queue2", "queue2 with # should still match")
		assert.Len(t, targetQueues, 1, "Should route to only queue2")
	})

	t.Run("Star wildcard not implemented", func(t *testing.T) {
		// This test documents the current limitation
		// Pattern "*.orange.*" should match "quick.orange.rabbit" but currently doesn't
		err = broker.storage.StoreBinding("topic.queue4", "test.topic", "*.orange.*", nil)
		require.NoError(t, err)

		message := &protocol.Message{
			Body:       []byte("topic message"),
			RoutingKey: "quick.orange.rabbit",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "quick.orange.rabbit", message)
		require.NoError(t, err)
		// Currently, star wildcard is NOT implemented, so this won't match
		assert.NotContains(t, targetQueues, "topic.queue4", "Star wildcard NOT IMPLEMENTED - this should fail")
	})
}

// TestHeadersExchangeRouting tests headers exchange routing logic
func TestHeadersExchangeRouting(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Create a headers exchange
	exchange := &protocol.Exchange{
		Name:       "test.headers",
		Kind:       "headers",
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	err := broker.storage.StoreExchange(exchange)
	require.NoError(t, err)

	// Create queues
	queue1 := &protocol.Queue{Name: "headers.queue1", Durable: false}
	queue2 := &protocol.Queue{Name: "headers.queue2", Durable: false}
	queue3 := &protocol.Queue{Name: "headers.queue3", Durable: false}

	err = broker.storage.StoreQueue(queue1)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue2)
	require.NoError(t, err)
	err = broker.storage.StoreQueue(queue3)
	require.NoError(t, err)

	// Bind queues with different header requirements
	// queue1: requires header1=value1 (implicit all mode)
	binding1Args := map[string]interface{}{
		"header1": "value1",
	}
	err = broker.storage.StoreBinding("headers.queue1", "test.headers", "", binding1Args)
	require.NoError(t, err)

	// queue2: requires header1=value1 AND header2=value2 (implicit all mode)
	binding2Args := map[string]interface{}{
		"header1": "value1",
		"header2": "value2",
	}
	err = broker.storage.StoreBinding("headers.queue2", "test.headers", "", binding2Args)
	require.NoError(t, err)

	// queue3: requires header3=value3
	binding3Args := map[string]interface{}{
		"header3": "value3",
	}
	err = broker.storage.StoreBinding("headers.queue3", "test.headers", "", binding3Args)
	require.NoError(t, err)

	t.Run("Matching headers deliver to bound queue", func(t *testing.T) {
		message := &protocol.Message{
			Body: []byte("headers message"),
			Headers: map[string]interface{}{
				"header1": "value1",
			},
		}

		targetQueues, err := broker.findTargetQueues(exchange, "", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "headers.queue1", "queue1 should match header1=value1")
		assert.NotContains(t, targetQueues, "headers.queue2", "queue2 requires both headers")
		assert.NotContains(t, targetQueues, "headers.queue3", "queue3 requires different header")
		assert.Len(t, targetQueues, 1, "Should route to exactly 1 queue")
	})

	t.Run("All required headers must match (all mode)", func(t *testing.T) {
		message := &protocol.Message{
			Body: []byte("headers message"),
			Headers: map[string]interface{}{
				"header1": "value1",
				"header2": "value2",
			},
		}

		targetQueues, err := broker.findTargetQueues(exchange, "", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "headers.queue1", "queue1 should match (has header1)")
		assert.Contains(t, targetQueues, "headers.queue2", "queue2 should match (has both headers)")
		assert.NotContains(t, targetQueues, "headers.queue3", "queue3 requires different header")
		assert.Len(t, targetQueues, 2, "Should route to 2 queues")
	})

	t.Run("Missing header does not deliver", func(t *testing.T) {
		message := &protocol.Message{
			Body: []byte("headers message"),
			Headers: map[string]interface{}{
				"header1": "value1",
				// Missing header2 for queue2
			},
		}

		targetQueues, err := broker.findTargetQueues(exchange, "", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "headers.queue1", "queue1 should match")
		assert.NotContains(t, targetQueues, "headers.queue2", "queue2 should NOT match (missing header2)")
		assert.Len(t, targetQueues, 1, "Should route to only 1 queue")
	})

	t.Run("Wrong header value does not deliver", func(t *testing.T) {
		message := &protocol.Message{
			Body: []byte("headers message"),
			Headers: map[string]interface{}{
				"header1": "wrong.value",
			},
		}

		targetQueues, err := broker.findTargetQueues(exchange, "", message)
		require.NoError(t, err)
		assert.Len(t, targetQueues, 0, "Should route to zero queues for wrong header value")
	})

	t.Run("Empty headers do not match", func(t *testing.T) {
		message := &protocol.Message{
			Body:    []byte("headers message"),
			Headers: nil,
		}

		targetQueues, err := broker.findTargetQueues(exchange, "", message)
		require.NoError(t, err)
		assert.Len(t, targetQueues, 0, "Should route to zero queues for empty headers")
	})

	t.Run("x-match any mode not implemented", func(t *testing.T) {
		// This test documents the current limitation
		// Currently, the implementation always uses "all" mode
		// There's no way to test "any" mode since it's not implemented
		bindingAnyArgs := map[string]interface{}{
			"x-match": "any", // This is ignored by current implementation
			"header1": "value1",
			"header2": "value2",
		}
		err = broker.storage.StoreBinding("headers.queue4", "test.headers", "", bindingAnyArgs)
		require.NoError(t, err)

		message := &protocol.Message{
			Body: []byte("headers message"),
			Headers: map[string]interface{}{
				"header1": "value1",
				// Missing header2
			},
		}

		targetQueues, err := broker.findTargetQueues(exchange, "", message)
		require.NoError(t, err)
		// Current implementation uses "all" mode, so queue4 won't match
		// With "any" mode, it should match because header1 matches
		assert.NotContains(t, targetQueues, "headers.queue4", "x-match: any NOT IMPLEMENTED - uses all mode")
	})
}

// TestDefaultExchangeRouting tests default exchange routing
func TestDefaultExchangeRouting(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Default exchange (empty name)
	exchange := &protocol.Exchange{
		Name:       "",
		Kind:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  make(map[string]interface{}),
	}
	err := broker.storage.StoreExchange(exchange)
	require.NoError(t, err)

	// Create a queue
	queue := &protocol.Queue{Name: "default.queue", Durable: false}
	err = broker.storage.StoreQueue(queue)
	require.NoError(t, err)

	t.Run("Default exchange routes to queue by name", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("default message"),
			RoutingKey: "default.queue", // Routing key = queue name
		}

		targetQueues, err := broker.findTargetQueues(exchange, "default.queue", message)
		require.NoError(t, err)
		assert.Contains(t, targetQueues, "default.queue", "Default exchange should route to queue by name")
		assert.Len(t, targetQueues, 1, "Should route to exactly 1 queue")
	})

	t.Run("Default exchange does not route to non-existent queue", func(t *testing.T) {
		message := &protocol.Message{
			Body:       []byte("default message"),
			RoutingKey: "nonexistent.queue",
		}

		targetQueues, err := broker.findTargetQueues(exchange, "nonexistent.queue", message)
		require.NoError(t, err)
		assert.Len(t, targetQueues, 0, "Should route to zero queues for non-existent queue name")
	})
}
