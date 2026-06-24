package storage

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// P0 Correctness fixes: storage layer tests
//
// C4: Segment format preserves AMQP metadata — serializeSegmentMessage must
//     store Exchange, RoutingKey, DeliveryMode (not just offset+body).
//     readSegmentMessageAt must restore all fields.
// ============================================================================

// createTestSegmentManager creates a SegmentManager for testing
func createTestSegmentManager(t *testing.T) *SegmentManager {
	t.Helper()
	tmpDir := t.TempDir()
	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() { sm.Close() })
	return sm
}

// TestSegmentPreservesMetadata verifies that serializeSegmentMessage /
// readSegmentMessageAt preserve Exchange, RoutingKey, and DeliveryMode (C4 fix).
func TestSegmentPreservesMetadata(t *testing.T) {
	sm := createTestSegmentManager(t)
	original := &protocol.Message{
		DeliveryTag:  42,
		Body:         []byte("test body content"),
		Exchange:     "amq.direct",
		RoutingKey:   "routing.key",
		DeliveryMode: 2, // persistent
	}

	// Write message to segment via CheckpointBatch (the normal write path)
	msgs := []*RecoveryMessage{{Message: original, Offset: 42, QueueName: "test-queue"}}
	if err := sm.CheckpointBatch("test-queue", msgs); err != nil {
		t.Fatalf("CheckpointBatch failed: %v", err)
	}

	// Read it back
	qs := sm.getOrCreateQueueSegments("test-queue")
	msg, err := qs.readMessage(42)
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}

	// Verify all fields preserved
	if msg.Exchange != original.Exchange {
		t.Errorf("Exchange: expected %q, got %q", original.Exchange, msg.Exchange)
	}
	if msg.RoutingKey != original.RoutingKey {
		t.Errorf("RoutingKey: expected %q, got %q", original.RoutingKey, msg.RoutingKey)
	}
	if msg.DeliveryMode != original.DeliveryMode {
		t.Errorf("DeliveryMode: expected %d, got %d", original.DeliveryMode, msg.DeliveryMode)
	}
	if string(msg.Body) != string(original.Body) {
		t.Errorf("Body: expected %q, got %q", string(original.Body), string(msg.Body))
	}
	if msg.DeliveryTag != original.DeliveryTag {
		t.Errorf("DeliveryTag: expected %d, got %d", original.DeliveryTag, msg.DeliveryTag)
	}
}

// TestSegmentEmptyExchangeAndRoutingKey verifies edge case: empty exchange/routing key
func TestSegmentEmptyExchangeAndRoutingKey(t *testing.T) {
	sm := createTestSegmentManager(t)
	original := &protocol.Message{
		DeliveryTag:  1,
		Body:         []byte("body"),
		Exchange:     "",
		RoutingKey:   "queue-name",
		DeliveryMode: 1,
	}

	msgs := []*RecoveryMessage{{Message: original, Offset: 1, QueueName: "test-queue"}}
	if err := sm.CheckpointBatch("test-queue", msgs); err != nil {
		t.Fatalf("CheckpointBatch failed: %v", err)
	}

	qs := sm.getOrCreateQueueSegments("test-queue")
	msg, err := qs.readMessage(1)
	if err != nil {
		t.Fatalf("readMessage failed: %v", err)
	}

	if msg.Exchange != "" {
		t.Errorf("Exchange: expected empty, got %q", msg.Exchange)
	}
	if msg.RoutingKey != "queue-name" {
		t.Errorf("RoutingKey: expected %q, got %q", "queue-name", msg.RoutingKey)
	}
}

// TestSegmentRoundtripMultipleMessages verifies multiple messages preserve metadata
func TestSegmentRoundtripMultipleMessages(t *testing.T) {
	sm := createTestSegmentManager(t)
	messages := []*protocol.Message{
		{DeliveryTag: 1, Body: []byte("msg1"), Exchange: "ex1", RoutingKey: "rk1", DeliveryMode: 2},
		{DeliveryTag: 2, Body: []byte("msg2"), Exchange: "ex2", RoutingKey: "rk2", DeliveryMode: 1},
		{DeliveryTag: 3, Body: []byte("msg3"), Exchange: "", RoutingKey: "q3", DeliveryMode: 2},
	}

	var recoveryMsgs []*RecoveryMessage
	for _, m := range messages {
		recoveryMsgs = append(recoveryMsgs, &RecoveryMessage{Message: m, Offset: m.DeliveryTag, QueueName: "test-queue"})
	}
	if err := sm.CheckpointBatch("test-queue", recoveryMsgs); err != nil {
		t.Fatalf("CheckpointBatch failed: %v", err)
	}

	qs := sm.getOrCreateQueueSegments("test-queue")
	for _, original := range messages {
		msg, err := qs.readMessage(original.DeliveryTag)
		if err != nil {
			t.Fatalf("readMessage %d failed: %v", original.DeliveryTag, err)
		}
		if msg.Exchange != original.Exchange || msg.RoutingKey != original.RoutingKey ||
			msg.DeliveryMode != original.DeliveryMode || string(msg.Body) != string(original.Body) {
			t.Errorf("message %d metadata mismatch: got ex=%q rk=%q dm=%d, want ex=%q rk=%q dm=%d",
				original.DeliveryTag, msg.Exchange, msg.RoutingKey, msg.DeliveryMode,
				original.Exchange, original.RoutingKey, original.DeliveryMode)
		}
	}
}
