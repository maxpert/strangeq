package metrics

import (
	"testing"
)

// Use a single collector for all tests to avoid duplicate registration
var testCollector = NewCollector("test")

func TestNewCollector(t *testing.T) {
	if testCollector == nil {
		t.Fatal("Expected collector to be created")
	}

	// Test that metrics are initialized
	if testCollector.ConnectionsTotal == nil {
		t.Error("ConnectionsTotal not initialized")
	}
	if testCollector.MessagesPublished == nil {
		t.Error("MessagesPublished not initialized")
	}
}

func TestRecordConnectionOperations(t *testing.T) {
	// Record connection created
	testCollector.RecordConnectionCreated()

	// Record connection closed
	testCollector.RecordConnectionClosed()

	// No assertions needed - just verify no panics
}

func TestRecordMessageOperations(t *testing.T) {
	// Record published message
	testCollector.RecordMessagePublished(1024)

	// Record delivered message
	testCollector.RecordMessageDelivered(512)

	// Record acknowledged
	testCollector.RecordMessageAcknowledged()

	// No assertions needed - just verify no panics
}

func TestQueueMetrics(t *testing.T) {
	// Update queue metrics
	testCollector.UpdateQueueMetrics("test-queue", "/", 10, 5, 2)

	// Delete queue metrics
	testCollector.DeleteQueueMetrics("test-queue", "/")

	// No assertions needed - just verify no panics
}

func TestTransactionMetrics(t *testing.T) {
	testCollector.RecordTransactionStarted()
	testCollector.RecordTransactionCommitted()
	testCollector.RecordTransactionRolledback()

	// No assertions needed - just verify no panics
}
