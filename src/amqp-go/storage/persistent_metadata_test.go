package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

func TestPersistentMetadataExchanges(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "persistent_metadata_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create metadata store
	store, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Create test exchange
	exchange := &protocol.Exchange{
		Name:       "test.exchange",
		Kind:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  map[string]interface{}{"x-test": "value"},
	}

	// Store exchange
	if err := store.StoreExchange(exchange); err != nil {
		t.Fatalf("failed to store exchange: %v", err)
	}

	// Verify file exists
	exchangePath := filepath.Join(tmpDir, MetadataDir, ExchangesDir, "test.exchange.cbor")
	if _, err := os.Stat(exchangePath); err != nil {
		t.Errorf("exchange file not created: %v", err)
	}

	// Get exchange
	retrieved, err := store.GetExchange("test.exchange")
	if err != nil {
		t.Fatalf("failed to get exchange: %v", err)
	}

	if retrieved.Name != exchange.Name || retrieved.Kind != exchange.Kind {
		t.Errorf("exchange mismatch: got %+v, want %+v", retrieved, exchange)
	}

	// List exchanges
	exchanges, err := store.ListExchanges()
	if err != nil {
		t.Fatalf("failed to list exchanges: %v", err)
	}

	if len(exchanges) != 1 {
		t.Errorf("expected 1 exchange, got %d", len(exchanges))
	}

	// Delete exchange
	if err := store.DeleteExchange("test.exchange"); err != nil {
		t.Fatalf("failed to delete exchange: %v", err)
	}

	// Verify file deleted
	if _, err := os.Stat(exchangePath); !os.IsNotExist(err) {
		t.Error("exchange file not deleted")
	}
}

func TestPersistentMetadataQueues(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "persistent_metadata_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create metadata store
	store, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Create test queue
	queue := &protocol.Queue{
		Name:       "test.queue",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		Arguments:  map[string]interface{}{"x-max-length": float64(1000)},
	}

	// Store queue
	if err := store.StoreQueue(queue); err != nil {
		t.Fatalf("failed to store queue: %v", err)
	}

	// Get queue
	retrieved, err := store.GetQueue("test.queue")
	if err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}

	if retrieved.Name != queue.Name || retrieved.Durable != queue.Durable {
		t.Errorf("queue mismatch: got %+v, want %+v", retrieved, queue)
	}

	// List queues
	queues, err := store.ListQueues()
	if err != nil {
		t.Fatalf("failed to list queues: %v", err)
	}

	if len(queues) != 1 {
		t.Errorf("expected 1 queue, got %d", len(queues))
	}

	// Delete queue
	if err := store.DeleteQueue("test.queue"); err != nil {
		t.Fatalf("failed to delete queue: %v", err)
	}
}

func TestPersistentMetadataBindings(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "persistent_metadata_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create metadata store
	store, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Create test binding
	queueName := "test.queue"
	exchangeName := "test.exchange"
	routingKey := "test.key"
	arguments := map[string]interface{}{"x-test": "value"}

	// Store binding
	if err := store.StoreBinding(queueName, exchangeName, routingKey, arguments); err != nil {
		t.Fatalf("failed to store binding: %v", err)
	}

	// Get binding
	retrieved, err := store.GetBinding(queueName, exchangeName, routingKey)
	if err != nil {
		t.Fatalf("failed to get binding: %v", err)
	}

	if retrieved.QueueName != queueName || retrieved.ExchangeName != exchangeName {
		t.Errorf("binding mismatch: got %+v", retrieved)
	}

	// List queue bindings
	queueBindings, err := store.GetQueueBindings(queueName)
	if err != nil {
		t.Fatalf("failed to get queue bindings: %v", err)
	}

	if len(queueBindings) != 1 {
		t.Errorf("expected 1 binding, got %d", len(queueBindings))
	}

	// List exchange bindings
	exchangeBindings, err := store.GetExchangeBindings(exchangeName)
	if err != nil {
		t.Fatalf("failed to get exchange bindings: %v", err)
	}

	if len(exchangeBindings) != 1 {
		t.Errorf("expected 1 binding, got %d", len(exchangeBindings))
	}

	// Delete binding
	if err := store.DeleteBinding(queueName, exchangeName, routingKey); err != nil {
		t.Fatalf("failed to delete binding: %v", err)
	}
}

func TestPersistentMetadataAtomicWrite(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "persistent_metadata_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create metadata store
	store, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Store queue multiple times (simulates updates)
	queue := &protocol.Queue{
		Name:       "test.queue",
		Durable:    true,
		AutoDelete: false,
	}

	for i := 0; i < 10; i++ {
		queue.Arguments = map[string]interface{}{"version": float64(i)}
		if err := store.StoreQueue(queue); err != nil {
			t.Fatalf("failed to store queue iteration %d: %v", i, err)
		}
	}

	// Verify final version
	retrieved, err := store.GetQueue("test.queue")
	if err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}

	if version, ok := retrieved.Arguments["version"].(float64); !ok || version != 9 {
		t.Errorf("expected version 9, got %v", retrieved.Arguments["version"])
	}

	// Verify no temp files left behind
	files, err := filepath.Glob(filepath.Join(tmpDir, MetadataDir, QueuesDir, "*.tmp"))
	if err != nil {
		t.Fatalf("failed to glob temp files: %v", err)
	}

	if len(files) > 0 {
		t.Errorf("found %d temp files, expected 0", len(files))
	}
}

func TestPersistentMetadataPersistence(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "persistent_metadata_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create first store and write data
	store1, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create first store: %v", err)
	}

	exchange := &protocol.Exchange{Name: "test.exchange", Kind: "direct", Durable: true}
	queue := &protocol.Queue{Name: "test.queue", Durable: true}

	if err := store1.StoreExchange(exchange); err != nil {
		t.Fatalf("failed to store exchange: %v", err)
	}
	if err := store1.StoreQueue(queue); err != nil {
		t.Fatalf("failed to store queue: %v", err)
	}
	if err := store1.StoreBinding("test.queue", "test.exchange", "test.key", nil); err != nil {
		t.Fatalf("failed to store binding: %v", err)
	}

	store1.Close()

	// Create second store and read data (simulates restart)
	store2, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	// Load all metadata
	exchanges, queues, bindings, consumers, err := store2.LoadAllMetadata()
	if err != nil {
		t.Fatalf("failed to load metadata: %v", err)
	}

	if len(exchanges) != 1 || exchanges[0].Name != "test.exchange" {
		t.Errorf("expected 1 exchange named 'test.exchange', got %d exchanges", len(exchanges))
	}

	if len(queues) != 1 || queues[0].Name != "test.queue" {
		t.Errorf("expected 1 queue named 'test.queue', got %d queues", len(queues))
	}

	if len(bindings) != 1 {
		t.Errorf("expected 1 binding, got %d", len(bindings))
	}

	if len(consumers) != 0 {
		t.Errorf("expected 0 consumers, got %d", len(consumers))
	}
}

func TestPersistentMetadataSpecialCharacters(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "persistent_metadata_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create metadata store
	store, err := NewPersistentMetadataStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Test binding with special characters in routing key
	queueName := "test.queue"
	exchangeName := "test.exchange"
	routingKey := "test.*.key.#"

	if err := store.StoreBinding(queueName, exchangeName, routingKey, nil); err != nil {
		t.Fatalf("failed to store binding with special chars: %v", err)
	}

	// Verify it can be retrieved
	retrieved, err := store.GetBinding(queueName, exchangeName, routingKey)
	if err != nil {
		t.Fatalf("failed to get binding with special chars: %v", err)
	}

	if retrieved.RoutingKey != routingKey {
		t.Errorf("routing key mismatch: got %s, want %s", retrieved.RoutingKey, routingKey)
	}
}
