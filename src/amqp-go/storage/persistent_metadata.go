package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	MetadataDir       = "metadata"
	ExchangesDir      = "exchanges"
	QueuesDir         = "queues"
	BindingsDir       = "bindings"
	ConsumersDir      = "consumers"
	FileExtension     = ".cbor"
	TempFileExtension = ".tmp"
)

// PersistentMetadataStore implements persistent metadata storage using CBOR binary format
// Phase 3: Simple, debuggable file-based metadata
// Phase 6D: In-memory cache for hot path optimization
// Phase 6F: Migrated from JSON to CBOR for 2-3x faster serialization
type PersistentMetadataStore struct {
	baseDir string
	mutex   sync.RWMutex

	// In-memory cache for hot path (Phase 6D)
	exchangeCache sync.Map // name -> *protocol.Exchange
	queueCache    sync.Map // name -> *protocol.Queue
	cacheEnabled  bool
}

// NewPersistentMetadataStore creates a new persistent metadata store
func NewPersistentMetadataStore(dataDir string) (*PersistentMetadataStore, error) {
	baseDir := filepath.Join(dataDir, MetadataDir)

	// Create directory structure
	dirs := []string{
		filepath.Join(baseDir, ExchangesDir),
		filepath.Join(baseDir, QueuesDir),
		filepath.Join(baseDir, BindingsDir),
		filepath.Join(baseDir, ConsumersDir),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create metadata directory %s: %w", dir, err)
		}
	}

	store := &PersistentMetadataStore{
		baseDir:      baseDir,
		cacheEnabled: true, // Enable cache - investigating consumer bottleneck at high load
	}

	// Pre-populate cache on startup (if enabled)
	if store.cacheEnabled {
		store.loadCacheFromDisk()
	}

	return store, nil
}

// loadCacheFromDisk pre-populates the cache on startup
func (pm *PersistentMetadataStore) loadCacheFromDisk() {
	// Load all exchanges into cache
	exchangesDir := filepath.Join(pm.baseDir, ExchangesDir)
	if entries, err := os.ReadDir(exchangesDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), FileExtension) {
				name := strings.TrimSuffix(entry.Name(), FileExtension)
				if exchange, err := pm.loadExchangeFromDisk(name); err == nil {
					pm.exchangeCache.Store(name, exchange)
				}
			}
		}
	}

	// Load all queues into cache
	queuesDir := filepath.Join(pm.baseDir, QueuesDir)
	if entries, err := os.ReadDir(queuesDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), FileExtension) {
				name := strings.TrimSuffix(entry.Name(), FileExtension)
				if queue, err := pm.loadQueueFromDisk(name); err == nil {
					pm.queueCache.Store(name, queue)
				}
			}
		}
	}
}

// atomicWrite writes data to a file atomically using temp file + rename
func (pm *PersistentMetadataStore) atomicWrite(path string, data []byte) error {
	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to temp file
	tempPath := path + TempFileExtension
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath) // Clean up on failure
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// StoreExchange persists an exchange to disk and updates cache
func (pm *PersistentMetadataStore) StoreExchange(exchange *protocol.Exchange) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	data, err := cbor.Marshal(exchange)
	if err != nil {
		return fmt.Errorf("failed to marshal exchange: %w", err)
	}

	path := filepath.Join(pm.baseDir, ExchangesDir, exchange.Name+FileExtension)
	if err := pm.atomicWrite(path, data); err != nil {
		return err
	}

	// Update cache (Phase 6D)
	if pm.cacheEnabled {
		pm.exchangeCache.Store(exchange.Name, exchange)
	}

	return nil
}

// GetExchange loads an exchange from cache or disk
func (pm *PersistentMetadataStore) GetExchange(name string) (*protocol.Exchange, error) {
	// Try cache first (Phase 6D: lock-free hot path!)
	if pm.cacheEnabled {
		if cached, ok := pm.exchangeCache.Load(name); ok {
			return cached.(*protocol.Exchange), nil
		}
	}

	// Cache miss - load from disk
	return pm.loadExchangeFromDisk(name)
}

// loadExchangeFromDisk loads an exchange from disk (cache miss path)
func (pm *PersistentMetadataStore) loadExchangeFromDisk(name string) (*protocol.Exchange, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	path := filepath.Join(pm.baseDir, ExchangesDir, name+FileExtension)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrExchangeNotFound
		}
		return nil, fmt.Errorf("failed to read exchange file: %w", err)
	}

	var exchange protocol.Exchange
	if err := cbor.Unmarshal(data, &exchange); err != nil {
		return nil, fmt.Errorf("failed to unmarshal exchange: %w", err)
	}

	// Update cache on load (Phase 6D)
	if pm.cacheEnabled {
		pm.exchangeCache.Store(name, &exchange)
	}

	return &exchange, nil
}

// DeleteExchange removes an exchange file and cache entry
func (pm *PersistentMetadataStore) DeleteExchange(name string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	path := filepath.Join(pm.baseDir, ExchangesDir, name+FileExtension)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete exchange file: %w", err)
	}

	// Remove from cache (Phase 6D)
	if pm.cacheEnabled {
		pm.exchangeCache.Delete(name)
	}

	return nil
}

// ListExchanges returns all exchanges
func (pm *PersistentMetadataStore) ListExchanges() ([]*protocol.Exchange, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dir := filepath.Join(pm.baseDir, ExchangesDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*protocol.Exchange{}, nil
		}
		return nil, fmt.Errorf("failed to read exchanges directory: %w", err)
	}

	exchanges := make([]*protocol.Exchange, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		name := strings.TrimSuffix(file.Name(), FileExtension)
		exchange, err := pm.GetExchange(name)
		if err != nil {
			continue // Skip corrupted files
		}
		exchanges = append(exchanges, exchange)
	}

	return exchanges, nil
}

// StoreQueue persists a queue to disk and updates cache
func (pm *PersistentMetadataStore) StoreQueue(queue *protocol.Queue) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	data, err := cbor.Marshal(queue)
	if err != nil {
		return fmt.Errorf("failed to marshal queue: %w", err)
	}

	path := filepath.Join(pm.baseDir, QueuesDir, queue.Name+FileExtension)
	if err := pm.atomicWrite(path, data); err != nil {
		return err
	}

	// Update cache (Phase 6D)
	if pm.cacheEnabled {
		pm.queueCache.Store(queue.Name, queue)
	}

	return nil
}

// GetQueue loads a queue from cache or disk
func (pm *PersistentMetadataStore) GetQueue(name string) (*protocol.Queue, error) {
	// Try cache first (Phase 6D: lock-free hot path!)
	if pm.cacheEnabled {
		if cached, ok := pm.queueCache.Load(name); ok {
			return cached.(*protocol.Queue), nil
		}
	}

	// Cache miss - load from disk
	return pm.loadQueueFromDisk(name)
}

// loadQueueFromDisk loads a queue from disk (cache miss path)
func (pm *PersistentMetadataStore) loadQueueFromDisk(name string) (*protocol.Queue, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	path := filepath.Join(pm.baseDir, QueuesDir, name+FileExtension)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrQueueNotFound
		}
		return nil, fmt.Errorf("failed to read queue file: %w", err)
	}

	var queue protocol.Queue
	if err := cbor.Unmarshal(data, &queue); err != nil {
		return nil, fmt.Errorf("failed to unmarshal queue: %w", err)
	}

	// Update cache on load (Phase 6D)
	if pm.cacheEnabled {
		pm.queueCache.Store(name, &queue)
	}

	return &queue, nil
}

// DeleteQueue removes a queue file and cache entry
func (pm *PersistentMetadataStore) DeleteQueue(name string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	path := filepath.Join(pm.baseDir, QueuesDir, name+FileExtension)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete queue file: %w", err)
	}

	// Remove from cache (Phase 6D)
	if pm.cacheEnabled {
		pm.queueCache.Delete(name)
	}

	return nil
}

// ListQueues returns all queues
func (pm *PersistentMetadataStore) ListQueues() ([]*protocol.Queue, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dir := filepath.Join(pm.baseDir, QueuesDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*protocol.Queue{}, nil
		}
		return nil, fmt.Errorf("failed to read queues directory: %w", err)
	}

	queues := make([]*protocol.Queue, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		name := strings.TrimSuffix(file.Name(), FileExtension)
		queue, err := pm.GetQueue(name)
		if err != nil {
			continue // Skip corrupted files
		}
		queues = append(queues, queue)
	}

	return queues, nil
}

// makeBindingFilename creates a unique filename for a binding
func makeBindingFilename(queueName, exchangeName, routingKey string) string {
	// Replace special characters to make it filesystem-safe
	safe := func(s string) string {
		s = strings.ReplaceAll(s, "/", "_")
		s = strings.ReplaceAll(s, "\\", "_")
		s = strings.ReplaceAll(s, ":", "_")
		s = strings.ReplaceAll(s, "*", "_")
		s = strings.ReplaceAll(s, "?", "_")
		s = strings.ReplaceAll(s, "<", "_")
		s = strings.ReplaceAll(s, ">", "_")
		s = strings.ReplaceAll(s, "|", "_")
		return s
	}

	return fmt.Sprintf("%s_%s_%s%s",
		safe(queueName),
		safe(exchangeName),
		safe(routingKey),
		FileExtension)
}

// StoreBinding persists a binding to disk
func (pm *PersistentMetadataStore) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	binding := &interfaces.QueueBinding{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Arguments:    arguments,
	}

	data, err := cbor.Marshal(binding)
	if err != nil {
		return fmt.Errorf("failed to marshal binding: %w", err)
	}

	filename := makeBindingFilename(queueName, exchangeName, routingKey)
	path := filepath.Join(pm.baseDir, BindingsDir, filename)
	return pm.atomicWrite(path, data)
}

// GetBinding loads a binding from disk
func (pm *PersistentMetadataStore) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	filename := makeBindingFilename(queueName, exchangeName, routingKey)
	path := filepath.Join(pm.baseDir, BindingsDir, filename)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrBindingNotFound
		}
		return nil, fmt.Errorf("failed to read binding file: %w", err)
	}

	var binding interfaces.QueueBinding
	if err := cbor.Unmarshal(data, &binding); err != nil {
		return nil, fmt.Errorf("failed to unmarshal binding: %w", err)
	}

	return &binding, nil
}

// DeleteBinding removes a binding file
func (pm *PersistentMetadataStore) DeleteBinding(queueName, exchangeName, routingKey string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	filename := makeBindingFilename(queueName, exchangeName, routingKey)
	path := filepath.Join(pm.baseDir, BindingsDir, filename)

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete binding file: %w", err)
	}

	return nil
}

// ListBindings returns all bindings
func (pm *PersistentMetadataStore) ListBindings() ([]*interfaces.QueueBinding, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dir := filepath.Join(pm.baseDir, BindingsDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*interfaces.QueueBinding{}, nil
		}
		return nil, fmt.Errorf("failed to read bindings directory: %w", err)
	}

	bindings := make([]*interfaces.QueueBinding, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		path := filepath.Join(dir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip unreadable files
		}

		var binding interfaces.QueueBinding
		if err := cbor.Unmarshal(data, &binding); err != nil {
			continue // Skip corrupted files
		}

		bindings = append(bindings, &binding)
	}

	return bindings, nil
}

// GetQueueBindings returns all bindings for a specific queue
func (pm *PersistentMetadataStore) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	allBindings, err := pm.ListBindings()
	if err != nil {
		return nil, err
	}

	result := make([]*interfaces.QueueBinding, 0)
	for _, binding := range allBindings {
		if binding.QueueName == queueName {
			result = append(result, binding)
		}
	}

	return result, nil
}

// GetExchangeBindings returns all bindings for a specific exchange
func (pm *PersistentMetadataStore) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	allBindings, err := pm.ListBindings()
	if err != nil {
		return nil, err
	}

	result := make([]*interfaces.QueueBinding, 0)
	for _, binding := range allBindings {
		if binding.ExchangeName == exchangeName {
			result = append(result, binding)
		}
	}

	return result, nil
}

// StoreConsumer is a no-op - consumers are runtime state and should not be persisted
// Consumers contain channel pointers, goroutines (Messages chan, Cancel chan), and other
// non-serializable state that causes circular references when attempting JSON marshalling.
// Consumer registration is ephemeral - they're recreated on each connection.
func (pm *PersistentMetadataStore) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	// No-op: Consumers are not persisted
	return nil
}

// GetConsumer loads a consumer from disk
func (pm *PersistentMetadataStore) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	path := filepath.Join(pm.baseDir, ConsumersDir, queueName, consumerTag+FileExtension)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrConsumerNotFound
		}
		return nil, fmt.Errorf("failed to read consumer file: %w", err)
	}

	var consumer protocol.Consumer
	if err := cbor.Unmarshal(data, &consumer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer: %w", err)
	}

	return &consumer, nil
}

// DeleteConsumer removes a consumer file
func (pm *PersistentMetadataStore) DeleteConsumer(queueName, consumerTag string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	path := filepath.Join(pm.baseDir, ConsumersDir, queueName, consumerTag+FileExtension)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete consumer file: %w", err)
	}

	return nil
}

// GetQueueConsumers returns all consumers for a queue
func (pm *PersistentMetadataStore) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	queueDir := filepath.Join(pm.baseDir, ConsumersDir, queueName)
	files, err := os.ReadDir(queueDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*protocol.Consumer{}, nil
		}
		return nil, fmt.Errorf("failed to read consumers directory: %w", err)
	}

	consumers := make([]*protocol.Consumer, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		consumerTag := strings.TrimSuffix(file.Name(), FileExtension)
		consumer, err := pm.GetConsumer(queueName, consumerTag)
		if err != nil {
			continue // Skip corrupted files
		}
		consumers = append(consumers, consumer)
	}

	return consumers, nil
}

// LoadAllMetadata loads all metadata from disk for recovery
func (pm *PersistentMetadataStore) LoadAllMetadata() (
	exchanges []*protocol.Exchange,
	queues []*protocol.Queue,
	bindings []*interfaces.QueueBinding,
	consumers map[string][]*protocol.Consumer,
	err error,
) {
	consumers = make(map[string][]*protocol.Consumer)

	// Load exchanges
	exchanges, err = pm.ListExchanges()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load exchanges: %w", err)
	}

	// Load queues
	queues, err = pm.ListQueues()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load queues: %w", err)
	}

	// Load bindings
	bindings, err = pm.ListBindings()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load bindings: %w", err)
	}

	// Load consumers for each queue
	for _, queue := range queues {
		queueConsumers, err := pm.GetQueueConsumers(queue.Name)
		if err != nil {
			continue // Skip errors for individual queues
		}
		if len(queueConsumers) > 0 {
			consumers[queue.Name] = queueConsumers
		}
	}

	return exchanges, queues, bindings, consumers, nil
}

// Close closes the metadata store (no-op for JSON files)
func (pm *PersistentMetadataStore) Close() error {
	return nil
}

// Helper to read a JSON file
func readJSONFile(path string, v interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	return cbor.Unmarshal(data, v)
}
