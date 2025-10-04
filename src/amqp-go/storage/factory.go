package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
)

// StorageBackend represents the supported storage backends
type StorageBackend string

const (
	BackendMemory StorageBackend = "memory"
	BackendBadger StorageBackend = "badger"
)

// StorageFactory creates storage implementations based on configuration
type StorageFactory struct {
	config *config.AMQPConfig
}

// NewStorageFactory creates a new storage factory
func NewStorageFactory(config *config.AMQPConfig) *StorageFactory {
	return &StorageFactory{
		config: config,
	}
}

// CreateMessageStore creates a MessageStore implementation based on configuration
func (f *StorageFactory) CreateMessageStore() (interfaces.MessageStore, error) {
	backend := StorageBackend(f.config.Storage.Backend)
	
	switch backend {
	case BackendMemory:
		return NewMemoryMessageStore(), nil
		
	case BackendBadger:
		path := f.config.Storage.Path
		if path == "" {
			path = "./data"
		}
		
		// Use separate subdirectory for messages
		messagePath := path + "/messages"
		
		ttl := time.Duration(f.config.Storage.MessageTTL) * time.Second
		if ttl == 0 {
			ttl = 24 * time.Hour // Default 24 hour TTL
		}
		
		return NewBadgerMessageStore(messagePath, ttl)
		
	default:
		return nil, fmt.Errorf("unsupported message store backend: %s", backend)
	}
}

// CreateMetadataStore creates a MetadataStore implementation based on configuration
func (f *StorageFactory) CreateMetadataStore() (interfaces.MetadataStore, error) {
	backend := StorageBackend(f.config.Storage.Backend)
	
	switch backend {
	case BackendMemory:
		return NewMemoryMetadataStore(), nil
		
	case BackendBadger:
		path := f.config.Storage.Path
		if path == "" {
			path = "./data"
		}
		
		// Use separate subdirectory for metadata
		metadataPath := path + "/metadata"
		
		return NewBadgerMetadataStore(metadataPath)
		
	default:
		return nil, fmt.Errorf("unsupported metadata store backend: %s", backend)
	}
}

// CreateTransactionStore creates a TransactionStore implementation based on configuration
func (f *StorageFactory) CreateTransactionStore() (interfaces.TransactionStore, error) {
	backend := StorageBackend(f.config.Storage.Backend)
	
	switch backend {
	case BackendMemory:
		return NewMemoryTransactionStore(), nil
		
	case BackendBadger:
		// For now, use memory-based transactions even with Badger
		// Full transaction persistence can be implemented later
		return NewMemoryTransactionStore(), nil
		
	default:
		return nil, fmt.Errorf("unsupported transaction store backend: %s", backend)
	}
}

// CreateAcknowledgmentStore creates an acknowledgment store based on configuration
func (f *StorageFactory) CreateAcknowledgmentStore() (interfaces.AcknowledgmentStore, error) {
	backend := f.config.Storage.Backend
	
	switch backend {
	case "memory":
		return NewMemoryAckStore(), nil
		
	case "badger":
		// Create acknowledgment-specific path
		ackPath := filepath.Join(f.config.Storage.Path, "ack")
		if err := os.MkdirAll(ackPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create ack storage directory: %w", err)
		}
		
		opts := badger.DefaultOptions(ackPath)
		opts.Logger = nil // Disable badger's default logging
		
		db, err := badger.Open(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open badger ack store: %w", err)
		}
		
		return NewBadgerAckStore(db), nil
		
	default:
		return nil, fmt.Errorf("unsupported acknowledgment store backend: %s", backend)
	}
}

// CreateDurabilityStore creates a durability store based on configuration
func (f *StorageFactory) CreateDurabilityStore() (interfaces.DurabilityStore, error) {
	backend := f.config.Storage.Backend
	
	switch backend {
	case "memory":
		return NewMemoryDurabilityStore(), nil
		
	case "badger":
		// Create durability-specific path
		durabilityPath := filepath.Join(f.config.Storage.Path, "durability")
		if err := os.MkdirAll(durabilityPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create durability storage directory: %w", err)
		}
		
		opts := badger.DefaultOptions(durabilityPath)
		opts.Logger = nil // Disable badger's default logging
		
		db, err := badger.Open(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open badger durability store: %w", err)
		}
		
		return NewBadgerDurabilityStore(db), nil
		
	default:
		return nil, fmt.Errorf("unsupported durability store backend: %s", backend)
	}
}

// CreateStorage creates all storage components and returns them as a single interface
func (f *StorageFactory) CreateStorage() (interfaces.Storage, error) {
	messageStore, err := f.CreateMessageStore()
	if err != nil {
		return nil, fmt.Errorf("failed to create message store: %w", err)
	}
	
	metadataStore, err := f.CreateMetadataStore()
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}
	
	transactionStore, err := f.CreateTransactionStore()
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction store: %w", err)
	}
	
	ackStore, err := f.CreateAcknowledgmentStore()
	if err != nil {
		return nil, fmt.Errorf("failed to create acknowledgment store: %w", err)
	}
	
	durabilityStore, err := f.CreateDurabilityStore()
	if err != nil {
		return nil, fmt.Errorf("failed to create durability store: %w", err)
	}
	
	composite := &CompositeStorage{
		MessageStore:        messageStore,
		MetadataStore:       metadataStore,
		TransactionStore:    transactionStore,
		AcknowledgmentStore: ackStore,
		DurabilityStore:     durabilityStore,
		backend:            f.config.Storage.Backend,
	}
	
	// Create transaction wrapper for Badger backend
	if f.config.Storage.Backend == "badger" {
		// Type assert to get the Badger-specific implementations
		badgerMsgStore, msgOk := messageStore.(*BadgerMessageStore)
		badgerMetaStore, metaOk := metadataStore.(*BadgerMetadataStore)
		badgerAckStore, ackOk := ackStore.(*BadgerAckStore)
		badgerDurStore, durOk := durabilityStore.(*BadgerDurabilityStore)
		
		if msgOk && metaOk && ackOk && durOk {
			composite.transactionWrapper = NewBadgerTransactionWrapper(
				badgerMsgStore, badgerMetaStore, badgerAckStore, badgerDurStore)
		}
	}
	
	return composite, nil
}

// CompositeStorage combines all storage interfaces into a single implementation
type CompositeStorage struct {
	interfaces.MessageStore
	interfaces.MetadataStore
	interfaces.TransactionStore
	interfaces.AcknowledgmentStore
	interfaces.DurabilityStore
	backend string // Track backend type for atomic operations
	transactionWrapper *BadgerTransactionWrapper // For Badger atomic operations
}

// Close closes all storage components
func (c *CompositeStorage) Close() error {
	var errors []error
	
	// Close message store if it has a Close method
	if closer, ok := c.MessageStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("message store close error: %w", err))
		}
	}
	
	// Close metadata store if it has a Close method
	if closer, ok := c.MetadataStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("metadata store close error: %w", err))
		}
	}
	
	// Close transaction store if it has a Close method
	if closer, ok := c.TransactionStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("transaction store close error: %w", err))
		}
	}
	
	// Return the first error if any occurred
	if len(errors) > 0 {
		return errors[0]
	}
	
	return nil
}

// GetStorageStats returns combined statistics from all storage components
func (c *CompositeStorage) GetStorageStats() (*interfaces.StorageStats, error) {
	stats := &interfaces.StorageStats{}
	
	// Get message store stats if available
	if statsProvider, ok := c.MessageStore.(interface{ GetStats() (*interfaces.StorageStats, error) }); ok {
		msgStats, err := statsProvider.GetStats()
		if err != nil {
			return nil, fmt.Errorf("failed to get message store stats: %w", err)
		}
		
		stats.TotalMessages = msgStats.TotalMessages
		stats.TotalSize += msgStats.TotalSize
		stats.LSMSize += msgStats.LSMSize
		stats.ValueLogSize += msgStats.ValueLogSize
		stats.PendingWrites += msgStats.PendingWrites
	}
	
	// Get metadata store stats if available
	if statsProvider, ok := c.MetadataStore.(interface{ GetMetadataStats() (*interfaces.MetadataStats, error) }); ok {
		metaStats, err := statsProvider.GetMetadataStats()
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata store stats: %w", err)
		}
		
		// Add metadata stats to storage stats
		stats.ExchangeCount = metaStats.ExchangeCount
		stats.QueueCount = metaStats.QueueCount
		stats.BindingCount = metaStats.BindingCount
		stats.ConsumerCount = metaStats.ConsumerCount
	}
	
	return stats, nil
}

// ExecuteAtomic runs operations atomically based on the backend type
func (c *CompositeStorage) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	switch c.backend {
	case "badger":
		if c.transactionWrapper != nil {
			// Use the Badger transaction wrapper for coordinated transactions
			return c.transactionWrapper.ExecuteAtomic(operations)
		}
		// Fallback to sequential execution if transaction wrapper not available
		return operations(c)
		
	case "memory":
		// For memory backend, operations are already atomic within Go
		return operations(c)
		
	default:
		// Fallback to non-atomic execution
		return operations(c)
	}
}

// ValidateBackend checks if the specified storage backend is supported
func ValidateBackend(backend string) error {
	switch StorageBackend(backend) {
	case BackendMemory, BackendBadger:
		return nil
	default:
		return fmt.Errorf("unsupported storage backend: %s (supported: %s, %s)", 
			backend, BackendMemory, BackendBadger)
	}
}

// GetSupportedBackends returns a list of supported storage backends
func GetSupportedBackends() []string {
	return []string{string(BackendMemory), string(BackendBadger)}
}