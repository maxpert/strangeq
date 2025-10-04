package storage

import (
	"fmt"
	"time"

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
	
	return &CompositeStorage{
		MessageStore:     messageStore,
		MetadataStore:    metadataStore,
		TransactionStore: transactionStore,
	}, nil
}

// CompositeStorage combines all storage interfaces into a single implementation
type CompositeStorage struct {
	interfaces.MessageStore
	interfaces.MetadataStore
	interfaces.TransactionStore
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