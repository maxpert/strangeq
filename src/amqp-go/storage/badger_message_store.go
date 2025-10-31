package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// BadgerMessageStore implements MessageStore interface using Badger database
type BadgerMessageStore struct {
	db          *badger.DB
	ttl         time.Duration // Default TTL for messages
	asyncWriter *AsyncWriteBuffer // Async write buffer for batching
}

// NewBadgerMessageStore creates a new BadgerMessageStore instance
func NewBadgerMessageStore(dbPath string, ttl time.Duration) (*BadgerMessageStore, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger's default logging to avoid noise

	// Performance tuning for RabbitMQ-style workloads
	opts.NumVersionsToKeep = 1           // We don't need MVCC history
	opts.NumLevelZeroTables = 2          // Reduce memory for L0 tables
	opts.NumLevelZeroTablesStall = 3     // Allow more L0 before stalling
	opts.ValueThreshold = 1024           // Store values > 1KB in value log
	opts.NumCompactors = 2               // Parallel compaction
	opts.CompactL0OnClose = true         // Compact on shutdown
	opts.BlockCacheSize = 64 * 1024 * 1024 // 64MB block cache

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger database: %w", err)
	}

	store := &BadgerMessageStore{
		db:  db,
		ttl: ttl,
	}

	// Initialize async write buffer
	store.asyncWriter = NewAsyncWriteBuffer(db, 100*time.Millisecond, 1000, ttl)

	// Start background cleanup goroutine
	go store.runTTLCleanup()

	return store, nil
}

// StoreMessage stores a message in the database with TTL
// Uses synchronous write by default for backward compatibility
// For async batched writes, enable via configuration
func (b *BadgerMessageStore) StoreMessage(queueName string, message *protocol.Message) error {
	key := b.messageKey(queueName, message.DeliveryTag)

	// Serialize message
	data, err := b.serializeMessage(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Store with TTL (synchronous for now - backward compatible)
	err = b.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, data).WithTTL(b.ttl)
		return txn.SetEntry(entry)
	})

	if err != nil {
		return fmt.Errorf("failed to store message: %w", err)
	}

	return nil
}

// StoreMessageAsync stores a message using async write buffer (non-blocking)
// This is much faster but writes are eventually consistent
func (b *BadgerMessageStore) StoreMessageAsync(queueName string, message *protocol.Message) error {
	if b.asyncWriter != nil {
		b.asyncWriter.Write(queueName, message)
		return nil
	}
	// Fall back to sync write if async not available
	return b.StoreMessage(queueName, message)
}

// StoreMessageSync stores a message synchronously (bypasses async buffer)
// Use this when you need guaranteed durability (e.g., publisher confirms)
func (b *BadgerMessageStore) StoreMessageSync(queueName string, message *protocol.Message) error {
	key := b.messageKey(queueName, message.DeliveryTag)

	data, err := b.serializeMessage(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, data).WithTTL(b.ttl)
		return txn.SetEntry(entry)
	})

	if err != nil {
		return fmt.Errorf("failed to store message: %w", err)
	}

	return nil
}

// GetMessage retrieves a message from the database
func (b *BadgerMessageStore) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	key := b.messageKey(queueName, deliveryTag)

	var message *protocol.Message
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return interfaces.ErrMessageNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			var err error
			message, err = b.deserializeMessage(val)
			return err
		})
	})

	if err != nil {
		return nil, err
	}

	return message, nil
}

// DeleteMessage removes a message from the database
func (b *BadgerMessageStore) DeleteMessage(queueName string, deliveryTag uint64) error {
	key := b.messageKey(queueName, deliveryTag)

	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err == badger.ErrKeyNotFound {
		return interfaces.ErrMessageNotFound
	}

	return err
}

// GetQueueMessages retrieves all messages for a specific queue
func (b *BadgerMessageStore) GetQueueMessages(queueName string) ([]*protocol.Message, error) {
	prefix := []byte(queueName + ":")
	var messages []*protocol.Message

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100 // Optimize for batch reading

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				message, err := b.deserializeMessage(val)
				if err != nil {
					return err
				}
				messages = append(messages, message)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return messages, err
}

// GetQueueMessageCount returns the number of messages in a queue
func (b *BadgerMessageStore) GetQueueMessageCount(queueName string) (int, error) {
	prefix := []byte(queueName + ":")
	count := 0

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Only need keys for counting

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

// PurgeQueue removes all messages from a specific queue
func (b *BadgerMessageStore) PurgeQueue(queueName string) (int, error) {
	prefix := []byte(queueName + ":")
	count := 0

	err := b.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)
		}

		// Delete collected keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
			count++
		}

		return nil
	})

	return count, err
}

// Close closes the database connection
func (b *BadgerMessageStore) Close() error {
	// Flush any pending writes
	if b.asyncWriter != nil {
		if err := b.asyncWriter.Close(); err != nil {
			log.Printf("Error closing async writer: %v", err)
		}
	}
	return b.db.Close()
}

// GetMessageBatch retrieves multiple messages in a single transaction
// This is much faster than individual GetMessage calls for sequential reads
func (b *BadgerMessageStore) GetMessageBatch(queueName string, deliveryTags []uint64) ([]*protocol.Message, error) {
	messages := make([]*protocol.Message, len(deliveryTags))

	err := b.db.View(func(txn *badger.Txn) error {
		for i, tag := range deliveryTags {
			key := b.messageKey(queueName, tag)
			item, err := txn.Get(key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					messages[i] = nil // Mark as not found
					continue
				}
				return err
			}

			err = item.Value(func(val []byte) error {
				msg, err := b.deserializeMessage(val)
				if err != nil {
					return err
				}
				messages[i] = msg
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return messages, err
}

// Flush forces a flush of the async write buffer
func (b *BadgerMessageStore) Flush() error {
	if b.asyncWriter != nil {
		return b.asyncWriter.Flush()
	}
	return nil
}

// GetStats returns storage statistics
func (b *BadgerMessageStore) GetStats() (*interfaces.StorageStats, error) {
	lsm, vlog := b.db.Size()

	return &interfaces.StorageStats{
		TotalMessages: 0, // Would need to iterate to count - expensive
		TotalSize:     lsm + vlog,
		LSMSize:       lsm,
		ValueLogSize:  vlog,
		PendingWrites: 0, // Badger doesn't expose this easily
	}, nil
}

// Private helper methods

// messageKey generates a key for storing messages: "queue_name:delivery_tag"
func (b *BadgerMessageStore) messageKey(queueName string, deliveryTag uint64) []byte {
	key := make([]byte, len(queueName)+1+8) // queue + ":" + 8 bytes for uint64
	copy(key, queueName)
	key[len(queueName)] = ':'
	binary.BigEndian.PutUint64(key[len(queueName)+1:], deliveryTag)
	return key
}

// serializeMessage converts a protocol.Message to bytes
func (b *BadgerMessageStore) serializeMessage(message *protocol.Message) ([]byte, error) {
	// Simple binary serialization
	// Format: [delivery_tag:8][routing_key_len:4][routing_key][exchange_len:4][exchange][headers_len:4][headers][body_len:4][body]

	routingKeyBytes := []byte(message.RoutingKey)
	exchangeBytes := []byte(message.Exchange)
	var headersBytes []byte
	if message.Headers != nil {
		// For simplicity, we'll serialize headers as a simple key=value format
		for key, value := range message.Headers {
			headersBytes = append(headersBytes, []byte(fmt.Sprintf("%s=%v\n", key, value))...)
		}
	}

	totalLen := 8 + 4 + len(routingKeyBytes) + 4 + len(exchangeBytes) + 4 + len(headersBytes) + 4 + len(message.Body)
	buf := make([]byte, totalLen)

	offset := 0

	// Delivery tag
	binary.BigEndian.PutUint64(buf[offset:], message.DeliveryTag)
	offset += 8

	// Routing key
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(routingKeyBytes)))
	offset += 4
	copy(buf[offset:], routingKeyBytes)
	offset += len(routingKeyBytes)

	// Exchange
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(exchangeBytes)))
	offset += 4
	copy(buf[offset:], exchangeBytes)
	offset += len(exchangeBytes)

	// Headers
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(headersBytes)))
	offset += 4
	copy(buf[offset:], headersBytes)
	offset += len(headersBytes)

	// Body
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(message.Body)))
	offset += 4
	copy(buf[offset:], message.Body)

	return buf, nil
}

// deserializeMessage converts bytes back to a protocol.Message
func (b *BadgerMessageStore) deserializeMessage(data []byte) (*protocol.Message, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid message data: too short")
	}

	message := &protocol.Message{}
	offset := 0

	// Delivery tag
	message.DeliveryTag = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Routing key
	if offset+4 > len(data) {
		return nil, fmt.Errorf("invalid message data: routing key length")
	}
	routingKeyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(routingKeyLen) > len(data) {
		return nil, fmt.Errorf("invalid message data: routing key")
	}
	message.RoutingKey = string(data[offset : offset+int(routingKeyLen)])
	offset += int(routingKeyLen)

	// Exchange
	if offset+4 > len(data) {
		return nil, fmt.Errorf("invalid message data: exchange length")
	}
	exchangeLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(exchangeLen) > len(data) {
		return nil, fmt.Errorf("invalid message data: exchange")
	}
	message.Exchange = string(data[offset : offset+int(exchangeLen)])
	offset += int(exchangeLen)

	// Headers
	if offset+4 > len(data) {
		return nil, fmt.Errorf("invalid message data: headers length")
	}
	headersLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(headersLen) > len(data) {
		return nil, fmt.Errorf("invalid message data: headers")
	}
	// Skip headers parsing for now - would need proper serialization
	offset += int(headersLen)

	// Body
	if offset+4 > len(data) {
		return nil, fmt.Errorf("invalid message data: body length")
	}
	bodyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(bodyLen) > len(data) {
		return nil, fmt.Errorf("invalid message data: body")
	}
	message.Body = make([]byte, bodyLen)
	copy(message.Body, data[offset:offset+int(bodyLen)])

	return message, nil
}

// runTTLCleanup runs periodic cleanup of expired messages
func (b *BadgerMessageStore) runTTLCleanup() {
	ticker := time.NewTicker(5 * time.Minute) // Run cleanup every 5 minutes
	defer ticker.Stop()

	for range ticker.C {
		err := b.db.RunValueLogGC(0.5) // Run garbage collection
		if err != nil && err != badger.ErrNoRewrite {
			log.Printf("Error during value log GC: %v", err)
		}
	}
}
