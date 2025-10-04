package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/amqp-go/protocol"
)

// BadgerDurabilityStore implements DurabilityStore using Badger
type BadgerDurabilityStore struct {
	db *badger.DB
}

// NewBadgerDurabilityStore creates a new Badger-based durability store
func NewBadgerDurabilityStore(db *badger.DB) *BadgerDurabilityStore {
	return &BadgerDurabilityStore{db: db}
}

// StoreDurableEntityMetadata stores metadata about durable entities
func (s *BadgerDurabilityStore) StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error {
	key := []byte("durable_metadata")
	metadata.LastUpdated = time.Now()
	
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal durable metadata: %w", err)
	}
	
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// GetDurableEntityMetadata retrieves durable entity metadata
func (s *BadgerDurabilityStore) GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error) {
	key := []byte("durable_metadata")
	
	var metadata protocol.DurableEntityMetadata
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				// Return empty metadata if none exists
				metadata = protocol.DurableEntityMetadata{
					Exchanges:   []protocol.Exchange{},
					Queues:      []protocol.Queue{},
					Bindings:    []protocol.Binding{},
					LastUpdated: time.Now(),
				}
				return nil
			}
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &metadata)
		})
	})
	
	if err != nil {
		return nil, err
	}
	
	return &metadata, nil
}

// ValidateStorageIntegrity checks storage for corruption and returns statistics
func (s *BadgerDurabilityStore) ValidateStorageIntegrity() (*protocol.RecoveryStats, error) {
	startTime := time.Now()
	stats := &protocol.RecoveryStats{
		ValidationErrors: []string{},
	}
	
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		// Validate different key types
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Validate based on key prefix
			switch {
			case strings.HasPrefix(key, "message:"):
				if err := s.validateMessageEntry(item, stats); err != nil {
					stats.ValidationErrors = append(stats.ValidationErrors, 
						fmt.Sprintf("Message validation error for key %s: %v", key, err))
				}
				
			case strings.HasPrefix(key, "exchange:"):
				if err := s.validateExchangeEntry(item, stats); err != nil {
					stats.ValidationErrors = append(stats.ValidationErrors, 
						fmt.Sprintf("Exchange validation error for key %s: %v", key, err))
				}
				
			case strings.HasPrefix(key, "queue:"):
				if err := s.validateQueueEntry(item, stats); err != nil {
					stats.ValidationErrors = append(stats.ValidationErrors, 
						fmt.Sprintf("Queue validation error for key %s: %v", key, err))
				}
				
			case strings.HasPrefix(key, "pending_ack:"):
				if err := s.validatePendingAckEntry(item, stats); err != nil {
					stats.ValidationErrors = append(stats.ValidationErrors, 
						fmt.Sprintf("Pending ack validation error for key %s: %v", key, err))
				}
			}
		}
		
		return nil
	})
	
	stats.RecoveryDuration = time.Since(startTime)
	
	if err != nil {
		return stats, fmt.Errorf("validation failed: %w", err)
	}
	
	return stats, nil
}

// RepairCorruption attempts to repair corrupted entries
func (s *BadgerDurabilityStore) RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error) {
	startTime := time.Now()
	stats := &protocol.RecoveryStats{
		ValidationErrors: []string{},
	}
	
	if !autoRepair {
		// Just validate without repairing
		return s.ValidateStorageIntegrity()
	}
	
	// Perform repair operations
	err := s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		var keysToDelete [][]byte
		
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			
			// Try to unmarshal and validate each entry
			corrupted := false
			item.Value(func(val []byte) error {
				// Attempt basic JSON validation
				var temp interface{}
				if err := json.Unmarshal(val, &temp); err != nil {
					corrupted = true
					stats.ValidationErrors = append(stats.ValidationErrors,
						fmt.Sprintf("Corrupted JSON in key %s: %v", key, err))
				}
				return nil
			})
			
			if corrupted {
				keyBytes := make([]byte, len(item.Key()))
				copy(keyBytes, item.Key())
				keysToDelete = append(keysToDelete, keyBytes)
				stats.CorruptedEntriesRepaired++
			}
		}
		
		// Delete corrupted keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return fmt.Errorf("failed to delete corrupted key: %w", err)
			}
		}
		
		return nil
	})
	
	stats.RecoveryDuration = time.Since(startTime)
	
	if err != nil {
		return stats, fmt.Errorf("repair failed: %w", err)
	}
	
	return stats, nil
}

// GetRecoverableMessages retrieves all persistent messages organized by queue
func (s *BadgerDurabilityStore) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	messages := make(map[string][]*protocol.Message)
	prefix := []byte("message:")
	
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var message protocol.Message
				if err := json.Unmarshal(val, &message); err != nil {
					return err
				}
				
				// Only recover persistent messages (DeliveryMode=2)
				if message.DeliveryMode == 2 {
					// Extract queue name from key: "message:queueName:deliveryTag"
					keyParts := strings.Split(string(item.Key()), ":")
					if len(keyParts) >= 2 {
						queueName := keyParts[1]
						messages[queueName] = append(messages[queueName], &message)
					}
				}
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

// MarkRecoveryComplete marks the recovery process as complete and stores stats
func (s *BadgerDurabilityStore) MarkRecoveryComplete(stats *protocol.RecoveryStats) error {
	key := []byte("recovery_stats")
	
	data, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal recovery stats: %w", err)
	}
	
	return s.db.Update(func(txn *badger.Txn) error {
		// Store with TTL of 7 days to avoid indefinite accumulation
		entry := badger.NewEntry(key, data).WithTTL(7 * 24 * time.Hour)
		return txn.SetEntry(entry)
	})
}

// Validation helper methods

func (s *BadgerDurabilityStore) validateMessageEntry(item *badger.Item, stats *protocol.RecoveryStats) error {
	return item.Value(func(val []byte) error {
		var message protocol.Message
		if err := json.Unmarshal(val, &message); err != nil {
			return err
		}
		
		// Validate message structure
		if message.DeliveryTag == 0 {
			return fmt.Errorf("invalid delivery tag")
		}
		
		stats.PersistentMessagesRecovered++
		return nil
	})
}

func (s *BadgerDurabilityStore) validateExchangeEntry(item *badger.Item, stats *protocol.RecoveryStats) error {
	return item.Value(func(val []byte) error {
		var exchange protocol.Exchange
		if err := json.Unmarshal(val, &exchange); err != nil {
			return err
		}
		
		// Validate exchange structure
		if exchange.Name == "" {
			return fmt.Errorf("exchange name cannot be empty")
		}
		
		if exchange.Durable {
			stats.DurableExchangesRecovered++
		}
		return nil
	})
}

func (s *BadgerDurabilityStore) validateQueueEntry(item *badger.Item, stats *protocol.RecoveryStats) error {
	return item.Value(func(val []byte) error {
		var queue protocol.Queue
		if err := json.Unmarshal(val, &queue); err != nil {
			return err
		}
		
		// Validate queue structure
		if queue.Name == "" {
			return fmt.Errorf("queue name cannot be empty")
		}
		
		if queue.Durable {
			stats.DurableQueuesRecovered++
		}
		return nil
	})
}

func (s *BadgerDurabilityStore) validatePendingAckEntry(item *badger.Item, stats *protocol.RecoveryStats) error {
	return item.Value(func(val []byte) error {
		var pendingAck protocol.PendingAck
		if err := json.Unmarshal(val, &pendingAck); err != nil {
			return err
		}
		
		// Validate pending ack structure
		if pendingAck.QueueName == "" || pendingAck.DeliveryTag == 0 {
			return fmt.Errorf("invalid pending acknowledgment")
		}
		
		stats.PendingAcksRecovered++
		return nil
	})
}