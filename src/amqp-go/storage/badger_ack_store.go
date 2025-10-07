package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// BadgerAckStore implements AcknowledgmentStore using Badger
type BadgerAckStore struct {
	db *badger.DB
}

// NewBadgerAckStore creates a new Badger-based acknowledgment store
func NewBadgerAckStore(db *badger.DB) *BadgerAckStore {
	return &BadgerAckStore{db: db}
}

// StorePendingAck stores a pending acknowledgment
func (s *BadgerAckStore) StorePendingAck(pendingAck *protocol.PendingAck) error {
	key := s.pendingAckKey(pendingAck.QueueName, pendingAck.DeliveryTag)

	data, err := json.Marshal(pendingAck)
	if err != nil {
		return fmt.Errorf("failed to marshal pending ack: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		// Set TTL of 24 hours for pending acks to prevent indefinite accumulation
		entry := badger.NewEntry(key, data).WithTTL(24 * time.Hour)
		return txn.SetEntry(entry)
	})
}

// GetPendingAck retrieves a pending acknowledgment by queue name and delivery tag
func (s *BadgerAckStore) GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error) {
	key := s.pendingAckKey(queueName, deliveryTag)

	var pendingAck protocol.PendingAck
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return interfaces.ErrPendingAckNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &pendingAck)
		})
	})

	if err != nil {
		return nil, err
	}

	return &pendingAck, nil
}

// DeletePendingAck removes a pending acknowledgment
func (s *BadgerAckStore) DeletePendingAck(queueName string, deliveryTag uint64) error {
	key := s.pendingAckKey(queueName, deliveryTag)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// GetQueuePendingAcks retrieves all pending acknowledgments for a queue
func (s *BadgerAckStore) GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error) {
	prefix := []byte("pending_ack:" + queueName + ":")
	var pendingAcks []*protocol.PendingAck

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var pendingAck protocol.PendingAck
				if err := json.Unmarshal(val, &pendingAck); err != nil {
					return err
				}
				pendingAcks = append(pendingAcks, &pendingAck)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return pendingAcks, err
}

// GetConsumerPendingAcks retrieves all pending acknowledgments for a consumer
func (s *BadgerAckStore) GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error) {
	prefix := []byte("pending_ack:")
	var pendingAcks []*protocol.PendingAck

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var pendingAck protocol.PendingAck
				if err := json.Unmarshal(val, &pendingAck); err != nil {
					return err
				}

				// Filter by consumer tag
				if pendingAck.ConsumerTag == consumerTag {
					pendingAcks = append(pendingAcks, &pendingAck)
				}
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return pendingAcks, err
}

// CleanupExpiredAcks removes acknowledgments older than maxAge
func (s *BadgerAckStore) CleanupExpiredAcks(maxAge time.Duration) error {
	cutoff := time.Now().Add(-maxAge)
	prefix := []byte("pending_ack:")

	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var keysToDelete [][]byte

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var pendingAck protocol.PendingAck
				if err := json.Unmarshal(val, &pendingAck); err != nil {
					return err
				}

				// Mark for deletion if expired
				if pendingAck.DeliveredAt.Before(cutoff) {
					key := make([]byte, len(item.Key()))
					copy(key, item.Key())
					keysToDelete = append(keysToDelete, key)
				}
				return nil
			})

			if err != nil {
				return err
			}
		}

		// Delete expired keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

// GetAllPendingAcks retrieves all pending acknowledgments
func (s *BadgerAckStore) GetAllPendingAcks() ([]*protocol.PendingAck, error) {
	prefix := []byte("pending_ack:")
	var pendingAcks []*protocol.PendingAck

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var pendingAck protocol.PendingAck
				if err := json.Unmarshal(val, &pendingAck); err != nil {
					return err
				}
				pendingAcks = append(pendingAcks, &pendingAck)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return pendingAcks, err
}

// Helper methods

// pendingAckKey creates a key for pending acknowledgments
func (s *BadgerAckStore) pendingAckKey(queueName string, deliveryTag uint64) []byte {
	return []byte(fmt.Sprintf("pending_ack:%s:%d", queueName, deliveryTag))
}
