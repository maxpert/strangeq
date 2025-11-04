package storage

import (
	"fmt"
	"sync"

	"github.com/maxpert/amqp-go/interfaces"
)

// InMemTransactionStore provides an in-memory implementation of TransactionStore
// This is used to track AMQP transactions temporarily, separate from message storage
type InMemTransactionStore struct {
	transactions map[string]*interfaces.Transaction
	mutex        sync.RWMutex
}

// NewInMemTransactionStore creates a new in-memory transaction store
func NewInMemTransactionStore() *InMemTransactionStore {
	return &InMemTransactionStore{
		transactions: make(map[string]*interfaces.Transaction),
	}
}

// BeginTransaction starts a new transaction
func (s *InMemTransactionStore) BeginTransaction(txID string) (*interfaces.Transaction, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.transactions[txID]; exists {
		return nil, fmt.Errorf("transaction %s already exists", txID)
	}

	tx := &interfaces.Transaction{
		ID:      txID,
		Actions: make([]*interfaces.TransactionAction, 0),
	}
	s.transactions[txID] = tx
	return tx, nil
}

// GetTransaction retrieves a transaction
func (s *InMemTransactionStore) GetTransaction(txID string) (*interfaces.Transaction, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	tx, exists := s.transactions[txID]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", txID)
	}
	return tx, nil
}

// AddAction adds an action to a transaction
func (s *InMemTransactionStore) AddAction(txID string, action *interfaces.TransactionAction) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tx, exists := s.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	tx.Actions = append(tx.Actions, action)
	return nil
}

// CommitTransaction commits all operations in a transaction
func (s *InMemTransactionStore) CommitTransaction(txID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	// Transaction is committed by the broker, we just clean up
	delete(s.transactions, txID)
	return nil
}

// RollbackTransaction rolls back all operations in a transaction
func (s *InMemTransactionStore) RollbackTransaction(txID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	// Transaction is rolled back by the broker, we just clean up
	delete(s.transactions, txID)
	return nil
}

// DeleteTransaction removes a transaction
func (s *InMemTransactionStore) DeleteTransaction(txID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.transactions, txID)
	return nil
}

// ListActiveTransactions returns all active transactions
func (s *InMemTransactionStore) ListActiveTransactions() ([]*interfaces.Transaction, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	transactions := make([]*interfaces.Transaction, 0, len(s.transactions))
	for _, tx := range s.transactions {
		transactions = append(transactions, tx)
	}
	return transactions, nil
}

// Close closes the transaction store
func (s *InMemTransactionStore) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.transactions = make(map[string]*interfaces.Transaction)
	return nil
}
