package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// BadgerMetadataStore implements MetadataStore interface using Badger database
type BadgerMetadataStore struct {
	db *badger.DB
}

// NewBadgerMetadataStore creates a new BadgerMetadataStore instance
func NewBadgerMetadataStore(dbPath string) (*BadgerMetadataStore, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger's default logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger database: %w", err)
	}

	return &BadgerMetadataStore{db: db}, nil
}

// Exchange operations

func (b *BadgerMetadataStore) StoreExchange(exchange *protocol.Exchange) error {
	key := b.exchangeKey(exchange.Name)
	data, err := json.Marshal(exchange)
	if err != nil {
		return fmt.Errorf("failed to marshal exchange: %w", err)
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *BadgerMetadataStore) GetExchange(name string) (*protocol.Exchange, error) {
	key := b.exchangeKey(name)
	var exchange *protocol.Exchange

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return interfaces.ErrExchangeNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			exchange = &protocol.Exchange{}
			return json.Unmarshal(val, exchange)
		})
	})

	return exchange, err
}

func (b *BadgerMetadataStore) DeleteExchange(name string) error {
	key := b.exchangeKey(name)

	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err == badger.ErrKeyNotFound {
		return interfaces.ErrExchangeNotFound
	}

	return err
}

func (b *BadgerMetadataStore) ListExchanges() ([]*protocol.Exchange, error) {
	prefix := []byte("exchange:")
	var exchanges []*protocol.Exchange

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				exchange := &protocol.Exchange{}
				if err := json.Unmarshal(val, exchange); err != nil {
					return err
				}
				exchanges = append(exchanges, exchange)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return exchanges, err
}

// Queue operations

func (b *BadgerMetadataStore) StoreQueue(queue *protocol.Queue) error {
	key := b.queueKey(queue.Name)
	data, err := json.Marshal(queue)
	if err != nil {
		return fmt.Errorf("failed to marshal queue: %w", err)
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *BadgerMetadataStore) GetQueue(name string) (*protocol.Queue, error) {
	key := b.queueKey(name)
	var queue *protocol.Queue

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return interfaces.ErrQueueNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			queue = &protocol.Queue{}
			return json.Unmarshal(val, queue)
		})
	})

	return queue, err
}

func (b *BadgerMetadataStore) DeleteQueue(name string) error {
	key := b.queueKey(name)

	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err == badger.ErrKeyNotFound {
		return interfaces.ErrQueueNotFound
	}

	return err
}

func (b *BadgerMetadataStore) ListQueues() ([]*protocol.Queue, error) {
	prefix := []byte("queue:")
	var queues []*protocol.Queue

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				queue := &protocol.Queue{}
				if err := json.Unmarshal(val, queue); err != nil {
					return err
				}
				queues = append(queues, queue)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return queues, err
}

// Binding operations

type QueueBinding struct {
	QueueName    string                 `json:"queue_name"`
	ExchangeName string                 `json:"exchange_name"`
	RoutingKey   string                 `json:"routing_key"`
	Arguments    map[string]interface{} `json:"arguments"`
}

func (b *BadgerMetadataStore) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	binding := &QueueBinding{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Arguments:    arguments,
	}

	key := b.bindingKey(queueName, exchangeName, routingKey)
	data, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("failed to marshal binding: %w", err)
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *BadgerMetadataStore) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	key := b.bindingKey(queueName, exchangeName, routingKey)
	var binding *QueueBinding

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return interfaces.ErrBindingNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			binding = &QueueBinding{}
			return json.Unmarshal(val, binding)
		})
	})

	if err != nil {
		return nil, err
	}

	return &interfaces.QueueBinding{
		QueueName:    binding.QueueName,
		ExchangeName: binding.ExchangeName,
		RoutingKey:   binding.RoutingKey,
		Arguments:    binding.Arguments,
	}, nil
}

func (b *BadgerMetadataStore) DeleteBinding(queueName, exchangeName, routingKey string) error {
	key := b.bindingKey(queueName, exchangeName, routingKey)

	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err == badger.ErrKeyNotFound {
		return interfaces.ErrBindingNotFound
	}

	return err
}

func (b *BadgerMetadataStore) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	prefix := []byte("binding:" + queueName + ":")
	var bindings []*interfaces.QueueBinding

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				binding := &QueueBinding{}
				if err := json.Unmarshal(val, binding); err != nil {
					return err
				}

				bindings = append(bindings, &interfaces.QueueBinding{
					QueueName:    binding.QueueName,
					ExchangeName: binding.ExchangeName,
					RoutingKey:   binding.RoutingKey,
					Arguments:    binding.Arguments,
				})
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return bindings, err
}

func (b *BadgerMetadataStore) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	// This requires scanning all bindings since we key by queue name first
	prefix := []byte("binding:")
	var bindings []*interfaces.QueueBinding

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				binding := &QueueBinding{}
				if err := json.Unmarshal(val, binding); err != nil {
					return err
				}

				// Filter by exchange name
				if binding.ExchangeName == exchangeName {
					bindings = append(bindings, &interfaces.QueueBinding{
						QueueName:    binding.QueueName,
						ExchangeName: binding.ExchangeName,
						RoutingKey:   binding.RoutingKey,
						Arguments:    binding.Arguments,
					})
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return bindings, err
}

// Consumer operations

// StorableConsumer represents a consumer that can be marshaled to JSON
type StorableConsumer struct {
	Tag       string                 `json:"tag"`
	Queue     string                 `json:"queue"`
	NoAck     bool                   `json:"no_ack"`
	Exclusive bool                   `json:"exclusive"`
	Args      map[string]interface{} `json:"args"`
}

func (b *BadgerMetadataStore) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	// Convert to storable format
	storable := &StorableConsumer{
		Tag:       consumer.Tag,
		Queue:     consumer.Queue,
		NoAck:     consumer.NoAck,
		Exclusive: consumer.Exclusive,
		Args:      consumer.Args,
	}

	key := b.consumerKey(queueName, consumerTag)
	data, err := json.Marshal(storable)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer: %w", err)
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *BadgerMetadataStore) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	key := b.consumerKey(queueName, consumerTag)
	var storable *StorableConsumer

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return interfaces.ErrConsumerNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			storable = &StorableConsumer{}
			return json.Unmarshal(val, storable)
		})
	})

	if err != nil {
		return nil, err
	}

	// Convert back to protocol.Consumer
	consumer := &protocol.Consumer{
		Tag:       storable.Tag,
		Queue:     storable.Queue,
		NoAck:     storable.NoAck,
		Exclusive: storable.Exclusive,
		Args:      storable.Args,
		// Note: Channels and other runtime fields are not restored
	}

	return consumer, nil
}

func (b *BadgerMetadataStore) DeleteConsumer(queueName, consumerTag string) error {
	key := b.consumerKey(queueName, consumerTag)

	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err == badger.ErrKeyNotFound {
		return interfaces.ErrConsumerNotFound
	}

	return err
}

func (b *BadgerMetadataStore) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	prefix := []byte("consumer:" + queueName + ":")
	var consumers []*protocol.Consumer

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				storable := &StorableConsumer{}
				if err := json.Unmarshal(val, storable); err != nil {
					return err
				}

				// Convert to protocol.Consumer
				consumer := &protocol.Consumer{
					Tag:       storable.Tag,
					Queue:     storable.Queue,
					NoAck:     storable.NoAck,
					Exclusive: storable.Exclusive,
					Args:      storable.Args,
				}

				consumers = append(consumers, consumer)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return consumers, err
}

// Close closes the database connection
func (b *BadgerMetadataStore) Close() error {
	return b.db.Close()
}

// Private helper methods for key generation

func (b *BadgerMetadataStore) exchangeKey(name string) []byte {
	return []byte("exchange:" + name)
}

func (b *BadgerMetadataStore) queueKey(name string) []byte {
	return []byte("queue:" + name)
}

func (b *BadgerMetadataStore) bindingKey(queueName, exchangeName, routingKey string) []byte {
	return []byte(fmt.Sprintf("binding:%s:%s:%s", queueName, exchangeName, routingKey))
}

func (b *BadgerMetadataStore) consumerKey(queueName, consumerTag string) []byte {
	return []byte(fmt.Sprintf("consumer:%s:%s", queueName, consumerTag))
}

// GetMetadataStats returns statistics about stored metadata
func (b *BadgerMetadataStore) GetMetadataStats() (*interfaces.MetadataStats, error) {
	stats := &interfaces.MetadataStats{}

	// Count exchanges
	exchanges, err := b.ListExchanges()
	if err != nil {
		return nil, err
	}
	stats.ExchangeCount = len(exchanges)

	// Count queues
	queues, err := b.ListQueues()
	if err != nil {
		return nil, err
	}
	stats.QueueCount = len(queues)

	// Count bindings - requires prefix scan
	bindingPrefix := []byte("binding:")
	err = b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(bindingPrefix); it.ValidForPrefix(bindingPrefix); it.Next() {
			keyStr := string(it.Item().Key())
			if strings.HasPrefix(keyStr, "binding:") && strings.Count(keyStr, ":") == 3 {
				stats.BindingCount++
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Count consumers
	consumerPrefix := []byte("consumer:")
	err = b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(consumerPrefix); it.ValidForPrefix(consumerPrefix); it.Next() {
			stats.ConsumerCount++
		}
		return nil
	})

	return stats, err
}
