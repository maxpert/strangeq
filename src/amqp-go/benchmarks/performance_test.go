package benchmarks

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/storage"
)

// BenchmarkMemoryStorage tests performance with in-memory storage
func BenchmarkMemoryStorage(b *testing.B) {
	server := setupTestServer(b, "memory")
	defer cleanupTestServer(server)

	broker := server.Broker

	b.Run("ExchangeDeclaration", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			exchangeName := fmt.Sprintf("bench.exchange.%d", i)
			err := broker.DeclareExchange(exchangeName, "direct", false, false, false, nil)
			if err != nil {
				b.Fatalf("Failed to declare exchange: %v", err)
			}
		}
	})

	b.Run("QueueDeclaration", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			queueName := fmt.Sprintf("bench.queue.%d", i)
			_, err := broker.DeclareQueue(queueName, false, false, false, nil)
			if err != nil {
				b.Fatalf("Failed to declare queue: %v", err)
			}
		}
	})

	b.Run("MessagePublishing", func(b *testing.B) {
		// Setup
		err := broker.DeclareExchange("bench.pub.exchange", "direct", false, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare exchange: %v", err)
		}
		_, err = broker.DeclareQueue("bench.pub.queue", false, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare queue: %v", err)
		}
		err = broker.BindQueue("bench.pub.queue", "bench.pub.exchange", "bench.key", nil)
		if err != nil {
			b.Fatalf("Failed to bind queue: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			message := &protocol.Message{
				DeliveryTag:  uint64(i + 1),
				Exchange:     "bench.pub.exchange",
				RoutingKey:   "bench.key",
				Headers:      make(map[string]interface{}),
				Body:         []byte(fmt.Sprintf("Benchmark message %d", i)),
				DeliveryMode: 1, // Non-persistent for memory benchmark
				Timestamp:    uint64(time.Now().Unix()),
			}

			err := broker.PublishMessage("bench.pub.exchange", "bench.key", message)
			if err != nil {
				b.Fatalf("Failed to publish message: %v", err)
			}
		}
	})
}

// BenchmarkBadgerStorage tests performance with Badger persistent storage
func BenchmarkBadgerStorage(b *testing.B) {
	server := setupTestServer(b, "badger")
	defer cleanupTestServer(server)

	broker := server.Broker

	b.Run("DurableExchangeDeclaration", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			exchangeName := fmt.Sprintf("durable.exchange.%d", i)
			err := broker.DeclareExchange(exchangeName, "direct", true, false, false, nil)
			if err != nil {
				b.Fatalf("Failed to declare durable exchange: %v", err)
			}
		}
	})

	b.Run("DurableQueueDeclaration", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			queueName := fmt.Sprintf("durable.queue.%d", i)
			_, err := broker.DeclareQueue(queueName, true, false, false, nil)
			if err != nil {
				b.Fatalf("Failed to declare durable queue: %v", err)
			}
		}
	})

	b.Run("PersistentMessagePublishing", func(b *testing.B) {
		// Setup
		err := broker.DeclareExchange("persistent.exchange", "direct", true, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare exchange: %v", err)
		}
		_, err = broker.DeclareQueue("persistent.queue", true, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare queue: %v", err)
		}
		err = broker.BindQueue("persistent.queue", "persistent.exchange", "persistent.key", nil)
		if err != nil {
			b.Fatalf("Failed to bind queue: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			message := &protocol.Message{
				DeliveryTag:  uint64(i + 1),
				Exchange:     "persistent.exchange",
				RoutingKey:   "persistent.key",
				Headers:      make(map[string]interface{}),
				Body:         []byte(fmt.Sprintf("Persistent benchmark message %d", i)),
				DeliveryMode: 2, // Persistent
				Timestamp:    uint64(time.Now().Unix()),
			}

			err := broker.PublishMessage("persistent.exchange", "persistent.key", message)
			if err != nil {
				b.Fatalf("Failed to publish persistent message: %v", err)
			}
		}
	})
}

// BenchmarkConcurrentOperations tests performance under concurrent load
func BenchmarkConcurrentOperations(b *testing.B) {
	server := setupTestServer(b, "memory")
	defer cleanupTestServer(server)

	broker := server.Broker

	b.Run("ConcurrentPublishing", func(b *testing.B) {
		// Setup
		err := broker.DeclareExchange("concurrent.exchange", "direct", false, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare exchange: %v", err)
		}
		_, err = broker.DeclareQueue("concurrent.queue", false, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare queue: %v", err)
		}
		err = broker.BindQueue("concurrent.queue", "concurrent.exchange", "concurrent.key", nil)
		if err != nil {
			b.Fatalf("Failed to bind queue: %v", err)
		}

		numWorkers := 10
		messagesPerWorker := b.N / numWorkers

		b.ResetTimer()
		var wg sync.WaitGroup
		for worker := 0; worker < numWorkers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < messagesPerWorker; i++ {
					message := &protocol.Message{
						DeliveryTag:  uint64(workerID*messagesPerWorker + i + 1),
						Exchange:     "concurrent.exchange",
						RoutingKey:   "concurrent.key",
						Headers:      make(map[string]interface{}),
						Body:         []byte(fmt.Sprintf("Concurrent message %d from worker %d", i, workerID)),
						DeliveryMode: 1,
						Timestamp:    uint64(time.Now().Unix()),
					}

					err := broker.PublishMessage("concurrent.exchange", "concurrent.key", message)
					if err != nil {
						b.Errorf("Failed to publish message: %v", err)
					}
				}
			}(worker)
		}
		wg.Wait()
	})

	b.Run("ConcurrentConsumerRegistration", func(b *testing.B) {
		// Setup queue
		_, err := broker.DeclareQueue("consumer.bench.queue", false, false, false, nil)
		if err != nil {
			b.Fatalf("Failed to declare queue: %v", err)
		}

		b.ResetTimer()
		var wg sync.WaitGroup
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func(consumerID int) {
				defer wg.Done()

				// Create minimal channel and consumer
				channel := &protocol.Channel{
					ID:        uint16(consumerID % 65535),
					Consumers: make(map[string]*protocol.Consumer),
				}

				consumer := &protocol.Consumer{
					Tag:      fmt.Sprintf("bench.consumer.%d", consumerID),
					Channel:  channel,
					Queue:    "consumer.bench.queue",
					NoAck:    true,
					Messages: make(chan *protocol.Delivery, 100),
				}

				err := broker.RegisterConsumer("consumer.bench.queue", consumer.Tag, consumer)
				if err != nil {
					b.Errorf("Failed to register consumer: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

// BenchmarkMessageSizes tests performance with different message sizes
func BenchmarkMessageSizes(b *testing.B) {
	server := setupTestServer(b, "memory")
	defer cleanupTestServer(server)

	broker := server.Broker

	// Setup
	err := broker.DeclareExchange("size.exchange", "direct", false, false, false, nil)
	if err != nil {
		b.Fatalf("Failed to declare exchange: %v", err)
	}
	_, err = broker.DeclareQueue("size.queue", false, false, false, nil)
	if err != nil {
		b.Fatalf("Failed to declare queue: %v", err)
	}
	err = broker.BindQueue("size.queue", "size.exchange", "size.key", nil)
	if err != nil {
		b.Fatalf("Failed to bind queue: %v", err)
	}

	sizes := []int{100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024} // 100B, 1KB, 10KB, 100KB, 1MB

	for _, size := range sizes {
		b.Run(fmt.Sprintf("MessageSize%dB", size), func(b *testing.B) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				message := &protocol.Message{
					DeliveryTag:  uint64(i + 1),
					Exchange:     "size.exchange",
					RoutingKey:   "size.key",
					Headers:      make(map[string]interface{}),
					Body:         payload,
					DeliveryMode: 1,
					Timestamp:    uint64(time.Now().Unix()),
				}

				err := broker.PublishMessage("size.exchange", "size.key", message)
				if err != nil {
					b.Fatalf("Failed to publish message: %v", err)
				}
			}

			// Report throughput
			mbps := float64(size*b.N) / float64(1024*1024) / b.Elapsed().Seconds()
			b.ReportMetric(mbps, "MB/s")
		})
	}
}

// BenchmarkExchangeTypes tests performance of different exchange types
func BenchmarkExchangeTypes(b *testing.B) {
	server := setupTestServer(b, "memory")
	defer cleanupTestServer(server)

	broker := server.Broker

	exchangeTypes := []string{"direct", "fanout", "topic", "headers"}

	for _, exchangeType := range exchangeTypes {
		b.Run(fmt.Sprintf("ExchangeType_%s", exchangeType), func(b *testing.B) {
			// Setup
			exchangeName := fmt.Sprintf("%s.exchange", exchangeType)
			err := broker.DeclareExchange(exchangeName, exchangeType, false, false, false, nil)
			if err != nil {
				b.Fatalf("Failed to declare %s exchange: %v", exchangeType, err)
			}

			queueName := fmt.Sprintf("%s.queue", exchangeType)
			_, err = broker.DeclareQueue(queueName, false, false, false, nil)
			if err != nil {
				b.Fatalf("Failed to declare queue: %v", err)
			}

			var routingKey string
			var headers map[string]interface{}
			switch exchangeType {
			case "direct":
				routingKey = "direct.key"
			case "fanout":
				routingKey = "" // Fanout ignores routing key
			case "topic":
				routingKey = "topic.*.test"
			case "headers":
				routingKey = ""
				headers = map[string]interface{}{"x-match": "all", "type": "test"}
			}

			err = broker.BindQueue(queueName, exchangeName, routingKey, headers)
			if err != nil {
				b.Fatalf("Failed to bind queue: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				message := &protocol.Message{
					DeliveryTag:  uint64(i + 1),
					Exchange:     exchangeName,
					RoutingKey:   routingKey,
					Headers:      headers,
					Body:         []byte(fmt.Sprintf("Message %d for %s exchange", i, exchangeType)),
					DeliveryMode: 1,
					Timestamp:    uint64(time.Now().Unix()),
				}

				err := broker.PublishMessage(exchangeName, routingKey, message)
				if err != nil {
					b.Fatalf("Failed to publish to %s exchange: %v", exchangeType, err)
				}
			}
		})
	}
}

// BenchmarkStorageOperations tests storage-specific operations
func BenchmarkStorageOperations(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "amqp_bench_storage")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := config.DefaultConfig()
	cfg.Storage.Backend = "badger"
	cfg.Storage.Path = tempDir
	cfg.Storage.Persistent = true

	factory := storage.NewStorageFactory(cfg)
	storageImpl, err := factory.CreateStorage()
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer func() {
		if closer, ok := storageImpl.(*storage.CompositeStorage); ok {
			closer.Close()
		}
	}()

	b.Run("MessageStorage", func(b *testing.B) {
		queueName := "bench.storage.queue"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			message := &protocol.Message{
				DeliveryTag:  uint64(i + 1),
				Exchange:     "bench.exchange",
				RoutingKey:   "bench.key",
				Headers:      make(map[string]interface{}),
				Body:         []byte(fmt.Sprintf("Storage benchmark message %d", i)),
				DeliveryMode: 2,
				Timestamp:    uint64(time.Now().Unix()),
			}

			err := storageImpl.StoreMessage(queueName, message)
			if err != nil {
				b.Fatalf("Failed to store message: %v", err)
			}
		}
	})

	b.Run("PendingAckStorage", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pendingAck := &protocol.PendingAck{
				QueueName:       "bench.ack.queue",
				DeliveryTag:     uint64(i + 1),
				ConsumerTag:     fmt.Sprintf("consumer.%d", i%10),
				MessageID:       fmt.Sprintf("msg_%d", i),
				DeliveredAt:     time.Now(),
				RedeliveryCount: 0,
				Redelivered:     false,
			}

			err := storageImpl.StorePendingAck(pendingAck)
			if err != nil {
				b.Fatalf("Failed to store pending ack: %v", err)
			}
		}
	})

	b.Run("AtomicOperations", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := storageImpl.ExecuteAtomic(func(txnStorage interfaces.Storage) error {
				// Simulate atomic acknowledgment: delete pending ack + delete message
				queueName := "atomic.bench.queue"
				deliveryTag := uint64(i + 1)

				// Delete pending ack
				err := txnStorage.DeletePendingAck(queueName, deliveryTag)
				if err != nil {
					return err
				}

				// Delete message
				return txnStorage.DeleteMessage(queueName, deliveryTag)
			})
			if err != nil {
				b.Fatalf("Atomic operation failed: %v", err)
			}
		}
	})
}

// Helper functions for benchmark setup

func setupTestServer(b *testing.B, backend string) *server.Server {
	var cfg *config.AMQPConfig

	if backend == "badger" {
		tempDir, err := os.MkdirTemp("", "amqp_benchmark")
		if err != nil {
			b.Fatalf("Failed to create temp dir: %v", err)
		}

		cfg = config.DefaultConfig()
		cfg.Storage.Backend = "badger"
		cfg.Storage.Path = tempDir
		cfg.Storage.Persistent = true
		cfg.Server.LogLevel = "error" // Reduce log noise during benchmarks
	} else {
		cfg = config.DefaultConfig()
		cfg.Storage.Backend = "memory"
		cfg.Storage.Persistent = false
		cfg.Server.LogLevel = "error"
	}

	serverBuilder := server.NewServerBuilderWithConfig(cfg)
	amqpServer, err := serverBuilder.
		WithZapLogger("error").
		Build()
	if err != nil {
		b.Fatalf("Failed to build server: %v", err)
	}

	return amqpServer
}

func cleanupTestServer(server *server.Server) {
	// In a real implementation, you'd call server.Close()
	// For now, just allow garbage collection
}
