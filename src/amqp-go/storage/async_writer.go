package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/maxpert/amqp-go/protocol"
)

// AsyncWriteBuffer batches writes for better performance
// This eliminates synchronous disk I/O on the publish path
type AsyncWriteBuffer struct {
	db           *badger.DB
	buffer       map[string]*protocol.Message // key -> message
	bufferMutex  sync.RWMutex
	flushChan    chan struct{}
	stopChan     chan struct{}
	wg           sync.WaitGroup

	// Configuration
	flushInterval time.Duration // How often to flush (default: 100ms)
	maxBatchSize  int           // Max messages before forced flush (default: 1000)
	ttl           time.Duration // Message TTL

	// Statistics
	totalWrites   uint64
	totalFlushes  uint64
	totalBatches  uint64
	lastFlushTime int64
}

// NewAsyncWriteBuffer creates a new async write buffer
func NewAsyncWriteBuffer(db *badger.DB, flushInterval time.Duration, maxBatchSize int, ttl time.Duration) *AsyncWriteBuffer {
	if flushInterval == 0 {
		flushInterval = 100 * time.Millisecond
	}
	if maxBatchSize == 0 {
		maxBatchSize = 1000
	}

	awb := &AsyncWriteBuffer{
		db:            db,
		buffer:        make(map[string]*protocol.Message, maxBatchSize),
		flushChan:     make(chan struct{}, 1),
		stopChan:      make(chan struct{}),
		flushInterval: flushInterval,
		maxBatchSize:  maxBatchSize,
		ttl:           ttl,
	}

	// Start background flush goroutine
	awb.wg.Add(1)
	go awb.flushLoop()

	return awb
}

// Write adds a message to the buffer (non-blocking)
func (awb *AsyncWriteBuffer) Write(queueName string, message *protocol.Message) {
	key := makeMessageKey(queueName, message.DeliveryTag)

	awb.bufferMutex.Lock()
	awb.buffer[key] = message
	bufferSize := len(awb.buffer)
	awb.bufferMutex.Unlock()

	atomic.AddUint64(&awb.totalWrites, 1)

	// Trigger flush if buffer is full
	if bufferSize >= awb.maxBatchSize {
		select {
		case awb.flushChan <- struct{}{}:
		default:
			// Flush already pending
		}
	}
}

// Flush immediately flushes all buffered writes
func (awb *AsyncWriteBuffer) Flush() error {
	awb.bufferMutex.Lock()
	if len(awb.buffer) == 0 {
		awb.bufferMutex.Unlock()
		return nil
	}

	// Take ownership of current buffer
	batch := awb.buffer
	awb.buffer = make(map[string]*protocol.Message, awb.maxBatchSize)
	awb.bufferMutex.Unlock()

	// Perform batch write
	err := awb.db.Update(func(txn *badger.Txn) error {
		for key, msg := range batch {
			data, err := serializeMessage(msg)
			if err != nil {
				return err
			}

			entry := badger.NewEntry([]byte(key), data)
			if awb.ttl > 0 {
				entry = entry.WithTTL(awb.ttl)
			}

			if err := txn.SetEntry(entry); err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		atomic.AddUint64(&awb.totalFlushes, 1)
		atomic.AddUint64(&awb.totalBatches, uint64(len(batch)))
		atomic.StoreInt64(&awb.lastFlushTime, time.Now().Unix())
	}

	return err
}

// flushLoop runs periodic flushes in the background
func (awb *AsyncWriteBuffer) flushLoop() {
	defer awb.wg.Done()

	ticker := time.NewTicker(awb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Periodic flush
			_ = awb.Flush()

		case <-awb.flushChan:
			// Forced flush (buffer full)
			_ = awb.Flush()

		case <-awb.stopChan:
			// Final flush before shutdown
			_ = awb.Flush()
			return
		}
	}
}

// Close stops the async writer and flushes remaining data
func (awb *AsyncWriteBuffer) Close() error {
	close(awb.stopChan)
	awb.wg.Wait()
	return nil
}

// Stats returns write buffer statistics
type AsyncWriteStats struct {
	TotalWrites   uint64
	TotalFlushes  uint64
	TotalBatches  uint64
	LastFlushTime int64
	BufferSize    int
	AvgBatchSize  float64
}

// GetStats returns current statistics
func (awb *AsyncWriteBuffer) GetStats() AsyncWriteStats {
	awb.bufferMutex.RLock()
	bufferSize := len(awb.buffer)
	awb.bufferMutex.RUnlock()

	totalFlushes := atomic.LoadUint64(&awb.totalFlushes)
	totalBatches := atomic.LoadUint64(&awb.totalBatches)
	avgBatchSize := 0.0
	if totalFlushes > 0 {
		avgBatchSize = float64(totalBatches) / float64(totalFlushes)
	}

	return AsyncWriteStats{
		TotalWrites:   atomic.LoadUint64(&awb.totalWrites),
		TotalFlushes:  totalFlushes,
		TotalBatches:  totalBatches,
		LastFlushTime: atomic.LoadInt64(&awb.lastFlushTime),
		BufferSize:    bufferSize,
		AvgBatchSize:  avgBatchSize,
	}
}

// Helper functions

func makeMessageKey(queueName string, deliveryTag uint64) string {
	return queueName + ":" + string(rune(deliveryTag))
}

func serializeMessage(message *protocol.Message) ([]byte, error) {
	// Same serialization logic as BadgerMessageStore
	routingKeyBytes := []byte(message.RoutingKey)
	exchangeBytes := []byte(message.Exchange)
	var headersBytes []byte
	if message.Headers != nil {
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
