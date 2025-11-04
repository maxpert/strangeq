package server

import (
	"reflect"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// consumerInfo holds cached references for a consumer
type consumerInfo struct {
	channel  *protocol.Channel
	consumer *protocol.Consumer
	msgChan  chan *protocol.Delivery
}

// consumerDeliveryLoop continuously reads from consumer channels and sends messages to clients
// Uses proper synchronization to safely access connection/channel state, then uses
// lock-free channel operations for actual message delivery
func (s *Server) consumerDeliveryLoop(conn *protocol.Connection) {
	s.Log.Debug("Starting consumer delivery loop", zap.String("connection_id", conn.ID))

	// Create a map to track consumer info (cached channel/consumer references)
	// This map is only accessed by this goroutine - no locks needed
	consumerInfos := make(map[string]*consumerInfo)

	const (
		selectTimeout = 500 * time.Microsecond // Wait timeout when no messages available
		maxBatchSize  = 100                    // Maximum messages to batch per consumer
	)

	for {
		// Check if connection is closed (with proper locking)
		conn.Mutex.RLock()
		closed := conn.Closed
		conn.Mutex.RUnlock()

		if closed {
			s.Log.Debug("Connection closed, stopping consumer delivery loop", zap.String("connection_id", conn.ID))
			return
		}

		// Take a snapshot of channels while holding the lock
		// This prevents data races with concurrent channel creation/deletion
		conn.Mutex.RLock()
		channelSnapshot := make([]*protocol.Channel, 0, len(conn.Channels))
		for _, channel := range conn.Channels {
			channelSnapshot = append(channelSnapshot, channel)
		}
		conn.Mutex.RUnlock()

		// Now iterate through the snapshot (no locks needed on conn)
		for _, channel := range channelSnapshot {
			// Take a snapshot of consumers for this channel
			channel.Mutex.RLock()
			consumerSnapshot := make([]*protocol.Consumer, 0, len(channel.Consumers))
			for _, consumer := range channel.Consumers {
				consumerSnapshot = append(consumerSnapshot, consumer)
			}
			channel.Mutex.RUnlock()

			// Process the consumer snapshot
			for _, consumer := range consumerSnapshot {
				// Add consumer info to our tracking map if not already there
				if _, exists := consumerInfos[consumer.Tag]; !exists {
					consumerInfos[consumer.Tag] = &consumerInfo{
						channel:  channel,
						consumer: consumer,
						msgChan:  consumer.Messages,
					}
					s.Log.Debug("Added consumer to delivery loop",
						zap.String("consumer_tag", consumer.Tag),
						zap.String("queue", consumer.Queue))
				}
			}
		}

		// If we have no consumers, wait a bit and continue
		if len(consumerInfos) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// P1.1 FIX: Use reflect.Select() ONLY - no O(N) hot path iteration!
		// Wait on ALL consumer channels simultaneously using reflect.Select()
		timeout := time.NewTimer(selectTimeout)

		// Build select cases: timeout + all consumer channels
		numConsumers := len(consumerInfos)
		cases := make([]reflect.SelectCase, numConsumers+1)
		consumerTags := make([]string, numConsumers)

		// Case 0: timeout channel
		cases[0] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(timeout.C),
		}

		// Cases 1...N: consumer message channels
		i := 1
		for tag, info := range consumerInfos {
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(info.msgChan),
			}
			consumerTags[i-1] = tag
			i++
		}

		// Wait for ANY channel to be ready - O(1) wait instead of O(N) polling!
		chosen, value, ok := reflect.Select(cases)

		timeout.Stop()

		if chosen == 0 {
			// Timeout - no messages available, continue to next iteration
			continue
		}

		// Got a message from consumer at index (chosen - 1)
		consumerTag := consumerTags[chosen-1]
		info := consumerInfos[consumerTag]

		if !ok {
			// Channel closed - remove consumer from tracking
			s.Log.Debug("Consumer channel closed, removing from delivery loop",
				zap.String("consumer_tag", consumerTag))
			delete(consumerInfos, consumerTag)
			continue
		}

		// Extract the delivery
		delivery := value.Interface().(*protocol.Delivery)

		// Try to collect more messages from this consumer
		batch := []*protocol.Delivery{delivery}
		additionalBatch := collectBatch(info.msgChan, maxBatchSize-1)
		batch = append(batch, additionalBatch...)

		// Send the batch
		err := s.sendBatchedDeliveries(conn, info.channel.ID, consumerTag, batch)
		if err != nil {
			s.Log.Error("Failed to send batched deliveries",
				zap.Error(err),
				zap.String("consumer_tag", consumerTag),
				zap.Int("batch_size", len(batch)))
			// Connection likely broken, exit loop
			return
		}
	}
}
