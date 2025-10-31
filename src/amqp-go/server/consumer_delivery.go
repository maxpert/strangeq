package server

import (
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

type consumerInfo struct {
	channel  *protocol.Channel
	consumer *protocol.Consumer
	msgChan  chan *protocol.Delivery
}

// consumerDeliveryLoop continuously reads from consumer channels and sends messages to clients
func (s *Server) consumerDeliveryLoop(conn *protocol.Connection) {
	s.Log.Info("Starting consumer delivery loop", zap.String("connection_id", conn.ID))

	// This loop reads from consumer channels and sends basic.deliver frames to clients

	// Create a map to track consumer info (cached channel/consumer references)
	consumerInfos := make(map[string]*consumerInfo)

	for {
		// Check if connection is closed
		conn.Mutex.RLock()
		closed := conn.Closed
		conn.Mutex.RUnlock()

		if closed {
			s.Log.Debug("Connection closed, stopping consumer delivery loop", zap.String("connection_id", conn.ID))
			break
		}

		// Refresh the list of consumers periodically
		// Cache channel and consumer references for fast lookup during delivery
		conn.Mutex.Lock()
		channelCount := len(conn.Channels)
		consumerCount := 0
		for _, channel := range conn.Channels {
			channel.Mutex.RLock()
			for _, consumer := range channel.Consumers {
				consumerCount++
				// Add consumer info to our tracking map if not already there
				if _, exists := consumerInfos[consumer.Tag]; !exists {
					consumerInfos[consumer.Tag] = &consumerInfo{
						channel:  channel,
						consumer: consumer,
						msgChan:  consumer.Messages,
					}
					s.Log.Info("Added consumer to delivery loop",
						zap.String("consumer_tag", consumer.Tag),
						zap.String("queue", consumer.Queue))
				}
			}
			channel.Mutex.RUnlock()
		}
		conn.Mutex.Unlock()

		s.Log.Debug("Consumer delivery loop iteration",
			zap.String("connection_id", conn.ID),
			zap.Int("channels", channelCount),
			zap.Int("consumers", consumerCount),
			zap.Int("tracked_consumers", len(consumerInfos)))

		// Check each consumer channel for messages
		// Use a short timeout to balance responsiveness and CPU usage
		timeout := time.NewTimer(1 * time.Millisecond)
		messageProcessed := false

		for consumerTag, info := range consumerInfos {
			select {
			case delivery := <-info.msgChan:
				messageProcessed = true
				// Got a message, send it to the client
				s.Log.Debug("Sending message to consumer",
					zap.String("consumer_tag", consumerTag),
					zap.Uint64("delivery_tag", delivery.DeliveryTag),
					zap.String("exchange", delivery.Exchange),
					zap.String("routing_key", delivery.RoutingKey))

				// Use cached channel and consumer references (no lookup needed!)
				s.Log.Debug("About to send basic.deliver",
					zap.String("consumer_tag", consumerTag),
					zap.Uint16("channel_id", info.channel.ID))

				// Send basic.deliver frame to the client
				err := s.sendBasicDeliver(
					conn,
					info.channel.ID,
					consumerTag,
					delivery.DeliveryTag,
					delivery.Redelivered,
					delivery.Exchange,
					delivery.RoutingKey,
					delivery.Message,
				)
				if err != nil {
					s.Log.Error("Failed to send basic.deliver",
						zap.Error(err),
						zap.String("consumer_tag", consumerTag))
				} else {
					s.Log.Debug("Sent basic.deliver successfully",
						zap.String("consumer_tag", consumerTag),
						zap.Uint64("delivery_tag", delivery.DeliveryTag))
				}
			default:
				// No message available on this channel, continue to next
				s.Log.Debug("No message available on consumer channel",
					zap.String("consumer_tag", consumerTag))
			}
		}

		timeout.Stop()

		// Only sleep if no messages were processed to prevent busy waiting
		if !messageProcessed {
			time.Sleep(1 * time.Millisecond)
		}
	}

	s.Log.Debug("Consumer delivery loop stopped", zap.String("connection_id", conn.ID))
}
