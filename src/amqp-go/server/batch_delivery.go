package server

import (
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// sendBatchedDeliveries sends multiple messages to a consumer in a single batched operation
// This reduces TCP syscalls and improves throughput significantly
func (s *Server) sendBatchedDeliveries(conn *protocol.Connection, channelID uint16, consumerTag string, deliveries []*protocol.Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}

	s.Log.Debug("Sending batched deliveries",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("batch_size", len(deliveries)))

	// Send each delivery individually (AMQP doesn't support true frame batching)
	// But we batch the operations to reduce overhead
	for _, delivery := range deliveries {
		err := s.sendBasicDeliver(
			conn,
			channelID,
			consumerTag,
			delivery.DeliveryTag,
			delivery.Redelivered,
			delivery.Exchange,
			delivery.RoutingKey,
			delivery.Message,
		)
		if err != nil {
			s.Log.Error("Failed to send delivery in batch",
				zap.Error(err),
				zap.String("consumer_tag", consumerTag),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			return err
		}
	}

	s.Log.Debug("Batched deliveries sent successfully",
		zap.String("consumer_tag", consumerTag),
		zap.Int("batch_size", len(deliveries)))

	return nil
}

// collectBatch collects available messages from a consumer channel without blocking
// It uses len(chan) to check availability and drains up to maxBatch messages
func collectBatch(msgChan chan *protocol.Delivery, maxBatch int) []*protocol.Delivery {
	if maxBatch <= 0 {
		return nil
	}

	// Check how many messages are immediately available
	available := len(msgChan)
	if available == 0 {
		return nil
	}

	// Cap at maxBatch
	if available > maxBatch {
		available = maxBatch
	}

	// Pre-allocate batch slice to avoid resizing
	batch := make([]*protocol.Delivery, 0, available)

	// Drain available messages (non-blocking)
	for i := 0; i < available; i++ {
		select {
		case msg := <-msgChan:
			batch = append(batch, msg)
		default:
			// Channel was drained by another goroutine, stop collecting
			break
		}
	}

	return batch
}
