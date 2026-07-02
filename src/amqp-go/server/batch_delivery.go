package server

import (
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

const deliveryOverheadEstimate = 256

// sendBatchedDeliveries serializes all deliveries in a batch into a single
// buffer and performs a single conn.Conn.Write under a single WriteMutex
// acquisition. This reduces N syscalls + N mutex acquisitions to 1 of each.
func (s *Server) sendBatchedDeliveries(conn *protocol.Connection, channelID uint16, consumerTag string, deliveries []*protocol.Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}

	s.Log.Debug("Sending batched deliveries",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("batch_size", len(deliveries)))

	estimatedSize := 0
	for _, d := range deliveries {
		if d.Message != nil {
			estimatedSize += deliveryOverheadEstimate + len(d.Message.Body)
		}
	}

	const maxPoolSize = 131 * 1024
	var batchBuf *[]byte
	if estimatedSize > maxPoolSize {
		b := make([]byte, 0, estimatedSize)
		batchBuf = &b
	} else {
		batchBuf = protocol.GetBufferForSize(estimatedSize)
	}
	defer func() {
		if cap(*batchBuf) <= maxPoolSize {
			protocol.PutBufferForSize(batchBuf)
		}
		batchBuf = nil
	}()

	for _, delivery := range deliveries {
		data, err := s.serializeDelivery(
			channelID,
			consumerTag,
			delivery.DeliveryTag,
			delivery.Redelivered,
			delivery.Exchange,
			delivery.RoutingKey,
			delivery.Message,
		)
		if err != nil {
			s.Log.Error("Failed to serialize delivery in batch",
				zap.Error(err),
				zap.String("consumer_tag", consumerTag),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			return err
		}
		*batchBuf = append(*batchBuf, data...)
		protocol.PutBufferForSize(&data)
	}

	s.Log.Debug("Writing batched deliveries atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("batch_size", len(deliveries)),
		zap.Int("total_bytes", len(*batchBuf)))

	conn.WriteMutex.Lock()
	_, err := conn.Conn.Write(*batchBuf)
	conn.WriteMutex.Unlock()

	if err != nil {
		s.Log.Error("Failed to write batched deliveries",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Int("batch_size", len(deliveries)))
		return err
	}

	if s.MetricsCollector != nil {
		for _, delivery := range deliveries {
			s.MetricsCollector.RecordMessageDelivered(len(delivery.Message.Body))
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
