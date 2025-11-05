package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// processChannelSpecificMethod handles specific channel methods
func (s *Server) processChannelSpecificMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ChannelOpen: // Method ID 10 for channel class
		// Create new channel
		newChannel := protocol.NewChannel(channelID, conn)
		conn.Channels.Store(channelID, newChannel)

		s.Log.Debug("Channel opened",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		// Record channel created metric
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordChannelCreated()
		}

		// Send channel.open-ok
		return s.sendChannelOpenOK(conn, channelID)

	case protocol.ChannelClose: // Method ID 40 for channel class
		s.Log.Debug("Channel close requested",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		// Clean up channel resources
		if value, exists := conn.Channels.Load(channelID); exists {
			channel := value.(*protocol.Channel)
			// Cancel all consumers on this channel
			channel.Mutex.Lock()
			consumerTags := make([]string, 0, len(channel.Consumers))
			for consumerTag := range channel.Consumers {
				consumerTags = append(consumerTags, consumerTag)
			}
			channel.Consumers = make(map[string]*protocol.Consumer) // Clear all consumers
			channel.Closed = true
			channel.Mutex.Unlock()

			// Unregister consumers from broker (stops poll goroutines)
			for _, consumerTag := range consumerTags {
				err := s.Broker.UnregisterConsumer(consumerTag)
				if err != nil {
					s.Log.Warn("Failed to unregister consumer on channel close",
						zap.String("consumer_tag", consumerTag),
						zap.Uint16("channel_id", channelID),
						zap.Error(err))
				} else {
					s.Log.Debug("Unregistered consumer on channel close",
						zap.String("consumer_tag", consumerTag),
						zap.Uint16("channel_id", channelID))
				}
			}

			// Remove channel from connection
			conn.Channels.Delete(channelID)

			// Record channel closed metric
			if s.MetricsCollector != nil {
				s.MetricsCollector.RecordChannelClosed()
			}
		}

		// Send channel.close-ok
		return s.sendChannelCloseOK(conn, channelID)

	default:
		s.Log.Warn("Unknown channel method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown channel method ID: %d", methodID)
	}
}
