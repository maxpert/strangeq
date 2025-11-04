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
			for consumerTag := range channel.Consumers {
				s.Log.Debug("Canceling consumer due to channel close",
					zap.String("consumer_tag", consumerTag),
					zap.Uint16("channel_id", channelID))
			}
			channel.Consumers = make(map[string]*protocol.Consumer) // Clear all consumers
			channel.Closed = true
			channel.Mutex.Unlock()

			// Remove channel from connection
			conn.Channels.Delete(channelID)
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
