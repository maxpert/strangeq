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
		if conn.MaxChannels > 0 && conn.ChannelCount.Load() >= int32(conn.MaxChannels) {
			s.Log.Warn("Max channels per connection exceeded",
				zap.Uint16("channel_id", channelID),
				zap.String("connection_id", conn.ID),
				zap.Uint16("max", conn.MaxChannels))
			s.sendChannelClose(conn, channelID, 503, "CHANNEL_ERROR - too many channels", 20, 10)
			return nil
		}

		// Create new channel
		newChannel := protocol.NewChannel(channelID, conn)
		conn.Channels.Store(channelID, newChannel)
		conn.ChannelCount.Add(1)

		s.Log.Debug("Channel opened",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		// Record channel created metric
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordChannelCreated()
		}

		// Send channel.open-ok
		return s.sendChannelOpenOK(conn, channelID)

	case protocol.ChannelFlow: // Method ID 20 — client requests start/stop of content frames
		flowMethod := &protocol.ChannelFlowMethod{}
		if err := flowMethod.Deserialize(payload); err != nil {
			s.Log.Error("Failed to deserialize channel.flow",
				zap.Error(err),
				zap.Uint16("channel_id", channelID),
				zap.String("connection_id", conn.ID))
			return err
		}

		value, exists := conn.Channels.Load(channelID)
		if !exists {
			return fmt.Errorf("channel %d does not exist", channelID)
		}
		channel := value.(*protocol.Channel)

		channel.FlowActive.Store(flowMethod.Active)
		s.Log.Debug("Channel flow state updated",
			zap.Uint16("channel_id", channelID),
			zap.Bool("active", flowMethod.Active))

		if flowMethod.Active {
			select {
			case channel.FlowWake <- struct{}{}:
			default:
			}
		}

		return s.sendChannelFlowOK(conn, channelID, channel.FlowActive.Load())

	case protocol.ChannelFlowOK: // Method ID 21 — client acknowledges server-initiated flow
		flowOKMethod := &protocol.ChannelFlowOKMethod{}
		if err := flowOKMethod.Deserialize(payload); err != nil {
			s.Log.Error("Failed to deserialize channel.flow-ok",
				zap.Error(err),
				zap.Uint16("channel_id", channelID),
				zap.String("connection_id", conn.ID))
			return err
		}

		// The client's flow-ok confirms OUR channel.flow request about the
		// CLIENT's content direction (client -> server publishes). It must NOT
		// be stored into FlowActive: that flag gates the SERVER -> client
		// delivery direction and is controlled exclusively by client-initiated
		// channel.flow (case above). Writing the echo here wedged the broker —
		// the server's reader-overflow flow(false) was echoed back as
		// flow-ok(false), which parked forwardConsumerMessages, halting the very
		// deliveries (and thus acks) that drain the overflow: a self-inflicted
		// deadlock. flow-ok is acknowledgement only; just log it.
		s.Log.Debug("Channel flow-ok received",
			zap.Uint16("channel_id", channelID),
			zap.Bool("active", flowOKMethod.Active))

		return nil

	case protocol.ChannelClose: // Method ID 40 for channel class
		s.Log.Debug("Channel close requested",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		s.teardownChannel(conn, channelID)

		// Send channel.close-ok
		return s.sendChannelCloseOK(conn, channelID)

	case protocol.ChannelCloseOK: // Method ID 41 — client confirms a server-initiated channel.close
		// The server sent channel.close (e.g. a 406 PreconditionFailed soft
		// error from sendChannelClose) and the client replied close-ok. This
		// completes the close handshake: tear down the channel's resources
		// and keep the CONNECTION alive. Before this case existed, close-ok
		// fell through to the default branch, whose returned error tore down
		// the whole connection — turning every channel-level soft error into
		// a connection loss.
		s.Log.Debug("Channel close-ok received",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))

		s.teardownChannel(conn, channelID)
		return nil

	default:
		s.Log.Warn("Unknown channel method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown channel method ID: %d", methodID)
	}
}

// teardownChannel releases all per-channel resources: cancels and
// unregisters the channel's consumers, removes the channel from the
// connection, closes its transaction state, and records metrics. Called for
// both directions of the close handshake — client-initiated channel.close
// and the client's close-ok reply to a server-initiated channel.close.
// Idempotent: a second call for the same channel ID is a no-op.
func (s *Server) teardownChannel(conn *protocol.Connection, channelID uint16) {
	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return
	}
	channel := value.(*protocol.Channel)

	// Wake any forwarder parked on flow control so it can observe stop
	select {
	case channel.FlowWake <- struct{}{}:
	default:
	}

	// Cancel all consumers on this channel
	channel.Mutex.Lock()
	consumerTags := make([]string, 0, len(channel.Consumers))
	for consumerTag := range channel.Consumers {
		consumerTags = append(consumerTags, consumerTag)
	}
	channel.Consumers = make(map[string]*protocol.Consumer) // Clear all consumers
	channel.Closed = true
	channel.Mutex.Unlock()
	conn.ConsumersDirty.Store(true)

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
	conn.ChannelCount.Add(-1)

	// Clean up transaction state for this channel
	if s.TransactionManager != nil {
		s.TransactionManager.Close(channelID)
	}

	// Record channel closed metric
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordChannelClosed()
	}
}
