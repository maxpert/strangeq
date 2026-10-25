package server

import (
	"fmt"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// processChannelSpecificMethod handles specific channel methods
func (s *Server) processChannelSpecificMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ChannelOpen: // Method ID 10 for channel class
		rt := s.runtimeFor(conn)
		if rt.isChannelClosing(channelID) {
			return nil // unreachable: the closing gate discards; defensive
		}
		if _, exists := conn.Channels.Load(channelID); exists {
			// AMQP 0-9-1: "second 'channel.open' seen" is a channel-error, and
			// 504 is a HARD code: connection.close. A duplicate open must never
			// overwrite the live channel (its consumers would leak in the
			// broker, unreachable from conn.Channels).
			return s.connectionError(conn, amqperrors.ChannelErrorCode,
				fmt.Sprintf("CHANNEL_ERROR - second 'channel.open' seen on channel %d", channelID), 20, 10)
		}
		if conn.MaxChannels > 0 && conn.ChannelCount.Load() >= int32(conn.MaxChannels) {
			s.Log.Warn("Max channels per connection exceeded",
				zap.Uint16("channel_id", channelID),
				zap.String("connection_id", conn.ID),
				zap.Uint16("max", conn.MaxChannels))
			return s.connectionError(conn, amqperrors.NotAllowed,
				fmt.Sprintf("NOT_ALLOWED - number of channels opened (%d) has reached the negotiated channel_max (%d)",
					conn.ChannelCount.Load(), conn.MaxChannels), 20, 10)
		}

		// Create new channel
		newChannel := protocol.NewChannel(channelID, conn)
		if _, loaded := conn.Channels.LoadOrStore(channelID, newChannel); loaded {
			return s.connectionError(conn, amqperrors.ChannelErrorCode,
				fmt.Sprintf("CHANNEL_ERROR - second 'channel.open' seen on channel %d", channelID), 20, 10)
		}
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
			return s.malformed(conn, err, 20, 20)
		}

		value, exists := conn.Channels.Load(channelID)
		if !exists {
			return s.connectionError(conn, amqperrors.ChannelErrorCode, "CHANNEL_ERROR - expected 'channel.open'", 20, 20)
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
			return s.malformed(conn, err, 20, 21)
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

		// SQ-5: flush any durable-but-unacked publisher confirms and seal
		// the channel's confirm state BEFORE teardown and channel.close-ok,
		// so the client receives every outstanding confirm and no straggling
		// flusher pass can write an ack on the closed channel afterwards.
		if value, exists := conn.Channels.Load(channelID); exists {
			channel := value.(*protocol.Channel)
			if channel.ConfirmMode.Load() {
				if err := channel.FlushAndCloseConfirms(func(tag uint64, multiple bool) error {
					return s.sendBasicAck(conn, channelID, tag, multiple)
				}); err != nil {
					s.Log.Warn("Failed to flush publisher confirms on channel close",
						zap.Uint16("channel_id", channelID),
						zap.String("connection_id", conn.ID),
						zap.Error(err))
				}
			}
		}

		s.teardownChannel(conn, channelID)
		// When both peers close at once, each side replies close-ok; receiving
		// channel.close also ends our closing state.
		s.runtimeFor(conn).clearChannelClosing(channelID)

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
		s.runtimeFor(conn).clearChannelClosing(channelID)
		return nil

	default:
		s.Log.Warn("Unknown channel method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown method 20.%d", methodID), 20, methodID)
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

	// Seal the channel's publisher-confirm state (SQ-5) so a straggling
	// confirm-flusher pass cannot write a basic.ack for this channel after
	// teardown. On the client-initiated close path the caller has already
	// flushed real acks and sealed (this is then a no-op); on the
	// server-initiated close-ok path any unflushed confirms are dropped —
	// per AMQP semantics a channel error fails outstanding confirms.
	if channel.ConfirmMode.Load() {
		_ = channel.FlushAndCloseConfirms(func(uint64, bool) error { return nil })
	}

	// Cancel all consumers on this channel. Collected as broker-internal
	// identities: unregistering by wire tag would tear down whichever
	// same-tagged consumer registered last on the broker, which may belong to
	// an entirely different connection.
	channel.Mutex.Lock()
	consumerIDs := make([]string, 0, len(channel.Consumers))
	for _, consumer := range channel.Consumers {
		consumerIDs = append(consumerIDs, consumer.ID)
	}
	channel.Consumers = make(map[string]*protocol.Consumer) // Clear all consumers
	channel.Closed = true
	channel.Mutex.Unlock()
	conn.ConsumersDirty.Store(true)

	// Drop a half-received publish on this channel (processor-owned map;
	// teardownChannel only runs on the processor or after it has exited).
	// PendingMessages is single-writer per the comment at structures.go, so
	// this stays outside channel.Mutex.
	if pm, ok := conn.PendingMessages[channelID]; ok {
		delete(conn.PendingMessages, channelID)
		// WS-I: a half-received publish aborts its lease here (zero token is a no-op).
		pm.LeaseToken.Abort()
		pm.LeaseToken = protocol.PublishLeaseToken{}
		protocol.PutBasicPublishMethod(pm.Method)
		if pm.Header != nil {
			protocol.PutContentHeader(pm.Header)
		}
		protocol.PutPendingMessage(pm)
	}

	// Unregister consumers from broker (stops poll goroutines)
	for _, consumerID := range consumerIDs {
		err := s.Broker.UnregisterConsumer(consumerID)
		if err != nil {
			s.Log.Warn("Failed to unregister consumer on channel close",
				zap.String("consumer_id", consumerID),
				zap.Uint16("channel_id", channelID),
				zap.Error(err))
		} else {
			s.Log.Debug("Unregistered consumer on channel close",
				zap.String("consumer_id", consumerID),
				zap.Uint16("channel_id", channelID))
		}
	}

	// Remove channel from connection
	conn.Channels.Delete(channelID)
	conn.ChannelCount.Add(-1)

	// Clean up transaction state for this channel (conn-scoped, D-NEW-1:
	// closing this connection's channel must not drop another connection's
	// same-numbered tx state).
	if txm := s.txm(conn); txm != nil {
		txm.Close(channelID)
	}

	// Record channel closed metric
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordChannelClosed()
	}
}
