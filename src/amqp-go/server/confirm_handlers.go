package server

import (
	"fmt"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// processConfirmMethod processes confirm class methods (class 85)
func (s *Server) processConfirmMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ConfirmSelect: // 85.10
		return s.handleConfirmSelect(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown confirm method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown method 85.%d", methodID), 85, methodID)
	}
}

// handleConfirmSelect handles the confirm.select method (85.10)
func (s *Server) handleConfirmSelect(conn *protocol.Connection, channelID uint16, payload []byte) error {
	method := &protocol.ConfirmSelectMethod{}
	if err := method.Deserialize(payload); err != nil {
		s.Log.Error("Failed to deserialize confirm.select",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 85, 10)
	}

	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return s.connectionError(conn, amqperrors.ChannelErrorCode, "CHANNEL_ERROR - expected 'channel.open'", 85, 10)
	}
	channel := value.(*protocol.Channel)

	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		s.Log.Warn("Cannot enable confirm mode on transactional channel",
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return s.channelError(conn, channelID, amqperrors.PreconditionFailed,
			"PRECONDITION_FAILED - cannot switch from tx to confirm mode", 85, 10)
	}

	channel.ConfirmMode.Store(true)
	// Arm the confirm flusher's bounded safety re-check for this connection (see
	// Connection.ConfirmActive): now that a channel emits publisher confirms, the
	// flusher must never rely solely on a ConfirmWake poke.
	conn.ConfirmActive.Store(true)

	s.Log.Info("Confirm mode enabled",
		zap.Uint16("channel_id", channelID),
		zap.String("connection_id", conn.ID))

	if !method.NoWait {
		return s.sendConfirmSelectOK(conn, channelID)
	}

	return nil
}
