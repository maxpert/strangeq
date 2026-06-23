package server

import (
	"strings"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// authorize checks if the connection's authenticated user has permission
// for the given operation. Returns nil if authorized, or an error if not.
// When authorization is disabled or no authenticator is configured, all
// operations are allowed (returns nil).
//
// Authorization failures are channel-level soft errors (403 access-refused):
// the caller should send channel.close and close the channel, not the connection.
func (s *Server) authorize(conn *protocol.Connection, channelID uint16, op interfaces.Operation) error {
	if s.Config == nil || !s.Config.Security.AuthorizationEnabled {
		return nil
	}
	if conn.User == nil {
		return amqperrors.NewAuthError(amqperrors.AccessRefused,
			"authorization enabled but no authenticated user", "", "", string(op.Action))
	}
	user, ok := conn.User.(*interfaces.User)
	if !ok {
		return amqperrors.NewAuthError(amqperrors.AccessRefused,
			"unsupported user type for authorization", "", "", string(op.Action))
	}
	if s.Authenticator == nil {
		return amqperrors.NewAuthError(amqperrors.AccessRefused,
			"authorization enabled but no authenticator configured", "", "", string(op.Action))
	}
	return s.Authenticator.Authorize(user, op)
}

// sendChannelClose sends a channel.close method frame with the given error
// code and reply text, then marks the channel as closed.
func (s *Server) sendChannelClose(conn *protocol.Connection, channelID uint16, replyCode uint16, replyText string, classID, methodID uint16) {
	closeMethod := &protocol.ChannelCloseMethod{
		ReplyCode: replyCode,
		ReplyText: replyText,
		ClassID:   classID,
		MethodID:  methodID,
	}

	methodData, err := closeMethod.Serialize()
	if err != nil {
		s.Log.Error("Failed to serialize channel.close", zap.Error(err))
		return
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 20, 40, methodData) // 20.40 = channel.close

	if err := protocol.WriteFrameToConnection(conn, frame); err != nil {
		s.Log.Error("Failed to send channel.close", zap.Error(err))
	}

	// Mark channel as closed
	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.Lock()
		ch.Closed = true
		ch.Mutex.Unlock()
	}
}

// authzChannelError sends a channel.close with 403 (access-refused) for the
// given authorization error and operation context. Returns the error so
// the handler can return it to stop further processing.
func (s *Server) authzChannelError(conn *protocol.Connection, channelID uint16, err error, classID, methodID uint16) error {
	replyText := "Access refused"
	if err != nil {
		replyText = err.Error()
	}
	s.Log.Warn("Authorization denied",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID),
		zap.Error(err))
	s.sendChannelClose(conn, channelID, amqperrors.AccessRefused, replyText, classID, methodID)
	return err
}

// isReservedName checks if a resource name starts with "amq." — the AMQP 0.9.1
// spec reserves these for pre-declared and standardized exchanges/queues.
// Clients MAY declare a name starting with "amq." only if the passive option
// is set or the resource already exists.
func isReservedName(name string) bool {
	return strings.HasPrefix(name, "amq.")
}
