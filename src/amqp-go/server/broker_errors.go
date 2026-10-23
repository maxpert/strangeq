package server

import (
	"errors"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// brokerErrorReply classifies a broker error into an AMQP channel-error reply.
// ok=false means "not a known semantic error" (caller keeps its existing path).
// WS-B's central ChannelError mapping MUST delegate broker errors to this
// function rather than duplicating the table (spec §19.1, M1-M11).
func brokerErrorReply(err error) (code uint16, text string, ok bool) {
	switch {
	case errors.Is(err, broker.ErrQueueLocked):
		return amqperrors.ResourceLocked, "RESOURCE_LOCKED - " + err.Error(), true
	case errors.Is(err, broker.ErrExchangeInternal):
		return amqperrors.AccessRefused, "ACCESS_REFUSED - " + err.Error(), true
	case errors.Is(err, interfaces.ErrQueueNotFound), errors.Is(err, interfaces.ErrExchangeNotFound):
		return amqperrors.NotFound, "NOT_FOUND - " + err.Error(), true
	case errors.Is(err, broker.ErrInequivalentArgs), errors.Is(err, broker.ErrExchangeTypeMismatch),
		errors.Is(err, broker.ErrInvalidQueueArgument), errors.Is(err, broker.ErrQueueInUse),
		errors.Is(err, broker.ErrQueueNotEmpty), errors.Is(err, broker.ErrExchangeInUse),
		errors.Is(err, broker.ErrInvalidBindingArgument), errors.Is(err, broker.ErrQueueLimitReached),
		errors.Is(err, broker.ErrExchangeLimitReached):
		return amqperrors.PreconditionFailed, "PRECONDITION_FAILED - " + err.Error(), true
	}
	return 0, "", false
}

// closeChannelWithBrokerError is the ONLY way WS-D handlers report a broker
// semantic error. Pre-WS-B body below (send channel.close, keep the
// connection: same pattern as today's ErrInvalidQueueArgument branch at
// queue_handlers.go). When WS-B merges, WS-B replaces the body with
// `return ChannelError{Code: code, Text: text, ClassID: classID, MethodID: methodID}`.
func (s *Server) closeChannelWithBrokerError(conn *protocol.Connection, channelID uint16, err error, classID, methodID uint16) error {
	code, text, ok := brokerErrorReply(err)
	if !ok {
		return err
	}
	s.sendChannelClose(conn, channelID, code, text, classID, methodID)
	return nil
}

// closeChannelWith sends a channel error that originates in the server (e.g.
// default-exchange 403s). Same pre-/post-WS-B rule as above.
func (s *Server) closeChannelWith(conn *protocol.Connection, channelID uint16, code uint16, text string, classID, methodID uint16) error {
	s.sendChannelClose(conn, channelID, code, text, classID, methodID)
	return nil
}
