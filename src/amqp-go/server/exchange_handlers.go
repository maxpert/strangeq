package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

func isSupportedExchangeType(kind string) bool {
	switch kind {
	case "direct", "fanout", "topic", "headers":
		return true
	default:
		return false
	}
}

// processExchangeMethod handles exchange-related methods
func (s *Server) processExchangeMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ExchangeDeclare: // Method ID 10 for exchange class
		return s.handleExchangeDeclare(conn, channelID, payload)
	case protocol.ExchangeDelete: // Method ID 20 for exchange class
		return s.handleExchangeDelete(conn, channelID, payload)
	case protocol.ExchangeBind: // Method ID 30 for exchange class
		return s.handleExchangeBind(conn, channelID, payload)
	case protocol.ExchangeUnbind: // Method ID 40 for exchange class
		return s.handleExchangeUnbind(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown exchange method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown exchange method ID: %d", methodID)
	}
}

// handleExchangeDeclare handles the exchange.declare method
func (s *Server) handleExchangeDeclare(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the exchange.declare method
	declareMethod := &protocol.ExchangeDeclareMethod{}
	// We need to implement deserialization from the payload
	// Since we only have serialization, let's implement a simple approach
	err := declareMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.declare",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	// WS-D §1.4 (R4 check_not_default_exchange): declaring the default
	// exchange is refused, passive or not.
	if declareMethod.Exchange == "" {
		return s.closeChannelWith(conn, channelID, uint16(amqperrors.AccessRefused),
			"ACCESS_REFUSED - operation not permitted on the default exchange", 40, 10)
	}
	// WS-D §1.4: passive declare is a strictly read-only path (R1).
	if declareMethod.Passive {
		return s.handleExchangeDeclarePassive(conn, channelID, declareMethod)
	}

	s.Log.Debug("Exchange declared",
		zap.String("exchange", declareMethod.Exchange),
		zap.String("type", declareMethod.Type),
		zap.Bool("durable", declareMethod.Durable),
		zap.Bool("auto_delete", declareMethod.AutoDelete),
		zap.Bool("internal", declareMethod.Internal))

	// AMQP 0.9.1 spec "reserved" rule (on-failure: access-refused 403):
	// Exchange names starting with "amq." are reserved. The client MAY declare
	// an exchange starting with "amq." if the passive option is set, or the
	// exchange already exists.
	if !declareMethod.Passive && isReservedName(declareMethod.Exchange) {
		return s.authzChannelError(conn, channelID,
			fmt.Errorf("exchange name %q is reserved (spec: amq. prefix)", declareMethod.Exchange),
			40, 10)
	}

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: interfaces.ResourceExchange,
		Resource:     declareMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 10)
	}

	if !declareMethod.Passive && !isSupportedExchangeType(declareMethod.Type) {
		s.sendConnectionClose(conn, 503, "COMMAND_INVALID - unsupported exchange type", 40, 10)
		return fmt.Errorf("unsupported exchange type: %s", declareMethod.Type)
	}

	err = s.Broker.DeclareExchange(
		declareMethod.Exchange,
		declareMethod.Type,
		declareMethod.Durable,
		declareMethod.AutoDelete,
		declareMethod.Internal,
		declareMethod.Arguments,
	)

	if err != nil {
		// WS-D: every known broker semantic error (type mismatch and the whole
		// inequivalent-args family, exchange-limit 406, ...) goes through the
		// shared mapping; the connection stays alive. Unknown errors keep the
		// log-and-return path.
		if _, _, ok := brokerErrorReply(err); ok {
			return s.closeChannelWithBrokerError(conn, channelID, err, 40, 10)
		}
		s.Log.Error("Failed to declare exchange",
			zap.Error(err),
			zap.String("exchange", declareMethod.Exchange))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordExchangeDeclared()
	}

	if declareMethod.NoWait {
		return nil
	}

	return s.sendExchangeDeclareOK(conn, channelID)
}

// handleExchangeDeclarePassive implements WS-D §1.4: a strictly read-only
// exchange.declare (R1). Type/durable/auto-delete/internal/arguments in the
// frame are IGNORED (the passive clause never calls assert_equivalence).
// Missing → 404 via the shared broker-error mapping.
func (s *Server) handleExchangeDeclarePassive(conn *protocol.Connection, channelID uint16, declareMethod *protocol.ExchangeDeclareMethod) error {
	// Authorization: same configure check as the active path (40/10).
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: interfaces.ResourceExchange,
		Resource:     declareMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 10)
	}

	var exchange *protocol.Exchange
	var err error
	sb, ok := s.Broker.(*StorageBrokerAdapter)
	if ok {
		exchange, err = sb.broker.ExchangeDeclarePassive(declareMethod.Exchange)
	} else {
		exchange = s.Broker.GetExchanges()[declareMethod.Exchange]
		if exchange == nil {
			err = broker.ExchangeNotFoundError(declareMethod.Exchange)
		}
	}
	if err != nil {
		return s.closeChannelWithBrokerError(conn, channelID, err, 40, 10)
	}

	// Nothing was declared: no RecordExchangeDeclared metric.
	if declareMethod.NoWait {
		return nil
	}
	return s.sendExchangeDeclareOK(conn, channelID)
}

// handleExchangeDelete handles the exchange.delete method
func (s *Server) handleExchangeDelete(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the exchange.delete method
	deleteMethod := &protocol.ExchangeDeleteMethod{}
	err := deleteMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.delete",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Exchange deleted",
		zap.String("exchange", deleteMethod.Exchange),
		zap.Bool("if_unused", deleteMethod.IfUnused))

	// Authorization check: exchange.delete requires configure permission on exchange
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: interfaces.ResourceExchange,
		Resource:     deleteMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 20)
	}

	// Call the broker to delete the exchange
	err = s.Broker.DeleteExchange(deleteMethod.Exchange, deleteMethod.IfUnused)
	if err != nil {
		s.Log.Error("Failed to delete exchange",
			zap.Error(err),
			zap.String("exchange", deleteMethod.Exchange))
		return err
	}

	// Record exchange deleted metric
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordExchangeDeleted()
	}

	// Send exchange.delete-ok response
	return s.sendExchangeDeleteOK(conn, channelID)
}

// handleExchangeBind handles the exchange.bind method
func (s *Server) handleExchangeBind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	bindMethod := &protocol.ExchangeBindMethod{}
	err := bindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.bind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Exchange bound",
		zap.String("destination", bindMethod.Destination),
		zap.String("source", bindMethod.Source),
		zap.String("routing_key", bindMethod.RoutingKey))

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceExchange,
		Resource:     bindMethod.Destination,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 30)
	}
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceExchange,
		Resource:     bindMethod.Source,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 30)
	}

	err = s.Broker.BindExchange(bindMethod.Destination, bindMethod.Source, bindMethod.RoutingKey, bindMethod.Arguments)
	if err != nil {
		s.Log.Error("Failed to bind exchange",
			zap.Error(err),
			zap.String("destination", bindMethod.Destination),
			zap.String("source", bindMethod.Source))
		return err
	}

	if bindMethod.NoWait {
		return nil
	}
	return s.sendExchangeBindOK(conn, channelID)
}

// handleExchangeUnbind handles the exchange.unbind method
func (s *Server) handleExchangeUnbind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	unbindMethod := &protocol.ExchangeUnbindMethod{}
	err := unbindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize exchange.unbind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Exchange unbound",
		zap.String("destination", unbindMethod.Destination),
		zap.String("source", unbindMethod.Source),
		zap.String("routing_key", unbindMethod.RoutingKey))

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceExchange,
		Resource:     unbindMethod.Destination,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 40)
	}
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceExchange,
		Resource:     unbindMethod.Source,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 40, 40)
	}

	err = s.Broker.UnbindExchange(unbindMethod.Destination, unbindMethod.Source, unbindMethod.RoutingKey)
	if err != nil {
		s.Log.Error("Failed to unbind exchange",
			zap.Error(err),
			zap.String("destination", unbindMethod.Destination),
			zap.String("source", unbindMethod.Source))
		return err
	}

	if unbindMethod.NoWait {
		return nil
	}
	return s.sendExchangeUnbindOK(conn, channelID)
}
