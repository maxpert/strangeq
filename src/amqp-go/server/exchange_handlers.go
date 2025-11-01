package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// processExchangeMethod handles exchange-related methods
func (s *Server) processExchangeMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.ExchangeDeclare: // Method ID 10 for exchange class
		return s.handleExchangeDeclare(conn, channelID, payload)
	case protocol.ExchangeDelete: // Method ID 20 for exchange class
		return s.handleExchangeDelete(conn, channelID, payload)
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

	s.Log.Debug("Exchange declared",
		zap.String("exchange", declareMethod.Exchange),
		zap.String("type", declareMethod.Type),
		zap.Bool("durable", declareMethod.Durable),
		zap.Bool("auto_delete", declareMethod.AutoDelete),
		zap.Bool("internal", declareMethod.Internal))

	// Call the broker to declare the exchange
	err = s.Broker.DeclareExchange(
		declareMethod.Exchange,
		declareMethod.Type,
		declareMethod.Durable,
		declareMethod.AutoDelete,
		declareMethod.Internal,
		declareMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to declare exchange",
			zap.Error(err),
			zap.String("exchange", declareMethod.Exchange))
		return err
	}

	// Send exchange.declare-ok response
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

	// Call the broker to delete the exchange
	err = s.Broker.DeleteExchange(deleteMethod.Exchange, deleteMethod.IfUnused)
	if err != nil {
		s.Log.Error("Failed to delete exchange",
			zap.Error(err),
			zap.String("exchange", deleteMethod.Exchange))
		return err
	}

	// Send exchange.delete-ok response
	return s.sendExchangeDeleteOK(conn, channelID)
}

// handleExchangeUnbind handles the exchange.unbind method
func (s *Server) handleExchangeUnbind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the exchange.unbind method
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

	// Call the broker to unbind the exchange
	// In a real implementation, you'd remove the binding between source and destination exchanges
	// For now, we'll just log the operation

	// Send exchange.unbind-ok response
	return s.sendExchangeUnbindOK(conn, channelID)
}
