package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// processQueueMethod handles queue-related methods
func (s *Server) processQueueMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.QueueDeclare: // Method ID 10 for queue class
		return s.handleQueueDeclare(conn, channelID, payload)
	case protocol.QueueBind: // Method ID 20 for queue class
		return s.handleQueueBind(conn, channelID, payload)
	case protocol.QueueUnbind: // Method ID 50 for queue class
		return s.handleQueueUnbind(conn, channelID, payload)
	case protocol.QueueDelete: // Method ID 40 for queue class
		return s.handleQueueDelete(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown queue method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown queue method ID: %d", methodID)
	}
}

// handleQueueDeclare handles the queue.declare method
func (s *Server) handleQueueDeclare(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.declare method
	declareMethod := &protocol.QueueDeclareMethod{}
	err := declareMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.declare",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue declared",
		zap.String("queue", declareMethod.Queue),
		zap.Bool("durable", declareMethod.Durable),
		zap.Bool("auto_delete", declareMethod.AutoDelete),
		zap.Bool("exclusive", declareMethod.Exclusive))

	// Call the broker to declare the queue
	queue, err := s.Broker.DeclareQueue(
		declareMethod.Queue,
		declareMethod.Durable,
		declareMethod.AutoDelete,
		declareMethod.Exclusive,
		declareMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to declare queue",
			zap.Error(err),
			zap.String("queue", declareMethod.Queue))
		return err
	}

	// Send queue.declare-ok response with queue info
	return s.sendQueueDeclareOK(conn, channelID, queue.Name, uint32(queue.MessageCount()), 0)
}

// handleQueueBind handles the queue.bind method
func (s *Server) handleQueueBind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.bind method
	bindMethod := &protocol.QueueBindMethod{}
	err := bindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.bind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue bound",
		zap.String("queue", bindMethod.Queue),
		zap.String("exchange", bindMethod.Exchange),
		zap.String("routing_key", bindMethod.RoutingKey))

	// Call the broker to bind the queue
	err = s.Broker.BindQueue(
		bindMethod.Queue,
		bindMethod.Exchange,
		bindMethod.RoutingKey,
		bindMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to bind queue",
			zap.Error(err),
			zap.String("queue", bindMethod.Queue),
			zap.String("exchange", bindMethod.Exchange))
		return err
	}

	// Send queue.bind-ok response
	return s.sendQueueBindOK(conn, channelID)
}

// handleQueueUnbind handles the queue.unbind method
func (s *Server) handleQueueUnbind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.unbind method
	// We need to create a structure for this, but for now we'll implement a basic approach
	// The queue.unbind method has similar structure to queue.bind but without arguments

	// Create a temporary approach - reuse QueueBindMethod for deserialization since structure is similar
	unbindMethod := &protocol.QueueBindMethod{}
	err := unbindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.unbind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue unbound",
		zap.String("queue", unbindMethod.Queue),
		zap.String("exchange", unbindMethod.Exchange),
		zap.String("routing_key", unbindMethod.RoutingKey))

	// Call the broker to unbind the queue
	err = s.Broker.UnbindQueue(
		unbindMethod.Queue,
		unbindMethod.Exchange,
		unbindMethod.RoutingKey,
	)

	if err != nil {
		s.Log.Error("Failed to unbind queue",
			zap.Error(err),
			zap.String("queue", unbindMethod.Queue),
			zap.String("exchange", unbindMethod.Exchange))
		return err
	}

	// Send queue.unbind-ok response
	return s.sendQueueUnbindOK(conn, channelID)
}

// handleQueueDelete handles the queue.delete method
func (s *Server) handleQueueDelete(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the queue.delete method
	deleteMethod := &protocol.QueueDeleteMethod{}
	err := deleteMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.delete",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Queue deleted",
		zap.String("queue", deleteMethod.Queue),
		zap.Bool("if_unused", deleteMethod.IfUnused),
		zap.Bool("if_empty", deleteMethod.IfEmpty))

	// Call the broker to delete the queue
	err = s.Broker.DeleteQueue(deleteMethod.Queue, deleteMethod.IfUnused, deleteMethod.IfEmpty)
	if err != nil {
		s.Log.Error("Failed to delete queue",
			zap.Error(err),
			zap.String("queue", deleteMethod.Queue))
		return err
	}

	// Send queue.delete-ok response with number of deleted messages
	return s.sendQueueDeleteOK(conn, channelID, 0)
}
