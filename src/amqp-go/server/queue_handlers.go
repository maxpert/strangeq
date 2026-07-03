package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// resolveQueueName resolves an empty queue name to the channel's "current
// queue" (the last queue declared on the channel), per AMQP 0.9.1 spec
// "queue-known" rule. Returns the resolved name, or an error if the name
// is empty and no queue was previously declared on the channel (spec:
// "this will result in a 502 (syntax error) channel exception").
func resolveQueueName(conn *protocol.Connection, channelID uint16, queueName string) (string, error) {
	if queueName != "" {
		return queueName, nil
	}
	// Empty name: resolve to the channel's current queue
	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.RLock()
		current := ch.CurrentQueue
		ch.Mutex.RUnlock()
		if current != "" {
			return current, nil
		}
	}
	return "", fmt.Errorf("no queue name specified and no current queue on channel %d (spec: 502 syntax error)", channelID)
}

// processQueueMethod handles queue-related methods
func (s *Server) processQueueMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.QueueDeclare: // Method ID 10 for queue class
		return s.handleQueueDeclare(conn, channelID, payload)
	case protocol.QueueBind: // Method ID 20 for queue class
		return s.handleQueueBind(conn, channelID, payload)
	case protocol.QueueUnbind: // Method ID 50 for queue class
		return s.handleQueueUnbind(conn, channelID, payload)
	case protocol.QueuePurge: // Method ID 30 for queue class
		return s.handleQueuePurge(conn, channelID, payload)
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

	// AMQP 0.9.1 spec "reserved" rule (on-failure: access-refused 403):
	// Queue names starting with "amq." are reserved. The client MAY declare
	// a queue starting with "amq." if the passive option is set, or the
	// queue already exists. This check only applies to client-provided names,
	// not server-generated names (amq.gen.*).
	if !declareMethod.Passive && isReservedName(declareMethod.Queue) {
		return s.authzChannelError(conn, channelID,
			fmt.Errorf("queue name %q is reserved (spec: amq. prefix)", declareMethod.Queue),
			50, 10)
	}

	// AMQP 0.9.1 spec "default-name" rule: if queue name is empty, the server
	// MUST create a new queue with a unique generated name.
	queueName := declareMethod.Queue
	if queueName == "" {
		queueName = protocol.GenerateQueueName()
	}

	s.Log.Debug("Queue declared",
		zap.String("queue", queueName),
		zap.Bool("durable", declareMethod.Durable),
		zap.Bool("auto_delete", declareMethod.AutoDelete),
		zap.Bool("exclusive", declareMethod.Exclusive))

	// Authorization check: queue.declare requires configure permission on queue
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 10)
	}

	// Call the broker to declare the queue
	queue, err := s.Broker.DeclareQueue(
		queueName,
		declareMethod.Durable,
		declareMethod.AutoDelete,
		declareMethod.Exclusive,
		declareMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to declare queue",
			zap.Error(err),
			zap.String("queue", queueName))
		return err
	}

	// AMQP 0.9.1 spec "queue-known" rule: track the last declared queue on
	// the channel so subsequent methods with empty queue name can resolve it.
	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.Lock()
		ch.CurrentQueue = queue.Name
		ch.Mutex.Unlock()
	}

	// Record queue declared metric
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordQueueDeclared()
	}

	// Send queue.declare-ok response with queue info (spec: must return the
	// actual queue name, including server-generated names)
	msgCount := uint32(queue.MessageCount.Load())
	return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, 0)
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

	// AMQP 0.9.1 spec "queue-known" rule: resolve empty queue name to the
	// channel's current queue (last declared queue).
	queueName, err := resolveQueueName(conn, channelID, bindMethod.Queue)
	if err != nil {
		return err
	}

	s.Log.Debug("Queue bound",
		zap.String("queue", queueName),
		zap.String("exchange", bindMethod.Exchange),
		zap.String("routing_key", bindMethod.RoutingKey))

	// Authorization check: queue.bind requires write on queue AND read on exchange
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 20)
	}
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceExchange,
		Resource:     bindMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 20)
	}

	// Call the broker to bind the queue
	err = s.Broker.BindQueue(
		queueName,
		bindMethod.Exchange,
		bindMethod.RoutingKey,
		bindMethod.Arguments,
	)

	if err != nil {
		s.Log.Error("Failed to bind queue",
			zap.Error(err),
			zap.String("queue", queueName),
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

	// AMQP 0.9.1 spec "queue-known" rule: resolve empty queue name to the
	// channel's current queue (last declared queue).
	queueName, err := resolveQueueName(conn, channelID, unbindMethod.Queue)
	if err != nil {
		return err
	}

	s.Log.Debug("Queue unbound",
		zap.String("queue", queueName),
		zap.String("exchange", unbindMethod.Exchange),
		zap.String("routing_key", unbindMethod.RoutingKey))

	// Authorization check: queue.unbind requires write on queue AND read on exchange
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 50)
	}
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceExchange,
		Resource:     unbindMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 50)
	}

	// Call the broker to unbind the queue
	err = s.Broker.UnbindQueue(
		queueName,
		unbindMethod.Exchange,
		unbindMethod.RoutingKey,
	)

	if err != nil {
		s.Log.Error("Failed to unbind queue",
			zap.Error(err),
			zap.String("queue", queueName),
			zap.String("exchange", unbindMethod.Exchange))
		return err
	}

	// Send queue.unbind-ok response
	return s.sendQueueUnbindOK(conn, channelID)
}

// handleQueuePurge handles the queue.purge method
func (s *Server) handleQueuePurge(conn *protocol.Connection, channelID uint16, payload []byte) error {
	purgeMethod := &protocol.QueuePurgeMethod{}
	err := purgeMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.purge",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	queueName, err := resolveQueueName(conn, channelID, purgeMethod.Queue)
	if err != nil {
		return err
	}

	s.Log.Debug("Queue purge requested",
		zap.String("queue", queueName))

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 30)
	}

	count, err := s.Broker.PurgeQueue(queueName)
	if err != nil {
		s.Log.Error("Failed to purge queue",
			zap.Error(err),
			zap.String("queue", queueName))
		return err
	}

	if !purgeMethod.NoWait {
		return s.sendQueuePurgeOK(conn, channelID, uint32(count))
	}

	return nil
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

	// AMQP 0.9.1 spec "queue-known" rule: resolve empty queue name to the
	// channel's current queue (last declared queue).
	queueName, err := resolveQueueName(conn, channelID, deleteMethod.Queue)
	if err != nil {
		return err
	}

	s.Log.Debug("Queue deleted",
		zap.String("queue", queueName),
		zap.Bool("if_unused", deleteMethod.IfUnused),
		zap.Bool("if_empty", deleteMethod.IfEmpty))

	// Authorization check: queue.delete requires configure permission on queue
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 40)
	}

	// Call the broker to delete the queue
	err = s.Broker.DeleteQueue(queueName, deleteMethod.IfUnused, deleteMethod.IfEmpty)
	if err != nil {
		s.Log.Error("Failed to delete queue",
			zap.Error(err),
			zap.String("queue", queueName))
		return err
	}

	// Clear the current queue if it was the one deleted
	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.Lock()
		if ch.CurrentQueue == queueName {
			ch.CurrentQueue = ""
		}
		ch.Mutex.Unlock()
	}

	// Record queue deleted metric
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordQueueDeleted()
	}

	// Send queue.delete-ok response with number of deleted messages
	return s.sendQueueDeleteOK(conn, channelID, 0)
}
