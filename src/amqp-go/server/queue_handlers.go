package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
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
	return "", fmt.Errorf("%w: channel %d", errNoCurrentQueue, channelID)
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
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown method 50.%d", methodID), 50, methodID)
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
		return s.malformed(conn, err, 50, 10)
	}

	// WS-D §1.3: passive declare is a strictly read-only path (R1) — it never
	// generates a name, never resolves the empty-name shortcut (D1), never
	// creates anything, and never runs redeclare equivalence.
	if declareMethod.Passive {
		return s.handleQueueDeclarePassive(conn, channelID, declareMethod)
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
		// WS-D: every known broker semantic error (invalid-argument 406,
		// inequivalent-args 406, queue-locked 405, queue-limit 406, ...) is a
		// channel-level soft error through the shared mapping; the connection
		// stays alive. Unknown errors keep the log-and-return path.
		if _, _, ok := brokerErrorReply(err); ok {
			return s.closeChannelWithBrokerError(conn, channelID, err, 50, 10)
		}
		s.Log.Error("Failed to declare queue",
			zap.Error(err),
			zap.String("queue", queueName))
		return err
	}

	if declareMethod.Exclusive {
		if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
			if !sb.broker.SetQueueOwnerIfFree(queue.Name, conn.ID) {
				replyText := fmt.Sprintf("QUEUE_LOCKED - queue '%s' is exclusive to another connection", queue.Name)
				return s.closeChannelWith(conn, channelID, uint16(amqperrors.ResourceLocked), replyText, 50, 10)
			}
			queue.OwnerConnID = conn.ID
		}
	}

	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.Lock()
		ch.CurrentQueue = queue.Name
		ch.Mutex.Unlock()
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordQueueDeclared()
	}

	if declareMethod.NoWait {
		return nil
	}

	// message-count must reflect the real READY depth — RabbitMQ returns the
	// count of messages available for delivery (excluding delivered-but-unacked)
	// on every declare, passive or not. The protocol.Queue.MessageCount runtime
	// counter is never maintained, so query the broker's ready count; fall back
	// to 0 for a non-storage broker.
	msgCount := uint32(0)
	consumerCount := uint32(0)
	if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
		msgCount = sb.broker.GetQueueReadyCount(queue.Name)
		consumerCount = uint32(sb.broker.GetQueueConsumerCount(queue.Name))
	}
	return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, consumerCount)
}

// handleQueueDeclarePassive implements WS-D §1.3: a strictly read-only
// queue.declare (R1). It never generates a server name and never resolves the
// empty-name "queue-known" shortcut (D1: queue.declare is not in RabbitMQ's
// expand_shortcuts), so an empty name is a plain 404 for queue ”. Flags and
// arguments in the frame are IGNORED: the passive clause never calls
// assert_equivalence, so a passive declare matching an existing queue with
// different flags/args still returns declare-ok with the real counts.
func (s *Server) handleQueueDeclarePassive(conn *protocol.Connection, channelID uint16, declareMethod *protocol.QueueDeclareMethod) error {
	// 1. D1: no name generation, no resolveQueueName.
	name := declareMethod.Queue

	// 2. Authorization: same configure check as the active path (changing the
	// required permission class is the authz owner's call, spec §16).
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: interfaces.ResourceQueue,
		Resource:     name,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 50, 10)
	}

	// 3. Read-only lookup through the storage broker; a non-storage broker
	// falls back to the management view.
	var queue *protocol.Queue
	var err error
	sb, ok := s.Broker.(*StorageBrokerAdapter)
	if ok {
		queue, err = sb.broker.QueueDeclarePassive(name)
	} else {
		queue = s.Broker.GetQueues()[name]
		if queue == nil {
			err = broker.QueueNotFoundError(name)
		}
	}
	if err != nil {
		return s.closeChannelWithBrokerError(conn, channelID, err, 50, 10)
	}

	// 4. Exclusive access (lax, R1 check_exclusive_access): 405 when the queue
	// is exclusively owned by ANOTHER connection.
	if ok {
		if owner, exists := sb.broker.GetQueueOwner(queue.Name); exists && owner != conn.ID {
			return s.closeChannelWith(conn, channelID, uint16(amqperrors.ResourceLocked),
				"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '"+queue.Name+
					"' in vhost '/'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
				50, 10)
		}
	}

	// 5. Track the channel's current queue exactly as the active path does.
	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.Lock()
		ch.CurrentQueue = queue.Name
		ch.Mutex.Unlock()
	}

	// 6. Nothing was declared: no RecordQueueDeclared metric.
	if declareMethod.NoWait {
		return nil
	}

	// 7. Declare-ok carries the REAL ready/consumer counts (read-only getters).
	msgCount := uint32(0)
	consumerCount := uint32(0)
	if ok {
		msgCount = sb.broker.GetQueueReadyCount(queue.Name)
		consumerCount = uint32(sb.broker.GetQueueConsumerCount(queue.Name))
	}
	return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, consumerCount)
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
		return s.malformed(conn, err, 50, 20)
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

// handleQueueUnbind handles the queue.unbind method (50.50).
func (s *Server) handleQueueUnbind(conn *protocol.Connection, channelID uint16, payload []byte) error {
	unbindMethod := &protocol.QueueUnbindMethod{}
	err := unbindMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize queue.unbind",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 50, 50)
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

	// AMQP 0-9-1 binding identity includes the arguments table: only the
	// binding whose arguments match may be removed.
	if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
		err = sb.broker.UnbindQueueWithArguments(queueName, unbindMethod.Exchange, unbindMethod.RoutingKey, unbindMethod.Arguments)
	} else {
		err = s.Broker.UnbindQueue(queueName, unbindMethod.Exchange, unbindMethod.RoutingKey)
	}

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
		return s.malformed(conn, err, 50, 30)
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

	// WS-D §4.4 (RabbitMQ with_exclusive_access_or_die): purging an exclusive
	// queue owned by another connection is 405.
	if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
		if ownerID, exists := sb.broker.GetQueueOwner(queueName); exists && ownerID != conn.ID {
			return s.closeChannelWith(conn, channelID, uint16(amqperrors.ResourceLocked),
				"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '"+queueName+
					"' in vhost '/'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
				50, 30)
		}
	}

	count, err := s.Broker.PurgeQueue(queueName)
	if err != nil {
		// WS-D §4.4: a purge of a nonexistent queue is 404 via the shared
		// mapping (the broker's closed-state path).
		if _, _, ok := brokerErrorReply(err); ok {
			return s.closeChannelWithBrokerError(conn, channelID, err, 50, 30)
		}
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
		return s.malformed(conn, err, 50, 40)
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

	if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
		if ownerID, exists := sb.broker.GetQueueOwner(queueName); exists && ownerID != conn.ID {
			replyText := fmt.Sprintf("QUEUE_LOCKED - queue '%s' is exclusive to another connection", queueName)
			return s.closeChannelWith(conn, channelID, uint16(amqperrors.ResourceLocked), replyText, 50, 40)
		}
	}

	var consumerIDs []string
	if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
		consumerIDs = sb.broker.GetQueueConsumerIDs(queueName)
	}

	count, err := s.Broker.DeleteQueue(queueName, deleteMethod.IfUnused, deleteMethod.IfEmpty)
	if err != nil {
		// WS-D §3.6: if-unused/if-empty refusals are 406 channel errors
		// (M8); the connection stays alive.
		if _, _, ok := brokerErrorReply(err); ok {
			return s.closeChannelWithBrokerError(conn, channelID, err, 50, 40)
		}
		s.Log.Error("Failed to delete queue",
			zap.Error(err),
			zap.String("queue", queueName))
		return err
	}

	if len(consumerIDs) > 0 {
		s.notifyConsumersCancelled(consumerIDs)
	}

	if val, exists := conn.Channels.Load(channelID); exists {
		ch := val.(*protocol.Channel)
		ch.Mutex.Lock()
		if ch.CurrentQueue == queueName {
			ch.CurrentQueue = ""
		}
		ch.Mutex.Unlock()
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordQueueDeleted()
	}

	if deleteMethod.NoWait {
		return nil
	}

	return s.sendQueueDeleteOK(conn, channelID, uint32(count))
}

// notifyConsumersCancelled sends a server-initiated basic.cancel to every
// consumer that was attached to a just-deleted queue.
//
// The consumers are addressed by broker-internal identity, and the scan
// compares Consumer.ID — NOT the map key. A bare wire tag does not identify a
// consumer across the server: AMQP 0-9-1 scopes it per channel, so a tag-keyed
// scan cancels whichever channel it reaches first and can close a live consumer
// belonging to an unrelated connection. Once the identity is matched, the
// basic.cancel carries that consumer's own wire tag, which is the only name its
// client knows it by.
func (s *Server) notifyConsumersCancelled(consumerIDs []string) {
	s.Mutex.RLock()
	connections := make([]*protocol.Connection, 0, len(s.Connections))
	for _, conn := range s.Connections {
		connections = append(connections, conn)
	}
	s.Mutex.RUnlock()

	for _, consumerID := range consumerIDs {
		for _, conn := range connections {
			if conn.Closed.Load() {
				continue
			}
			var foundChannel *protocol.Channel
			var foundTag string
			conn.Channels.Range(func(key, value interface{}) bool {
				ch := value.(*protocol.Channel)
				ch.Mutex.Lock()
				for tag, consumer := range ch.Consumers {
					if consumer.ID != consumerID {
						continue
					}
					foundChannel = ch
					foundTag = tag
					close(consumer.Cancel)
					delete(ch.Consumers, tag)
					break
				}
				ch.Mutex.Unlock()
				return foundChannel == nil
			})
			if foundChannel != nil {
				conn.ConsumersDirty.Store(true)
				if err := s.sendBasicCancel(conn, foundChannel.ID, foundTag); err != nil {
					s.Log.Warn("Failed to send basic.cancel to consumer",
						zap.String("consumer_tag", foundTag),
						zap.String("consumer_id", consumerID),
						zap.String("connection_id", conn.ID),
						zap.Error(err))
				}
				break
			}
		}
	}
}
