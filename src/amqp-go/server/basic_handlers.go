package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/transaction"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (s *Server) processBasicMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.BasicQos: // Method ID 10 for basic class
		return s.handleBasicQos(conn, channelID, payload)
	case protocol.BasicPublish: // Method ID 40 for basic class
		return s.handleBasicPublish(conn, channelID, payload)
	case protocol.BasicConsume: // Method ID 20 for basic class
		return s.handleBasicConsume(conn, channelID, payload)
	case protocol.BasicCancel: // Method ID 30 for basic class
		return s.handleBasicCancel(conn, channelID, payload)
	case protocol.BasicGet: // Method ID 70 for basic class
		return s.handleBasicGet(conn, channelID, payload)
	case protocol.BasicAck: // Method ID 80 for basic class
		return s.handleBasicAck(conn, channelID, payload)
	case protocol.BasicReject: // Method ID 90 for basic class
		return s.handleBasicReject(conn, channelID, payload)
	case protocol.BasicNack: // Method ID 120 for basic class
		return s.handleBasicNack(conn, channelID, payload)
	case protocol.BasicRecover: // Method ID 110 for basic class
		return s.handleBasicRecover(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown basic method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown basic method ID: %d", methodID)
	}
}

// handleBasicQos handles the basic.qos method
func (s *Server) handleBasicQos(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.qos method
	qosMethod := &protocol.BasicQosMethod{}
	err := qosMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.qos",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic QoS settings",
		zap.Uint32("prefetch_size", qosMethod.PrefetchSize),
		zap.Uint16("prefetch_count", qosMethod.PrefetchCount),
		zap.Bool("global", qosMethod.Global))

	if qosMethod.PrefetchSize != 0 {
		s.sendChannelClose(conn, channelID, 540, "prefetch_size not implemented", 60, 10)
		return fmt.Errorf("prefetch_size not implemented")
	}

	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}
	channel := value.(*protocol.Channel)

	// Update channel prefetch settings
	channel.Mutex.Lock()
	channel.PrefetchCount = qosMethod.PrefetchCount
	channel.PrefetchSize = qosMethod.PrefetchSize
	channel.GlobalPrefetch = qosMethod.Global
	consumerTags := make([]string, 0, len(channel.Consumers))
	for tag := range channel.Consumers {
		consumerTags = append(consumerTags, tag)
	}
	channel.Mutex.Unlock()

	for _, tag := range consumerTags {
		s.Broker.UpdateConsumerPrefetch(tag, qosMethod.PrefetchCount)
	}

	return s.sendBasicQosOK(conn, channelID)
}

// handleBasicPublish handles the basic.publish method
func (s *Server) handleBasicPublish(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.publish method
	publishMethod := protocol.GetBasicPublishMethod()
	err := publishMethod.Deserialize(payload)
	if err != nil {
		protocol.PutBasicPublishMethod(publishMethod)
		s.Log.Error("Failed to deserialize basic.publish",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	if publishMethod.Immediate {
		protocol.PutBasicPublishMethod(publishMethod)
		s.sendConnectionClose(conn, 540, "NOT_IMPLEMENTED - immediate=true is not implemented", 60, 40)
		return fmt.Errorf("basic.publish immediate=true is not implemented")
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Basic publish received"); ce != nil {
		ce.Write(
			zap.String("exchange", publishMethod.Exchange),
			zap.String("routing_key", publishMethod.RoutingKey),
			zap.Bool("mandatory", publishMethod.Mandatory),
			zap.Bool("immediate", publishMethod.Immediate))
	}

	// Authorization check: basic.publish requires write permission on exchange
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceExchange,
		Resource:     publishMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 40)
	}

	// RabbitMQ-style memory alarm: Log warning when queue usage is high but DO NOT close connection
	// With 10GB threshold and BrokerV2 architecture, we prioritize connection stability over strict memory limits
	blocked := conn.Blocked.Load()

	if blocked {
		// Log warning but continue processing to maintain connection stability
		// RabbitMQ blocks publishers but keeps connections alive - we do the same
		s.Log.Warn("High memory usage detected but allowing publish to maintain connection stability",
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID),
			zap.String("exchange", publishMethod.Exchange))
		// Continue processing instead of returning error
	}

	// Create a pending message to track this publication
	// The full message will be completed when we receive the header and body frames
	var channelRef *protocol.Channel
	if value, ok := conn.Channels.Load(channelID); ok {
		channelRef = value.(*protocol.Channel)
	}
	pendingMsg := protocol.GetPendingMessage()
	pendingMsg.Method = publishMethod
	pendingMsg.Channel = channelRef

	// Store the pending message for this channel
	conn.PendingMessages[channelID] = pendingMsg

	if ce := s.Log.Check(zapcore.DebugLevel, "Started tracking pending message"); ce != nil {
		ce.Write(zap.Uint16("channel_id", channelID), zap.String("connection_id", conn.ID))
	}

	return nil
}

// processHeaderFrame processes content header frames
func (s *Server) processHeaderFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	// Check if there's a pending message for this channel that needs a header
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		// This could be a valid scenario - maybe not every header frame is part of a publish
		s.Log.Warn("Header frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Parse the content header
	contentHeader, err := protocol.ReadContentHeader(frame)
	if err != nil {
		s.Log.Error("Failed to parse content header",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel", frame.Channel))
		return err
	}

	// Attach the header to the pending message
	pendingMsg.Header = contentHeader

	// Defense-in-depth: reject oversized messages before allocation.
	// contentHeader.BodySize is a client-controlled uint64 — without this
	// check, make([]byte, 0, BodySize) can OOM the broker with a single frame.
	if s.Config != nil && contentHeader.BodySize > uint64(s.Config.Server.MaxMessageSize) {
		s.Log.Warn("Message body size exceeds MaxMessageSize, rejecting",
			zap.Uint64("body_size", contentHeader.BodySize),
			zap.Int64("max_message_size", s.Config.Server.MaxMessageSize),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", frame.Channel))
		s.sendChannelClose(conn, frame.Channel, amqperrors.ResourceError,
			fmt.Sprintf("message body size %d exceeds max %d", contentHeader.BodySize, s.Config.Server.MaxMessageSize),
			60, 0)
		return fmt.Errorf("message body size %d exceeds MaxMessageSize %d", contentHeader.BodySize, s.Config.Server.MaxMessageSize)
	}

	// Pre-allocate body slice to BodySize capacity to eliminate reallocations
	// during body frame append (saves ~4.6GB allocations at high throughput)
	if contentHeader.BodySize > 0 && cap(pendingMsg.Body) < int(contentHeader.BodySize) {
		pendingMsg.Body = make([]byte, 0, contentHeader.BodySize)
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Content header received for pending message"); ce != nil {
		ce.Write(
			zap.String("exchange", pendingMsg.Method.Exchange),
			zap.String("routing_key", pendingMsg.Method.RoutingKey),
			zap.Uint64("body_size", contentHeader.BodySize))
	}

	// For empty messages (body_size = 0), no body frame will be sent
	// We should process the complete message immediately
	if contentHeader.BodySize == 0 {
		if ce := s.Log.Check(zapcore.DebugLevel, "Empty message detected - processing immediately"); ce != nil {
			ce.Write(zap.String("exchange", pendingMsg.Method.Exchange), zap.String("routing_key", pendingMsg.Method.RoutingKey))
		}

		delete(conn.PendingMessages, frame.Channel)

		err := s.processCompleteMessage(conn, frame.Channel, pendingMsg)
		protocol.PutBasicPublishMethod(pendingMsg.Method)
		protocol.PutContentHeader(pendingMsg.Header)
		protocol.PutPendingMessage(pendingMsg)
		if err != nil {
			s.Log.Error("Failed to process complete empty message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}
	}

	return nil
}

// processBodyFrame processes content body frames
func (s *Server) processBodyFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	// Check if there's a pending message for this channel that needs body content
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		s.Log.Warn("Body frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Check if we have a header for this message
	if pendingMsg.Header == nil {
		s.Log.Warn("Body frame received before header frame",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("body frame received before header frame")
	}

	// Append the body content to the pending message
	pendingMsg.Body = append(pendingMsg.Body, frame.Payload...)
	pendingMsg.Received += uint64(len(frame.Payload))

	if ce := s.Log.Check(zapcore.DebugLevel, "Body frame received for pending message"); ce != nil {
		ce.Write(zap.Uint16("channel", frame.Channel),
			zap.Uint64("received", pendingMsg.Received),
			zap.Uint64("expected", pendingMsg.Header.BodySize))
	}

	// Check if we've received the complete message
	if pendingMsg.Received >= pendingMsg.Header.BodySize {
		// We have received all the body data
		if pendingMsg.Received > pendingMsg.Header.BodySize {
			s.Log.Error("Received more body data than expected",
				zap.Uint64("received", pendingMsg.Received),
				zap.Uint64("expected", pendingMsg.Header.BodySize))
			return fmt.Errorf("received more body data than expected")
		}

		// Finalize the message by trimming any extra bytes
		if pendingMsg.Received > uint64(len(pendingMsg.Body)) {
			// This shouldn't happen, but just in case
			pendingMsg.Body = pendingMsg.Body[:pendingMsg.Header.BodySize]
		} else if pendingMsg.Received < uint64(len(pendingMsg.Body)) {
			pendingMsg.Body = pendingMsg.Body[:pendingMsg.Received]
		}

		// Remove the pending message from the map before processing
		delete(conn.PendingMessages, frame.Channel)

		err := s.processCompleteMessage(conn, frame.Channel, pendingMsg)
		protocol.PutBasicPublishMethod(pendingMsg.Method)
		protocol.PutContentHeader(pendingMsg.Header)
		protocol.PutPendingMessage(pendingMsg)
		if err != nil {
			s.Log.Error("Failed to process complete message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}

		return nil
	}

	return nil
}

// processCompleteMessage processes a message that has been fully received (method + header + body)
func (s *Server) processCompleteMessage(conn *protocol.Connection, channelID uint16, pendingMsg *protocol.PendingMessage) error {
	if ce := s.Log.Check(zapcore.DebugLevel, "Processing complete message"); ce != nil {
		ce.Write(zap.String("exchange", pendingMsg.Method.Exchange),
			zap.String("routing_key", pendingMsg.Method.RoutingKey),
			zap.Uint64("body_size", uint64(len(pendingMsg.Body))))
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Creating message from pending message"); ce != nil {
		ce.Write(zap.String("content_type", pendingMsg.Header.ContentType),
			zap.Int("body_size", len(pendingMsg.Body)),
			zap.Uint16("property_flags", pendingMsg.Header.PropertyFlags))
	}

	// ALIASING INVARIANT: Message.Body and Message.Headers are zero-copy
	// aliases to pendingMsg.Body and pendingMsg.Header.Headers respectively.
	// After PublishMessage stores the message, the caller (processHeaderFrame
	// or processBodyFrame) recycles the PendingMessage and ContentHeader via
	// PutPendingMessage / PutContentHeader. Those pool Put helpers MUST only
	// nil out the references — they MUST NOT recycle the backing []byte or
	// map storage — because the stored delivery retains these aliases.
	// Violating this invariant would corrupt every stored message body and
	// header map on the next pool Get.
	message := &protocol.Message{
		Body:            pendingMsg.Body,
		Headers:         pendingMsg.Header.Headers,
		Exchange:        pendingMsg.Method.Exchange,
		RoutingKey:      pendingMsg.Method.RoutingKey,
		ContentType:     pendingMsg.Header.ContentType,
		ContentEncoding: pendingMsg.Header.ContentEncoding,
		DeliveryMode:    pendingMsg.Header.DeliveryMode,
		Priority:        pendingMsg.Header.Priority,
		CorrelationID:   pendingMsg.Header.CorrelationID,
		ReplyTo:         pendingMsg.Header.ReplyTo,
		Expiration:      pendingMsg.Header.Expiration,
		MessageID:       pendingMsg.Header.MessageID,
		Timestamp:       pendingMsg.Header.Timestamp,
		Type:            pendingMsg.Header.Type,
		UserID:          pendingMsg.Header.UserID,
		AppID:           pendingMsg.Header.AppID,
		ClusterID:       pendingMsg.Header.ClusterID,
		Mandatory:       pendingMsg.Method.Mandatory,
	}

	if message.UserID != "" && message.UserID != conn.Username {
		s.sendChannelClose(conn, channelID, amqperrors.PreconditionFailed,
			fmt.Sprintf("user_id '%s' does not match authenticated user '%s'", message.UserID, conn.Username), 60, 40)
		return fmt.Errorf("user-id mismatch")
	}

	// Assign confirm delivery tag if channel is in confirm mode (before
	// transactional check so the tag is sequential across all publishes).
	var confirmTag uint64
	if val, ok := conn.Channels.Load(channelID); ok {
		ch := val.(*protocol.Channel)
		if ch.ConfirmMode.Load() {
			confirmTag = ch.ConfirmSequence.Add(1)
		}
	}

	// Buffer into transaction if channel is in transactional mode
	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		op := transaction.NewPublishOperation(message.Exchange, message.RoutingKey, message)
		if confirmTag > 0 {
			op.DeliveryTag = confirmTag
		}
		return s.TransactionManager.AddOperation(channelID, op)
	}

	var publishStart time.Time
	if s.MetricsCollector != nil {
		publishStart = time.Now()
	}

	err := s.Broker.PublishMessage(message.Exchange, message.RoutingKey, message)

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordPublishLatency(time.Since(publishStart).Seconds())
	}

	if err != nil {
		if errors.Is(err, broker.ErrNoRoute) {
			if s.MetricsCollector != nil {
				s.MetricsCollector.RecordMessageUnroutable()
			}
			if retErr := s.sendBasicReturn(conn, channelID, 312, "NO_ROUTE", message.Exchange, message.RoutingKey, message); retErr != nil {
				return retErr
			}
			if confirmTag > 0 {
				return s.sendBasicAck(conn, channelID, confirmTag, false)
			}
			return nil
		}
		s.Log.Error("Failed to route message",
			zap.Error(err),
			zap.String("exchange", message.Exchange),
			zap.String("routing_key", message.RoutingKey))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessagePublished(len(message.Body))
	}

	s.messagesPublished.Add(1)
	s.bytesReceived.Add(int64(len(message.Body)))

	if ce := s.Log.Check(zapcore.DebugLevel, "Message successfully routed"); ce != nil {
		ce.Write(zap.String("exchange", message.Exchange), zap.String("routing_key", message.RoutingKey))
	}

	if confirmTag > 0 {
		return s.sendBasicAck(conn, channelID, confirmTag, false)
	}

	return nil
}

// handleBasicConsume handles the basic.consume method
func (s *Server) handleBasicConsume(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.consume method
	consumeMethod := &protocol.BasicConsumeMethod{}
	err := consumeMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.consume",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	// Get the channel
	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}
	channel := value.(*protocol.Channel)

	s.Log.Debug("Basic consume requested",
		zap.String("queue", consumeMethod.Queue),
		zap.String("consumer_tag", consumeMethod.ConsumerTag),
		zap.Bool("no_ack", consumeMethod.NoAck),
		zap.Bool("exclusive", consumeMethod.Exclusive),
		zap.Uint16("prefetch_count", channel.PrefetchCount))

	// Authorization check: basic.consume requires read permission on queue
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceQueue,
		Resource:     consumeMethod.Queue,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 20)
	}

	// Check if queue exists in the broker
	// This is a simplified check - in a real implementation you'd verify queue exists
	// For now, we'll proceed assuming the queue exists

	// FIXED BUFFER SIZE: Consumer.Messages channel is for TCP buffering only
	// Prefetch flow control is now handled by semaphore in broker layer
	// Small fixed buffer provides TCP/transport buffering without memory bloat
	// Blocking send provides natural backpressure to broker when consumer is slow
	bufferSize := 100 // Fixed size for all consumers

	s.Log.Debug("Creating consumer with bounded channel",
		zap.String("consumer_tag", consumeMethod.ConsumerTag),
		zap.Uint16("prefetch_count", channel.PrefetchCount),
		zap.Int("buffer_size", bufferSize))

	// Create a new consumer
	consumer := &protocol.Consumer{
		Tag:           consumeMethod.ConsumerTag,
		Channel:       channel,
		Queue:         consumeMethod.Queue,
		NoAck:         consumeMethod.NoAck,
		Exclusive:     consumeMethod.Exclusive,
		Args:          consumeMethod.Arguments,
		PrefetchCount: channel.PrefetchCount, // CRITICAL FIX: Must set prefetch for consumer poll loop

		Messages: make(chan *protocol.Delivery, bufferSize), // BOUNDED: Buffer based on prefetch + margin
		Cancel:   make(chan struct{}, 1),                    // Channel to signal cancellation
	}

	// Add the consumer to the channel
	channel.Mutex.Lock()
	channel.Consumers[consumer.Tag] = consumer
	channel.Mutex.Unlock()
	conn.ConsumersDirty.Store(true)

	// Register the consumer with the broker
	err = s.Broker.RegisterConsumer(consumeMethod.Queue, consumer.Tag, consumer)
	if err != nil {
		s.Log.Error("Failed to register consumer with broker",
			zap.Error(err),
			zap.String("consumer_tag", consumer.Tag),
			zap.String("queue", consumeMethod.Queue))
		return err
	}

	// Send basic.consume-ok response
	err = s.sendBasicConsumeOK(conn, channelID, consumer.Tag)
	if err != nil {
		s.Log.Error("Failed to send basic.consume-ok",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.updateConsumersTotal()

	s.Log.Debug("Consumer registered",
		zap.String("consumer_tag", consumer.Tag),
		zap.String("queue", consumer.Queue))

	return nil
}

// handleBasicCancel handles the basic.cancel method
func (s *Server) handleBasicCancel(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.cancel method
	cancelMethod := &protocol.BasicCancelMethod{}
	err := cancelMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.cancel",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Basic cancel requested",
		zap.String("consumer_tag", cancelMethod.ConsumerTag))

	// Get the channel
	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}
	channel := value.(*protocol.Channel)

	// Remove the consumer
	channel.Mutex.Lock()
	consumer, exists := channel.Consumers[cancelMethod.ConsumerTag]
	if !exists {
		channel.Mutex.Unlock()
		// Consumer doesn't exist, but this might be OK depending on the spec
		s.Log.Warn("Attempted to cancel non-existent consumer",
			zap.String("consumer_tag", cancelMethod.ConsumerTag))
		return nil
	}

	// Close the consumer's message channel and signal cancellation
	close(consumer.Cancel)
	delete(channel.Consumers, cancelMethod.ConsumerTag)
	channel.Mutex.Unlock()
	conn.ConsumersDirty.Store(true)

	// Unregister the consumer with the broker
	err = s.Broker.UnregisterConsumer(cancelMethod.ConsumerTag)
	if err != nil {
		s.Log.Error("Failed to unregister consumer with broker",
			zap.Error(err),
			zap.String("consumer_tag", cancelMethod.ConsumerTag))
		// Continue anyway since we've already removed it locally
	}

	// Send basic.cancel-ok response
	if !cancelMethod.NoWait {
		err = s.sendBasicCancelOK(conn, channelID, cancelMethod.ConsumerTag)
		if err != nil {
			s.Log.Error("Failed to send basic.cancel-ok",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel_id", channelID))
			return err
		}
	}

	s.Log.Debug("Consumer cancelled",
		zap.String("consumer_tag", cancelMethod.ConsumerTag))

	s.updateConsumersTotal()

	return nil
}

// handleBasicGet handles the basic.get method
func (s *Server) handleBasicGet(conn *protocol.Connection, channelID uint16, payload []byte) error {
	getMethod := &protocol.BasicGetMethod{}
	err := getMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.get",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Basic get requested"); ce != nil {
		ce.Write(
			zap.String("queue", getMethod.Queue),
			zap.Bool("no_ack", getMethod.NoAck))
	}

	queueName, err := resolveQueueName(conn, channelID, getMethod.Queue)
	if err != nil {
		return err
	}

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 70)
	}

	message, deliveryTag, messageCount, err := s.Broker.GetMessageForGet(queueName, getMethod.NoAck)
	if err != nil {
		s.Log.Error("Failed to get message for basic.get",
			zap.Error(err),
			zap.String("queue", queueName),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	if message == nil {
		return s.sendBasicGetEmpty(conn, channelID)
	}

	// SQ-18: translate the broker msgID into a per-channel monotonic wire
	// delivery tag so basic.get deliveries share the channel's tag space with
	// basic.deliver (and cumulative acks over a mixed stream stay coherent).
	// Manual-ack gets are tracked so their ack/nack/reject can be routed back to
	// the msgID-keyed broker get-delivery ledger; no-ack gets are never acked.
	wireTag := deliveryTag
	if v, ok := conn.Channels.Load(channelID); ok {
		channel := v.(*protocol.Channel)
		wireTag = channel.NextWireTag()
		if !getMethod.NoAck {
			channel.TrackDelivery(wireTag, deliveryTag, "", true)
		}
	}

	const maxPoolSize = 131 * 1024
	estimatedSize := deliveryOverheadEstimate + len(message.Body)
	var buf *[]byte
	if estimatedSize > maxPoolSize {
		b := make([]byte, 0, estimatedSize)
		buf = &b
	} else {
		buf = protocol.GetBufferForSize(estimatedSize)
	}
	defer func() {
		if cap(*buf) <= maxPoolSize {
			protocol.PutBufferForSize(buf)
		}
	}()

	if err := s.serializeGetDeliveryInto(buf, channelID, wireTag, message.Redelivered, message.Exchange, message.RoutingKey, messageCount, message); err != nil {
		s.Log.Error("Failed to serialize basic.get-ok delivery",
			zap.Error(err),
			zap.Uint16("channel_id", channelID))
		return err
	}

	conn.WriteMutex.Lock()
	_, err = conn.Conn.Write(*buf)
	conn.WriteMutex.Unlock()

	if err != nil {
		s.Log.Error("Error sending basic.get-ok frames",
			zap.Error(err),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("error sending basic.get-ok frames: %v", err)
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageDelivered(len(message.Body))
	}

	s.messagesDelivered.Add(1)
	s.bytesSent.Add(int64(len(message.Body)))

	return nil
}

// serializeDeliveryInto serializes a basic.deliver (method frame + header
// frame + body frames) directly into the provided buffer, avoiding all
// intermediate *Frame, *BasicDeliverMethod, and *ContentHeader allocations.
// This is the zero-alloc delivery serializer for hot paths.
func (s *Server) serializeDeliveryInto(buf *[]byte, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	// --- Method frame (basic.deliver) ---
	// class(2) + method(2) + consumerTag(1+N) + deliveryTag(8) + redelivered(1) + exchange(1+N) + routingKey(1+N)
	var payload []byte
	payload = binary.BigEndian.AppendUint16(payload, 60) // class ID
	payload = binary.BigEndian.AppendUint16(payload, 60) // method ID
	payload = protocol.AppendShortString(payload, consumerTag)
	payload = binary.BigEndian.AppendUint64(payload, deliveryTag)
	if redelivered {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	payload = protocol.AppendShortString(payload, exchange)
	payload = protocol.AppendShortString(payload, routingKey)
	*buf = protocol.AppendFrame(*buf, protocol.FrameMethod, channelID, payload)

	return s.appendHeaderAndBodyFrames(buf, channelID, message)
}

// serializeGetDeliveryInto serializes a basic.get-ok (method frame + header
// frame + body frames) directly into the provided buffer, avoiding all
// intermediate allocations. This is the synchronous-response counterpart to
// serializeDeliveryInto, using basic.get-ok (60.71) instead of basic.deliver
// (60.60) and including the message-count field.
func (s *Server) serializeGetDeliveryInto(buf *[]byte, channelID uint16, deliveryTag uint64, redelivered bool, exchange, routingKey string, messageCount uint32, message *protocol.Message) error {
	// --- Method frame (basic.get-ok) ---
	var payload []byte
	payload = binary.BigEndian.AppendUint16(payload, 60) // class ID
	payload = binary.BigEndian.AppendUint16(payload, 71) // method ID (basic.get-ok)
	payload = binary.BigEndian.AppendUint64(payload, deliveryTag)
	if redelivered {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	payload = protocol.AppendShortString(payload, exchange)
	payload = protocol.AppendShortString(payload, routingKey)
	payload = binary.BigEndian.AppendUint32(payload, messageCount)
	*buf = protocol.AppendFrame(*buf, protocol.FrameMethod, channelID, payload)

	return s.appendHeaderAndBodyFrames(buf, channelID, message)
}

// sendBasicDeliver sends a basic.deliver method frame to a consumer.
// Used for single-delivery paths (basic.get-ok, non-batched deliveries).
func (s *Server) sendBasicDeliver(conn *protocol.Connection, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	startTime := time.Now()
	defer func() {
		if s.MetricsCollector != nil {
			duration := time.Since(startTime).Seconds()
			s.MetricsCollector.RecordDeliveryLatency(duration)
		}
	}()

	const maxPoolSize = 131 * 1024
	estimatedSize := deliveryOverheadEstimate + len(message.Body)
	var buf *[]byte
	if estimatedSize > maxPoolSize {
		b := make([]byte, 0, estimatedSize)
		buf = &b
	} else {
		buf = protocol.GetBufferForSize(estimatedSize)
	}
	defer func() {
		if cap(*buf) <= maxPoolSize {
			protocol.PutBufferForSize(buf)
		}
	}()

	if err := s.serializeDeliveryInto(buf, channelID, consumerTag, deliveryTag, redelivered, exchange, routingKey, message); err != nil {
		s.Log.Error("Failed to serialize delivery",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint16("channel_id", channelID))
		return err
	}

	conn.WriteMutex.Lock()
	_, err := conn.Conn.Write(*buf)
	conn.WriteMutex.Unlock()

	if err != nil {
		s.Log.Error("Error sending delivery frames",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("error sending frames: %v", err)
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageDelivered(len(message.Body))
	}

	s.messagesDelivered.Add(1)
	s.bytesSent.Add(int64(len(message.Body)))

	return nil
}

// AMQP 0.9.1 packs the trailing bit arguments of basic.ack / basic.reject /
// basic.nack into a SINGLE OCTET at payload[8], so a spec client's payload is
// exactly 9 bytes. Reading two bytes here (a former uint16 decode) can never
// match that framing: basic.ack "multiple" always parsed false — silently
// turning every client multi-ack into a single ack until the prefetch window
// filled and delivery wedged (BenchmarkVersus_MultiAck1000) — and reject/nack
// "requeue" always parsed as the default. An 8-byte payload (no flag octet) is
// tolerated for wire-compat with the pre-fix encoder and keeps each method's
// spec defaults.

func parseBasicAckArgs(payload []byte) (deliveryTag uint64, multiple bool, err error) {
	if len(payload) < 8 {
		return 0, false, fmt.Errorf("basic.ack payload too short: %d bytes", len(payload))
	}
	deliveryTag = binary.BigEndian.Uint64(payload[:8])
	if len(payload) >= 9 {
		multiple = payload[8]&0x01 != 0
	}
	return deliveryTag, multiple, nil
}

func parseBasicRejectArgs(payload []byte) (deliveryTag uint64, requeue bool, err error) {
	if len(payload) < 8 {
		return 0, false, fmt.Errorf("basic.reject payload too short: %d bytes", len(payload))
	}
	deliveryTag = binary.BigEndian.Uint64(payload[:8])
	requeue = true // spec default when the flag octet is absent
	if len(payload) >= 9 {
		requeue = payload[8]&0x01 != 0
	}
	return deliveryTag, requeue, nil
}

func parseBasicNackArgs(payload []byte) (deliveryTag uint64, multiple, requeue bool, err error) {
	if len(payload) < 8 {
		return 0, false, false, fmt.Errorf("basic.nack payload too short: %d bytes", len(payload))
	}
	deliveryTag = binary.BigEndian.Uint64(payload[:8])
	requeue = true // spec default when the flag octet is absent
	if len(payload) >= 9 {
		multiple = payload[8]&0x01 != 0
		requeue = payload[8]&0x02 != 0
	}
	return deliveryTag, multiple, requeue, nil
}

// wireChannel returns the channel for channelID, or nil when it is not
// registered. Real connections always register a channel via channel.open
// before any delivery; a nil result (some unit tests) selects the legacy
// wire-tag == msgID fallback in resolveDeliveryTag.
func (s *Server) wireChannel(conn *protocol.Connection, channelID uint16) *protocol.Channel {
	if v, ok := conn.Channels.Load(channelID); ok {
		return v.(*protocol.Channel)
	}
	return nil
}

// resolveDeliveryTag (SQ-18) translates a wire delivery tag received on
// basic.ack/nack/reject into the broker identity behind it and removes the
// tracking entry (the tag is about to be settled). With a channel present the
// per-channel wire-tag table is authoritative; without one it falls back to
// treating the wire tag as the broker msgID (legacy path). known=false means
// the tag is unknown or already settled (duplicate/no-ack/stale) and the caller
// must no-op, matching AMQP's tolerant handling of stale acks.
func (s *Server) resolveDeliveryTag(channel *protocol.Channel, wireTag uint64) (msgID uint64, consumerTag string, isGet, known bool) {
	if channel != nil {
		ref, ok := channel.TakeWireTag(wireTag)
		if !ok {
			return 0, "", false, false
		}
		return ref.MsgID, ref.ConsumerTag, ref.IsGet, true
	}
	ctag, ok := s.Broker.GetConsumerForDelivery(wireTag)
	if !ok {
		return 0, "", false, false
	}
	return wireTag, ctag, ctag == "", true
}

// handleBasicAck handles the basic.ack method
func (s *Server) handleBasicAck(conn *protocol.Connection, channelID uint16, payload []byte) error {
	wireTag, multiple, err := parseBasicAckArgs(payload)
	if err != nil {
		return err
	}
	channel := s.wireChannel(conn, channelID)
	tx := s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID)

	// Non-transactional cumulative ack spans every consumer on the channel
	// (SQ-18): the client's "all tags <= N" may cover deliveries that came from
	// different queues, so it cannot be routed to a single consumer's broker
	// multi-ack. The per-channel wire-tag table is the channel-wide unacked set.
	if multiple && !tx {
		s.cumulativeSettle(channel, wireTag, settleAck, false)
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordMessageAcknowledged()
		}
		return nil
	}

	msgID, consumerTag, isGet, known := s.resolveDeliveryTag(channel, wireTag)
	if !known {
		s.Log.Debug("Delivery tag already processed (consumer cancelled?)",
			zap.Uint64("delivery_tag", wireTag),
			zap.Uint16("channel_id", channelID))
		return nil
	}

	if tx {
		op := transaction.NewAckOperation(consumerTag, msgID, multiple)
		return s.TransactionManager.AddOperation(channelID, op)
	}

	if isGet {
		if err := s.Broker.AcknowledgeGetDelivery(msgID); err != nil {
			return err
		}
	} else if err := s.Broker.AcknowledgeMessage(consumerTag, msgID, false); err != nil {
		s.Log.Error("Failed to acknowledge message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("msg_id", msgID))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageAcknowledged()
	}

	return nil
}

// handleBasicReject handles the basic.reject method
func (s *Server) handleBasicReject(conn *protocol.Connection, channelID uint16, payload []byte) error {
	wireTag, requeue, err := parseBasicRejectArgs(payload)
	if err != nil {
		return err
	}
	channel := s.wireChannel(conn, channelID)

	msgID, consumerTag, isGet, known := s.resolveDeliveryTag(channel, wireTag)
	if !known {
		s.Log.Debug("Delivery tag already processed (consumer cancelled?)",
			zap.Uint64("delivery_tag", wireTag),
			zap.Uint16("channel_id", channelID))
		return nil
	}

	// Buffer into transaction if channel is in transactional mode
	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		op := transaction.NewRejectOperation(consumerTag, msgID, requeue)
		return s.TransactionManager.AddOperation(channelID, op)
	}

	if isGet {
		if err := s.Broker.RejectGetDelivery(msgID, requeue); err != nil {
			return err
		}
	} else if err := s.Broker.RejectMessage(consumerTag, msgID, requeue); err != nil {
		s.Log.Error("Failed to reject message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("msg_id", msgID),
			zap.Bool("requeue", requeue))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageRejected()
	}

	return nil
}

// handleBasicNack handles the basic.nack method
func (s *Server) handleBasicNack(conn *protocol.Connection, channelID uint16, payload []byte) error {
	wireTag, multiple, requeue, err := parseBasicNackArgs(payload)
	if err != nil {
		return err
	}
	channel := s.wireChannel(conn, channelID)
	tx := s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID)

	// Non-transactional cumulative nack spans every consumer on the channel
	// (SQ-18), mirroring cumulative ack.
	if multiple && !tx {
		s.cumulativeSettle(channel, wireTag, settleNack, requeue)
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordMessageRejected()
		}
		return nil
	}

	msgID, consumerTag, isGet, known := s.resolveDeliveryTag(channel, wireTag)
	if !known {
		s.Log.Debug("Delivery tag already processed (consumer cancelled?)",
			zap.Uint64("delivery_tag", wireTag),
			zap.Uint16("channel_id", channelID))
		return nil
	}

	if tx {
		op := transaction.NewNackOperation(consumerTag, msgID, multiple, requeue)
		return s.TransactionManager.AddOperation(channelID, op)
	}

	if isGet {
		if err := s.Broker.NackGetDelivery(msgID, requeue); err != nil {
			return err
		}
	} else if err := s.Broker.NacknowledgeMessage(consumerTag, msgID, false, requeue); err != nil {
		s.Log.Error("Failed to nack message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("msg_id", msgID),
			zap.Bool("requeue", requeue))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageRejected()
	}

	return nil
}

// settleKind selects the terminal transition applied by cumulativeSettle.
type settleKind int

const (
	settleAck settleKind = iota
	settleNack
)

// cumulativeSettle applies a cumulative (multiple=true) ack or nack to every
// still-outstanding wire tag <= wireTag on the channel — across ALL consumers
// and any tracked basic.get deliveries (SQ-18). Each tag is settled through the
// broker's single-message path (its own settle() LoadAndDelete linearization
// point), so credit accounting stays exact and requeue-reused msgIDs cannot be
// over-settled. When channel is nil (legacy tests) it falls back to routing the
// wire tag (== msgID) to the owning consumer's broker multi-ack.
func (s *Server) cumulativeSettle(channel *protocol.Channel, wireTag uint64, kind settleKind, requeue bool) {
	// Spec: delivery-tag 0 with multiple=true means "every outstanding delivery
	// on the channel". Wire tags are 1-based, so 0 would otherwise match nothing
	// and ack-all / nack-all would silently no-op.
	if wireTag == 0 {
		wireTag = math.MaxUint64
	}
	if channel == nil {
		ctag, ok := s.Broker.GetConsumerForDelivery(wireTag)
		if !ok {
			return
		}
		switch {
		case ctag == "" && kind == settleAck:
			s.Broker.AcknowledgeGetDelivery(wireTag)
		case ctag == "":
			s.Broker.NackGetDelivery(wireTag, requeue)
		case kind == settleAck:
			s.Broker.AcknowledgeMessage(ctag, wireTag, true)
		default:
			s.Broker.NacknowledgeMessage(ctag, wireTag, true, requeue)
		}
		return
	}

	refs := channel.TakeWireTagsUpTo(wireTag)
	for _, ref := range refs {
		switch {
		case ref.IsGet && kind == settleAck:
			s.Broker.AcknowledgeGetDelivery(ref.MsgID)
		case ref.IsGet:
			s.Broker.NackGetDelivery(ref.MsgID, requeue)
		case kind == settleAck:
			s.Broker.AcknowledgeMessage(ref.ConsumerTag, ref.MsgID, false)
		default:
			s.Broker.NacknowledgeMessage(ref.ConsumerTag, ref.MsgID, false, requeue)
		}
	}
}

func (s *Server) handleBasicRecover(conn *protocol.Connection, channelID uint16, payload []byte) error {
	recoverMethod := &protocol.BasicRecoverMethod{}
	if err := recoverMethod.Deserialize(payload); err != nil {
		s.Log.Error("Failed to deserialize basic.recover",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	// requeue field is deserialized for wire-format correctness but not
	// acted on — RabbitMQ also ignores requeue=false and always requeues
	// to the shared queue. Redelivery to the same consumer is not supported.
	_ = recoverMethod.Requeue

	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return fmt.Errorf("channel %d does not exist", channelID)
	}
	channel := value.(*protocol.Channel)

	channel.Mutex.RLock()
	consumerTags := make([]string, 0, len(channel.Consumers))
	for tag := range channel.Consumers {
		consumerTags = append(consumerTags, tag)
	}
	channel.Mutex.RUnlock()

	for _, tag := range consumerTags {
		if err := s.Broker.RequeueAllForConsumer(tag); err != nil {
			s.Log.Error("Failed to requeue messages for consumer",
				zap.Error(err),
				zap.String("consumer_tag", tag))
		}
	}

	return s.sendBasicRecoverOK(conn, channelID)
}

func (s *Server) updateConsumersTotal() {
	if s.MetricsCollector != nil && s.Broker != nil {
		s.MetricsCollector.SetConsumersTotal(len(s.Broker.GetConsumers()))
	}
}

// Stop gracefully stops the server
