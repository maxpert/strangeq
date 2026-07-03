package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/broker"
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

	// Get the channel
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
	channel.Mutex.Unlock()

	// If global is true, we would apply these settings to all channels
	// For now, we'll just apply to this channel

	// Send basic.qos-ok response
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

	// Buffer into transaction if channel is in transactional mode
	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		op := transaction.NewPublishOperation(message.Exchange, message.RoutingKey, message)
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
			return s.sendBasicReturn(conn, channelID, 312, "NO_ROUTE", message.Exchange, message.RoutingKey, message)
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

	if ce := s.Log.Check(zapcore.DebugLevel, "Message successfully routed"); ce != nil {
		ce.Write(zap.String("exchange", message.Exchange), zap.String("routing_key", message.RoutingKey))
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

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceQueue,
		Resource:     getMethod.Queue,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 70)
	}

	message, deliveryTag, messageCount, err := s.Broker.GetMessageForGet(getMethod.Queue, getMethod.NoAck)
	if err != nil {
		s.Log.Error("Failed to get message for basic.get",
			zap.Error(err),
			zap.String("queue", getMethod.Queue),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	if message == nil {
		return s.sendBasicGetEmpty(conn, channelID)
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

	if err := s.serializeGetDeliveryInto(buf, channelID, deliveryTag, false, message.Exchange, message.RoutingKey, messageCount, message); err != nil {
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

	// --- Header frame (content header) ---
	propertyFlags := buildPropertyFlags(message)
	var hdrPayload []byte
	hdrPayload, err := (&protocol.ContentHeader{
		ClassID:         60,
		Weight:          0,
		BodySize:        uint64(len(message.Body)),
		PropertyFlags:   propertyFlags,
		Headers:         message.Headers,
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    message.DeliveryMode,
		Priority:        message.Priority,
		CorrelationID:   message.CorrelationID,
		ReplyTo:         message.ReplyTo,
		Expiration:      message.Expiration,
		MessageID:       message.MessageID,
		Timestamp:       message.Timestamp,
		Type:            message.Type,
		UserID:          message.UserID,
		AppID:           message.AppID,
		ClusterID:       message.ClusterID,
	}).SerializeInto(hdrPayload)
	if err != nil {
		return fmt.Errorf("error serializing content header: %v", err)
	}
	*buf = protocol.AppendFrame(*buf, protocol.FrameHeader, channelID, hdrPayload)

	// --- Body frames ---
	maxFrameSize := uint32(s.Config.Server.MaxFrameSize)
	maxBodyPerFrame := int(maxFrameSize) - 8
	if maxBodyPerFrame <= 0 {
		maxBodyPerFrame = 4096
	}
	for offset := 0; offset < len(message.Body); offset += maxBodyPerFrame {
		end := offset + maxBodyPerFrame
		if end > len(message.Body) {
			end = len(message.Body)
		}
		*buf = protocol.AppendFrame(*buf, protocol.FrameBody, channelID, message.Body[offset:end])
	}

	return nil
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

	// --- Header frame (content header) ---
	propertyFlags := buildPropertyFlags(message)
	var hdrPayload []byte
	hdrPayload, err := (&protocol.ContentHeader{
		ClassID:         60,
		Weight:          0,
		BodySize:        uint64(len(message.Body)),
		PropertyFlags:   propertyFlags,
		Headers:         message.Headers,
		ContentType:     message.ContentType,
		ContentEncoding: message.ContentEncoding,
		DeliveryMode:    message.DeliveryMode,
		Priority:        message.Priority,
		CorrelationID:   message.CorrelationID,
		ReplyTo:         message.ReplyTo,
		Expiration:      message.Expiration,
		MessageID:       message.MessageID,
		Timestamp:       message.Timestamp,
		Type:            message.Type,
		UserID:          message.UserID,
		AppID:           message.AppID,
		ClusterID:       message.ClusterID,
	}).SerializeInto(hdrPayload)
	if err != nil {
		return fmt.Errorf("error serializing content header: %v", err)
	}
	*buf = protocol.AppendFrame(*buf, protocol.FrameHeader, channelID, hdrPayload)

	// --- Body frames ---
	maxFrameSize := uint32(s.Config.Server.MaxFrameSize)
	maxBodyPerFrame := int(maxFrameSize) - 8
	if maxBodyPerFrame <= 0 {
		maxBodyPerFrame = 4096
	}
	for offset := 0; offset < len(message.Body); offset += maxBodyPerFrame {
		end := offset + maxBodyPerFrame
		if end > len(message.Body) {
			end = len(message.Body)
		}
		*buf = protocol.AppendFrame(*buf, protocol.FrameBody, channelID, message.Body[offset:end])
	}

	return nil
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

	return nil
}

// handleBasicAck handles the basic.ack method
func (s *Server) handleBasicAck(conn *protocol.Connection, channelID uint16, payload []byte) error {
	if len(payload) < 8 {
		return fmt.Errorf("basic.ack payload too short: %d bytes", len(payload))
	}
	deliveryTag := binary.BigEndian.Uint64(payload[:8])
	var multiple bool
	if len(payload) >= 10 {
		flags := binary.BigEndian.Uint16(payload[8:10])
		multiple = (flags & (1 << 0)) != 0
	}

	consumerTag, ok := s.Broker.GetConsumerForDelivery(deliveryTag)
	if !ok {
		s.Log.Debug("Delivery tag already processed (consumer cancelled?)",
			zap.Uint64("delivery_tag", deliveryTag),
			zap.Uint16("channel_id", channelID))
		return nil
	}

	// Buffer into transaction if channel is in transactional mode
	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		op := transaction.NewAckOperation(consumerTag, deliveryTag, multiple)
		return s.TransactionManager.AddOperation(channelID, op)
	}

	if consumerTag == "" {
		return s.Broker.AcknowledgeGetDelivery(deliveryTag)
	}

	err := s.Broker.AcknowledgeMessage(consumerTag, deliveryTag, multiple)
	if err != nil {
		s.Log.Error("Failed to acknowledge message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", deliveryTag),
			zap.Bool("multiple", multiple))
		return err
	}

	return nil
}

// handleBasicReject handles the basic.reject method
func (s *Server) handleBasicReject(conn *protocol.Connection, channelID uint16, payload []byte) error {
	if len(payload) < 8 {
		return fmt.Errorf("basic.reject payload too short: %d bytes", len(payload))
	}
	deliveryTag := binary.BigEndian.Uint64(payload[:8])
	var requeue bool = true // default: requeue
	if len(payload) >= 10 {
		flags := binary.BigEndian.Uint16(payload[8:10])
		requeue = (flags & (1 << 0)) != 0
	}

	consumerTag, ok := s.Broker.GetConsumerForDelivery(deliveryTag)
	if !ok {
		s.Log.Debug("Delivery tag already processed (consumer cancelled?)",
			zap.Uint64("delivery_tag", deliveryTag),
			zap.Uint16("channel_id", channelID))
		return nil
	}

	// Buffer into transaction if channel is in transactional mode
	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		op := transaction.NewRejectOperation(consumerTag, deliveryTag, requeue)
		return s.TransactionManager.AddOperation(channelID, op)
	}

	if consumerTag == "" {
		return s.Broker.RejectGetDelivery(deliveryTag, requeue)
	}

	err := s.Broker.RejectMessage(consumerTag, deliveryTag, requeue)
	if err != nil {
		s.Log.Error("Failed to reject message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", deliveryTag),
			zap.Bool("requeue", requeue))
		return err
	}

	return nil
}

// handleBasicNack handles the basic.nack method
func (s *Server) handleBasicNack(conn *protocol.Connection, channelID uint16, payload []byte) error {
	if len(payload) < 8 {
		return fmt.Errorf("basic.nack payload too short: %d bytes", len(payload))
	}
	deliveryTag := binary.BigEndian.Uint64(payload[:8])
	var multiple, requeue bool
	requeue = true // default: requeue
	if len(payload) >= 10 {
		flags := binary.BigEndian.Uint16(payload[8:10])
		multiple = (flags & (1 << 0)) != 0
		requeue = (flags & (1 << 1)) != 0
	}

	consumerTag, ok := s.Broker.GetConsumerForDelivery(deliveryTag)
	if !ok {
		s.Log.Debug("Delivery tag already processed (consumer cancelled?)",
			zap.Uint64("delivery_tag", deliveryTag),
			zap.Uint16("channel_id", channelID))
		return nil
	}

	// Buffer into transaction if channel is in transactional mode
	if s.TransactionManager != nil && s.TransactionManager.IsTransactional(channelID) {
		op := transaction.NewNackOperation(consumerTag, deliveryTag, multiple, requeue)
		return s.TransactionManager.AddOperation(channelID, op)
	}

	if consumerTag == "" {
		return s.Broker.NackGetDelivery(deliveryTag, requeue)
	}

	err := s.Broker.NacknowledgeMessage(consumerTag, deliveryTag, multiple, requeue)
	if err != nil {
		s.Log.Error("Failed to nack message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", deliveryTag),
			zap.Bool("multiple", multiple),
			zap.Bool("requeue", requeue))
		return err
	}

	return nil
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

// Stop gracefully stops the server
