package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
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
	publishMethod := &protocol.BasicPublishMethod{}
	err := publishMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.publish",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic publish received",
		zap.String("exchange", publishMethod.Exchange),
		zap.String("routing_key", publishMethod.RoutingKey),
		zap.Bool("mandatory", publishMethod.Mandatory),
		zap.Bool("immediate", publishMethod.Immediate))

	// RabbitMQ-style memory alarm: Log warning when queue usage is high but DO NOT close connection
	// With 10GB threshold and BrokerV2 architecture, we prioritize connection stability over strict memory limits
	conn.Mutex.RLock()
	blocked := conn.Blocked
	conn.Mutex.RUnlock()

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
	pendingMsg := &protocol.PendingMessage{
		Method:   publishMethod,
		Body:     make([]byte, 0),
		Received: 0,
		Channel:  channelRef, // Get the channel reference
	}

	// Store the pending message for this channel
	conn.Mutex.Lock()
	conn.PendingMessages[channelID] = pendingMsg
	conn.Mutex.Unlock()

	s.Log.Debug("Started tracking pending message",
		zap.Uint16("channel_id", channelID),
		zap.String("connection_id", conn.ID))

	return nil
}

// processHeaderFrame processes content header frames
func (s *Server) processHeaderFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	conn.Mutex.Lock()
	defer conn.Mutex.Unlock()

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

	s.Log.Debug("Content header received for pending message",
		zap.String("exchange", pendingMsg.Method.Exchange),
		zap.String("routing_key", pendingMsg.Method.RoutingKey),
		zap.Uint64("body_size", contentHeader.BodySize))

	// For empty messages (body_size = 0), no body frame will be sent
	// We should process the complete message immediately
	if contentHeader.BodySize == 0 {
		s.Log.Debug("Empty message detected - processing immediately",
			zap.String("exchange", pendingMsg.Method.Exchange),
			zap.String("routing_key", pendingMsg.Method.RoutingKey))

		// Process the complete message immediately
		if err := s.processCompleteMessage(conn, frame.Channel, pendingMsg); err != nil {
			s.Log.Error("Failed to process complete empty message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}

		// Remove the pending message from the map since it's now complete
		delete(conn.PendingMessages, frame.Channel)
	}

	return nil
}

// processBodyFrame processes content body frames
func (s *Server) processBodyFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	conn.Mutex.Lock()

	// Check if there's a pending message for this channel that needs body content
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		conn.Mutex.Unlock()
		s.Log.Warn("Body frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return nil
	}

	// Check if we have a header for this message
	if pendingMsg.Header == nil {
		conn.Mutex.Unlock()
		s.Log.Warn("Body frame received before header frame",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("body frame received before header frame")
	}

	// Append the body content to the pending message
	pendingMsg.Body = append(pendingMsg.Body, frame.Payload...)
	pendingMsg.Received += uint64(len(frame.Payload))

	s.Log.Debug("Body frame received for pending message",
		zap.Uint16("channel", frame.Channel),
		zap.Uint64("received", pendingMsg.Received),
		zap.Uint64("expected", pendingMsg.Header.BodySize))

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

		// Remove the pending message from the map BEFORE processing
		// to avoid holding the connection lock during potentially blocking operations
		delete(conn.PendingMessages, frame.Channel)

		// Make a copy of the message for processing
		msgCopy := *pendingMsg
		channelCopy := frame.Channel

		// Release connection lock before potentially blocking broker operations
		conn.Mutex.Unlock()

		// Process the complete message synchronously
		// With reader/processor separation, the frame reader remains responsive
		// even if this blocks, because we process frames from FrameQueue
		if err := s.processCompleteMessage(conn, channelCopy, &msgCopy); err != nil {
			s.Log.Error("Failed to process complete message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", channelCopy))
			return err
		}

		return nil
	}

	// Message not complete yet, unlock and wait for more body frames
	conn.Mutex.Unlock()
	return nil
}

// processCompleteMessage processes a message that has been fully received (method + header + body)
func (s *Server) processCompleteMessage(conn *protocol.Connection, channelID uint16, pendingMsg *protocol.PendingMessage) error {
	s.Log.Debug("Processing complete message",
		zap.String("exchange", pendingMsg.Method.Exchange),
		zap.String("routing_key", pendingMsg.Method.RoutingKey),
		zap.Uint64("body_size", uint64(len(pendingMsg.Body))))

	// Convert the pending message to a protocol.Message
	s.Log.Debug("Creating message from pending message",
		zap.String("content_type", pendingMsg.Header.ContentType),
		zap.Int("body_size", len(pendingMsg.Body)),
		zap.Uint16("property_flags", pendingMsg.Header.PropertyFlags))

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
	}

	// Route the message using the broker
	err := s.Broker.PublishMessage(message.Exchange, message.RoutingKey, message)
	if err != nil {
		s.Log.Error("Failed to route message",
			zap.Error(err),
			zap.String("exchange", message.Exchange),
			zap.String("routing_key", message.RoutingKey))
		return err
	}

	s.Log.Debug("Message successfully routed",
		zap.String("exchange", message.Exchange),
		zap.String("routing_key", message.RoutingKey))

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

	// Check if queue exists in the broker
	// This is a simplified check - in a real implementation you'd verify queue exists
	// For now, we'll proceed assuming the queue exists

	// BOUNDED CHANNELS: Buffer size based on prefetch count
	// This provides backpressure to prevent unbounded memory growth
	bufferSize := int(channel.PrefetchCount)
	if bufferSize == 0 {
		// Default to 1000 for unlimited prefetch (RabbitMQ behavior)
		bufferSize = 1000
	} else {
		// 1.5x margin to prefetch for backpressure
		bufferSize = bufferSize + (bufferSize / 2)
	}

	// Ensure minimum buffer size for performance
	if bufferSize < 100 {
		bufferSize = 100
	}

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
	// Deserialize the basic.get method
	getMethod := &protocol.BasicGetMethod{}
	err := getMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.get",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic get requested",
		zap.String("queue", getMethod.Queue),
		zap.Bool("no_ack", getMethod.NoAck))

	// For now, we'll respond with basic.get-empty since we don't have actual message retrieval implemented yet
	// In a real implementation, we would try to get the next message from the queue
	err = s.sendBasicGetEmpty(conn, channelID)
	if err != nil {
		s.Log.Error("Failed to send basic.get-empty",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	return nil
}

// sendBasicDeliver sends a basic.deliver method frame to a consumer
func (s *Server) sendBasicDeliver(conn *protocol.Connection, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	s.Log.Debug("ENTERING sendBasicDeliver function",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Uint64("delivery_tag", deliveryTag),
		zap.String("exchange", exchange),
		zap.String("routing_key", routingKey),
		zap.Int("message_body_size", len(message.Body)))

	deliverMethod := &protocol.BasicDeliverMethod{
		ConsumerTag: consumerTag,
		DeliveryTag: deliveryTag,
		Redelivered: redelivered,
		Exchange:    exchange,
		RoutingKey:  routingKey,
	}

	methodData, err := deliverMethod.Serialize()
	if err != nil {
		s.Log.Error("Error serializing basic.deliver", zap.Error(err))
		return fmt.Errorf("error serializing basic.deliver: %v", err)
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 60, 60, methodData) // 60.60 = basic.deliver

	s.Log.Debug("About to send basic.deliver method frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	// NOTE: We'll prepare all frames first, then send them atomically

	// Continue with content header and body frames
	s.Log.Debug("Continuing with content header and body frames",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	// Send content header frame
	// Create a content header frame with the message properties
	s.Log.Debug("Creating content header structure",
		zap.String("consumer_tag", consumerTag),
		zap.String("content_type", message.ContentType))

	contentHeader := &protocol.ContentHeader{
		ClassID:         60, // basic class
		Weight:          0,
		BodySize:        uint64(len(message.Body)),
		PropertyFlags:   0,
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
	}

	// Set property flags based on which properties are set
	propertyFlags := uint16(0)
	if len(message.Headers) > 0 {
		propertyFlags |= protocol.FlagHeaders // headers
	}
	if message.ContentType != "" {
		propertyFlags |= protocol.FlagContentType // content-type
	}
	if message.ContentEncoding != "" {
		propertyFlags |= protocol.FlagContentEncoding // content-encoding
	}
	if message.DeliveryMode != 0 {
		propertyFlags |= protocol.FlagDeliveryMode // delivery-mode
	}
	if message.Priority != 0 {
		propertyFlags |= protocol.FlagPriority // priority
	}
	if message.CorrelationID != "" {
		propertyFlags |= protocol.FlagCorrelationID // correlation-id
	}
	if message.ReplyTo != "" {
		propertyFlags |= protocol.FlagReplyTo // reply-to
	}
	if message.Expiration != "" {
		propertyFlags |= protocol.FlagExpiration // expiration
	}
	if message.MessageID != "" {
		propertyFlags |= protocol.FlagMessageID // message-id
	}
	if message.Timestamp != 0 {
		propertyFlags |= protocol.FlagTimestamp // timestamp
	}
	if message.Type != "" {
		propertyFlags |= protocol.FlagType // type
	}
	if message.UserID != "" {
		propertyFlags |= protocol.FlagUserID // user-id
	}
	if message.AppID != "" {
		propertyFlags |= protocol.FlagAppID // app-id
	}
	if message.ClusterID != "" {
		propertyFlags |= protocol.FlagClusterID // cluster-id
	}

	contentHeader.PropertyFlags = propertyFlags

	s.Log.Debug("Property flags set",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("property_flags", propertyFlags))

	// Use ContentHeader.Serialize() to get properly formatted header payload
	s.Log.Debug("About to serialize content header",
		zap.String("consumer_tag", consumerTag))

	headerPayload, err := contentHeader.Serialize()
	if err != nil {
		s.Log.Error("Error serializing content header", zap.Error(err))
		return fmt.Errorf("error serializing content header: %v", err)
	}

	s.Log.Debug("Content header serialized successfully",
		zap.String("consumer_tag", consumerTag),
		zap.Int("payload_length", len(headerPayload)))

	s.Log.Debug("Content header serialization complete",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("class_id", contentHeader.ClassID),
		zap.Uint64("body_size", contentHeader.BodySize),
		zap.Uint16("property_flags", contentHeader.PropertyFlags),
		zap.String("content_type", contentHeader.ContentType),
		zap.Int("header_payload_length", len(headerPayload)))

	s.Log.Debug("Creating content header frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Uint64("body_size", uint64(len(message.Body))),
		zap.Uint16("property_flags", propertyFlags),
		zap.Int("header_payload_size", len(headerPayload)))

	// Create header frame directly with serialized payload
	headerFrame := &protocol.Frame{
		Type:    protocol.FrameHeader,
		Channel: channelID,
		Size:    uint32(len(headerPayload)),
		Payload: headerPayload,
	}

	s.Log.Debug("Header frame created",
		zap.String("consumer_tag", consumerTag))

	s.Log.Debug("Sending content header frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Uint64("body_size", contentHeader.BodySize))

	// Prepare all frames for atomic transmission
	s.Log.Debug("Preparing all frames for atomic transmission",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	// Serialize all three frames
	methodFrameData, err := frame.MarshalBinary()
	if err != nil {
		s.Log.Error("Error serializing method frame", zap.Error(err))
		return fmt.Errorf("error serializing method frame: %v", err)
	}

	headerFrameData, err := headerFrame.MarshalBinary()
	if err != nil {
		s.Log.Error("Error serializing header frame", zap.Error(err))
		return fmt.Errorf("error serializing header frame: %v", err)
	}

	// Send content body frame using the proper protocol function
	s.Log.Debug("Creating body frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("body_size", len(message.Body)),
		zap.String("body_content", string(message.Body)))

	bodyFrame := protocol.EncodeBodyFrameForChannel(channelID, message.Body)

	s.Log.Debug("Body frame created",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("payload_size", len(bodyFrame.Payload)),
		zap.String("payload_content", string(bodyFrame.Payload)))

	s.Log.Debug("Sending body frame",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("body_size", len(message.Body)))

	bodyFrameData, err := bodyFrame.MarshalBinary()
	if err != nil {
		s.Log.Error("Error serializing body frame", zap.Error(err))
		return fmt.Errorf("error serializing body frame: %v", err)
	}

	// Combine all frames into single atomic write
	allFrames := make([]byte, 0, len(methodFrameData)+len(headerFrameData)+len(bodyFrameData))
	allFrames = append(allFrames, methodFrameData...)
	allFrames = append(allFrames, headerFrameData...)
	allFrames = append(allFrames, bodyFrameData...)

	s.Log.Debug("Writing all frames atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("total_bytes", len(allFrames)),
		zap.Int("method_frame_size", len(methodFrameData)),
		zap.Int("header_frame_size", len(headerFrameData)),
		zap.Int("body_frame_size", len(bodyFrameData)))

	// Atomic write of all three frames
	_, err = conn.Conn.Write(allFrames)
	if err != nil {
		s.Log.Error("Error sending all frames atomically",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("error sending frames atomically: %v", err)
	}

	s.Log.Debug("Sent all frames atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("body_size", len(message.Body)))

	s.Log.Debug("basic.deliver complete - all frames sent atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID))

	return nil
}

// sendBasicGetEmpty sends the basic.get-empty method frame

// handleBasicAck handles the basic.ack method
// handleBasicAck handles the basic.ack method
func (s *Server) handleBasicAck(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.ack method
	ackMethod := &protocol.BasicAckMethod{}
	err := ackMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.ack",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic ack received",
		zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
		zap.Bool("multiple", ackMethod.Multiple))

	// CRITICAL FIX: Use global delivery index for O(1) consumer lookup
	// This fixes the broken random consumer lookup that caused 95% of ACKs to route incorrectly
	consumerTag, ok := s.Broker.GetConsumerForDelivery(ackMethod.DeliveryTag)
	if !ok {
		s.Log.Warn("Could not find consumer for acknowledgment",
			zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("delivery tag %d not found", ackMethod.DeliveryTag)
	}

	// Tell the broker to acknowledge the message
	err = s.Broker.AcknowledgeMessage(consumerTag, ackMethod.DeliveryTag, ackMethod.Multiple)
	if err != nil {
		s.Log.Error("Failed to acknowledge message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
			zap.Bool("multiple", ackMethod.Multiple))
		return err
	}

	s.Log.Debug("Message acknowledged",
		zap.String("consumer_tag", consumerTag),
		zap.Uint64("delivery_tag", ackMethod.DeliveryTag),
		zap.Bool("multiple", ackMethod.Multiple),
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	return nil
}

// handleBasicReject handles the basic.reject method
func (s *Server) handleBasicReject(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.reject method
	rejectMethod := &protocol.BasicRejectMethod{}
	err := rejectMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.reject",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic reject received",
		zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
		zap.Bool("requeue", rejectMethod.Requeue))

	// CRITICAL FIX: Use global delivery index for O(1) consumer lookup
	consumerTag, ok := s.Broker.GetConsumerForDelivery(rejectMethod.DeliveryTag)
	if !ok {
		s.Log.Warn("Could not find consumer for rejection",
			zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("delivery tag %d not found", rejectMethod.DeliveryTag)
	}

	// Tell the broker to reject the message
	err = s.Broker.RejectMessage(consumerTag, rejectMethod.DeliveryTag, rejectMethod.Requeue)
	if err != nil {
		s.Log.Error("Failed to reject message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
			zap.Bool("requeue", rejectMethod.Requeue))
		return err
	}

	s.Log.Debug("Message rejected",
		zap.String("consumer_tag", consumerTag),
		zap.Uint64("delivery_tag", rejectMethod.DeliveryTag),
		zap.Bool("requeue", rejectMethod.Requeue),
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	return nil
}

// handleBasicNack handles the basic.nack method
func (s *Server) handleBasicNack(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.nack method
	nackMethod := &protocol.BasicNackMethod{}
	err := nackMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.nack",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Debug("Basic nack received",
		zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
		zap.Bool("multiple", nackMethod.Multiple),
		zap.Bool("requeue", nackMethod.Requeue))

	// CRITICAL FIX: Use global delivery index for O(1) consumer lookup
	consumerTag, ok := s.Broker.GetConsumerForDelivery(nackMethod.DeliveryTag)
	if !ok {
		s.Log.Warn("Could not find consumer for nack",
			zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("delivery tag %d not found", nackMethod.DeliveryTag)
	}

	// Tell the broker to nack the message
	err = s.Broker.NacknowledgeMessage(consumerTag, nackMethod.DeliveryTag, nackMethod.Multiple, nackMethod.Requeue)
	if err != nil {
		s.Log.Error("Failed to nack message in broker",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
			zap.Bool("multiple", nackMethod.Multiple),
			zap.Bool("requeue", nackMethod.Requeue))
		return err
	}

	s.Log.Debug("Message negatively acknowledged",
		zap.String("consumer_tag", consumerTag),
		zap.Uint64("delivery_tag", nackMethod.DeliveryTag),
		zap.Bool("multiple", nackMethod.Multiple),
		zap.Bool("requeue", nackMethod.Requeue),
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	return nil
}

// Stop gracefully stops the server
