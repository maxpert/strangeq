package server

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// SerializableMethod interface for methods that can be serialized
type SerializableMethod interface {
	Serialize() ([]byte, error)
}

// sendMethodResponse sends a method response frame with serialized method data
// This consolidates the pattern used by all sendXXXOK functions
func (s *Server) sendMethodResponse(conn *protocol.Connection, channelID uint16, classID, methodID uint16, method SerializableMethod) error {
	methodData, err := method.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize method %d.%d: %w", classID, methodID, err)
	}

	var frame *protocol.Frame
	if channelID == 0 {
		frame = protocol.EncodeMethodFrame(classID, methodID, methodData)
	} else {
		frame = protocol.EncodeMethodFrameForChannel(channelID, classID, methodID, methodData)
	}

	return protocol.WriteFrameToConnection(conn, frame)
}

// sendConnectionCloseOK sends the connection.close-ok method frame
func (s *Server) sendConnectionCloseOK(conn *protocol.Connection) error {
	return s.sendMethodResponse(conn, 0, 10, 51, &protocol.ConnectionCloseOKMethod{})
}

// sendChannelOpenOK sends the channel.open-ok method frame
func (s *Server) sendChannelOpenOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 20, 11, &protocol.ChannelOpenOKMethod{Reserved1: ""})
}

// sendChannelCloseOK sends the channel.close-ok method frame
func (s *Server) sendChannelCloseOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 20, 41, &protocol.ChannelCloseOKMethod{})
}

// sendChannelFlowOK sends the channel.flow-ok method frame, echoing the
// current active state per AMQP 0.9.1 spec.
func (s *Server) sendChannelFlowOK(conn *protocol.Connection, channelID uint16, active bool) error {
	return s.sendMethodResponse(conn, channelID, 20, 21, &protocol.ChannelFlowOKMethod{Active: active})
}

// readerFlowWriteTimeout bounds a server-initiated reader-overflow channel.flow
// write so a client that has stopped reading its socket cannot wedge the reader
// goroutine on the send.
const readerFlowWriteTimeout = 5 * time.Second

// sendChannelFlow sends a server-initiated channel.flow (class 20, method 20) on
// one channel, asking the client to pause (active=false) or resume (active=true)
// content frames. The write is deadline-bounded because it is issued from the
// reader goroutine, which must never park (see setConnectionFlow / readFrames).
func (s *Server) sendChannelFlow(conn *protocol.Connection, channelID uint16, active bool) error {
	data, err := (&protocol.ChannelFlowMethod{Active: active}).Serialize()
	if err != nil {
		return err
	}
	frame := protocol.EncodeMethodFrameForChannel(channelID, 20, 20, data)
	return protocol.WriteFrameToConnectionWithDeadline(conn, frame, readerFlowWriteTimeout)
}

// setConnectionFlow asserts channel.flow(active) on every open channel of the
// connection (channel 0 is the control channel and is skipped). Used by the
// reader-overflow backpressure path to tell a fast publisher to pause and later
// resume. Returns the first send error, if any.
func (s *Server) setConnectionFlow(conn *protocol.Connection, active bool) error {
	var firstErr error
	conn.Channels.Range(func(k, _ interface{}) bool {
		chID, ok := k.(uint16)
		if !ok || chID == 0 {
			return true
		}
		if err := s.sendChannelFlow(conn, chID, active); err != nil && firstErr == nil {
			firstErr = err
		}
		return true
	})
	return firstErr
}

// sendExchangeDeclareOK sends the exchange.declare-ok method frame
func (s *Server) sendExchangeDeclareOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 11, &protocol.ExchangeDeclareOKMethod{})
}

// sendExchangeDeleteOK sends the exchange.delete-ok method frame
func (s *Server) sendExchangeDeleteOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 21, &protocol.ExchangeDeleteOKMethod{})
}

// sendExchangeBindOK sends the exchange.bind-ok method frame
func (s *Server) sendExchangeBindOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 31, &protocol.ExchangeBindOKMethod{})
}

// sendExchangeUnbindOK sends the exchange.unbind-ok method frame
func (s *Server) sendExchangeUnbindOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 41, &protocol.ExchangeUnbindOKMethod{})
}

// sendQueueDeclareOK sends the queue.declare-ok method frame
func (s *Server) sendQueueDeclareOK(conn *protocol.Connection, channelID uint16, queueName string, messageCount, consumerCount uint32) error {
	method := &protocol.QueueDeclareOKMethod{
		Queue:         queueName,
		MessageCount:  messageCount,
		ConsumerCount: consumerCount,
	}
	return s.sendMethodResponse(conn, channelID, 50, 11, method)
}

// sendQueueBindOK sends the queue.bind-ok method frame
func (s *Server) sendQueueBindOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 50, 21, &protocol.QueueBindOKMethod{})
}

// sendQueueUnbindOK sends the queue.unbind-ok method frame
// Note: Reuses QueueBindOKMethod since both have no content
func (s *Server) sendQueueUnbindOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 50, 51, &protocol.QueueBindOKMethod{})
}

// sendQueueDeleteOK sends the queue.delete-ok method frame
func (s *Server) sendQueueDeleteOK(conn *protocol.Connection, channelID uint16, messageCount uint32) error {
	method := &protocol.QueueDeleteOKMethod{
		MessageCount: messageCount,
	}
	return s.sendMethodResponse(conn, channelID, 50, 41, method)
}

// sendBasicQosOK sends the basic.qos-ok method frame
func (s *Server) sendBasicQosOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 60, 11, &protocol.BasicQosOKMethod{})
}

// sendBasicConsumeOK sends the basic.consume-ok method frame
func (s *Server) sendBasicConsumeOK(conn *protocol.Connection, channelID uint16, consumerTag string) error {
	method := &protocol.BasicConsumeOKMethod{
		ConsumerTag: consumerTag,
	}
	return s.sendMethodResponse(conn, channelID, 60, 21, method)
}

// sendBasicCancelOK sends the basic.cancel-ok method frame
func (s *Server) sendBasicCancelOK(conn *protocol.Connection, channelID uint16, consumerTag string) error {
	method := &protocol.BasicCancelOKMethod{
		ConsumerTag: consumerTag,
	}
	return s.sendMethodResponse(conn, channelID, 60, 31, method)
}

func (s *Server) sendBasicCancel(conn *protocol.Connection, channelID uint16, consumerTag string) error {
	method := &protocol.BasicCancelMethod{
		ConsumerTag: consumerTag,
		NoWait:      true,
	}
	return s.sendMethodResponse(conn, channelID, 60, 30, method)
}

// sendBasicGetEmpty sends the basic.get-empty method frame
func (s *Server) sendBasicGetEmpty(conn *protocol.Connection, channelID uint16) error {
	method := &protocol.BasicGetEmptyMethod{
		Reserved1: "amq.empty", // Standard reserved value
	}
	return s.sendMethodResponse(conn, channelID, 60, 72, method)
}

// sendBasicGetOK sends the basic.get-ok method frame (content header and body
// frames must follow on the same channel).
func (s *Server) sendBasicGetOK(conn *protocol.Connection, channelID uint16, deliveryTag uint64, redelivered bool, exchange, routingKey string, messageCount uint32) error {
	method := &protocol.BasicGetOKMethod{
		DeliveryTag:  deliveryTag,
		Redelivered:  redelivered,
		Exchange:     exchange,
		RoutingKey:   routingKey,
		MessageCount: messageCount,
	}
	return s.sendMethodResponse(conn, channelID, 60, 71, method)
}

// sendQueuePurgeOK sends the queue.purge-ok method frame
func (s *Server) sendQueuePurgeOK(conn *protocol.Connection, channelID uint16, messageCount uint32) error {
	method := &protocol.QueuePurgeOKMethod{
		MessageCount: messageCount,
	}
	return s.sendMethodResponse(conn, channelID, 50, 31, method)
}

// buildPropertyFlags builds the property flags bitmap from a message
// Extracted from sendBasicDeliver to make it testable
func buildPropertyFlags(msg *protocol.Message) uint16 {
	var flags uint16

	if msg.ContentType != "" {
		flags |= protocol.FlagContentType
	}
	if msg.ContentEncoding != "" {
		flags |= protocol.FlagContentEncoding
	}
	if msg.Headers != nil && len(msg.Headers) > 0 {
		flags |= protocol.FlagHeaders
	}
	if msg.DeliveryMode > 0 {
		flags |= protocol.FlagDeliveryMode
	}
	if msg.Priority > 0 {
		flags |= protocol.FlagPriority
	}
	if msg.CorrelationID != "" {
		flags |= protocol.FlagCorrelationID
	}
	if msg.ReplyTo != "" {
		flags |= protocol.FlagReplyTo
	}
	if msg.Expiration != "" {
		flags |= protocol.FlagExpiration
	}
	if msg.MessageID != "" {
		flags |= protocol.FlagMessageID
	}
	if msg.Timestamp > 0 {
		flags |= protocol.FlagTimestamp
	}
	if msg.Type != "" {
		flags |= protocol.FlagType
	}
	if msg.UserID != "" {
		flags |= protocol.FlagUserID
	}
	if msg.AppID != "" {
		flags |= protocol.FlagAppID
	}
	if msg.ClusterID != "" {
		flags |= protocol.FlagClusterID
	}

	return flags
}

// buildContentHeaderPayload serializes the AMQP content-header payload (class
// 60 basic properties) for message. It is shared by the contiguous delivery
// encoder (appendHeaderAndBodyFrames) and the vectored delivery encoder
// (appendHeaderAndBodyFramesVec) so the two paths can never diverge on which
// message properties are emitted or in what order. The bytes returned are
// exactly what the contiguous path appended inline before this extraction.
func buildContentHeaderPayload(message *protocol.Message) ([]byte, error) {
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
		return nil, fmt.Errorf("error serializing content header: %v", err)
	}
	return hdrPayload, nil
}

func (s *Server) appendHeaderAndBodyFrames(buf *[]byte, channelID uint16, message *protocol.Message) error {
	hdrPayload, err := buildContentHeaderPayload(message)
	if err != nil {
		return err
	}
	*buf = protocol.AppendFrame(*buf, protocol.FrameHeader, channelID, hdrPayload)

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

func (s *Server) sendBasicReturn(conn *protocol.Connection, channelID uint16, replyCode uint16, replyText, exchange, routingKey string, message *protocol.Message) error {
	method := &protocol.BasicReturnMethod{
		ReplyCode:  replyCode,
		ReplyText:  replyText,
		Exchange:   exchange,
		RoutingKey: routingKey,
	}
	methodData, err := method.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize basic.return: %w", err)
	}

	estimatedSize := deliveryOverheadEstimate + len(message.Body)
	const maxPoolSize = 131 * 1024
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

	var methodPayload []byte
	methodPayload = binary.BigEndian.AppendUint16(methodPayload, 60)
	methodPayload = binary.BigEndian.AppendUint16(methodPayload, 50)
	methodPayload = append(methodPayload, methodData...)
	*buf = protocol.AppendFrame(*buf, protocol.FrameMethod, channelID, methodPayload)

	if err := s.appendHeaderAndBodyFrames(buf, channelID, message); err != nil {
		return err
	}

	conn.WriteMutex.Lock()
	_, err = conn.Conn.Write(*buf)
	conn.WriteMutex.Unlock()

	if err != nil {
		return fmt.Errorf("error sending basic.return frames: %v", err)
	}

	return nil
}

func (s *Server) sendBasicRecoverOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 60, 111, &protocol.BasicRecoverOKMethod{})
}

// sendConfirmSelectOK sends the confirm.select-ok method frame
func (s *Server) sendConfirmSelectOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 85, 11, &protocol.ConfirmSelectOKMethod{})
}

// sendBasicAck sends a basic.ack method frame to the client. Used for publisher
// confirms: the server acknowledges that a message with the given delivery tag
// has been processed.
func (s *Server) sendBasicAck(conn *protocol.Connection, channelID uint16, deliveryTag uint64, multiple bool) error {
	method := &protocol.BasicAckMethod{
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
	}
	return s.sendMethodResponse(conn, channelID, 60, 80, method)
}

// sendBasicNack sends a basic.nack method frame to the client. Used for SQ-11
// x-overflow=reject-publish: the broker refused the publish because a target
// queue is at/over its x-max-length / x-max-length-bytes limit, so the
// publisher confirm for that tag is settled as a negative acknowledgement
// (requeue is always false — the message was never enqueued).
func (s *Server) sendBasicNack(conn *protocol.Connection, channelID uint16, deliveryTag uint64, multiple, requeue bool) error {
	method := &protocol.BasicNackMethod{
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
		Requeue:     requeue,
	}
	return s.sendMethodResponse(conn, channelID, 60, 120, method)
}
