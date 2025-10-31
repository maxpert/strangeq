package server

import (
	"fmt"

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

// sendExchangeDeclareOK sends the exchange.declare-ok method frame
func (s *Server) sendExchangeDeclareOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 11, &protocol.ExchangeDeclareOKMethod{})
}

// sendExchangeDeleteOK sends the exchange.delete-ok method frame
func (s *Server) sendExchangeDeleteOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 21, &protocol.ExchangeDeleteOKMethod{})
}

// sendExchangeUnbindOK sends the exchange.unbind-ok method frame
func (s *Server) sendExchangeUnbindOK(conn *protocol.Connection, channelID uint16) error {
	return s.sendMethodResponse(conn, channelID, 40, 51, &protocol.ExchangeUnbindOKMethod{})
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

// sendBasicGetEmpty sends the basic.get-empty method frame
func (s *Server) sendBasicGetEmpty(conn *protocol.Connection, channelID uint16) error {
	method := &protocol.BasicGetEmptyMethod{
		Reserved1: "amq.empty", // Standard reserved value
	}
	return s.sendMethodResponse(conn, channelID, 60, 72, method)
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
