package server

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
	"github.com/maxpert/amqp-go/protocol"
)

// The envelope header uses the same AMQP properties payload as a client
// delivery. The queue command encoder appends the body after this header.
func encodeClusterMessageHeader(message *protocol.Message) ([]byte, error) {
	if len(message.Exchange) > 255 || len(message.RoutingKey) > 255 {
		return nil, errors.New("exchange or routing key exceeds the AMQP short-string limit")
	}
	header, err := buildContentHeaderPayload(message)
	if err != nil {
		return nil, err
	}
	if len(header) > math.MaxUint32 {
		return nil, errors.New("AMQP properties exceed the cluster envelope limit")
	}
	data := make([]byte, 0, 7+len(message.Exchange)+len(message.RoutingKey)+len(header))
	data = append(data, clusterpayload.RawEnvelope, byte(len(message.Exchange)), byte(len(message.RoutingKey)))
	data = binary.BigEndian.AppendUint32(data, uint32(len(header)))
	data = append(data, message.Exchange...)
	data = append(data, message.RoutingKey...)
	return append(data, header...), nil
}

func decodeClusterMessage(data []byte) (*protocol.Message, error) {
	return decodeStoredClusterMessage(data, false)
}

func decodeStoredClusterMessage(data []byte, packed bool) (*protocol.Message, error) {
	if len(data) < 7 || (!packed && data[0] != clusterpayload.RawEnvelope) ||
		(packed && !clusterpayload.IsPackedEnvelope(data[0])) {
		return nil, errors.New("invalid cluster message envelope")
	}
	exchangeEnd := 7 + int(data[1])
	keyEnd := exchangeEnd + int(data[2])
	headerEnd := uint64(keyEnd) + uint64(binary.BigEndian.Uint32(data[3:7]))
	if headerEnd > uint64(len(data)) {
		return nil, errors.New("truncated cluster message envelope")
	}
	props, err := protocol.ReadContentHeader(&protocol.Frame{Type: protocol.FrameHeader, Payload: data[keyEnd:headerEnd]})
	if err != nil {
		return nil, err
	}
	defer protocol.PutContentHeader(props)
	body := data[headerEnd:]
	if packed {
		body, err = clusterpayload.Unpack(body, props.BodySize, data[0])
		if err != nil {
			return nil, err
		}
	} else if uint64(len(body)) != props.BodySize {
		return nil, errors.New("cluster message body size mismatch")
	}
	return &protocol.Message{
		Exchange: string(data[7:exchangeEnd]), RoutingKey: string(data[exchangeEnd:keyEnd]),
		Body: body, Headers: props.Headers, ContentType: props.ContentType, ContentEncoding: props.ContentEncoding,
		DeliveryMode: props.DeliveryMode, Priority: props.Priority, CorrelationID: props.CorrelationID,
		ReplyTo: props.ReplyTo, Expiration: props.Expiration, MessageID: props.MessageID, Timestamp: props.Timestamp,
		Type: props.Type, UserID: props.UserID, AppID: props.AppID, ClusterID: props.ClusterID,
	}, nil
}
