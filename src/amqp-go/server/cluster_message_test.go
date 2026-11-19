package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestClusterMessageRoundTripPreservesAMQPProperties(t *testing.T) {
	message := &protocol.Message{
		Exchange: "source", RoutingKey: "jobs.green", Body: []byte{0, 1, 2, 3},
		ContentType: "application/octet-stream", ContentEncoding: "binary",
		DeliveryMode: 2, Priority: 3, CorrelationID: "corr", ReplyTo: "reply",
		MessageID: "mid", Timestamp: 1234, Type: "job", UserID: "u", AppID: "a", ClusterID: "c",
		Headers: map[string]interface{}{
			"bytes": []byte{7, 8}, "when": time.Unix(4, 0).UTC(),
			"table": map[string]interface{}{"n": int32(5)}, "array": []interface{}{true, "ok", nil},
		},
	}
	header, err := encodeClusterMessageHeader(message)
	require.NoError(t, err)
	data := append(header, message.Body...)
	recovered, err := decodeClusterMessage(data)
	require.NoError(t, err)
	require.Equal(t, message.Body, recovered.Body)
	require.Equal(t, message.Exchange, recovered.Exchange)
	require.Equal(t, message.RoutingKey, recovered.RoutingKey)
	require.True(t, protocol.FieldTablesEqual(message.Headers, recovered.Headers))
	recovered.Headers = message.Headers
	require.Equal(t, message, recovered)
	_, err = decodeClusterMessage(data[:len(data)-1])
	require.Error(t, err)
}

func TestClusterCanonicalArgumentsSortNestedTables(t *testing.T) {
	args := map[string]interface{}{
		"nested": []interface{}{map[string]interface{}{"b": int32(1), "a": int32(2)}},
		"one":    "two", "three": true,
	}
	first, err := protocol.EncodeCanonicalFieldTable(args)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		again, err := protocol.EncodeCanonicalFieldTable(args)
		require.NoError(t, err)
		require.Equal(t, first, again)
	}
	decoded, err := protocol.DecodeFieldTable(first)
	require.NoError(t, err)
	require.True(t, protocol.FieldTablesEqual(args, decoded))
}
