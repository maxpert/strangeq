package server

import (
	"bytes"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestClusterSnapshotCaptureOwnsMutableShardState(t *testing.T) {
	state := &localClusterMachine{}
	broker := NewClusterBroker(state)
	queue, err := broker.DeclareQueue("captured", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, broker.DeclareExchange("captured-direct", "direct", true, false, false, nil))
	require.NoError(t, broker.BindQueue("captured", "captured-direct", "before", nil))
	message := &protocol.Message{
		Exchange: "captured-direct", RoutingKey: "before", Body: bytes.Repeat([]byte{7, 0}, 32<<10),
	}
	header, err := encodeClusterMessageHeader(message)
	require.NoError(t, err)
	wire, err := clusterqueue.EncodePublishCommand("captured", clusterqueue.QueueID(queue.Ordinal),
		clusterqueue.PublishRecord{Prefix: header, Body: message.Body})
	require.NoError(t, err)
	receipt, err := state.machine.ApplyJournaled(wire, true, bytes.NewReader(bytes.Clone(wire)))
	require.NoError(t, err)
	published, err := clusterqueue.DecodeResponse(receipt)
	require.NoError(t, err)
	require.NoError(t, published.Err())

	prefix := []byte("existing Raft prefix")
	want, err := state.machine.AppendSnapshot(bytes.Clone(prefix))
	require.NoError(t, err)
	encode := state.machine.AppendSnapshot
	if capturer, ok := any(&state.machine).(interface {
		CaptureSnapshot() func([]byte) ([]byte, error)
	}); ok {
		encode = capturer.CaptureSnapshot()
	}

	claimed, _, _, err := broker.GetMessageForGetOwned("captured", false, "mutation")
	require.NoError(t, err)
	require.True(t, bytes.Equal(message.Body, claimed.Body), "the active machine must claim its original cold body")
	require.NoError(t, broker.BindQueue("captured", "captured-direct", "after", nil))
	require.NoError(t, broker.PublishMessage("captured-direct", "after", &protocol.Message{Body: []byte("new live state")}))
	after, err := state.machine.AppendSnapshot(bytes.Clone(prefix))
	require.NoError(t, err)
	require.False(t, bytes.Equal(want, after), "the active queue and binding must have changed")

	got, err := encode(bytes.Clone(prefix))
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "the frozen checkpoint must retain the original state and wire bytes")
	restored := &localClusterMachine{}
	require.NoError(t, restored.machine.Restore(got[len(prefix):]))
	reader := NewClusterBroker(restored)
	replayed, _, _, err := reader.GetMessageForGetOwned("captured", false, "restored")
	require.NoError(t, err)
	require.True(t, bytes.Equal(message.Body, replayed.Body))
	reader.RequeueGetDeliveries("restored")
	broker.RequeueGetDeliveries("mutation")
}
