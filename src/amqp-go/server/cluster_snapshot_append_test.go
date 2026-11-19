package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestClusterSnapshotAppendsWithExistingShardLayout(t *testing.T) {
	state := &localClusterMachine{}
	broker := NewClusterBroker(state)
	_, err := broker.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, broker.DeclareExchange("jobs-topic", "topic", true, false, false, nil))
	require.NoError(t, broker.BindQueue("jobs", "jobs-topic", "job.*", nil))
	body := bytes.Repeat([]byte{0, 255, 'x'}, 256)
	require.NoError(t, broker.PublishMessage("jobs-topic", "job.ready", &protocol.Message{Body: body}))

	exchanges, err := json.Marshal(state.machine.exchanges)
	require.NoError(t, err)
	require.Contains(t, string(exchanges), "jobs-topic")
	queues, err := state.machine.queues.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, queues)
	require.Equal(t, byte(2), queues[0], "queue content must still use the existing CBOR version")
	require.True(t, bytes.Contains(queues, body), "the snapshot must include the queued binary body")
	want := binary.BigEndian.AppendUint32([]byte{1}, uint32(len(exchanges)))
	want = append(want, exchanges...)
	want = append(want, queues...)

	appender, ok := any(&state.machine).(interface {
		AppendSnapshot([]byte) ([]byte, error)
	})
	require.True(t, ok, "the shard machine must accept an existing checkpoint prefix")
	prefix := []byte("existing checkpoint prefix")
	snapshot, err := appender.AppendSnapshot(bytes.Clone(prefix))
	require.NoError(t, err)
	require.Equal(t, append(bytes.Clone(prefix), want...), snapshot)
	standalone, err := state.machine.Snapshot()
	require.NoError(t, err)
	require.Equal(t, want, standalone)

	restored := &localClusterMachine{}
	require.NoError(t, restored.machine.Restore(snapshot[len(prefix):]))
	reader := NewClusterBroker(restored)
	require.NoError(t, reader.ExchangeDeclarePassive("jobs-topic"))
	delivered, _, _, err := reader.GetMessageForGetOwned("jobs", true, "reader")
	require.NoError(t, err)
	require.Equal(t, body, delivered.Body)
	reader.RequeueGetDeliveries("reader")
}
