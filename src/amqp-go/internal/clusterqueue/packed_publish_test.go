package clusterqueue

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
	"github.com/stretchr/testify/require"
)

func TestPackedPublishKeepsLogicalQuotaAcrossRestore(t *testing.T) {
	const queue = "packed-bodies"
	var m Machine
	declare, err := EncodeCommand(QueueCommand{Kind: DeclareQueue, Queue: queue, QueueSpec: QueueSpec{Durable: true}})
	require.NoError(t, err)
	declared := m.applyCommand(declare, true, nil)
	require.NoError(t, declared.Err())

	body := bytes.Repeat([]byte("ABCD"), 16<<10)
	header := make([]byte, 21)
	header[0] = clusterpayload.PackedEnvelope
	binary.BigEndian.PutUint32(header[3:7], 14)
	binary.BigEndian.PutUint64(header[11:19], uint64(len(body)))
	compact := PublishRecord{Prefix: header, Body: clusterpayload.Pack(body), Packed: true}
	require.NotEmpty(t, compact.Body)
	raw := PublishRecord{Prefix: bytes.Clone(header), Body: body}
	raw.Prefix[0] = clusterpayload.RawEnvelope
	mixed, err := EncodePublishBatchRecords(queue, declared.Queue.Incarnation, []PublishRecord{compact, raw})
	require.NoError(t, err)
	mixedReply := m.applyCommand(mixed, true, nil)
	require.NoError(t, mixedReply.Err())
	require.EqualValues(t, 2, mixedReply.Count)
	logical := len(header) + len(body)
	require.EqualValues(t, 2*logical, m.state.bodies)
	require.EqualValues(t, len(body)-len(compact.Body), m.state.packingSavings)

	wire, err := EncodePublishCommand(queue, declared.Queue.Incarnation, compact)
	require.NoError(t, err)
	maxLegal := maxBodyBytes / logical
	for i := 2; i < maxLegal; i++ {
		require.NoError(t, m.applyCommand(wire, true, nil).Err())
	}
	require.ErrorIs(t, m.applyCommand(wire, true, nil).Err(), ErrCapacity)
	image, err := m.Snapshot()
	require.NoError(t, err)
	var restored Machine
	require.NoError(t, restored.Restore(image))
	require.Equal(t, m.state.bodies, restored.state.bodies)
	require.Equal(t, m.state.packingSavings, restored.state.packingSavings)
	require.ErrorIs(t, restored.applyCommand(wire, true, nil).Err(), ErrCapacity)

	broken := compact
	broken.Prefix = bytes.Clone(header)
	binary.BigEndian.PutUint64(broken.Prefix[11:19], uint64(len(body))+1)
	bad, err := EncodePublishCommand(queue, declared.Queue.Incarnation, broken)
	require.NoError(t, err)
	_, err = DecodeCommand(bad)
	require.ErrorIs(t, err, ErrInvalidCommand)

	// A length prefix by itself does not encode a complete block.
	broken = compact
	broken.Body = binary.AppendUvarint(nil, uint64(len(body)))
	bad, err = EncodePublishCommand(queue, declared.Queue.Incarnation, broken)
	require.NoError(t, err)
	_, err = DecodeCommand(bad)
	require.ErrorIs(t, err, ErrInvalidCommand)
}

func TestSmallPackedPublishUsesItsOwnCommandVersion(t *testing.T) {
	body := bytes.Repeat([]byte("ABCD"), 256)
	packed, version := clusterpayload.PackSmall(body)
	require.NotEmpty(t, packed)
	header := make([]byte, 21)
	header[0] = version
	binary.BigEndian.PutUint32(header[3:7], 14)
	binary.BigEndian.PutUint64(header[11:19], uint64(len(body)))
	small := PublishRecord{Prefix: header, Body: packed, Packed: true}
	raw := PublishRecord{Prefix: bytes.Clone(header), Body: body}
	raw.Prefix[0] = clusterpayload.RawEnvelope
	single, err := EncodePublishCommand("jobs", 0, small)
	require.NoError(t, err)
	batch, err := EncodePublishBatchRecords("jobs", 0, []PublishRecord{raw, small})
	require.NoError(t, err)
	var machine Machine
	declare, err := EncodeCommand(QueueCommand{Kind: DeclareQueue, Queue: "jobs", QueueSpec: QueueSpec{Durable: true}})
	require.NoError(t, err)
	require.NoError(t, machine.applyCommand(declare, true, nil).Err())
	applied := machine.applyCommand(batch, true, nil)
	require.NoError(t, applied.Err())
	require.EqualValues(t, 2, applied.Count)
	require.EqualValues(t, 2*(len(header)+len(body)), machine.state.bodies)
	image, err := machine.Snapshot()
	require.NoError(t, err)
	var restored Machine
	require.NoError(t, restored.Restore(image))
	require.Equal(t, machine.state.bodies, restored.state.bodies)
	for _, command := range [][]byte{single, batch} {
		require.NotEqual(t, byte(packedPublishVersion), command[0], "small bodies need a distinct command version")
		_, err := DecodeCommand(command)
		require.NoError(t, err)
		command[0] = packedPublishVersion
		_, err = DecodeCommand(command)
		require.ErrorIs(t, err, ErrInvalidCommand, "legacy packed commands must reject small envelopes")
	}
}
