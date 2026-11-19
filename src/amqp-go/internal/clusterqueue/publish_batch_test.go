package clusterqueue

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitPublishEncodersPreserveOwnedWireBytes(t *testing.T) {
	parts := []PublishRecord{
		{Prefix: []byte{1, 3}, Body: bytes.Repeat([]byte{3, 0}, 32<<10)},
		{Prefix: []byte{1, 7}, Body: bytes.Repeat([]byte{7, 0}, 32<<10)},
	}
	first := append(bytes.Clone(parts[0].Prefix), parts[0].Body...)
	second := append(bytes.Clone(parts[1].Prefix), parts[1].Body...)
	single, err := EncodePublishCommand(queueName, 42, parts[0])
	require.NoError(t, err)
	batch, err := EncodePublishBatchRecords(queueName, 42, parts)
	require.NoError(t, err)
	for _, part := range parts {
		clear(part.Prefix)
		clear(part.Body)
	}
	wantSingle, err := encodeFrame(QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: 42}, first)
	require.NoError(t, err)
	wantBatch, err := encodeFrame(QueueCommand{Kind: PublishBatch, Queue: queueName, Incarnation: 42},
		publishBatchWireBody(first, second))
	require.NoError(t, err)
	require.True(t, bytes.Equal(wantSingle, single), "single publish must own the original wire bytes")
	require.True(t, bytes.Equal(wantBatch, batch), "publish batch must own the original ordered wire bytes")
}

// Pin the first publish-batch wire fixtures independently of the production
// encoder so these tests exercise replicated command application.
const publishBatchWireKind QueueCommandKind = 16

func publishBatchWireBody(bodies ...[]byte) []byte {
	var frame []byte
	for _, body := range bodies {
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
		frame = append(frame, body...)
	}
	return frame
}

func applyPublishBatchWire(t *testing.T, m *Machine, command QueueCommand, wireBody []byte) QueueResponse {
	t.Helper()
	wire, err := encodeFrame(command, wireBody)
	require.NoError(t, err)
	response, err := DecodeResponse(m.Apply(wire))
	require.NoError(t, err)
	return response
}

func expectPublishedBodies(t *testing.T, m *Machine, incarnation QueueID, bodies ...[]byte) {
	t.Helper()
	session := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: incarnation,
		Session: 1, Owner: "reader", Prefetch: uint32(len(bodies)), Deadline: 10}
	require.NoError(t, apply(t, m, session).Err())
	session.Kind, session.Prefetch, session.Deadline = ClaimMessage, 0, 0
	for i, body := range bodies {
		session.Sequence++
		response := apply(t, m, session)
		require.NoError(t, response.Err())
		require.Equal(t, body, response.Result.Delivery.Body)
		require.EqualValues(t, i+1, response.Result.Delivery.MessageID)
	}
	require.Zero(t, apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName}).Queue.Ready)
}

func TestPublishBatchCommitsRawBodiesInOrder(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	legacy := []byte{0xff, 0, 0xc0}
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: legacy}).Err())
	bodies := [][]byte{{0, 0x80, 0xff}, nil, {0xff, 2, 3}}
	result := applyPublishBatchWire(t, m, QueueCommand{Kind: publishBatchWireKind, Queue: queueName}, publishBatchWireBody(bodies...))
	require.NoError(t, result.Err())
	require.EqualValues(t, len(bodies), result.Count)

	// A batch affects the ordinary ready queue and uses its existing snapshot
	// format, so old and new publish records recover in the same FIFO.
	restored := &Machine{}
	require.NoError(t, restored.Restore(mustSnapshot(t, m)))
	expectPublishedBodies(t, restored, q.Queue.Incarnation, append([][]byte{legacy}, bodies...)...)
}

func TestPublishBatchRejectsStaleIncarnation(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	stale := q.Queue.Incarnation
	require.NoError(t, apply(t, m, QueueCommand{Kind: DeleteQueue, Queue: queueName, Incarnation: stale}).Err())
	q = apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	require.NotEqual(t, stale, q.Queue.Incarnation)

	bodies := [][]byte{[]byte("one"), []byte("two")}
	command := QueueCommand{Kind: publishBatchWireKind, Queue: queueName, Incarnation: stale}
	reply := applyPublishBatchWire(t, m, command, publishBatchWireBody(bodies...))
	require.ErrorIs(t, reply.Err(), ErrQueueIncarnation)
	require.Zero(t, reply.Count)
	command.Incarnation = q.Queue.Incarnation
	reply = applyPublishBatchWire(t, m, command, publishBatchWireBody(bodies...))
	require.NoError(t, reply.Err())
	require.EqualValues(t, 2, reply.Count)
	expectPublishedBodies(t, m, q.Queue.Incarnation, bodies...)
}

func TestPublishBatchCommitsOnlySuccessfulPrefix(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	const filler = "filler"
	require.NoError(t, apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: filler}).Err())

	// Two immutable ready bodies leave a small genuine shard-byte budget.
	// Sharing the test backing slice keeps the fixture small while snapshot
	// validation checks and accounts for both logical records.
	body := make([]byte, maxMessageBytes)
	full := m.state.Queues[filler]
	full.LastMessage = 2
	full.Ready = []messageState{{ID: 1, Body: body}, {ID: 2, Body: body}}
	require.NoError(t, m.state.validate(m.assignments))
	remaining := maxBodyBytes - int(m.state.bodies)
	require.Positive(t, remaining)

	first, tail := []byte("first"), []byte("tail")
	require.LessOrEqual(t, len(first)+len(tail), remaining, "tail would fit if the failed message were bypassed")
	tooLargeForShard := make([]byte, remaining)
	result := applyPublishBatchWire(t, m, QueueCommand{Kind: publishBatchWireKind, Queue: queueName, Incarnation: q.Queue.Incarnation},
		publishBatchWireBody(first, tooLargeForShard, tail))
	require.ErrorIs(t, result.Err(), ErrCapacity)
	require.EqualValues(t, 1, result.Count)
	expectPublishedBodies(t, m, q.Queue.Incarnation, first)
}

func TestPublishBatchWireBoundsAndSingleMessageFallback(t *testing.T) {
	const batchByteLimit = 1 << 20
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	command := QueueCommand{Kind: publishBatchWireKind, Queue: queueName, Incarnation: q.Queue.Incarnation}
	validBodies := make([][]byte, 64)
	for i := range validBodies {
		validBodies[i] = []byte{byte(i)}
	}
	validBodies[0] = make([]byte, batchByteLimit-4*len(validBodies)-(len(validBodies)-1))
	valid := publishBatchWireBody(validBodies...)
	require.Len(t, valid, batchByteLimit)

	oversizedSingle := make([]byte, batchByteLimit+1)
	incompatible := command
	incompatible.Session, incompatible.Sequence = 1, 1
	for _, tc := range []struct {
		name    string
		command QueueCommand
		body    []byte
	}{
		{"single_record", command, publishBatchWireBody([]byte("one"))},
		{"too_many_records", command, publishBatchWireBody(make([][]byte, 65)...)},
		{"body_over_limit", command, publishBatchWireBody(nil, oversizedSingle)},
		{"incomplete_length", command, append(publishBatchWireBody(nil, nil), 1)},
		{"incomplete_message", command, append(publishBatchWireBody(nil, nil), 0, 0, 0, 1)},
		{"session_metadata", incompatible, valid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := applyPublishBatchWire(t, m, tc.command, tc.body)
			require.ErrorIs(t, reply.Err(), ErrInvalidCommand)
			require.Zero(t, reply.Count)
			require.Zero(t, apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName}).Queue.Ready)
		})
	}
	reply := applyPublishBatchWire(t, m, command, valid)
	require.NoError(t, reply.Err())
	require.EqualValues(t, 64, reply.Count)
	legacy := apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: oversizedSingle})
	require.NoError(t, legacy.Err())
	require.EqualValues(t, 65, legacy.Result.MessageID)
}

func TestPublishBatchEncoderMatchesWire(t *testing.T) {
	bodies := [][]byte{{0xff, 0, 0xc0}, nil, {0x80, 2}}
	want, err := encodeFrame(QueueCommand{Kind: publishBatchWireKind, Queue: queueName, Incarnation: 7}, publishBatchWireBody(bodies...))
	require.NoError(t, err)
	wire, err := EncodePublishBatch(queueName, 7, bodies)
	require.NoError(t, err)
	require.Equal(t, want, wire)

	bodies[0][0] = 0
	require.Equal(t, want, wire, "the encoded batch owns its wire bytes")
	command, err := DecodeCommand(wire)
	require.NoError(t, err)
	require.Equal(t, PublishBatch, command.Kind)
	encodedAgain, err := EncodeCommand(command)
	require.NoError(t, err)
	require.Equal(t, wire, encodedAgain)

	_, err = EncodePublishBatch(queueName, 0, [][]byte{nil})
	require.ErrorIs(t, err, ErrInvalidCommand)
	_, err = EncodePublishBatch(queueName, 0, make([][]byte, 65))
	require.ErrorIs(t, err, ErrInvalidCommand)
	_, err = EncodePublishBatch(queueName, 0, [][]byte{nil, make([]byte, (1<<20)-7)})
	require.ErrorIs(t, err, ErrCapacity)
}
